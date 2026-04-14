from __future__ import annotations

import hashlib
import json
import math
import re
import unicodedata
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable


INGEST_CATEGORIES = {
    "service_family_gap",
    "service_code_gap",
    "protocol_coverage_gap",
}

REASONING_CATEGORIES = {
    "disease_hypothesis_gap",
    "service_expectation_gap",
}

SHARED_CATEGORIES = {
    "successful_pattern",
}

SEVERITY_BASE_IMPORTANCE = {
    "high": 0.84,
    "medium": 0.66,
    "low": 0.42,
}

CATEGORY_IMPORTANCE_BONUS = {
    "service_code_gap": 0.08,
    "service_family_gap": 0.08,
    "disease_hypothesis_gap": 0.08,
    "protocol_coverage_gap": 0.06,
    "service_expectation_gap": 0.04,
    "successful_pattern": 0.02,
}

MEMORY_KIND_IMPORTANCE_BONUS = {
    "episodic": 0.0,
    "semantic": 0.05,
    "procedural": 0.09,
}

MEMORY_KIND_RETRIEVAL_BONUS = {
    "episodic": 0.12,
    "semantic": 0.28,
    "procedural": 0.34,
}


def now_iso() -> str:
    return datetime.now().astimezone().isoformat()


def as_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    return str(value).strip()


def normalize_text(value: Any) -> str:
    text = as_text(value).lower().replace("đ", "d").replace("Đ", "d")
    normalized = unicodedata.normalize("NFD", text)
    stripped = "".join(ch for ch in normalized if unicodedata.category(ch) != "Mn")
    stripped = re.sub(r"[^a-z0-9 ]+", " ", stripped)
    return re.sub(r"\s+", " ", stripped).strip()


def unique_texts(values: list[Any]) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for value in values:
        text = as_text(value)
        if not text:
            continue
        key = normalize_text(text)
        if not key or key in seen:
            continue
        seen.add(key)
        ordered.append(text)
    return ordered


def tokenize(value: Any) -> list[str]:
    generic = {
        "",
        "benh",
        "hoi",
        "chung",
        "tinh",
        "cap",
        "man",
        "va",
        "co",
        "khong",
        "tai",
        "mui",
        "hong",
    }
    return [token for token in normalize_text(value).split() if len(token) > 2 and token not in generic]


def overlap_score(left: list[str], right: list[str]) -> float:
    left_set = {token for token in left if token}
    right_set = {token for token in right if token}
    if not left_set or not right_set:
        return 0.0
    overlap = left_set & right_set
    if not overlap:
        return 0.0
    return len(overlap) / max(len(left_set | right_set), 1)


def infer_scope(category: str) -> str:
    category = as_text(category)
    if category in SHARED_CATEGORIES:
        return "shared"
    if category in INGEST_CATEGORIES:
        return "ingest"
    if category in REASONING_CATEGORIES:
        return "reasoning"
    return "shared"


def infer_importance(category: str, severity: str, memory_kind: str) -> float:
    severity_key = normalize_text(severity) or "medium"
    memory_kind_key = normalize_text(memory_kind) or "episodic"
    base = SEVERITY_BASE_IMPORTANCE.get(severity_key, 0.56)
    category_bonus = CATEGORY_IMPORTANCE_BONUS.get(as_text(category), 0.0)
    kind_bonus = MEMORY_KIND_IMPORTANCE_BONUS.get(memory_kind_key, 0.0)
    return round(min(0.98, max(0.15, base + category_bonus + kind_bonus)), 4)


def parse_iso_datetime(value: Any) -> datetime | None:
    text = as_text(value)
    if not text:
        return None
    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def recency_bonus(timestamp: Any) -> float:
    dt = parse_iso_datetime(timestamp)
    if dt is None:
        return 0.0
    age_days = max((datetime.now().astimezone() - dt.astimezone()).total_seconds() / 86400.0, 0.0)
    if age_days <= 7:
        return 0.24
    if age_days <= 30:
        return 0.14
    if age_days <= 90:
        return 0.06
    return 0.0


@dataclass
class ReasoningExperience:
    experience_id: str
    created_at: str
    category: str
    severity: str
    specialty: str
    source_request_id: str
    source_case_title: str
    disease_name: str
    trigger_summary: str
    recommendation: str
    sign_terms: list[str]
    service_terms: list[str]
    query_terms: list[str]
    evidence: dict[str, Any]
    memory_kind: str = "episodic"
    scope: str = "reasoning"
    importance: float = 0.5
    access_count: int = 0
    last_accessed_at: str = ""
    source_experience_ids: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class ReasoningExperienceMemory:
    def __init__(self, memory_path: Path) -> None:
        self.memory_path = Path(memory_path)
        self.memory_path.parent.mkdir(parents=True, exist_ok=True)
        if not self.memory_path.exists():
            self.memory_path.write_text("", encoding="utf-8")

    @staticmethod
    def build_experience_id(
        *,
        source_request_id: str,
        category: str,
        disease_name: str,
        trigger_summary: str,
        memory_kind: str = "",
    ) -> str:
        raw = "||".join(
            [
                normalize_text(source_request_id),
                normalize_text(category),
                normalize_text(memory_kind),
                normalize_text(disease_name),
                normalize_text(trigger_summary),
            ]
        )
        digest = hashlib.sha1(raw.encode("utf-8")).hexdigest()[:12]
        return f"exp_{digest}"

    def _normalize_memory_row(self, payload: dict[str, Any]) -> dict[str, Any]:
        row = dict(payload)
        category = as_text(row.get("category"))
        severity = as_text(row.get("severity")) or "medium"
        memory_kind = normalize_text(row.get("memory_kind")) or "episodic"
        scope = normalize_text(row.get("scope")) or infer_scope(category)
        importance_raw = row.get("importance")
        try:
            importance = float(importance_raw)
        except (TypeError, ValueError):
            importance = infer_importance(category, severity, memory_kind)

        sign_terms = unique_texts(list(row.get("sign_terms") or []))
        service_terms = unique_texts(list(row.get("service_terms") or []))
        query_terms = unique_texts(list(row.get("query_terms") or []))
        if not query_terms:
            tokens: list[str] = []
            tokens.extend(tokenize(row.get("disease_name")))
            tokens.extend(tokenize(row.get("specialty")))
            for item in sign_terms[:6]:
                tokens.extend(tokenize(item))
            for item in service_terms[:6]:
                tokens.extend(tokenize(item))
            query_terms = unique_texts(tokens)

        source_experience_ids = unique_texts(list(row.get("source_experience_ids") or []))
        if not source_experience_ids and as_text(row.get("experience_id")) and memory_kind != "episodic":
            source_experience_ids = [as_text(row.get("experience_id"))]

        try:
            access_count = int(row.get("access_count") or 0)
        except (TypeError, ValueError):
            access_count = 0

        normalized = {
            "experience_id": as_text(row.get("experience_id")),
            "created_at": as_text(row.get("created_at")) or now_iso(),
            "category": category,
            "severity": severity,
            "specialty": as_text(row.get("specialty")),
            "source_request_id": as_text(row.get("source_request_id")),
            "source_case_title": as_text(row.get("source_case_title")),
            "disease_name": as_text(row.get("disease_name")),
            "trigger_summary": as_text(row.get("trigger_summary")),
            "recommendation": as_text(row.get("recommendation")),
            "sign_terms": sign_terms,
            "service_terms": service_terms,
            "query_terms": query_terms,
            "evidence": row.get("evidence") if isinstance(row.get("evidence"), dict) else {},
            "memory_kind": memory_kind,
            "scope": scope or "shared",
            "importance": round(min(0.98, max(0.15, importance)), 4),
            "access_count": max(access_count, 0),
            "last_accessed_at": as_text(row.get("last_accessed_at")),
            "source_experience_ids": source_experience_ids,
        }

        if not normalized["experience_id"]:
            normalized["experience_id"] = self.build_experience_id(
                source_request_id=normalized["source_request_id"] or "memory",
                category=normalized["category"] or "unknown",
                disease_name=normalized["disease_name"],
                trigger_summary=normalized["trigger_summary"] or normalized["recommendation"],
                memory_kind=normalized["memory_kind"],
            )
        return normalized

    def load_all(self) -> list[dict[str, Any]]:
        if not self.memory_path.exists() or not self.memory_path.read_text(encoding="utf-8").strip():
            return []
        rows: list[dict[str, Any]] = []
        with self.memory_path.open("r", encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                try:
                    payload = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if isinstance(payload, dict):
                    rows.append(self._normalize_memory_row(payload))
        return rows

    def _write_rows(self, rows: list[dict[str, Any]]) -> None:
        with self.memory_path.open("w", encoding="utf-8") as handle:
            for payload in rows:
                handle.write(json.dumps(payload, ensure_ascii=False) + "\n")

    def normalize_store(self) -> dict[str, Any]:
        rows = self.load_all()
        self._write_rows(rows)
        return {"normalized_rows": len(rows), "memory_path": str(self.memory_path)}

    def append(self, experiences: list[ReasoningExperience | dict[str, Any]]) -> dict[str, int]:
        existing = self.load_all()
        existing_ids = {as_text(item.get("experience_id")) for item in existing if as_text(item.get("experience_id"))}
        new_rows: list[dict[str, Any]] = []
        for item in experiences:
            payload = item.to_dict() if isinstance(item, ReasoningExperience) else dict(item)
            normalized = self._normalize_memory_row(payload)
            experience_id = normalized["experience_id"]
            if not experience_id or experience_id in existing_ids:
                continue
            existing_ids.add(experience_id)
            new_rows.append(normalized)
        if not new_rows:
            return {"appended": 0, "total": len(existing)}
        with self.memory_path.open("a", encoding="utf-8") as handle:
            for payload in new_rows:
                handle.write(json.dumps(payload, ensure_ascii=False) + "\n")
        return {"appended": len(new_rows), "total": len(existing) + len(new_rows)}

    def _build_semantic_fact(self, row: dict[str, Any]) -> str:
        disease_name = as_text(row.get("disease_name")) or "ca bệnh này"
        category = as_text(row.get("category"))
        if category == "successful_pattern":
            signs = ", ".join((row.get("sign_terms") or [])[:4]) or "không rõ"
            services = ", ".join((row.get("service_terms") or [])[:4]) or "không rõ"
            return f"Pattern ổn định cho {disease_name}: signs {signs} thường đi với services {services}."
        if category == "service_code_gap":
            services = ", ".join((row.get("service_terms") or [])[:4]) or "không rõ"
            return f"{disease_name} còn thiếu canonical service code cho các dịch vụ: {services}."
        if category == "service_family_gap":
            services = ", ".join((row.get("service_terms") or [])[:4]) or "không rõ"
            return f"{disease_name} còn thiếu family resolution cho các dịch vụ: {services}."
        if category == "disease_hypothesis_gap":
            signs = ", ".join((row.get("sign_terms") or [])[:5]) or "không rõ"
            return f"{disease_name} cần làm dày sign concept/profile vì signs {signs} chưa kéo đúng hypothesis."
        if category == "service_expectation_gap":
            return f"{disease_name} còn hở link Disease -> Expected Services trong reasoning."
        if category == "protocol_coverage_gap":
            return f"{disease_name} chưa phủ tốt trong protocol graph hiện tại."
        return as_text(row.get("trigger_summary")) or as_text(row.get("recommendation"))

    def _promote_row(self, row: dict[str, Any], memory_kind: str) -> dict[str, Any]:
        source_id = as_text(row.get("experience_id"))
        scope = as_text(row.get("scope")) or infer_scope(row.get("category"))
        category = as_text(row.get("category"))
        disease_name = as_text(row.get("disease_name"))
        if memory_kind == "procedural":
            trigger_summary = as_text(row.get("trigger_summary")) or as_text(row.get("recommendation"))
            recommendation = as_text(row.get("recommendation")) or as_text(row.get("trigger_summary"))
        else:
            trigger_summary = self._build_semantic_fact(row)
            recommendation = as_text(row.get("recommendation")) or trigger_summary

        promoted = {
            "experience_id": self.build_experience_id(
                source_request_id=f"promoted:{scope}:{memory_kind}",
                category=category,
                disease_name=disease_name,
                trigger_summary=trigger_summary,
                memory_kind=memory_kind,
            ),
            "created_at": now_iso(),
            "category": category,
            "severity": as_text(row.get("severity")) or "medium",
            "specialty": as_text(row.get("specialty")),
            "source_request_id": f"promoted:{scope}:{memory_kind}",
            "source_case_title": f"promoted::{as_text(row.get('source_case_title')) or disease_name}",
            "disease_name": disease_name,
            "trigger_summary": trigger_summary,
            "recommendation": recommendation,
            "sign_terms": unique_texts(list(row.get("sign_terms") or [])),
            "service_terms": unique_texts(list(row.get("service_terms") or [])),
            "query_terms": unique_texts(list(row.get("query_terms") or [])),
            "evidence": {
                "promoted_from": source_id,
                "promoted_from_category": category,
                "promoted_from_kind": as_text(row.get("memory_kind")) or "episodic",
                "original_evidence": row.get("evidence") or {},
            },
            "memory_kind": memory_kind,
            "scope": "shared" if scope == "shared" else scope,
            "importance": infer_importance(category, as_text(row.get("severity")), memory_kind),
            "access_count": 0,
            "last_accessed_at": "",
            "source_experience_ids": [source_id] if source_id else [],
        }
        return self._normalize_memory_row(promoted)

    def promote_memories(self, experiences: Iterable[ReasoningExperience | dict[str, Any]]) -> dict[str, Any]:
        source_rows = [
            self._normalize_memory_row(item.to_dict() if isinstance(item, ReasoningExperience) else dict(item))
            for item in experiences
        ]
        episodic_rows = [row for row in source_rows if as_text(row.get("memory_kind")) == "episodic"]
        promoted_rows: list[dict[str, Any]] = []
        for row in episodic_rows:
            promoted_rows.append(self._promote_row(row, "procedural"))
            promoted_rows.append(self._promote_row(row, "semantic"))
        append_stats = self.append(promoted_rows)
        return {
            "source_episodic_count": len(episodic_rows),
            "generated_count": len(promoted_rows),
            "appended": append_stats["appended"],
            "total": append_stats["total"],
        }

    def promote_existing_memories(self) -> dict[str, Any]:
        rows = self.load_all()
        episodic_rows = [row for row in rows if as_text(row.get("memory_kind")) == "episodic"]
        return self.promote_memories(episodic_rows)

    def query(
        self,
        *,
        disease_name: str = "",
        specialty: str = "",
        sign_terms: list[str] | None = None,
        service_terms: list[str] | None = None,
        scopes: str | list[str] | None = None,
        memory_kinds: list[str] | None = None,
        categories: list[str] | None = None,
        min_importance: float = 0.0,
        top_k: int = 5,
    ) -> list[dict[str, Any]]:
        sign_terms = unique_texts(sign_terms or [])
        service_terms = unique_texts(service_terms or [])
        disease_tokens = tokenize(disease_name)
        sign_tokens = [token for term in sign_terms for token in tokenize(term)]
        service_tokens = [token for term in service_terms for token in tokenize(term)]
        query_tokens = unique_texts(disease_tokens + sign_tokens + service_tokens)

        allowed_scopes = {
            normalize_text(item)
            for item in (
                [scopes] if isinstance(scopes, str) else (scopes or [])
            )
            if normalize_text(item)
        }
        allowed_kinds = {normalize_text(item) for item in (memory_kinds or []) if normalize_text(item)}
        allowed_categories = {as_text(item) for item in (categories or []) if as_text(item)}

        scored: list[tuple[float, dict[str, Any]]] = []
        for experience in self.load_all():
            row_scope = normalize_text(experience.get("scope")) or "shared"
            if allowed_scopes and row_scope not in allowed_scopes and row_scope != "shared":
                continue
            row_kind = normalize_text(experience.get("memory_kind")) or "episodic"
            if allowed_kinds and row_kind not in allowed_kinds:
                continue
            row_category = as_text(experience.get("category"))
            if allowed_categories and row_category not in allowed_categories:
                continue

            importance = float(experience.get("importance") or 0.0)
            if importance < min_importance:
                continue

            score = 0.0
            exp_specialty = as_text(experience.get("specialty"))
            if specialty and normalize_text(exp_specialty) == normalize_text(specialty):
                score += 1.2
            score += 4.0 * overlap_score(disease_tokens, tokenize(experience.get("disease_name")))
            score += 2.4 * overlap_score(
                sign_tokens,
                [token for term in experience.get("sign_terms") or [] for token in tokenize(term)],
            )
            score += 2.1 * overlap_score(
                service_tokens,
                [token for term in experience.get("service_terms") or [] for token in tokenize(term)],
            )
            score += 1.8 * overlap_score(
                query_tokens,
                [token for term in experience.get("query_terms") or [] for token in tokenize(term)],
            )
            score += MEMORY_KIND_RETRIEVAL_BONUS.get(row_kind, 0.0)
            score += 0.9 * importance
            score += recency_bonus(experience.get("last_accessed_at") or experience.get("created_at"))
            score += min(math.log1p(max(int(experience.get("access_count") or 0), 0)) * 0.08, 0.3)
            if allowed_scopes and row_scope in allowed_scopes:
                score += 0.2
            if row_category == "successful_pattern":
                score += 0.12
            if score <= 0.0:
                continue
            payload = dict(experience)
            payload["match_score"] = round(score, 4)
            scored.append((score, payload))

        scored.sort(key=lambda item: item[0], reverse=True)
        return [payload for _, payload in scored[:top_k]]

    def summarize_matches(self, matches: list[dict[str, Any]], limit: int = 3) -> list[str]:
        ordered_matches = sorted(
            matches,
            key=lambda item: (
                {"procedural": 0, "semantic": 1, "episodic": 2}.get(normalize_text(item.get("memory_kind")), 9),
                -float(item.get("match_score") or 0.0),
            ),
        )
        recommendations: list[str] = []
        for item in ordered_matches:
            text = as_text(item.get("recommendation"))
            if text and text not in recommendations:
                recommendations.append(text)
            if len(recommendations) >= limit:
                break
        return recommendations

    def stats(self) -> dict[str, Any]:
        rows = self.load_all()
        by_category: dict[str, int] = {}
        by_severity: dict[str, int] = {}
        by_kind: dict[str, int] = {}
        by_scope: dict[str, int] = {}
        for item in rows:
            category = as_text(item.get("category")) or "unknown"
            severity = as_text(item.get("severity")) or "unknown"
            kind = as_text(item.get("memory_kind")) or "unknown"
            scope = as_text(item.get("scope")) or "unknown"
            by_category[category] = by_category.get(category, 0) + 1
            by_severity[severity] = by_severity.get(severity, 0) + 1
            by_kind[kind] = by_kind.get(kind, 0) + 1
            by_scope[scope] = by_scope.get(scope, 0) + 1
        return {
            "memory_path": str(self.memory_path),
            "experience_count": len(rows),
            "by_category": by_category,
            "by_severity": by_severity,
            "by_kind": by_kind,
            "by_scope": by_scope,
        }
