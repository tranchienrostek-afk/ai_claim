from __future__ import annotations

import json
import re
import unicodedata
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
from rapidfuzz import fuzz
from sklearn.feature_extraction.text import TfidfVectorizer

from auto_review import strip_noise, to_skeleton
from chuan_hoa_dich_vu import normalize_service_name


DATA_DIR = Path(__file__).parent
DEFAULT_TAXONOMY_PATH = DATA_DIR / "tmh_service_family_taxonomy.json"
MOJIBAKE_HINTS = ("Ã", "Ä", "Å", "Æ", "Â", "Ð")


def repair_text(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    if any(hint in text for hint in MOJIBAKE_HINTS):
        try:
            repaired = text.encode("latin1").decode("utf-8")
            if repaired:
                return repaired
        except Exception:
            pass
    return text


def ascii_fold(value: str) -> str:
    lowered = repair_text(value).lower()
    lowered = lowered.replace("đ", "d").replace("Đ", "d")
    normalized = unicodedata.normalize("NFD", lowered)
    stripped = "".join(ch for ch in normalized if unicodedata.category(ch) != "Mn")
    stripped = re.sub(r"[^a-z0-9 ]+", " ", stripped)
    return re.sub(r"\s+", " ", stripped).strip()


def normalize_family_text(value: Any) -> tuple[str, str, str]:
    repaired = repair_text(value)
    cleaned, _rules = normalize_service_name(repaired)
    stripped = strip_noise(cleaned)
    skeleton = to_skeleton(stripped or cleaned)
    folded = ascii_fold(skeleton or stripped or cleaned)
    return cleaned, stripped, folded


def token_set(value: str) -> set[str]:
    return {token for token in ascii_fold(value).split() if len(token) >= 2}


@dataclass(frozen=True)
class FamilyAlias:
    family_id: str
    family_label: str
    alias_text: str
    cleaned_name: str
    stripped_name: str
    folded_name: str
    tokens: frozenset[str]


class ServiceFamilyMapper:
    def __init__(self, taxonomy_path: Path = DEFAULT_TAXONOMY_PATH) -> None:
        self.taxonomy_path = taxonomy_path
        payload = json.loads(taxonomy_path.read_text(encoding="utf-8"))
        self.version = str(payload.get("version") or "")
        self.family_labels: dict[str, str] = {}
        self.aliases: list[FamilyAlias] = []
        self._load_aliases(payload)

        if not self.aliases:
            raise ValueError(f"No family aliases loaded from taxonomy: {taxonomy_path}")

        self.cleaned_corpus = [alias.cleaned_name for alias in self.aliases]
        self.folded_corpus = [alias.folded_name for alias in self.aliases]

        self.word_vectorizer = TfidfVectorizer(analyzer="word", ngram_range=(1, 3), lowercase=False)
        self.char_vectorizer = TfidfVectorizer(analyzer="char_wb", ngram_range=(3, 5), lowercase=False)
        self.word_matrix = self.word_vectorizer.fit_transform(self.cleaned_corpus)
        self.char_matrix = self.char_vectorizer.fit_transform(self.folded_corpus)

    def _load_aliases(self, payload: dict[str, Any]) -> None:
        seen: set[tuple[str, str]] = set()
        for family in payload.get("families") or []:
            family_id = str(family.get("family_id") or "").strip()
            family_label = str(family.get("label") or family_id).strip()
            if not family_id:
                continue
            self.family_labels[family_id] = family_label
            for alias in family.get("aliases") or []:
                cleaned, stripped, folded = normalize_family_text(alias)
                if not folded:
                    continue
                key = (family_id, folded)
                if key in seen:
                    continue
                seen.add(key)
                self.aliases.append(
                    FamilyAlias(
                        family_id=family_id,
                        family_label=family_label,
                        alias_text=str(alias),
                        cleaned_name=cleaned,
                        stripped_name=stripped,
                        folded_name=folded,
                        tokens=frozenset(token_set(folded)),
                    )
                )

    def _alias_score(
        self,
        *,
        query_cleaned: str,
        query_stripped: str,
        query_folded: str,
        query_tokens: set[str],
        alias: FamilyAlias,
        lexical_score: float,
    ) -> tuple[float, list[str]]:
        reasons: list[str] = []
        score = float(lexical_score)

        if query_cleaned and query_cleaned == alias.cleaned_name:
            score = max(score, 1.0)
            reasons.append("exact_cleaned")
        if query_stripped and query_stripped == alias.stripped_name:
            score = max(score, 0.99)
            reasons.append("exact_stripped")
        if query_folded and query_folded == alias.folded_name:
            score = max(score, 0.995)
            reasons.append("exact_folded")

        if query_folded and alias.folded_name:
            partial = fuzz.partial_ratio(query_folded, alias.folded_name) / 100.0
            score = max(score, partial * 0.92)
            if partial >= 0.92:
                reasons.append("high_partial_ratio")

        overlap = query_tokens & set(alias.tokens)
        if overlap:
            coverage = len(overlap) / max(len(alias.tokens), 1)
            query_coverage = len(overlap) / max(len(query_tokens), 1)
            overlap_score = 0.5 * coverage + 0.5 * query_coverage
            score = max(score, overlap_score * 0.95)
            if coverage >= 0.8 or query_coverage >= 0.8:
                reasons.append("high_token_overlap")

        return min(score, 1.0), reasons

    def score_text(self, raw_text: Any, top_k: int = 3) -> dict[str, Any]:
        cleaned, stripped, folded = normalize_family_text(raw_text)
        query_tokens = token_set(folded)

        if not folded:
            return {
                "mapping_status": "unknown",
                "family_id": None,
                "family_label": None,
                "family_score": 0.0,
                "family_confidence": "unknown",
                "matched_alias": None,
                "gap": None,
                "top_candidates": [],
                "query": {
                    "raw_text": str(raw_text or ""),
                    "cleaned_name": cleaned,
                    "stripped_name": stripped,
                    "folded_name": folded,
                },
            }

        query_word = self.word_vectorizer.transform([cleaned])
        query_char = self.char_vectorizer.transform([folded])
        word_scores = (self.word_matrix @ query_word.T).toarray().ravel()
        char_scores = (self.char_matrix @ query_char.T).toarray().ravel()
        lexical_scores = 0.45 * word_scores + 0.55 * char_scores

        family_buckets: dict[str, list[dict[str, Any]]] = {}
        for idx, alias in enumerate(self.aliases):
            score, reasons = self._alias_score(
                query_cleaned=cleaned,
                query_stripped=stripped,
                query_folded=folded,
                query_tokens=query_tokens,
                alias=alias,
                lexical_score=float(lexical_scores[idx]),
            )
            family_buckets.setdefault(alias.family_id, []).append(
                {
                    "family_id": alias.family_id,
                    "family_label": alias.family_label,
                    "alias_text": alias.alias_text,
                    "score": round(score, 4),
                    "reasons": reasons,
                }
            )

        family_candidates: list[dict[str, Any]] = []
        for family_id, rows in family_buckets.items():
            rows_sorted = sorted(rows, key=lambda row: (-row["score"], row["alias_text"]))
            top_rows = rows_sorted[:3]
            top_score = float(top_rows[0]["score"])
            avg_top3 = float(np.mean([row["score"] for row in top_rows]))
            support_hits = sum(1 for row in rows if row["score"] >= 0.65)
            family_score = min(1.0, 0.7 * top_score + 0.25 * avg_top3 + 0.05 * min(support_hits, 3) / 3)
            family_candidates.append(
                {
                    "family_id": family_id,
                    "family_label": self.family_labels.get(family_id, family_id),
                    "family_score": round(family_score, 4),
                    "top_alias": top_rows[0]["alias_text"],
                    "top_alias_score": round(top_score, 4),
                    "avg_top3_score": round(avg_top3, 4),
                    "support_hits": support_hits,
                    "top_alias_reasons": top_rows[0]["reasons"],
                }
            )

        family_candidates.sort(key=lambda row: (-row["family_score"], -row["top_alias_score"], row["family_id"]))
        best = family_candidates[0] if family_candidates else None
        second = family_candidates[1] if len(family_candidates) > 1 else None
        gap = round(float(best["family_score"] - second["family_score"]), 4) if best and second else None

        status = "unknown"
        confidence = "unknown"
        if best:
            score = float(best["family_score"])
            if score >= 0.98:
                status = "exact"
                confidence = "high"
            elif score >= 0.82 and (gap is None or gap >= 0.08):
                status = "probable"
                confidence = "high" if score >= 0.9 else "medium"
            elif score >= 0.68:
                status = "ambiguous"
                confidence = "medium" if score >= 0.76 else "low"

        return {
            "mapping_status": status,
            "family_id": best["family_id"] if best else None,
            "family_label": best["family_label"] if best else None,
            "family_score": best["family_score"] if best else 0.0,
            "family_confidence": confidence,
            "matched_alias": best["top_alias"] if best else None,
            "gap": gap,
            "top_candidates": family_candidates[:top_k],
            "query": {
                "raw_text": str(raw_text or ""),
                "cleaned_name": cleaned,
                "stripped_name": stripped,
                "folded_name": folded,
            },
        }
