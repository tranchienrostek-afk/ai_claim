from __future__ import annotations

import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any


BASE_DIR = Path(__file__).resolve().parent
PIPELINE_DIR = BASE_DIR.parents[1] / "workspaces" / "claims_insights" / "pipeline"
if str(PIPELINE_DIR) not in sys.path:
    sys.path.insert(0, str(PIPELINE_DIR))

from reasoning_experience_memory import (  # noqa: E402
    ReasoningExperience,
    ReasoningExperienceMemory,
    as_text,
    infer_importance,
    infer_scope,
    tokenize,
    unique_texts,
)


DEFAULT_MEMORY_PATH = BASE_DIR / "experience_memory" / "reasoning_experience_memory.jsonl"


def now_iso() -> str:
    return datetime.now().astimezone().isoformat()


def load_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"Expected JSON object at {path}")
    return payload


def collect_sign_terms(report: dict[str, Any]) -> list[str]:
    sign_sets = report.get("sign_sets") or {}
    items = []
    items.extend(sign_sets.get("structured_signs") or [])
    items.extend(sign_sets.get("chief_plus_findings") or [])
    return unique_texts(items)[:12]


def collect_service_terms(report: dict[str, Any]) -> list[str]:
    rows = ((report.get("service_mapping_test") or {}).get("rows") or [])
    return unique_texts([as_text(row.get("service_name_raw")) for row in rows])[:12]


def build_query_terms(disease_name: str, sign_terms: list[str], service_terms: list[str], specialty: str) -> list[str]:
    tokens: list[str] = []
    tokens.extend(tokenize(disease_name))
    tokens.extend(tokenize(specialty))
    for item in sign_terms[:6]:
        tokens.extend(tokenize(item))
    for item in service_terms[:6]:
        tokens.extend(tokenize(item))
    return unique_texts(tokens)


def summarize_unresolved_services(report: dict[str, Any], limit: int = 4) -> list[str]:
    rows = ((report.get("service_mapping_test") or {}).get("rows") or [])
    unresolved: list[str] = []
    for row in rows:
        resolution = as_text(row.get("base_resolution"))
        confidence = as_text(((row.get("base_top") or {}).get("confidence")))
        if resolution == "coded" and confidence == "HIGH":
            continue
        service_name = as_text(row.get("service_name_raw"))
        if service_name and service_name not in unresolved:
            unresolved.append(service_name)
    return unresolved[:limit]


def build_case_experiences(case_summary: dict[str, Any], report: dict[str, Any]) -> list[ReasoningExperience]:
    case_level = report.get("case_level") or {}
    request_id = as_text(case_summary.get("request_id") or case_level.get("request_id"))
    specialty = as_text(case_summary.get("specialty") or case_level.get("specialty"))
    disease_name = as_text(case_summary.get("expected_disease") or case_level.get("main_disease_name_vi"))
    case_title = as_text(case_summary.get("testcase_title") or case_level.get("testcase_title"))
    sign_terms = collect_sign_terms(report)
    service_terms = collect_service_terms(report)
    query_terms = build_query_terms(disease_name, sign_terms, service_terms, specialty)
    protocol_probe = report.get("protocol_probe") or {}
    experiences: list[ReasoningExperience] = []

    def add_experience(
        *,
        category: str,
        severity: str,
        trigger_summary: str,
        recommendation: str,
        evidence: dict[str, Any],
    ) -> None:
        experience_id = ReasoningExperienceMemory.build_experience_id(
            source_request_id=request_id,
            category=category,
            disease_name=disease_name,
            trigger_summary=trigger_summary,
        )
        experiences.append(
            ReasoningExperience(
                experience_id=experience_id,
                created_at=now_iso(),
                category=category,
                severity=severity,
                specialty=specialty,
                source_request_id=request_id,
                source_case_title=case_title,
                disease_name=disease_name,
                trigger_summary=trigger_summary,
                recommendation=recommendation,
                sign_terms=sign_terms,
                service_terms=service_terms,
                query_terms=query_terms,
                evidence=evidence,
                memory_kind="episodic",
                scope=infer_scope(category),
                importance=infer_importance(category, severity, "episodic"),
            )
        )

    top1_hit = bool(case_summary.get("disease_top1_hit"))
    top3_hit = bool(case_summary.get("disease_top3_hit"))
    top1_disease = as_text(case_summary.get("top1_disease"))
    service_count = int(case_summary.get("service_count") or 0)
    family_resolved = int(case_summary.get("base_family_resolved_count") or 0)
    coded_high = int(case_summary.get("base_high_confidence_count") or 0)
    overlap_codes = case_summary.get("sign_service_overlap_codes") or []
    unresolved_services = summarize_unresolved_services(report)

    if not top1_hit:
        add_experience(
            category="disease_hypothesis_gap",
            severity="high",
            trigger_summary=f"Top-1 disease miss for {disease_name}; predicted {top1_disease or 'unknown'}",
            recommendation=(
                f"Curate atomic sign concepts and disease profile for '{disease_name}'. "
                f"Ưu tiên các dấu hiệu: {', '.join(sign_terms[:5]) or 'không có'}."
            ),
            evidence={
                "top1_disease": top1_disease,
                "top3_hit": top3_hit,
                "report_json": case_summary.get("report_json"),
            },
        )

    if family_resolved < service_count:
        add_experience(
            category="service_family_gap",
            severity="high",
            trigger_summary=f"Only {family_resolved}/{service_count} services reached family resolution",
            recommendation=(
                f"Expand service family taxonomy/aliases cho '{disease_name}'. "
                f"Dịch vụ còn hở: {', '.join(unresolved_services) or 'không rõ'}."
            ),
            evidence={
                "service_count": service_count,
                "family_resolved": family_resolved,
                "unresolved_services": unresolved_services,
                "report_json": case_summary.get("report_json"),
            },
        )

    if coded_high < service_count:
        add_experience(
            category="service_code_gap",
            severity="medium" if family_resolved == service_count else "high",
            trigger_summary=f"Only {coded_high}/{service_count} services reached high-confidence code mapping",
            recommendation=(
                f"Giữ family-first, nhưng bổ sung alias/canonical code cho '{disease_name}'. "
                f"Ưu tiên code hóa các dịch vụ: {', '.join(unresolved_services) or ', '.join(service_terms[:4]) or 'không rõ'}."
            ),
            evidence={
                "service_count": service_count,
                "coded_high": coded_high,
                "unresolved_services": unresolved_services,
                "report_json": case_summary.get("report_json"),
            },
        )

    if not overlap_codes:
        add_experience(
            category="service_expectation_gap",
            severity="medium",
            trigger_summary=f"Sign reasoning produced no overlap with observed services for {disease_name}",
            recommendation=(
                f"Rà lại link Disease -> Expected Services cho '{disease_name}', "
                f"đồng thời kiểm tra sign decomposition với các dấu hiệu: {', '.join(sign_terms[:4]) or 'không rõ'}."
            ),
            evidence={
                "overlap_codes": overlap_codes,
                "service_terms": service_terms[:6],
                "report_json": case_summary.get("report_json"),
            },
        )

    if disease_name and not bool(protocol_probe.get("disease_protocol_hit")):
        add_experience(
            category="protocol_coverage_gap",
            severity="medium",
            trigger_summary=f"Protocol text pack does not contain direct hit for {disease_name}",
            recommendation=(
                f"Ingest hoặc canonicalize lại disease title '{disease_name}' vào protocol graph, "
                "để reasoning không phải dựa quá nhiều vào fallback text matching."
            ),
            evidence={
                "disease_protocol_hit": False,
                "story_title_protocol_hit": bool(protocol_probe.get("story_title_protocol_hit")),
                "report_json": case_summary.get("report_json"),
            },
        )

    if top1_hit and family_resolved == service_count and service_count > 0:
        add_experience(
            category="successful_pattern",
            severity="low",
            trigger_summary=f"Stable pattern for {disease_name}",
            recommendation=(
                f"Giữ pattern hiện tại cho '{disease_name}': signs {', '.join(sign_terms[:4]) or 'không rõ'} "
                f"-> services {', '.join(service_terms[:4]) or 'không rõ'}."
            ),
            evidence={
                "top1_disease": top1_disease,
                "service_count": service_count,
                "overlap_codes": overlap_codes,
                "report_json": case_summary.get("report_json"),
            },
        )

    return experiences


def build_reasoning_experience_artifacts(
    batch_payload: dict[str, Any],
    output_dir: Path,
    memory_path: Path = DEFAULT_MEMORY_PATH,
) -> dict[str, Any]:
    memory = ReasoningExperienceMemory(memory_path)
    run_experiences: list[ReasoningExperience] = []
    for case_summary in batch_payload.get("cases") or []:
        report_json = as_text(case_summary.get("report_json"))
        if not report_json:
            continue
        report_path = Path(report_json)
        if not report_path.exists():
            continue
        report = load_json(report_path)
        run_experiences.extend(build_case_experiences(case_summary, report))

    experience_dir = output_dir / "experience_memory"
    experience_dir.mkdir(parents=True, exist_ok=True)
    run_jsonl = experience_dir / "run_reasoning_experiences.jsonl"
    run_jsonl.write_text(
        "".join(json.dumps(item.to_dict(), ensure_ascii=False) + "\n" for item in run_experiences),
        encoding="utf-8",
    )

    append_stats = memory.append(run_experiences)
    promotion_stats = memory.promote_memories(run_experiences)
    normalization_stats = memory.normalize_store()
    memory_stats = memory.stats()
    summary = {
        "generated_at": now_iso(),
        "run_experience_count": len(run_experiences),
        "memory_append": append_stats,
        "memory_promotion": promotion_stats,
        "memory_normalization": normalization_stats,
        "memory_stats": memory_stats,
        "categories": {},
        "severity": {},
        "by_scope": {},
        "by_kind": {},
    }
    for item in run_experiences:
        summary["categories"][item.category] = summary["categories"].get(item.category, 0) + 1
        summary["severity"][item.severity] = summary["severity"].get(item.severity, 0) + 1
        summary["by_scope"][item.scope] = summary["by_scope"].get(item.scope, 0) + 1
        summary["by_kind"][item.memory_kind] = summary["by_kind"].get(item.memory_kind, 0) + 1

    summary_json = experience_dir / "reasoning_experience_summary.json"
    summary_md = experience_dir / "reasoning_experience_summary.md"
    summary_json.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    lines = [
        "# Reasoning Experience Summary",
        "",
        f"- Run experiences: `{summary['run_experience_count']}`",
        f"- Appended to memory: `{append_stats['appended']}`",
        f"- Promoted memories appended: `{promotion_stats['appended']}`",
        f"- Memory total: `{memory_stats['experience_count']}`",
        "",
        "## Category Mix",
        "",
    ]
    for category, count in sorted(summary["categories"].items()):
        lines.append(f"- `{category}`: `{count}`")
    lines.extend(["", "## Scope Mix", ""])
    for scope, count in sorted(summary["by_scope"].items()):
        lines.append(f"- `{scope}`: `{count}`")
    lines.extend(["", "## Promotion", ""])
    lines.append(f"- Episodic source rows: `{promotion_stats['source_episodic_count']}`")
    lines.append(f"- Generated promoted rows: `{promotion_stats['generated_count']}`")
    lines.append(f"- Appended promoted rows: `{promotion_stats['appended']}`")
    lines.extend(["", "## Latest Recommendations", ""])
    for item in run_experiences[:12]:
        lines.append(f"- `{item.category}` | `{item.source_request_id}` | {item.recommendation}")
    summary_md.write_text("\n".join(lines) + "\n", encoding="utf-8")

    return {
        "run_jsonl": str(run_jsonl),
        "summary_json": str(summary_json),
        "summary_md": str(summary_md),
        "memory_path": str(memory_path),
        "appended": append_stats["appended"],
        "promoted_appended": promotion_stats["appended"],
        "memory_total": memory_stats["experience_count"],
    }


def main() -> None:
    if len(sys.argv) < 2:
        raise SystemExit("Usage: python build_reasoning_experiences_from_batch.py <batch_summary.json> [memory_path]")
    batch_summary = Path(sys.argv[1]).resolve()
    if not batch_summary.exists():
        raise SystemExit(f"Batch summary not found: {batch_summary}")
    memory_path = Path(sys.argv[2]).resolve() if len(sys.argv) >= 3 else DEFAULT_MEMORY_PATH
    payload = load_json(batch_summary)
    outputs = build_reasoning_experience_artifacts(payload, batch_summary.parent, memory_path)
    print(json.dumps(outputs, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
