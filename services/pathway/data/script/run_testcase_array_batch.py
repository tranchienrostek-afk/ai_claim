from __future__ import annotations

import json
import re
import sys
import unicodedata
from datetime import datetime
from pathlib import Path
from typing import Any


BASE_DIR = Path(__file__).resolve().parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))
PIPELINE_DIR = BASE_DIR.parents[1] / "workspaces" / "claims_insights" / "pipeline"
if str(PIPELINE_DIR) not in sys.path:
    sys.path.insert(0, str(PIPELINE_DIR))

from test_kich_ban_json_case import (  # noqa: E402
    as_text,
    collect_sign_sets,
    protocol_keyword_probe,
    run_service_mapping,
    run_sign_tests,
)
from build_reasoning_experiences_from_batch import (  # noqa: E402
    DEFAULT_MEMORY_PATH,
    build_reasoning_experience_artifacts,
)
from reasoning_experience_memory import ReasoningExperienceMemory  # noqa: E402


GENERIC_DISEASE_TOKENS = {
    "benh",
    "hoi",
    "chung",
    "da",
    "ly",
    "nguy",
    "kich",
    "cap",
    "tinh",
    "do",
    "test",
    "onto",
    "medical",
}


EXPERIENCE_MEMORY_PATH = DEFAULT_MEMORY_PATH


def now_iso() -> str:
    return datetime.now().astimezone().isoformat()


def load_cases(path: Path) -> list[dict[str, Any]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if isinstance(payload, dict):
        return [payload]
    return []


def ascii_fold(value: Any) -> str:
    text = as_text(value).lower().replace("đ", "d").replace("Đ", "d")
    normalized = unicodedata.normalize("NFD", text)
    stripped = "".join(ch for ch in normalized if unicodedata.category(ch) != "Mn")
    stripped = re.sub(r"[^a-z0-9 ]+", " ", stripped)
    return re.sub(r"\s+", " ", stripped).strip()


def important_disease_tokens(value: Any) -> list[str]:
    tokens: list[str] = []
    for token in ascii_fold(value).split():
        if len(token) <= 2 or token in GENERIC_DISEASE_TOKENS:
            continue
        tokens.append(token)
    return tokens


def disease_hit_flags(report: dict[str, Any]) -> dict[str, Any]:
    expected_disease = as_text((report.get("case_level") or {}).get("main_disease_name_vi"))
    expected_tokens = important_disease_tokens(expected_disease)
    suspected = ((report.get("sign_test") or {}).get("chief_plus_findings") or {}).get("suspected_diseases") or []

    def score_item(item: dict[str, Any]) -> float:
        predicted_tokens = set(important_disease_tokens(item.get("disease_name")))
        if not expected_tokens or not predicted_tokens:
            return 0.0
        overlap = set(expected_tokens) & predicted_tokens
        return len(overlap) / max(len(set(expected_tokens)), 1)

    scored = [score_item(item) for item in suspected]
    top1_score = scored[0] if scored else 0.0
    top3_score = max(scored[:3]) if scored else 0.0
    return {
        "expected_disease": expected_disease,
        "expected_tokens": expected_tokens,
        "top1_score": round(top1_score, 4),
        "top3_score": round(top3_score, 4),
        "top1_hit": top1_score >= 0.6,
        "top3_hit": top3_score >= 0.6,
    }


def summarize_service_rows(rows: list[dict[str, Any]]) -> dict[str, Any]:
    base_counts = {"HIGH": 0, "MEDIUM": 0, "LOW": 0, "REVIEW": 0}
    hybrid_counts = {"HIGH": 0, "MEDIUM": 0, "LOW": 0, "REVIEW": 0}
    for row in rows:
        base_conf = as_text((row.get("base_top") or {}).get("confidence")) or "REVIEW"
        hybrid_conf = as_text((row.get("hybrid_top") or {}).get("confidence")) or "REVIEW"
        base_counts[base_conf] = base_counts.get(base_conf, 0) + 1
        hybrid_counts[hybrid_conf] = hybrid_counts.get(hybrid_conf, 0) + 1
    return {
        "base_confidence_mix": base_counts,
        "hybrid_confidence_mix": hybrid_counts,
    }


def build_memory_advice(report: dict[str, Any]) -> dict[str, Any]:
    memory = ReasoningExperienceMemory(EXPERIENCE_MEMORY_PATH)
    case_level = report.get("case_level") or {}
    sign_sets = report.get("sign_sets") or {}
    sign_terms = (sign_sets.get("structured_signs") or []) + (sign_sets.get("chief_plus_findings") or [])
    service_rows = ((report.get("service_mapping_test") or {}).get("rows") or [])
    service_terms = [as_text(row.get("service_name_raw")) for row in service_rows if as_text(row.get("service_name_raw"))]
    matches = memory.query(
        disease_name=as_text(case_level.get("main_disease_name_vi")),
        specialty=as_text(case_level.get("specialty")),
        sign_terms=sign_terms,
        service_terms=service_terms,
        top_k=5,
    )
    return {
        "memory_path": str(EXPERIENCE_MEMORY_PATH),
        "match_count": len(matches),
        "matches": matches,
        "recommendations": memory.summarize_matches(matches, limit=3),
    }


def build_case_report(case_payload: dict[str, Any], input_path: Path, index: int) -> dict[str, Any]:
    case_level = ((case_payload.get("du_lieu_labeling_mau") or {}).get("case_level") or {})
    service_rows = ((case_payload.get("du_lieu_labeling_mau") or {}).get("service_lines") or [])
    sign_sets = collect_sign_sets(case_payload)
    sign_tests = run_sign_tests(sign_sets)
    service_mapping = run_service_mapping(service_rows)

    sign_engine_codes = {
        as_text(item.get("service_code"))
        for item in (sign_tests.get("chief_plus_findings") or {}).get("recommended_services", [])
        if as_text(item.get("service_code"))
    }
    mapped_codes = set(service_mapping["summary"]["base_mapped_codes"])
    overlap_codes = sorted(sign_engine_codes & mapped_codes)

    report = {
        "generated_at": now_iso(),
        "input_json": str(input_path),
        "case_index": index,
        "case_level": case_level,
        "story_title": as_text(((case_payload.get("cau_chuyen_y_khoa") or {}).get("tieu_de"))),
        "sign_sets": sign_sets,
        "sign_test": sign_tests,
        "service_mapping_test": service_mapping,
        "service_overlap_with_sign_engine": {
            "recommended_codes": sorted(sign_engine_codes),
            "mapped_codes": sorted(mapped_codes),
            "overlap_codes": overlap_codes,
        },
        "protocol_probe": protocol_keyword_probe(case_payload, service_rows),
    }
    report["experience_memory"] = build_memory_advice(report)
    return report


def write_case_outputs(report: dict[str, Any], output_dir: Path) -> dict[str, str]:
    request_id = as_text((report.get("case_level") or {}).get("request_id")) or f"case_{report['case_index']:03d}"
    safe_id = re.sub(r"[^A-Za-z0-9_-]+", "_", request_id)
    json_path = output_dir / f"{safe_id}_test_report.json"
    md_path = output_dir / f"{safe_id}_test_report.md"
    json_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    mapping_summary = (report.get("service_mapping_test") or {}).get("summary") or {}
    rows = (report.get("service_mapping_test") or {}).get("rows") or []
    disease_flags = disease_hit_flags(report)
    overlap_codes = ((report.get("service_overlap_with_sign_engine") or {}).get("overlap_codes") or [])
    suspected = ((report.get("sign_test") or {}).get("chief_plus_findings") or {}).get("suspected_diseases") or []

    lines = [
        f"# {safe_id} Test Report",
        "",
        f"- Input: `{report['input_json']}`",
        f"- Story title: `{report['story_title']}`",
        f"- Expected disease: `{disease_flags['expected_disease']}`",
        f"- Base high-confidence: `{mapping_summary.get('base_high_confidence_count', 0)}/{mapping_summary.get('service_count', 0)}`",
        f"- Hybrid high-confidence: `{mapping_summary.get('hybrid_high_confidence_count', 0)}/{mapping_summary.get('service_count', 0)}`",
        f"- Base family-resolved: `{mapping_summary.get('base_family_resolved_count', 0)}/{mapping_summary.get('service_count', 0)}`",
        f"- Hybrid family-resolved: `{mapping_summary.get('hybrid_family_resolved_count', 0)}/{mapping_summary.get('service_count', 0)}`",
        f"- Top-1 disease token hit: `{disease_flags['top1_hit']}`",
        f"- Top-3 disease token hit: `{disease_flags['top3_hit']}`",
        f"- Sign/service overlap codes: `{', '.join(overlap_codes) if overlap_codes else 'none'}`",
        "",
        "## Top Suspected Diseases",
        "",
    ]
    for item in suspected[:5]:
        lines.append(f"- `{item.get('icd10')}` {item.get('disease_name')} | score `{item.get('score')}`")
    lines.extend(["", "## Service Mapping", ""])
    for row in rows:
        base_top = row.get("base_top") or {}
        lines.append(
            f"- `{row.get('service_name_raw')}` -> base `{base_top.get('service_code', '')}` `{base_top.get('canonical_name', '')}` `{base_top.get('confidence', '')}`"
        )
    recommendations = ((report.get("experience_memory") or {}).get("recommendations") or [])
    lines.extend(["", "## Experience Memory", ""])
    if recommendations:
        for item in recommendations:
            lines.append(f"- {item}")
    else:
        lines.append("- No prior experience matched this case.")
    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return {"json": str(json_path), "md": str(md_path)}


def build_case_summary(report: dict[str, Any]) -> dict[str, Any]:
    case_level = report.get("case_level") or {}
    mapping_summary = (report.get("service_mapping_test") or {}).get("summary") or {}
    rows = (report.get("service_mapping_test") or {}).get("rows") or []
    disease_flags = disease_hit_flags(report)
    service_mix = summarize_service_rows(rows)
    suspected = ((report.get("sign_test") or {}).get("chief_plus_findings") or {}).get("suspected_diseases") or []

    return {
        "request_id": as_text(case_level.get("request_id")),
        "testcase_title": as_text(case_level.get("testcase_title")),
        "expected_disease": disease_flags["expected_disease"],
        "specialty": as_text(case_level.get("specialty")),
        "service_count": int(mapping_summary.get("service_count") or 0),
        "base_high_confidence_count": int(mapping_summary.get("base_high_confidence_count") or 0),
        "hybrid_high_confidence_count": int(mapping_summary.get("hybrid_high_confidence_count") or 0),
        "base_family_resolved_count": int(mapping_summary.get("base_family_resolved_count") or 0),
        "hybrid_family_resolved_count": int(mapping_summary.get("hybrid_family_resolved_count") or 0),
        "top1_disease": as_text((suspected[0] if suspected else {}).get("disease_name")),
        "top3_diseases": [as_text(item.get("disease_name")) for item in suspected[:3]],
        "disease_top1_hit": disease_flags["top1_hit"],
        "disease_top3_hit": disease_flags["top3_hit"],
        "sign_service_overlap_codes": (report.get("service_overlap_with_sign_engine") or {}).get("overlap_codes") or [],
        "base_confidence_mix": service_mix["base_confidence_mix"],
        "hybrid_confidence_mix": service_mix["hybrid_confidence_mix"],
        "experience_match_count": int(((report.get("experience_memory") or {}).get("match_count") or 0)),
    }


def build_batch_for_file(input_path: Path, output_dir: Path) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    cases = load_cases(input_path)
    case_summaries: list[dict[str, Any]] = []
    report_paths: list[dict[str, str]] = []

    for index, case_payload in enumerate(cases, start=1):
        report = build_case_report(case_payload, input_path, index)
        paths = write_case_outputs(report, output_dir)
        summary = build_case_summary(report)
        summary["report_json"] = paths["json"]
        summary["report_md"] = paths["md"]
        case_summaries.append(summary)
        report_paths.append(paths)

    aggregate = {
        "evaluated_case_count": len(case_summaries),
        "total_service_lines": sum(item["service_count"] for item in case_summaries),
        "base_high_confidence_total": sum(item["base_high_confidence_count"] for item in case_summaries),
        "hybrid_high_confidence_total": sum(item["hybrid_high_confidence_count"] for item in case_summaries),
        "base_family_resolved_total": sum(item["base_family_resolved_count"] for item in case_summaries),
        "hybrid_family_resolved_total": sum(item["hybrid_family_resolved_count"] for item in case_summaries),
        "disease_top1_hit_count": sum(1 for item in case_summaries if item["disease_top1_hit"]),
        "disease_top3_hit_count": sum(1 for item in case_summaries if item["disease_top3_hit"]),
        "sign_service_overlap_case_count": sum(1 for item in case_summaries if item["sign_service_overlap_codes"]),
    }

    payload = {
        "generated_at": now_iso(),
        "input_file": str(input_path),
        "aggregate": aggregate,
        "cases": case_summaries,
    }

    payload["experience_artifacts"] = build_reasoning_experience_artifacts(payload, output_dir, EXPERIENCE_MEMORY_PATH)

    summary_json = output_dir / "batch_summary.json"
    summary_md = output_dir / "batch_summary.md"
    summary_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    lines = [
        f"# Batch Summary: {input_path.name}",
        "",
        f"- Evaluated cases: `{aggregate['evaluated_case_count']}`",
        f"- Total service lines: `{aggregate['total_service_lines']}`",
        f"- Base high-confidence mappings: `{aggregate['base_high_confidence_total']}`",
        f"- Hybrid high-confidence mappings: `{aggregate['hybrid_high_confidence_total']}`",
        f"- Base family-resolved mappings: `{aggregate['base_family_resolved_total']}`",
        f"- Hybrid family-resolved mappings: `{aggregate['hybrid_family_resolved_total']}`",
        f"- Disease top-1 hit count: `{aggregate['disease_top1_hit_count']}`",
        f"- Disease top-3 hit count: `{aggregate['disease_top3_hit_count']}`",
        f"- Cases with sign/service overlap: `{aggregate['sign_service_overlap_case_count']}`",
        f"- Experience memory appended: `{payload['experience_artifacts']['appended']}`",
        f"- Experience memory total: `{payload['experience_artifacts']['memory_total']}`",
        "",
        "## Case Table",
        "",
    ]
    for item in case_summaries:
        lines.append(
            f"- `{item['request_id']}` | `{item['expected_disease']}` | base `{item['base_high_confidence_count']}/{item['service_count']}` | top1 `{item['top1_disease']}` | top3_hit `{item['disease_top3_hit']}` | overlap `{', '.join(item['sign_service_overlap_codes']) if item['sign_service_overlap_codes'] else 'none'}` | memory `{item['experience_match_count']}`"
        )
    summary_md.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return {"summary_json": str(summary_json), "summary_md": str(summary_md)}


def main() -> None:
    if len(sys.argv) < 2:
        raise SystemExit("Usage: python run_testcase_array_batch.py <input_json> [output_dir]")

    input_path = Path(sys.argv[1]).resolve()
    if not input_path.exists():
        raise SystemExit(f"Input file not found: {input_path}")

    if len(sys.argv) >= 3:
        output_dir = Path(sys.argv[2]).resolve()
    else:
        output_dir = input_path.parent / f"{input_path.stem}_batch"

    outputs = build_batch_for_file(input_path, output_dir)
    print(f"Batch summary JSON: {outputs['summary_json']}")
    print(f"Batch summary MD: {outputs['summary_md']}")


if __name__ == "__main__":
    main()
