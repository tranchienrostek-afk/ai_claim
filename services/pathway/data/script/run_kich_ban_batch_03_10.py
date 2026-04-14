from __future__ import annotations

import json
import re
import sys
import unicodedata
from pathlib import Path
from typing import Any


BASE_DIR = Path(__file__).resolve().parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from test_kich_ban_json_case import build_report  # noqa: E402


OUTPUT_DIR = BASE_DIR / "kich_ban_batch_03_10"
OUTPUT_SUMMARY_JSON = OUTPUT_DIR / "batch_summary.json"
OUTPUT_SUMMARY_MD = OUTPUT_DIR / "batch_summary.md"
FILE_RANGE = range(3, 11)

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
    "multipletests",
}


def as_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    return str(value).strip()


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
    base_review_rows: list[str] = []
    hybrid_review_rows: list[str] = []
    for row in rows:
        service_name = as_text(row.get("service_name_raw"))
        base_conf = as_text((row.get("base_top") or {}).get("confidence")) or "REVIEW"
        hybrid_conf = as_text((row.get("hybrid_top") or {}).get("confidence")) or "REVIEW"
        base_counts[base_conf] = base_counts.get(base_conf, 0) + 1
        hybrid_counts[hybrid_conf] = hybrid_counts.get(hybrid_conf, 0) + 1
        if base_conf == "REVIEW":
            base_review_rows.append(service_name)
        if hybrid_conf == "REVIEW":
            hybrid_review_rows.append(service_name)
    return {
        "base_confidence_mix": base_counts,
        "hybrid_confidence_mix": hybrid_counts,
        "base_review_rows": base_review_rows,
        "hybrid_review_rows": hybrid_review_rows,
    }


def write_case_outputs(report: dict[str, Any], source_path: Path) -> dict[str, str]:
    stem = source_path.stem
    json_path = OUTPUT_DIR / f"{stem}_test_report.json"
    md_path = OUTPUT_DIR / f"{stem}_test_report.md"
    json_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    mapping_summary = (report.get("service_mapping_test") or {}).get("summary") or {}
    rows = (report.get("service_mapping_test") or {}).get("rows") or []
    disease_flags = disease_hit_flags(report)
    overlap_codes = ((report.get("service_overlap_with_sign_engine") or {}).get("overlap_codes") or [])
    suspected = ((report.get("sign_test") or {}).get("chief_plus_findings") or {}).get("suspected_diseases") or []

    lines = [
        f"# {stem} Test Report",
        "",
        f"- Input: `{source_path}`",
        f"- Expected disease: `{disease_flags['expected_disease']}`",
        f"- Base high-confidence: `{mapping_summary.get('base_high_confidence_count', 0)}/{mapping_summary.get('service_count', 0)}`",
        f"- Hybrid high-confidence: `{mapping_summary.get('hybrid_high_confidence_count', 0)}/{mapping_summary.get('service_count', 0)}`",
        f"- Base family-resolved: `{mapping_summary.get('base_family_resolved_count', 0)}/{mapping_summary.get('service_count', 0)}`",
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
    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return {"json": str(json_path), "md": str(md_path)}


def build_case_summary(report: dict[str, Any], source_path: Path) -> dict[str, Any]:
    case_level = report.get("case_level") or {}
    mapping_summary = (report.get("service_mapping_test") or {}).get("summary") or {}
    rows = (report.get("service_mapping_test") or {}).get("rows") or []
    disease_flags = disease_hit_flags(report)
    service_mix = summarize_service_rows(rows)
    suspected = ((report.get("sign_test") or {}).get("chief_plus_findings") or {}).get("suspected_diseases") or []

    return {
        "file": source_path.name,
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
        "base_review_rows": service_mix["base_review_rows"],
        "hybrid_review_rows": service_mix["hybrid_review_rows"],
    }


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    case_summaries: list[dict[str, Any]] = []
    skipped_files: list[dict[str, Any]] = []

    for idx in FILE_RANGE:
        source_path = BASE_DIR / f"kich_ban_{idx:02d}.json"
        if not source_path.exists():
            skipped_files.append({"file": source_path.name, "reason": "missing"})
            continue
        if source_path.stat().st_size == 0:
            skipped_files.append({"file": source_path.name, "reason": "empty"})
            continue

        report = build_report(source_path)
        output_paths = write_case_outputs(report, source_path)
        summary = build_case_summary(report, source_path)
        summary["report_json"] = output_paths["json"]
        summary["report_md"] = output_paths["md"]
        case_summaries.append(summary)

    aggregate = {
        "target_file_count": len(list(FILE_RANGE)),
        "evaluated_file_count": len(case_summaries),
        "skipped_file_count": len(skipped_files),
        "skipped_files": skipped_files,
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
        "generated_at": __import__("datetime").datetime.now().astimezone().isoformat(),
        "aggregate": aggregate,
        "cases": case_summaries,
    }
    OUTPUT_SUMMARY_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    lines = [
        "# Kich Ban 03-10 Batch Summary",
        "",
        f"- Evaluated files: `{aggregate['evaluated_file_count']}/{aggregate['target_file_count']}`",
        f"- Skipped files: `{aggregate['skipped_file_count']}`",
        f"- Total service lines: `{aggregate['total_service_lines']}`",
        f"- Base high-confidence mappings: `{aggregate['base_high_confidence_total']}`",
        f"- Hybrid high-confidence mappings: `{aggregate['hybrid_high_confidence_total']}`",
        f"- Base family-resolved mappings: `{aggregate['base_family_resolved_total']}`",
        f"- Disease top-1 hit count: `{aggregate['disease_top1_hit_count']}`",
        f"- Disease top-3 hit count: `{aggregate['disease_top3_hit_count']}`",
        f"- Cases with sign/service overlap: `{aggregate['sign_service_overlap_case_count']}`",
        "",
        "## Case Table",
        "",
    ]
    for item in case_summaries:
        lines.append(
            f"- `{item['file']}` | `{item['request_id']}` | disease `{item['expected_disease']}` | base `{item['base_high_confidence_count']}/{item['service_count']}` | top1 `{item['top1_disease']}` | top3_hit `{item['disease_top3_hit']}` | overlap `{', '.join(item['sign_service_overlap_codes']) if item['sign_service_overlap_codes'] else 'none'}`"
        )
    if skipped_files:
        lines.extend(["", "## Skipped", ""])
        for item in skipped_files:
            lines.append(f"- `{item['file']}`: `{item['reason']}`")
    OUTPUT_SUMMARY_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"Batch summary JSON: {OUTPUT_SUMMARY_JSON}")
    print(f"Batch summary MD: {OUTPUT_SUMMARY_MD}")


if __name__ == "__main__":
    main()
