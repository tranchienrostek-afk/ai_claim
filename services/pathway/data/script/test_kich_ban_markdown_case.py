from __future__ import annotations

import json
import re
import sys
from pathlib import Path
from typing import Any


BASE_DIR = Path(__file__).resolve().parent
PROJECT_DIR = BASE_DIR.parents[2]
NOTEBOOKLM_DIR = BASE_DIR.parents[1]
STANDARDIZE_DIR = NOTEBOOKLM_DIR / "workspaces" / "claims_insights" / "02_standardize"
PIPELINE_DIR = NOTEBOOKLM_DIR / "workspaces" / "claims_insights" / "pipeline"
PHAC_DO_DIR = NOTEBOOKLM_DIR / "workspaces" / "claims_insights" / "05_reference" / "phac_do"
SIGNS_DIR = NOTEBOOKLM_DIR / "workspaces" / "claims_insights" / "05_reference" / "signs"
for candidate in (STANDARDIZE_DIR, PIPELINE_DIR):
    if str(candidate) not in sys.path:
        sys.path.insert(0, str(candidate))

from service_text_mapper import ServiceTextMapper  # noqa: E402
from service_text_mapper_hybrid import HybridSemanticServiceTextMapper  # noqa: E402
from sign_to_service_engine import SignToServiceEngine  # noqa: E402


INPUT_MD = BASE_DIR / "kich_ban_01.md"
OUTPUT_JSON = BASE_DIR / "kich_ban_01_test_report.json"
OUTPUT_MD = BASE_DIR / "kich_ban_01_test_report.md"
ACTIVE_SIGN_CATALOG = SIGNS_DIR / "sign_concept_catalog_active_v1.json"
PROTOCOL_BUNDLE_JSON = PHAC_DO_DIR / "tmh_protocol_neo4j_bundle.json"

SIGN_FILLERS = [
    "thỉnh thoảng",
    "thi thoảng",
    "dai dẳng",
    "bên trái",
    "bên phải",
    "trái",
    "phải",
    "ra chút",
]


def as_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    return str(value).strip()


def extract_csv_blocks(text: str) -> list[str]:
    return [block.strip() for block in re.findall(r"```csv\n(.*?)```", text, re.S)]


def split_csv_line(line: str) -> list[str]:
    return [cell.strip() for cell in line.rstrip("\n").split(",")]


def parse_block_with_single_merge(
    block_text: str,
    merge_field: str | None = None,
) -> tuple[list[dict[str, str]], dict[str, Any]]:
    lines = [line for line in block_text.splitlines() if line.strip()]
    header = split_csv_line(lines[0])
    rows: list[dict[str, str]] = []
    issues: list[dict[str, Any]] = []
    merge_index = header.index(merge_field) if merge_field and merge_field in header else -1

    for row_no, raw_line in enumerate(lines[1:], start=1):
        cells = split_csv_line(raw_line)
        original_cell_count = len(cells)
        repaired = False

        if merge_index >= 0 and len(cells) > len(header):
            right_count = len(header) - merge_index - 1
            left = cells[:merge_index]
            middle = cells[merge_index : len(cells) - right_count]
            right = cells[len(cells) - right_count :] if right_count else []
            cells = left + [", ".join(middle).strip()] + right
            repaired = True

        if len(cells) != len(header):
            issues.append(
                {
                    "row_no": row_no,
                    "expected_columns": len(header),
                    "actual_columns": len(cells),
                    "repaired": repaired,
                    "raw_line": raw_line,
                }
            )
            continue

        row = {header[idx]: cells[idx] for idx in range(len(header))}
        row["__row_no__"] = str(row_no)
        rows.append(row)
        if repaired or original_cell_count != len(header):
            issues.append(
                {
                    "row_no": row_no,
                    "expected_columns": len(header),
                    "actual_columns": original_cell_count,
                    "repaired": repaired,
                    "merge_field": merge_field,
                }
            )

    return rows, {"header": header, "issues": issues}


def extract_bold_segments(text: str) -> list[str]:
    return [as_text(item) for item in re.findall(r"\*\*(.*?)\*\*", text, re.S)]


def simplify_sign_text(text: str) -> str:
    simplified = as_text(text)
    for filler in SIGN_FILLERS:
        simplified = re.sub(rf"\b{re.escape(filler)}\b", " ", simplified, flags=re.IGNORECASE)
    simplified = re.sub(r"\s+", " ", simplified).strip(" ,.;")
    return simplified


def extract_story_signs(text: str) -> dict[str, Any]:
    bolds = extract_bold_segments(text)
    complaint = bolds[1] if len(bolds) > 1 else ""
    diagnosis = bolds[10] if len(bolds) > 10 else ""
    raw_parts = [as_text(part) for part in re.split(r",|\svà\s", complaint) if as_text(part)]
    simplified_parts = []
    for part in raw_parts:
        simplified = simplify_sign_text(part)
        if simplified:
            simplified_parts.append(simplified)
    return {
        "chief_complaint_from_story": complaint,
        "diagnosis_from_story": diagnosis,
        "raw_sign_parts": raw_parts,
        "simplified_sign_parts": simplified_parts,
    }


def run_sign_test(signs: list[str]) -> dict[str, Any]:
    engine = SignToServiceEngine(sign_catalog_path=ACTIVE_SIGN_CATALOG)
    inference = engine.infer_from_signs(signs, top_diseases=8, top_services=12)
    return {
        "input_signs": signs,
        "matched_signs": inference.get("matched_signs") or [],
        "suspected_diseases": inference.get("suspected_diseases") or [],
        "recommended_services": inference.get("recommended_services") or [],
    }


def run_service_mapping(service_rows: list[dict[str, str]]) -> list[dict[str, Any]]:
    base_mapper = ServiceTextMapper()
    hybrid_mapper = HybridSemanticServiceTextMapper()
    results: list[dict[str, Any]] = []
    for row in service_rows:
        name = as_text(row.get("service_name_raw"))
        base = base_mapper.score_text(name, top_k=3)
        hybrid = hybrid_mapper.score_text(name, top_k=3)
        results.append(
            {
                "line_no": as_text(row.get("line_no")),
                "service_name_raw": name,
                "expected_label": as_text(row.get("final_label")),
                "medical_necessity_view": as_text(row.get("medical_necessity_view")),
                "base_top": (base.get("suggestions") or [{}])[0] if base.get("suggestions") else {},
                "hybrid_top": (hybrid.get("suggestions") or [{}])[0] if hybrid.get("suggestions") else {},
                "base_resolution": base.get("mapping_resolution"),
                "hybrid_resolution": hybrid.get("mapping_resolution"),
                "base_family_hint": {
                    "family_id": base.get("family_hint_id"),
                    "family_status": base.get("family_hint_status"),
                },
                "hybrid_family_hint": {
                    "family_id": hybrid.get("family_hint_id"),
                    "family_status": hybrid.get("family_hint_status"),
                },
                "base_family_only_hint": base.get("family_only_hint"),
                "hybrid_family_only_hint": hybrid.get("family_only_hint"),
                "base_alternatives": base.get("suggestions") or [],
                "hybrid_alternatives": hybrid.get("suggestions") or [],
            }
        )
    return results


def summarize_mapping_coverage(service_results: list[dict[str, Any]]) -> dict[str, Any]:
    base_high = 0
    hybrid_high = 0
    base_family_resolved = 0
    hybrid_family_resolved = 0
    base_codes: list[str] = []
    hybrid_codes: list[str] = []
    for row in service_results:
        base_top = row.get("base_top") or {}
        hybrid_top = row.get("hybrid_top") or {}
        if as_text(base_top.get("service_code")):
            base_codes.append(as_text(base_top.get("service_code")))
        if as_text(hybrid_top.get("service_code")):
            hybrid_codes.append(as_text(hybrid_top.get("service_code")))
        if as_text(base_top.get("confidence")) == "HIGH":
            base_high += 1
        if as_text(hybrid_top.get("confidence")) == "HIGH":
            hybrid_high += 1
        if as_text(row.get("base_resolution")) in {"coded", "family_only"}:
            base_family_resolved += 1
        if as_text(row.get("hybrid_resolution")) in {"coded", "family_only"}:
            hybrid_family_resolved += 1
    return {
        "service_count": len(service_results),
        "base_high_confidence_count": base_high,
        "hybrid_high_confidence_count": hybrid_high,
        "base_family_resolved_count": base_family_resolved,
        "hybrid_family_resolved_count": hybrid_family_resolved,
        "base_mapped_codes": base_codes,
        "hybrid_mapped_codes": hybrid_codes,
    }


def protocol_keyword_probe(diagnosis_text: str, service_rows: list[dict[str, str]]) -> dict[str, Any]:
    bundle_text = PROTOCOL_BUNDLE_JSON.read_text(encoding="utf-8").lower() if PROTOCOL_BUNDLE_JSON.exists() else ""
    diagnosis_hit = diagnosis_text.lower() in bundle_text if diagnosis_text else False
    service_hits = []
    for row in service_rows:
        service_name = as_text(row.get("service_name_raw"))
        service_hits.append(
            {
                "service_name_raw": service_name,
                "protocol_text_hit": service_name.lower() in bundle_text if service_name else False,
            }
        )
    return {
        "diagnosis_text": diagnosis_text,
        "diagnosis_protocol_hit": diagnosis_hit,
        "service_protocol_hits": service_hits,
    }


def build_report() -> dict[str, Any]:
    text = INPUT_MD.read_text(encoding="utf-8")
    csv_blocks = extract_csv_blocks(text)
    case_rows, case_parse = parse_block_with_single_merge(csv_blocks[0], merge_field="why_this_case_was_selected")
    service_rows, service_parse = parse_block_with_single_merge(csv_blocks[1], merge_field=None)
    story = extract_story_signs(text)
    case_row = case_rows[0] if case_rows else {}

    narrative_signs = story["simplified_sign_parts"]
    finding_signs = [as_text(part) for part in as_text(case_row.get("initial_signs_pipe")).split("|") if as_text(part)]
    complaint_only = run_sign_test(narrative_signs)
    complaint_plus_findings = run_sign_test(narrative_signs + finding_signs)
    service_results = run_service_mapping(service_rows)
    mapping_summary = summarize_mapping_coverage(service_results)

    recommended_codes = {as_text(item.get("service_code")) for item in complaint_plus_findings["recommended_services"] if as_text(item.get("service_code"))}
    mapped_codes = set(mapping_summary["base_mapped_codes"])
    overlap_codes = sorted(recommended_codes & mapped_codes)

    report = {
        "generated_at": datetime_now(),
        "input_markdown": str(INPUT_MD),
        "story_extraction": story,
        "case_csv_parse": {
            "row_count": len(case_rows),
            "issues": case_parse["issues"],
            "repaired_case_row": case_row,
        },
        "service_csv_parse": {
            "row_count": len(service_rows),
            "issues": service_parse["issues"],
            "service_rows": service_rows,
        },
        "sign_test": {
            "complaint_only": complaint_only,
            "complaint_plus_findings": complaint_plus_findings,
        },
        "service_mapping_test": {
            "summary": mapping_summary,
            "rows": service_results,
        },
        "service_overlap_with_sign_engine": {
            "recommended_codes": sorted(recommended_codes),
            "mapped_codes": sorted(mapped_codes),
            "overlap_codes": overlap_codes,
        },
        "protocol_probe": protocol_keyword_probe(
            as_text(case_row.get("main_disease_name_vi")) or as_text(story.get("diagnosis_from_story")),
            service_rows,
        ),
    }
    return report


def datetime_now() -> str:
    from datetime import datetime

    return datetime.now().astimezone().isoformat()


def main() -> None:
    report = build_report()
    OUTPUT_JSON.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    complaint_only = report["sign_test"]["complaint_only"]
    complaint_plus = report["sign_test"]["complaint_plus_findings"]
    mapping_summary = report["service_mapping_test"]["summary"]
    case_issues = report["case_csv_parse"]["issues"]
    overlap = report["service_overlap_with_sign_engine"]["overlap_codes"]

    lines = [
        "# Kich Ban 01 Test Report",
        "",
        f"- Input: `{INPUT_MD}`",
        f"- Case CSV rows: `{report['case_csv_parse']['row_count']}`",
        f"- Service CSV rows: `{report['service_csv_parse']['row_count']}`",
        f"- Case CSV issues: `{len(case_issues)}`",
        f"- Base service mapper high-confidence: `{mapping_summary['base_high_confidence_count']}/{mapping_summary['service_count']}`",
        f"- Hybrid service mapper high-confidence: `{mapping_summary['hybrid_high_confidence_count']}/{mapping_summary['service_count']}`",
        f"- Base service mapper family-resolved: `{mapping_summary['base_family_resolved_count']}/{mapping_summary['service_count']}`",
        f"- Hybrid service mapper family-resolved: `{mapping_summary['hybrid_family_resolved_count']}/{mapping_summary['service_count']}`",
        f"- Sign-engine recommended/mapped overlap codes: `{', '.join(overlap) if overlap else 'none'}`",
        "",
        "## Story Signs",
        "",
    ]
    for sign in report["story_extraction"]["simplified_sign_parts"]:
        lines.append(f"- `{sign}`")
    lines.extend(
        [
            "",
            "## Top Suspected Diseases",
            "",
        ]
    )
    for item in complaint_plus.get("suspected_diseases", [])[:5]:
        lines.append(f"- `{item.get('icd10')}` {item.get('disease_name')} | score `{item.get('score')}`")
    lines.extend(
        [
            "",
            "## Service Mapping",
            "",
        ]
    )
    for row in report["service_mapping_test"]["rows"]:
        base_top = row.get("base_top") or {}
        base_family = row.get("base_family_hint") or {}
        lines.append(
            f"- `{row['service_name_raw']}` -> base `{base_top.get('service_code', '')}` `{base_top.get('canonical_name', '')}` `{base_top.get('confidence', '')}` | family `{base_family.get('family_id', '')}` `{row.get('base_resolution', '')}`"
        )
    OUTPUT_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"Report JSON: {OUTPUT_JSON}")
    print(f"Report MD: {OUTPUT_MD}")


if __name__ == "__main__":
    main()
