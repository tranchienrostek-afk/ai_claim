from __future__ import annotations

import json
import sys
from datetime import datetime
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
from sign_to_service_engine import SignToServiceEngine, as_text  # noqa: E402


INPUT_JSON = BASE_DIR / "kich_ban_02.json"
OUTPUT_JSON = BASE_DIR / "kich_ban_02_test_report.json"
OUTPUT_MD = BASE_DIR / "kich_ban_02_test_report.md"
ACTIVE_SIGN_CATALOG = SIGNS_DIR / "sign_concept_catalog_active_v1.json"
PROTOCOL_BUNDLE_JSON = PHAC_DO_DIR / "tmh_protocol_neo4j_bundle.json"


def now_iso() -> str:
    return datetime.now().astimezone().isoformat()


def load_payload(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def split_pipe(value: Any) -> list[str]:
    return [item.strip() for item in as_text(value).split("|") if item.strip()]


def dedupe_keep_order(items: list[str]) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for item in items:
        key = as_text(item)
        if not key or key in seen:
            continue
        seen.add(key)
        ordered.append(key)
    return ordered


def collect_patient_context(payload: dict[str, Any]) -> dict[str, Any]:
    story = payload.get("cau_chuyen_y_khoa") or {}
    patient = (story.get("benh_nhan") or {})
    return {
        "gioi_tinh": as_text(patient.get("gioi_tinh")),
        "tuoi": patient.get("tuoi"),
    }


def collect_structured_signs(payload: dict[str, Any]) -> list[dict[str, Any]]:
    story = payload.get("cau_chuyen_y_khoa") or {}
    history = (story.get("benh_su_va_trieu_chung") or {})
    extracted = ((history.get("ontology_extraction") or {}).get("sign_concepts") or [])
    structured: list[dict[str, Any]] = []
    for item in extracted:
        concept = as_text(item.get("concept"))
        modifiers = [as_text(modifier) for modifier in (item.get("modifiers") or []) if as_text(modifier)]
        if concept:
            structured.append({"concept": concept, "modifiers": modifiers})
    return structured


def collect_sign_sets(payload: dict[str, Any]) -> dict[str, list[str]]:
    story = payload.get("cau_chuyen_y_khoa") or {}
    history = (story.get("benh_su_va_trieu_chung") or {})
    case_level = ((payload.get("du_lieu_labeling_mau") or {}).get("case_level") or {})

    symptom_story = [as_text(item) for item in (history.get("trieu_chung_co_nang") or []) if as_text(item)]
    raw_sign_mentions = [as_text(item) for item in (history.get("raw_sign_mentions") or []) if as_text(item)]
    structured_signs = collect_structured_signs(payload)
    structured_texts: list[str] = []
    for item in structured_signs:
        if as_text(item.get("concept")):
            structured_texts.append(as_text(item["concept"]))
        structured_texts.extend([as_text(modifier) for modifier in (item.get("modifiers") or []) if as_text(modifier)])

    chief = as_text(case_level.get("chief_complaint"))
    initial_findings = split_pipe(case_level.get("initial_signs_pipe"))
    medical_history = [as_text(case_level.get("medical_history_pipe"))] if as_text(case_level.get("medical_history_pipe")) else []

    signs_for_engine = []
    if chief:
        signs_for_engine.append(chief)
    signs_for_engine.extend(raw_sign_mentions)
    signs_for_engine.extend(initial_findings)
    signs_for_engine.extend(structured_texts)
    signs_for_engine = dedupe_keep_order(signs_for_engine)

    return {
        "symptom_story": dedupe_keep_order(symptom_story + raw_sign_mentions),
        "chief_plus_findings": signs_for_engine,
        "structured_signs": dedupe_keep_order(structured_texts),
        "all_case_signs": dedupe_keep_order(signs_for_engine + medical_history),
    }


def run_sign_tests(
    sign_sets: dict[str, list[str]],
    structured_signs: list[dict[str, Any]] | None = None,
    patient_context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    engine = SignToServiceEngine(sign_catalog_path=ACTIVE_SIGN_CATALOG)
    results: dict[str, Any] = {}
    for name, signs in sign_sets.items():
        results[name] = engine.infer_from_signs(
            signs,
            top_diseases=8,
            top_services=12,
            structured_signs=structured_signs,
            patient_context=patient_context,
        )
    return results


def run_service_mapping(service_rows: list[dict[str, Any]]) -> dict[str, Any]:
    base_mapper = ServiceTextMapper()
    hybrid_mapper = HybridSemanticServiceTextMapper()

    rows: list[dict[str, Any]] = []
    base_high = 0
    hybrid_high = 0
    base_family_resolved = 0
    hybrid_family_resolved = 0
    base_codes: list[str] = []
    hybrid_codes: list[str] = []

    for row in service_rows:
        service_name = as_text(row.get("service_name_raw"))
        base = base_mapper.score_text(service_name, top_k=3)
        hybrid = hybrid_mapper.score_text(service_name, top_k=3)
        base_top = (base.get("suggestions") or [{}])[0] if base.get("suggestions") else {}
        hybrid_top = (hybrid.get("suggestions") or [{}])[0] if hybrid.get("suggestions") else {}
        if as_text(base_top.get("confidence")) == "HIGH":
            base_high += 1
        if as_text(hybrid_top.get("confidence")) == "HIGH":
            hybrid_high += 1
        if as_text(base.get("mapping_resolution")) in {"coded", "family_only"}:
            base_family_resolved += 1
        if as_text(hybrid.get("mapping_resolution")) in {"coded", "family_only"}:
            hybrid_family_resolved += 1
        if as_text(base_top.get("service_code")):
            base_codes.append(as_text(base_top.get("service_code")))
        if as_text(hybrid_top.get("service_code")):
            hybrid_codes.append(as_text(hybrid_top.get("service_code")))
        rows.append(
            {
                "line_no": row.get("line_no"),
                "service_name_raw": service_name,
                "expected_label": as_text(row.get("final_label")),
                "reason_layer": as_text(row.get("reason_layer")),
                "base_top": base_top,
                "hybrid_top": hybrid_top,
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

    return {
        "summary": {
            "service_count": len(service_rows),
            "base_high_confidence_count": base_high,
            "hybrid_high_confidence_count": hybrid_high,
            "base_family_resolved_count": base_family_resolved,
            "hybrid_family_resolved_count": hybrid_family_resolved,
            "base_mapped_codes": base_codes,
            "hybrid_mapped_codes": hybrid_codes,
        },
        "rows": rows,
    }


def protocol_keyword_probe(payload: dict[str, Any], service_rows: list[dict[str, Any]]) -> dict[str, Any]:
    bundle_text = PROTOCOL_BUNDLE_JSON.read_text(encoding="utf-8").lower() if PROTOCOL_BUNDLE_JSON.exists() else ""
    disease_name = as_text((((payload.get("du_lieu_labeling_mau") or {}).get("case_level") or {}).get("main_disease_name_vi")))
    story_title = as_text((((payload.get("cau_chuyen_y_khoa") or {}).get("tieu_de"))))
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
        "disease_name": disease_name,
        "disease_protocol_hit": disease_name.lower() in bundle_text if disease_name else False,
        "story_title": story_title,
        "story_title_protocol_hit": story_title.lower() in bundle_text if story_title else False,
        "service_protocol_hits": service_hits,
    }


def build_report(input_path: Path = INPUT_JSON) -> dict[str, Any]:
    payload = load_payload(input_path)
    case_level = ((payload.get("du_lieu_labeling_mau") or {}).get("case_level") or {})
    service_rows = ((payload.get("du_lieu_labeling_mau") or {}).get("service_lines") or [])
    sign_sets = collect_sign_sets(payload)
    structured_signs = collect_structured_signs(payload)
    patient_context = collect_patient_context(payload)
    sign_tests = run_sign_tests(sign_sets, structured_signs=structured_signs, patient_context=patient_context)
    service_mapping = run_service_mapping(service_rows)

    sign_engine_codes = {
        as_text(item.get("service_code"))
        for item in (sign_tests.get("chief_plus_findings") or {}).get("recommended_services", [])
        if as_text(item.get("service_code"))
    }
    mapped_codes = set(service_mapping["summary"]["base_mapped_codes"])
    overlap_codes = sorted(sign_engine_codes & mapped_codes)

    return {
        "generated_at": now_iso(),
        "input_json": str(input_path),
        "case_level": case_level,
        "patient_context": patient_context,
        "story_title": as_text(((payload.get("cau_chuyen_y_khoa") or {}).get("tieu_de"))),
        "structured_signs": structured_signs,
        "sign_sets": sign_sets,
        "sign_test": sign_tests,
        "service_mapping_test": service_mapping,
        "service_overlap_with_sign_engine": {
            "recommended_codes": sorted(sign_engine_codes),
            "mapped_codes": sorted(mapped_codes),
            "overlap_codes": overlap_codes,
        },
        "protocol_probe": protocol_keyword_probe(payload, service_rows),
    }


def write_report(report: dict[str, Any]) -> None:
    OUTPUT_JSON.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    sign_test = report["sign_test"]["chief_plus_findings"]
    mapping_summary = report["service_mapping_test"]["summary"]
    overlap_codes = report["service_overlap_with_sign_engine"]["overlap_codes"]

    lines = [
        "# Kich Ban 02 Test Report",
        "",
        f"- Input: `{report['input_json']}`",
        f"- Story title: `{report['story_title']}`",
        f"- Service rows: `{mapping_summary['service_count']}`",
        f"- Base service mapper high-confidence: `{mapping_summary['base_high_confidence_count']}/{mapping_summary['service_count']}`",
        f"- Hybrid service mapper high-confidence: `{mapping_summary['hybrid_high_confidence_count']}/{mapping_summary['service_count']}`",
        f"- Base service mapper family-resolved: `{mapping_summary['base_family_resolved_count']}/{mapping_summary['service_count']}`",
        f"- Hybrid service mapper family-resolved: `{mapping_summary['hybrid_family_resolved_count']}/{mapping_summary['service_count']}`",
        f"- Sign-engine recommended/mapped overlap codes: `{', '.join(overlap_codes) if overlap_codes else 'none'}`",
        "",
        "## Sign Sets",
        "",
    ]
    for key, value in report["sign_sets"].items():
        lines.append(f"- `{key}`: `{len(value)}` items")
    lines.extend(["", "## Top Suspected Diseases", ""])
    for item in sign_test.get("suspected_diseases", [])[:5]:
        lines.append(f"- `{item.get('icd10')}` {item.get('disease_name')} | score `{item.get('score')}`")
    lines.extend(["", "## Service Mapping", ""])
    for row in report["service_mapping_test"]["rows"]:
        base_top = row.get("base_top") or {}
        base_family = row.get("base_family_hint") or {}
        lines.append(
            f"- `{row['service_name_raw']}` -> base `{base_top.get('service_code', '')}` `{base_top.get('canonical_name', '')}` `{base_top.get('confidence', '')}` | family `{base_family.get('family_id', '')}` `{row.get('base_resolution', '')}`"
        )
    OUTPUT_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    report = build_report()
    write_report(report)
    print(f"Report JSON: {OUTPUT_JSON}")
    print(f"Report MD: {OUTPUT_MD}")


if __name__ == "__main__":
    main()
