from __future__ import annotations

import argparse
import json
import re
import unicodedata
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any


PROJECT_DIR = Path(__file__).parent.parent
DEFAULT_INPUT_JSONL = PROJECT_DIR / "09_unified_story_testcase" / "step2_contract_clause" / "contract_clause_scored.jsonl"
DEFAULT_CASESET_JSONL = PROJECT_DIR / "09_unified_story_testcase" / "parallel_merged" / "data" / "unified_case_testset.jsonl"
DEFAULT_OUTPUT_JSONL = PROJECT_DIR / "09_unified_story_testcase" / "step3_admin_timing" / "admin_timing_scored.jsonl"
DEFAULT_SUMMARY_JSON = PROJECT_DIR / "09_unified_story_testcase" / "step3_admin_timing" / "admin_timing_summary.json"
DEFAULT_EXAMPLES_JSON = PROJECT_DIR / "09_unified_story_testcase" / "step3_admin_timing" / "admin_timing_examples.json"

PREOP_SERVICE_MARKERS = (
    "nhom mau",
    "abo",
    "rh",
    "pt",
    "aptt",
    "prothrombin",
    "thromboplastin",
    "hbsag",
    "hiv",
    "hcv",
    "hcg",
    "xquang nguc",
    "xq tim phoi",
    "dien tim",
    "ecg",
    "tong phan tich nuoc tieu",
    "nuoc tieu",
    "creatinin",
    "ure",
    "glucose",
    "cong thuc mau",
    "te bao mau",
)
PROCEDURE_CONTEXT_MARKERS = (
    "phau thuat",
    "phẫu thuật",
    "mo",
    "mổ",
    "sinh thiet",
    "sinh thiết",
    "biopsy",
    "cat",
    "cắt",
    "chan doan te bao",
    "chẩn đoán tế bào",
    "gay me",
    "gây mê",
    "thu thuat",
    "thủ thuật",
    "noi soi",
    "nội soi",
    "choc",
    "chọc",
)
TRACEABILITY_KEYS = ("so_hoso_boithuong",)


def as_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    return str(value).strip()


def strip_diacritics(text: str) -> str:
    normalized = unicodedata.normalize("NFD", as_text(text))
    return "".join(char for char in normalized if unicodedata.category(char) != "Mn")


def normalize_text(text: str) -> str:
    lowered = strip_diacritics(text).lower().replace("đ", "d").replace("Đ", "d")
    lowered = re.sub(r"\s+", " ", lowered)
    return lowered.strip()


def load_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with path.open(encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows


def write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")


def contains_any(text: str, markers: tuple[str, ...]) -> bool:
    haystack = normalize_text(text)
    return any(marker in haystack for marker in markers)


class AdminTimingStep3:
    def __init__(self, caseset_rows: list[dict[str, Any]]) -> None:
        self.case_by_message_hash = {as_text(row.get("message_hash_id")): row for row in caseset_rows}

    def _build_case_profile(self, case_row: dict[str, Any] | None) -> dict[str, Any]:
        if case_row is None:
            return {
                "available": False,
                "service_names": [],
                "lab_document_count": 0,
                "observation_count": 0,
                "source_claim_ids": [],
                "so_hoso_candidates": [],
                "admission_reason": "",
                "medical_history": "",
                "symptom": "",
                "outpatient_department": "",
                "facility_name": "",
                "preop_bundle_hits": [],
                "procedure_context_hits": [],
            }

        claim_info = case_row.get("claim_info_merged") or {}
        lab_context = case_row.get("lab_context") or {}
        services = case_row.get("service_lines") or []
        service_names = [
            as_text(service.get("service_name") or service.get("description"))
            for service in services
            if as_text(service.get("service_name") or service.get("description"))
        ]
        joined_service_text = " | ".join(service_names)
        diagnosis_text = as_text(claim_info.get("diagnosis"))
        preop_hits = [marker for marker in PREOP_SERVICE_MARKERS if marker in normalize_text(joined_service_text)]
        procedure_hits = []
        for marker in PROCEDURE_CONTEXT_MARKERS:
            if marker in normalize_text(joined_service_text) or marker in normalize_text(diagnosis_text):
                procedure_hits.append(marker)

        return {
            "available": True,
            "service_names": service_names,
            "lab_document_count": int(lab_context.get("document_count") or 0),
            "observation_count": int((lab_context.get("observation_summary") or {}).get("observation_count") or 0),
            "source_claim_ids": list(case_row.get("source_claim_ids") or []),
            "so_hoso_candidates": list(case_row.get("so_hoso_boithuong_candidates") or []),
            "admission_reason": as_text((case_row.get("admission_context") or {}).get("visit_reason_enriched") or claim_info.get("admission_reason")),
            "medical_history": as_text((case_row.get("admission_context") or {}).get("medical_history_enriched") or claim_info.get("medical_history")),
            "symptom": as_text(claim_info.get("symptom")),
            "outpatient_department": as_text(claim_info.get("outpatient_department")),
            "facility_name": as_text(claim_info.get("clinic_name")),
            "preop_bundle_hits": preop_hits,
            "procedure_context_hits": procedure_hits,
        }

    def assess_row(self, row: dict[str, Any]) -> dict[str, Any]:
        case_row = self.case_by_message_hash.get(as_text(row.get("message_hash_id")))
        case_profile = self._build_case_profile(case_row)

        medical = row.get("step1_clinical_necessity") or {}
        contract = row.get("step2_contract_clause") or {}
        risk_flags = contract.get("contract_risk_flags") or {}
        recognized = row.get("recognized_service") or {}
        service_name = as_text(recognized.get("canonical_name") or row.get("service_name_raw"))
        category_code = as_text(recognized.get("category_code"))
        service_name_norm = normalize_text(service_name)

        traceability_flags: list[str] = []
        document_flags: list[str] = []
        routing_flags: list[str] = []

        so_hoso = as_text(row.get("so_hoso_boithuong"))
        source_claim_ids = case_profile.get("source_claim_ids") or []
        if not so_hoso and not case_profile.get("so_hoso_candidates") and not source_claim_ids:
            traceability_flags.append("missing_claim_reference")

        contract_resolution = contract.get("contract_resolution") or {}
        if as_text(contract_resolution.get("status")) == "contract_unknown":
            traceability_flags.append("missing_contract_reference")

        if not case_profile.get("available"):
            traceability_flags.append("missing_case_bundle")

        has_clinical_context = bool(
            as_text(row.get("primary_icd"))
            or (row.get("initial_signs") or [])
            or case_profile.get("admission_reason")
            or case_profile.get("medical_history")
            or case_profile.get("symptom")
            or case_profile.get("lab_document_count")
            or case_profile.get("observation_count")
        )
        if not has_clinical_context and as_text(medical.get("medical_necessity_status")) == "uncertain":
            document_flags.append("missing_clinical_context")

        preop_markers = case_profile.get("preop_bundle_hits") or []
        procedure_markers = case_profile.get("procedure_context_hits") or []
        service_in_preop_bundle = any(marker in service_name_norm for marker in PREOP_SERVICE_MARKERS)
        if service_in_preop_bundle and len(preop_markers) >= 4:
            if procedure_markers or category_code.startswith(("LAB", "IMG", "FUN")):
                routing_flags.append("preop_bundle_or_workup_case")

        if risk_flags.get("documents_sensitive"):
            document_flags.append("documents_sensitive_from_contract_history")
        if category_code.startswith("LAB") and case_profile.get("lab_document_count", 0) == 0 and case_profile.get("observation_count", 0) == 0:
            if as_text(medical.get("medical_necessity_status")) == "uncertain":
                document_flags.append("no_lab_document_evidence")

        step2_decision = as_text(contract.get("decision"))
        if step2_decision.startswith("contract_unknown_with_"):
            routing_flags.append("needs_contract_side_review")
        if step2_decision in {"contract_review_admin_or_clause_sensitive", "contract_review_screening_sensitive"}:
            routing_flags.append("clause_sensitive_review")

        if traceability_flags and ("missing_case_bundle" in traceability_flags or "missing_claim_reference" in traceability_flags):
            decision = "admin_review_missing_traceability"
            reason = "Case-level traceability is incomplete; final adjudication should not proceed automatically."
        elif "preop_bundle_or_workup_case" in routing_flags:
            decision = "admin_review_preop_or_workup_bundle"
            reason = "This service sits inside a likely pre-op or procedural workup bundle and should be reviewed in context."
        elif document_flags:
            decision = "admin_review_document_or_context_gap"
            reason = "Administrative or documentation gaps remain before a reliable final decision."
        elif "needs_contract_side_review" in routing_flags or "clause_sensitive_review" in routing_flags:
            decision = "admin_review_contract_followup"
            reason = "The service needs contract-side or clause-side follow-up before final adjudication."
        else:
            decision = "admin_clear"
            reason = "No strong administrative, traceability, or document gap was detected at Step 3."

        return {
            "decision": decision,
            "reason": reason,
            "traceability_flags": traceability_flags,
            "document_flags": document_flags,
            "routing_flags": routing_flags,
            "case_profile": {
                "available": case_profile.get("available"),
                "lab_document_count": case_profile.get("lab_document_count"),
                "observation_count": case_profile.get("observation_count"),
                "source_claim_ids": case_profile.get("source_claim_ids"),
                "so_hoso_candidates": case_profile.get("so_hoso_candidates"),
                "admission_reason": case_profile.get("admission_reason"),
                "medical_history": case_profile.get("medical_history"),
                "symptom": case_profile.get("symptom"),
                "outpatient_department": case_profile.get("outpatient_department"),
                "facility_name": case_profile.get("facility_name"),
                "preop_bundle_hits": case_profile.get("preop_bundle_hits"),
                "procedure_context_hits": case_profile.get("procedure_context_hits"),
            },
        }


def build_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    decision_counts = Counter()
    traceability_counts = Counter()
    document_counts = Counter()
    routing_counts = Counter()

    for row in rows:
        step3 = row.get("step3_admin_timing") or {}
        decision_counts[as_text(step3.get("decision")) or "unknown"] += 1
        for item in step3.get("traceability_flags") or []:
            traceability_counts[item] += 1
        for item in step3.get("document_flags") or []:
            document_counts[item] += 1
        for item in step3.get("routing_flags") or []:
            routing_counts[item] += 1

    return {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "stats": {
            "total_rows": len(rows),
            "decision_distribution": dict(decision_counts),
            "traceability_flag_counts": dict(traceability_counts),
            "document_flag_counts": dict(document_counts),
            "routing_flag_counts": dict(routing_counts),
        },
    }


def build_examples(rows: list[dict[str, Any]]) -> dict[str, Any]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        step3 = row.get("step3_admin_timing") or {}
        decision = as_text(step3.get("decision")) or "unknown"
        bucket = grouped.setdefault(decision, [])
        if len(bucket) >= 5:
            continue
        bucket.append(
            {
                "benchmark_id": row.get("benchmark_id"),
                "service_name_raw": row.get("service_name_raw"),
                "recognized_service": row.get("recognized_service"),
                "step1_status": (row.get("step1_clinical_necessity") or {}).get("medical_necessity_status"),
                "step2_status": (row.get("step2_contract_clause") or {}).get("decision"),
                "step3": step3,
            }
        )
    return {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "examples_by_decision": grouped,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Step 3 admin / timing / document reasoning.")
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT_JSONL)
    parser.add_argument("--caseset", type=Path, default=DEFAULT_CASESET_JSONL)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT_JSONL)
    parser.add_argument("--summary", type=Path, default=DEFAULT_SUMMARY_JSON)
    parser.add_argument("--examples", type=Path, default=DEFAULT_EXAMPLES_JSON)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    rows = load_jsonl(args.input)
    case_rows = load_jsonl(args.caseset)
    engine = AdminTimingStep3(case_rows)

    scored_rows: list[dict[str, Any]] = []
    for row in rows:
        enriched = dict(row)
        enriched["step3_admin_timing"] = engine.assess_row(row)
        scored_rows.append(enriched)

    write_jsonl(args.output, scored_rows)
    args.summary.parent.mkdir(parents=True, exist_ok=True)
    args.summary.write_text(json.dumps(build_summary(scored_rows), ensure_ascii=False, indent=2), encoding="utf-8")
    args.examples.parent.mkdir(parents=True, exist_ok=True)
    args.examples.write_text(json.dumps(build_examples(scored_rows), ensure_ascii=False, indent=2), encoding="utf-8")


if __name__ == "__main__":
    main()
