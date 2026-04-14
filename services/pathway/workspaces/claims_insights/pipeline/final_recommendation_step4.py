from __future__ import annotations

import argparse
import json
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any


PROJECT_DIR = Path(__file__).parent.parent
DEFAULT_INPUT_JSONL = PROJECT_DIR / "09_unified_story_testcase" / "step3_admin_timing" / "admin_timing_scored.jsonl"
DEFAULT_OUTPUT_JSONL = PROJECT_DIR / "09_unified_story_testcase" / "step4_final_recommendation" / "final_recommendation_scored.jsonl"
DEFAULT_SUMMARY_JSON = PROJECT_DIR / "09_unified_story_testcase" / "step4_final_recommendation" / "final_recommendation_summary.json"
DEFAULT_EXAMPLES_JSON = PROJECT_DIR / "09_unified_story_testcase" / "step4_final_recommendation" / "final_recommendation_examples.json"

MEDICALLY_SUPPORTED_STATUSES = {
    "supported_by_final_diagnosis",
    "supported_by_initial_signs",
    "supported_by_both",
    "supported_by_lab_results",
}


def as_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    return str(value).strip()


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


class FinalRecommendationStep4:
    def assess_row(self, row: dict[str, Any]) -> dict[str, Any]:
        step1 = row.get("step1_clinical_necessity") or {}
        step2 = row.get("step2_contract_clause") or {}
        step3 = row.get("step3_admin_timing") or {}

        medical_status = as_text(step1.get("medical_necessity_status"))
        step2_decision = as_text(step2.get("decision"))
        step3_decision = as_text(step3.get("decision"))
        contract_resolution = step2.get("contract_resolution") or {}
        contract_status = as_text(contract_resolution.get("status"))

        decision = ""
        reason = ""
        route_group = ""
        priority = ""

        if step3_decision == "admin_review_missing_traceability":
            decision = "manual_review_admin_traceability"
            route_group = "admin"
            priority = "high"
            reason = "Missing claim or case traceability prevents a safe final decision."
        elif step3_decision == "admin_review_preop_or_workup_bundle":
            decision = "manual_review_bundle_context"
            route_group = "admin"
            priority = "high"
            reason = "This service sits inside a pre-op or procedural workup bundle and should be reviewed in whole-case context."
        elif step3_decision == "admin_review_document_or_context_gap":
            decision = "manual_review_admin_documents"
            route_group = "admin"
            priority = "medium"
            reason = "Documentation or administrative context is still incomplete."
        elif medical_status == "not_medically_supported":
            decision = "deny_candidate_medical"
            route_group = "medical"
            priority = "high"
            reason = "The service is currently not supported by initial signs, final diagnosis, or result-aware medical evidence."
        elif step2_decision in {"contract_partial_pay_sensitive", "contract_unknown_with_partial_pay_prior"}:
            decision = "manual_review_partial_pay"
            route_group = "contract"
            priority = "medium"
            reason = "Coverage looks potentially partial-pay and needs benefit-level review."
        elif step2_decision in {
            "contract_review_not_covered_sensitive",
            "contract_review_screening_sensitive",
            "contract_review_admin_or_clause_sensitive",
            "contract_unknown_with_screening_prior",
            "contract_unknown_with_not_covered_prior",
        }:
            decision = "manual_review_contract_sensitive"
            route_group = "contract"
            priority = "high"
            reason = "Contract-side or clause-side signals are strong enough to require reviewer attention."
        elif medical_status == "uncertain":
            decision = "manual_review_clinical_uncertain"
            route_group = "medical"
            priority = "medium"
            reason = "Medical necessity remains uncertain after the current reasoning layers."
        elif contract_status == "contract_unknown":
            decision = "medical_clear_pending_contract"
            route_group = "contract"
            priority = "medium"
            reason = "The service looks medically supportable, but coverage cannot be finalized until contract mapping is available."
        elif step2_decision == "contract_clear" and medical_status in MEDICALLY_SUPPORTED_STATUSES and step3_decision == "admin_clear":
            decision = "approve_candidate"
            route_group = "approve"
            priority = "normal"
            reason = "Medical, contract, and administrative layers are currently aligned."
        else:
            decision = "manual_review_combined"
            route_group = "mixed"
            priority = "medium"
            reason = "Signals across steps do not yet support a clean automated routing decision."

        return {
            "decision": decision,
            "route_group": route_group,
            "priority": priority,
            "reason": reason,
            "inputs": {
                "medical_necessity_status": medical_status,
                "step2_decision": step2_decision,
                "contract_status": contract_status,
                "step3_decision": step3_decision,
            },
        }


def build_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    decision_counts = Counter()
    route_group_counts = Counter()
    priority_counts = Counter()

    cross_step_counts = {
        "medical_not_supported_and_contract_sensitive": 0,
        "medically_supported_but_contract_unknown": 0,
        "medically_supported_but_admin_review": 0,
        "clinical_uncertain_but_admin_clear": 0,
    }

    for row in rows:
        step1 = row.get("step1_clinical_necessity") or {}
        step3 = row.get("step3_admin_timing") or {}
        step4 = row.get("step4_final_recommendation") or {}
        contract_resolution = (row.get("step2_contract_clause") or {}).get("contract_resolution") or {}

        medical_status = as_text(step1.get("medical_necessity_status"))
        contract_status = as_text(contract_resolution.get("status"))
        step3_decision = as_text(step3.get("decision"))
        step4_decision = as_text(step4.get("decision")) or "unknown"

        decision_counts[step4_decision] += 1
        route_group_counts[as_text(step4.get("route_group")) or "unknown"] += 1
        priority_counts[as_text(step4.get("priority")) or "unknown"] += 1

        if medical_status == "not_medically_supported" and step4_decision == "manual_review_contract_sensitive":
            cross_step_counts["medical_not_supported_and_contract_sensitive"] += 1
        if medical_status in MEDICALLY_SUPPORTED_STATUSES and contract_status == "contract_unknown":
            cross_step_counts["medically_supported_but_contract_unknown"] += 1
        if medical_status in MEDICALLY_SUPPORTED_STATUSES and step3_decision != "admin_clear":
            cross_step_counts["medically_supported_but_admin_review"] += 1
        if medical_status == "uncertain" and step3_decision == "admin_clear":
            cross_step_counts["clinical_uncertain_but_admin_clear"] += 1

    return {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "stats": {
            "total_rows": len(rows),
            "decision_distribution": dict(decision_counts),
            "route_group_distribution": dict(route_group_counts),
            "priority_distribution": dict(priority_counts),
            "cross_step_counts": cross_step_counts,
        },
    }


def build_examples(rows: list[dict[str, Any]]) -> dict[str, Any]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        step4 = row.get("step4_final_recommendation") or {}
        decision = as_text(step4.get("decision")) or "unknown"
        bucket = grouped.setdefault(decision, [])
        if len(bucket) >= 5:
            continue
        bucket.append(
            {
                "benchmark_id": row.get("benchmark_id"),
                "service_name_raw": row.get("service_name_raw"),
                "recognized_service": row.get("recognized_service"),
                "step1": row.get("step1_clinical_necessity"),
                "step2": row.get("step2_contract_clause"),
                "step3": row.get("step3_admin_timing"),
                "step4": step4,
            }
        )
    return {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "examples_by_decision": grouped,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Step 4 final routing recommendation.")
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT_JSONL)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT_JSONL)
    parser.add_argument("--summary", type=Path, default=DEFAULT_SUMMARY_JSON)
    parser.add_argument("--examples", type=Path, default=DEFAULT_EXAMPLES_JSON)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    rows = load_jsonl(args.input)
    engine = FinalRecommendationStep4()

    scored_rows: list[dict[str, Any]] = []
    for row in rows:
        enriched = dict(row)
        enriched["step4_final_recommendation"] = engine.assess_row(row)
        scored_rows.append(enriched)

    write_jsonl(args.output, scored_rows)
    args.summary.parent.mkdir(parents=True, exist_ok=True)
    args.summary.write_text(json.dumps(build_summary(scored_rows), ensure_ascii=False, indent=2), encoding="utf-8")
    args.examples.parent.mkdir(parents=True, exist_ok=True)
    args.examples.write_text(json.dumps(build_examples(scored_rows), ensure_ascii=False, indent=2), encoding="utf-8")


if __name__ == "__main__":
    main()
