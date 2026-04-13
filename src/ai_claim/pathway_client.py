from __future__ import annotations

from dataclasses import dataclass
from time import perf_counter
from typing import Any

import httpx

from .settings import SETTINGS


def _count_values(rows: list[dict[str, Any]], key: str) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        value = str(row.get(key) or "unknown")
        counts[value] = counts.get(value, 0) + 1
    return counts


@dataclass(slots=True)
class PathwayClient:
    base_url: str = SETTINGS.pathway_api_base_url
    timeout_seconds: float = 120.0

    def _post(self, path: str, payload: dict[str, Any]) -> dict[str, Any]:
        with httpx.Client(base_url=self.base_url, timeout=self.timeout_seconds) as client:
            started_at = perf_counter()
            response = client.post(path, json=payload)
            duration_ms = round((perf_counter() - started_at) * 1000, 1)
            response.raise_for_status()
            return {
                "path": path,
                "duration_ms": duration_ms,
                "status_code": response.status_code,
                "payload": response.json(),
                "request": payload,
            }

    def _get(self, path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        with httpx.Client(base_url=self.base_url, timeout=self.timeout_seconds) as client:
            started_at = perf_counter()
            response = client.get(path, params=params)
            duration_ms = round((perf_counter() - started_at) * 1000, 1)
            response.raise_for_status()
            return {
                "path": path,
                "duration_ms": duration_ms,
                "status_code": response.status_code,
                "payload": response.json(),
                "params": params or {},
            }

    def build_medical_request(self, case_packet: dict[str, Any]) -> dict[str, Any]:
        clinical = case_packet.get("clinical_context", {}) or {}
        insurance = case_packet.get("insurance_context", {}) or {}
        return {
            "case_id": case_packet.get("case_id", ""),
            "known_diseases": list(case_packet.get("known_diseases", []) or []),
            "symptoms": list(clinical.get("symptoms", []) or []),
            "medical_history": "; ".join(clinical.get("medical_history", []) or []),
            "admission_reason": clinical.get("admission_reason", ""),
            "service_lines": [
                {
                    "service_name_raw": item.get("service_name_raw", ""),
                    "contract_id": insurance.get("contract_id", ""),
                    "insurer": insurance.get("insurer", ""),
                    "cost_vnd": item.get("cost_vnd", 0),
                    "symptoms": list(clinical.get("symptoms", []) or []),
                    "medical_history": "; ".join(clinical.get("medical_history", []) or []),
                    "admission_reason": clinical.get("admission_reason", ""),
                }
                for item in case_packet.get("service_lines", []) or []
            ],
        }

    def build_adjudicate_request(self, case_packet: dict[str, Any]) -> dict[str, Any]:
        clinical = case_packet.get("clinical_context", {}) or {}
        insurance = case_packet.get("insurance_context", {}) or {}
        return {
            "claim_id": case_packet.get("case_id", ""),
            "contract_id": insurance.get("contract_id", ""),
            "insurer": insurance.get("insurer", ""),
            "known_diseases": list(case_packet.get("known_diseases", []) or []),
            "symptoms": list(clinical.get("symptoms", []) or []),
            "medical_history": "; ".join(clinical.get("medical_history", []) or []),
            "admission_reason": clinical.get("admission_reason", ""),
            "service_lines": [
                {
                    "service_name_raw": item.get("service_name_raw", ""),
                    "contract_id": insurance.get("contract_id", ""),
                    "insurer": insurance.get("insurer", ""),
                    "cost_vnd": item.get("cost_vnd", 0),
                    "symptoms": list(clinical.get("symptoms", []) or []),
                    "medical_history": "; ".join(clinical.get("medical_history", []) or []),
                    "admission_reason": clinical.get("admission_reason", ""),
                }
                for item in case_packet.get("service_lines", []) or []
            ],
        }

    def run_medical_reasoning(self, case_packet: dict[str, Any]) -> dict[str, Any]:
        return self._post("/api/medical/reason-services", self.build_medical_request(case_packet))

    def run_adjudication(self, case_packet: dict[str, Any]) -> dict[str, Any]:
        return self._post("/api/adjudicate/v2", self.build_adjudicate_request(case_packet))

    def graph_operating_health(self) -> dict[str, Any]:
        return self._get("/api/graph-operating/health")

    @staticmethod
    def summarize_medical_metrics(result: dict[str, Any]) -> dict[str, Any]:
        payload = result.get("payload", {}) or {}
        line_results = list(payload.get("line_results", []) or [])
        return {
            "mode": payload.get("mode"),
            "case_reasoning_trace_steps": len(payload.get("reasoning_trace", []) or []),
            "case_verification_plan_items": len(payload.get("verification_plan", []) or []),
            "case_evidence_ledger_items": len(payload.get("evidence_ledger", []) or []),
            "case_coverage_gap_items": len(payload.get("coverage_gaps", []) or []),
            "line_result_count": len(line_results),
            "line_reasoning_trace_steps": sum(len(item.get("reasoning_trace", []) or []) for item in line_results),
            "line_verification_plan_items": sum(len(item.get("verification_plan", []) or []) for item in line_results),
            "line_evidence_ledger_items": sum(len(item.get("evidence_ledger", []) or []) for item in line_results),
            "line_coverage_gap_items": sum(len(item.get("coverage_gaps", []) or []) for item in line_results),
            "medical_decision_breakdown": _count_values(line_results, "medical_decision"),
        }

    @staticmethod
    def summarize_adjudication_metrics(result: dict[str, Any]) -> dict[str, Any]:
        payload = result.get("payload", {}) or {}
        items = list(payload.get("results", []) or [])
        return {
            "result_count": len(items),
            "final_decision_breakdown": _count_values(items, "final_decision"),
            "medical_decision_breakdown": _count_values(items, "medical_decision"),
            "summary_vi": payload.get("summary_vi", ""),
        }
