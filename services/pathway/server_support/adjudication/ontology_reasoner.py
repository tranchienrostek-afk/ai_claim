"""Ontology-first clinical reasoning for disease -> service adjudication.

This module pulls the core logic out of testcase/debug flows and makes it
reusable in the live adjudication API:
1. Read case-level signs/symptoms
2. Infer top disease hypotheses from the ontology-aware hypothesis engine
3. Resolve active diseases against the Neo4j ontology
4. Retrieve disease-specific expected services from DISEASE_EXPECTS_SERVICE
5. Judge each requested service against ontology evidence
"""

from __future__ import annotations

import re
from typing import Any

from .engine_bridge import _ensure_importable, get_adjudication_mvp, get_disease_hypothesis_engine
from .models import ServiceLineInput
from server_support.ontology_v2_inspector_store import DEFAULT_NAMESPACE, OntologyV2InspectorStore

_ensure_importable()
from sign_phrase_decomposer import SignPhraseDecomposer, ascii_fold  # type: ignore[import-not-found]


GENERIC_TOKENS = {
    "",
    "benh",
    "hoi",
    "chung",
    "cap",
    "man",
    "tinh",
    "khong",
    "co",
    "va",
    "theo",
    "doi",
    "nghi",
    "chan",
    "doan",
    "benhly",
}

ROLE_WEIGHTS = {
    "confirmatory": 1.0,
    "contraindication": 1.0,
    "diagnostic": 0.96,
    "rule_out": 0.88,
    "screening": 0.80,
    "severity": 0.86,
    "treatment": 0.84,
    "monitoring": 0.78,
}

SOURCE_WEIGHTS = {
    "assertion_contraindicates_service": 0.99,
    "assertion_indicates_service": 0.98,
    "expected_service": 1.0,
    "required_service": 0.92,
    "key_service": 0.72,
    "service_mention": 0.58,
    "graph_context_snippet": 0.68,
    "assertion_snippet": 0.70,
}


def as_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def dedupe_keep_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for value in values:
        text = as_text(value)
        key = ascii_fold(text)
        if not key or key in seen:
            continue
        seen.add(key)
        ordered.append(text)
    return ordered


def normalize_icd(value: Any) -> str:
    return "".join(ch for ch in as_text(value).upper() if ch.isalnum() or ch == ".")


def icd_group(value: Any) -> str:
    code = normalize_icd(value)
    if not code:
        return ""
    return code.split(".")[0][:3]


def important_tokens(value: Any) -> set[str]:
    return {
        token
        for token in ascii_fold(value).split()
        if len(token) > 2 and token not in GENERIC_TOKENS
    }


def text_overlap_score(left: Any, right: Any) -> float:
    left_key = ascii_fold(left)
    right_key = ascii_fold(right)
    if not left_key or not right_key:
        return 0.0
    if left_key == right_key:
        return 1.0
    if len(left_key.split()) >= 2 and (left_key in right_key or right_key in left_key):
        return 0.92
    if (left_key in right_key or right_key in left_key) and min(len(left_key), len(right_key)) >= 5:
        return 0.90

    left_tokens = important_tokens(left)
    right_tokens = important_tokens(right)
    if not left_tokens or not right_tokens:
        return 0.0
    overlap = left_tokens & right_tokens
    if not overlap:
        return 0.0
    return len(overlap) / max(len(left_tokens | right_tokens), 1)


def role_weight(role: str) -> float:
    return ROLE_WEIGHTS.get(ascii_fold(role).replace(" ", "_"), 0.82)


def source_weight(source: str) -> float:
    return SOURCE_WEIGHTS.get(source, 0.60)


class OntologyClinicalReasoner:
    """Case-level ontology reasoner shared by the live adjudication API."""

    def __init__(self, namespace: str = DEFAULT_NAMESPACE) -> None:
        self.namespace = namespace
        self._inspector = OntologyV2InspectorStore()
        self._hypothesis_engine = get_disease_hypothesis_engine()
        self._service_mapper = get_adjudication_mvp()
        self._decomposer = SignPhraseDecomposer()
        self._disease_catalog_cache: list[dict[str, Any]] | None = None
        self._expected_service_cache: dict[str, list[dict[str, Any]]] = {}
        self._disease_graph_cache: dict[str, dict[str, Any]] = {}
        self._label_cache: set[str] | None = None

    def _trace_step(self, phase: str, action: str, *, details: dict[str, Any] | None = None) -> dict[str, Any]:
        step = {
            "phase": phase,
            "action": action,
        }
        if details:
            step["details"] = details
        return step

    def _service_mapping_summary(self, service_info: dict[str, Any]) -> dict[str, Any]:
        return {
            "service_code": as_text(service_info.get("service_code")),
            "canonical_name": as_text(service_info.get("canonical_name")),
            "category_code": as_text(service_info.get("category_code")),
            "mapping_status": as_text(service_info.get("mapping_status")),
            "suggested_service_code": as_text(service_info.get("suggested_service_code")),
            "suggested_canonical_name": as_text(service_info.get("suggested_canonical_name")),
            "top_candidates": [
                {
                    "service_code": as_text(item.get("service_code")),
                    "canonical_name": as_text(item.get("canonical_name")),
                    "score": item.get("score"),
                    "confidence": as_text(item.get("confidence")),
                }
                for item in (service_info.get("top_candidates") or [])[:3]
            ],
        }

    def _disease_evidence_summary(self, disease: dict[str, Any]) -> dict[str, Any]:
        return {
            "disease_name": as_text(disease.get("disease_name")),
            "confidence": float(disease.get("confidence") or 0.0),
            "source": as_text(disease.get("source")),
            "expected_service_count": int(disease.get("expected_service_count") or 0),
            "assertion_service_count": int(disease.get("assertion_service_count") or 0),
            "contraindicated_service_count": int(disease.get("contraindicated_service_count") or 0),
            "key_service_count": int(disease.get("key_service_count") or 0),
            "service_mention_count": int(disease.get("service_mention_count") or 0),
            "required_service_count": len(disease.get("required_services") or []),
            "example_expected_services": [
                as_text(item.get("service_name"))
                for item in (disease.get("expected_services") or [])[:3]
                if as_text(item.get("service_name"))
            ],
            "example_assertion_services": [
                as_text(item.get("service_name"))
                for item in (disease.get("assertion_service_links") or [])[:3]
                if as_text(item.get("service_name"))
            ],
            "example_key_services": [as_text(item) for item in (disease.get("key_services") or [])[:3] if as_text(item)],
        }

    def _match_trace_summary(self, match: dict[str, Any]) -> dict[str, Any]:
        return {
            "disease_name": as_text(match.get("disease_name")),
            "target_service_name": as_text(match.get("target_service_name")),
            "target_service_code": as_text(match.get("target_service_code")),
            "support_source": as_text(match.get("support_source")),
            "polarity": as_text(match.get("polarity") or "support"),
            "role": as_text(match.get("role")),
            "raw_match_score": float(match.get("raw_match_score") or 0.0),
            "support_score": float(match.get("support_score") or 0.0),
            "assertion_id": as_text(match.get("assertion_id")),
            "section_title": as_text(match.get("section_title")),
            "source_page": match.get("source_page"),
        }

    def _assertion_role(self, assertion_type: Any) -> str:
        text = ascii_fold(assertion_type).replace(" ", "_")
        if "contra" in text:
            return "contraindication"
        if "monitor" in text:
            return "monitoring"
        if "treat" in text:
            return "treatment"
        if "screen" in text or "prevent" in text:
            return "screening"
        if "rule_out" in text or "exclude" in text:
            return "rule_out"
        return "diagnostic"

    def _match_provenance(self, match: dict[str, Any]) -> str:
        parts: list[str] = []
        if as_text(match.get("assertion_id")):
            parts.append(f"assertion={as_text(match.get('assertion_id'))}")
        if as_text(match.get("section_title")):
            parts.append(f"section={as_text(match.get('section_title'))}")
        if match.get("source_page") not in (None, ""):
            parts.append(f"page={match.get('source_page')}")
        if as_text(match.get("doc_title")):
            parts.append(f"document={as_text(match.get('doc_title'))}")
        return ", ".join(parts)

    def _case_next_actions(self, disease_packets: list[dict[str, Any]], coverage_ratio: float) -> list[str]:
        actions: list[str] = []
        if not disease_packets:
            actions.append("Chua resolve duoc benh active; can them known_diseases hoac bo sung dau hieu ban dau.")
        if coverage_ratio < 0.45:
            actions.append("Ontology coverage con thap; uu tien bo sung disease -> expected service va assertion edges.")
        if any(
            not item.get("expected_service_count")
            and not item.get("assertion_service_count")
            and not item.get("contraindicated_service_count")
            and not item.get("key_service_count")
            and not item.get("service_mention_count")
            for item in disease_packets
        ):
            actions.append("Mot so benh active chua co goi evidence day du; can re-ingest hoac canonicalize them.")
        return dedupe_keep_order(actions)

    def _verification_item(
        self,
        item_id: str,
        description: str,
        *,
        status: str = "completed",
        details: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        payload = {
            "id": item_id,
            "description": description,
            "status": status,
        }
        if details:
            payload["details"] = details
        return payload

    def _ledger_entry(
        self,
        category: str,
        surface: str,
        key: str,
        summary: str,
        *,
        status: str = "observed",
        details: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        payload = {
            "category": category,
            "surface": surface,
            "key": key,
            "status": status,
            "summary": summary,
        }
        if details:
            payload["details"] = details
        return payload

    def _coverage_gap(
        self,
        gap_id: str,
        severity: str,
        message: str,
        *,
        next_action: str = "",
        details: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        payload = {
            "id": gap_id,
            "severity": severity,
            "message": message,
        }
        if next_action:
            payload["next_action"] = next_action
        if details:
            payload["details"] = details
        return payload

    def _service_mapping_success(self, service_info: dict[str, Any]) -> bool:
        return any(
            as_text(service_info.get(key))
            for key in (
                "service_code",
                "canonical_name",
                "suggested_service_code",
                "suggested_canonical_name",
            )
        ) or bool(service_info.get("top_candidates"))

    def _build_case_verification_plan(
        self,
        *,
        lines: list[ServiceLineInput],
        signs: list[str],
        disease_hints: list[dict[str, str]],
        top_hypotheses: list[dict[str, Any]],
        disease_packets: list[dict[str, Any]],
        coverage_ratio: float,
    ) -> list[dict[str, Any]]:
        return [
            self._verification_item(
                "normalize_case_input",
                "Chuan hoa input case thanh signs, disease hints, va observed services de reasoner co the lap scope ban dau.",
                details={
                    "line_count": len(lines),
                    "sign_count": len(signs),
                    "disease_hint_count": len(disease_hints),
                },
            ),
            self._verification_item(
                "resolve_known_disease_hints",
                "Neu input co benh da biet, doi chieu ve catalog ontology truoc khi suy tu dau hieu.",
                status="completed" if disease_hints else "skipped",
                details={
                    "disease_hint_preview": disease_hints[:4],
                },
            ),
            self._verification_item(
                "infer_top_diseases",
                "Sinh top disease hypotheses tu dau hieu ban dau va observed services.",
                status="completed" if top_hypotheses else "warning",
                details={
                    "hypothesis_count": len(top_hypotheses),
                    "top_hypotheses": [
                        {
                            "disease_name": as_text(item.get("disease_name")),
                            "confidence": float(item.get("confidence") or 0.0),
                        }
                        for item in top_hypotheses[:5]
                    ],
                },
            ),
            self._verification_item(
                "load_active_disease_packets",
                "Nap goi evidence ontology cho cac benh active da chot de phuc vu service reasoning.",
                status="completed" if disease_packets else "warning",
                details={
                    "active_disease_count": len(disease_packets),
                },
            ),
            self._verification_item(
                "check_case_coverage",
                "Do do phu ontology truoc khi cho phep ket luan o cap dich vu.",
                status="completed" if coverage_ratio >= 0.45 else "warning",
                details={
                    "coverage_ratio": round(coverage_ratio, 4),
                },
            ),
        ]

    def _build_case_evidence_ledger(
        self,
        *,
        lines: list[ServiceLineInput],
        signs: list[str],
        disease_hints: list[dict[str, str]],
        top_hypotheses: list[dict[str, Any]],
        disease_packets: list[dict[str, Any]],
        coverage_ratio: float,
    ) -> list[dict[str, Any]]:
        ledger = [
            self._ledger_entry(
                "runtime_signal",
                "request_payload",
                "case_input",
                f"Nhan {len(lines)} dich vu dau vao, trich {len(signs)} signs va {len(disease_hints)} disease hints.",
                details={
                    "input_signs": signs[:12],
                    "disease_hints": disease_hints[:6],
                    "service_names": [as_text(line.service_name_raw) for line in lines[:8] if as_text(line.service_name_raw)],
                },
            ),
            self._ledger_entry(
                "inference",
                "disease_hypothesis_engine",
                "top_hypotheses",
                f"Sinh {len(top_hypotheses)} disease hypotheses tu input da chuan hoa.",
                details={
                    "top_hypotheses": [
                        {
                            "disease_name": as_text(item.get("disease_name")),
                            "confidence": float(item.get("confidence") or 0.0),
                            "status": as_text(item.get("status")),
                        }
                        for item in top_hypotheses[:5]
                    ],
                },
            ),
            self._ledger_entry(
                "neo4j_fact",
                f"neo4j:{self.namespace}",
                "active_disease_packets",
                f"Mo {len(disease_packets)} disease packets tu ontology namespace '{self.namespace}'.",
                details={
                    "active_diseases": [self._disease_evidence_summary(item) for item in disease_packets[:5]],
                },
            ),
            self._ledger_entry(
                "runtime_signal",
                "reasoning_runtime",
                "case_coverage",
                f"Coverage ratio cua case dat {round(coverage_ratio, 4)}.",
                details={
                    "coverage_ratio": round(coverage_ratio, 4),
                    "covered_disease_count": sum(
                        1
                        for item in disease_packets
                        if item.get("expected_service_count")
                        or item.get("assertion_service_count")
                        or item.get("contraindicated_service_count")
                        or item.get("required_services")
                        or item.get("key_service_count")
                        or item.get("service_mention_count")
                    ),
                    "active_disease_count": len(disease_packets),
                },
            ),
        ]
        if not signs and not disease_hints:
            ledger.append(
                self._ledger_entry(
                    "unknown",
                    "request_payload",
                    "missing_case_clinical_context",
                    "Case khong co signs ro rang va cung khong co known diseases; reasoner se de mat nhieu o buoc hypothesis generation.",
                    status="warning",
                )
            )
        return ledger

    def _build_case_coverage_gaps(
        self,
        *,
        signs: list[str],
        disease_hints: list[dict[str, str]],
        disease_packets: list[dict[str, Any]],
        coverage_ratio: float,
    ) -> list[dict[str, Any]]:
        gaps: list[dict[str, Any]] = []
        if not signs:
            gaps.append(
                self._coverage_gap(
                    "no_structured_signs",
                    "medium",
                    "Input chua cung cap du signs/symptoms de hypothesis engine co du chat lieu suy luan.",
                    next_action="Bo sung symptoms hoac admission_reason co nghia lam sang.",
                )
            )
        if not disease_hints:
            gaps.append(
                self._coverage_gap(
                    "no_known_disease_hints",
                    "low",
                    "Case khong co known_diseases; he thong phai phu thuoc nhieu hon vao sign inference.",
                    next_action="Neu da biet benh cua khach hang, nen truyen known_diseases de khoa scope nhanh hon.",
                )
            )
        if not disease_packets:
            gaps.append(
                self._coverage_gap(
                    "no_active_diseases_resolved",
                    "high",
                    "Khong resolve duoc benh active nao trong ontology tu input hien tai.",
                    next_action="Kiem tra lai disease hints, bo sung signs, hoac canonicalize disease catalog.",
                )
            )
        if coverage_ratio < 0.45:
            gaps.append(
                self._coverage_gap(
                    "low_case_coverage",
                    "high",
                    "Do phu ontology cho case nay con thap, de gay uncertain hoac deny do thieu bang chung.",
                    next_action="Bo sung expected services va assertion edges cho benh active trong ontology.",
                    details={"coverage_ratio": round(coverage_ratio, 4)},
                )
            )
        for item in disease_packets:
            if (
                not item.get("expected_service_count")
                and not item.get("assertion_service_count")
                and not item.get("contraindicated_service_count")
                and not item.get("key_service_count")
                and not item.get("service_mention_count")
            ):
                gaps.append(
                    self._coverage_gap(
                        f"sparse_disease_packet:{as_text(item.get('disease_id'))}",
                        "medium",
                        f"Benh '{as_text(item.get('disease_name'))}' da duoc kich hoat nhung disease packet con rat mong.",
                        next_action="Re-ingest PDF benh nay hoac bo sung canonical mappings cho service/assertion.",
                    )
                )
        return gaps

    def _build_case_audit_summary(
        self,
        *,
        lines: list[ServiceLineInput],
        signs: list[str],
        disease_hints: list[dict[str, str]],
        top_hypotheses: list[dict[str, Any]],
        disease_packets: list[dict[str, Any]],
        coverage_ratio: float,
        evidence_ledger: list[dict[str, Any]],
        coverage_gaps: list[dict[str, Any]],
    ) -> dict[str, Any]:
        return {
            "data_domain": "medical_knowledge",
            "mission_locked": True,
            "verification_plan_generated": True,
            "input_understood": bool(lines),
            "input_sign_count": len(signs),
            "disease_hint_count": len(disease_hints),
            "hypothesis_generated": bool(top_hypotheses),
            "hypothesis_count": len(top_hypotheses),
            "active_disease_count": len(disease_packets),
            "coverage_ratio": round(coverage_ratio, 4),
            "coverage_sufficient": coverage_ratio >= 0.45,
            "ready_for_line_reasoning": bool(disease_packets),
            "evidence_ledger_entries": len(evidence_ledger),
            "coverage_gap_count": len(coverage_gaps),
            "decision_rule": "prepare_case_only",
            "next_actions": self._case_next_actions(disease_packets, coverage_ratio),
        }

    def _build_line_verification_plan(
        self,
        *,
        line: ServiceLineInput,
        service_info: dict[str, Any],
        active_diseases: list[dict[str, Any]],
        coverage_ratio: float,
        matches: list[dict[str, Any]],
        contraindication_matches: list[dict[str, Any]],
        decision: str,
        decision_rule: str,
        grounded: bool,
    ) -> list[dict[str, Any]]:
        return [
            self._verification_item(
                "map_service_input",
                "Chuan hoa ten dich vu dau vao ve service code/canonical service de co the doi chieu ontology.",
                status="completed" if self._service_mapping_success(service_info) else "warning",
                details=self._service_mapping_summary(service_info),
            ),
            self._verification_item(
                "lock_active_disease_scope",
                "Chot tap benh active can kiem tra cho dich vu nay.",
                status="completed" if active_diseases else "warning",
                details={
                    "active_disease_count": len(active_diseases),
                    "coverage_ratio": round(coverage_ratio, 4),
                },
            ),
            self._verification_item(
                "search_support_evidence",
                "Kiem tra support evidence theo thu tu uu tien: assertion edges -> expected services -> required services -> key services -> mentions/snippets.",
                status="completed" if active_diseases else "skipped",
                details={
                    "support_match_count": len(matches),
                    "sources_checked": [
                        "assertion_indicates_service",
                        "expected_service",
                        "required_service",
                        "key_service",
                        "service_mention",
                        "graph_context_snippet",
                        "assertion_snippet",
                    ],
                },
            ),
            self._verification_item(
                "search_contradiction_evidence",
                "Kiem tra assertion contraindications cho dich vu nay tren cac benh active.",
                status="completed" if active_diseases else "skipped",
                details={
                    "contraindication_match_count": len(contraindication_matches),
                    "sources_checked": ["assertion_contraindicates_service"],
                },
            ),
            self._verification_item(
                "apply_grounded_decision_gate",
                "Chi cho phep ket luan neu support/contra/coverage dat nguong, neu khong thi giu uncertain.",
                details={
                    "decision": decision,
                    "decision_rule": decision_rule,
                    "grounded": grounded,
                    "service_name_raw": as_text(line.service_name_raw),
                },
            ),
        ]

    def _build_line_evidence_ledger(
        self,
        *,
        line: ServiceLineInput,
        service_info: dict[str, Any],
        active_diseases: list[dict[str, Any]],
        coverage_ratio: float,
        matches: list[dict[str, Any]],
        contraindication_matches: list[dict[str, Any]],
        decision: str,
        decision_rule: str,
    ) -> list[dict[str, Any]]:
        best_match = matches[0] if matches else None
        best_contra = contraindication_matches[0] if contraindication_matches else None
        ledger = [
            self._ledger_entry(
                "runtime_signal",
                "request_payload",
                "service_line_input",
                f"Nhan dich vu dau vao '{as_text(line.service_name_raw)}' de y-khoa reasoner tham dinh.",
                details={
                    "service_name_raw": as_text(line.service_name_raw),
                    "diagnosis_text": as_text(line.diagnosis_text),
                    "symptoms": [as_text(item) for item in line.symptoms[:8] if as_text(item)],
                },
            ),
            self._ledger_entry(
                "inference",
                "service_mapper",
                "service_mapping",
                "Da chuan hoa ten dich vu de tao candidate codes/canonical names cho buoc doi chieu ontology.",
                status="observed" if self._service_mapping_success(service_info) else "warning",
                details=self._service_mapping_summary(service_info),
            ),
            self._ledger_entry(
                "neo4j_fact",
                f"neo4j:{self.namespace}",
                "active_disease_scope",
                f"Doi chieu dich vu voi {len(active_diseases)} benh active trong ontology namespace '{self.namespace}'.",
                details={
                    "active_diseases": [
                        {
                            "disease_name": as_text(item.get("disease_name")),
                            "confidence": float(item.get("confidence") or 0.0),
                            "source": as_text(item.get("source")),
                        }
                        for item in active_diseases[:5]
                    ],
                    "coverage_ratio": round(coverage_ratio, 4),
                },
            ),
            self._ledger_entry(
                "neo4j_fact",
                f"neo4j:{self.namespace}",
                "support_search",
                f"Da quet {len(matches)} support matches cho dich vu tu cac nguon ontology.",
                details={
                    "best_support_match": self._match_trace_summary(best_match) if best_match else None,
                    "support_match_count": len(matches),
                },
            ),
            self._ledger_entry(
                "neo4j_fact",
                f"neo4j:{self.namespace}",
                "contraindication_search",
                f"Da quet {len(contraindication_matches)} contradiction matches cho dich vu.",
                details={
                    "best_contra_match": self._match_trace_summary(best_contra) if best_contra else None,
                    "contraindication_match_count": len(contraindication_matches),
                },
            ),
            self._ledger_entry(
                "inference",
                "decision_gate",
                "grounded_service_decision",
                f"Decision gate tra ket qua '{decision}' theo rule '{decision_rule}'.",
                details={
                    "decision": decision,
                    "decision_rule": decision_rule,
                },
            ),
        ]
        if not matches and not contraindication_matches:
            ledger.append(
                self._ledger_entry(
                    "unknown",
                    "neo4j:ontology_gap",
                    "no_direct_service_evidence",
                    "Khong tim thay direct support/contra match cho dich vu nay trong disease scope hien tai.",
                    status="warning",
                )
            )
        return ledger

    def _build_line_coverage_gaps(
        self,
        *,
        service_info: dict[str, Any],
        active_diseases: list[dict[str, Any]],
        coverage_ratio: float,
        matches: list[dict[str, Any]],
        contraindication_matches: list[dict[str, Any]],
        decision: str,
    ) -> list[dict[str, Any]]:
        gaps: list[dict[str, Any]] = []
        if not self._service_mapping_success(service_info):
            gaps.append(
                self._coverage_gap(
                    "weak_service_mapping",
                    "high",
                    "Ten dich vu dau vao chua map du manh vao canonical service trong ontology.",
                    next_action="Bo sung alias/canonical mapping cho service nay hoac truyen ten dich vu chuan hon.",
                )
            )
        if not active_diseases:
            gaps.append(
                self._coverage_gap(
                    "no_active_disease_scope",
                    "high",
                    "Khong co benh active nao de doi chieu cho dich vu nay.",
                    next_action="Bo sung known_diseases hoac input symptoms de reasoner resolve disease scope truoc.",
                )
            )
        if coverage_ratio < 0.45:
            gaps.append(
                self._coverage_gap(
                    "low_case_coverage",
                    "high",
                    "Case context con mong, nen decision cho dich vu nay de bi uncertain hoac deny do thieu bang chung.",
                    next_action="Bo sung ontology coverage cho benh active truoc khi ket luan cho dich vu.",
                    details={"coverage_ratio": round(coverage_ratio, 4)},
                )
            )
        if not matches and decision != "approve":
            gaps.append(
                self._coverage_gap(
                    "no_support_evidence",
                    "medium",
                    "Khong tim thay support evidence truc tiep cho dich vu nay trong disease scope hien tai.",
                    next_action="Kiem tra lai disease packets, assertion edges, expected services va service mapping.",
                )
            )
        if decision == "uncertain":
            gaps.append(
                self._coverage_gap(
                    "uncertain_decision",
                    "medium",
                    "Decision bi giu o muc uncertain vi evidence hoac coverage chua du de ket luan grounded.",
                    next_action="Bo sung case context hoac ontology evidence roi chay lai service reasoning.",
                )
            )
        if contraindication_matches and not matches and decision != "deny":
            gaps.append(
                self._coverage_gap(
                    "unchecked_contradiction_dominance",
                    "medium",
                    "Co contradiction match nhung ket qua cuoi chua chot deny; can review rule threshold.",
                    next_action="Rasoat nguong support/contra va logic decision gate.",
                )
            )
        return gaps

    def _build_line_audit_summary(
        self,
        *,
        service_info: dict[str, Any],
        coverage_ratio: float,
        matches: list[dict[str, Any]],
        contraindication_matches: list[dict[str, Any]],
        evidence_ledger: list[dict[str, Any]],
        coverage_gaps: list[dict[str, Any]],
        decision: str,
        decision_rule: str,
        grounded: bool,
        next_actions: list[str],
    ) -> dict[str, Any]:
        return {
            "data_domain": "medical_knowledge",
            "mission_locked": True,
            "verification_plan_generated": True,
            "service_mapping_checked": True,
            "service_mapping_success": self._service_mapping_success(service_info),
            "support_checked": True,
            "contradictions_checked": True,
            "support_match_count": len(matches),
            "contraindication_match_count": len(contraindication_matches),
            "coverage_ratio": round(coverage_ratio, 4),
            "coverage_sufficient": coverage_ratio >= 0.45,
            "evidence_ledger_entries": len(evidence_ledger),
            "coverage_gap_count": len(coverage_gaps),
            "decision_gate_ran": True,
            "evidence_sufficient": grounded,
            "grounded": grounded,
            "decision": decision,
            "decision_rule": decision_rule,
            "next_actions": next_actions,
        }

    def _build_line_result(
        self,
        *,
        line: ServiceLineInput,
        service_info: dict[str, Any],
        context: dict[str, Any],
        active_diseases: list[dict[str, Any]],
        coverage_ratio: float,
        matches: list[dict[str, Any]],
        contraindication_matches: list[dict[str, Any]],
        result_matches: list[dict[str, Any]],
        decision: str,
        confidence: float,
        reasoning_vi: str,
        decision_rule: str,
        grounded: bool,
        next_actions: list[str],
        reasoning_trace: list[dict[str, Any]],
    ) -> dict[str, Any]:
        verification_plan = self._build_line_verification_plan(
            line=line,
            service_info=service_info,
            active_diseases=active_diseases,
            coverage_ratio=coverage_ratio,
            matches=matches,
            contraindication_matches=contraindication_matches,
            decision=decision,
            decision_rule=decision_rule,
            grounded=grounded,
        )
        evidence_ledger = self._build_line_evidence_ledger(
            line=line,
            service_info=service_info,
            active_diseases=active_diseases,
            coverage_ratio=coverage_ratio,
            matches=matches,
            contraindication_matches=contraindication_matches,
            decision=decision,
            decision_rule=decision_rule,
        )
        coverage_gaps = self._build_line_coverage_gaps(
            service_info=service_info,
            active_diseases=active_diseases,
            coverage_ratio=coverage_ratio,
            matches=matches,
            contraindication_matches=contraindication_matches,
            decision=decision,
        )
        audit_summary = self._build_line_audit_summary(
            service_info=service_info,
            coverage_ratio=coverage_ratio,
            matches=matches,
            contraindication_matches=contraindication_matches,
            evidence_ledger=evidence_ledger,
            coverage_gaps=coverage_gaps,
            decision=decision,
            decision_rule=decision_rule,
            grounded=grounded,
            next_actions=next_actions,
        )
        return {
            "decision": decision,
            "confidence": confidence,
            "reasoning_vi": reasoning_vi,
            "matches": result_matches,
            "service_info": service_info,
            "verification_plan": verification_plan,
            "evidence_ledger": evidence_ledger,
            "coverage_gaps": coverage_gaps,
            "audit_summary": audit_summary,
            "meta": {
                "mode": context.get("mode"),
                "coverage_ratio": coverage_ratio,
                "input_signs": context.get("input_signs") or [],
                "active_diseases": [
                    {
                        "disease_name": item.get("disease_name"),
                        "icd10": item.get("icd10"),
                        "confidence": item.get("confidence"),
                        "source": item.get("source"),
                    }
                    for item in active_diseases
                ],
                "decision_rule": decision_rule,
            },
            "reasoning_trace": reasoning_trace,
        }

    def _build_case_reasoning_trace(
        self,
        *,
        mode: str,
        signs: list[str],
        disease_hints: list[dict[str, str]],
        top_hypotheses: list[dict[str, Any]],
        disease_packets: list[dict[str, Any]],
        coverage_ratio: float,
    ) -> list[dict[str, Any]]:
        return [
            self._trace_step(
                "mission_lock",
                "Khoa nhiem vu: chi duoc ket luan tu ontology graph hien co, khong duoc bu dap bang tri thuc ben ngoai.",
                details={
                    "mode": mode,
                    "input_sign_count": len(signs),
                    "known_disease_hint_count": len(disease_hints),
                },
            ),
            self._trace_step(
                "case_understanding",
                "Chuan hoa dau vao de xac dinh benh active va pham vi suy luan.",
                details={
                    "input_signs": signs[:12],
                    "disease_hints": disease_hints[:6],
                },
            ),
            self._trace_step(
                "hypothesis_generation",
                "Sinh top disease hypotheses tu dau hieu va goi y benh.",
                details={
                    "top_hypotheses": [
                        {
                            "disease_name": as_text(item.get("disease_name")),
                            "confidence": float(item.get("confidence") or 0.0),
                            "status": as_text(item.get("status")),
                        }
                        for item in top_hypotheses[:5]
                    ],
                },
            ),
            self._trace_step(
                "disease_scope",
                "Chot danh sach benh active se duoc mo goi tri thuc ontology.",
                details={
                    "active_diseases": [self._disease_evidence_summary(item) for item in disease_packets[:5]],
                },
            ),
            self._trace_step(
                "knowledge_access_plan",
                "Lap ke hoach lay tri thuc tu direct assertion edges, expected services, key services, service mentions va assertion texts.",
                details={
                    "coverage_ratio": round(coverage_ratio, 4),
                    "next_actions": self._case_next_actions(disease_packets, coverage_ratio),
                },
            ),
        ]

    # ------------------------------------------------------------------
    # Case preparation
    # ------------------------------------------------------------------

    def prepare_case(self, lines: list[ServiceLineInput]) -> dict[str, Any]:
        signs = self._collect_case_signs(lines)
        disease_hints = self._collect_disease_hints(lines)
        observed_services = [
            {
                "service_name_raw": as_text(line.service_name_raw),
                "service_name": as_text(line.service_name_raw),
            }
            for line in lines
            if as_text(line.service_name_raw)
        ]

        hypothesis_result = self._hypothesis_engine.infer(
            signs=signs,
            observed_services=observed_services,
            seed_disease_hints=[row["hint_text"] for row in disease_hints if row["hint_text"]],
            top_k=6,
        )

        resolved_known = self._resolve_known_diseases(disease_hints)
        resolved_from_hypotheses = self._resolve_hypothesis_diseases(hypothesis_result.get("hypotheses") or [])
        resolved_known = self._enrich_resolved_known_with_hypotheses(resolved_known, resolved_from_hypotheses)

        active_diseases: list[dict[str, Any]] = []
        mode = "sign_inference"
        if resolved_known:
            mode = "known_disease"
            active_diseases.extend(resolved_known[:3])
        for disease in resolved_from_hypotheses:
            if len(active_diseases) >= 5:
                break
            if self._contains_disease(active_diseases, disease):
                continue
            active_diseases.append(disease)

        disease_packets = [self._build_disease_packet(disease) for disease in active_diseases]
        disease_packets = [packet for packet in disease_packets if packet]
        coverage_count = sum(
            1
            for packet in disease_packets
            if packet.get("expected_service_count")
            or packet.get("assertion_service_count")
            or packet.get("contraindicated_service_count")
            or packet.get("required_services")
            or packet.get("key_service_count")
            or packet.get("service_mention_count")
        )
        coverage_ratio = coverage_count / max(len(disease_packets), 1) if disease_packets else 0.0
        verification_plan = self._build_case_verification_plan(
            lines=lines,
            signs=signs,
            disease_hints=disease_hints,
            top_hypotheses=hypothesis_result.get("hypotheses") or [],
            disease_packets=disease_packets,
            coverage_ratio=coverage_ratio,
        )
        evidence_ledger = self._build_case_evidence_ledger(
            lines=lines,
            signs=signs,
            disease_hints=disease_hints,
            top_hypotheses=hypothesis_result.get("hypotheses") or [],
            disease_packets=disease_packets,
            coverage_ratio=coverage_ratio,
        )
        coverage_gaps = self._build_case_coverage_gaps(
            signs=signs,
            disease_hints=disease_hints,
            disease_packets=disease_packets,
            coverage_ratio=coverage_ratio,
        )
        audit_summary = self._build_case_audit_summary(
            lines=lines,
            signs=signs,
            disease_hints=disease_hints,
            top_hypotheses=hypothesis_result.get("hypotheses") or [],
            disease_packets=disease_packets,
            coverage_ratio=coverage_ratio,
            evidence_ledger=evidence_ledger,
            coverage_gaps=coverage_gaps,
        )

        return {
            "mode": mode,
            "input_signs": signs,
            "disease_hints": disease_hints,
            "top_hypotheses": hypothesis_result.get("hypotheses") or [],
            "active_diseases": disease_packets,
            "coverage_ratio": round(coverage_ratio, 4),
            "verification_plan": verification_plan,
            "evidence_ledger": evidence_ledger,
            "coverage_gaps": coverage_gaps,
            "audit_summary": audit_summary,
            "reasoning_trace": self._build_case_reasoning_trace(
                mode=mode,
                signs=signs,
                disease_hints=disease_hints,
                top_hypotheses=hypothesis_result.get("hypotheses") or [],
                disease_packets=disease_packets,
                coverage_ratio=coverage_ratio,
            ),
        }

    # ------------------------------------------------------------------
    # Line adjudication
    # ------------------------------------------------------------------

    def assess_line(self, line: ServiceLineInput, case_context: dict[str, Any] | None = None) -> dict[str, Any]:
        context = case_context or self.prepare_case([line])
        service_info = self._service_mapper.recognize_service(as_text(line.service_name_raw))
        active_diseases = context.get("active_diseases") or []
        coverage_ratio = float(context.get("coverage_ratio") or 0.0)
        reasoning_trace = [
            self._trace_step(
                "mission_lock",
                "Khoa nhiem vu cho tung dich vu: chi tra loi hop ly/khong hop ly dua tren evidence ontology co the truy vet.",
                details={
                    "service_name_raw": as_text(line.service_name_raw),
                    "mode": as_text(context.get("mode")),
                },
            ),
            self._trace_step(
                "service_mapping",
                "Chuan hoa ten dich vu dau vao de co the doi chieu voi ontology.",
                details=self._service_mapping_summary(service_info),
            ),
            self._trace_step(
                "disease_scope",
                "Lay danh sach benh active da duoc chot o cap case de doi chieu.",
                details={
                    "active_diseases": [
                        {
                            "disease_name": as_text(item.get("disease_name")),
                            "confidence": float(item.get("confidence") or 0.0),
                            "source": as_text(item.get("source")),
                        }
                        for item in active_diseases[:5]
                    ],
                    "coverage_ratio": round(coverage_ratio, 4),
                },
            ),
            self._trace_step(
                "knowledge_access_plan",
                "Mo goi evidence cho tung benh active theo thu tu: assertion support/contra edges -> expected services -> required services -> key services -> service mentions -> assertion snippets.",
                details={
                    "evidence_budget": [self._disease_evidence_summary(item) for item in active_diseases[:5]],
                },
            ),
        ]

        matches: list[dict[str, Any]] = []
        contraindication_matches: list[dict[str, Any]] = []
        for disease in active_diseases:
            matches.extend(self._match_line_to_disease(line, service_info, disease))
            contraindication_matches.extend(self._match_line_to_contraindications(line, service_info, disease))
        matches.sort(key=lambda item: item["support_score"], reverse=True)
        contraindication_matches.sort(key=lambda item: item["support_score"], reverse=True)
        best_match = matches[0] if matches else None
        best_contra = contraindication_matches[0] if contraindication_matches else None
        reasoning_trace.append(
            self._trace_step(
                "support_ranking",
                "Xep hang ca evidence support va contradiction cho dich vu nay trong ontology.",
                details={
                    "support_match_count": len(matches),
                    "contraindication_match_count": len(contraindication_matches),
                    "best_support_match": self._match_trace_summary(best_match) if best_match else None,
                    "best_contra_match": self._match_trace_summary(best_contra) if best_contra else None,
                    "top_support_matches": [self._match_trace_summary(item) for item in matches[:3]],
                    "top_contra_matches": [self._match_trace_summary(item) for item in contraindication_matches[:3]],
                },
            )
        )

        if best_contra and best_contra["support_score"] >= 0.45 and (
            not best_match or best_contra["support_score"] >= best_match["support_score"]
        ):
            confidence = min(0.98, max(0.74, best_contra["support_score"]))
            provenance = self._match_provenance(best_contra)
            next_actions: list[str] = []
            reasoning_trace.append(
                self._trace_step(
                    "decision_gate",
                    "Chot deny vi ton tai assertion contraindication khop truc tiep voi dich vu.",
                    details={
                        "decision": "deny",
                        "grounded": True,
                        "decision_rule": "assertion_contraindicates_service",
                        "best_contra": self._match_trace_summary(best_contra),
                        "next_actions": [],
                    },
                )
            )
            return self._build_line_result(
                line=line,
                service_info=service_info,
                context=context,
                active_diseases=active_diseases,
                coverage_ratio=coverage_ratio,
                matches=matches,
                contraindication_matches=contraindication_matches,
                result_matches=contraindication_matches[:5],
                decision="deny",
                confidence=round(confidence, 4),
                reasoning_vi=(
                    f"Khong hop ly theo ontology: '{as_text(line.service_name_raw)}' trung voi dich vu bi assertion "
                    f"phan chi dinh cho benh '{best_contra['disease_name']}'"
                    + (f" ({provenance})." if provenance else ".")
                ),
                decision_rule="assertion_contraindicates_service",
                grounded=True,
                next_actions=next_actions,
                reasoning_trace=reasoning_trace,
            )

        if best_match and best_match["support_score"] >= 0.45:
            confidence = min(0.96, max(0.62, best_match["support_score"]))
            provenance = self._match_provenance(best_match)
            next_actions = []
            reasoning_trace.append(
                self._trace_step(
                    "decision_gate",
                    "Chot approve vi co duong evidence ontology ho tro ro rang cho dich vu.",
                    details={
                        "decision": "approve",
                        "grounded": True,
                        "decision_rule": "supported_service_match",
                        "best_match": self._match_trace_summary(best_match),
                        "next_actions": [],
                    },
                )
            )
            return self._build_line_result(
                line=line,
                service_info=service_info,
                context=context,
                active_diseases=active_diseases,
                coverage_ratio=coverage_ratio,
                matches=matches,
                contraindication_matches=contraindication_matches,
                result_matches=matches[:5],
                decision="approve",
                confidence=round(confidence, 4),
                reasoning_vi=(
                    f"Hop ly theo ontology: '{as_text(line.service_name_raw)}' khop voi "
                    f"'{best_match['target_service_name']}' cho benh '{best_match['disease_name']}' "
                    f"(role={best_match['role'] or 'diagnostic'}, source={best_match['support_source']})"
                    + (f" [{provenance}]." if provenance else ".")
                ),
                decision_rule="supported_service_match",
                grounded=True,
                next_actions=next_actions,
                reasoning_trace=reasoning_trace,
            )

        if active_diseases and coverage_ratio >= 0.45:
            known_mode = context.get("mode") == "known_disease"
            max_disease_conf = max(float(item.get("confidence") or 0.0) for item in active_diseases)
            if known_mode or max_disease_conf >= 0.62:
                confidence = 0.78 if known_mode else 0.67
                next_actions = [
                    "Neu nghi dich vu nay van hop ly, can kiem tra lai canonical mapping hoac re-ingest protocol disease lien quan.",
                ]
                reasoning_trace.append(
                    self._trace_step(
                        "decision_gate",
                    "Chot deny vi graph da co do phu benh active kha tot nhung khong tim thay evidence ho tro dich vu nay.",
                    details={
                        "decision": "deny",
                        "grounded": True,
                        "decision_rule": "covered_diseases_but_no_supported_service",
                        "best_contra_match": self._match_trace_summary(best_contra) if best_contra else None,
                        "max_disease_confidence": round(max_disease_conf, 4),
                        "next_actions": next_actions,
                    },
                    )
                )
                return self._build_line_result(
                    line=line,
                    service_info=service_info,
                    context=context,
                    active_diseases=active_diseases,
                    coverage_ratio=coverage_ratio,
                    matches=matches,
                    contraindication_matches=contraindication_matches,
                    result_matches=[],
                    decision="deny",
                    confidence=confidence,
                    reasoning_vi=(
                        f"Khong thay bang chung ontology cho dich vu '{as_text(line.service_name_raw)}' "
                        f"trong cac benh dang xet ({', '.join(as_text(item.get('disease_name')) for item in active_diseases[:3])})."
                    ),
                    decision_rule="covered_diseases_but_no_supported_service",
                    grounded=True,
                    next_actions=next_actions,
                    reasoning_trace=reasoning_trace,
                )

        next_actions = [
            "Bo sung known_diseases neu da biet benh cua khach hang.",
            "Tang ontology coverage cho benh active bang expected service va assertion edges.",
            "Kiem tra lai service mapping neu ten dich vu dang qua mo ho hoac chua canonicalize.",
        ]
        reasoning_trace.append(
            self._trace_step(
                "decision_gate",
                "Giu ket qua uncertain vi ontology coverage hoac do manh evidence chua du de ket luan.",
                details={
                    "decision": "uncertain",
                    "grounded": False,
                    "decision_rule": "insufficient_ontology_coverage",
                    "next_actions": next_actions,
                },
            )
        )
        return self._build_line_result(
            line=line,
            service_info=service_info,
            context=context,
            active_diseases=active_diseases,
            coverage_ratio=coverage_ratio,
            matches=matches,
            contraindication_matches=contraindication_matches,
            result_matches=matches[:5],
            decision="uncertain",
            confidence=0.45,
            reasoning_vi=(
                f"Chua du ontology evidence de ket luan cho dich vu '{as_text(line.service_name_raw)}'. "
                "Can them bang chung ve benh active hoac bo sung disease -> expected service."
            ),
            decision_rule="insufficient_ontology_coverage",
            grounded=False,
            next_actions=next_actions,
            reasoning_trace=reasoning_trace,
        )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _collect_case_signs(self, lines: list[ServiceLineInput]) -> list[str]:
        signs: list[str] = []
        for line in lines:
            signs.extend([as_text(item) for item in line.symptoms if as_text(item)])
            if as_text(line.admission_reason):
                signs.extend(self._decompose_text(line.admission_reason, limit=6))

        if not signs:
            for line in lines:
                if as_text(line.medical_history):
                    signs.extend(self._decompose_text(line.medical_history, limit=6))

        return dedupe_keep_order(signs)[:24]

    def _collect_disease_hints(self, lines: list[ServiceLineInput]) -> list[dict[str, str]]:
        hints: list[dict[str, str]] = []
        for line in lines:
            pieces = [as_text(piece) for piece in re.split(r"[;|/]", as_text(line.diagnosis_text)) if as_text(piece)]
            if not pieces and as_text(line.diagnosis_text):
                pieces = [as_text(line.diagnosis_text)]
            if not pieces and as_text(line.primary_icd):
                pieces = [as_text(line.primary_icd)]
            for piece in pieces:
                hints.append(
                    {
                        "hint_text": piece,
                        "icd10": as_text(line.primary_icd),
                    }
                )
        return hints

    def _decompose_text(self, text: str, limit: int = 6) -> list[str]:
        parts = self._decomposer.decompose(text)
        return dedupe_keep_order([text] + parts)[:limit]

    def _contains_disease(self, diseases: list[dict[str, Any]], candidate: dict[str, Any]) -> bool:
        candidate_id = as_text(candidate.get("disease_id"))
        candidate_name = ascii_fold(candidate.get("disease_name"))
        for item in diseases:
            if candidate_id and candidate_id == as_text(item.get("disease_id")):
                return True
            if candidate_name and candidate_name == ascii_fold(item.get("disease_name")):
                return True
        return False

    def _disease_catalog(self) -> list[dict[str, Any]]:
        if self._disease_catalog_cache is not None:
            return self._disease_catalog_cache

        self._disease_catalog_cache = [
            {
                "hypothesis_id": "",
                "disease_id": as_text(row.get("disease_id")),
                "disease_name": as_text(row.get("disease_name")),
                "icd10": "",
            }
            for row in self._inspector.list_diseases(self.namespace)
            if as_text(row.get("disease_id")) and as_text(row.get("disease_name"))
        ]
        return self._disease_catalog_cache

    def _schema_labels(self) -> set[str]:
        if self._label_cache is not None:
            return self._label_cache
        with self._inspector.driver.session() as session:
            records = session.run("CALL db.labels() YIELD label RETURN label")
            self._label_cache = {as_text(record["label"]) for record in records}
        return self._label_cache

    def _resolve_known_diseases(self, hints: list[dict[str, str]]) -> list[dict[str, Any]]:
        resolved: list[dict[str, Any]] = []
        for row in hints:
            match = self._best_disease_match(row.get("hint_text"), row.get("icd10"))
            if not match or float(match.get("match_score") or 0.0) < 0.48:
                continue
            resolved.append(
                {
                    **match,
                    "confidence": round(max(0.78, float(match.get("match_score") or 0.0)), 4),
                    "source": "known_disease",
                    "status": "confirmed",
                }
            )
        resolved.sort(key=lambda item: (float(item.get("confidence") or 0.0), item.get("disease_name")), reverse=True)
        deduped: list[dict[str, Any]] = []
        for item in resolved:
            if self._contains_disease(deduped, item):
                continue
            deduped.append(item)
        return deduped

    def _enrich_resolved_known_with_hypotheses(
        self,
        resolved_known: list[dict[str, Any]],
        resolved_from_hypotheses: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        enriched: list[dict[str, Any]] = []
        for disease in resolved_known:
            merged = dict(disease)
            for hypothesis in resolved_from_hypotheses:
                if text_overlap_score(merged.get("disease_name"), hypothesis.get("disease_name")) < 0.45:
                    continue
                if not merged.get("required_services"):
                    merged["required_services"] = hypothesis.get("required_services") or []
                if not merged.get("graph_context_snippets"):
                    merged["graph_context_snippets"] = hypothesis.get("graph_context_snippets") or []
                if float(hypothesis.get("confidence") or 0.0) > float(merged.get("confidence") or 0.0):
                    merged["confidence"] = hypothesis.get("confidence")
                break
            enriched.append(merged)
        return enriched

    def _resolve_hypothesis_diseases(self, hypotheses: list[dict[str, Any]]) -> list[dict[str, Any]]:
        resolved: list[dict[str, Any]] = []
        for hypothesis in hypotheses[:6]:
            match = self._best_disease_match(hypothesis.get("disease_name"), hypothesis.get("icd10"))
            if not match or float(match.get("match_score") or 0.0) < 0.42:
                continue
            resolved.append(
                {
                    **match,
                    "confidence": round(max(float(hypothesis.get("confidence") or 0.0), float(match.get("match_score") or 0.0)), 4),
                    "source": "hypothesis",
                    "status": as_text(hypothesis.get("status")) or "active",
                    "hypothesis_score": float(hypothesis.get("score") or 0.0),
                    "supporting_signs": hypothesis.get("supporting_signs") or [],
                    "required_services": hypothesis.get("required_services") or [],
                    "graph_context_snippets": hypothesis.get("graph_context_snippets") or [],
                }
            )
        resolved.sort(key=lambda item: (float(item.get("confidence") or 0.0), item.get("disease_name")), reverse=True)
        deduped: list[dict[str, Any]] = []
        for item in resolved:
            if self._contains_disease(deduped, item):
                continue
            deduped.append(item)
        return deduped

    def _best_disease_match(self, disease_name: Any, icd10: Any) -> dict[str, Any] | None:
        best: dict[str, Any] | None = None
        best_score = 0.0
        target_name = as_text(disease_name)
        target_icd = normalize_icd(icd10)
        target_group = icd_group(icd10)

        for row in self._disease_catalog():
            score = 0.0
            row_icd = normalize_icd(row.get("icd10"))
            if target_icd and row_icd == target_icd:
                score = max(score, 1.0)
            elif target_group and icd_group(row.get("icd10")) == target_group:
                score = max(score, 0.84)

            if target_name:
                score = max(score, text_overlap_score(target_name, row.get("disease_name")))

            if score > best_score:
                best_score = score
                best = {
                    "hypothesis_id": as_text(row.get("hypothesis_id")),
                    "disease_id": as_text(row.get("disease_id")),
                    "disease_name": as_text(row.get("disease_name")),
                    "icd10": as_text(row.get("icd10")),
                    "match_score": round(score, 4),
                }

        return best

    def _build_disease_packet(self, disease: dict[str, Any]) -> dict[str, Any]:
        disease_id = as_text(disease.get("disease_id"))
        if not disease_id:
            return {}

        expected_services = self._get_expected_services(disease_id)
        graph = self._get_disease_graph(disease_id)
        summary = graph.get("summary") or {}
        assertion_service_links = graph.get("assertion_service_links") or []
        contraindicated_services = graph.get("contraindicated_services") or []
        key_services = dedupe_keep_order([as_text(item) for item in summary.get("key_services") or [] if as_text(item)])
        service_mentions = dedupe_keep_order(
            [
                as_text(item.get("mention_text"))
                for item in graph.get("service_mentions") or []
                if as_text(item.get("mention_text"))
            ]
        )
        assertion_texts = dedupe_keep_order(
            [
                as_text(item.get("assertion_text")) or as_text(item.get("action_text"))
                for item in graph.get("assertions") or []
                if as_text(item.get("assertion_text")) or as_text(item.get("action_text"))
            ]
        )

        return {
            **disease,
            "expected_services": expected_services,
            "expected_service_count": len(expected_services),
            "assertion_service_links": assertion_service_links[:12],
            "assertion_service_count": len(assertion_service_links),
            "contraindicated_services": contraindicated_services[:12],
            "contraindicated_service_count": len(contraindicated_services),
            "required_services": dedupe_keep_order([as_text(item) for item in disease.get("required_services") or [] if as_text(item)])[:12],
            "key_services": key_services[:12],
            "key_service_count": len(key_services),
            "service_mentions": service_mentions[:12],
            "service_mention_count": len(service_mentions),
            "assertion_texts": assertion_texts[:12],
            "graph_context_snippets": dedupe_keep_order(
                [as_text(item) for item in disease.get("graph_context_snippets") or [] if as_text(item)]
            )[:8],
            "summary_text": as_text(summary.get("summary_text"))[:280],
        }

    def _get_expected_services(self, disease_id: str) -> list[dict[str, Any]]:
        if disease_id in self._expected_service_cache:
            return self._expected_service_cache[disease_id]

        labels = self._schema_labels()
        if "DiseaseHypothesis" not in labels:
            self._expected_service_cache[disease_id] = []
            return []

        with self._inspector.driver.session() as session:
            records = session.run(
                """
                MATCH (h:DiseaseHypothesis)
                OPTIONAL MATCH (h)-[:HYPOTHESIS_FOR_DISEASE]->(d:DiseaseEntity)
                WITH h, d, coalesce(d.namespace, h.namespace, $ns) AS resolved_namespace
                WHERE resolved_namespace = $ns
                  AND coalesce(d.disease_id, h.disease_id) = $disease_id
                MATCH (h)-[r:DISEASE_EXPECTS_SERVICE]->(s)
                WHERE s:ProtocolService OR s:CIService
                RETURN s.service_code AS service_code,
                       coalesce(s.service_name, s.name) AS service_name,
                       r.role AS role,
                       coalesce(r.category_code, s.category_code, '') AS category_code
                ORDER BY role, service_name
                """,
                ns=self.namespace,
                disease_id=disease_id,
            )
            rows = [dict(record) for record in records]
        self._expected_service_cache[disease_id] = rows
        return rows

    def _get_disease_graph(self, disease_id: str) -> dict[str, Any]:
        if disease_id in self._disease_graph_cache:
            return self._disease_graph_cache[disease_id]
        graph: dict[str, Any] = {
            "summary": {},
            "service_mentions": [],
            "assertions": [],
            "assertion_service_links": [],
            "contraindicated_services": [],
        }
        labels = self._schema_labels()
        with self._inspector.driver.session() as session:
            if "ProtocolDiseaseSummary" in labels:
                row = session.run(
                    """
                    MATCH (s:ProtocolDiseaseSummary {namespace:$ns})-[:SUMMARIZES]->(:DiseaseEntity {disease_id:$disease_id})
                    RETURN s.summary_text AS summary_text,
                           coalesce(s.key_services, []) AS key_services
                    LIMIT 1
                    """,
                    ns=self.namespace,
                    disease_id=disease_id,
                ).single()
                if row:
                    graph["summary"] = dict(row)

            if "RawServiceMention" in labels:
                graph["service_mentions"] = [
                    dict(record)
                    for record in session.run(
                        """
                        MATCH (c:RawChunk {namespace:$ns})-[:CHUNK_ABOUT_DISEASE]->(:DiseaseEntity {disease_id:$disease_id})
                        MATCH (c)-[:MENTIONS_SERVICE]->(m:RawServiceMention {namespace:$ns})
                        RETURN DISTINCT m.mention_text AS mention_text,
                               m.medical_role AS medical_role
                        ORDER BY mention_text
                        """,
                        ns=self.namespace,
                        disease_id=disease_id,
                    )
                ]

            if "ProtocolAssertion" in labels:
                graph["assertions"] = [
                    dict(record)
                    for record in session.run(
                        """
                        MATCH (a:ProtocolAssertion {namespace:$ns})-[:ASSERTION_ABOUT_DISEASE]->(:DiseaseEntity {disease_id:$disease_id})
                        OPTIONAL MATCH (sec:ProtocolSection)-[:CONTAINS_ASSERTION]->(a)
                        OPTIONAL MATCH (book:ProtocolBook)-[:BOOK_HAS_SECTION]->(sec)
                        OPTIONAL MATCH (chunk:RawChunk {chunk_id:a.source_chunk_id})
                        OPTIONAL MATCH (chunk)-[:FROM_DOCUMENT]->(doc:RawDocument)
                        OPTIONAL MATCH (a)-[:ASSERTION_INDICATES_SERVICE]->(svc)
                        WITH a, sec, book, doc, collect(DISTINCT coalesce(svc.service_name, svc.name, svc.service_code)) AS related_services
                        OPTIONAL MATCH (a)-[:ASSERTION_CONTRAINDICATES]->(csvc)
                        WITH a, sec, book, doc, related_services, collect(DISTINCT coalesce(csvc.service_name, csvc.name, csvc.service_code)) AS contraindicated_services
                        RETURN DISTINCT a.assertion_text AS assertion_text,
                               a.action_text AS action_text,
                               a.assertion_id AS assertion_id,
                               a.assertion_type AS assertion_type,
                               a.condition_text AS condition_text,
                               a.evidence_level AS evidence_level,
                               a.source_page AS source_page,
                               sec.section_title AS section_title,
                               book.book_name AS book_name,
                               doc.title AS doc_title,
                               related_services,
                               contraindicated_services
                        ORDER BY assertion_text
                        """,
                        ns=self.namespace,
                        disease_id=disease_id,
                    )
                ]

            if "ProtocolAssertion" in labels and ("ProtocolService" in labels or "CIService" in labels):
                graph["assertion_service_links"] = [
                    dict(record)
                    for record in session.run(
                        """
                        MATCH (a:ProtocolAssertion {namespace:$ns})-[:ASSERTION_ABOUT_DISEASE]->(:DiseaseEntity {disease_id:$disease_id})
                        MATCH (a)-[:ASSERTION_INDICATES_SERVICE]->(svc)
                        WHERE svc:ProtocolService OR svc:CIService
                        OPTIONAL MATCH (sec:ProtocolSection)-[:CONTAINS_ASSERTION]->(a)
                        OPTIONAL MATCH (book:ProtocolBook)-[:BOOK_HAS_SECTION]->(sec)
                        OPTIONAL MATCH (chunk:RawChunk {chunk_id:a.source_chunk_id})
                        OPTIONAL MATCH (chunk)-[:FROM_DOCUMENT]->(doc:RawDocument)
                        RETURN DISTINCT
                               svc.service_code AS service_code,
                               coalesce(svc.service_name, svc.name) AS service_name,
                               a.assertion_id AS assertion_id,
                               a.assertion_type AS assertion_type,
                               a.condition_text AS condition_text,
                               a.action_text AS action_text,
                               a.evidence_level AS evidence_level,
                               a.status AS status,
                               a.source_chunk_id AS source_chunk_id,
                               a.source_page AS source_page,
                               sec.section_title AS section_title,
                               book.book_name AS book_name,
                               doc.title AS doc_title,
                               doc.file_path AS doc_file_path
                        ORDER BY service_name, assertion_id
                        """,
                        ns=self.namespace,
                        disease_id=disease_id,
                    )
                ]
                graph["contraindicated_services"] = [
                    dict(record)
                    for record in session.run(
                        """
                        MATCH (a:ProtocolAssertion {namespace:$ns})-[:ASSERTION_ABOUT_DISEASE]->(:DiseaseEntity {disease_id:$disease_id})
                        MATCH (a)-[:ASSERTION_CONTRAINDICATES]->(svc)
                        WHERE svc:ProtocolService OR svc:CIService
                        OPTIONAL MATCH (sec:ProtocolSection)-[:CONTAINS_ASSERTION]->(a)
                        OPTIONAL MATCH (book:ProtocolBook)-[:BOOK_HAS_SECTION]->(sec)
                        OPTIONAL MATCH (chunk:RawChunk {chunk_id:a.source_chunk_id})
                        OPTIONAL MATCH (chunk)-[:FROM_DOCUMENT]->(doc:RawDocument)
                        RETURN DISTINCT
                               svc.service_code AS service_code,
                               coalesce(svc.service_name, svc.name) AS service_name,
                               a.assertion_id AS assertion_id,
                               a.assertion_type AS assertion_type,
                               a.condition_text AS condition_text,
                               a.action_text AS action_text,
                               a.evidence_level AS evidence_level,
                               a.status AS status,
                               a.source_chunk_id AS source_chunk_id,
                               a.source_page AS source_page,
                               sec.section_title AS section_title,
                               book.book_name AS book_name,
                               doc.title AS doc_title,
                               doc.file_path AS doc_file_path
                        ORDER BY service_name, assertion_id
                        """,
                        ns=self.namespace,
                        disease_id=disease_id,
                    )
                ]
        self._disease_graph_cache[disease_id] = graph
        return graph

    def _line_service_candidates(
        self,
        line: ServiceLineInput,
        service_info: dict[str, Any],
        disease: dict[str, Any],
    ) -> tuple[list[str], list[str], float]:
        service_name_raw = as_text(line.service_name_raw)
        candidate_codes = [
            as_text(service_info.get("service_code")),
            as_text(service_info.get("suggested_service_code")),
        ]
        candidate_codes.extend(
            [
                as_text(item.get("service_code"))
                for item in service_info.get("top_candidates") or []
                if as_text(item.get("service_code"))
            ]
        )
        candidate_codes = [code for code in dedupe_keep_order(candidate_codes) if code]
        candidate_names = dedupe_keep_order(
            [
                service_name_raw,
                as_text(service_info.get("canonical_name")),
                as_text(service_info.get("suggested_canonical_name")),
            ]
        )
        disease_confidence = float(disease.get("confidence") or 0.0)
        disease_factor = 0.65 + (0.35 * min(max(disease_confidence, 0.0), 1.0))
        return candidate_codes, candidate_names, disease_factor

    def _match_line_to_disease(
        self,
        line: ServiceLineInput,
        service_info: dict[str, Any],
        disease: dict[str, Any],
    ) -> list[dict[str, Any]]:
        matches: list[dict[str, Any]] = []
        candidate_codes, candidate_names, disease_factor = self._line_service_candidates(line, service_info, disease)

        for row in disease.get("assertion_service_links") or []:
            matched = self._score_service_candidate(
                candidate_codes=candidate_codes,
                candidate_names=candidate_names,
                target_code=as_text(row.get("service_code")),
                target_name=as_text(row.get("service_name")),
                target_category="",
                support_source="assertion_indicates_service",
                role=self._assertion_role(row.get("assertion_type")),
                disease=disease,
                disease_factor=disease_factor,
                polarity="support",
                extra={
                    "assertion_id": as_text(row.get("assertion_id")),
                    "assertion_type": as_text(row.get("assertion_type")),
                    "condition_text": as_text(row.get("condition_text")),
                    "action_text": as_text(row.get("action_text")),
                    "evidence_level": as_text(row.get("evidence_level")),
                    "source_page": row.get("source_page"),
                    "section_title": as_text(row.get("section_title")),
                    "book_name": as_text(row.get("book_name")),
                    "doc_title": as_text(row.get("doc_title")),
                    "doc_file_path": as_text(row.get("doc_file_path")),
                },
            )
            if matched:
                matches.append(matched)

        for row in disease.get("expected_services") or []:
            matched = self._score_service_candidate(
                candidate_codes=candidate_codes,
                candidate_names=candidate_names,
                target_code=as_text(row.get("service_code")),
                target_name=as_text(row.get("service_name")),
                target_category=as_text(row.get("category_code")),
                support_source="expected_service",
                role=as_text(row.get("role")) or "diagnostic",
                disease=disease,
                disease_factor=disease_factor,
                polarity="support",
            )
            if matched:
                matches.append(matched)

        for service_name in disease.get("required_services") or []:
            matched = self._score_service_candidate(
                candidate_codes=candidate_codes,
                candidate_names=candidate_names,
                target_code="",
                target_name=as_text(service_name),
                target_category="",
                support_source="required_service",
                role="diagnostic",
                disease=disease,
                disease_factor=disease_factor,
                polarity="support",
            )
            if matched:
                matches.append(matched)

        for service_name in disease.get("key_services") or []:
            matched = self._score_service_candidate(
                candidate_codes=candidate_codes,
                candidate_names=candidate_names,
                target_code="",
                target_name=as_text(service_name),
                target_category="",
                support_source="key_service",
                role="diagnostic",
                disease=disease,
                disease_factor=disease_factor,
                polarity="support",
            )
            if matched:
                matches.append(matched)

        for service_name in disease.get("service_mentions") or []:
            matched = self._score_service_candidate(
                candidate_codes=candidate_codes,
                candidate_names=candidate_names,
                target_code="",
                target_name=as_text(service_name),
                target_category="",
                support_source="service_mention",
                role="diagnostic",
                disease=disease,
                disease_factor=disease_factor,
                polarity="support",
            )
            if matched:
                matches.append(matched)

        for snippet in disease.get("graph_context_snippets") or []:
            matched = self._score_service_candidate(
                candidate_codes=candidate_codes,
                candidate_names=candidate_names,
                target_code="",
                target_name=as_text(snippet),
                target_category="",
                support_source="graph_context_snippet",
                role="diagnostic",
                disease=disease,
                disease_factor=disease_factor,
                polarity="support",
            )
            if matched:
                matches.append(matched)

        for snippet in disease.get("assertion_texts") or []:
            matched = self._score_service_candidate(
                candidate_codes=candidate_codes,
                candidate_names=candidate_names,
                target_code="",
                target_name=as_text(snippet),
                target_category="",
                support_source="assertion_snippet",
                role="diagnostic",
                disease=disease,
                disease_factor=disease_factor,
                polarity="support",
            )
            if matched:
                matches.append(matched)

        matches.sort(key=lambda item: item["support_score"], reverse=True)
        return matches[:6]

    def _match_line_to_contraindications(
        self,
        line: ServiceLineInput,
        service_info: dict[str, Any],
        disease: dict[str, Any],
    ) -> list[dict[str, Any]]:
        matches: list[dict[str, Any]] = []
        candidate_codes, candidate_names, disease_factor = self._line_service_candidates(line, service_info, disease)

        for row in disease.get("contraindicated_services") or []:
            matched = self._score_service_candidate(
                candidate_codes=candidate_codes,
                candidate_names=candidate_names,
                target_code=as_text(row.get("service_code")),
                target_name=as_text(row.get("service_name")),
                target_category="",
                support_source="assertion_contraindicates_service",
                role="contraindication",
                disease=disease,
                disease_factor=disease_factor,
                polarity="contraindication",
                extra={
                    "assertion_id": as_text(row.get("assertion_id")),
                    "assertion_type": as_text(row.get("assertion_type")),
                    "condition_text": as_text(row.get("condition_text")),
                    "action_text": as_text(row.get("action_text")),
                    "evidence_level": as_text(row.get("evidence_level")),
                    "source_page": row.get("source_page"),
                    "section_title": as_text(row.get("section_title")),
                    "book_name": as_text(row.get("book_name")),
                    "doc_title": as_text(row.get("doc_title")),
                    "doc_file_path": as_text(row.get("doc_file_path")),
                },
            )
            if matched:
                matches.append(matched)

        matches.sort(key=lambda item: item["support_score"], reverse=True)
        return matches[:6]

    def _score_service_candidate(
        self,
        *,
        candidate_codes: list[str],
        candidate_names: list[str],
        target_code: str,
        target_name: str,
        target_category: str,
        support_source: str,
        role: str,
        disease: dict[str, Any],
        disease_factor: float,
        polarity: str = "support",
        extra: dict[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        raw_score = 0.0
        if target_code and target_code in candidate_codes:
            raw_score = 1.0
        if not raw_score:
            for name in candidate_names:
                raw_score = max(raw_score, text_overlap_score(name, target_name))
        if not raw_score and target_category:
            service_info = self._service_mapper.recognize_service(candidate_names[0] if candidate_names else "")
            if as_text(service_info.get("category_code")) == target_category:
                raw_score = 0.34

        if raw_score < 0.34:
            return None

        support_score = raw_score * role_weight(role) * source_weight(support_source) * disease_factor
        payload = {
            "disease_id": as_text(disease.get("disease_id")),
            "disease_name": as_text(disease.get("disease_name")),
            "disease_confidence": round(float(disease.get("confidence") or 0.0), 4),
            "target_service_code": target_code,
            "target_service_name": target_name,
            "support_source": support_source,
            "polarity": polarity,
            "role": role,
            "raw_match_score": round(raw_score, 4),
            "support_score": round(min(support_score, 0.99), 4),
        }
        if extra:
            payload.update({key: value for key, value in extra.items() if value not in (None, "", [])})
        return payload
