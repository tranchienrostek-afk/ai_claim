"""Adjudicator Agent — orchestrates clinical, contract, and anomaly agents.

This is the central orchestrator that:
1. Fans out each service line to the three specialist agents
2. Merges their verdicts using a deterministic conflict resolution matrix
3. Returns the final adjudication result with full audit trace

The conflict resolution is rule-based (no LLM) to ensure determinism and
auditability. The key accuracy fix is Rule #4: double-uncertainty = deny.
"""

from __future__ import annotations

import logging
import time
from typing import Any

from .anomaly_agent import AnomalyAgent
from . import canonical_resolver
from .clinical_agent import ClinicalAgent
from .contract_agent import ContractAgent
# Neo4j-powered contract agent option
try:
    from .contract_agent_neo4j import ContractAgentNeo4j, get_neo4j_contract_agent
    NEO4J_CONTRACT_AVAILABLE = True
except ImportError:
    NEO4J_CONTRACT_AVAILABLE = False
    ContractAgentNeo4j = None
    get_neo4j_contract_agent = None

from .models import (
    AdjudicationResult,
    AgentVerdict,
    EvidenceItem,
    MultiAgentAdjudicateRequest,
    MultiAgentAdjudicateResponse,
    ServiceLineInput,
)

logger = logging.getLogger(__name__)

# Flag to enable/disable Neo4j contract agent
USE_NEO4J_CONTRACT_AGENT = True  # Set to False to use file-based agent


class AdjudicatorAgent:
    """Multiagent orchestrator for insurance claim adjudication."""

    def __init__(self, use_neo4j: bool = None) -> None:
        if use_neo4j is None:
            use_neo4j = USE_NEO4J_CONTRACT_AGENT and NEO4J_CONTRACT_AVAILABLE

        self.clinical = ClinicalAgent()
        if use_neo4j and NEO4J_CONTRACT_AVAILABLE:
            self.contract = get_neo4j_contract_agent()
            logger.info("Using Neo4j-powered Contract Agent")
        else:
            self.contract = ContractAgent()
            logger.info("Using file-based Contract Agent")
        self.anomaly = AnomalyAgent()

    def adjudicate_claim(
        self, request: MultiAgentAdjudicateRequest
    ) -> MultiAgentAdjudicateResponse:
        """Process a full claim through the multiagent system."""
        t0 = time.time()

        # Apply claim-level defaults to each service line
        lines = request.service_lines
        for line in lines:
            if not line.contract_id and request.contract_id:
                line.contract_id = request.contract_id
            if not line.insurer and request.insurer:
                line.insurer = request.insurer
            if request.symptoms:
                merged_symptoms = list(line.symptoms) + [item for item in request.symptoms if item not in line.symptoms]
                line.symptoms = merged_symptoms
            if not line.medical_history and request.medical_history:
                line.medical_history = request.medical_history
            if not line.admission_reason and request.admission_reason:
                line.admission_reason = request.admission_reason
            if not line.diagnosis_text and request.known_diseases:
                line.diagnosis_text = "; ".join(request.known_diseases)

        clinical_case_context = self.clinical.prepare_case(lines)

        results: list[AdjudicationResult] = []
        for line in lines:
            result = self._adjudicate_line(line, lines, clinical_case_context)
            results.append(result)

        # Build summary
        decision_counts: dict[str, int] = {}
        for r in results:
            decision_counts[r.final_decision] = decision_counts.get(r.final_decision, 0) + 1

        approve_count = decision_counts.get("approve", 0)
        deny_count = decision_counts.get("deny", 0)
        review_count = decision_counts.get("review", 0)
        partial_count = decision_counts.get("partial_pay", 0)
        total = len(results)

        summary_vi = (
            f"Ket qua tham dinh {total} dich vu: "
            f"{approve_count} chap thuan, {deny_count} tu choi, "
            f"{review_count} can xem xet, {partial_count} chi tra mot phan."
        )

        elapsed_ms = round((time.time() - t0) * 1000, 1)

        return MultiAgentAdjudicateResponse(
            claim_id=request.claim_id,
            results=results,
            summary_vi=summary_vi,
            meta={
                "decision_counts": decision_counts,
                "total_lines": total,
                "elapsed_ms": elapsed_ms,
                "clinical_case_context": clinical_case_context,
            },
        )

    def _adjudicate_line(
        self,
        line: ServiceLineInput,
        all_lines: list[ServiceLineInput],
        clinical_case_context: dict[str, Any] | None = None,
    ) -> AdjudicationResult:
        """Run the three specialist agents and resolve conflicts."""

        # 1. Clinical agent (also provides service recognition for others)
        clinical_verdict = self.clinical.assess(line, case_context=clinical_case_context)
        service_info = self.clinical.recognize_service(line.service_name_raw)

        # 1b. Enrich with CanonicalService (BYT MAANHXA) — adds byt_price,
        #     taxonomy, and canonical code when available. Falls back gracefully
        #     if Neo4j is down (returns service_info unchanged).
        maanhxa_from_claim = getattr(line, "maanhxa", None)
        service_info = canonical_resolver.resolve_and_enrich(
            service_info, maanhxa_from_claim=maanhxa_from_claim,
        )
        ontology_meta = ((clinical_verdict.meta or {}).get("ontology") or {}).get("meta") or {}
        service_info["medical_context"] = {
            "active_diseases": ontology_meta.get("active_diseases") or (clinical_case_context or {}).get("active_diseases") or [],
            "input_signs": ontology_meta.get("input_signs") or (clinical_case_context or {}).get("input_signs") or [],
            "clinical_decision": clinical_verdict.decision,
            "clinical_reasoning": clinical_verdict.reasoning_vi,
        }

        # Derive medical_status for contract agent
        medical_status = "uncertain"
        if clinical_verdict.decision == "approve":
            medical_status = "supported_by_final_diagnosis"
        elif clinical_verdict.decision == "deny":
            medical_status = "not_medically_supported"

        # 2. Contract agent
        contract_verdict = self.contract.assess(line, service_info, medical_status)

        # 3. Anomaly agent
        anomaly_verdict = self.anomaly.assess(line, service_info, all_lines)

        # 4. Conflict resolution
        final_decision, resolution_rule, confidence = self._resolve(
            clinical_verdict, contract_verdict, anomaly_verdict
        )

        # 5. Merge evidence
        all_evidence: list[EvidenceItem] = []
        for v in (clinical_verdict, contract_verdict, anomaly_verdict):
            all_evidence.extend(v.evidence)

        # 6. Build reasoning
        reasoning_parts = []
        for v in (clinical_verdict, contract_verdict, anomaly_verdict):
            if v.reasoning_vi:
                reasoning_parts.append(f"[{v.agent_name}] {v.reasoning_vi}")
        reasoning_vi = " | ".join(reasoning_parts)

        return AdjudicationResult(
            service_name=line.service_name_raw,
            recognized_service_code=service_info.get("service_code", ""),
            recognized_canonical_name=service_info.get("canonical_name", ""),
            medical_decision=clinical_verdict.decision,
            medical_confidence=clinical_verdict.confidence,
            medical_reasoning_vi=clinical_verdict.reasoning_vi,
            final_decision=final_decision,
            confidence=confidence,
            agent_verdicts={
                "clinical": clinical_verdict,
                "contract": contract_verdict,
                "anomaly": anomaly_verdict,
            },
            reasoning_vi=reasoning_vi,
            evidence_summary=all_evidence[:10],  # top 10 for response size
            resolution_rule=resolution_rule,
            canonical_enrichment={
                "maanhxa": service_info.get("canonical_maanhxa"),
                "canonical_name_byt": service_info.get("canonical_name_byt"),
                "canonical_byt_price": service_info.get("canonical_byt_price"),
                "classification": service_info.get("canonical_classification"),
                "resolve_method": service_info.get("canonical_resolve_method"),
                "resolve_confidence": service_info.get("canonical_resolve_confidence"),
            },
        )

    def _resolve(
        self,
        clinical: AgentVerdict,
        contract: AgentVerdict,
        anomaly: AgentVerdict,
    ) -> tuple[str, str, float]:
        """Deterministic conflict resolution matrix.

        Returns (final_decision, resolution_rule_name, confidence).

        Rules are evaluated in priority order:
        1. Hard deny: any agent denies with confidence >= 0.7
        2. Anomaly override: anomaly flags present -> review (unless clinical strongly approves)
        3. Contract partial pay
        4. Double uncertainty = deny (CRITICAL FIX for approval bias)
        5. Both approve = approve
        6. Fallback = review
        """

        # Rule 1: Hard deny from any agent with high confidence
        for agent_verdict in (clinical, contract, anomaly):
            if agent_verdict.decision == "deny" and agent_verdict.confidence >= 0.70:
                return (
                    "deny",
                    f"rule_1_hard_deny_from_{agent_verdict.agent_name}",
                    agent_verdict.confidence,
                )

        # Rule 1b: Clinical deny even with lower confidence
        if clinical.decision == "deny":
            return (
                "deny",
                "rule_1b_clinical_deny",
                clinical.confidence,
            )

        # Rule 1c: Contract deny even with lower confidence
        if contract.decision == "deny":
            return (
                "deny",
                "rule_1c_contract_deny",
                contract.confidence,
            )

        # Rule 2: Anomaly flags -> review (unless clinical strongly approves)
        if anomaly.decision == "review" and anomaly.flags:
            if clinical.decision == "approve" and clinical.confidence >= 0.80:
                # Strong clinical approval overrides minor anomalies
                return (
                    "approve",
                    "rule_2_anomaly_overridden_by_strong_clinical",
                    min(clinical.confidence, 0.75),  # slightly reduced
                )
            return (
                "review",
                "rule_2_anomaly_flags",
                0.55,
            )

        # Rule 3: Contract says partial pay
        if contract.decision == "partial_pay":
            return (
                "partial_pay",
                "rule_3_contract_partial_pay",
                contract.confidence,
            )

        # Rule 4: CRITICAL FIX — double uncertainty = deny
        # This is the main accuracy lever. The old system treated this as approve.
        if clinical.decision == "uncertain" and contract.decision in ("uncertain", "review"):
            return (
                "deny",
                "rule_4_double_uncertainty_deny",
                0.55,
            )

        if clinical.decision == "uncertain" and contract.decision == "approve":
            # Only approve if clinical has some evidence (not pure fallback)
            # AND contract is strongly clear. Pure fallback uncertain (0.55)
            # should NOT be approved just because contract is clear.
            # Only approve when clinical is not pure fallback (>0.55 means
            # some matrix evidence exists, not the default uncertain).
            has_matrix_evidence = "fallback_uncertain" not in clinical.flags
            if has_matrix_evidence and contract.confidence >= 0.80:
                return (
                    "approve",
                    "rule_4b_contract_approve_with_clinical_evidence",
                    min(clinical.confidence, contract.confidence) * 0.7,
                )
            return (
                "review",
                "rule_4b_clinical_uncertain_contract_approve",
                0.50,
            )

        # Rule 5: Both approve -> approve
        if clinical.decision == "approve" and contract.decision == "approve":
            return (
                "approve",
                "rule_5_both_approve",
                min(clinical.confidence, contract.confidence),
            )

        # Rule 5b: Clinical approve, contract uncertain -> review
        if clinical.decision == "approve" and contract.decision == "uncertain":
            return (
                "review",
                "rule_5b_clinical_approve_contract_uncertain",
                0.55,
            )

        # Rule 6: Fallback -> review
        return (
            "review",
            "rule_6_fallback_review",
            0.40,
        )
