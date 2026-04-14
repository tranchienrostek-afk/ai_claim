"""Contract/Policy Agent — evaluates insurance contract eligibility.

Wraps the offline ContractClauseStep2 engine (contract rules, exclusion packs,
benefit mapping, cross-contract risk priors) and maps its decisions into the
unified AgentVerdict format.
"""

from __future__ import annotations

import logging
from typing import Any

from .engine_bridge import get_contract_clause_engine
from .models import AgentVerdict, EvidenceItem, ServiceLineInput

logger = logging.getLogger(__name__)

# Map ContractClauseStep2 decisions -> AgentVerdict decisions
_DECISION_MAP: dict[str, str] = {
    "contract_clear": "approve",
    "contract_review_not_covered_sensitive": "deny",
    "contract_review_screening_sensitive": "deny",
    "contract_review_admin_or_clause_sensitive": "review",
    "contract_partial_pay_sensitive": "partial_pay",
    "contract_unknown": "uncertain",
    "contract_unknown_with_screening_prior": "review",
    "contract_unknown_with_not_covered_prior": "deny",
    "contract_unknown_with_partial_pay_prior": "partial_pay",
}

_CONFIDENCE_MAP: dict[str, float] = {
    "contract_clear": 0.85,
    "contract_review_not_covered_sensitive": 0.75,
    "contract_review_screening_sensitive": 0.70,
    "contract_review_admin_or_clause_sensitive": 0.60,
    "contract_partial_pay_sensitive": 0.65,
    "contract_unknown": 0.30,
    "contract_unknown_with_screening_prior": 0.55,
    "contract_unknown_with_not_covered_prior": 0.60,
    "contract_unknown_with_partial_pay_prior": 0.55,
}


class ContractAgent:
    """Specialist agent for contract coverage evaluation."""

    agent_name: str = "contract"

    def __init__(self) -> None:
        self._engine = get_contract_clause_engine()

    def assess(
        self,
        line: ServiceLineInput,
        service_info: dict[str, Any],
        medical_status: str = "uncertain",
    ) -> AgentVerdict:
        """Evaluate whether *line* is covered by the insurance contract.

        Parameters
        ----------
        line : ServiceLineInput
            The service line to evaluate.
        service_info : dict
            Output from ClinicalAgent.recognize_service() (service_code, category_code, etc.).
        medical_status : str
            The clinical agent's medical_necessity_status (used by contract screening logic).
        """
        # Build a row dict compatible with ContractClauseStep2.assess_row()
        row: dict[str, Any] = {
            "recognized_service": service_info,
            "service_name_raw": line.service_name_raw,
            "step1_clinical_necessity": {
                "medical_necessity_status": medical_status,
            },
        }

        # Attach contract identification fields
        if line.contract_id:
            row["contract_name"] = line.contract_id
        if line.insurer:
            row["insurer"] = line.insurer

        result = self._engine.assess_row(row)

        step2_decision = result.get("decision", "contract_unknown")
        agent_decision = _DECISION_MAP.get(step2_decision, "uncertain")
        confidence = _CONFIDENCE_MAP.get(step2_decision, 0.40)

        # Build evidence
        evidence: list[EvidenceItem] = []
        flags: list[str] = []

        # Contract resolution evidence
        contract_res = result.get("contract_resolution", {})
        contract_status = contract_res.get("status", "contract_unknown")
        if contract_res.get("contract_name"):
            evidence.append(EvidenceItem(
                source="contract_resolution",
                key=contract_res["contract_name"],
                value=f"Contract resolved via {contract_status}",
                weight=0.8 if contract_status == "provided_in_row" else 0.5,
            ))
            flags.append(f"contract_{contract_status}")
        else:
            flags.append("no_contract_attached")

        # Risk flags
        risk_flags = result.get("contract_risk_flags", {})
        for risk_name, enabled in risk_flags.items():
            if enabled:
                flags.append(f"risk_{risk_name}")
                evidence.append(EvidenceItem(
                    source="contract_risk_profile",
                    key=risk_name,
                    value=f"Contract risk flag: {risk_name}",
                    weight=0.6,
                ))

        # Cross-contract prior evidence
        cross_prior = result.get("cross_contract_prior", {})
        if cross_prior.get("available"):
            top_reasons = cross_prior.get("top_reasons", [])
            if top_reasons:
                reasons_text = "; ".join(
                    r.get("atomic_reason", "?") for r in top_reasons[:3]
                )
                evidence.append(EvidenceItem(
                    source="cross_contract_prior",
                    key=service_info.get("service_code", ""),
                    value=f"Historical reasons: {reasons_text}",
                    weight=0.5,
                ))

        # Benefit hints
        benefit_hints = result.get("benefit_hints", [])
        if benefit_hints:
            evidence.append(EvidenceItem(
                source="benefit_hints",
                key=contract_res.get("contract_name", ""),
                value=f"Benefit labels: {', '.join(benefit_hints[:3])}",
                weight=0.4,
            ))

        reason_vi = result.get("reason", "")

        return AgentVerdict(
            agent_name=self.agent_name,
            decision=agent_decision,
            confidence=confidence,
            evidence=evidence,
            flags=flags,
            reasoning_vi=reason_vi,
        )
