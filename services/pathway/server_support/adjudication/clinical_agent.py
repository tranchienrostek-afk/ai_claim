"""Clinical Reasoning Agent — evaluates medical necessity of each service line.

Wraps the offline AdjudicationMVP engine (service-disease matrix, BHYT mapping)
and applies the critical fix: uncertain/fallback results are NOT treated as approve.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from .engine_bridge import get_adjudication_mvp
from .ontology_reasoner import OntologyClinicalReasoner
from .models import AgentVerdict, EvidenceItem, ServiceLineInput

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)

# Threshold below which a fallback-approve becomes "uncertain"
_FALLBACK_CONFIDENCE_THRESHOLD = 0.65
_FALLBACK_REASON_MARKER = "No direct matrix evidence"


class ClinicalAgent:
    """Specialist agent for clinical necessity assessment."""

    agent_name: str = "clinical"

    def __init__(self) -> None:
        self._mvp = get_adjudication_mvp()
        self._ontology = OntologyClinicalReasoner()

    # ------------------------------------------------------------------
    # Public
    # ------------------------------------------------------------------

    def recognize_service(self, service_name_raw: str) -> dict:
        """Delegate to AdjudicationMVP.recognize_service (fuzzy text -> code)."""
        return self._mvp.recognize_service(service_name_raw)

    def prepare_case(self, lines: list[ServiceLineInput]) -> dict[str, Any]:
        """Build shared ontology-first case context once per claim."""
        return self._ontology.prepare_case(lines)

    def assess(
        self,
        line: ServiceLineInput,
        case_context: dict[str, Any] | None = None,
    ) -> AgentVerdict:
        """Evaluate whether *line* is medically necessary.

        Returns an AgentVerdict with:
          - approve  if matrix evidence supports the service-diagnosis pair
          - deny     if the service is clearly unrelated (pathogen safeguard)
          - uncertain if there is no matrix evidence (fallback)
        """
        service_info = self.recognize_service(line.service_name_raw)
        ontology_result = self._ontology.assess_line(line, case_context)
        clinical = self._mvp.assess_clinical_necessity(
            service_name=line.service_name_raw,
            service_info=service_info,
            diagnosis=line.diagnosis_text,
            icd_code=line.primary_icd,
        )

        evidence: list[EvidenceItem] = []
        flags: list[str] = []

        # Service recognition evidence
        svc_code = service_info.get("service_code", "")
        if svc_code:
            evidence.append(EvidenceItem(
                source="service_mapper",
                key=svc_code,
                value=f"{service_info.get('canonical_name', '')} (score={service_info.get('mapper_score', 0):.2f})",
                weight=service_info.get("mapper_score", 0.0),
            ))
            flags.append("service_recognized")
        else:
            flags.append("service_unmapped")

        # BHYT price reference
        bhyt_price = service_info.get("bhyt_price")
        if bhyt_price:
            evidence.append(EvidenceItem(
                source="bhyt_price",
                key=svc_code,
                value=f"BHYT TT39 price: {bhyt_price:,.0f} VND",
                weight=0.5,
            ))

        # Clinical assessment evidence
        evidence.append(EvidenceItem(
            source="service_disease_matrix",
            key=f"{svc_code} <-> {line.primary_icd}",
            value=clinical.reason,
            weight=clinical.confidence,
        ))

        # Ontology-first case reasoning evidence
        ontology_matches = ontology_result.get("matches") or []
        if ontology_matches:
            best = ontology_matches[0]
            evidence.append(EvidenceItem(
                source="ontology_expected_service",
                key=best.get("target_service_code") or best.get("target_service_name", ""),
                value=(
                    f"{best.get('disease_name', '')} -> {best.get('target_service_name', '')} "
                    f"(role={best.get('role', '')}, source={best.get('support_source', '')})"
                ),
                weight=float(best.get("support_score") or 0.0),
            ))
        else:
            evidence.append(EvidenceItem(
                source="ontology_case_reasoner",
                key="no_direct_match",
                value=ontology_result.get("reasoning_vi", ""),
                weight=float(ontology_result.get("confidence") or 0.0),
            ))

        if not clinical.justified:
            return AgentVerdict(
                agent_name=self.agent_name,
                decision="deny",
                confidence=clinical.confidence,
                evidence=evidence,
                flags=flags + ["not_justified", "matrix_hard_deny"],
                reasoning_vi=f"Khong hop ly y khoa: {clinical.reason}",
                meta={
                    "matrix": {
                        "justified": clinical.justified,
                        "confidence": clinical.confidence,
                        "reason": clinical.reason,
                    },
                    "ontology": ontology_result,
                },
            )

        matrix_direct_support = bool(clinical.justified) and _FALLBACK_REASON_MARKER not in clinical.reason
        matrix_fallback_uncertain = (
            clinical.confidence < _FALLBACK_CONFIDENCE_THRESHOLD
            and _FALLBACK_REASON_MARKER in clinical.reason
        )
        ontology_decision = ontology_result.get("decision", "uncertain")
        ontology_confidence = float(ontology_result.get("confidence") or 0.0)

        if ontology_decision == "approve":
            return AgentVerdict(
                agent_name=self.agent_name,
                decision="approve",
                confidence=max(ontology_confidence, clinical.confidence if matrix_direct_support else 0.0),
                evidence=evidence,
                flags=flags + ["ontology_supported"] + ([f"role_{clinical.role}"] if matrix_direct_support else []),
                reasoning_vi=ontology_result.get("reasoning_vi", ""),
                meta={
                    "matrix": {
                        "justified": clinical.justified,
                        "confidence": clinical.confidence,
                        "reason": clinical.reason,
                        "direct_support": matrix_direct_support,
                    },
                    "ontology": ontology_result,
                },
            )

        if ontology_decision == "deny" and matrix_direct_support:
            return AgentVerdict(
                agent_name=self.agent_name,
                decision="uncertain",
                confidence=max(ontology_confidence, 0.58),
                evidence=evidence,
                flags=flags + ["ontology_matrix_conflict"],
                reasoning_vi=(
                    f"Ontology khong thay support ro rang, nhung ma tran dich vu-benh van co dau hieu ho tro. "
                    f"Can bo sung disease -> expected service. Matrix: {clinical.reason}"
                ),
                meta={
                    "matrix": {
                        "justified": clinical.justified,
                        "confidence": clinical.confidence,
                        "reason": clinical.reason,
                        "direct_support": matrix_direct_support,
                    },
                    "ontology": ontology_result,
                },
            )

        if ontology_decision == "deny":
            return AgentVerdict(
                agent_name=self.agent_name,
                decision="deny",
                confidence=max(ontology_confidence, 0.62),
                evidence=evidence,
                flags=flags + ["ontology_not_supported"],
                reasoning_vi=ontology_result.get("reasoning_vi", ""),
                meta={
                    "matrix": {
                        "justified": clinical.justified,
                        "confidence": clinical.confidence,
                        "reason": clinical.reason,
                        "direct_support": matrix_direct_support,
                    },
                    "ontology": ontology_result,
                },
            )

        if matrix_fallback_uncertain:
            return AgentVerdict(
                agent_name=self.agent_name,
                decision="uncertain",
                confidence=max(clinical.confidence, ontology_confidence),
                evidence=evidence,
                flags=flags + ["fallback_uncertain"],
                reasoning_vi=ontology_result.get("reasoning_vi", "")
                or (
                    f"Khong co bang chung truc tiep tu ma tran dich vu-benh. "
                    f"Confidence={clinical.confidence:.2f}. {clinical.reason}"
                ),
                meta={
                    "matrix": {
                        "justified": clinical.justified,
                        "confidence": clinical.confidence,
                        "reason": clinical.reason,
                        "direct_support": matrix_direct_support,
                    },
                    "ontology": ontology_result,
                },
            )

        if matrix_direct_support:
            reasoning_vi = clinical.reason
            if ontology_decision == "uncertain":
                reasoning_vi = (
                    f"Matrix co bang chung truc tiep; ontology chua du bao phu de xac nhan day du. {clinical.reason}"
                )
            return AgentVerdict(
                agent_name=self.agent_name,
                decision="approve",
                confidence=clinical.confidence,
                evidence=evidence,
                flags=flags + [f"role_{clinical.role}", "matrix_direct_support"],
                reasoning_vi=reasoning_vi,
                meta={
                    "matrix": {
                        "justified": clinical.justified,
                        "confidence": clinical.confidence,
                        "reason": clinical.reason,
                        "direct_support": matrix_direct_support,
                    },
                    "ontology": ontology_result,
                },
            )

        return AgentVerdict(
            agent_name=self.agent_name,
            decision="uncertain",
            confidence=max(clinical.confidence, ontology_confidence),
            evidence=evidence,
            flags=flags + ["ontology_insufficient"],
            reasoning_vi=ontology_result.get("reasoning_vi", ""),
            meta={
                "matrix": {
                    "justified": clinical.justified,
                    "confidence": clinical.confidence,
                    "reason": clinical.reason,
                    "direct_support": matrix_direct_support,
                },
                "ontology": ontology_result,
            },
        )
