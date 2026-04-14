"""Pydantic data contracts for the multiagent adjudication system."""

from __future__ import annotations

from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, Field


# ---------------------------------------------------------------------------
# Agent-level types
# ---------------------------------------------------------------------------

Decision = Literal["approve", "deny", "partial_pay", "review", "uncertain"]


class ServiceLineInput(BaseModel):
    """One service line from an insurance claim."""

    service_name_raw: str
    maanhxa: str = ""  # BYT MAANHXA code from hospital HIS (if available)
    diagnosis_text: str = ""
    primary_icd: str = ""
    contract_id: str = ""
    insurer: str = ""
    symptoms: List[str] = Field(default_factory=list)
    medical_history: str = ""
    admission_reason: str = ""
    cost_vnd: float = 0.0
    quantity: int = 1


class EvidenceItem(BaseModel):
    """One piece of structured evidence backing an agent decision."""

    source: str  # e.g. "service_disease_matrix", "contract_rules", "bhyt_price"
    key: str  # e.g. "LAB-BIO-002 <-> J18.9"
    value: str  # human-readable evidence text
    weight: float = 1.0  # evidence strength 0.0-1.0


class AgentVerdict(BaseModel):
    """Result from a single specialist agent."""

    agent_name: str
    decision: Decision
    confidence: float = Field(ge=0.0, le=1.0)
    evidence: List[EvidenceItem] = Field(default_factory=list)
    flags: List[str] = Field(default_factory=list)
    reasoning_vi: str = ""
    meta: Dict[str, Any] = Field(default_factory=dict)


class AdjudicationResult(BaseModel):
    """Final merged result for one service line."""

    service_name: str
    recognized_service_code: str = ""
    recognized_canonical_name: str = ""
    medical_decision: Decision = "uncertain"
    medical_confidence: float = Field(default=0.0, ge=0.0, le=1.0)
    medical_reasoning_vi: str = ""
    final_decision: Literal["approve", "deny", "partial_pay", "review"]
    confidence: float = Field(ge=0.0, le=1.0)
    agent_verdicts: Dict[str, AgentVerdict] = Field(default_factory=dict)
    reasoning_vi: str = ""
    evidence_summary: List[EvidenceItem] = Field(default_factory=list)
    resolution_rule: str = ""  # which conflict resolution rule fired
    canonical_enrichment: Dict[str, Any] = Field(default_factory=dict)  # BYT canonical data


# ---------------------------------------------------------------------------
# API request / response
# ---------------------------------------------------------------------------

class MultiAgentAdjudicateRequest(BaseModel):
    """Request to the /api/adjudicate/v2 endpoint."""

    claim_id: str = ""
    service_lines: List[ServiceLineInput]
    contract_id: str = ""
    insurer: str = ""
    symptoms: List[str] = Field(default_factory=list)
    medical_history: str = ""
    admission_reason: str = ""
    known_diseases: List[str] = Field(default_factory=list)
    session_id: Optional[str] = None


class MultiAgentAdjudicateResponse(BaseModel):
    """Response from the /api/adjudicate/v2 endpoint."""

    claim_id: str = ""
    results: List[AdjudicationResult] = Field(default_factory=list)
    summary_vi: str = ""
    meta: Dict[str, Any] = Field(default_factory=dict)


class MedicalReasoningLineResult(BaseModel):
    """Medical-only reasoning result for one requested service."""

    service_name_raw: str
    recognized_service_code: str = ""
    recognized_canonical_name: str = ""
    medical_decision: Decision = "uncertain"
    medical_confidence: float = Field(default=0.0, ge=0.0, le=1.0)
    medical_reasoning_vi: str = ""
    ontology_matches: List[Dict[str, Any]] = Field(default_factory=list)
    ontology_meta: Dict[str, Any] = Field(default_factory=dict)
    verification_plan: List[Dict[str, Any]] = Field(default_factory=list)
    evidence_ledger: List[Dict[str, Any]] = Field(default_factory=list)
    coverage_gaps: List[Dict[str, Any]] = Field(default_factory=list)
    audit_summary: Dict[str, Any] = Field(default_factory=dict)
    reasoning_trace: List[Dict[str, Any]] = Field(default_factory=list)


class MedicalReasoningRequest(BaseModel):
    """Request for ontology-first medical service reasoning only."""

    case_id: str = ""
    known_diseases: List[str] = Field(default_factory=list)
    symptoms: List[str] = Field(default_factory=list)
    medical_history: str = ""
    admission_reason: str = ""
    service_lines: List[ServiceLineInput] = Field(default_factory=list)


class MedicalReasoningResponse(BaseModel):
    """Response for the medical-only reasoning endpoint."""

    case_id: str = ""
    mode: str = "sign_inference"
    summary_vi: str = ""
    input_signs: List[str] = Field(default_factory=list)
    disease_hints: List[Dict[str, str]] = Field(default_factory=list)
    top_hypotheses: List[Dict[str, Any]] = Field(default_factory=list)
    active_diseases: List[Dict[str, Any]] = Field(default_factory=list)
    verification_plan: List[Dict[str, Any]] = Field(default_factory=list)
    evidence_ledger: List[Dict[str, Any]] = Field(default_factory=list)
    coverage_gaps: List[Dict[str, Any]] = Field(default_factory=list)
    audit_summary: Dict[str, Any] = Field(default_factory=dict)
    results: List[MedicalReasoningLineResult] = Field(default_factory=list)
    reasoning_trace: List[Dict[str, Any]] = Field(default_factory=list)
    meta: Dict[str, Any] = Field(default_factory=dict)
