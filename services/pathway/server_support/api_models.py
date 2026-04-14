from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, Field


class QuestionRequest(BaseModel):
    question: str
    top_k: Optional[int] = 8
    style: Optional[str] = "APA"
    disease_name: Optional[str] = None
    hospital_name: Optional[str] = None
    session_id: Optional[str] = None
    deep_reasoning: Optional[bool] = False


class Source(BaseModel):
    title: str
    url: str
    source: str
    page: int = 1
    pdf_file: Optional[str] = None
    disease_name: Optional[str] = None
    source_type: Optional[str] = None
    hospital_name: Optional[str] = None
    node_id: Optional[str] = None


class TraceStep(BaseModel):
    step: str
    detail: Optional[Dict] = None
    duration_ms: Optional[float] = None


class ReasoningTraceEntry(BaseModel):
    """One step of the graph reasoning trace — shown live in the UI."""
    phase: str                                  # e.g. "vector_search", "disease_resolve", "edge_traverse", "rerank", "llm_generate"
    action: str                                 # human-readable Vietnamese description
    node_ids: Optional[List[str]] = None        # graph node IDs to highlight at this step
    edge_keys: Optional[List[str]] = None       # "source→target" edge keys to highlight
    details: Optional[Dict] = None              # extra data (scores, counts, etc.)
    duration_ms: Optional[float] = None


class AskResponse(BaseModel):
    answer: str
    sources: List[Source]
    disease_detected: Optional[str] = None
    search_mode: str = "enhanced"
    source_priority: Optional[str] = None
    hospital_name: Optional[str] = None
    trace: Optional[List[TraceStep]] = None
    verification: Optional[Dict] = None
    session_id: Optional[str] = None
    reasoning_node_ids: Optional[List[str]] = None
    reasoning_edge_keys: Optional[List[str]] = None
    reasoning_trace: Optional[List[ReasoningTraceEntry]] = None


class CrawlRequest(BaseModel):
    query: Optional[str] = None
    limit: int = 5


class ClaimAdjudicateRequest(BaseModel):
    claim_text: str
    disease_name: Optional[str] = None
    session_id: Optional[str] = None


class AdjudicationItem(BaseModel):
    service_name: str
    status: str  # e.g., "Approved", "Denied", "Need Review"
    reason: str


class ClaimAdjudicateResponse(BaseModel):
    items: List[AdjudicationItem]
    summary: str
    reasoning_node_ids: Optional[List[str]] = None
    reasoning_trace: Optional[List[ReasoningTraceEntry]] = None


class ClaudeDuetRequest(BaseModel):
    topic: str
    context: Optional[str] = None
    turns: int = Field(default=4, ge=2, le=8)
    model: Optional[str] = "claude-opus-4-6"
    agent_a_name: Optional[str] = "Claude Strategist"
    agent_a_prompt: Optional[str] = None
    agent_b_name: Optional[str] = "Claude Challenger"
    agent_b_prompt: Optional[str] = None
    max_output_chars: int = Field(default=1200, ge=300, le=4000)


class ClaudeDecisionGateRequest(BaseModel):
    workflow: str
    checkpoint: str
    objective: str
    context: Optional[str] = None
    state: Dict = Field(default_factory=dict)
    candidate_actions: List[str] = Field(default_factory=list)
    model: Optional[str] = "claude-opus-4-6"
    system_prompt: Optional[str] = None
    max_output_chars: int = Field(default=1600, ge=400, le=5000)


class ClaudeDecisionGateResult(BaseModel):
    schema_version: str = "claude_decision_gate.v1"
    workflow: str
    checkpoint: str
    recommended_action: str
    confidence: Literal["low", "medium", "high"] = "medium"
    proceed: bool = True
    needs_human_review: bool = False
    stop_signal: Literal["continue", "pause", "terminate"] = "continue"
    reasoning: str
    risks: List[str]
    suggested_changes: List[str]
    next_owner: str
    next_step: str


class ClaudeDecisionGateResponse(BaseModel):
    decision: ClaudeDecisionGateResult
    raw_content: Optional[str] = None
    duration_ms: Optional[float] = None
    repair_attempts: int = 0
    claude_status: Optional[Dict] = None
    bridge_trace: Optional[Dict[str, Any]] = None


class PipelineDecisionControlRequest(BaseModel):
    action: Literal[
        "continue_to_ingestion",
        "accept_current_result",
        "run_optimization",
        "abort_run",
    ]
    note: Optional[str] = None


class PipelineDecisionControlResponse(BaseModel):
    run_id: str
    status: str
    selected_action: str
    checkpoint: Optional[str] = None
    message: str
    allowed_actions: List[str] = Field(default_factory=list)


class ClaudeDuetNextStep(BaseModel):
    target: str
    request: str
    termination_signal: Literal["continue", "escalate", "terminate"] = "continue"


class ClaudeDuetStructuredTurn(BaseModel):
    schema_version: str = "claude_duet_turn.v1"
    phase: Literal["bootstrap", "propose", "critique", "tighten", "converge", "escalate", "terminate"]
    decision: str
    why: str
    protocol_delta: List[str]
    risks: List[str]
    next: ClaudeDuetNextStep


class ClaudeDuetTurn(BaseModel):
    index: int
    speaker: str
    role: str
    content: str
    raw_content: Optional[str] = None
    structured: Optional[ClaudeDuetStructuredTurn] = None
    duration_ms: Optional[float] = None
    repair_attempts: int = 0


class ClaudeDuetResponse(BaseModel):
    topic: str
    model: str
    turns: int
    final_output: str
    final_structured_output: Optional[ClaudeDuetStructuredTurn] = None
    transcript: List[ClaudeDuetTurn]
    schema_version: str = "claude_duet_turn.v1"
    claude_status: Optional[Dict] = None
    bridge_trace: Optional[Dict[str, Any]] = None
