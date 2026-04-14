# CLAUDE_PROJECT_MAP.md

Curated repo memory for Claude running inside the Pathway API container.

This file is meant to help Claude reason about the real Pathway workspace without pretending it has freshly read every file in the repo.

## What Pathway Actually Is

- One main FastAPI app centered on `api_server.py`
- Neo4j-backed clinical knowledge graph and ontology inspection platform
- Azure OpenAI-backed retrieval and generation in `medical_agent.py`
- Static HTML surfaces under `static/claims_insights/`
- A separate ingestion sub-workspace in `medical_pipeline_agent/`

Do not default to Django or a generic chatbot mental model. The current app is a layered clinical graph platform with multiple tooling surfaces.

## Main Runtime Entry Points

### `api_server.py`

This is the runtime hub.

- mounts `/static` and `/pdfs`
- lazily creates a singleton `AcademicAgent` via `_get_agent()`
- owns dashboard bootstrap payloads, API routes, ingest run orchestration, and websocket log streaming

Important public routes:

- `GET /health`
- `GET /api/platform/bootstrap`
- `GET /api/claude/status`
- `POST /api/claude/duet`
- `POST /api/ask`
- `POST /api/adjudicate`
- `POST /api/ingest`
- `GET /api/ingest/{run_id}`
- `GET /api/pipeline-runs`
- `GET /api/ontology-v2/inspector/bootstrap`
- `GET /api/ontology-v2/inspector/disease/{disease_id}`
- `GET /api/claims-insights/explorer/bootstrap`
- `GET /api/claims-insights/explorer/graph/{disease_id}`
- `POST /api/claims-insights/testcase-trace`
- `WS /ws/pipeline/{run_id}`

### `medical_agent.py`

This is the core retrieval and reasoning engine.

- loads `.env` through `runtime_env.load_notebooklm_env()`
- uses Azure OpenAI embeddings via `text-embedding-ada-002`
- uses Azure OpenAI chat via `MODEL2` from env
- connects to Neo4j using `NEO4J_URI`, `NEO4J_USER`, `NEO4J_PASSWORD`

Main capabilities:

- vector search on `rawchunk_vector_idx`
- fulltext search on `raw_chunk_fulltext`
- hybrid retrieval using Reciprocal Rank Fusion
- disease resolution and scoped search
- iterative reasoning / verification traces
- ontology-v2 traversal for disease, signs, services, observations, assertions
- claim adjudication with graph-backed reasoning trace

### `academic_agent.py`

`AcademicAgent` extends `MedicalAgent`.

- formats citations
- extracts rough metadata with the LLM
- generates citation-rich answers for `/api/ask`

It is the default app agent created by `api_server.py`.

## Main User-Facing Flows

### General Clinical Q and A

`POST /api/ask`

- uses `SessionStore` for short in-memory chat history
- resolves disease if possible
- runs scoped or enhanced retrieval
- calls `AcademicAgent.generate_academic_response()`
- returns sources and reasoning trace

If `deep_reasoning=true`, the route switches into `agentic_ask(...)` mode with reflection rounds instead of the lighter direct answer path.
When Claude CLI is available in the API container, deep reasoning can also route through a grounded "Claude Brain" layer that:

- receives a structured packet with retrieved evidence, ontology snapshot, testcase inventory, and recent history
- is required to produce a task plan before answering
- must mark uncertainty instead of concluding when evidence is weak
- returns a grounded answer plus knowledge-access next steps

### Claim Adjudication

`POST /api/adjudicate`

- stores request/response summary in `SessionStore`
- prefers ontology-v2 context through `_query_ontology_v2_context(...)`
- falls back to legacy retrieval if ontology-v2 context is missing
- asks the LLM to return adjudication JSON
- returns itemized decisions plus a reasoning trace for UI rendering

This flow is graph-aware, trace-heavy, and meant for explainability rather than minimal-latency chat.

### Ontology Inspector

Backed by `server_support/ontology_v2_inspector_store.py`

- chooses a Neo4j URI from a candidate list
- summarizes namespaces
- exposes disease-level graph details
- works with labels such as `DiseaseEntity`, `RawChunk`, `RawSignMention`, `RawServiceMention`, `RawObservationMention`, `ProtocolAssertion`, `ProtocolDiseaseSummary`

Default namespace is `ontology_v2`.

### Claims Insights Explorer

Backed by `server_support/claims_insights_graph_store.py`

- can read from a bundle JSON fallback
- can also import graph data into Neo4j namespace `claims_insights_explorer_v1`
- models disease, sign, service, and observation evidence for claims analysis

Important nuance:

- the claims-insights importer can clear its own namespace before re-importing
- this is separate from the medical ingestion safety rules in `medical_pipeline_agent/`

### Testcase Trace

Backed by `server_support/testcase_trace_runner.py`

- consumes testcase JSON files
- queries graph expectations and protocol evidence
- builds planning, service-line, and chat traces
- writes JSON and Markdown artifacts for later inspection

This is not a generic benchmark runner; it is a trace-first diagnostic surface for claim/testcase reasoning.

### Claude Duet Lab

Backed by `server_support/claude_duet.py`

- invokes Claude turn by turn with `claude -p`
- usually text-only with tools disabled
- replays transcript each turn rather than using long-lived autonomous workers
- now expects machine-readable structured turns for orchestration

Do not describe it as a truly persistent multi-agent runtime unless the implementation changes.

### Claude Decision Gate

Backed by `server_support/claude_decision.py`

- returns one structured recommendation at a workflow checkpoint
- is exposed through `POST /api/claude/decision-gate`
- is also used by the medical ingestion pipeline as an advisory gate after pipeline design and after quality testing
- can pause a run for human review, after which `POST /api/ingest/{run_id}/control` becomes the operator handoff
- the dashboard now renders checkpoint-aware controls instead of a generic approve button

Treat it as a checkpoint advisor with explicit proceed/pause/terminate semantics, not as a free-form chat bot.

## State, Storage, And Persistence

### In-memory state

- `SessionStore` keeps short chat history only in memory
- session IDs are short generated IDs unless a client provides one
- this history is not durable across process restarts

### Disk-backed run state

- `PipelineRunStore` writes run metadata to `data/pipeline_runs/*.json`
- logs are streamed through websocket listeners and also stored in run payloads
- stale running runs are inferred in `api_server.py` from timestamps, not by a separate job queue
- decision-gate resumes append `human_decision_events` and can end as `paused_for_human_review`, `aborted_by_decision_gate`, or `aborted_by_human_review`

### PDF discovery

`DiseasePdfCatalog` resolves PDFs from:

- repo root PDFs
- `data/uploads/`
- `assets/reference_pdfs/`

It also keeps a best-effort disease-to-PDF mapping using Neo4j and a few hard-coded fallbacks.

## Sub-Workspace: `medical_pipeline_agent/`

Treat this as a semi-independent ingestion system with its own rules.

The main orchestrator is `medical_pipeline_agent/scripts/orchestrator.py`.

Pipeline phases:

- Phase 0: PDF analysis
- Phase 1: pipeline design
- Phase 2: ingestion
- Phase 3: quality testing
- Phase 4: self-improvement / optimization

Important properties:

- writes audit-style artifacts to `data/pipeline_runs/...`
- distinguishes `universal_ingest.py` vs `multi_disease_ingest.py`
- contains its own CLAUDE rules, hooks, and agent docs
- includes an `experience_memory.py` subsystem for storing lessons from past runs

Do not confuse the pipeline agent safety constraints with the whole FastAPI app. They overlap, but they are not the same layer.

## Platform Surfaces To Keep In Mind

These are not hypothetical modules. They already exist in the Pathway UX:

- Unified dashboard
- Legacy chat
- Graph Forge / cinematic ontology view
- Ontology Inspector
- Disease Explorer
- Testcase Trace
- Claude Duet Lab

When proposing changes, prefer fitting into one of these surfaces before inventing a new one.

## Working Mental Model For The Repo

Pathway is best understood as four interacting layers:

1. Retrieval and reasoning core
- `medical_agent.py`
- `academic_agent.py`

2. FastAPI orchestration and observability
- `api_server.py`
- `server_support/api_models.py`
- `server_support/pipeline_store.py`
- `server_support/session_store.py`

3. Graph inspection and analysis surfaces
- `server_support/ontology_v2_inspector_store.py`
- `server_support/claims_insights_graph_store.py`
- `server_support/testcase_trace_runner.py`
- `static/claims_insights/*.html`

4. PDF ingestion and quality loop
- `medical_pipeline_agent/scripts/orchestrator.py`
- `universal_ingest.py`
- `multi_disease_ingest.py`
- `ontology_v2_ingest.py`

## What Not To Assume

- Do not assume all graph data uses the same labels or namespaces.
- Do not assume every route writes durable state.
- Do not assume duet agents have tool access or persistent memory.
- Do not assume claims-insights graph data and ontology-v2 data are the same model.
- Do not assume the ingestion sub-workspace is the same thing as the live Q and A runtime.

## Preferred Design Language

When talking about Pathway, prefer language like:

- "FastAPI hub"
- "Neo4j-backed retrieval"
- "ontology-v2 namespace"
- "claims-insights namespace"
- "disk-backed pipeline run state"
- "in-memory session history"
- "turn-by-turn Claude duet runtime"
