# CLAUDE.md — ai_claim

AI Claim reasoning platform for medical and insurance graph workflows. Domain-locked reasoning agent + knowledge management + benchmark harness, backed by Neo4j and Azure OpenAI.

## Quick Start

```bash
# Start server (requires Neo4j, Pathway API, Azure proxy, 9router already running)
powershell -File scripts/start_ai_claim_server.ps1

# Stop server
powershell -File scripts/stop_ai_claim_server.ps1

# Smoke test
python scripts/smoke_test.py

# Check full runtime stack
python scripts/check_runtime_stack.py
```

Server runs at `http://127.0.0.1:9780`. Dashboard at `/` or `/dashboard`.

## Architecture

```
ai_claim (FastAPI :9780)
├── reasoning_agent.py    ← Azure OpenAI tool-use loop with Neo4j + knowledge surface
├── neo4j_toolkit.py      ← Direct Neo4j queries (disease, service, benefit, exclusion, evidence)
├── knowledge_registry.py ← Local file registry with versioning, dedup, upload validation
├── knowledge_surface.py  ← Text search over knowledge roots (protocols, notes, benchmarks)
├── ingest_compiler.py    ← Routes uploads to Pathway for graph ingestion
├── pathway_knowledge_bridge.py ← HTTP bridge to Pathway knowledge API
├── pathway_client.py     ← HTTP client to Pathway /api/medical/* and /api/adjudicate/*
├── live_duel_runner.py   ← Runs ai_claim vs Pathway side-by-side, saves artifacts
├── benchmark_analyzer.py ← Scores duel runs against gold labels, generates gap reports
├── domain_policy.py      ← Domain lock: allowed tools, knowledge roots, decision rules
├── knowledge_layout.py   ← Ensures knowledge root directories + disease workspaces
├── azure_openai_backend.py ← Azure OpenAI chat completions with tool-use
└── settings.py           ← Env-driven config (Neo4j, Pathway, Azure, router URLs)
```

## Module Roles

| Module | What it does | When to touch it |
|--------|-------------|------------------|
| `main.py` | FastAPI routes, system status, production readiness | Adding/changing API endpoints |
| `reasoning_agent.py` | Domain-locked tool-use agent (system prompt + 11 tools) | Changing reasoning behavior, adding tools |
| `neo4j_toolkit.py` | All Neo4j queries (CIDisease, DiseaseHypothesis, Benefits, Exclusions) | Graph query bugs, new query patterns |
| `knowledge_registry.py` | File catalog with SHA1 dedup, versioning, upload validation | Upload/ingest pipeline changes |
| `ingest_compiler.py` | Decides which roots can direct-ingest vs catalog-only | Promoting roots to first-class ingest |
| `live_duel_runner.py` | Orchestrates ai_claim vs Pathway comparison runs | Duel test mechanics |
| `benchmark_analyzer.py` | Scoring logic delegated to `run_pathway_vs_agent_claude_duel.py` | Score calculation bugs |
| `domain_policy.py` | Tool whitelist + domain lock for agent_claude launch specs | Tightening/loosening agent permissions |
| `settings.py` | All env vars with sensible defaults | Adding new config |

## Key API Endpoints

### System
- `GET /health` — Basic health + config summary
- `GET /api/system/status` — All components (ai_claim, Pathway, Neo4j, Azure proxy, router)
- `GET /api/production-readiness` — Checklist: all infra + graph key audit + ingest support

### Knowledge Management
- `GET /api/knowledge/root-summary` — Asset counts per root, duplicate groups
- `GET /api/knowledge/assets?root_key=&limit=&offset=` — Paginated asset list
- `GET /api/knowledge/assets/{asset_id}` — Asset detail
- `POST /api/knowledge/scan` — Re-scan all roots, update registry
- `POST /api/knowledge/upload` — Upload file to a root (form: `root_key`, `file`)
- `GET /api/knowledge/surface/search?query=&root_key=&disease_key=&limit=` — Text search
- `GET /api/knowledge/surface/read?path=` — Read knowledge file content

### Neo4j
- `GET /api/neo4j/health` — Graph health (ontology, claims, insurance counts)
- `GET /api/neo4j/mapping-key-audit` — Duplicate key check across canonical labels
- `GET /api/neo4j/disease-service-coverage?icd_code=&disease_name=` — Coverage for one disease
- `GET /api/neo4j/benchmark-coverage` — Coverage check for all benchmark diseases

### Ingest & Bridge
- `GET /api/ingest/support-matrix` — Which roots support direct ingest vs catalog
- `POST /api/knowledge/bridge-upload` — Upload + bridge to Pathway (optional auto_ingest)
- `POST /api/knowledge/assets/{asset_id}/bridge` — Bridge existing asset to Pathway
- `GET /api/pathway/knowledge/bootstrap` — Pathway knowledge registry bootstrap

### Reasoning & Benchmark
- `POST /api/reasoning/run` — Run reasoning agent on a case packet
- `POST /api/duel/run` — Run live duel (ai_claim vs Pathway)
- `GET /api/benchmark/summary?run_dir=` — Reasoning gap analysis for a duel run
- `GET /api/benchmark/report?run_dir=` — Markdown report for a duel run

## Knowledge Roots

Defined in `configs/knowledge_roots.json`. Each root has accepted file types and a graph target.

| Root Key | Label | Graph Target | Ingest Status |
|----------|-------|-------------|---------------|
| `protocols` | Phac do | `ontology_v2` | Direct ingest via Pathway pipeline |
| `service_tables` | Danh muc dich vu | `claims_insights_explorer_v1` | Direct ingest via mapper scripts |
| `insurance_rules` | Quy tac bao hiem | `claims_insights_insurance_v1` | Direct ingest via `neo4j_ingest_insurance.py` |
| `benefit_tables` | Bang quyen loi | `claims_insights_insurance_v1` | Direct ingest via benefit knowledge pack |
| `symptom_tables` | Bang trieu chung | `claims_insights_explorer_v1` | Direct ingest via CISign nodes |
| `legal_documents` | Van ban phap ly | `claims_insights_insurance_v1` | Catalog only (needs parser) |
| `diseases` | Thu muc benh | `mixed` | Workspace directories, not file upload |
| `adjuster_notes` | Ghi chu tham dinh vien | `memory` | Local registry only |

Files live under `data/knowledge/{root_key}/`.

## Domain Policy

`configs/domain_policy.json` locks the reasoning agent to medical/insurance domain:
- **Allowed tools**: `Read`, `Bash`, 16 MCP Neo4j tools, `search_knowledge_surface`, `read_knowledge_asset`
- **Blocked tools**: `Edit`, `Write`, `WebFetch`, `WebSearch`, `Agent` (no code modification, no web access)
- **Decision rules**: Every decision needs evidence. No evidence → `review` or `insufficient_evidence`. No out-of-graph knowledge without explicit disclaimer.

## Neo4j Graph Domains Used

### Claims Insights (`claims_insights_explorer_v1`)
- `CIDisease → CI_HAS_SIGN → CISign` — Disease-symptom links
- `CIDisease → CI_INDICATES_SERVICE → CIService` — Disease-service links
- `CIService → MAPS_TO_CANONICAL → CanonicalService` — Service standardization

### Insurance (`claims_insights_insurance_v1`)
- `Insurer → ISSUES → InsuranceContract → HAS_BENEFIT → Benefit`
- `InsuranceContract → HAS_EXCLUSION → Exclusion → HAS_REASON → ExclusionReason`
- `InsuranceContract → COVERS_PLAN → ContractPlan → COVERS_BENEFIT → Benefit`
- `CIService → FALLS_UNDER_BENEFIT → Benefit` (service-benefit bridge)

### Disease-Service Seed (`ontology_v2_seed`)
- `DiseaseHypothesis → DISEASE_EXPECTS_SERVICE → CIService` — Role-typed edges (screening, diagnostic, confirmatory, rule_out, treatment, monitoring, severity)
- Seeded by `pathway/notebooklm/scripts/seed_disease_service_expectations.py`
- 16 diseases, 74 service-expectation edges

### Query Fallback Chain (disease-service)
1. `Disease ← ABOUT_DISEASE ← Chunk → MENTIONS → Service` (clinical graph)
2. `CIDisease → CI_INDICATES_SERVICE → CIService` (claims insights)
3. `DiseaseHypothesis → DISEASE_EXPECTS_SERVICE → CIService|ProtocolService` (seed data)

## Reasoning Agent Tools (11 tools)

The `AzureReasoningAgent` uses Azure OpenAI function calling with these tools backed by `Neo4jToolkit` and `KnowledgeSurface`:

| Tool | Source | Purpose |
|------|--------|---------|
| `graph_health` | Neo4j | Overall graph status |
| `list_recent_ci_diseases` | Neo4j | Browse available diseases |
| `query_ci_disease_snapshot` | Neo4j | Signs + services for a disease |
| `query_disease_services` | Neo4j | Services with fallback chain |
| `query_contract_stats` | Neo4j | Contract summary |
| `query_benefits_for_contract` | Neo4j | Benefits (relevance-ranked) |
| `query_exclusions_by_contract` | Neo4j | Exclusions by usage frequency |
| `query_service_exclusions` | Neo4j | Service-level exclusions |
| `query_clinical_service_info` | Neo4j | Canonical service + related diseases |
| `trace_service_evidence` | Neo4j | Combined medical + insurance evidence |
| `search_knowledge_surface` | Files | Text search in knowledge roots |
| `read_knowledge_asset` | Files | Read a specific knowledge file |

## Duel Benchmark System

The benchmark compares `ai_claim` (Azure reasoning agent) vs `Pathway` (FastAPI adjudication) against gold labels.

### Running a Duel
```bash
python scripts/run_live_duel.py  # Uses case packet from stdin or --case-file
```
Or via API: `POST /api/duel/run` with case packet JSON.

### Scoring
Scoring is in `pathway/notebooklm/scripts/testing/run_pathway_vs_agent_claude_duel.py`:
- **Disease match**: exact → substring → ICD alias → token overlap (60%)
- **Decision match**: normalized via `DECISION_EQUIVALENCE` (maps `medically_necessary`→`approve`, `pay_full`→`approve`, `not_indicated`→`deny`, `conditional_indication`→`review`, etc.)
- **Weighted score**: `0.15×disease + 0.35×medical + 0.30×final + 0.20×claim_level`

### Gold Test Cases
- `pathway/notebooklm/workspaces/claims_insights/07_architecture/09_*_meniere.json`
- `pathway/notebooklm/workspaces/claims_insights/07_architecture/10_*_pneumonia.json`

## Connection Details

| Service | Address | Notes |
|---------|---------|-------|
| ai_claim | `http://127.0.0.1:9780` | This server |
| Pathway API | `http://localhost:9600` | `PATHWAY_API_BASE_URL` |
| Neo4j Bolt | `bolt://localhost:7688` | `NEO4J_URI` |
| Neo4j Auth | `neo4j/password123` | `NEO4J_USER`/`NEO4J_PASSWORD` |
| Azure Proxy | `http://127.0.0.1:8009` | `AZURE_PROXY_BASE_URL` |
| 9Router | `http://127.0.0.1:20128` | `ROUTER_BASE_URL` |

All configurable via env vars or `.env.local` at project root.

## Scripts

| Script | Purpose |
|--------|---------|
| `start_ai_claim_server.ps1` | Start uvicorn on port 9780, save PID |
| `stop_ai_claim_server.ps1` | Stop server by PID |
| `smoke_test.py` | Hit /health and /api/system/status |
| `check_runtime_stack.py` | Verify all 5 components are up |
| `run_live_duel.py` | Run ai_claim vs Pathway duel |
| `run_reasoning_case.py` | Run reasoning agent on a single case |
| `analyze_duel_run.py` | Analyze an existing duel run directory |
| `bootstrap_knowledge_tree.py` | Create knowledge root directories |
| `emit_restricted_agent_launch_spec.py` | Generate domain-locked agent_claude launch config |
| `import_external_context.py` | Import external files into knowledge roots |

## Architecture Docs

In `docs/`:
1. `01_chenh_lech_tu_duy_pathway_va_agent_claude.md` — Reasoning gap between Pathway and agent_claude
2. `02_tieu_chi_ingest_neo4j.md` — Criteria for Neo4j ingestion
3. `03_kien_truc_muc_tieu_ai_claim.md` — Target architecture
4. `05_luong_quyet_dinh_y_te_bao_hiem.md` — Medical-insurance decision flow
5. `07_domain_lock_va_azure.md` — Domain lock + Azure integration
6. `08_trang_thai_trien_khai.md` — Deployment status
7. `09_goi_ai_claim_len_web.md` — Web deployment guide

## Key Conventions

- Python 3.11+. Dependencies in `pyproject.toml`.
- Windows dev: use `python -X utf8` for Vietnamese text.
- All user-facing text and logs are in Vietnamese.
- Decision output labels: `approve`, `deny`, `partial_pay`, `review`, `uncertain`.
- Every decision must cite evidence from graph or knowledge surface.
- Neo4j queries use namespace filtering (`claims_insights_explorer_v1`, `claims_insights_insurance_v1`, `ontology_v2_seed`).

## Restart vs Rebuild

| Change | Action Required |
|--------|----------------|
| Python source in `src/ai_claim/` | Restart uvicorn |
| `configs/*.json` | Restart uvicorn |
| Knowledge files in `data/knowledge/` | `POST /api/knowledge/scan` (no restart) |
| Neo4j seed data | Run seed script, then restart MCP server |
| Dashboard HTML | Restart uvicorn |
| `.env.local` | Restart uvicorn |

## graphify

This project has a graphify knowledge graph at graphify-out/.

Rules:
- Before answering architecture or codebase questions, read graphify-out/GRAPH_REPORT.md for god nodes and community structure
- If graphify-out/wiki/index.md exists, navigate it instead of reading raw files
- After modifying code files in this session, run `python3 -c "from graphify.watch import _rebuild_code; from pathlib import Path; _rebuild_code(Path('.'))"` to keep the graph current
