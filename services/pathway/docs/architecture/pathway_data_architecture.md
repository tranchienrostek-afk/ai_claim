# Pathway Data Architecture

Pathway should understand the medical and insurance data system the way Claude Code understands a source-code workspace.

The core shift is:

- Claude Code reasons over files, modules, imports, tests, and diffs.
- Pathway must reason over Neo4j namespaces, ontology nodes, chunks, assertions, claims, contracts, benchmarks, traces, and memory.

## Mission

Pathway must be able to answer four questions before it is considered "data-aware":

1. What data surfaces exist right now?
2. Which surface contains the evidence needed for this task?
3. Which facts are raw evidence, canonical concepts, rules, runtime inference, or supervision memory?
4. What is missing or weak in the current knowledge system?

## Non-Negotiable Principles

- Medical truth must stay separate from contract truth.
- Raw evidence must stay separate from compiled inference.
- Benchmarks and testcases must not pollute runtime truth.
- Memory and operator feedback are supervision layers, not clinical facts.
- Every decision must trace back to chunks, assertions, rules, or claim evidence.
- Filesystem and Neo4j are both first-class storage surfaces.

## Logical Layers

### L0 Storage Surfaces

- Filesystem directories
- Neo4j namespaces
- Runtime memory files
- Generated run artifacts

### L1 Raw Evidence

- PDFs
- Extracted text
- Raw chunks
- Raw claims
- Raw contract documents
- Testcase inputs

### L2 Canonical Models

- Disease
- Sign
- Service
- Observation
- Policy / benefit / exclusion concepts
- Aliases and stable IDs

### L3 Assertions And Rules

- Protocol assertions
- Contraindications
- Contract rules
- Summaries
- Provenance links

### L4 Reasoning Runtime

- Disease hypotheses
- Expected services
- Contradiction signals
- Reasoning traces
- Coverage signals

### L5 Supervision And Memory

- Datatest corpora
- Testcase traces
- Claude bridge interactions
- Experience memory
- Operator feedback

## Canonical Domains

### Medical Knowledge

- Neo4j namespace: `ontology_v2`
- Filesystem anchors:
  - `data/uploads/`
  - `assets/reference_pdfs/`
  - `data/extracted_text/`
  - `config/ingest_configs/`
  - `config/ontology_v2_schema.json`

### Claims And Contracts

- Neo4j namespace: `claims_insights_explorer_v1`
- Filesystem anchors:
  - `workspaces/claims_insights/01_claims/`
  - `workspaces/claims_insights/06_insurance/`
  - `workspaces/claims_insights/04_reports/`
  - `server_support/adjudication/test_sample_100.jsonl`

### Benchmarks And Testcases

- Filesystem anchors:
  - `data/datatest/`
  - `data/datatest_v02/`
  - `data/datatest_v03/`
  - `data/script/testcase_trace_runs/`

### Runtime And Observability

- Filesystem anchors:
  - `data/pipeline_runs/`
  - `data/reports/`
  - `logs/`
  - `data/data_view/`

### Memory And Supervision

- Filesystem anchors:
  - `data/claude_bridge/`
  - `data/script/experience_memory/`
  - `CLAUDE_PROJECT_MAP.md`
  - `CLAUDE_RUNTIME_MEMORY.md`

## Operational Contract

Any Pathway reasoning flow should be able to declare:

- `mission`
- `data_domain`
- `verification_plan`
- `knowledge_access_plan`
- `evidence_ledger`
- `coverage_gaps`
- `audit_summary`
- `decision_rule`
- `next_actions`

## Machine-Readable Spec

The living machine-readable spec lives at:

- `config/pathway_data_architecture_v1.json`

The runtime bootstrap surface for this architecture should come from:

- `server_support/pathway_data_architecture.py`
- `GET /api/data-architecture/bootstrap`
- `python scripts/utility/validate_pathway_data_architecture.py --summary-only`

## Why This Exists

This standard makes Pathway data-native.

It does not try to turn Pathway into Claude Code.
It turns Pathway into a system that understands its own medical, insurance, benchmark, runtime, and memory world with the same discipline Claude Code applies to a codebase.
