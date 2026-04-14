---
name: pathway-runtime-context
description: Use this skill when the request mentions Pathway, api_server.py, medical_agent.py, Neo4j, ontology inspection, Graph Forge, dashboard flows, claims insights, duet lab, or asks how the Pathway repo is wired together. It helps Claude reason from the actual Pathway runtime shape instead of treating the repo as a flat code dump.
---

# Pathway Runtime Context

Use this skill to build a grounded model of the Pathway workspace before making architectural claims.

## Start Here

Read these files first when the request depends on current repo behavior:

- `/app/CLAUDE.md`
- `/app/CLAUDE_PROJECT_MAP.md`

Then inspect the nearest runtime files that match the question. Default anchors:

- `/app/api_server.py` for routes, dashboard bootstrap, and API orchestration
- `/app/medical_agent.py` for retrieval and reasoning behavior
- `/app/server_support/` for stores, Claude runtime glue, PDFs, and structured response models
- `/app/static/claims_insights/` for operator and analysis surfaces
- `/app/medical_pipeline_agent/` for ingestion and optimization workflow

## Working Model

Use these boundaries unless the code proves otherwise:

- `api_server.py` is the FastAPI hub
- `medical_agent.py` and `academic_agent.py` own retrieval and answer behavior
- `server_support/claude_runtime.py` is the wrapper that actually invokes Claude CLI
- `server_support/claude_duet.py` and `server_support/claude_decision.py` are specialized orchestration layers, not standalone platforms
- `medical_pipeline_agent/` is a separate ingestion workbench with its own lifecycle

Keep state stores separate in your reasoning:

- `SessionStore` is short-lived in-memory chat history
- `PipelineRunStore` is disk-backed run state and audit output
- `experience_memory.py` is persistent pipeline learning memory

Do not merge these into one vague “memory” concept.

## Output Discipline

When you answer:

- Separate confirmed behavior from inference
- Name the exact file or subsystem that supports each important claim
- Prefer current repo structure over generic framework advice
- If the request spans API, UI, and pipeline layers, say which layer owns which responsibility
