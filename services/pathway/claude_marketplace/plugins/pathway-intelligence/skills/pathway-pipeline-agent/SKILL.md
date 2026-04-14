---
name: pathway-pipeline-agent
description: Use this skill when the request mentions medical_pipeline_agent, orchestrator.py, experience_memory.py, pipeline phases, Phase 0, Phase 1, Phase 2, Phase 3, Phase 4, post_design, post_test, paused_for_human_review, run_summary.json, decision_gate events, resume_pipeline, optimization loops, or asks how the Pathway ingestion workflow actually behaves. It helps Claude reason from the real ingestion pipeline state machine instead of hand-waving about generic ETL.
---

# Pathway Pipeline Agent

Use this skill when the user is talking about the ingestion workbench inside Pathway.

## First Files To Read

Start from:

- `/app/medical_pipeline_agent/scripts/orchestrator.py`
- `/app/medical_pipeline_agent/scripts/experience_memory.py`
- `/app/server_support/claude_decision.py`
- `/app/server_support/api_models.py`
- `/app/api_server.py`

Use `/app/CLAUDE.md` and `/app/CLAUDE_PROJECT_MAP.md` to anchor the broader workspace model, but let the pipeline files decide the details.

## Pipeline Mental Model

Treat `medical_pipeline_agent/` as a checkpointed workflow, not a loose collection of helpers.

Core phases in `orchestrator.py`:

- Phase 0: PDF analysis
- Phase 1: pipeline design
- Phase 2: ingestion
- Phase 3: quality testing
- Phase 4: self-improvement and optimization when needed

Important checkpoints:

- `post_design` happens after design, before ingestion writes to the graph
- `post_test` happens after quality testing, before optimization

The decision gate may continue, pause for human review, or abort depending on checkpoint and risk.

## State And Persistence

Keep these artifacts separate in your reasoning:

- `run_summary.json` is the main roll-up of a pipeline run
- `decision_gate_<checkpoint>.json` stores a specific gate decision snapshot
- `human_decision_events.json` records operator override actions
- websocket logs stream live execution but are not the same as the durable summary

Do not compress these into one vague “pipeline state file”.

## Operator Handoff Model

When a run is `paused_for_human_review`, operator control is checkpoint-aware.

Typical control logic:

- `post_design`: continue to ingestion or abort
- `post_test`: accept current result, run optimization, or abort

When describing control flow, always say:

- which checkpoint is active
- which actions are allowed there
- what status transition follows each action
- whether the action resumes in a background thread or terminates immediately

## Experience Memory

`experience_memory.py` is persistent learning memory for the pipeline, backed by Neo4j and embeddings.

Use it as:

- prior-run retrieval before or during design/improvement
- storage for lessons, ontology templates, prompt versions, and pipeline outcomes

Do not describe it as generic chat memory for all Claude sessions.

## Optimization And Safety

When the question touches Phase 4 or self-improvement:

- call out loop limits and rollback expectations
- distinguish improvement proposals from already-applied changes
- separate smoke-test evidence from production confidence

If a proposal changes pipeline behavior, mention the audit and operator-review consequences.

## Recommended Answer Shape

When explaining the pipeline, prefer this order:

1. current phase or checkpoint
2. inputs and persisted state
3. allowed transitions
4. operator or Claude decision role
5. audit artifacts and risk notes
