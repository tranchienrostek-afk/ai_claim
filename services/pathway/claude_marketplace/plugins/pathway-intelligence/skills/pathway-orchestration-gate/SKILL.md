---
name: pathway-orchestration-gate
description: Use this skill when the request mentions multi-agent orchestration, Claude duet, planner vs challenger roles, decision gate, checkpoints, pause_for_human_review, post_design, post_test, resume_pipeline, abort_run, operator handoff, or workflow control in Pathway. It helps Claude reason like a strict protocol designer instead of a generic brainstormer.
---

# Pathway Orchestration Gate

Use this skill for tightly controlled workflow reasoning inside Pathway.

## Primary Files

Start from these files:

- `/app/server_support/claude_runtime.py`
- `/app/server_support/claude_duet.py`
- `/app/server_support/claude_decision.py`
- `/app/server_support/api_models.py`
- `/app/medical_pipeline_agent/scripts/orchestrator.py`
- `/app/api_server.py`

## Reasoning Frame

Treat Pathway orchestration as checkpointed protocol state, not freeform chat.

For each proposal or analysis, make these decisions explicit:

- what checkpoint or phase you are in
- what artifact or evidence is being evaluated
- what action is permitted next
- what human override exists
- what gets logged or persisted
- what ends, retries, pauses, or aborts the flow

Prefer structured outputs and explicit handoffs over open-ended prose.

## Guardrails

Do not suggest changes that erase these constraints without calling it out:

- operator review points
- schema compatibility for structured Claude outputs
- run audit artifacts in `data/pipeline_runs`
- distinction between duet mode and decision-gate mode
- explicit termination and retry policy

If a proposal lacks timeout, retry cap, or abort behavior, treat it as incomplete.

## Recommended Answer Shape

When the user is designing or debugging orchestration, prefer this order:

1. Current checkpoint or phase
2. Decision or control problem
3. Risks and broken invariants
4. Suggested protocol change
5. Next owner: counterpart, operator, or pipeline
