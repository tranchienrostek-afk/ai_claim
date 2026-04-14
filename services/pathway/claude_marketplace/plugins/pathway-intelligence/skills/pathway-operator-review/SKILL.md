---
name: pathway-operator-review
description: Use this skill when the request mentions operator review, human review, paused_for_human_review, dashboard decision gate panel, control endpoint, human_decision_events, review UX, approval flow, allowed_actions, or asks how a human operator should inspect and resume a Pathway run. It helps Claude reason about the operator workflow as a first-class part of the system.
---

# Pathway Operator Review

Use this skill when the user is designing, explaining, or debugging the human-in-the-loop review flow in Pathway.

## Read These Files First

- `/app/api_server.py`
- `/app/medical_pipeline_agent/scripts/orchestrator.py`
- `/app/static/claims_insights/platform_dashboard.html`
- `/app/server_support/api_models.py`

## Mental Model

Treat the operator as a separate actor from Claude and from the pipeline thread.

The operator workflow is not generic approval UI. It is checkpoint-aware review for runs that are:

- `paused_for_human_review`

At that point, the system should expose:

- the current checkpoint
- the latest decision-gate reasoning
- the allowed actions for that checkpoint
- the audit trail of prior human decisions

## Checkpoint Rules

Always reason from checkpoint-specific controls:

- `post_design`: continue to ingestion or abort
- `post_test`: accept current result, run optimization, or abort

Do not collapse these into one generic “approve” button.

## Audit Model

Keep the audit artifacts separate:

- decision-gate snapshots explain what Claude recommended
- `human_decision_events.json` records what the operator actually chose
- `run_summary.json` is the durable roll-up
- dashboard state is a projection of these artifacts, not the source of truth

## Review Checklist

When the user asks if an operator flow is correct, verify:

1. Is the run actually in `paused_for_human_review`?
2. Does the UI show the active checkpoint and allowed actions?
3. Does each action map to a real API path and status transition?
4. Is abort handled distinctly from resume actions?
5. Is the human decision recorded durably?

## Output Style

Prefer answers that separate:

- current review state
- allowed operator actions
- expected result of each action
- audit consequences
- UX or safety gaps
