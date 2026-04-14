---
name: pathway-change-safety
description: Use this skill when the request involves editing, refactoring, extending, or debugging the Pathway codebase itself, especially around api_server.py, server_support, dashboard UI, duet runtime, decision gate, or the medical pipeline agent. It helps Claude preserve contracts, operator flows, and project memory while making changes.
---

# Pathway Change Safety

Use this skill before proposing or applying code changes in Pathway.

## Pre-Edit Checklist

Map the change across these surfaces before editing:

- API contract
- structured response schema
- dashboard or operator UI expectations
- pipeline checkpoint behavior
- audit or persistence side effects
- Docker mount or runtime path assumptions

If the change touches Claude integration, always inspect:

- `/app/server_support/claude_runtime.py`
- `/app/server_support/claude_duet.py`
- `/app/server_support/claude_decision.py`
- `/app/server_support/api_models.py`

If the change touches pipeline control, inspect:

- `/app/medical_pipeline_agent/scripts/orchestrator.py`
- `/app/api_server.py`

## Change Rules

- Preserve existing structured output fields unless there is a deliberate migration
- Keep operator pause, continue, optimize, and abort flows coherent
- Avoid introducing hidden shared state when explicit run state already exists
- Update project memory files when architecture meaningfully changes

Project memory files to update when needed:

- `/app/CLAUDE.md`
- `/app/CLAUDE_PROJECT_MAP.md`

## Verification

Prefer targeted verification over hand-waving:

- route-level smoke tests for API behavior
- structured output validation for duet or decision-gate responses
- checkpoint-aware tests for pipeline control paths
- container/runtime checks when the change depends on mounted files or CLI behavior
