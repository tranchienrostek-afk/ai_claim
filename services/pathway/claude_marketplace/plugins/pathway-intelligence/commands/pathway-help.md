---
description: Show the Pathway project plugin surfaces and when to use them.
---

# Pathway Help

State clearly that the project plugin `pathway-intelligence` is active for this workspace.

Then summarize these plugin capabilities in a short list:

- `pathway-runtime-context`: use for architecture, API, graph, and UI reasoning inside the Pathway repo.
- `pathway-orchestration-gate`: use for Claude duet, decision-gate, checkpoint, and operator handoff reasoning.
- `pathway-pipeline-agent`: use for Phase 0-4 ingestion workflow, run summaries, resume logic, and experience memory reasoning.
- `pathway-operator-review`: use for paused human-review runs, dashboard controls, audit trail, and operator resume logic.
- `pathway-change-safety`: use before editing or refactoring Pathway code so contracts, schemas, and operator flows stay intact.

Close by telling the user that the fastest deep-context files are:

- `/app/CLAUDE.md`
- `/app/CLAUDE_PROJECT_MAP.md`
- `/app/api_server.py`
- `/app/server_support/claude_runtime.py`
- `/app/server_support/claude_duet.py`
- `/app/server_support/claude_decision.py`
- `/app/medical_pipeline_agent/scripts/orchestrator.py`
- `/app/medical_pipeline_agent/scripts/experience_memory.py`
