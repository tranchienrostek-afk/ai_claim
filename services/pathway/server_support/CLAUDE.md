# CLAUDE.md

This folder contains the support layer behind the main Pathway app runtime.

Path:

- `pathway/notebooklm/server_support/`

This is where structured Claude integration, stores, and supporting runtime contracts live.

## Primary Responsibility

Own the code that helps `api_server.py` do its job, especially:

- Claude runtime invocation
- duet and decision-gate execution
- bridge traces and runtime memory
- session, pipeline, and graph support stores
- API-facing schemas shared with the app

## Key Files

- `claude_runtime.py`
- `claude_bridge.py`
- `claude_decision.py`
- `claude_duet.py`
- `claude_workspace_memory.py`
- `api_models.py`
- `session_store.py`
- `pipeline_store.py`
- `claims_insights_graph_store.py`
- `ontology_v2_inspector_store.py`
- `testcase_trace_runner.py`

## Change Here When

- Claude behavior in Pathway should change
- a structured response contract needs to evolve
- evidence packing or bridge logging changes
- a supporting store or runtime utility needs modification

## Change Elsewhere When

- the main route or HTTP surface changes a lot -> also update `api_server.py`
- the user-facing dashboard or browser behavior changes -> update `static/`
- ingestion workflow logic changes -> inspect `medical_pipeline_agent/`

## Local Rules

- keep contracts structured and auditable
- prefer one clear schema over multiple ad-hoc text outputs
- preserve evidence provenance
- avoid hiding important policy in scattered prompt strings
- if a change affects runtime behavior, note whether it impacts the next Claude invocation or requires API restart

## Operational Note

Most Python changes in this folder require restarting the Pathway `api` service.
Prompt/plugin-only changes elsewhere may take effect on the next Claude call, but changes here usually affect backend runtime logic.
