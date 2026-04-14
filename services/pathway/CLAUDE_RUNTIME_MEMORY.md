# CLAUDE_RUNTIME_MEMORY.md

This file is auto-generated from local Pathway Claude bridge interactions and operator feedback.
Do not treat it as a hand-authored architecture source of truth.

- schema_version: pathway_claude_runtime_memory.v1
- generated_at: 2026-04-06T03:48:52

## Stable Operating Reminders
- Bias to pause_for_human_review when checkpoint state is ambiguous, schema-risky, or operator intent is under-specified.
- Keep decision-gate actions checkpoint-aware: post_design maps to continue_to_ingestion/abort_run; post_test maps to accept_current_result/run_optimization/abort_run.
- Preserve explicit ownership, timeout, retry, and termination rules in duet-style orchestration outputs.
- Operator aborts are real signals; do not hand-wave away human review outcomes when proposing the next workflow step.

## Source Counts
- interactions: 7
- feedback_files: 1

## Action Patterns
### Recommended Actions
- pause_for_human_review: 2

### Operator Actions
- abort_run: 2

### Outcome Statuses
- aborted_by_human_review: 1

## Recent Decision Gate Memory
- [2026-04-02T17:01:24] checkpoint=post_design action=pause_for_human_review confidence=high next_owner=human_operator
  reasoning: Experience advice flag 'operator ambiguity' cho thay intent cua operator chua ro rang o lan chay gan nhat. Runtime memory xac nhan 2 lan abort lien tiep boi human review. Voi ti...
- [2026-04-02T16:19:29] checkpoint=post_test action=pause_for_human_review confidence=high next_owner=human_operator
  reasoning: Accuracy 72% thap hon target 85% (chenh 13 diem). Test FAILED voi 2 high-risk topics (schema drift, operator ambiguity) — ca hai deu anh huong truc tiep den do tin cay cua graph...

## Recent Duet Memory
- [2026-04-02T16:20:32] phase=critique topic=Tighten the orchestration contract between planner and reviewer for Pathway decision gates.
  decision: Timeout 30s khong du cho reviewer reasoning phuc tap. Tang len 45s. Retry cap la 2 lan, lan thu 3 bat buoc escalate to human operator.
  handoff: Xac nhan timeout 45s+15s backoff va retry cap 2. Chot converge neu dong y, hoac de xuat thay doi cu the neu khong.

## Recent Protocol Deltas
- Timeout reviewer: 30s -> 45s, auto-pause neu qua han, log event {type: reviewer_timeout, run_id, elapsed_ms}
- Retry cap: max 2 lan retry voi cung checkpoint_type + run_id, lan thu 3 signal bat buoc la escalate
- Moi retry phai tang timeout them 15s (45s, 60s) de tranh cascade timeout
- Khi escalate, payload gui human operator phai gom: retry_count, last_error, full reasoning trace
- Planner KHONG duoc tu dong proceed neu reviewer timeout, chi duoc pause hoac escalate

## Recent Operator Feedback
- [2026-04-02T16:22:43] event=operator_action_selected checkpoint=post_design selected_action=abort_run terminal_status=-
- [2026-04-02T16:22:44] event=operator_outcome checkpoint=post_design selected_action=abort_run terminal_status=aborted_by_human_review
