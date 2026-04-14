#!/usr/bin/env python3
"""
Pathway-sensitive file guard for Claude Code.

The first time a session tries to edit a critical Pathway file, block once and
print a focused checklist so the next attempt is more deliberate.
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path


SENSITIVE_RULES = [
    {
        "match": "/app/medical_pipeline_agent/scripts/orchestrator.py",
        "label": "pipeline orchestrator",
        "review_points": [
            "phase transitions, especially post_design and post_test",
            "resume_pipeline paths and checkpoint-specific allowed actions",
            "run_summary.json, decision_gate_<checkpoint>.json, and human_decision_events.json side effects",
            "operator handoff and aborted/paused final statuses",
        ],
    },
    {
        "match": "/app/api_server.py",
        "label": "FastAPI control surface",
        "review_points": [
            "route contracts and response models",
            "decision gate extraction and allowed action mapping",
            "dashboard bootstrap fields used by the operator UI",
            "backward compatibility for paused_for_human_review control flow",
        ],
    },
    {
        "match": "/app/server_support/api_models.py",
        "label": "structured API schema",
        "review_points": [
            "Claude duet and decision gate JSON schema compatibility",
            "dashboard/client expectations for response fields",
            "checkpoint and control-action literals",
        ],
    },
    {
        "match": "/app/server_support/claude_runtime.py",
        "label": "Claude runtime wrapper",
        "review_points": [
            "CLI flags, plugin loading, and project memory injection",
            "status payload fields consumed by the UI and API",
            "effects on headless claude -p invocations in Pathway",
        ],
    },
    {
        "match": "/app/server_support/claude_duet.py",
        "label": "Claude duet runner",
        "review_points": [
            "structured turn schema and repair flow",
            "role prompts, transcript replay, and loop control",
            "compatibility with duet lab UI rendering",
        ],
    },
    {
        "match": "/app/server_support/claude_decision.py",
        "label": "Claude decision gate runner",
        "review_points": [
            "claude_decision_gate.v1 schema behavior",
            "candidate action validation and checkpoint semantics",
            "repair logic and operator-facing reasoning fields",
        ],
    },
    {
        "match": "/app/static/claims_insights/platform_dashboard.html",
        "label": "operator dashboard UI",
        "review_points": [
            "Decision Gate panel rendering and action button mapping",
            "paused, aborted, and completed state handling",
            "human review affordances and confirmation flow",
        ],
    },
]


def _claude_home_dir() -> Path:
    explicit_home = os.getenv("HOME")
    if explicit_home:
        return Path(explicit_home)
    return Path.home()


def state_file(session_id: str) -> Path:
    return _claude_home_dir() / ".claude" / f"pathway_change_guard_{session_id}.json"


def load_seen(session_id: str) -> set[str]:
    path = state_file(session_id)
    if not path.exists():
        return set()
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return set()
    if not isinstance(payload, list):
        return set()
    return {str(item) for item in payload}


def save_seen(session_id: str, seen: set[str]) -> None:
    path = state_file(session_id)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(sorted(seen)), encoding="utf-8")


def normalize_path(raw_path: str) -> str:
    return raw_path.replace("\\", "/").strip()


def match_rule(file_path: str) -> dict | None:
    for rule in SENSITIVE_RULES:
        if file_path.endswith(rule["match"]) or file_path == rule["match"]:
            return rule
    return None


def main() -> int:
    try:
        hook_input = json.loads(sys.stdin.read() or "{}")
    except json.JSONDecodeError:
        return 0

    tool_name = hook_input.get("tool_name") or ""
    if tool_name not in {"Edit", "Write", "MultiEdit"}:
        return 0

    tool_input = hook_input.get("tool_input") or {}
    file_path = normalize_path(str(tool_input.get("file_path") or ""))
    if not file_path:
        return 0

    rule = match_rule(file_path)
    if not rule:
        return 0

    session_id = str(hook_input.get("session_id") or "default")
    seen = load_seen(session_id)
    key = f"{tool_name}:{file_path}"
    if key in seen:
        return 0

    seen.add(key)
    save_seen(session_id, seen)

    checklist = "\n".join(f"- {item}" for item in rule["review_points"])
    message = (
        f"Pathway change guard: you are editing a sensitive {rule['label']} file:\n"
        f"{file_path}\n\n"
        "Pause and verify these surfaces before retrying the edit:\n"
        f"{checklist}\n\n"
        "If the change is still intentional, inspect the related files and retry once you understand the contract impact."
    )
    print(message, file=sys.stderr)
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
