from __future__ import annotations

import json
import os
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from server_support.paths import DATA_DIR


NOTEBOOKLM_DIR = Path(__file__).resolve().parent.parent
RUNTIME_MEMORY_PATH = NOTEBOOKLM_DIR / "CLAUDE_RUNTIME_MEMORY.md"
INTERACTIONS_DIR = DATA_DIR / "claude_bridge" / "interactions"
FEEDBACK_DIR = DATA_DIR / "claude_bridge" / "feedback"
RUNTIME_MEMORY_SCHEMA_VERSION = "pathway_claude_runtime_memory.v1"
_RUNTIME_MEMORY_CACHE: Dict[str, Any] = {"mtime": None, "text": None}


def _env_int(name: str, default: int) -> int:
    raw_value = os.getenv(name)
    if raw_value is None:
        return default
    try:
        return max(0, int(raw_value))
    except ValueError:
        return default


MAX_MEMORY_CHARS = _env_int("PATHWAY_CLAUDE_RUNTIME_MEMORY_MAX_CHARS", 9000)
MAX_INTERACTIONS = _env_int("PATHWAY_CLAUDE_RUNTIME_MEMORY_INTERACTIONS", 18)
MAX_FEEDBACK_FILES = _env_int("PATHWAY_CLAUDE_RUNTIME_MEMORY_FEEDBACK", 18)
MAX_DECISION_ITEMS = _env_int("PATHWAY_CLAUDE_RUNTIME_MEMORY_DECISIONS", 8)
MAX_DUET_ITEMS = _env_int("PATHWAY_CLAUDE_RUNTIME_MEMORY_DUETS", 6)
MAX_FEEDBACK_ITEMS = _env_int("PATHWAY_CLAUDE_RUNTIME_MEMORY_FEEDBACK_ITEMS", 8)


def _read_json(path: Path) -> Dict[str, Any]:
    try:
        with open(path, "r", encoding="utf-8") as handle:
            payload = json.load(handle)
            if isinstance(payload, dict):
                return payload
    except (OSError, json.JSONDecodeError):
        pass
    return {}


def _trim_text(text: str, max_chars: int) -> str:
    if max_chars <= 0 or len(text) <= max_chars:
        return text
    truncated = text[:max_chars]
    if "\n" in truncated:
        truncated = truncated.rsplit("\n", 1)[0]
    return truncated.rstrip() + "\n\n[Truncated runtime memory]"


def _short(text: Any, max_chars: int = 220) -> str:
    value = str(text or "").strip()
    if not value:
        return ""
    if len(value) <= max_chars:
        return value
    return value[: max_chars - 3].rstrip() + "..."


def _latest_json_files(path: Path, limit: int) -> List[Path]:
    if not path.exists():
        return []
    files = [item for item in path.glob("*.json") if item.is_file()]
    files.sort(key=lambda item: item.stat().st_mtime, reverse=True)
    return files[:limit]


def _normalize_timestamp(raw_value: Any) -> str:
    text = str(raw_value or "").strip()
    if not text:
        return "-"
    return text


def _collect_recent_interactions() -> List[Dict[str, Any]]:
    return [_read_json(path) for path in _latest_json_files(INTERACTIONS_DIR, MAX_INTERACTIONS)]


def _collect_recent_feedback() -> List[Dict[str, Any]]:
    return [_read_json(path) for path in _latest_json_files(FEEDBACK_DIR, MAX_FEEDBACK_FILES)]


def _build_runtime_memory_payload() -> Dict[str, Any]:
    interactions = _collect_recent_interactions()
    feedback_files = _collect_recent_feedback()

    recommended_action_counts: Counter[str] = Counter()
    checkpoint_counts: Counter[str] = Counter()
    final_phase_counts: Counter[str] = Counter()
    outcome_counts: Counter[str] = Counter()
    operator_action_counts: Counter[str] = Counter()

    decision_items: List[Dict[str, str]] = []
    duet_items: List[Dict[str, Any]] = []
    feedback_items: List[Dict[str, str]] = []
    protocol_deltas: List[str] = []

    for interaction in interactions:
        mode = str(interaction.get("mode") or "").strip()
        response_payload = interaction.get("response_payload") or {}
        saved_at = _normalize_timestamp(interaction.get("saved_at"))

        if mode == "decision_gate":
            decision = response_payload.get("decision") or {}
            checkpoint = str(decision.get("checkpoint") or interaction.get("tags", {}).get("checkpoint") or "").strip()
            action = str(decision.get("recommended_action") or "").strip()
            confidence = str(decision.get("confidence") or "").strip()
            reasoning = _short(decision.get("reasoning"), 180)
            next_owner = str(decision.get("next_owner") or "").strip()
            if checkpoint:
                checkpoint_counts[checkpoint] += 1
            if action:
                recommended_action_counts[action] += 1
            if len(decision_items) < MAX_DECISION_ITEMS:
                decision_items.append(
                    {
                        "saved_at": saved_at,
                        "checkpoint": checkpoint or "-",
                        "action": action or "-",
                        "confidence": confidence or "-",
                        "next_owner": next_owner or "-",
                        "reasoning": reasoning or "-",
                    }
                )
            continue

        if mode == "duet":
            final_structured = response_payload.get("final_structured_output") or {}
            final_phase = str(final_structured.get("phase") or "").strip()
            topic = _short(response_payload.get("topic"), 120)
            if final_phase:
                final_phase_counts[final_phase] += 1
            if len(duet_items) < MAX_DUET_ITEMS:
                duet_items.append(
                    {
                        "saved_at": saved_at,
                        "topic": topic or "-",
                        "final_phase": final_phase or "-",
                        "decision": _short(final_structured.get("decision"), 160) or "-",
                        "handoff": _short((final_structured.get("next") or {}).get("request"), 160) or "-",
                    }
                )
            for delta in final_structured.get("protocol_delta") or []:
                delta_text = _short(delta, 140)
                if delta_text and delta_text not in protocol_deltas:
                    protocol_deltas.append(delta_text)

    for feedback in feedback_files:
        interaction_id = str(feedback.get("interaction_id") or "").strip()
        for event in (feedback.get("events") or [])[-3:]:
            event_name = str(event.get("event") or "").strip()
            selected_action = str(event.get("selected_action") or "").strip()
            terminal_status = str(event.get("terminal_status") or "").strip()
            checkpoint = str(event.get("checkpoint") or "").strip()
            if selected_action:
                operator_action_counts[selected_action] += 1
            if terminal_status:
                outcome_counts[terminal_status] += 1
            if len(feedback_items) < MAX_FEEDBACK_ITEMS:
                feedback_items.append(
                    {
                        "recorded_at": _normalize_timestamp(event.get("recorded_at")),
                        "interaction_id": interaction_id or "-",
                        "event": event_name or "-",
                        "checkpoint": checkpoint or "-",
                        "selected_action": selected_action or "-",
                        "terminal_status": terminal_status or "-",
                    }
                )

    stable_reminders: List[str] = []
    if recommended_action_counts.get("pause_for_human_review") or operator_action_counts.get("abort_run"):
        stable_reminders.append(
            "Bias to pause_for_human_review when checkpoint state is ambiguous, schema-risky, or operator intent is under-specified."
        )
    if checkpoint_counts.get("post_design") or checkpoint_counts.get("post_test"):
        stable_reminders.append(
            "Keep decision-gate actions checkpoint-aware: post_design maps to continue_to_ingestion/abort_run; post_test maps to accept_current_result/run_optimization/abort_run."
        )
    if protocol_deltas:
        stable_reminders.append(
            "Preserve explicit ownership, timeout, retry, and termination rules in duet-style orchestration outputs."
        )
    if outcome_counts.get("aborted_by_human_review"):
        stable_reminders.append(
            "Operator aborts are real signals; do not hand-wave away human review outcomes when proposing the next workflow step."
        )
    if not stable_reminders:
        stable_reminders.append(
            "Treat this runtime memory as a supplement to CLAUDE.md and CLAUDE_PROJECT_MAP.md, not a replacement for them."
        )

    return {
        "schema_version": RUNTIME_MEMORY_SCHEMA_VERSION,
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "source_counts": {
            "interactions": len(interactions),
            "feedback_files": len(feedback_files),
        },
        "recommended_action_counts": dict(recommended_action_counts),
        "checkpoint_counts": dict(checkpoint_counts),
        "final_phase_counts": dict(final_phase_counts),
        "outcome_counts": dict(outcome_counts),
        "operator_action_counts": dict(operator_action_counts),
        "stable_reminders": stable_reminders,
        "recent_decision_gate_items": decision_items,
        "recent_duet_items": duet_items,
        "recent_feedback_items": feedback_items,
        "recent_protocol_deltas": protocol_deltas[:8],
    }


def _render_key_value_lines(values: Dict[str, Any]) -> List[str]:
    if not values:
        return ["- none"]
    lines = []
    for key, value in values.items():
        lines.append(f"- {key}: {value}")
    return lines


def render_runtime_memory_markdown(payload: Dict[str, Any]) -> str:
    lines: List[str] = [
        "# CLAUDE_RUNTIME_MEMORY.md",
        "",
        "This file is auto-generated from local Pathway Claude bridge interactions and operator feedback.",
        "Do not treat it as a hand-authored architecture source of truth.",
        "",
        f"- schema_version: {payload.get('schema_version', RUNTIME_MEMORY_SCHEMA_VERSION)}",
        f"- generated_at: {payload.get('generated_at', '-')}",
        "",
        "## Stable Operating Reminders",
    ]
    lines.extend(f"- {item}" for item in (payload.get("stable_reminders") or []))

    lines.extend(["", "## Source Counts"])
    lines.extend(_render_key_value_lines(payload.get("source_counts") or {}))

    lines.extend(["", "## Action Patterns"])
    lines.append("### Recommended Actions")
    lines.extend(_render_key_value_lines(payload.get("recommended_action_counts") or {}))
    lines.append("")
    lines.append("### Operator Actions")
    lines.extend(_render_key_value_lines(payload.get("operator_action_counts") or {}))
    lines.append("")
    lines.append("### Outcome Statuses")
    lines.extend(_render_key_value_lines(payload.get("outcome_counts") or {}))

    lines.extend(["", "## Recent Decision Gate Memory"])
    decision_items = payload.get("recent_decision_gate_items") or []
    if not decision_items:
        lines.append("- none")
    else:
        for item in decision_items:
            lines.append(
                f"- [{item['saved_at']}] checkpoint={item['checkpoint']} action={item['action']} confidence={item['confidence']} next_owner={item['next_owner']}"
            )
            lines.append(f"  reasoning: {item['reasoning']}")

    lines.extend(["", "## Recent Duet Memory"])
    duet_items = payload.get("recent_duet_items") or []
    if not duet_items:
        lines.append("- none")
    else:
        for item in duet_items:
            lines.append(
                f"- [{item['saved_at']}] phase={item['final_phase']} topic={item['topic']}"
            )
            lines.append(f"  decision: {item['decision']}")
            lines.append(f"  handoff: {item['handoff']}")

    lines.extend(["", "## Recent Protocol Deltas"])
    protocol_deltas = payload.get("recent_protocol_deltas") or []
    if not protocol_deltas:
        lines.append("- none")
    else:
        lines.extend(f"- {item}" for item in protocol_deltas)

    lines.extend(["", "## Recent Operator Feedback"])
    feedback_items = payload.get("recent_feedback_items") or []
    if not feedback_items:
        lines.append("- none")
    else:
        for item in feedback_items:
            lines.append(
                f"- [{item['recorded_at']}] event={item['event']} checkpoint={item['checkpoint']} selected_action={item['selected_action']} terminal_status={item['terminal_status']}"
            )

    body = "\n".join(lines).strip() + "\n"
    return _trim_text(body, MAX_MEMORY_CHARS)


def load_runtime_memory_text() -> Optional[str]:
    try:
        stat = RUNTIME_MEMORY_PATH.stat()
    except FileNotFoundError:
        _RUNTIME_MEMORY_CACHE["mtime"] = None
        _RUNTIME_MEMORY_CACHE["text"] = None
        return None

    if _RUNTIME_MEMORY_CACHE.get("mtime") == stat.st_mtime and _RUNTIME_MEMORY_CACHE.get("text") is not None:
        return _RUNTIME_MEMORY_CACHE["text"]

    text = RUNTIME_MEMORY_PATH.read_text(encoding="utf-8").strip()
    text = _trim_text(text, MAX_MEMORY_CHARS)
    _RUNTIME_MEMORY_CACHE["mtime"] = stat.st_mtime
    _RUNTIME_MEMORY_CACHE["text"] = text
    return text


def runtime_memory_status() -> Dict[str, Any]:
    text = load_runtime_memory_text()
    updated_at = None
    if RUNTIME_MEMORY_PATH.exists():
        updated_at = datetime.fromtimestamp(RUNTIME_MEMORY_PATH.stat().st_mtime).isoformat(timespec="seconds")
    return {
        "available": bool(text),
        "path": str(RUNTIME_MEMORY_PATH),
        "chars": len(text or ""),
        "max_chars": MAX_MEMORY_CHARS,
        "updated_at": updated_at,
    }


def refresh_runtime_memory(trigger: str = "manual") -> Dict[str, Any]:
    payload = _build_runtime_memory_payload()
    rendered = render_runtime_memory_markdown(payload)
    RUNTIME_MEMORY_PATH.write_text(rendered, encoding="utf-8")
    _RUNTIME_MEMORY_CACHE["mtime"] = None
    _RUNTIME_MEMORY_CACHE["text"] = None
    status = runtime_memory_status()
    status["trigger"] = trigger
    status["source_counts"] = payload.get("source_counts") or {}
    return status
