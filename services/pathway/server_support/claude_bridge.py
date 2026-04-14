from __future__ import annotations

import json
import os
import re
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

from server_support.paths import DATA_DIR
from server_support.claude_workspace_memory import refresh_runtime_memory, runtime_memory_status


BRIDGE_SCHEMA_VERSION = "pathway_claude_bridge.v1"
REQUEST_SCHEMA_VERSION = "pathway_claude_request.v1"
INTERACTION_SCHEMA_VERSION = "pathway_claude_interaction.v1"
FEEDBACK_SCHEMA_VERSION = "pathway_claude_feedback.v1"
BRIDGE_ROOT = DATA_DIR / "claude_bridge"
INTERACTIONS_DIR = BRIDGE_ROOT / "interactions"
FEEDBACK_DIR = BRIDGE_ROOT / "feedback"


def _env_int(name: str, default: int) -> int:
    raw_value = os.getenv(name)
    if raw_value is None:
        return default
    try:
        return max(0, int(raw_value))
    except ValueError:
        return default


REQUEST_PACKET_MAX_CHARS = _env_int("PATHWAY_CLAUDE_BRIDGE_PACKET_MAX_CHARS", 4000)
SCALAR_TEXT_MAX_CHARS = _env_int("PATHWAY_CLAUDE_BRIDGE_SCALAR_MAX_CHARS", 240)
PROMPT_PREVIEW_MAX_CHARS = _env_int("PATHWAY_CLAUDE_BRIDGE_PROMPT_PREVIEW_MAX_CHARS", 4000)
RESPONSE_PREVIEW_MAX_CHARS = _env_int("PATHWAY_CLAUDE_BRIDGE_RESPONSE_PREVIEW_MAX_CHARS", 6000)


def _now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _ensure_dir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def _write_json(path: Path, payload: Dict[str, Any]) -> None:
    _ensure_dir(path.parent)
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)


def _read_json(path: Path, default: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    try:
        with open(path, "r", encoding="utf-8") as handle:
            data = json.load(handle)
            if isinstance(data, dict):
                return data
    except (OSError, json.JSONDecodeError):
        pass
    return dict(default or {})


def _trim_text(text: str, max_chars: int) -> str:
    if max_chars <= 0 or len(text) <= max_chars:
        return text
    truncated = text[:max_chars]
    if "\n" in truncated:
        truncated = truncated.rsplit("\n", 1)[0]
    return truncated.rstrip() + "\n\n[Truncated for bridge budget]"


def _slugify(text: str, *, max_chars: int = 48) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", text.strip().lower())
    slug = slug.strip("-")
    if not slug:
        slug = "interaction"
    return slug[:max_chars].rstrip("-")


def _summarize_value(value: Any, *, depth: int = 0) -> Any:
    if value is None:
        return None
    if isinstance(value, (bool, int, float)):
        return value
    if isinstance(value, str):
        return _trim_text(value.strip(), SCALAR_TEXT_MAX_CHARS)

    if depth >= 2:
        if isinstance(value, dict):
            return {"type": "dict", "keys": list(value.keys())[:8]}
        if isinstance(value, list):
            return {"type": "list", "len": len(value)}
        return str(value)

    if isinstance(value, list):
        return {
            "type": "list",
            "len": len(value),
            "sample": [_summarize_value(item, depth=depth + 1) for item in value[:3]],
        }

    if isinstance(value, dict):
        items = list(value.items())
        summary: Dict[str, Any] = {}
        for key, item in items[:8]:
            summary[str(key)] = _summarize_value(item, depth=depth + 1)
        if len(items) > 8:
            summary["_omitted_keys"] = len(items) - 8
        return summary

    return str(value)


def _latest_file(path: Path) -> Optional[Path]:
    files = [item for item in path.glob("*.json") if item.is_file()]
    if not files:
        return None
    return max(files, key=lambda item: item.stat().st_mtime)


class PathwayClaudeBridge:
    def __init__(self, root_dir: Path = BRIDGE_ROOT) -> None:
        self.root_dir = root_dir
        self.interactions_dir = self.root_dir / "interactions"
        self.feedback_dir = self.root_dir / "feedback"

    def build_request_packet(
        self,
        *,
        mode: str,
        objective: str,
        context: Optional[str] = None,
        state: Optional[Dict[str, Any]] = None,
        evidence: Optional[Dict[str, Any]] = None,
        response_contract: Optional[Dict[str, Any]] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        packet = {
            "schema_version": REQUEST_SCHEMA_VERSION,
            "mode": mode,
            "objective": (objective or "").strip(),
            "context": _trim_text((context or "").strip(), 1200) or None,
            "state_summary": _summarize_value(state or {}),
            "evidence": _summarize_value(evidence or {}),
            "response_contract": response_contract or {},
            "metadata": metadata or {},
            "created_at": _now_iso(),
        }
        return {key: value for key, value in packet.items() if value not in (None, {}, [])}

    def render_request_packet(self, packet: Dict[str, Any]) -> str:
        body = json.dumps(packet, ensure_ascii=False, indent=2)
        body = _trim_text(body, REQUEST_PACKET_MAX_CHARS)
        return "\n".join(
            [
                "Pathway structured interaction packet:",
                "- Treat the packet fields as the authoritative interface state for this call.",
                "- Prefer packet facts over broad guesses.",
                "- If a key is missing or weak, mention the uncertainty explicitly instead of inventing detail.",
                body,
            ]
        )

    def persist_interaction(
        self,
        *,
        mode: str,
        request_packet: Dict[str, Any],
        response_payload: Dict[str, Any],
        prompt_preview: Optional[str] = None,
        system_prompt_preview: Optional[str] = None,
        model: Optional[str] = None,
        claude_status: Optional[Dict[str, Any]] = None,
        tags: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        _ensure_dir(self.interactions_dir)
        interaction_id = f"{mode}-{datetime.now().strftime('%Y%m%d%H%M%S')}-{uuid.uuid4().hex[:8]}"
        interaction_path = self.interactions_dir / f"{interaction_id}.json"
        feedback_path = self.feedback_dir / f"{interaction_id}.json"
        payload = {
            "schema_version": INTERACTION_SCHEMA_VERSION,
            "bridge_schema_version": BRIDGE_SCHEMA_VERSION,
            "interaction_id": interaction_id,
            "mode": mode,
            "saved_at": _now_iso(),
            "tags": tags or {},
            "request_packet": request_packet,
            "response_payload": response_payload,
            "prompt_preview": _trim_text((prompt_preview or "").strip(), PROMPT_PREVIEW_MAX_CHARS) or None,
            "system_prompt_preview": _trim_text((system_prompt_preview or "").strip(), PROMPT_PREVIEW_MAX_CHARS) or None,
            "model": model,
            "claude_status": claude_status,
            "feedback_path": str(feedback_path),
        }
        _write_json(interaction_path, payload)
        memory_status = None
        try:
            memory_status = refresh_runtime_memory(trigger=f"interaction:{mode}")
        except Exception:
            memory_status = None
        return {
            "schema_version": BRIDGE_SCHEMA_VERSION,
            "interaction_id": interaction_id,
            "mode": mode,
            "interaction_path": str(interaction_path),
            "feedback_path": str(feedback_path),
            "saved_at": payload["saved_at"],
            "runtime_memory": memory_status,
        }

    def record_feedback(self, interaction_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        _ensure_dir(self.feedback_dir)
        feedback_path = self.feedback_dir / f"{interaction_id}.json"
        existing = _read_json(
            feedback_path,
            {
                "schema_version": FEEDBACK_SCHEMA_VERSION,
                "bridge_schema_version": BRIDGE_SCHEMA_VERSION,
                "interaction_id": interaction_id,
                "events": [],
            },
        )
        events = list(existing.get("events") or [])
        event = dict(payload)
        event["recorded_at"] = _now_iso()
        events.append(event)
        existing["events"] = events
        existing["updated_at"] = event["recorded_at"]
        _write_json(feedback_path, existing)
        memory_status = None
        try:
            memory_status = refresh_runtime_memory(trigger="feedback")
        except Exception:
            memory_status = None
        return {
            "schema_version": BRIDGE_SCHEMA_VERSION,
            "interaction_id": interaction_id,
            "feedback_path": str(feedback_path),
            "feedback_events": len(events),
            "updated_at": existing["updated_at"],
            "runtime_memory": memory_status,
        }

    def status(self) -> Dict[str, Any]:
        _ensure_dir(self.interactions_dir)
        _ensure_dir(self.feedback_dir)
        latest_interaction = _latest_file(self.interactions_dir)
        latest_feedback = _latest_file(self.feedback_dir)
        return {
            "available": True,
            "schema_version": BRIDGE_SCHEMA_VERSION,
            "root": str(self.root_dir),
            "request_packet_max_chars": REQUEST_PACKET_MAX_CHARS,
            "interactions": {
                "count": len(list(self.interactions_dir.glob("*.json"))),
                "latest_path": str(latest_interaction) if latest_interaction else None,
            },
            "feedback": {
                "count": len(list(self.feedback_dir.glob("*.json"))),
                "latest_path": str(latest_feedback) if latest_feedback else None,
            },
            "runtime_memory": runtime_memory_status(),
        }

    def feedback_file_for(self, interaction_id: str) -> Path:
        _ensure_dir(self.feedback_dir)
        return self.feedback_dir / f"{interaction_id}.json"

    def objective_slug(self, objective: str) -> str:
        return _slugify(objective)

    def response_preview(self, response_payload: Dict[str, Any]) -> str:
        raw = json.dumps(response_payload, ensure_ascii=False, indent=2)
        return _trim_text(raw, RESPONSE_PREVIEW_MAX_CHARS)


bridge = PathwayClaudeBridge()


def get_bridge_status() -> Dict[str, Any]:
    return bridge.status()


def record_bridge_feedback(interaction_id: Optional[str], payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if not interaction_id:
        return None
    return bridge.record_feedback(interaction_id, payload)
