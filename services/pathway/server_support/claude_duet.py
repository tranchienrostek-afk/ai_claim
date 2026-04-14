from __future__ import annotations

import json
from typing import Any, Dict, List, Optional

from pydantic import ValidationError

from server_support.api_models import ClaudeDuetStructuredTurn
from server_support.claude_bridge import bridge
from server_support.claude_runtime import (
    DEFAULT_MODEL,
    ClaudeCliError,
    ClaudeCliRuntime,
    ClaudeCliUnavailableError,
    env_int,
)


DEFAULT_AGENT_A_NAME = "Claude Strategist"
DEFAULT_AGENT_B_NAME = "Claude Challenger"
TURN_SCHEMA_VERSION = "claude_duet_turn.v1"
TURN_PHASES = (
    "bootstrap",
    "propose",
    "critique",
    "tighten",
    "converge",
    "escalate",
    "terminate",
)
TURN_TERMINATION_SIGNALS = ("continue", "escalate", "terminate")
DEFAULT_AGENT_A_PROMPT = (
    "Ban la Claude Strategist cho Pathway trong che do multi-agent orchestration chat. "
    "Nhiem vu cua ban la chot phase hien tai, de xuat control flow, state transition, "
    "ownership, va handoff tiep theo. Moi luot phai tra ve JSON hop le theo schema duoc yeu cau, "
    "ro quyet dinh, ro protocol delta, ro rui ro, va handoff machine-readable. "
    "Phan hoi bang tieng Viet, tap trung, khong dai dong."
)
DEFAULT_AGENT_B_PROMPT = (
    "Ban la Claude Challenger cho Pathway trong che do multi-agent orchestration chat. "
    "Nhiem vu cua ban la kiem tra invariant, timeout, retry cap, stale state, race condition, "
    "termination rule, va guardrail. Moi luot phai tra ve JSON hop le theo schema duoc yeu cau, "
    "chi ra protocol delta, rui ro that su, va handoff machine-readable. "
    "Phan hoi bang tieng Viet, tap trung, khong dai dong."
)

MAX_FORMAT_REPAIRS = env_int("CLAUDE_DUET_MAX_FORMAT_REPAIRS", 1)


def _transcript_to_text(transcript: List[Dict]) -> str:
    if not transcript:
        return "(Chua co luot nao. Ban la nguoi mo dau.)"
    lines = []
    for turn in transcript:
        index = turn.get("index", "?")
        speaker = turn.get("speaker", "Claude")
        content = (turn.get("content") or "").strip()
        lines.append(f"Luot {index} - {speaker}:\n{content}")
    return "\n\n".join(lines)

def _model_dump(model: Any) -> Dict[str, Any]:
    if hasattr(model, "model_dump"):
        return model.model_dump()
    return model.dict()


def _strip_code_fences(raw_text: str) -> str:
    stripped = raw_text.strip()
    if not stripped.startswith("```"):
        return stripped

    lines = stripped.splitlines()
    if lines and lines[0].strip().startswith("```"):
        lines = lines[1:]
    if lines and lines[-1].strip() == "```":
        lines = lines[:-1]
    return "\n".join(lines).strip()


def _extract_json_payload(raw_text: str) -> Dict[str, Any]:
    stripped = _strip_code_fences(raw_text)
    decoder = json.JSONDecoder()

    for candidate in (stripped, stripped.removeprefix("json").lstrip()):
        if not candidate:
            continue
        try:
            payload = json.loads(candidate)
        except json.JSONDecodeError:
            payload = None
        if isinstance(payload, dict):
            return payload

    for index, char in enumerate(stripped):
        if char != "{":
            continue
        try:
            payload, _ = decoder.raw_decode(stripped[index:])
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict):
            return payload

    raise ClaudeCliError("Claude tra ve noi dung khong parse duoc thanh JSON object.")


def _require_text(value: Any, field_name: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise ClaudeCliError(f"Thieu truong bat buoc: {field_name}")
    return text


def _normalize_string_list(value: Any, field_name: str) -> List[str]:
    if value is None:
        return []
    if isinstance(value, str):
        text = value.strip()
        return [text] if text else []
    if isinstance(value, list):
        normalized = [str(item).strip() for item in value if str(item).strip()]
        return normalized
    raise ClaudeCliError(f"Truong {field_name} phai la chuoi hoac danh sach chuoi.")


def _normalize_turn_payload(raw_payload: Dict[str, Any], counterpart_speaker: str) -> Dict[str, Any]:
    if not isinstance(raw_payload, dict):
        raise ClaudeCliError("Claude phai tra ve mot JSON object cho moi luot.")

    phase = _require_text(raw_payload.get("phase"), "phase").lower()
    if phase not in TURN_PHASES:
        raise ClaudeCliError(f"Phase khong hop le: {phase}")

    next_payload = raw_payload.get("next")
    if isinstance(next_payload, str):
        next_payload = {"target": counterpart_speaker, "request": next_payload}
    if not isinstance(next_payload, dict):
        raise ClaudeCliError("Truong next phai la object hoac chuoi.")

    default_signal = "continue"
    if phase == "escalate":
        default_signal = "escalate"
    elif phase == "terminate":
        default_signal = "terminate"

    termination_signal = str(next_payload.get("termination_signal") or default_signal).strip().lower()
    if termination_signal not in TURN_TERMINATION_SIGNALS:
        termination_signal = default_signal

    normalized = {
        "schema_version": TURN_SCHEMA_VERSION,
        "phase": phase,
        "decision": _require_text(raw_payload.get("decision"), "decision"),
        "why": _require_text(raw_payload.get("why"), "why"),
        "protocol_delta": _normalize_string_list(raw_payload.get("protocol_delta"), "protocol_delta"),
        "risks": _normalize_string_list(raw_payload.get("risks"), "risks"),
        "next": {
            "target": _require_text(next_payload.get("target") or counterpart_speaker, "next.target"),
            "request": _require_text(
                next_payload.get("request") or next_payload.get("message") or next_payload.get("prompt"),
                "next.request",
            ),
            "termination_signal": termination_signal,
        },
    }

    try:
        if hasattr(ClaudeDuetStructuredTurn, "model_validate"):
            structured = ClaudeDuetStructuredTurn.model_validate(normalized)
        else:
            structured = ClaudeDuetStructuredTurn.parse_obj(normalized)
    except ValidationError as exc:
        raise ClaudeCliError(f"JSON schema khong hop le: {exc}") from exc
    return _model_dump(structured)


def _turn_example(counterpart_speaker: str) -> str:
    return json.dumps(
        {
            "schema_version": TURN_SCHEMA_VERSION,
            "phase": "tighten",
            "decision": "Chot 1 protocol delta chinh cho luot nay.",
            "why": "Giam mo ho va de counterpart tiep tuc de dang.",
            "protocol_delta": [
                "Moi luot phai cap nhat mot state transition hoac guardrail ro rang."
            ],
            "risks": [
                "Neu handoff mo ho, doi phuong se lap lai y cu."
            ],
            "next": {
                "target": counterpart_speaker,
                "request": "Kiem tra invariant va neu can thi thiet lap timeout/retry cap.",
                "termination_signal": "continue",
            },
        },
        ensure_ascii=False,
        indent=2,
    )


class ClaudeDuetRunner(ClaudeCliRuntime):

    def _build_turn_prompt(
        self,
        *,
        topic: str,
        context: Optional[str],
        history_text: str,
        current: Dict[str, str],
        counterpart: Dict[str, str],
        max_output_chars: int,
        bridge_context: Optional[str] = None,
    ) -> str:
        return "\n\n".join(
            part
            for part in [
                f"Chu de trung tam:\n{topic.strip()}",
                f"Boi canh them:\n{context.strip()}" if context else "",
                bridge_context.strip() if bridge_context else "",
                f"Hoi thoai hien tai:\n{history_text}",
                (
                    f"Ban la {current['speaker']}. Doi phuong la {counterpart['speaker']}.\n"
                    "Hay viet mot luot phan hoi moi de day cuoc trao doi tien len."
                ),
                (
                    "Yeu cau bat buoc:\n"
                    "- Chi tra ve DUY NHAT 1 JSON object hop le. Khong markdown, khong code fence, khong giai thich them.\n"
                    f"- schema_version phai la {TURN_SCHEMA_VERSION}.\n"
                    f"- phase phai thuoc tap: {', '.join(TURN_PHASES)}.\n"
                    "- protocol_delta va risks phai la mang chuoi.\n"
                    f"- next.target uu tien ban giao cho {counterpart['speaker']}.\n"
                    f"- next.termination_signal phai thuoc tap: {', '.join(TURN_TERMINATION_SIGNALS)}.\n"
                    "- Moi luot phai chot 1 quyet dinh hoac 1 protocol change ro rang.\n"
                    "- Neu dong y, van phai them it nhat 1 refinement ve timeout, ownership, schema, retry, hoac termination.\n"
                    f"- Giu tong phan hoi trong khoang {max_output_chars} ky tu.\n"
                    "- Muc tieu la multi-agent orchestration chat, khong brainstorm mo.\n"
                    "- Mau JSON tham khao:\n"
                    f"{_turn_example(counterpart['speaker'])}"
                ),
            ]
            if part
        )

    def _build_repair_prompt(
        self,
        *,
        base_prompt: str,
        raw_content: str,
        format_error: str,
    ) -> str:
        return "\n\n".join(
            [
                base_prompt,
                "Noi dung vua tra ve KHONG hop le. Sua lai ngay theo cung schema.",
                f"Loi dinh dang: {format_error}",
                f"Noi dung khong hop le:\n{raw_content}",
                "Nhac lai: chi tra ve 1 JSON object hop le, khong markdown, khong code fence, khong van ban ben ngoai JSON.",
            ]
        )

    def _invoke_structured_turn(
        self,
        *,
        topic: str,
        context: Optional[str],
        transcript: List[Dict],
        current: Dict[str, str],
        counterpart: Dict[str, str],
        model: str,
        max_output_chars: int,
        bridge_context: Optional[str] = None,
    ) -> Dict[str, Any]:
        history_text = _transcript_to_text(transcript)
        base_prompt = self._build_turn_prompt(
            topic=topic,
            context=context,
            history_text=history_text,
            current=current,
            counterpart=counterpart,
            max_output_chars=max_output_chars,
            bridge_context=bridge_context,
        )

        last_error = "Khong ro loi dinh dang."
        total_duration_ms = 0.0
        raw_content = ""
        for attempt in range(MAX_FORMAT_REPAIRS + 1):
            prompt = base_prompt
            if attempt:
                prompt = self._build_repair_prompt(
                    base_prompt=base_prompt,
                    raw_content=raw_content,
                    format_error=last_error,
                )

            # In this container runtime, Claude's native --json-schema flag returned blank stdout,
            # so we keep schema enforcement here via prompt + parse + repair.
            result = self.invoke(
                prompt,
                system_prompt=current["system_prompt"],
                model=model,
            )
            total_duration_ms += float(result["duration_ms"])
            raw_content = result["content"]

            try:
                structured = _normalize_turn_payload(
                    _extract_json_payload(raw_content),
                    counterpart["speaker"],
                )
            except ClaudeCliError as exc:
                last_error = str(exc)
                continue

            canonical_content = json.dumps(structured, ensure_ascii=False, indent=2)
            return {
                "speaker": current["speaker"],
                "role": current["role"],
                "content": canonical_content,
                "raw_content": raw_content,
                "structured": structured,
                "duration_ms": round(total_duration_ms, 1),
                "repair_attempts": attempt,
            }

        raise ClaudeCliError(
            f"{current['speaker']} khong tra ve dung JSON schema sau {MAX_FORMAT_REPAIRS + 1} lan. Loi cuoi: {last_error}"
        )

    def run_duet(
        self,
        *,
        topic: str,
        context: Optional[str] = None,
        turns: int = 4,
        model: Optional[str] = None,
        agent_a_name: Optional[str] = None,
        agent_a_prompt: Optional[str] = None,
        agent_b_name: Optional[str] = None,
        agent_b_prompt: Optional[str] = None,
        max_output_chars: int = 1200,
    ) -> Dict:
        transcript: List[Dict] = []
        chosen_model = (model or DEFAULT_MODEL).strip()
        speaker_defs = [
            {
                "role": "agent_a",
                "speaker": (agent_a_name or DEFAULT_AGENT_A_NAME).strip(),
                "system_prompt": (agent_a_prompt or DEFAULT_AGENT_A_PROMPT).strip(),
            },
            {
                "role": "agent_b",
                "speaker": (agent_b_name or DEFAULT_AGENT_B_NAME).strip(),
                "system_prompt": (agent_b_prompt or DEFAULT_AGENT_B_PROMPT).strip(),
            },
        ]

        for index in range(turns):
            current = speaker_defs[index % 2]
            counterpart = speaker_defs[(index + 1) % 2]
            latest_structured = transcript[-1].get("structured") if transcript else {}
            turn_request_packet = bridge.build_request_packet(
                mode="duet_turn",
                objective=topic.strip(),
                context=context,
                state={
                    "turn_index": index + 1,
                    "turn_budget": turns,
                    "completed_turns": len(transcript),
                    "current_speaker": current["speaker"],
                    "counterpart_speaker": counterpart["speaker"],
                    "last_phase": (latest_structured or {}).get("phase"),
                },
                evidence={
                    "history_turns": len(transcript),
                    "recent_handoff": (latest_structured or {}).get("next"),
                },
                response_contract={
                    "schema_version": TURN_SCHEMA_VERSION,
                    "phases": list(TURN_PHASES),
                    "termination_signals": list(TURN_TERMINATION_SIGNALS),
                    "target_handoff": counterpart["speaker"],
                },
                metadata={
                    "agent_role": current["role"],
                    "agent_name": current["speaker"],
                },
            )
            turn_payload = self._invoke_structured_turn(
                topic=topic,
                context=context,
                transcript=transcript,
                current=current,
                counterpart=counterpart,
                model=chosen_model,
                max_output_chars=max_output_chars,
                bridge_context=bridge.render_request_packet(turn_request_packet),
            )
            transcript.append(
                {
                    "index": index + 1,
                    "speaker": turn_payload["speaker"],
                    "role": turn_payload["role"],
                    "content": turn_payload["content"],
                    "raw_content": turn_payload["raw_content"],
                    "structured": turn_payload["structured"],
                    "duration_ms": turn_payload["duration_ms"],
                    "repair_attempts": turn_payload["repair_attempts"],
                }
            )

        final_output = transcript[-1]["content"] if transcript else ""
        final_structured_output = transcript[-1].get("structured") if transcript else None
        session_request_packet = bridge.build_request_packet(
            mode="duet",
            objective=topic.strip(),
            context=context,
            state={
                "turn_budget": turns,
                "completed_turns": len(transcript),
                "speakers": [speaker["speaker"] for speaker in speaker_defs],
                "phases": [
                    turn.get("structured", {}).get("phase")
                    for turn in transcript
                    if isinstance(turn.get("structured"), dict)
                ],
            },
            evidence={
                "final_handoff": (final_structured_output or {}).get("next"),
                "termination_signal": ((final_structured_output or {}).get("next") or {}).get("termination_signal"),
            },
            response_contract={
                "schema_version": TURN_SCHEMA_VERSION,
                "per_turn_json": True,
                "termination_signals": list(TURN_TERMINATION_SIGNALS),
            },
            metadata={
                "agent_a_name": speaker_defs[0]["speaker"],
                "agent_b_name": speaker_defs[1]["speaker"],
                "model": chosen_model,
            },
        )
        claude_status = self.status()
        bridge_trace = None
        try:
            bridge_trace = bridge.persist_interaction(
                mode="duet",
                request_packet=session_request_packet,
                response_payload={
                    "topic": topic,
                    "turns": turns,
                    "final_output": final_output,
                    "final_structured_output": final_structured_output,
                    "transcript": transcript,
                    "schema_version": TURN_SCHEMA_VERSION,
                },
                prompt_preview=bridge.render_request_packet(session_request_packet),
                system_prompt_preview="\n\n".join(
                    [
                        f"{speaker['speaker']} ({speaker['role']}):\n{speaker['system_prompt']}"
                        for speaker in speaker_defs
                    ]
                ),
                model=chosen_model,
                claude_status=claude_status,
                tags={
                    "agent_a_name": speaker_defs[0]["speaker"],
                    "agent_b_name": speaker_defs[1]["speaker"],
                    "final_phase": (final_structured_output or {}).get("phase"),
                },
            )
        except Exception as exc:
            bridge_trace = {
                "schema_version": "pathway_claude_bridge.v1",
                "mode": "duet",
                "error": str(exc),
            }

        return {
            "topic": topic,
            "model": chosen_model,
            "turns": turns,
            "final_output": final_output,
            "final_structured_output": final_structured_output,
            "transcript": transcript,
            "schema_version": TURN_SCHEMA_VERSION,
            "claude_status": claude_status,
            "bridge_trace": bridge_trace,
        }
