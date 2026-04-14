from __future__ import annotations

import json
from typing import Any, Dict, List, Optional

from pydantic import ValidationError

from server_support.api_models import ClaudeDecisionGateResult
from server_support.claude_bridge import bridge
from server_support.claude_runtime import DEFAULT_MODEL, ClaudeCliError, ClaudeCliRuntime, env_int


DECISION_SCHEMA_VERSION = "claude_decision_gate.v1"
DEFAULT_DECISION_ACTIONS = [
    "continue",
    "pause_for_human_review",
    "abort_run",
]
DEFAULT_DECISION_SYSTEM_PROMPT = (
    "Ban la Claude Decision Gate cho Pathway. "
    "Ban ngoi tai mot checkpoint trong workflow va phai chon duy nhat 1 hanh dong tiep theo. "
    "Ban uu tien an toan van hanh, reviewability, chi phi hop ly, va handoff ro rang. "
    "Neu state mo ho, rui ro cao, hoac thieu du lieu de tu tin tiep tuc, nghieng ve pause_for_human_review. "
    "Tra ve DUY NHAT 1 JSON object hop le, khong van ban ben ngoai JSON."
)
MAX_DECISION_REPAIRS = env_int("CLAUDE_DECISION_MAX_REPAIRS", 1)
STATE_PROMPT_MAX_CHARS = env_int("CLAUDE_DECISION_STATE_MAX_CHARS", 10000)


def _model_dump(model: Any) -> Dict[str, Any]:
    if hasattr(model, "model_dump"):
        return model.model_dump()
    return model.dict()


def _truncate_text(text: str, max_chars: int) -> str:
    if max_chars <= 0 or len(text) <= max_chars:
        return text
    truncated = text[:max_chars]
    if "\n" in truncated:
        truncated = truncated.rsplit("\n", 1)[0]
    return truncated.rstrip() + "\n\n[Truncated state for prompt budget]"


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

    raise ClaudeCliError("Claude Decision Gate tra ve noi dung khong parse duoc thanh JSON object.")


def _require_text(value: Any, field_name: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise ClaudeCliError(f"Thieu truong bat buoc: {field_name}")
    return text


def _normalize_string_list(value: Any) -> List[str]:
    if value is None:
        return []
    if isinstance(value, str):
        text = value.strip()
        return [text] if text else []
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    raise ClaudeCliError("Truong danh sach phai la chuoi hoac danh sach chuoi.")


def _normalize_bool(value: Any, default: bool) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in {"true", "1", "yes", "y"}:
        return True
    if text in {"false", "0", "no", "n"}:
        return False
    return default


def _canonicalize_action(value: Any, candidate_actions: List[str]) -> str:
    text = _require_text(value, "recommended_action")
    action_map = {action.lower(): action for action in candidate_actions}
    if text.lower() in action_map:
        return action_map[text.lower()]
    raise ClaudeCliError(
        f"recommended_action phai nam trong tap candidate_actions: {', '.join(candidate_actions)}"
    )


def _normalize_stop_signal(value: Any, action: str, proceed: bool, needs_human_review: bool) -> str:
    allowed = {"continue", "pause", "terminate"}
    if value is not None:
        text = str(value).strip().lower()
        if text in allowed:
            return text
    if needs_human_review or action == "pause_for_human_review":
        return "pause"
    if not proceed or action == "abort_run":
        return "terminate"
    return "continue"


def _state_to_prompt(state: Dict[str, Any]) -> str:
    text = json.dumps(state, ensure_ascii=False, indent=2, default=str)
    return _truncate_text(text, STATE_PROMPT_MAX_CHARS)


def _decision_example(candidate_actions: List[str]) -> str:
    action = candidate_actions[0]
    stop_signal = "continue"
    if action == "pause_for_human_review":
        stop_signal = "pause"
    elif action == "abort_run":
        stop_signal = "terminate"
    return json.dumps(
        {
            "schema_version": DECISION_SCHEMA_VERSION,
            "recommended_action": action,
            "confidence": "medium",
            "proceed": stop_signal == "continue",
            "needs_human_review": action == "pause_for_human_review",
            "stop_signal": stop_signal,
            "reasoning": "Tom tat ngan gon tai sao nen chon hanh dong nay o checkpoint hien tai.",
            "risks": ["Neu bo qua checkpoint nay, co the rat de di sai huong."],
            "suggested_changes": ["Ghi ro dieu kien tiep tuc hoac dieu kien can review."],
            "next_owner": "pipeline_orchestrator",
            "next_step": "Thuc thi hanh dong duoc de xuat va ghi audit trail.",
        },
        ensure_ascii=False,
        indent=2,
    )


def _normalize_decision_payload(
    raw_payload: Dict[str, Any],
    workflow: str,
    checkpoint: str,
    candidate_actions: List[str],
) -> Dict[str, Any]:
    if not isinstance(raw_payload, dict):
        raise ClaudeCliError("Claude Decision Gate phai tra ve mot JSON object.")

    action = _canonicalize_action(
        raw_payload.get("recommended_action")
        or raw_payload.get("action")
        or raw_payload.get("decision"),
        candidate_actions,
    )

    proceed_default = action not in {"pause_for_human_review", "abort_run"}
    needs_human_review_default = action == "pause_for_human_review"
    proceed = _normalize_bool(raw_payload.get("proceed"), proceed_default)
    needs_human_review = _normalize_bool(raw_payload.get("needs_human_review"), needs_human_review_default)
    stop_signal = _normalize_stop_signal(raw_payload.get("stop_signal"), action, proceed, needs_human_review)

    if stop_signal == "continue":
        proceed = True
    elif stop_signal == "pause":
        proceed = False
        needs_human_review = True
    elif stop_signal == "terminate":
        proceed = False

    confidence = str(raw_payload.get("confidence") or "medium").strip().lower()
    if confidence not in {"low", "medium", "high"}:
        confidence = "medium"

    normalized = {
        "schema_version": DECISION_SCHEMA_VERSION,
        "workflow": workflow,
        "checkpoint": checkpoint,
        "recommended_action": action,
        "confidence": confidence,
        "proceed": proceed,
        "needs_human_review": needs_human_review,
        "stop_signal": stop_signal,
        "reasoning": _require_text(raw_payload.get("reasoning") or raw_payload.get("why"), "reasoning"),
        "risks": _normalize_string_list(raw_payload.get("risks")),
        "suggested_changes": _normalize_string_list(
            raw_payload.get("suggested_changes") or raw_payload.get("changes") or raw_payload.get("protocol_delta")
        ),
        "next_owner": _require_text(raw_payload.get("next_owner") or raw_payload.get("owner"), "next_owner"),
        "next_step": _require_text(raw_payload.get("next_step") or raw_payload.get("next"), "next_step"),
    }

    try:
        if hasattr(ClaudeDecisionGateResult, "model_validate"):
            structured = ClaudeDecisionGateResult.model_validate(normalized)
        else:
            structured = ClaudeDecisionGateResult.parse_obj(normalized)
    except ValidationError as exc:
        raise ClaudeCliError(f"Decision schema khong hop le: {exc}") from exc
    return _model_dump(structured)


class ClaudeDecisionGateRunner(ClaudeCliRuntime):
    def _build_prompt(
        self,
        *,
        workflow: str,
        checkpoint: str,
        objective: str,
        context: Optional[str],
        state: Dict[str, Any],
        candidate_actions: List[str],
        max_output_chars: int,
        bridge_context: Optional[str] = None,
    ) -> str:
        return "\n\n".join(
            part
            for part in [
                f"Workflow:\n{workflow}",
                f"Checkpoint:\n{checkpoint}",
                f"Objective:\n{objective}",
                f"Context bo sung:\n{context.strip()}" if context else "",
                bridge_context.strip() if bridge_context else "",
                f"Candidate actions duoc phep:\n- " + "\n- ".join(candidate_actions),
                f"State hien tai:\n{_state_to_prompt(state)}",
                (
                    "Yeu cau bat buoc:\n"
                    "- Chi chon DUY NHAT 1 recommended_action nam trong candidate_actions.\n"
                    "- Neu rui ro cao, state mo ho, hoac thieu du lieu quan trong, nghieng ve pause_for_human_review.\n"
                    "- Chi tra ve DUY NHAT 1 JSON object hop le, khong markdown, khong code fence, khong giai thich ngoai JSON.\n"
                    f"- schema_version phai la {DECISION_SCHEMA_VERSION}.\n"
                    "- stop_signal phai la continue, pause, hoac terminate.\n"
                    "- reasoning phai ngan gon nhung ro rang.\n"
                    "- risks va suggested_changes phai la mang chuoi.\n"
                    f"- Giu tong phan hoi trong khoang {max_output_chars} ky tu.\n"
                    "- Mau JSON tham khao:\n"
                    f"{_decision_example(candidate_actions)}"
                ),
            ]
            if part
        )

    def _build_repair_prompt(self, *, base_prompt: str, raw_content: str, format_error: str) -> str:
        return "\n\n".join(
            [
                base_prompt,
                "Noi dung vua tra ve KHONG hop le. Sua lai ngay theo cung schema.",
                f"Loi dinh dang: {format_error}",
                f"Noi dung khong hop le:\n{raw_content}",
                "Nhac lai: chi tra ve 1 JSON object hop le, khong markdown, khong van ban ben ngoai JSON.",
            ]
        )

    def decide(
        self,
        *,
        workflow: str,
        checkpoint: str,
        objective: str,
        context: Optional[str] = None,
        state: Optional[Dict[str, Any]] = None,
        candidate_actions: Optional[List[str]] = None,
        model: Optional[str] = None,
        system_prompt: Optional[str] = None,
        max_output_chars: int = 1600,
    ) -> Dict[str, Any]:
        chosen_actions = [str(action).strip() for action in (candidate_actions or DEFAULT_DECISION_ACTIONS) if str(action).strip()]
        if not chosen_actions:
            chosen_actions = list(DEFAULT_DECISION_ACTIONS)
        chosen_model = (model or DEFAULT_MODEL).strip()
        effective_system_prompt = (system_prompt or DEFAULT_DECISION_SYSTEM_PROMPT).strip()
        current_state = state or {}
        request_packet = bridge.build_request_packet(
            mode="decision_gate",
            objective=objective.strip(),
            context=context,
            state=current_state,
            evidence={
                "candidate_actions": chosen_actions,
                "state_keys": list(current_state.keys())[:12],
            },
            response_contract={
                "schema_version": DECISION_SCHEMA_VERSION,
                "candidate_actions": chosen_actions,
                "stop_signals": ["continue", "pause", "terminate"],
            },
            metadata={
                "workflow": workflow.strip(),
                "checkpoint": checkpoint.strip(),
                "max_output_chars": max_output_chars,
            },
        )
        bridge_context = bridge.render_request_packet(request_packet)

        base_prompt = self._build_prompt(
            workflow=workflow.strip(),
            checkpoint=checkpoint.strip(),
            objective=objective.strip(),
            context=context,
            state=current_state,
            candidate_actions=chosen_actions,
            max_output_chars=max_output_chars,
            bridge_context=bridge_context,
        )

        last_error = "Khong ro loi dinh dang."
        total_duration_ms = 0.0
        raw_content = ""
        for attempt in range(MAX_DECISION_REPAIRS + 1):
            prompt = base_prompt
            if attempt:
                prompt = self._build_repair_prompt(
                    base_prompt=base_prompt,
                    raw_content=raw_content,
                    format_error=last_error,
                )

            result = self.invoke(
                prompt,
                system_prompt=effective_system_prompt,
                model=chosen_model,
                tools="",
            )
            total_duration_ms += float(result["duration_ms"])
            raw_content = result["content"]

            try:
                structured = _normalize_decision_payload(
                    _extract_json_payload(raw_content),
                    workflow.strip(),
                    checkpoint.strip(),
                    chosen_actions,
                )
            except ClaudeCliError as exc:
                last_error = str(exc)
                continue

            claude_status = self.status()
            bridge_trace = None
            try:
                bridge_trace = bridge.persist_interaction(
                    mode="decision_gate",
                    request_packet=request_packet,
                    response_payload={
                        "decision": structured,
                        "raw_content": raw_content,
                        "duration_ms": round(total_duration_ms, 1),
                        "repair_attempts": attempt,
                    },
                    prompt_preview=base_prompt,
                    system_prompt_preview=effective_system_prompt,
                    model=chosen_model,
                    claude_status=claude_status,
                    tags={
                        "workflow": workflow.strip(),
                        "checkpoint": checkpoint.strip(),
                        "recommended_action": structured.get("recommended_action"),
                    },
                )
            except Exception as exc:
                bridge_trace = {
                    "schema_version": "pathway_claude_bridge.v1",
                    "mode": "decision_gate",
                    "error": str(exc),
                }

            return {
                "decision": structured,
                "raw_content": raw_content,
                "duration_ms": round(total_duration_ms, 1),
                "repair_attempts": attempt,
                "claude_status": claude_status,
                "bridge_trace": bridge_trace,
            }

        raise ClaudeCliError(
            f"Claude Decision Gate khong tra ve dung JSON schema sau {MAX_DECISION_REPAIRS + 1} lan. Loi cuoi: {last_error}"
        )
