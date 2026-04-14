from __future__ import annotations

import argparse
import json
import os
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator

import httpx

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover
    load_dotenv = None


def _load_env_file(path: Path) -> None:
    if not path.exists():
        return
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def _sanitize_env_value(key: str, value: str) -> str:
    cleaned = str(value or "").strip().strip('"').strip("'")
    duplicated_prefix = f"{key}="
    if cleaned.startswith(duplicated_prefix):
        cleaned = cleaned[len(duplicated_prefix):].strip()
    return cleaned


def _maybe_load_dotenv(path: Path | None = None) -> None:
    if path is not None:
        _load_env_file(path)
        return
    if load_dotenv is not None:
        load_dotenv()


def _json_default(value: Any) -> Any:
    return value


def _normalize_base_url(base_url: str) -> str:
    normalized = str(base_url or "").strip().rstrip("/")
    if normalized.endswith("/v1/messages"):
        normalized = normalized[: -len("/v1/messages")]
    return normalized


def flatten_text_content(content: Any) -> str:
    if isinstance(content, str):
        return content
    if not isinstance(content, list):
        return ""
    parts: list[str] = []
    for block in content:
        if not isinstance(block, dict):
            continue
        if block.get("type") == "text":
            text = str(block.get("text") or "").strip()
            if text:
                parts.append(text)
        elif block.get("type") == "tool_result":
            result_text = flatten_text_content(block.get("content") or "")
            if result_text:
                parts.append(result_text)
    return "\n".join(parts)


def anthropic_tools_to_openai(tools: list[dict[str, Any]] | None) -> list[dict[str, Any]] | None:
    if not tools:
        return None
    converted: list[dict[str, Any]] = []
    for tool in tools:
        converted.append(
            {
                "type": "function",
                "function": {
                    "name": str(tool.get("name") or ""),
                    "description": str(tool.get("description") or ""),
                    "parameters": tool.get("input_schema") or {"type": "object", "properties": {}},
                },
            }
        )
    return converted


def anthropic_tool_choice_to_openai(tool_choice: Any) -> Any:
    if not tool_choice:
        return None
    if isinstance(tool_choice, str):
        lowered = tool_choice.lower()
        if lowered in {"auto", "none", "required"}:
            return lowered
        return {"type": "function", "function": {"name": tool_choice}}
    if not isinstance(tool_choice, dict):
        return None
    choice_type = str(tool_choice.get("type") or "auto").lower()
    if choice_type == "auto":
        return "auto"
    if choice_type == "any":
        return "required"
    if choice_type == "tool":
        return {"type": "function", "function": {"name": str(tool_choice.get("name") or "")}}
    return None


def anthropic_messages_to_openai(messages: list[dict[str, Any]], system: Any = None) -> list[dict[str, Any]]:
    openai_messages: list[dict[str, Any]] = []

    if system:
        system_text = flatten_text_content(system)
        if system_text:
            openai_messages.append({"role": "system", "content": system_text})

    for message in messages:
        role = str(message.get("role") or "user")
        content = message.get("content", "")

        if isinstance(content, str):
            openai_messages.append(
                {
                    "role": role if role in {"system", "user", "assistant"} else "user",
                    "content": content,
                }
            )
            continue

        if not isinstance(content, list):
            openai_messages.append({"role": "user", "content": str(content)})
            continue

        text_parts: list[str] = []
        assistant_tool_calls: list[dict[str, Any]] = []
        tool_results: list[dict[str, Any]] = []

        for block in content:
            if not isinstance(block, dict):
                continue
            block_type = str(block.get("type") or "")
            if block_type == "text":
                text = str(block.get("text") or "").strip()
                if text:
                    text_parts.append(text)
            elif block_type == "tool_use":
                assistant_tool_calls.append(
                    {
                        "id": str(block.get("id") or f"toolu_{uuid.uuid4().hex[:12]}"),
                        "type": "function",
                        "function": {
                            "name": str(block.get("name") or "tool"),
                            "arguments": json.dumps(block.get("input") or {}, ensure_ascii=False),
                        },
                    }
                )
            elif block_type == "tool_result":
                tool_results.append(
                    {
                        "role": "tool",
                        "tool_call_id": str(block.get("tool_use_id") or block.get("id") or f"toolu_{uuid.uuid4().hex[:12]}"),
                        "content": flatten_text_content(block.get("content") or ""),
                    }
                )

        if role == "assistant":
            assistant_payload: dict[str, Any] = {
                "role": "assistant",
                "content": "\n".join(text_parts),
            }
            if assistant_tool_calls:
                assistant_payload["tool_calls"] = assistant_tool_calls
            openai_messages.append(assistant_payload)
        else:
            user_text = "\n".join(text_parts)
            if user_text or not tool_results:
                openai_messages.append({"role": "user", "content": user_text})

        openai_messages.extend(tool_results)

    return openai_messages


def _parse_tool_arguments(arguments: str) -> dict[str, Any]:
    try:
        parsed = json.loads(arguments or "{}")
    except json.JSONDecodeError:
        parsed = {"raw_arguments": arguments}
    if isinstance(parsed, dict):
        return parsed
    return {"value": parsed}


def openai_message_to_anthropic_content(message: Any) -> list[dict[str, Any]]:
    content_blocks: list[dict[str, Any]] = []
    text = str(getattr(message, "content", "") or "")
    if text:
        content_blocks.append({"type": "text", "text": text})

    tool_calls = list(getattr(message, "tool_calls", None) or [])
    for tool_call in tool_calls:
        function = getattr(tool_call, "function", None)
        content_blocks.append(
            {
                "type": "tool_use",
                "id": str(getattr(tool_call, "id", "") or f"toolu_{uuid.uuid4().hex[:12]}"),
                "name": str(getattr(function, "name", "") or "tool"),
                "input": _parse_tool_arguments(str(getattr(function, "arguments", "") or "{}")),
            }
        )

    if not content_blocks:
        content_blocks.append({"type": "text", "text": ""})
    return content_blocks


def map_finish_reason_to_anthropic(finish_reason: str | None, has_tool_use: bool = False) -> str | None:
    if has_tool_use or finish_reason == "tool_calls":
        return "tool_use"
    if finish_reason in {"stop", "end_turn"}:
        return "end_turn"
    if finish_reason == "length":
        return "max_tokens"
    if finish_reason in {"content_filter", "safety"}:
        return "stop_sequence"
    return finish_reason


@dataclass(slots=True)
class AzureAnthropicConfig:
    endpoint: str
    api_key: str
    api_version: str
    deployment_name: str
    temperature: float | None = 0.1
    request_timeout_seconds: float = 300.0

    @classmethod
    def from_env(cls, env_file: str | Path | None = None) -> "AzureAnthropicConfig":
        env_path = Path(env_file) if env_file else None
        _maybe_load_dotenv(env_path)
        temperature_value = _sanitize_env_value(
            "AZURE_TEMPERATURE",
            os.getenv("AZURE_TEMPERATURE", os.getenv("TEMPERATURE", "0.1")),
        )
        return cls(
            endpoint=_sanitize_env_value("AZURE_OPENAI_ENDPOINT", os.getenv("AZURE_OPENAI_ENDPOINT", "")).rstrip("/"),
            api_key=_sanitize_env_value("AZURE_OPENAI_API_KEY", os.getenv("AZURE_OPENAI_API_KEY", "")),
            api_version=_sanitize_env_value("AZURE_OPENAI_API_VERSION", os.getenv("AZURE_OPENAI_API_VERSION", "2024-10-21")),
            deployment_name=_sanitize_env_value(
                "AZURE_DEPLOYMENT_NAME",
                os.getenv("AZURE_DEPLOYMENT_NAME", os.getenv("AZURE_OPENAI_CHAT_DEPLOYMENT", "")),
            ),
            temperature=float(temperature_value) if temperature_value else None,
            request_timeout_seconds=float(
                _sanitize_env_value("AZURE_REQUEST_TIMEOUT_SECONDS", os.getenv("AZURE_REQUEST_TIMEOUT_SECONDS", "300"))
            ),
        )

    def is_configured(self) -> bool:
        return all([self.endpoint, self.api_key, self.api_version, self.deployment_name])

    def validate(self) -> None:
        missing = [
            key
            for key, value in {
                "AZURE_OPENAI_ENDPOINT": self.endpoint,
                "AZURE_OPENAI_API_KEY": self.api_key,
                "AZURE_OPENAI_API_VERSION": self.api_version,
                "AZURE_DEPLOYMENT_NAME": self.deployment_name,
            }.items()
            if not value
        ]
        if missing:
            raise RuntimeError(f"Missing Azure config: {', '.join(missing)}")


@dataclass(slots=True)
class GLMAnthropicConfig:
    base_url: str
    api_key: str
    model_name: str
    anthropic_version: str
    request_timeout_seconds: float = 300.0

    @classmethod
    def from_env(cls, env_file: str | Path | None = None) -> "GLMAnthropicConfig":
        env_path = Path(env_file) if env_file else None
        _maybe_load_dotenv(env_path)
        return cls(
            base_url=_normalize_base_url(
                _sanitize_env_value(
                    "GLM_BASE_URL",
                    os.getenv("GLM_BASE_URL", "https://api.z.ai/api/anthropic"),
                )
            ),
            api_key=_sanitize_env_value("GLM_API_KEY", os.getenv("GLM_API_KEY", "")),
            model_name=_sanitize_env_value("GLM_MODEL_NAME", os.getenv("GLM_MODEL_NAME", "glm-5-turbo")),
            anthropic_version=_sanitize_env_value(
                "GLM_ANTHROPIC_VERSION",
                os.getenv(
                    "GLM_ANTHROPIC_VERSION",
                    os.getenv("GLM_MODEL_VERSION", os.getenv("ANTHROPIC_VERSION", "2023-06-01")),
                ),
            ),
            request_timeout_seconds=float(
                _sanitize_env_value(
                    "GLM_REQUEST_TIMEOUT_SECONDS",
                    os.getenv("GLM_REQUEST_TIMEOUT_SECONDS", os.getenv("AZURE_REQUEST_TIMEOUT_SECONDS", "300")),
                )
            ),
        )

    def is_configured(self) -> bool:
        return all([self.base_url, self.api_key, self.model_name])

    def validate(self) -> None:
        missing = [
            key
            for key, value in {
                "GLM_BASE_URL": self.base_url,
                "GLM_API_KEY": self.api_key,
                "GLM_MODEL_NAME": self.model_name,
            }.items()
            if not value
        ]
        if missing:
            raise RuntimeError(f"Missing GLM config: {', '.join(missing)}")

    def messages_endpoint(self) -> str:
        return f"{self.base_url}/v1/messages"


class AzureAnthropicSDK:
    def __init__(
        self,
        azure_config: AzureAnthropicConfig,
        glm_config: GLMAnthropicConfig,
        *,
        default_provider: str = "azure",
    ) -> None:
        self.azure_config = azure_config
        self.glm_config = glm_config
        self.default_provider = (default_provider or "azure").strip().lower()

    @property
    def config(self) -> AzureAnthropicConfig:
        return self.azure_config

    @classmethod
    def from_env(cls, env_file: str | Path | None = None) -> "AzureAnthropicSDK":
        env_path = Path(env_file) if env_file else None
        _maybe_load_dotenv(env_path)
        default_provider = _sanitize_env_value(
            "ANTHROPIC_COMPAT_DEFAULT_PROVIDER",
            os.getenv("ANTHROPIC_COMPAT_DEFAULT_PROVIDER", os.getenv("MODEL_PROVIDER", "azure")),
        )
        return cls(
            AzureAnthropicConfig.from_env(env_file=env_file),
            GLMAnthropicConfig.from_env(env_file=env_file),
            default_provider=default_provider or "azure",
        )

    def available_providers(self) -> list[str]:
        providers: list[str] = []
        if self.azure_config.is_configured():
            providers.append("azure")
        if self.glm_config.is_configured():
            providers.append("glm")
        return providers

    def is_configured(self, provider: str | None = None) -> bool:
        if provider:
            provider_name = provider.strip().lower()
            if provider_name == "azure":
                return self.azure_config.is_configured()
            if provider_name == "glm":
                return self.glm_config.is_configured()
            return False
        return bool(self.available_providers())

    def _resolve_provider(self, model_alias: str) -> str:
        lowered = str(model_alias or "").strip().lower()
        if lowered.startswith("glm"):
            if not self.glm_config.is_configured():
                raise RuntimeError("GLM provider requested but GLM is not configured")
            return "glm"
        if lowered.startswith("azure"):
            if not self.azure_config.is_configured():
                raise RuntimeError("Azure provider requested but Azure is not configured")
            return "azure"

        if self.default_provider in self.available_providers():
            return self.default_provider
        if self.azure_config.is_configured():
            return "azure"
        if self.glm_config.is_configured():
            return "glm"
        raise RuntimeError("No provider configured. Expected Azure and/or GLM configuration.")

    def describe(self) -> dict[str, Any]:
        return {
            "default_provider": self.default_provider,
            "available_providers": self.available_providers(),
            "providers": {
                "azure": {
                    "configured": self.azure_config.is_configured(),
                    "endpoint": self.azure_config.endpoint,
                    "deployment_name": self.azure_config.deployment_name,
                },
                "glm": {
                    "configured": self.glm_config.is_configured(),
                    "base_url": self.glm_config.base_url,
                    "model_name": self.glm_config.model_name,
                    "anthropic_version": self.glm_config.anthropic_version,
                },
            },
        }

    def _azure_client(self):
        self.azure_config.validate()
        try:
            from openai import AzureOpenAI
        except ImportError as exc:  # pragma: no cover
            raise RuntimeError("Missing dependency: openai") from exc
        return AzureOpenAI(
            api_key=self.azure_config.api_key,
            api_version=self.azure_config.api_version,
            azure_endpoint=self.azure_config.endpoint,
            timeout=self.azure_config.request_timeout_seconds,
        )

    @staticmethod
    def _should_retry_with_max_completion_tokens(exc: Exception) -> bool:
        message = str(exc or "")
        return "max_completion_tokens" in message and "max_tokens" in message

    @staticmethod
    def _should_retry_without_temperature(exc: Exception) -> bool:
        message = str(exc or "").lower()
        return "temperature" in message and "supported" in message

    def _azure_chat_completion_create(self, client: Any, payload: dict[str, Any]) -> Any:
        try:
            return client.chat.completions.create(**payload)
        except Exception as exc:
            retry_payload = dict(payload)
            changed = False
            if "max_tokens" in retry_payload and self._should_retry_with_max_completion_tokens(exc):
                retry_payload["max_completion_tokens"] = retry_payload.pop("max_tokens")
                changed = True
            if "temperature" in retry_payload and self._should_retry_without_temperature(exc):
                retry_payload.pop("temperature", None)
                changed = True
            if not changed:
                raise
            return self._azure_chat_completion_create(client, retry_payload)

    def _azure_effective_max_tokens(self, requested_max_tokens: int) -> int:
        deployment = (self.azure_config.deployment_name or "").strip().lower()
        safe_requested = max(int(requested_max_tokens or 0), 1)
        if deployment.startswith("gpt-5") and safe_requested < 128:
            # GPT-5 style reasoning deployments may consume hidden reasoning tokens
            # before emitting visible text. A small floor avoids empty-text responses
            # for short Anthropic-style calls routed through Azure.
            return 128
        return safe_requested

    def _resolved_glm_model_name(self, model_alias: str) -> str:
        lowered = str(model_alias or "").strip().lower()
        if lowered.startswith("glm-") and lowered not in {"glm-sonnet", "glm-opus"}:
            return model_alias
        return self.glm_config.model_name

    def _glm_headers(self) -> dict[str, str]:
        return {
            "content-type": "application/json",
            "x-api-key": self.glm_config.api_key,
            "anthropic-version": self.glm_config.anthropic_version or "2023-06-01",
        }

    def _glm_payload(
        self,
        *,
        messages: list[dict[str, Any]],
        system: Any,
        model_alias: str,
        max_tokens: int,
        temperature: float | None,
        tools: list[dict[str, Any]] | None,
        tool_choice: Any,
        stream: bool,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "model": self._resolved_glm_model_name(model_alias),
            "max_tokens": max_tokens,
            "messages": messages,
            "stream": stream,
        }
        if system is not None:
            payload["system"] = system
        if temperature is not None:
            payload["temperature"] = temperature
        if tools:
            payload["tools"] = tools
        if tool_choice is not None:
            payload["tool_choice"] = tool_choice
        return payload

    def _glm_create_message(
        self,
        *,
        messages: list[dict[str, Any]],
        system: Any,
        model_alias: str,
        max_tokens: int,
        temperature: float | None,
        tools: list[dict[str, Any]] | None,
        tool_choice: Any,
    ) -> dict[str, Any]:
        self.glm_config.validate()
        payload = self._glm_payload(
            messages=messages,
            system=system,
            model_alias=model_alias,
            max_tokens=max_tokens,
            temperature=temperature,
            tools=tools,
            tool_choice=tool_choice,
            stream=False,
        )
        with httpx.Client(timeout=self.glm_config.request_timeout_seconds) as client:
            response = client.post(
                self.glm_config.messages_endpoint(),
                headers=self._glm_headers(),
                json=payload,
            )
            response.raise_for_status()
            data = response.json()
        if isinstance(data, dict):
            data.setdefault("model", model_alias)
        return data

    def _glm_stream_message(
        self,
        *,
        messages: list[dict[str, Any]],
        system: Any,
        model_alias: str,
        max_tokens: int,
        temperature: float | None,
        tools: list[dict[str, Any]] | None,
        tool_choice: Any,
    ) -> Iterator[dict[str, Any]]:
        self.glm_config.validate()
        payload = self._glm_payload(
            messages=messages,
            system=system,
            model_alias=model_alias,
            max_tokens=max_tokens,
            temperature=temperature,
            tools=tools,
            tool_choice=tool_choice,
            stream=True,
        )
        with httpx.Client(timeout=self.glm_config.request_timeout_seconds) as client:
            with client.stream(
                "POST",
                self.glm_config.messages_endpoint(),
                headers=self._glm_headers(),
                json=payload,
            ) as response:
                response.raise_for_status()
                current_event = "message"
                data_lines: list[str] = []
                for raw_line in response.iter_lines():
                    if raw_line is None:
                        continue
                    line = str(raw_line).strip()
                    if not line:
                        if not data_lines:
                            current_event = "message"
                            continue
                        payload_text = "\n".join(data_lines)
                        data_lines = []
                        if payload_text == "[DONE]":
                            current_event = "message"
                            continue
                        try:
                            parsed = json.loads(payload_text)
                        except json.JSONDecodeError:
                            parsed = {"raw": payload_text}
                        yield {"event": current_event or "message", "data": parsed}
                        current_event = "message"
                        continue
                    if line.startswith("event:"):
                        current_event = line.split(":", 1)[1].strip() or "message"
                        continue
                    if line.startswith("data:"):
                        data_lines.append(line.split(":", 1)[1].lstrip())
                if data_lines:
                    payload_text = "\n".join(data_lines)
                    if payload_text != "[DONE]":
                        try:
                            parsed = json.loads(payload_text)
                        except json.JSONDecodeError:
                            parsed = {"raw": payload_text}
                        yield {"event": current_event or "message", "data": parsed}

    def _azure_create_message(
        self,
        *,
        messages: list[dict[str, Any]],
        system: Any,
        model_alias: str,
        max_tokens: int,
        temperature: float | None,
        tools: list[dict[str, Any]] | None,
        tool_choice: Any,
    ) -> dict[str, Any]:
        client = self._azure_client()
        payload: dict[str, Any] = {
            "model": self.azure_config.deployment_name,
            "messages": anthropic_messages_to_openai(messages, system=system),
            "max_tokens": self._azure_effective_max_tokens(max_tokens),
        }
        resolved_temperature = self.azure_config.temperature if temperature is None else temperature
        if resolved_temperature is not None:
            payload["temperature"] = resolved_temperature
        openai_tools = anthropic_tools_to_openai(tools)
        if openai_tools:
            payload["tools"] = openai_tools
        converted_tool_choice = anthropic_tool_choice_to_openai(tool_choice)
        if converted_tool_choice is not None:
            payload["tool_choice"] = converted_tool_choice

        response = self._azure_chat_completion_create(client, payload)
        choice = response.choices[0]
        message = choice.message
        content_blocks = openai_message_to_anthropic_content(message)
        has_tool_use = any(block.get("type") == "tool_use" for block in content_blocks)
        return {
            "id": f"msg_{uuid.uuid4().hex}",
            "type": "message",
            "role": "assistant",
            "model": model_alias,
            "content": content_blocks,
            "stop_reason": map_finish_reason_to_anthropic(choice.finish_reason, has_tool_use=has_tool_use),
            "stop_sequence": None,
            "usage": {
                "input_tokens": getattr(response.usage, "prompt_tokens", 0) or 0,
                "output_tokens": getattr(response.usage, "completion_tokens", 0) or 0,
            },
            "azure_usage": {
                "prompt_tokens": getattr(response.usage, "prompt_tokens", 0) or 0,
                "completion_tokens": getattr(response.usage, "completion_tokens", 0) or 0,
                "total_tokens": getattr(response.usage, "total_tokens", 0) or 0,
            },
        }

    def _azure_stream_message(
        self,
        *,
        messages: list[dict[str, Any]],
        system: Any,
        model_alias: str,
        max_tokens: int,
        temperature: float | None,
        tools: list[dict[str, Any]] | None,
        tool_choice: Any,
    ) -> Iterator[dict[str, Any]]:
        client = self._azure_client()
        payload: dict[str, Any] = {
            "model": self.azure_config.deployment_name,
            "messages": anthropic_messages_to_openai(messages, system=system),
            "max_tokens": self._azure_effective_max_tokens(max_tokens),
            "stream": True,
        }
        resolved_temperature = self.azure_config.temperature if temperature is None else temperature
        if resolved_temperature is not None:
            payload["temperature"] = resolved_temperature
        openai_tools = anthropic_tools_to_openai(tools)
        if openai_tools:
            payload["tools"] = openai_tools
        converted_tool_choice = anthropic_tool_choice_to_openai(tool_choice)
        if converted_tool_choice is not None:
            payload["tool_choice"] = converted_tool_choice

        stream = self._azure_chat_completion_create(client, payload)
        message_id = f"msg_{uuid.uuid4().hex}"

        yield {
            "event": "message_start",
            "data": {
                "type": "message_start",
                "message": {
                    "id": message_id,
                    "type": "message",
                    "role": "assistant",
                    "model": model_alias,
                    "content": [],
                    "stop_reason": None,
                    "stop_sequence": None,
                    "usage": {"input_tokens": 0, "output_tokens": 0},
                },
            },
        }

        # Block-tracking state. We lazily open content blocks so that a
        # tool_calls-only response does not leave a dangling empty text block.
        next_block_index = 0
        text_block_open = False
        text_block_index: int | None = None
        # Map OpenAI tool_call.index -> our Anthropic block index
        tool_block_indices: dict[int, int] = {}
        tool_block_open: set[int] = set()
        final_finish_reason: str | None = None
        final_usage: Any = None
        saw_tool_use = False

        def open_text_block() -> int:
            nonlocal next_block_index, text_block_open, text_block_index
            idx = next_block_index
            next_block_index += 1
            text_block_index = idx
            text_block_open = True
            return idx

        def open_tool_block(tool_index: int, tool_id: str, tool_name: str) -> int:
            nonlocal next_block_index
            idx = next_block_index
            next_block_index += 1
            tool_block_indices[tool_index] = idx
            tool_block_open.add(idx)
            return idx

        for chunk in stream:
            chunk_usage = getattr(chunk, "usage", None)
            if chunk_usage is not None:
                final_usage = chunk_usage
            choices = list(getattr(chunk, "choices", None) or [])
            if not choices:
                continue
            choice = choices[0]
            chunk_finish = getattr(choice, "finish_reason", None)
            if chunk_finish:
                final_finish_reason = chunk_finish
            delta = getattr(choice, "delta", None)
            if delta is None:
                continue

            text = str(getattr(delta, "content", "") or "")
            if text:
                if not text_block_open:
                    idx = open_text_block()
                    yield {
                        "event": "content_block_start",
                        "data": {
                            "type": "content_block_start",
                            "index": idx,
                            "content_block": {"type": "text", "text": ""},
                        },
                    }
                yield {
                    "event": "content_block_delta",
                    "data": {
                        "type": "content_block_delta",
                        "index": text_block_index,
                        "delta": {"type": "text_delta", "text": text},
                    },
                }

            tool_calls = getattr(delta, "tool_calls", None) or []
            for tc in tool_calls:
                saw_tool_use = True
                tc_index = getattr(tc, "index", None)
                if tc_index is None:
                    continue
                tc_function = getattr(tc, "function", None)
                tc_name = str(getattr(tc_function, "name", "") or "") if tc_function else ""
                tc_arguments = str(getattr(tc_function, "arguments", "") or "") if tc_function else ""
                tc_id = str(getattr(tc, "id", "") or "")

                if tc_index not in tool_block_indices:
                    # First delta for this tool call — must carry name and id.
                    # If text block was open, close it before starting tool_use.
                    if text_block_open and text_block_index is not None:
                        yield {
                            "event": "content_block_stop",
                            "data": {"type": "content_block_stop", "index": text_block_index},
                        }
                        text_block_open = False
                    idx = open_tool_block(
                        tc_index,
                        tc_id or f"toolu_{uuid.uuid4().hex[:12]}",
                        tc_name or "tool",
                    )
                    yield {
                        "event": "content_block_start",
                        "data": {
                            "type": "content_block_start",
                            "index": idx,
                            "content_block": {
                                "type": "tool_use",
                                "id": tc_id or f"toolu_{uuid.uuid4().hex[:12]}",
                                "name": tc_name or "tool",
                                "input": {},
                            },
                        },
                    }

                if tc_arguments:
                    yield {
                        "event": "content_block_delta",
                        "data": {
                            "type": "content_block_delta",
                            "index": tool_block_indices[tc_index],
                            "delta": {
                                "type": "input_json_delta",
                                "partial_json": tc_arguments,
                            },
                        },
                    }

        # Close any blocks that are still open.
        if text_block_open and text_block_index is not None:
            yield {
                "event": "content_block_stop",
                "data": {"type": "content_block_stop", "index": text_block_index},
            }
            text_block_open = False
        for tool_idx in sorted(tool_block_open):
            yield {
                "event": "content_block_stop",
                "data": {"type": "content_block_stop", "index": tool_idx},
            }
        tool_block_open.clear()

        output_tokens = 0
        if final_usage is not None:
            output_tokens = int(getattr(final_usage, "completion_tokens", 0) or 0)
        mapped_stop = (
            map_finish_reason_to_anthropic(final_finish_reason, has_tool_use=saw_tool_use)
            or ("tool_use" if saw_tool_use else "end_turn")
        )
        yield {
            "event": "message_delta",
            "data": {
                "type": "message_delta",
                "delta": {
                    "stop_reason": mapped_stop,
                    "stop_sequence": None,
                },
                "usage": {"output_tokens": output_tokens},
            },
        }
        yield {"event": "message_stop", "data": {"type": "message_stop"}}

    def create_message(
        self,
        *,
        messages: list[dict[str, Any]],
        system: Any = None,
        model_alias: str = "azure-sonnet",
        max_tokens: int = 1024,
        temperature: float | None = None,
        tools: list[dict[str, Any]] | None = None,
        tool_choice: Any = None,
    ) -> dict[str, Any]:
        provider = self._resolve_provider(model_alias)
        if provider == "glm":
            return self._glm_create_message(
                messages=messages,
                system=system,
                model_alias=model_alias,
                max_tokens=max_tokens,
                temperature=temperature,
                tools=tools,
                tool_choice=tool_choice,
            )
        return self._azure_create_message(
            messages=messages,
            system=system,
            model_alias=model_alias,
            max_tokens=max_tokens,
            temperature=temperature,
            tools=tools,
            tool_choice=tool_choice,
        )

    def stream_message(
        self,
        *,
        messages: list[dict[str, Any]],
        system: Any = None,
        model_alias: str = "azure-sonnet",
        max_tokens: int = 1024,
        temperature: float | None = None,
        tools: list[dict[str, Any]] | None = None,
        tool_choice: Any = None,
    ) -> Iterator[dict[str, Any]]:
        provider = self._resolve_provider(model_alias)
        if provider == "glm":
            return self._glm_stream_message(
                messages=messages,
                system=system,
                model_alias=model_alias,
                max_tokens=max_tokens,
                temperature=temperature,
                tools=tools,
                tool_choice=tool_choice,
            )
        return self._azure_stream_message(
            messages=messages,
            system=system,
            model_alias=model_alias,
            max_tokens=max_tokens,
            temperature=temperature,
            tools=tools,
            tool_choice=tool_choice,
        )


def _main() -> int:
    parser = argparse.ArgumentParser(description="Anthropic-compatible SDK router for Azure OpenAI and GLM.")
    parser.add_argument("--env-file", default="", help="Optional path to env file.")
    parser.add_argument("--prompt", required=True, help="User prompt.")
    parser.add_argument("--system", default="", help="Optional system prompt.")
    parser.add_argument("--model-alias", default="azure-sonnet", help="Anthropic-style model alias to echo back.")
    parser.add_argument("--max-tokens", type=int, default=512, help="Max output tokens.")
    parser.add_argument("--temperature", type=float, default=None, help="Optional temperature override.")
    args = parser.parse_args()

    sdk = AzureAnthropicSDK.from_env(env_file=args.env_file or None)
    result = sdk.create_message(
        messages=[{"role": "user", "content": args.prompt}],
        system=args.system or None,
        model_alias=args.model_alias,
        max_tokens=args.max_tokens,
        temperature=args.temperature,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2, default=_json_default))
    return 0


if __name__ == "__main__":
    raise SystemExit(_main())
