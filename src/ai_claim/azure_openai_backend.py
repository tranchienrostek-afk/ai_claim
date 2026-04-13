from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(slots=True)
class AzureOpenAIConfig:
    endpoint: str
    api_key: str
    api_version: str
    chat_deployment: str


class AzureOpenAIBackend:
    """
    Minimal Azure OpenAI backend for the future `ai_claim` runtime.

    This backend is intentionally simple: it gives the project a safe and
    explicit place to move away from GLM / Claude-native sessions without
    modifying the vendor `agent_claude` release binary.
    """

    def __init__(self, config: AzureOpenAIConfig) -> None:
        self.config = config

    def is_configured(self) -> bool:
        return all(
            [
                self.config.endpoint,
                self.config.api_key,
                self.config.api_version,
                self.config.chat_deployment,
            ]
        )

    def chat_completion(
        self,
        messages: list[dict[str, Any]],
        temperature: float = 0.1,
        max_tokens: int = 2000,
        response_format: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        if not self.is_configured():
            raise RuntimeError(
                "Azure OpenAI chua duoc cau hinh. Hay set AZURE_OPENAI_* env vars."
            )
        try:
            from openai import AzureOpenAI
        except ImportError as exc:
            raise RuntimeError(
                "Thu vien openai chua co san. Hay cai dependency trong pyproject.toml."
            ) from exc

        client = AzureOpenAI(
            api_key=self.config.api_key,
            api_version=self.config.api_version,
            azure_endpoint=self.config.endpoint,
        )
        payload: dict[str, Any] = {
            "model": self.config.chat_deployment,
            "messages": messages,
            "temperature": temperature,
            "max_tokens": max_tokens,
        }
        if response_format:
            payload["response_format"] = response_format
        response = client.chat.completions.create(**payload)
        choice = response.choices[0]
        return {
            "model": response.model,
            "content": choice.message.content if choice.message else "",
            "finish_reason": choice.finish_reason,
            "usage": {
                "prompt_tokens": getattr(response.usage, "prompt_tokens", None),
                "completion_tokens": getattr(response.usage, "completion_tokens", None),
                "total_tokens": getattr(response.usage, "total_tokens", None),
            },
        }

