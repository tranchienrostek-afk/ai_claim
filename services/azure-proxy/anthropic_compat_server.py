from __future__ import annotations

import json

import httpx
from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.responses import JSONResponse, StreamingResponse

from azure_anthropic_skd import AzureAnthropicSDK


sdk = AzureAnthropicSDK.from_env()
app = FastAPI(title="Azure Anthropic Compatibility Server")


def _sse(event: str, data: dict) -> str:
    return f"event: {event}\ndata: {json.dumps(data, ensure_ascii=False)}\n\n"


def _upstream_error_to_response(exc: Exception) -> JSONResponse:
    if isinstance(exc, httpx.HTTPStatusError):
        response = exc.response
        try:
            content = response.json()
        except Exception:
            content = {
                "type": "error",
                "error": {
                    "type": "upstream_error",
                    "message": response.text,
                },
            }
        return JSONResponse(status_code=response.status_code, content=content)
    return JSONResponse(
        status_code=500,
        content={
            "type": "error",
            "error": {"type": "api_error", "message": str(exc)},
        },
    )


@app.get("/health")
async def health() -> dict:
    description = sdk.describe()
    return {
        "status": "ok" if sdk.is_configured() else "misconfigured",
        "configured": sdk.is_configured(),
        "default_provider": description["default_provider"],
        "available_providers": description["available_providers"],
        "providers": description["providers"],
    }


@app.post("/v1/messages")
async def create_message(
    request: Request,
    anthropic_version: str | None = Header(default=None, alias="anthropic-version"),
):
    if not anthropic_version:
        raise HTTPException(status_code=400, detail="Missing anthropic-version header")

    body = await request.json()
    messages = list(body.get("messages", []) or [])
    system = body.get("system")
    max_tokens = int(body.get("max_tokens", 1024) or 1024)
    temperature = body.get("temperature")
    model_alias = str(body.get("model") or "azure-sonnet")
    tools = body.get("tools")
    tool_choice = body.get("tool_choice")
    stream = bool(body.get("stream", False))

    if not stream:
        try:
            result = sdk.create_message(
                messages=messages,
                system=system,
                model_alias=model_alias,
                max_tokens=max_tokens,
                temperature=temperature,
                tools=tools,
                tool_choice=tool_choice,
            )
        except Exception as exc:
            return _upstream_error_to_response(exc)
        return JSONResponse(content=result)

    async def event_generator():
        try:
            for event in sdk.stream_message(
                messages=messages,
                system=system,
                model_alias=model_alias,
                max_tokens=max_tokens,
                temperature=temperature,
                tools=tools,
                tool_choice=tool_choice,
            ):
                yield _sse(event["event"], event["data"])
        except Exception as exc:
            yield _sse(
                "error",
                {
                    "type": "error",
                    "error": {"type": "api_error", "message": str(exc)},
                },
            )

    return StreamingResponse(event_generator(), media_type="text/event-stream")
