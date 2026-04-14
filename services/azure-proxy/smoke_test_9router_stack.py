from __future__ import annotations

import argparse
import json
from pathlib import Path

import requests


PROJECT_ROOT = Path(__file__).resolve().parent
STATE_PATH = PROJECT_ROOT / "runtime" / "9router_stack.json"


def main() -> int:
    parser = argparse.ArgumentParser(description="Smoke test 9router stack.")
    parser.add_argument("--model", default="router-sonnet")
    parser.add_argument("--prompt", default="Tra loi duy nhat mot tu: ok")
    parser.add_argument("--max-tokens", type=int, default=128)
    args = parser.parse_args()

    if not STATE_PATH.exists():
        raise SystemExit(f"Missing stack state: {STATE_PATH}")

    state = json.loads(STATE_PATH.read_text(encoding="utf-8"))
    base_url = state["router_base_url"]
    api_key = state["router_api_key"]

    headers = {
        "x-api-key": api_key,
        "anthropic-version": "2023-06-01",
        "content-type": "application/json",
    }

    models_response = requests.get(f"{base_url}/v1/models", headers=headers, timeout=20)
    models_response.raise_for_status()
    model_ids = [item["id"] for item in models_response.json().get("data", [])]
    if args.model not in model_ids:
        # Alias models (e.g. azure-sonnet, router-sonnet) may not appear in /v1/models
        # but still route correctly via POST /v1/messages. Warn and continue.
        print(f"Warning: '{args.model}' not listed by /v1/models (may be an alias — continuing)")

    payload = {
        "model": args.model,
        "max_tokens": args.max_tokens,
        "messages": [{"role": "user", "content": args.prompt}],
    }
    response = requests.post(f"{base_url}/v1/messages", headers=headers, json=payload, timeout=120)
    response.raise_for_status()
    raw_text = response.text.strip()
    if "data: [DONE]" in raw_text:
        raw_text = raw_text.split("data: [DONE]", 1)[0].strip()
    body = json.loads(raw_text)
    text = "\n".join(
        block.get("text", "")
        for block in body.get("content", [])
        if isinstance(block, dict) and block.get("type") == "text"
    ).strip()

    print(f"Router base URL: {base_url}")
    print(f"Model: {args.model}")
    print(f"Response id: {body.get('id')}")
    print(f"Stop reason: {body.get('stop_reason')}")
    print(f"Output: {text}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
