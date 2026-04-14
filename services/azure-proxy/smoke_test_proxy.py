from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

import httpx


def _load_env(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    if not path.exists():
        return values
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip().strip('"').strip("'")
    return values


def main() -> int:
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    parser = argparse.ArgumentParser(description="Smoke test local Anthropic-compatible proxy.")
    parser.add_argument("--model", default="", help="Model alias to send to local proxy.")
    parser.add_argument("--prompt", default="Tra ve 1 cau chao ngan gon bang tieng Viet.", help="Prompt for smoke test.")
    args = parser.parse_args()

    project_root = Path(__file__).resolve().parent
    env = _load_env(project_root / ".env")
    port = int(env.get("PROXY_PORT", "8009"))
    base_url = f"http://127.0.0.1:{port}"
    model_alias = args.model or env.get("SMOKE_MODEL_ALIAS", env.get("DEFAULT_SMOKE_MODEL", "azure-sonnet"))

    payload = {
        "model": model_alias,
        "max_tokens": 256,
        "messages": [
            {
                "role": "user",
                "content": [{"type": "text", "text": args.prompt}],
            }
        ],
    }

    with httpx.Client(base_url=base_url, timeout=60.0) as client:
        response = client.post(
            "/v1/messages",
            headers={
                "content-type": "application/json",
                "anthropic-version": "2023-06-01",
                "x-api-key": os.getenv("ANTHROPIC_API_KEY", "local-azure-proxy"),
            },
            json=payload,
        )
        response.raise_for_status()
        data = response.json()

    print(json.dumps(data, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
