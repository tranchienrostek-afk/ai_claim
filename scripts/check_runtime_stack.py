from __future__ import annotations

import json
import sys
from pathlib import Path

import httpx

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.ai_claim.settings import SETTINGS


def _fetch(url: str) -> dict[str, object]:
    try:
        with httpx.Client(timeout=5.0) as client:
            response = client.get(url)
            return {
                "status": "up" if response.is_success else "error",
                "status_code": response.status_code,
                "body_preview": response.text[:300],
            }
    except Exception as exc:
        return {
            "status": "down",
            "error": str(exc),
        }


def main() -> None:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    payload = {
        "ai_claim": _fetch("http://127.0.0.1:9780/health"),
        "pathway": _fetch(f"{SETTINGS.pathway_api_base_url.rstrip('/')}/health"),
        "router": _fetch(f"{SETTINGS.router_base_url.rstrip('/')}/v1/models"),
        "azure_proxy": _fetch(f"{SETTINGS.azure_proxy_base_url.rstrip('/')}/health"),
        "config": {
            "pathway_api_base_url": SETTINGS.pathway_api_base_url,
            "router_base_url": SETTINGS.router_base_url,
            "azure_proxy_base_url": SETTINGS.azure_proxy_base_url,
            "max_upload_bytes": SETTINGS.max_upload_bytes,
        },
    }
    print(json.dumps(payload, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
