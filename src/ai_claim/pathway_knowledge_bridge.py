from __future__ import annotations

import mimetypes
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import httpx

from .settings import SETTINGS


ROOT_KIND_MAP = {
    "insurance_rules": {"domain": "insurance", "kind": "insurance_rulebook"},
    "benefit_tables": {"domain": "insurance", "kind": "benefit_table"},
    "service_tables": {"domain": "taxonomy", "kind": "service_table"},
    "symptom_tables": {"domain": "taxonomy", "kind": "symptom_table"},
    "legal_documents": {"domain": "legal", "kind": "legal_document"},
}


def _infer_protocol_kind(file_path: Path) -> str:
    suffix = file_path.suffix.lower()
    if suffix == ".pdf":
        return "protocol_pdf"
    if suffix == ".txt":
        return "protocol_text"
    return "protocol_markdown"


@dataclass(slots=True)
class PathwayKnowledgeBridge:
    base_url: str = SETTINGS.pathway_api_base_url
    timeout_seconds: float = 180.0

    def _client(self) -> httpx.Client:
        return httpx.Client(base_url=self.base_url, timeout=self.timeout_seconds)

    def _request_json(self, method: str, path: str, **kwargs: Any) -> dict[str, Any]:
        with self._client() as client:
            response = client.request(method, path, **kwargs)
            response.raise_for_status()
            return response.json()

    def bootstrap(self) -> dict[str, Any]:
        try:
            return self._request_json("GET", "/api/knowledge/bootstrap")
        except Exception as exc:
            return {"status": "unavailable", "error": str(exc)}

    def list_assets(self, limit: int = 100) -> dict[str, Any]:
        try:
            return self._request_json("GET", "/api/knowledge/assets", params={"limit": limit})
        except Exception as exc:
            return {"status": "unavailable", "error": str(exc)}

    def get_asset(self, asset_id: str) -> dict[str, Any]:
        return self._request_json("GET", f"/api/knowledge/assets/{asset_id}")

    def impact_report(self, asset_id: str) -> dict[str, Any]:
        return self._request_json("GET", f"/api/knowledge/assets/{asset_id}/impact-report")

    def graph_trace(self, asset_id: str) -> dict[str, Any]:
        return self._request_json("GET", f"/api/knowledge/assets/{asset_id}/graph-trace")

    def text_workspace(self, asset_id: str) -> dict[str, Any]:
        return self._request_json("GET", f"/api/knowledge/assets/{asset_id}/text-workspace")

    def get_run_status(self, run_id: str) -> dict[str, Any]:
        return self._request_json("GET", f"/api/ingest/{run_id}")

    def wait_for_run(self, run_id: str, *, timeout_seconds: float = 240.0, poll_seconds: float = 2.0) -> dict[str, Any]:
        deadline = time.monotonic() + timeout_seconds
        last_payload: dict[str, Any] = {}
        while time.monotonic() < deadline:
            last_payload = self.get_run_status(run_id)
            if str(last_payload.get("status") or "").lower() not in {"running", "pending"}:
                return last_payload
            time.sleep(poll_seconds)
        return {
            "status": "timeout",
            "run_id": run_id,
            "last_payload": last_payload,
        }

    def upload_asset(
        self,
        *,
        root_key: str,
        file_path: Path,
        auto_ingest: bool = False,
        namespace: str = "ontology_v2",
        source_type: str | None = None,
        title: str | None = None,
        wait_for_completion: bool = False,
    ) -> dict[str, Any]:
        file_path = Path(file_path)
        if not file_path.exists():
            raise FileNotFoundError(file_path)
        domain_kind = self._domain_kind_for_root(root_key, file_path)
        content_type = mimetypes.guess_type(file_path.name)[0] or "application/octet-stream"
        data = {
            "kind": domain_kind["kind"],
            "domain": domain_kind["domain"],
            "title": title or file_path.stem,
            "source_type": source_type or "",
            "auto_ingest": "true" if auto_ingest else "false",
            "namespace": namespace,
        }
        with file_path.open("rb") as handle, self._client() as client:
            response = client.post(
                "/api/knowledge/upload",
                data=data,
                files={"file": (file_path.name, handle, content_type)},
            )
            response.raise_for_status()
            payload = response.json()
        started_run = payload.get("started_run") or {}
        run_id = started_run.get("run_id")
        if wait_for_completion and run_id:
            payload["final_run"] = self.wait_for_run(run_id)
        payload["bridge"] = {
            "root_key": root_key,
            "kind": domain_kind["kind"],
            "domain": domain_kind["domain"],
            "direct_ingest_supported": domain_kind["kind"] in {"protocol_pdf", "protocol_text", "protocol_markdown", "service_table"},
        }
        return payload

    def _domain_kind_for_root(self, root_key: str, file_path: Path) -> dict[str, str]:
        if root_key == "protocols":
            return {"domain": "protocols", "kind": _infer_protocol_kind(file_path)}
        if root_key in ROOT_KIND_MAP:
            return dict(ROOT_KIND_MAP[root_key])
        raise KeyError(f"Root {root_key} has no Pathway bridge mapping")
