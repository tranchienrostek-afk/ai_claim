from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

from server_support.ontology_v2_inspector_store import OntologyV2InspectorStore
from server_support.paths import (
    ARCHITECTURE_DOCS_DIR,
    BASE_DIR,
    CLAIMS_INSIGHTS_CLAIMS_DIR,
    CLAIMS_INSIGHTS_INSURANCE_DIR,
    CLAIMS_INSIGHTS_REFERENCE_DIR,
    CLAIMS_INSIGHTS_REPORTS_DIR,
    CLAUDE_BRIDGE_DIR,
    CLAUDE_BRIDGE_FEEDBACK_DIR,
    CLAUDE_BRIDGE_INTERACTIONS_DIR,
    CONFIG_DIR,
    DATA_DIR,
    DATA_SCRIPT_DIR,
    DATA_VIEW_DIR,
    DATATEST_ASSETS_DIR,
    DATATEST_CASES_DIR,
    DATATEST_DATASETS_DIR,
    DATATEST_DIR,
    DATATEST_REPORTS_DIR,
    DATATEST_SOURCE_DOCS_DIR,
    DATATEST_V02_DIR,
    DATATEST_V03_DIR,
    EXPERIENCE_MEMORY_DIR,
    EXTRACTED_TEXT_DIR,
    INGEST_CONFIGS_DIR,
    LOGS_DIR,
    PATHWAY_DATA_ARCHITECTURE_DOC_PATH,
    PATHWAY_DATA_ARCHITECTURE_SPEC_PATH,
    REFERENCE_PDFS_DIR,
    REPORTS_DIR,
    ROOT_DIR,
    RUNS_DIR,
    TESTCASE_TRACE_RUNS_DIR,
    UPLOADS_DIR,
    WORKSPACES_DIR,
    ensure_pathway_data_layout,
)


MAX_SAMPLE_ENTRIES = 6
MAX_RECURSIVE_COUNT = 50000

_PATH_REF_MAP: Dict[str, Path] = {
    "ARCHITECTURE_DOCS_DIR": ARCHITECTURE_DOCS_DIR,
    "CLAIMS_INSIGHTS_CLAIMS_DIR": CLAIMS_INSIGHTS_CLAIMS_DIR,
    "CLAIMS_INSIGHTS_INSURANCE_DIR": CLAIMS_INSIGHTS_INSURANCE_DIR,
    "CLAIMS_INSIGHTS_REFERENCE_DIR": CLAIMS_INSIGHTS_REFERENCE_DIR,
    "CLAIMS_INSIGHTS_REPORTS_DIR": CLAIMS_INSIGHTS_REPORTS_DIR,
    "CLAUDE_BRIDGE_DIR": CLAUDE_BRIDGE_DIR,
    "CLAUDE_BRIDGE_FEEDBACK_DIR": CLAUDE_BRIDGE_FEEDBACK_DIR,
    "CLAUDE_BRIDGE_INTERACTIONS_DIR": CLAUDE_BRIDGE_INTERACTIONS_DIR,
    "CONFIG_DIR": CONFIG_DIR,
    "DATA_DIR": DATA_DIR,
    "DATA_SCRIPT_DIR": DATA_SCRIPT_DIR,
    "DATA_VIEW_DIR": DATA_VIEW_DIR,
    "DATATEST_ASSETS_DIR": DATATEST_ASSETS_DIR,
    "DATATEST_CASES_DIR": DATATEST_CASES_DIR,
    "DATATEST_DATASETS_DIR": DATATEST_DATASETS_DIR,
    "DATATEST_DIR": DATATEST_DIR,
    "DATATEST_REPORTS_DIR": DATATEST_REPORTS_DIR,
    "DATATEST_SOURCE_DOCS_DIR": DATATEST_SOURCE_DOCS_DIR,
    "DATATEST_V02_DIR": DATATEST_V02_DIR,
    "DATATEST_V03_DIR": DATATEST_V03_DIR,
    "EXPERIENCE_MEMORY_DIR": EXPERIENCE_MEMORY_DIR,
    "EXTRACTED_TEXT_DIR": EXTRACTED_TEXT_DIR,
    "INGEST_CONFIGS_DIR": INGEST_CONFIGS_DIR,
    "LOGS_DIR": LOGS_DIR,
    "REFERENCE_PDFS_DIR": REFERENCE_PDFS_DIR,
    "REPORTS_DIR": REPORTS_DIR,
    "ROOT_DIR": ROOT_DIR,
    "RUNS_DIR": RUNS_DIR,
    "TESTCASE_TRACE_RUNS_DIR": TESTCASE_TRACE_RUNS_DIR,
    "UPLOADS_DIR": UPLOADS_DIR,
    "WORKSPACES_DIR": WORKSPACES_DIR,
}


def _safe_iso(path: Path) -> Optional[str]:
    try:
        return datetime.fromtimestamp(path.stat().st_mtime).isoformat(timespec="seconds")
    except OSError:
        return None


def _count_recursive_files(path: Path) -> dict[str, Any]:
    if not path.exists() or not path.is_dir():
        return {"file_count": 0, "truncated": False}

    count = 0
    truncated = False
    for child in path.rglob("*"):
        if child.is_file():
            count += 1
            if count >= MAX_RECURSIVE_COUNT:
                truncated = True
                break
    return {"file_count": count, "truncated": truncated}


def _sample_entries(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    if path.is_file():
        return [
            {
                "name": path.name,
                "kind": "file",
                "size_bytes": path.stat().st_size,
            }
        ]

    entries = []
    for child in sorted(path.iterdir(), key=lambda item: item.name.lower())[:MAX_SAMPLE_ENTRIES]:
        item = {
            "name": child.name,
            "kind": "directory" if child.is_dir() else "file",
        }
        if child.is_file():
            item["size_bytes"] = child.stat().st_size
        entries.append(item)
    return entries


class PathwayDataArchitectureStore:
    def __init__(
        self,
        ontology_store: Optional[OntologyV2InspectorStore] = None,
        spec_path: Path = PATHWAY_DATA_ARCHITECTURE_SPEC_PATH,
    ) -> None:
        self.ontology_store = ontology_store or OntologyV2InspectorStore()
        self.spec_path = spec_path

    def _load_spec(self) -> dict[str, Any]:
        with open(self.spec_path, "r", encoding="utf-8") as handle:
            payload = json.load(handle)
        if not isinstance(payload, dict):
            raise ValueError("Pathway data architecture spec must be a JSON object.")
        return payload

    def _resolve_surface_path(self, surface: dict[str, Any]) -> Path:
        path_ref = str(surface.get("path_ref") or "").strip()
        if path_ref:
            if path_ref not in _PATH_REF_MAP:
                raise KeyError(f"Unknown path_ref in data architecture spec: {path_ref}")
            return _PATH_REF_MAP[path_ref]

        relative_path = str(surface.get("relative_path") or "").strip()
        if not relative_path:
            raise ValueError("Each surface must declare path_ref or relative_path.")
        return BASE_DIR / Path(relative_path)

    def _surface_payload(self, surface: dict[str, Any]) -> dict[str, Any]:
        resolved = self._resolve_surface_path(surface)
        exists = resolved.exists()
        payload = {
            "kind": surface.get("kind", "directory"),
            "role": surface.get("role", ""),
            "status": surface.get("status", "active"),
            "required": bool(surface.get("required", False)),
            "path": str(resolved),
            "exists": exists,
            "last_modified": _safe_iso(resolved) if exists else None,
            "sample_entries": _sample_entries(resolved) if exists else [],
        }
        if resolved.is_dir():
            stats = _count_recursive_files(resolved)
            payload["file_count"] = stats["file_count"]
            payload["recursive_count_truncated"] = stats["truncated"]
        elif exists and resolved.is_file():
            payload["size_bytes"] = resolved.stat().st_size
        return payload

    def ensure_layout(self) -> dict[str, Any]:
        ensure_pathway_data_layout()
        spec = self._load_spec()
        created_paths: list[str] = []
        for domain in spec.get("domains", []):
            for surface in domain.get("surfaces", []):
                if surface.get("kind") != "directory" or not surface.get("create_if_missing"):
                    continue
                resolved = self._resolve_surface_path(surface)
                if not resolved.exists():
                    resolved.mkdir(parents=True, exist_ok=True)
                    created_paths.append(str(resolved))
        return {
            "created_paths": created_paths,
            "created_count": len(created_paths),
        }

    def bootstrap(self) -> dict[str, Any]:
        spec = self._load_spec()
        ontology_error = None
        ontology_bootstrap: dict[str, Any] = {}
        actual_namespaces: set[str] = set()
        try:
            ontology_bootstrap = self.ontology_store.bootstrap()
            actual_namespaces = {
                str(item.get("namespace") or "").strip()
                for item in (ontology_bootstrap.get("namespaces") or [])
                if str(item.get("namespace") or "").strip()
            }
            active_namespace = str(ontology_bootstrap.get("active_namespace") or "").strip()
            if active_namespace:
                actual_namespaces.add(active_namespace)
        except Exception as exc:
            ontology_error = str(exc)

        domains: list[dict[str, Any]] = []
        warnings: list[str] = []
        required_missing_count = 0
        total_surface_count = 0
        existing_surface_count = 0

        for domain in spec.get("domains", []):
            surfaces = [self._surface_payload(surface) for surface in domain.get("surfaces", [])]
            total_surface_count += len(surfaces)
            existing_surface_count += sum(1 for surface in surfaces if surface.get("exists"))
            missing_required = [surface for surface in surfaces if surface.get("required") and not surface.get("exists")]
            required_missing_count += len(missing_required)
            for surface in missing_required:
                warnings.append(
                    f"Missing required surface for domain '{domain.get('id')}': {surface.get('role')} -> {surface.get('path')}"
                )

            expected_namespaces = [str(item).strip() for item in (domain.get("neo4j_namespaces") or []) if str(item).strip()]
            namespace_status = [
                {
                    "namespace": namespace,
                    "present": namespace in actual_namespaces,
                }
                for namespace in expected_namespaces
            ]
            for item in namespace_status:
                if not item["present"]:
                    warnings.append(
                        f"Expected Neo4j namespace missing for domain '{domain.get('id')}': {item['namespace']}"
                    )

            domains.append(
                {
                    "id": domain.get("id"),
                    "label": domain.get("label"),
                    "description": domain.get("description"),
                    "query_contracts": domain.get("query_contracts") or [],
                    "neo4j_namespaces": namespace_status,
                    "surfaces": surfaces,
                    "surface_count": len(surfaces),
                    "existing_surface_count": sum(1 for surface in surfaces if surface.get("exists")),
                    "missing_required_count": len(missing_required),
                }
            )

        summary = {
            "domain_count": len(domains),
            "surface_count": total_surface_count,
            "existing_surface_count": existing_surface_count,
            "missing_required_count": required_missing_count,
            "warning_count": len(warnings),
            "ontology_namespace_count": len(actual_namespaces),
        }

        return {
            "schema_version": spec.get("schema_version", "pathway_data_architecture.v1"),
            "updated": spec.get("updated"),
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "mission": spec.get("mission", ""),
            "principles": spec.get("principles") or [],
            "logical_layers": spec.get("logical_layers") or [],
            "question_families": spec.get("question_families") or [],
            "operating_contract": spec.get("operating_contract") or {},
            "summary": summary,
            "domains": domains,
            "warnings": warnings[:32],
            "documentation": {
                "spec_path": str(self.spec_path),
                "doc_path": str(PATHWAY_DATA_ARCHITECTURE_DOC_PATH),
                "doc_exists": PATHWAY_DATA_ARCHITECTURE_DOC_PATH.exists(),
            },
            "storage_roots": {
                "base_dir": str(BASE_DIR),
                "data_dir": str(DATA_DIR),
                "workspaces_dir": str(WORKSPACES_DIR),
                "config_dir": str(CONFIG_DIR),
                "spec_path": str(self.spec_path),
            },
            "ontology_context": {
                "available": ontology_error is None,
                "error": ontology_error,
                "active_namespace": ontology_bootstrap.get("active_namespace"),
                "namespaces": ontology_bootstrap.get("namespaces") or [],
            },
        }
