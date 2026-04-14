from __future__ import annotations

import hashlib
import json
import os
import threading
import unicodedata
from datetime import datetime
from pathlib import Path, PureWindowsPath
from typing import Any, Optional

from neo4j import GraphDatabase
from openai import AzureOpenAI

from multi_disease_ingest import MultiDiseaseSplitter
from ontology_v2_ingest import OntologyV2Ingest
from runtime_env import load_notebooklm_env
from server_support.pathway_service_table_ingest import (
    DEFAULT_NAMESPACE as SERVICE_TABLE_DEFAULT_NAMESPACE,
    PathwayServiceTableIngestor,
)
from server_support.paths import (
    BASE_DIR,
    CLAIMS_INSIGHTS_INSURANCE_DIR,
    CLAIMS_INSIGHTS_REFERENCE_DIR,
    DATATEST_CASES_DIR,
    DATATEST_SOURCE_DOCS_DIR,
    EXPERIENCE_MEMORY_DIR,
    KNOWLEDGE_ASSETS_DIR,
    KNOWLEDGE_BENCHMARKS_DIR,
    KNOWLEDGE_BENEFIT_TABLES_DIR,
    KNOWLEDGE_ASSET_GUIDES_DIR,
    KNOWLEDGE_DISEASE_GUIDES_DIR,
    KNOWLEDGE_DOMAIN_GUIDES_DIR,
    KNOWLEDGE_GUIDES_DIR,
    KNOWLEDGE_INSURANCE_RULES_DIR,
    KNOWLEDGE_LEGAL_DOCS_DIR,
    KNOWLEDGE_MANIFESTS_DIR,
    KNOWLEDGE_MEMORY_DIR,
    KNOWLEDGE_MISC_DIR,
    KNOWLEDGE_PROTOCOL_PDFS_DIR,
    KNOWLEDGE_PROTOCOL_TEXTS_DIR,
    KNOWLEDGE_REGISTRY_PATH,
    KNOWLEDGE_SERVICE_TABLES_DIR,
    KNOWLEDGE_SNAPSHOTS_DIR,
    KNOWLEDGE_SYMPTOM_TABLES_DIR,
    KNOWLEDGE_TEXT_VIEWS_DIR,
    REFERENCE_PDFS_DIR,
    ROOT_DIR,
    UPLOADS_DIR,
    ensure_pathway_data_layout,
)
from universal_ingest import DocumentAnalyzer, UniversalIngest, _slugify


load_notebooklm_env()


SUPPORTED_PROTOCOL_INGEST_KINDS = {
    "protocol_pdf",
    "protocol_text",
    "protocol_markdown",
}

SUPPORTED_DIRECT_INGEST_KINDS = SUPPORTED_PROTOCOL_INGEST_KINDS | {
    "service_table",
}

SCANNED_FILE_EXTENSIONS = {
    ".pdf",
    ".txt",
    ".md",
    ".markdown",
    ".docx",
    ".json",
    ".jsonl",
    ".xlsx",
    ".xls",
    ".csv",
}

IGNORED_DIR_NAMES = {
    "__pycache__",
    ".git",
    ".venv",
    "node_modules",
}


class PathwayKnowledgeRegistryStore:
    """Registry of knowledge assets and the path from source files to graph evidence."""

    def __init__(self, ontology_store=None):
        self.ontology_store = ontology_store
        self.registry_path = KNOWLEDGE_REGISTRY_PATH
        self._lock = threading.RLock()
        self.service_table_ingestor = PathwayServiceTableIngestor()
        self.ensure_layout()

    def ensure_layout(self) -> None:
        ensure_pathway_data_layout()
        for path in (
            KNOWLEDGE_ASSETS_DIR,
            KNOWLEDGE_PROTOCOL_PDFS_DIR,
            KNOWLEDGE_PROTOCOL_TEXTS_DIR,
            KNOWLEDGE_INSURANCE_RULES_DIR,
            KNOWLEDGE_BENEFIT_TABLES_DIR,
            KNOWLEDGE_LEGAL_DOCS_DIR,
            KNOWLEDGE_SERVICE_TABLES_DIR,
            KNOWLEDGE_SYMPTOM_TABLES_DIR,
            KNOWLEDGE_BENCHMARKS_DIR,
            KNOWLEDGE_MEMORY_DIR,
            KNOWLEDGE_MISC_DIR,
            KNOWLEDGE_MANIFESTS_DIR,
            KNOWLEDGE_SNAPSHOTS_DIR,
            KNOWLEDGE_TEXT_VIEWS_DIR,
            KNOWLEDGE_GUIDES_DIR,
            KNOWLEDGE_DOMAIN_GUIDES_DIR,
            KNOWLEDGE_DISEASE_GUIDES_DIR,
            KNOWLEDGE_ASSET_GUIDES_DIR,
        ):
            path.mkdir(parents=True, exist_ok=True)
        self._ensure_domain_guide_scaffolds()

        if not self.registry_path.exists():
            self._save_registry(self._default_registry())

    def _default_registry(self) -> dict[str, Any]:
        return {
            "schema_version": "pathway_knowledge_registry.v1",
            "generated_at": self._now_iso(),
            "mission": (
                "Quan ly tri thuc toan bo Pathway theo chu trinh file -> config -> ingest -> graph -> reverse trace."
            ),
            "assets": {},
            "managed_roots": self._managed_roots_manifest(),
        }

    def _ensure_domain_guide_scaffolds(self) -> None:
        domain_specs = [
            ("protocols", "Kho phác đồ, bệnh, guideline và text bóc từ PDF."),
            ("insurance", "Kho quyền lợi, loại trừ, rulebook và tác động hợp đồng."),
            ("legal", "Kho pháp lý, văn bản, quy tắc và căn cứ áp dụng."),
            ("taxonomy", "Kho bảng dịch vụ, bảng triệu chứng, danh mục chuẩn hóa."),
            ("benchmark", "Kho testcase, benchmark và bộ đối chiếu."),
            ("memory", "Kho kinh nghiệm, lesson learned và anti-pattern."),
        ]
        for slug, summary in domain_specs:
            self._ensure_workspace_scaffold(
                KNOWLEDGE_DOMAIN_GUIDES_DIR / slug,
                title=f"Domain {slug}",
                summary=summary,
                extra_lines=[
                    f"- Domain: {slug}",
                    "- Mục tiêu: giúp Pathway biết nên tìm dữ liệu, graph và evidence ở đâu trước.",
                ],
            )

    def _ensure_workspace_scaffold(
        self,
        directory: Path,
        *,
        title: str,
        summary: str,
        extra_lines: Optional[list[str]] = None,
    ) -> None:
        directory.mkdir(parents=True, exist_ok=True)
        (directory / "skills").mkdir(parents=True, exist_ok=True)
        (directory / "notes").mkdir(parents=True, exist_ok=True)
        (directory / "review").mkdir(parents=True, exist_ok=True)
        claude_path = directory / "CLAUDE.md"
        readme_path = directory / "README.md"
        review_path = directory / "review" / "impact_latest.json"
        review_queue_path = directory / "review" / "human_review.md"
        if not claude_path.exists():
            lines = [
                f"# {title}",
                "",
                summary,
                "",
                "## Cách Pathway nên tìm thông tin",
                "- Bắt đầu từ file nguồn, source_file, version và graph trace.",
                "- Sau đó tìm node/edge liên quan trong Neo4j.",
                "- Nếu thiếu evidence, mở thêm bảng biểu, testcase và memory liên quan.",
                "- Khi có thay đổi quy tắc hoặc quyền lợi, luôn chạy impact review trước khi kết luận.",
            ]
            if extra_lines:
                lines.extend(["", "## Gợi ý thêm", *extra_lines])
            claude_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        if not readme_path.exists():
            readme_path.write_text(
                "\n".join(
                    [
                        f"# {title}",
                        "",
                        "- `CLAUDE.md`: hướng dẫn tìm kiếm và suy luận cho Pathway.",
                        "- `skills/`: skill hoặc snippet domain-specific.",
                        "- `notes/`: ghi chú vận hành.",
                        "- `review/`: kết quả impact analysis và review queue.",
                        "",
                    ]
                ),
                encoding="utf-8",
            )
        if not review_path.exists():
            review_path.write_text(
                json.dumps(
                    {
                        "status": "not_generated",
                        "message": "Impact report sẽ được ghi vào đây khi asset hoặc rule được phân tích.",
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )
        if not review_queue_path.exists():
            review_queue_path.write_text(
                "\n".join(
                    [
                        f"# Human Review - {title}",
                        "",
                        "- Chưa có review item nào.",
                        "",
                    ]
                ),
                encoding="utf-8",
            )

    def _load_registry(self) -> dict[str, Any]:
        self.ensure_layout()
        try:
            return json.loads(self.registry_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            payload = self._default_registry()
            self._save_registry(payload)
            return payload

    def _save_registry(self, payload: dict[str, Any]) -> None:
        payload["generated_at"] = self._now_iso()
        payload["managed_roots"] = self._managed_roots_manifest()
        self.registry_path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    def _managed_roots_manifest(self) -> list[dict[str, Any]]:
        return [
            {
                "key": "managed_protocol_pdfs",
                "label": "Protocol PDFs",
                "path": str(KNOWLEDGE_PROTOCOL_PDFS_DIR),
                "relative_path": self._portable_path(KNOWLEDGE_PROTOCOL_PDFS_DIR),
                "domain": "protocols",
                "default_kind": "protocol_pdf",
            },
            {
                "key": "managed_protocol_texts",
                "label": "Protocol Texts",
                "path": str(KNOWLEDGE_PROTOCOL_TEXTS_DIR),
                "relative_path": self._portable_path(KNOWLEDGE_PROTOCOL_TEXTS_DIR),
                "domain": "protocols",
                "default_kind": "protocol_text",
            },
            {
                "key": "managed_insurance_rulebooks",
                "label": "Insurance Rulebooks",
                "path": str(KNOWLEDGE_INSURANCE_RULES_DIR),
                "relative_path": self._portable_path(KNOWLEDGE_INSURANCE_RULES_DIR),
                "domain": "insurance",
                "default_kind": "insurance_rulebook",
            },
            {
                "key": "managed_benefit_tables",
                "label": "Benefit Tables",
                "path": str(KNOWLEDGE_BENEFIT_TABLES_DIR),
                "relative_path": self._portable_path(KNOWLEDGE_BENEFIT_TABLES_DIR),
                "domain": "insurance",
                "default_kind": "benefit_table",
            },
            {
                "key": "managed_legal_documents",
                "label": "Legal Documents",
                "path": str(KNOWLEDGE_LEGAL_DOCS_DIR),
                "relative_path": self._portable_path(KNOWLEDGE_LEGAL_DOCS_DIR),
                "domain": "legal",
                "default_kind": "legal_document",
            },
            {
                "key": "managed_service_tables",
                "label": "Service Tables",
                "path": str(KNOWLEDGE_SERVICE_TABLES_DIR),
                "relative_path": self._portable_path(KNOWLEDGE_SERVICE_TABLES_DIR),
                "domain": "taxonomy",
                "default_kind": "service_table",
            },
            {
                "key": "managed_symptom_tables",
                "label": "Symptom Tables",
                "path": str(KNOWLEDGE_SYMPTOM_TABLES_DIR),
                "relative_path": self._portable_path(KNOWLEDGE_SYMPTOM_TABLES_DIR),
                "domain": "taxonomy",
                "default_kind": "symptom_table",
            },
            {
                "key": "managed_benchmarks",
                "label": "Benchmarks",
                "path": str(KNOWLEDGE_BENCHMARKS_DIR),
                "relative_path": self._portable_path(KNOWLEDGE_BENCHMARKS_DIR),
                "domain": "benchmark",
                "default_kind": "benchmark_bundle",
            },
            {
                "key": "managed_memory",
                "label": "Memory",
                "path": str(KNOWLEDGE_MEMORY_DIR),
                "relative_path": self._portable_path(KNOWLEDGE_MEMORY_DIR),
                "domain": "memory",
                "default_kind": "experience_memory",
            },
            {
                "key": "managed_misc",
                "label": "Misc",
                "path": str(KNOWLEDGE_MISC_DIR),
                "relative_path": self._portable_path(KNOWLEDGE_MISC_DIR),
                "domain": "misc",
                "default_kind": "knowledge_file",
            },
            {
                "key": "domain_guides",
                "label": "Domain Guides",
                "path": str(KNOWLEDGE_DOMAIN_GUIDES_DIR),
                "relative_path": self._portable_path(KNOWLEDGE_DOMAIN_GUIDES_DIR),
                "domain": "governance",
                "default_kind": "guide_workspace",
            },
            {
                "key": "disease_guides",
                "label": "Disease Guides",
                "path": str(KNOWLEDGE_DISEASE_GUIDES_DIR),
                "relative_path": self._portable_path(KNOWLEDGE_DISEASE_GUIDES_DIR),
                "domain": "governance",
                "default_kind": "guide_workspace",
            },
            {
                "key": "asset_guides",
                "label": "Asset Guides",
                "path": str(KNOWLEDGE_ASSET_GUIDES_DIR),
                "relative_path": self._portable_path(KNOWLEDGE_ASSET_GUIDES_DIR),
                "domain": "governance",
                "default_kind": "guide_workspace",
            },
        ]

    def _source_roots(self) -> list[dict[str, Any]]:
        roadmap_protocol_root = ROOT_DIR / "roadmap_master_data" / "Extracted_Protocols"
        return [
            {
                "key": "managed_protocol_pdfs",
                "path": KNOWLEDGE_PROTOCOL_PDFS_DIR,
                "domain": "protocols",
                "default_kind": "protocol_pdf",
                "managed": True,
            },
            {
                "key": "managed_protocol_texts",
                "path": KNOWLEDGE_PROTOCOL_TEXTS_DIR,
                "domain": "protocols",
                "default_kind": "protocol_text",
                "managed": True,
            },
            {
                "key": "managed_insurance_rulebooks",
                "path": KNOWLEDGE_INSURANCE_RULES_DIR,
                "domain": "insurance",
                "default_kind": "insurance_rulebook",
                "managed": True,
            },
            {
                "key": "managed_benefit_tables",
                "path": KNOWLEDGE_BENEFIT_TABLES_DIR,
                "domain": "insurance",
                "default_kind": "benefit_table",
                "managed": True,
            },
            {
                "key": "managed_legal_documents",
                "path": KNOWLEDGE_LEGAL_DOCS_DIR,
                "domain": "legal",
                "default_kind": "legal_document",
                "managed": True,
            },
            {
                "key": "managed_service_tables",
                "path": KNOWLEDGE_SERVICE_TABLES_DIR,
                "domain": "taxonomy",
                "default_kind": "service_table",
                "managed": True,
            },
            {
                "key": "managed_symptom_tables",
                "path": KNOWLEDGE_SYMPTOM_TABLES_DIR,
                "domain": "taxonomy",
                "default_kind": "symptom_table",
                "managed": True,
            },
            {
                "key": "uploads",
                "path": UPLOADS_DIR,
                "domain": "protocols",
                "default_kind": "protocol_pdf",
                "managed": False,
            },
            {
                "key": "roadmap_protocol_texts",
                "path": roadmap_protocol_root,
                "domain": "protocols",
                "default_kind": "protocol_text",
                "managed": False,
            },
            {
                "key": "reference_pdfs",
                "path": REFERENCE_PDFS_DIR,
                "domain": "protocols",
                "default_kind": "protocol_pdf",
                "managed": False,
            },
            {
                "key": "claims_reference",
                "path": CLAIMS_INSIGHTS_REFERENCE_DIR,
                "domain": "protocols",
                "default_kind": "knowledge_file",
                "managed": False,
            },
            {
                "key": "insurance_workspace",
                "path": CLAIMS_INSIGHTS_INSURANCE_DIR,
                "domain": "insurance",
                "default_kind": "knowledge_file",
                "managed": False,
            },
            {
                "key": "datatest_source_docs",
                "path": DATATEST_SOURCE_DOCS_DIR,
                "domain": "benchmark",
                "default_kind": "benchmark_source_doc",
                "managed": False,
            },
            {
                "key": "datatest_cases",
                "path": DATATEST_CASES_DIR,
                "domain": "benchmark",
                "default_kind": "testcase_bundle",
                "managed": False,
            },
            {
                "key": "experience_memory",
                "path": EXPERIENCE_MEMORY_DIR,
                "domain": "memory",
                "default_kind": "experience_memory",
                "managed": False,
            },
        ]
    def sync_known_sources(self) -> dict[str, Any]:
        with self._lock:
            registry = self._load_registry()
            assets = registry.setdefault("assets", {})
            discovered_ids: list[str] = []
            new_assets: list[str] = []
            updated_assets: list[str] = []

            for spec in self._source_roots():
                root_path = Path(spec["path"])
                if not root_path.exists():
                    continue

                for file_path in self._iter_candidate_files(root_path):
                    asset_id = self._asset_id(spec["key"], file_path, root_path)
                    asset = assets.get(asset_id)
                    version_payload = self._build_version_payload(file_path, spec, root_path)
                    domain, kind, tags = self._infer_asset_profile(file_path, spec)
                    relative_path = self._relative_to_root(file_path, root_path)
                    managed_relative_path = self._relative_to_root(file_path, BASE_DIR)
                    source_type = self._infer_source_type(file_path)

                    if asset is None:
                        asset = {
                            "asset_id": asset_id,
                            "title": file_path.stem,
                            "domain": domain,
                            "kind": kind,
                            "tags": tags,
                            "file_extension": file_path.suffix.lower(),
                            "source_root_key": spec["key"],
                            "source_root_path": str(root_path),
                            "source_path": str(file_path),
                            "source_relative_path": relative_path,
                            "managed": bool(spec.get("managed")),
                            "managed_relative_path": managed_relative_path,
                            "status": "discovered",
                            "created_at": self._now_iso(),
                            "updated_at": self._now_iso(),
                            "config": self._default_asset_config(
                                kind=kind,
                                source_type=source_type,
                                managed=bool(spec.get("managed")),
                            ),
                            "ingest": self._default_ingest_state(kind),
                            "versions": [version_payload],
                            "current_version_id": version_payload["version_id"],
                            "version_count": 1,
                        }
                        asset["review"] = self._default_review_state()
                        self._ensure_asset_scaffolds(asset)
                        assets[asset_id] = asset
                        new_assets.append(asset_id)
                    else:
                        asset["title"] = asset.get("title") or file_path.stem
                        asset["domain"] = domain
                        asset["kind"] = kind
                        asset["tags"] = sorted(set(asset.get("tags") or []) | set(tags))
                        asset["file_extension"] = file_path.suffix.lower()
                        asset["source_root_key"] = spec["key"]
                        asset["source_root_path"] = str(root_path)
                        asset["source_path"] = str(file_path)
                        asset["source_relative_path"] = relative_path
                        asset["managed"] = bool(spec.get("managed"))
                        asset["managed_relative_path"] = managed_relative_path
                        asset.setdefault("config", self._default_asset_config(kind, source_type, bool(spec.get("managed"))))
                        asset.setdefault("ingest", self._default_ingest_state(kind))
                        if asset["config"].get("source_type") in {None, ""}:
                            asset["config"]["source_type"] = source_type

                        versions = asset.setdefault("versions", [])
                        if not versions or versions[-1].get("sha1") != version_payload["sha1"]:
                            versions.append(version_payload)
                            updated_assets.append(asset_id)
                            self._mark_asset_for_review(
                                asset,
                                reason=f"Phát hiện phiên bản mới cho {asset.get('kind') or 'asset'}",
                            )
                        asset["current_version_id"] = versions[-1]["version_id"]
                        asset["version_count"] = len(versions)
                        asset["updated_at"] = self._now_iso()
                        self._ensure_asset_scaffolds(asset)

                    discovered_ids.append(asset_id)

            registry["last_sync_at"] = self._now_iso()
            registry["last_sync_summary"] = {
                "asset_count": len(assets),
                "new_asset_count": len(new_assets),
                "updated_asset_count": len(updated_assets),
                "new_asset_ids": new_assets,
                "updated_asset_ids": updated_assets,
            }
            self._save_registry(registry)
            return registry["last_sync_summary"]

    def bootstrap(self, refresh: bool = False) -> dict[str, Any]:
        if refresh:
            self.sync_known_sources()
        registry = self._load_registry()
        assets = list(registry.get("assets", {}).values())
        if not assets:
            self.sync_known_sources()
            registry = self._load_registry()
            assets = list(registry.get("assets", {}).values())

        assets_sorted = sorted(
            assets,
            key=lambda item: item.get("updated_at") or item.get("created_at") or "",
            reverse=True,
        )
        domain_counts: dict[str, int] = {}
        kind_counts: dict[str, int] = {}
        ready_for_ingest = 0
        graph_ready = 0
        needs_review = 0
        for asset in assets_sorted:
            domain_key = asset.get("domain") or "unknown"
            kind_key = asset.get("kind") or "unknown"
            domain_counts[domain_key] = domain_counts.get(domain_key, 0) + 1
            kind_counts[kind_key] = kind_counts.get(kind_key, 0) + 1
            if (asset.get("ingest") or {}).get("supported"):
                ready_for_ingest += 1
            if (asset.get("ingest") or {}).get("latest_status") == "completed":
                graph_ready += 1
            if (asset.get("review") or {}).get("needs_review"):
                needs_review += 1

        return {
            "schema_version": registry.get("schema_version"),
            "mission": registry.get("mission"),
            "generated_at": registry.get("generated_at"),
            "last_sync_at": registry.get("last_sync_at"),
            "managed_roots": self._managed_roots_manifest(),
            "summary": {
                "asset_count": len(assets_sorted),
                "domain_count": len(domain_counts),
                "kind_count": len(kind_counts),
                "ready_for_ingest_count": ready_for_ingest,
                "graph_ready_count": graph_ready,
                "needs_review_count": needs_review,
            },
            "counts": {
                "domains": domain_counts,
                "kinds": kind_counts,
            },
            "recent_assets": [self._public_asset(item) for item in assets_sorted[:20]],
            "last_sync_summary": registry.get("last_sync_summary") or {},
        }

    def list_assets(
        self,
        domain: Optional[str] = None,
        kind: Optional[str] = None,
        query: Optional[str] = None,
        limit: int = 200,
    ) -> list[dict[str, Any]]:
        registry = self._load_registry()
        assets = list(registry.get("assets", {}).values())
        query_folded = self._ascii_fold(query or "")
        filtered: list[dict[str, Any]] = []
        for asset in assets:
            if domain and asset.get("domain") != domain:
                continue
            if kind and asset.get("kind") != kind:
                continue
            if query_folded:
                haystack = " ".join(
                    [
                        asset.get("title") or "",
                        asset.get("source_path") or "",
                        asset.get("kind") or "",
                        asset.get("domain") or "",
                        " ".join(asset.get("tags") or []),
                    ]
                )
                if query_folded not in self._ascii_fold(haystack):
                    continue
            filtered.append(asset)

        filtered.sort(key=lambda item: item.get("updated_at") or item.get("created_at") or "", reverse=True)
        return [self._public_asset(item) for item in filtered[:limit]]

    def get_asset(self, asset_id: str) -> Optional[dict[str, Any]]:
        registry = self._load_registry()
        asset = (registry.get("assets") or {}).get(asset_id)
        return self._public_asset(asset) if asset else None

    def _public_asset(self, asset: Optional[dict[str, Any]]) -> Optional[dict[str, Any]]:
        if asset is None:
            return None
        self._ensure_asset_scaffolds(asset)
        cloned = json.loads(json.dumps(asset, ensure_ascii=False))
        cloned["companion_paths"] = self._asset_companion_paths(asset)
        review = dict(cloned.get("review") or {})
        review.setdefault("needs_review", bool(review.get("pending_reason")))
        review.setdefault("status", "needs_review" if review.get("needs_review") else "clear")
        cloned["review"] = review
        return cloned

    def _asset_companion_paths(self, asset: dict[str, Any]) -> dict[str, str]:
        asset_dir = KNOWLEDGE_ASSET_GUIDES_DIR / str(asset.get("asset_id") or "asset")
        disease_slug = self._asset_disease_slug(asset)
        disease_dir = KNOWLEDGE_DISEASE_GUIDES_DIR / disease_slug if disease_slug else None
        domain_dir = KNOWLEDGE_DOMAIN_GUIDES_DIR / str(asset.get("domain") or "misc")
        return {
            "domain_workspace": str(domain_dir),
            "domain_workspace_relative": self._portable_path(domain_dir),
            "domain_guide": str(domain_dir / "CLAUDE.md"),
            "domain_guide_relative": self._portable_path(domain_dir / "CLAUDE.md"),
            "asset_workspace": str(asset_dir),
            "asset_workspace_relative": self._portable_path(asset_dir),
            "asset_guide": str(asset_dir / "CLAUDE.md"),
            "asset_guide_relative": self._portable_path(asset_dir / "CLAUDE.md"),
            "asset_review_dir": str(asset_dir / "review"),
            "asset_review_dir_relative": self._portable_path(asset_dir / "review"),
            "asset_impact_report": str(asset_dir / "review" / "impact_latest.json"),
            "asset_impact_report_relative": self._portable_path(asset_dir / "review" / "impact_latest.json"),
            "asset_review_queue": str(asset_dir / "review" / "human_review.md"),
            "asset_review_queue_relative": self._portable_path(asset_dir / "review" / "human_review.md"),
            "disease_workspace": str(disease_dir) if disease_dir else "",
            "disease_workspace_relative": self._portable_path(disease_dir) if disease_dir else "",
            "disease_guide": str((disease_dir / "CLAUDE.md")) if disease_dir else "",
            "disease_guide_relative": self._portable_path(disease_dir / "CLAUDE.md") if disease_dir else "",
        }

    def _asset_disease_slug(self, asset: dict[str, Any]) -> str:
        kind = str(asset.get("kind") or "")
        configured = str(((asset.get("config") or {}).get("preferred_disease_name")) or "").strip()
        if not configured and kind not in SUPPORTED_PROTOCOL_INGEST_KINDS:
            return ""
        title = str(asset.get("title") or "").strip()
        source_stem = self._path_stem_hint(asset.get("source_relative_path") or asset.get("source_path") or "")
        candidate = configured or title or source_stem
        safe = _slugify(candidate or "")
        return safe or ""

    def _ensure_asset_scaffolds(self, asset: dict[str, Any]) -> None:
        paths = self._asset_companion_paths(asset)
        domain_dir = Path(paths["domain_workspace"])
        asset_dir = Path(paths["asset_workspace"])
        disease_dir = Path(paths["disease_workspace"]) if paths.get("disease_workspace") else None
        self._ensure_workspace_scaffold(
            asset_dir,
            title=f"Asset {asset.get('title') or asset.get('asset_id')}",
            summary="Workspace hướng dẫn tìm evidence, skill và review cho một asset tri thức cụ thể.",
            extra_lines=[
                f"- Asset id: {asset.get('asset_id')}",
                f"- Kind: {asset.get('kind')}",
                f"- Source: {asset.get('source_path')}",
                f"- Domain guide: {domain_dir / 'CLAUDE.md'}",
            ],
        )
        if disease_dir:
            self._ensure_workspace_scaffold(
                disease_dir,
                title=f"Disease {self._asset_disease_slug(asset)}",
                summary="Workspace disease-centric để gom phác đồ, dấu hiệu, dịch vụ, quyền lợi và review theo từng bệnh.",
                extra_lines=[
                    f"- Bệnh/slug: {self._asset_disease_slug(asset)}",
                    f"- Asset liên quan gần nhất: {asset.get('asset_id')}",
                ],
            )

    def update_asset_config(self, asset_id: str, updates: dict[str, Any]) -> dict[str, Any]:
        allowed = {
            "preferred_disease_name",
            "preferred_namespace",
            "source_type",
            "split_strategy",
            "auto_ingest",
            "title",
            "tags",
        }
        with self._lock:
            registry = self._load_registry()
            asset = (registry.get("assets") or {}).get(asset_id)
            if asset is None:
                raise KeyError(f"Asset not found: {asset_id}")

            for key, value in updates.items():
                if key not in allowed:
                    continue
                if key == "title":
                    asset["title"] = str(value or "").strip() or asset.get("title")
                elif key == "tags":
                    asset["tags"] = sorted({str(item).strip() for item in value or [] if str(item).strip()})
                else:
                    asset.setdefault("config", {})[key] = value
            asset["updated_at"] = self._now_iso()
            self._save_registry(registry)
            self._ensure_asset_scaffolds(asset)
            return self._public_asset(asset)

    def get_text_workspace(
        self,
        asset_id: str,
        *,
        create_if_missing: bool = True,
        force_refresh: bool = False,
    ) -> dict[str, Any]:
        asset = self.get_asset(asset_id)
        if asset is None:
            raise KeyError(f"Asset not found: {asset_id}")
        kind = asset.get("kind") or "knowledge_file"
        if kind not in SUPPORTED_PROTOCOL_INGEST_KINDS:
            raise ValueError(f"Asset kind {kind} does not support text workspace")

        if create_if_missing:
            self._prepare_text_workspace(asset_id, force_refresh=force_refresh)
            asset = self.get_asset(asset_id) or asset

        workspace_path = self._text_workspace_path(asset)
        existing_workspace_path = self._existing_text_workspace_path(asset)
        metadata = dict(asset.get("text_workspace") or {})
        content = ""
        if existing_workspace_path and existing_workspace_path.exists():
            content = existing_workspace_path.read_text(encoding="utf-8")
        source_path = str(asset.get("source_path") or "")
        resolved_source_path = str(self._resolve_asset_file_path(asset))
        return {
            "asset_id": asset_id,
            "kind": kind,
            "source_path": source_path,
            "resolved_source_path": resolved_source_path,
            "workspace_path": str(workspace_path),
            "workspace_relative_path": self._relative_to_root(workspace_path, BASE_DIR),
            "exists": bool(existing_workspace_path and existing_workspace_path.exists()),
            "content": content,
            "content_length": len(content),
            "metadata": metadata,
            "can_edit": True,
        }

    def list_manual_labels(self, asset_id: str) -> list[dict[str, Any]]:
        asset = self.get_asset(asset_id)
        if asset is None:
            raise KeyError(f"Asset not found: {asset_id}")
        labels = list(asset.get("manual_labels") or [])
        labels.sort(key=lambda item: (str(item.get("kind") or ""), str(item.get("text") or ""), str(item.get("label_id") or "")))
        return labels

    def upsert_manual_label(self, asset_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        with self._lock:
            registry = self._load_registry()
            asset = (registry.get("assets") or {}).get(asset_id)
            if asset is None:
                raise KeyError(f"Asset not found: {asset_id}")
            existing_labels = list(asset.get("manual_labels") or [])
            label_id = str((payload or {}).get("label_id") or "").strip()
            existing = next((item for item in existing_labels if str(item.get("label_id") or "") == label_id), None) if label_id else None
            normalized = self._normalize_manual_label(asset_id, payload or {}, existing=existing)
            replaced = False
            next_labels: list[dict[str, Any]] = []
            for item in existing_labels:
                if str(item.get("label_id") or "") == normalized["label_id"]:
                    next_labels.append(normalized)
                    replaced = True
                else:
                    next_labels.append(item)
            if not replaced:
                next_labels.append(normalized)
            asset["manual_labels"] = next_labels
            asset["updated_at"] = self._now_iso()
            self._save_registry(registry)
            return normalized

    def replace_manual_labels(self, asset_id: str, labels: list[dict[str, Any]]) -> list[dict[str, Any]]:
        with self._lock:
            registry = self._load_registry()
            asset = (registry.get("assets") or {}).get(asset_id)
            if asset is None:
                raise KeyError(f"Asset not found: {asset_id}")
            existing_by_id = {
                str(item.get("label_id") or ""): item
                for item in (asset.get("manual_labels") or [])
                if str(item.get("label_id") or "").strip()
            }
            normalized_labels = [
                self._normalize_manual_label(
                    asset_id,
                    payload or {},
                    existing=existing_by_id.get(str((payload or {}).get("label_id") or "").strip()),
                )
                for payload in labels or []
                if str((payload or {}).get("text") or "").strip()
            ]
            asset["manual_labels"] = normalized_labels
            asset["updated_at"] = self._now_iso()
            self._save_registry(registry)
            return normalized_labels

    def delete_manual_label(self, asset_id: str, label_id: str) -> bool:
        with self._lock:
            registry = self._load_registry()
            asset = (registry.get("assets") or {}).get(asset_id)
            if asset is None:
                raise KeyError(f"Asset not found: {asset_id}")
            current = list(asset.get("manual_labels") or [])
            next_labels = [item for item in current if str(item.get("label_id") or "") != str(label_id or "")]
            changed = len(next_labels) != len(current)
            if changed:
                asset["manual_labels"] = next_labels
                asset["updated_at"] = self._now_iso()
                self._save_registry(registry)
            return changed

    def refresh_text_workspace_from_source(self, asset_id: str) -> dict[str, Any]:
        return self.get_text_workspace(asset_id, create_if_missing=True, force_refresh=True)

    def save_text_workspace(self, asset_id: str, content: str, note: str = "") -> dict[str, Any]:
        with self._lock:
            registry = self._load_registry()
            asset = (registry.get("assets") or {}).get(asset_id)
            if asset is None:
                raise KeyError(f"Asset not found: {asset_id}")
            kind = asset.get("kind") or "knowledge_file"
            if kind not in SUPPORTED_PROTOCOL_INGEST_KINDS:
                raise ValueError(f"Asset kind {kind} does not support text workspace")

            workspace_path = self._text_workspace_path(asset)
            workspace_path.parent.mkdir(parents=True, exist_ok=True)
            metadata = asset.setdefault("text_workspace", {})
            if not metadata.get("source_sha1"):
                try:
                    source_text = self._extract_source_text_for_asset(asset, self._resolve_asset_file_path(asset))
                    metadata["source_sha1"] = self._text_sha1(source_text)
                except Exception:
                    metadata["source_sha1"] = ""
            workspace_path.write_text(content or "", encoding="utf-8")
            current_sha1 = self._text_sha1(content or "")
            metadata.update(
                {
                    "workspace_path": str(workspace_path),
                    "current_sha1": current_sha1,
                    "content_length": len(content or ""),
                    "dirty": bool(metadata.get("source_sha1")) and current_sha1 != metadata.get("source_sha1"),
                    "last_saved_at": self._now_iso(),
                    "last_note": note or "",
                    "source_mode": metadata.get("source_mode") or "editable_text",
                }
            )
            asset["updated_at"] = self._now_iso()
            self._save_registry(registry)

        return self.get_text_workspace(asset_id, create_if_missing=False)

    def reserve_managed_path(self, filename: str, kind: str) -> Path:
        directory = self._managed_dir_for_kind(kind)
        directory.mkdir(parents=True, exist_ok=True)
        original = Path(filename or "asset.bin")
        stem = _slugify(original.stem or "asset") or "asset"
        suffix = original.suffix or self._default_suffix_for_kind(kind)
        candidate = directory / f"{stem}{suffix}"
        if not candidate.exists():
            return candidate
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        counter = 1
        while True:
            candidate = directory / f"{stem}_{timestamp}_{counter}{suffix}"
            if not candidate.exists():
                return candidate
            counter += 1

    def register_managed_file(
        self,
        file_path: Path,
        *,
        kind: str,
        domain: Optional[str] = None,
        title: Optional[str] = None,
        source_type: Optional[str] = None,
    ) -> dict[str, Any]:
        file_path = file_path.resolve()
        directory_spec = {
            "key": self._managed_root_key_for_kind(kind),
            "path": file_path.parent,
            "domain": domain or self._default_domain_for_kind(kind),
            "default_kind": kind,
            "managed": True,
        }
        with self._lock:
            registry = self._load_registry()
            assets = registry.setdefault("assets", {})
            asset_id = self._asset_id(directory_spec["key"], file_path, file_path.parent)
            domain_resolved, kind_resolved, tags = self._infer_asset_profile(file_path, directory_spec)
            version_payload = self._build_version_payload(file_path, directory_spec, file_path.parent)
            asset = assets.get(asset_id) or {
                "asset_id": asset_id,
                "created_at": self._now_iso(),
                "versions": [],
                "ingest": self._default_ingest_state(kind_resolved),
            }
            asset.update(
                {
                    "title": title or file_path.stem,
                    "domain": domain or domain_resolved,
                    "kind": kind,
                    "tags": sorted(set((asset.get("tags") or []) + tags)),
                    "file_extension": file_path.suffix.lower(),
                    "source_root_key": directory_spec["key"],
                    "source_root_path": str(file_path.parent),
                    "source_path": str(file_path),
                    "source_relative_path": file_path.name,
                    "managed": True,
                    "managed_relative_path": self._relative_to_root(file_path, BASE_DIR),
                    "status": "managed",
                    "config": asset.get("config") or self._default_asset_config(kind, source_type, True),
                }
            )
            if source_type:
                asset["config"]["source_type"] = source_type
            if not asset["versions"] or asset["versions"][-1].get("sha1") != version_payload["sha1"]:
                asset["versions"].append(version_payload)
                self._mark_asset_for_review(
                    asset,
                    reason=f"Managed asset cập nhật version: {file_path.name}",
                )
            asset["current_version_id"] = asset["versions"][-1]["version_id"]
            asset["version_count"] = len(asset["versions"])
            asset["updated_at"] = self._now_iso()
            assets[asset_id] = asset
            self._save_registry(registry)
            self._ensure_asset_scaffolds(asset)
            return self._public_asset(asset)

    def mark_ingest_started(self, asset_id: str, run_id: str) -> dict[str, Any]:
        with self._lock:
            registry = self._load_registry()
            asset = (registry.get("assets") or {}).get(asset_id)
            if asset is None:
                raise KeyError(f"Asset not found: {asset_id}")
            ingest_state = asset.setdefault("ingest", self._default_ingest_state(asset.get("kind") or "knowledge_file"))
            ingest_state["latest_run_id"] = run_id
            ingest_state["latest_status"] = "running"
            ingest_state["last_ingest_started_at"] = self._now_iso()
            ingest_state["last_error"] = None
            asset["status"] = "ingesting"
            asset["updated_at"] = self._now_iso()
            self._save_registry(registry)
            return asset

    def record_ingest_result(self, asset_id: str, run_id: str, result: dict[str, Any]) -> dict[str, Any]:
        trace = self.graph_trace(asset_id)
        with self._lock:
            registry = self._load_registry()
            asset = (registry.get("assets") or {}).get(asset_id)
            if asset is None:
                raise KeyError(f"Asset not found: {asset_id}")

            ingest_state = asset.setdefault("ingest", self._default_ingest_state(asset.get("kind") or "knowledge_file"))
            ingest_state["latest_run_id"] = run_id
            ingest_state["latest_status"] = result.get("status", "completed")
            ingest_state["last_ingested_at"] = self._now_iso()
            ingest_state["last_error"] = None
            ingest_state["latest_result"] = {
                "mode": result.get("mode"),
                "namespace": result.get("namespace"),
                "disease_count": len(result.get("diseases") or []),
                "diseases": result.get("diseases") or [],
                "document_count": result.get("document_count"),
                "service_count": result.get("service_count"),
                "canonical_count": result.get("canonical_count"),
                "row_count": result.get("row_count"),
            }
            asset["status"] = "graph_ready" if result.get("status") == "completed" else result.get("status", "completed")
            asset["updated_at"] = self._now_iso()
            asset["graph_trace_cache"] = trace
            self._save_registry(registry)

        manifest = {
            "asset_id": asset_id,
            "run_id": run_id,
            "recorded_at": self._now_iso(),
            "result": result,
            "graph_trace": trace,
        }
        manifest_path = KNOWLEDGE_MANIFESTS_DIR / f"{run_id}.json"
        manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
        try:
            manifest["impact_report"] = self.impact_report(asset_id)
            manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
        except Exception:
            # Keep ingest success durable even if impact review generation fails.
            pass
        return manifest

    def record_ingest_error(self, asset_id: str, run_id: str, error: str) -> dict[str, Any]:
        with self._lock:
            registry = self._load_registry()
            asset = (registry.get("assets") or {}).get(asset_id)
            if asset is None:
                raise KeyError(f"Asset not found: {asset_id}")
            ingest_state = asset.setdefault("ingest", self._default_ingest_state(asset.get("kind") or "knowledge_file"))
            ingest_state["latest_run_id"] = run_id
            ingest_state["latest_status"] = "error"
            ingest_state["last_error"] = error
            ingest_state["last_ingested_at"] = self._now_iso()
            asset["status"] = "error"
            asset["updated_at"] = self._now_iso()
            self._save_registry(registry)
            return asset

    def graph_trace(self, asset_id: str) -> dict[str, Any]:
        asset = self.get_asset(asset_id)
        if asset is None:
            raise KeyError(f"Asset not found: {asset_id}")
        try:
            driver = self._driver()
        except Exception as exc:
            return {
                "asset_id": asset_id,
                "status": "neo4j_unavailable",
                "error": str(exc),
                "ontology_documents": [],
                "insurance_sources": [],
                "taxonomy_assets": [],
            }

        trace_terms = self._trace_terms_for_asset(asset)
        source_path = Path(asset.get("source_path") or "")
        source_path_norm = str(source_path).replace("\\", "/").lower()
        source_name = source_path.name.lower()
        source_stem = source_path.stem.lower()
        with driver.session() as session:
            ontology_rows = [
                dict(record)
                for record in session.run(
                    """
                    MATCH (doc:RawDocument)
                    WITH doc, toLower(replace(coalesce(doc.file_path, ''), '\\\\', '/')) AS doc_path
                    WHERE (size($source_path) > 0 AND doc_path = $source_path)
                       OR (size($source_name) > 0 AND doc_path ENDS WITH $source_name)
                       OR (size($source_stem) > 0 AND doc_path CONTAINS $source_stem)
                       OR any(term IN $terms WHERE doc_path CONTAINS term)
                    OPTIONAL MATCH (doc)<-[:FROM_DOCUMENT]-(chunk:RawChunk)
                    OPTIONAL MATCH (chunk)-[:CHUNK_ABOUT_DISEASE]->(d:DiseaseEntity)
                    OPTIONAL MATCH (chunk)-[:CONTAINS_ASSERTION]->(a:ProtocolAssertion)
                    RETURN doc.doc_id AS doc_id,
                           doc.title AS doc_title,
                           doc.file_path AS file_path,
                           doc.source_type AS source_type,
                           doc.doc_type AS doc_type,
                           count(DISTINCT chunk) AS chunk_count,
                           count(DISTINCT a) AS assertion_count,
                           collect(DISTINCT d.disease_name) AS disease_names
                    ORDER BY chunk_count DESC, doc_title
                    """,
                    source_path=source_path_norm,
                    source_name=source_name,
                    source_stem=source_stem,
                    terms=trace_terms,
                )
            ]
            insurance_rows = [
                dict(record)
                for record in session.run(
                    """
                    MATCH (n)
                    WHERE (n:ContractClause OR n:BenefitDetailEvidence OR n:Rulebook OR n:Exclusion)
                    WITH n, toLower(replace(coalesce(n.source_file, ''), '\\\\', '/')) AS source_path
                    WHERE (size($source_path) > 0 AND source_path = $source_path)
                       OR (size($source_name) > 0 AND source_path ENDS WITH $source_name)
                       OR (size($source_stem) > 0 AND source_path CONTAINS $source_stem)
                       OR any(term IN $terms WHERE source_path CONTAINS term)
                    OPTIONAL MATCH (c:InsuranceContract)-[]-(n)
                    RETURN labels(n) AS labels,
                           coalesce(n.source_file, '') AS source_file,
                           count(DISTINCT n) AS node_count,
                           collect(DISTINCT c.contract_id) AS contract_ids
                    ORDER BY node_count DESC
                    """,
                    source_path=source_path_norm,
                    source_name=source_name,
                    source_stem=source_stem,
                    terms=trace_terms,
                )
            ]
            taxonomy_rows = [
                dict(record)
                for record in session.run(
                    """
                    MATCH (asset:HospitalServiceAsset {asset_id:$asset_id})
                    OPTIONAL MATCH (asset)-[:HAS_SERVICE_ROW]->(row:HospitalServiceRow)
                    OPTIONAL MATCH (row)-[:MAPS_TO_CI_SERVICE]->(svc:CIService)
                    OPTIONAL MATCH (row)-[:MAPS_TO_CANONICAL]->(cs:CanonicalService)
                    RETURN asset.asset_id AS asset_id,
                           asset.title AS title,
                           asset.source_file AS source_file,
                           count(DISTINCT row) AS row_count,
                           count(DISTINCT svc) AS ci_service_count,
                           count(DISTINCT cs) AS canonical_service_count,
                           collect(DISTINCT row.sheet_name)[0..12] AS sheet_names
                    """,
                    asset_id=asset_id,
                )
            ]

        return {
            "asset_id": asset_id,
            "status": "ok",
            "summary": {
                "ontology_document_count": len(ontology_rows),
                "insurance_source_count": len(insurance_rows),
                "taxonomy_asset_count": len([row for row in taxonomy_rows if row.get("asset_id")]),
                "ontology_disease_count": len(
                    {
                        disease
                        for row in ontology_rows
                        for disease in (row.get("disease_names") or [])
                        if disease
                    }
                ),
            },
            "ontology_documents": ontology_rows,
            "insurance_sources": insurance_rows,
            "taxonomy_assets": taxonomy_rows,
        }

    def impact_report(self, asset_id: str) -> dict[str, Any]:
        asset = self.get_asset(asset_id)
        if asset is None:
            raise KeyError(f"Asset not found: {asset_id}")
        trace = self.graph_trace(asset_id)
        source_params = self._source_match_params(asset)
        report = {
            "asset_id": asset_id,
            "generated_at": self._now_iso(),
            "asset": {
                "title": asset.get("title"),
                "kind": asset.get("kind"),
                "domain": asset.get("domain"),
                "source_path": asset.get("source_path"),
                "current_version_id": asset.get("current_version_id"),
                "version_count": asset.get("version_count"),
            },
            "graph_trace": trace,
            "impacts": {
                "ontology": {
                    "documents": trace.get("ontology_documents") or [],
                    "diseases": [],
                    "services": [],
                    "signs": [],
                },
                "taxonomy": {
                    "assets": trace.get("taxonomy_assets") or [],
                    "rows": [],
                    "ci_services": [],
                    "canonical_services": [],
                },
                "insurance": {
                    "contracts": [],
                    "benefits": [],
                    "exclusions": [],
                    "rulebooks": [],
                    "services": [],
                },
            },
            "review": {
                "needs_human_review": False,
                "severity": "low",
                "reasons": [],
                "review_items": [],
            },
        }
        try:
            driver = self._driver()
        except Exception as exc:
            report["review"]["needs_human_review"] = True
            report["review"]["severity"] = "medium"
            report["review"]["reasons"].append(f"Neo4j unavailable: {exc}")
            return self._record_impact_report(asset_id, report)

        doc_ids = [row.get("doc_id") for row in trace.get("ontology_documents") or [] if row.get("doc_id")]
        with driver.session() as session:
            if doc_ids:
                report["impacts"]["ontology"]["diseases"] = [
                    dict(row)
                    for row in session.run(
                        """
                        MATCH (doc:RawDocument)
                        WHERE doc.doc_id IN $doc_ids
                        MATCH (doc)<-[:FROM_DOCUMENT]-(chunk:RawChunk)-[:CHUNK_ABOUT_DISEASE]->(d:DiseaseEntity)
                        RETURN DISTINCT d.disease_id AS disease_id,
                               d.disease_name AS disease_name
                        ORDER BY d.disease_name
                        """,
                        doc_ids=doc_ids,
                    )
                ]
                report["impacts"]["ontology"]["services"] = [
                    dict(row)
                    for row in session.run(
                        """
                        MATCH (doc:RawDocument)
                        WHERE doc.doc_id IN $doc_ids
                        MATCH (doc)<-[:FROM_DOCUMENT]-(chunk:RawChunk)-[:MENTIONS_SERVICE]->(m:RawServiceMention)
                        OPTIONAL MATCH (m)-[:MAPS_TO_SERVICE]->(svc)
                        RETURN DISTINCT coalesce(svc.service_code, m.mention_id) AS service_ref,
                               coalesce(svc.service_name, svc.name, m.mention_text) AS service_name,
                               count(DISTINCT m) AS mention_count
                        ORDER BY mention_count DESC, service_name
                        LIMIT 120
                        """,
                        doc_ids=doc_ids,
                    )
                ]
                report["impacts"]["ontology"]["signs"] = [
                    dict(row)
                    for row in session.run(
                        """
                        MATCH (doc:RawDocument)
                        WHERE doc.doc_id IN $doc_ids
                        MATCH (doc)<-[:FROM_DOCUMENT]-(chunk:RawChunk)-[:MENTIONS_SIGN]->(m:RawSignMention)
                        OPTIONAL MATCH (m)-[:MAPS_TO_SIGN]->(s)
                        RETURN DISTINCT coalesce(s.sign_id, s.claim_sign_id, s.canonical_label, m.mention_id) AS sign_ref,
                               coalesce(s.canonical_label, s.sign_id, s.claim_sign_id, m.mention_text) AS sign_label,
                               count(DISTINCT m) AS mention_count
                        ORDER BY mention_count DESC, sign_label
                        LIMIT 120
                        """,
                        doc_ids=doc_ids,
                    )
                ]

            if asset.get("kind") == "service_table":
                report["impacts"]["taxonomy"]["rows"] = [
                    dict(row)
                    for row in session.run(
                        """
                        MATCH (row:HospitalServiceRow {asset_id:$asset_id})
                        RETURN row.sheet_name AS sheet_name,
                               row.row_number AS row_number,
                               row.service_name_raw AS service_name_raw,
                               row.hospital_service_code AS hospital_service_code,
                               row.mapped_service_code AS mapped_service_code,
                               row.canonical_maanhxa AS canonical_maanhxa,
                               row.review_status AS review_status
                        ORDER BY row.sheet_name, row.row_number
                        LIMIT 200
                        """,
                        asset_id=asset_id,
                    )
                ]
                report["impacts"]["taxonomy"]["ci_services"] = [
                    dict(row)
                    for row in session.run(
                        """
                        MATCH (asset:HospitalServiceAsset {asset_id:$asset_id})-[:HAS_SERVICE_ROW]->(:HospitalServiceRow)-[:MAPS_TO_CI_SERVICE]->(svc:CIService)
                        RETURN svc.service_code AS service_code,
                               coalesce(svc.service_name, svc.service_code) AS service_name,
                               count(*) AS support_rows
                        ORDER BY support_rows DESC, service_name
                        LIMIT 120
                        """,
                        asset_id=asset_id,
                    )
                ]
                report["impacts"]["taxonomy"]["canonical_services"] = [
                    dict(row)
                    for row in session.run(
                        """
                        MATCH (asset:HospitalServiceAsset {asset_id:$asset_id})-[:HAS_SERVICE_ROW]->(:HospitalServiceRow)-[:MAPS_TO_CANONICAL]->(cs:CanonicalService)
                        OPTIONAL MATCH (cs)-[:CLASSIFIED_AS]->(cls:ServiceClassification)
                        RETURN cs.maanhxa AS maanhxa,
                               cs.canonical_name_primary AS canonical_name_primary,
                               collect(DISTINCT cls.name)[0] AS classification_name,
                               count(*) AS support_rows
                        ORDER BY support_rows DESC, canonical_name_primary
                        LIMIT 120
                        """,
                        asset_id=asset_id,
                    )
                ]

            report["impacts"]["insurance"]["benefits"] = [
                dict(row)
                for row in session.run(
                    """
                    MATCH (b:Benefit)
                    WITH b, toLower(replace(coalesce(b.source_file, ''), '\\\\', '/')) AS source_path
                    WHERE (size($source_path) > 0 AND source_path = $source_path)
                       OR (size($source_name) > 0 AND source_path ENDS WITH $source_name)
                       OR (size($source_stem) > 0 AND source_path CONTAINS $source_stem)
                       OR any(term IN $terms WHERE source_path CONTAINS term)
                    OPTIONAL MATCH (c:InsuranceContract)-[:HAS_BENEFIT]->(b)
                    OPTIONAL MATCH (svc:CIService)-[:SUPPORTED_BY_CONTRACT_BENEFIT]->(b)
                    WITH b, c, svc, properties(b) AS props
                    RETURN b.entry_id AS entry_id,
                           coalesce(props['canonical_name'], props['label'], props['label_text'], props['raw_label'], props['entry_id'], '') AS benefit_label,
                           [item IN collect(DISTINCT c.contract_id) WHERE item IS NOT NULL] AS contract_ids,
                           [item IN collect(DISTINCT svc.service_code) WHERE item IS NOT NULL] AS service_codes
                    ORDER BY size(contract_ids) DESC, benefit_label
                    LIMIT 120
                    """,
                    **source_params,
                )
            ]
            report["impacts"]["insurance"]["exclusions"] = [
                dict(row)
                for row in session.run(
                    """
                    MATCH (e:Exclusion)
                    WITH e, toLower(replace(coalesce(e.source_file, ''), '\\\\', '/')) AS source_path
                    WHERE (size($source_path) > 0 AND source_path = $source_path)
                       OR (size($source_name) > 0 AND source_path ENDS WITH $source_name)
                       OR (size($source_stem) > 0 AND source_path CONTAINS $source_stem)
                       OR any(term IN $terms WHERE source_path CONTAINS term)
                    OPTIONAL MATCH (c:InsuranceContract)-[:HAS_EXCLUSION]->(e)
                    OPTIONAL MATCH (svc:CIService)-[:EXCLUDED_BY_CONTRACT|EXCLUDED_BY]->(e)
                    WITH e, c, svc, properties(e) AS props
                    RETURN e.code AS exclusion_code,
                           coalesce(props['reason'], props['group_name'], props['label'], props['code']) AS exclusion_label,
                           [item IN collect(DISTINCT c.contract_id) WHERE item IS NOT NULL] AS contract_ids,
                           [item IN collect(DISTINCT svc.service_code) WHERE item IS NOT NULL] AS service_codes
                    ORDER BY size(contract_ids) DESC, exclusion_label
                    LIMIT 120
                    """,
                    **source_params,
                )
            ]
            report["impacts"]["insurance"]["rulebooks"] = [
                dict(row)
                for row in session.run(
                    """
                    MATCH (r:Rulebook)
                    WITH r, toLower(replace(coalesce(r.source_file, ''), '\\\\', '/')) AS source_path
                    WHERE (size($source_path) > 0 AND source_path = $source_path)
                       OR (size($source_name) > 0 AND source_path ENDS WITH $source_name)
                       OR (size($source_stem) > 0 AND source_path CONTAINS $source_stem)
                       OR any(term IN $terms WHERE source_path CONTAINS term)
                    OPTIONAL MATCH (c:InsuranceContract)-[:HAS_RULEBOOK]->(r)
                    OPTIONAL MATCH (r)-[:HAS_CHAPTER]->(ch:RulebookChapter)
                    OPTIONAL MATCH (ch)-[:HAS_CLAUSE]->(cl:RulebookClause)
                    RETURN r.rulebook_id AS rulebook_id,
                           coalesce(r.display_name, r.rulebook_id) AS rulebook_name,
                           [item IN collect(DISTINCT c.contract_id) WHERE item IS NOT NULL] AS contract_ids,
                           count(DISTINCT ch) AS chapter_count,
                           count(DISTINCT cl) AS clause_count
                    ORDER BY size(contract_ids) DESC, rulebook_name
                    LIMIT 80
                    """,
                    **source_params,
                )
            ]
            report["impacts"]["insurance"]["contracts"] = [
                dict(row)
                for row in session.run(
                    """
                    MATCH (c:InsuranceContract)
                    OPTIONAL MATCH (c)-[:HAS_BENEFIT]->(b:Benefit)
                    WITH c, collect(DISTINCT toLower(replace(coalesce(b.source_file, ''), '\\\\', '/'))) AS benefit_sources
                    OPTIONAL MATCH (c)-[:HAS_EXCLUSION]->(e:Exclusion)
                    WITH c, benefit_sources, collect(DISTINCT toLower(replace(coalesce(e.source_file, ''), '\\\\', '/'))) AS exclusion_sources
                    OPTIONAL MATCH (c)-[:HAS_RULEBOOK]->(r:Rulebook)
                    WITH c, benefit_sources, exclusion_sources, collect(DISTINCT toLower(replace(coalesce(r.source_file, ''), '\\\\', '/'))) AS rulebook_sources
                    WITH c, benefit_sources + exclusion_sources + rulebook_sources AS source_paths
                    WHERE any(source_path IN source_paths WHERE
                        (size($source_path) > 0 AND source_path = $source_path)
                        OR (size($source_name) > 0 AND source_path ENDS WITH $source_name)
                        OR (size($source_stem) > 0 AND source_path CONTAINS $source_stem)
                        OR any(term IN $terms WHERE source_path CONTAINS term)
                    )
                    RETURN c.contract_id AS contract_id,
                           coalesce(c.product_name, c.contract_id) AS contract_label
                    ORDER BY contract_label
                    LIMIT 120
                    """,
                    **source_params,
                )
            ]
            report["impacts"]["insurance"]["services"] = [
                dict(row)
                for row in session.run(
                    """
                    MATCH (svc:CIService)-[rel]->(target)
                    WHERE (
                        (target:Benefit AND (
                            size($source_path) > 0 AND toLower(replace(coalesce(target.source_file, ''), '\\\\', '/')) = $source_path
                            OR size($source_name) > 0 AND toLower(replace(coalesce(target.source_file, ''), '\\\\', '/')) ENDS WITH $source_name
                            OR size($source_stem) > 0 AND toLower(replace(coalesce(target.source_file, ''), '\\\\', '/')) CONTAINS $source_stem
                            OR any(term IN $terms WHERE toLower(replace(coalesce(target.source_file, ''), '\\\\', '/')) CONTAINS term)
                        ))
                        OR
                        (target:Exclusion AND (
                            size($source_path) > 0 AND toLower(replace(coalesce(target.source_file, ''), '\\\\', '/')) = $source_path
                            OR size($source_name) > 0 AND toLower(replace(coalesce(target.source_file, ''), '\\\\', '/')) ENDS WITH $source_name
                            OR size($source_stem) > 0 AND toLower(replace(coalesce(target.source_file, ''), '\\\\', '/')) CONTAINS $source_stem
                            OR any(term IN $terms WHERE toLower(replace(coalesce(target.source_file, ''), '\\\\', '/')) CONTAINS term)
                        ))
                    )
                    RETURN svc.service_code AS service_code,
                           coalesce(svc.service_name, svc.service_code) AS service_name,
                           collect(DISTINCT type(rel)) AS relation_types,
                           count(DISTINCT target) AS touched_targets
                    ORDER BY touched_targets DESC, service_name
                    LIMIT 160
                    """,
                    **source_params,
                )
            ]

        self._apply_review_heuristics(asset, report)
        return self._record_impact_report(asset_id, report)

    def _apply_review_heuristics(self, asset: dict[str, Any], report: dict[str, Any]) -> None:
        review = report["review"]
        impacts = report["impacts"]
        contract_count = len(impacts["insurance"]["contracts"])
        benefit_count = len(impacts["insurance"]["benefits"])
        exclusion_count = len(impacts["insurance"]["exclusions"])
        rulebook_count = len(impacts["insurance"]["rulebooks"])
        service_count = len(impacts["insurance"]["services"])
        ontology_doc_count = len(impacts["ontology"]["documents"])
        kind = str(asset.get("kind") or "")

        pending_reason = ((asset.get("review") or {}).get("pending_reason")) or ""
        if pending_reason:
            review["reasons"].append(pending_reason)
            review["needs_human_review"] = True

        if kind in {"insurance_rulebook", "benefit_table", "legal_document"}:
            if any([contract_count, benefit_count, exclusion_count, rulebook_count, service_count]):
                review["needs_human_review"] = True
                review["reasons"].append("Asset bảo hiểm/quy tắc đã chạm vào graph hợp đồng/quyền lợi.")
                review["review_items"].append(
                    "Xem lại các contract, benefit, exclusion và service bị ảnh hưởng trước khi cho adjudication chạy production."
                )
            else:
                review["needs_human_review"] = True
                review["reasons"].append("Asset bảo hiểm/quy tắc chưa thấy dấu vết trong graph hoặc mapping source_file còn thiếu.")
                review["review_items"].append(
                    "Cần kiểm tra ingest/mapping vì file thay đổi nhưng graph chưa phản ánh được impacted contracts/services."
                )

        if kind in {"service_table", "symptom_table"}:
            review["needs_human_review"] = True
            review["reasons"].append("Bảng taxonomy thay đổi có thể ảnh hưởng canonical mapping toàn hệ thống.")
            review["review_items"].append(
                "Review lại bridge canonical, entity mapping và các manual labels đang phụ thuộc vào taxonomy này."
            )

        if kind in {"protocol_pdf", "protocol_text", "protocol_markdown"} and ontology_doc_count:
            review["review_items"].append(
                "Nếu phác đồ đổi, hãy kiểm tra lại các disease/service/sign quan trọng và các testcase benchmark liên quan."
            )

        severity = "low"
        if kind == "legal_document" or exclusion_count > 0 or contract_count >= 5:
            severity = "high"
        elif review["needs_human_review"] or service_count >= 10 or benefit_count >= 10:
            severity = "medium"
        review["severity"] = severity

    def _record_impact_report(self, asset_id: str, report: dict[str, Any]) -> dict[str, Any]:
        asset = self.get_asset(asset_id)
        companion_paths = (asset or {}).get("companion_paths") or {}
        impact_path = Path(companion_paths.get("asset_impact_report") or (KNOWLEDGE_ASSET_GUIDES_DIR / asset_id / "review" / "impact_latest.json"))
        impact_path.parent.mkdir(parents=True, exist_ok=True)
        impact_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        review_queue_path = Path(companion_paths.get("asset_review_queue") or (KNOWLEDGE_ASSET_GUIDES_DIR / asset_id / "review" / "human_review.md"))
        review_lines = [
            f"# Human Review - {asset_id}",
            "",
            f"- Generated at: {report.get('generated_at')}",
            f"- Needs review: {report['review'].get('needs_human_review')}",
            f"- Severity: {report['review'].get('severity')}",
            "",
            "## Reasons",
        ]
        for item in report["review"].get("reasons") or ["- Không có."]:
            review_lines.append(f"- {item}")
        review_lines.extend(["", "## Review Items"])
        for item in report["review"].get("review_items") or ["- Không có."]:
            review_lines.append(f"- {item}")
        review_queue_path.write_text("\n".join(review_lines) + "\n", encoding="utf-8")

        with self._lock:
            registry = self._load_registry()
            asset_raw = (registry.get("assets") or {}).get(asset_id)
            if asset_raw is not None:
                review_state = asset_raw.setdefault("review", self._default_review_state())
                review_state["needs_review"] = bool(report["review"].get("needs_human_review"))
                review_state["status"] = "needs_review" if review_state["needs_review"] else "clear"
                review_state["pending_reason"] = (report["review"].get("reasons") or [None])[0]
                review_state["last_impact_report_at"] = report.get("generated_at")
                review_state["last_review_items"] = report["review"].get("review_items") or []
                self._save_registry(registry)
        report["stored_paths"] = {
            "impact_report_path": str(impact_path),
            "impact_report_relative_path": self._portable_path(impact_path),
            "review_queue_path": str(review_queue_path),
            "review_queue_relative_path": self._portable_path(review_queue_path),
        }
        return report

    def _source_match_params(self, asset: dict[str, Any]) -> dict[str, Any]:
        source_path = Path(asset.get("source_path") or "")
        return {
            "source_path": str(source_path).replace("\\", "/").lower(),
            "source_name": source_path.name.lower(),
            "source_stem": source_path.stem.lower(),
            "terms": self._trace_terms_for_asset(asset),
        }

    def ingest_asset(
        self,
        asset_id: str,
        *,
        namespace: str = "ontology_v2",
        split_strategy: str = "auto",
        preferred_disease_name: Optional[str] = None,
        source_type: Optional[str] = None,
    ) -> dict[str, Any]:
        asset = self.get_asset(asset_id)
        if asset is None:
            raise KeyError(f"Asset not found: {asset_id}")

        file_path = self._resolve_asset_file_path(asset)
        if not file_path.exists():
            raise FileNotFoundError(f"Asset file not found: {file_path}")

        kind = asset.get("kind") or "knowledge_file"
        if kind not in SUPPORTED_DIRECT_INGEST_KINDS:
            raise ValueError(f"Asset kind {kind} is not yet supported for direct ingest")

        configured = asset.get("config") or {}
        effective_namespace = namespace or configured.get("preferred_namespace") or "ontology_v2"
        if kind == "service_table" and effective_namespace == "ontology_v2":
            effective_namespace = configured.get("preferred_namespace") or SERVICE_TABLE_DEFAULT_NAMESPACE
        effective_source_type = source_type or configured.get("source_type") or self._infer_source_type(file_path)
        effective_disease_name = preferred_disease_name or configured.get("preferred_disease_name") or ""
        effective_split_strategy = split_strategy or configured.get("split_strategy") or "auto"

        if kind == "service_table":
            result = self.service_table_ingestor.ingest_asset(
                asset=asset,
                file_path=file_path,
                namespace=effective_namespace,
                source_type=effective_source_type,
            )
            result["namespace"] = effective_namespace
            return result

        workspace_payload = self.get_text_workspace(asset_id, create_if_missing=False)
        workspace_text = (workspace_payload.get("content") or "").strip() if workspace_payload.get("exists") else ""

        if kind in {"protocol_text", "protocol_markdown"}:
            text = workspace_text or file_path.read_text(encoding="utf-8")
            classification = {} if effective_disease_name else self._classify_text_content(text)
            disease_name = effective_disease_name or classification.get("disease_name") or asset.get("title") or file_path.stem
            result = self._ingest_text_document(
                file_path=file_path,
                text=text,
                disease_name=disease_name,
                namespace=effective_namespace,
                source_type=effective_source_type,
            )
            return {
                "status": "completed",
                "mode": "single_text_workspace" if workspace_text else "single_text",
                "namespace": effective_namespace,
                "asset_id": asset_id,
                "document_count": 1,
                "diseases": [result.get("disease_name") or disease_name],
                "items": [result],
            }

        full_text = workspace_text or self._extract_protocol_text(file_path)
        sections = []
        if effective_split_strategy in {"auto", "multi"}:
            sections = self._split_protocol_pdf(full_text, file_path)

        if sections and effective_split_strategy != "single":
            items = []
            diseases = []
            for section in sections:
                disease_name = section["disease_name"]
                item = self._ingest_text_document(
                    file_path=file_path,
                    text=section["text_content"],
                    disease_name=disease_name,
                    namespace=effective_namespace,
                    source_type=effective_source_type,
                )
                items.append(item)
                diseases.append(disease_name)
            return {
                "status": "completed",
                "mode": "multi_protocol_pdf_text_workspace" if workspace_text else "multi_protocol_pdf",
                "namespace": effective_namespace,
                "asset_id": asset_id,
                "document_count": 1,
                "diseases": diseases,
                "items": items,
            }

        if workspace_text:
            classification = {} if effective_disease_name else self._classify_text_content(full_text)
            disease_name = effective_disease_name or classification.get("disease_name") or asset.get("title") or file_path.stem
            item = self._ingest_text_document(
                file_path=file_path,
                text=full_text,
                disease_name=disease_name,
                namespace=effective_namespace,
                source_type=effective_source_type,
            )
            return {
                "status": "completed",
                "mode": "single_protocol_pdf_text_workspace",
                "namespace": effective_namespace,
                "asset_id": asset_id,
                "document_count": 1,
                "diseases": [disease_name],
                "items": [item],
            }

        classification = self._classify_pdf(file_path)
        disease_name = effective_disease_name or classification.get("disease_name") or asset.get("title") or file_path.stem
        item = self._ingest_pdf_document(
            file_path=file_path,
            disease_name=disease_name,
            namespace=effective_namespace,
            source_type=effective_source_type,
        )
        return {
            "status": "completed",
            "mode": "single_protocol_pdf",
            "namespace": effective_namespace,
            "asset_id": asset_id,
            "document_count": 1,
            "diseases": [disease_name],
            "items": [item],
        }

    def _ingest_pdf_document(
        self,
        *,
        file_path: Path,
        disease_name: str,
        namespace: str,
        source_type: Optional[str],
    ) -> dict[str, Any]:
        pipeline = OntologyV2Ingest(namespace=namespace)
        try:
            result = pipeline.run(
                pdf_path=str(file_path),
                disease_name=disease_name,
                source_type=source_type,
            )
            result["disease_name"] = disease_name
            result["source_path"] = str(file_path)
            return result
        finally:
            pipeline.close()

    def _ingest_text_document(
        self,
        *,
        file_path: Path,
        text: str,
        disease_name: str,
        namespace: str,
        source_type: Optional[str],
    ) -> dict[str, Any]:
        pipeline = OntologyV2Ingest(namespace=namespace)
        try:
            result = pipeline.run(
                pdf_path=str(file_path),
                disease_name=disease_name,
                pre_extracted_text=text,
                source_type=source_type,
            )
            result["disease_name"] = disease_name
            result["source_path"] = str(file_path)
            return result
        finally:
            pipeline.close()

    def _extract_protocol_text(self, file_path: Path) -> str:
        ingestor = UniversalIngest()
        try:
            return ingestor.extract_text(str(file_path))
        finally:
            ingestor.close()

    def _classify_pdf(self, file_path: Path) -> dict[str, Any]:
        client, model = self._azure_chat_client()
        analyzer = DocumentAnalyzer(client, model)
        profile = analyzer.analyze(str(file_path))
        return profile.model_dump()

    def _classify_text_content(self, text: str) -> dict[str, Any]:
        client, model = self._azure_chat_client()
        analyzer = DocumentAnalyzer(client, model)
        sample = text[:6000]
        return analyzer.classify_text(sample_text=sample, total_pages=max(1, len(text) // 2200)) or {}

    def _split_protocol_pdf(self, full_text: str, file_path: Path) -> list[dict[str, Any]]:
        try:
            client, model = self._azure_chat_client()
            splitter = MultiDiseaseSplitter(client, model)
            sections = splitter.split(full_text, str(file_path))
        except Exception:
            return []

        result = []
        for section in sections or []:
            text_content = getattr(section, "text_content", "") or ""
            if not text_content.strip():
                continue
            result.append(
                {
                    "disease_name": getattr(section, "disease_name", "") or file_path.stem,
                    "icd_code": getattr(section, "icd_code", "") or "",
                    "part_name": getattr(section, "part_name", "") or "",
                    "text_content": text_content,
                    "start_page": getattr(section, "start_page", None),
                    "end_page": getattr(section, "end_page", None),
                }
            )
        return result

    def _azure_chat_client(self) -> tuple[AzureOpenAI, str]:
        client = AzureOpenAI(
            api_key=os.getenv("AZURE_OPENAI_API_KEY"),
            api_version=os.getenv("AZURE_OPENAI_API_VERSION"),
            azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT"),
        )
        model = os.getenv("MODEL1", "gpt-4o-mini")
        return client, model

    def _driver(self):
        if self.ontology_store is not None:
            return self.ontology_store.driver
        uri = os.getenv("NEO4J_URI", os.getenv("neo4j_uri", "bolt://localhost:7688"))
        user = os.getenv("NEO4J_USER", os.getenv("neo4j_user", "neo4j"))
        password = os.getenv("NEO4J_PASSWORD", os.getenv("neo4j_password", "password123"))
        return GraphDatabase.driver(uri, auth=(user, password))

    def _prepare_text_workspace(self, asset_id: str, *, force_refresh: bool = False) -> dict[str, Any]:
        with self._lock:
            registry = self._load_registry()
            asset = (registry.get("assets") or {}).get(asset_id)
            if asset is None:
                raise KeyError(f"Asset not found: {asset_id}")
            kind = asset.get("kind") or "knowledge_file"
            if kind not in SUPPORTED_PROTOCOL_INGEST_KINDS:
                raise ValueError(f"Asset kind {kind} does not support text workspace")

            file_path = self._resolve_asset_file_path(asset)
            if not file_path.exists():
                raise FileNotFoundError(f"Asset file not found: {file_path}")

            workspace_path = self._text_workspace_path(asset)
            metadata = asset.setdefault("text_workspace", {})
            existing_workspace_path = self._existing_text_workspace_path(asset)
            if existing_workspace_path and existing_workspace_path.exists() and not force_refresh:
                if existing_workspace_path != workspace_path:
                    workspace_path.parent.mkdir(parents=True, exist_ok=True)
                    workspace_path.write_text(existing_workspace_path.read_text(encoding="utf-8"), encoding="utf-8")
                metadata.update(
                    {
                        "workspace_path": str(workspace_path),
                        "content_length": len(workspace_path.read_text(encoding="utf-8")),
                    }
                )
                self._save_registry(registry)
                return asset

            source_text = self._extract_source_text_for_asset(asset, file_path)
            workspace_path.parent.mkdir(parents=True, exist_ok=True)
            workspace_path.write_text(source_text, encoding="utf-8")
            source_sha1 = self._text_sha1(source_text)
            metadata.update(
                {
                    "workspace_path": str(workspace_path),
                    "source_mode": "pdf_extract" if kind == "protocol_pdf" else "source_text_copy",
                    "source_sha1": source_sha1,
                    "current_sha1": source_sha1,
                    "content_length": len(source_text),
                    "dirty": False,
                    "last_refreshed_from_source_at": self._now_iso(),
                }
            )
            asset["updated_at"] = self._now_iso()
            self._save_registry(registry)
            return asset

    def _text_workspace_path(self, asset: dict[str, Any]) -> Path:
        source_name = self._workspace_name_stem(asset)
        safe_name = _slugify(source_name or "asset") or "asset"
        return KNOWLEDGE_TEXT_VIEWS_DIR / f"{safe_name}__{asset.get('asset_id')}.txt"

    def _existing_text_workspace_path(self, asset: dict[str, Any]) -> Optional[Path]:
        seen: set[str] = set()
        for candidate in self._text_workspace_candidate_paths(asset):
            key = str(candidate)
            if key in seen:
                continue
            seen.add(key)
            if candidate.exists():
                return candidate
        return None

    def _text_workspace_candidate_paths(self, asset: dict[str, Any]) -> list[Path]:
        canonical = self._text_workspace_path(asset)
        candidates = [canonical]
        metadata_path = str((asset.get("text_workspace") or {}).get("workspace_path") or "").strip()
        if metadata_path:
            candidates.append(self._normalize_runtime_path(metadata_path))
        for raw_hint in (
            asset.get("managed_relative_path"),
            asset.get("source_relative_path"),
            asset.get("title"),
            asset.get("source_path"),
        ):
            stem = self._path_stem_hint(raw_hint)
            if not stem:
                continue
            safe_name = _slugify(stem or "asset") or "asset"
            candidates.append(KNOWLEDGE_TEXT_VIEWS_DIR / f"{safe_name}__{asset.get('asset_id')}.txt")
        return candidates

    def _workspace_name_stem(self, asset: dict[str, Any]) -> str:
        for raw_hint in (
            asset.get("managed_relative_path"),
            asset.get("source_relative_path"),
            asset.get("title"),
            asset.get("source_path"),
        ):
            stem = self._path_stem_hint(raw_hint)
            if stem:
                return stem
        return str(asset.get("asset_id") or "asset")

    def _path_stem_hint(self, raw_hint: Any) -> str:
        value = str(raw_hint or "").strip()
        if not value:
            return ""
        normalized = value.replace("\\", "/")
        posix_name = Path(normalized).stem
        windows_name = PureWindowsPath(value).stem
        for candidate in (posix_name, windows_name, value):
            candidate = str(candidate or "").strip()
            if not candidate:
                continue
            if "/" not in candidate and "\\" not in candidate and ":" not in candidate:
                return candidate
        return posix_name or windows_name or value

    def _normalize_runtime_path(self, raw_path: str) -> Path:
        value = str(raw_path or "").strip()
        if not value:
            return Path()
        path = Path(value)
        if path.exists():
            return path
        normalized = value.replace("\\", "/")
        if normalized.startswith("/app/"):
            relative = normalized.removeprefix("/app/").strip("/")
            return BASE_DIR / Path(relative)
        return path

    def _portable_path(self, path_like: Any) -> str:
        if not path_like:
            return ""
        try:
            path = Path(path_like)
        except TypeError:
            return str(path_like)
        try:
            return path.resolve().relative_to(BASE_DIR.resolve()).as_posix()
        except Exception:
            normalized = str(path).replace("\\", "/")
            if normalized.startswith("/app/"):
                return normalized.removeprefix("/app/")
            return normalized

    def _resolve_asset_file_path(self, asset: dict[str, Any]) -> Path:
        raw_source_path = Path(str(asset.get("source_path") or ""))
        candidates: list[Path] = []
        if str(raw_source_path):
            candidates.append(raw_source_path)
        managed_relative = str(asset.get("managed_relative_path") or "").strip()
        if managed_relative:
            candidates.append(BASE_DIR / Path(managed_relative))
        source_root_key = str(asset.get("source_root_key") or "").strip()
        source_relative_path = str(asset.get("source_relative_path") or "").strip()
        if source_root_key and source_relative_path:
            for spec in self._source_roots():
                if spec.get("key") == source_root_key:
                    candidates.append(Path(spec["path"]) / source_relative_path)
                    break
        for candidate in candidates:
            if candidate.exists():
                return candidate
        return candidates[0] if candidates else raw_source_path

    def _extract_source_text_for_asset(self, asset: dict[str, Any], file_path: Path) -> str:
        kind = asset.get("kind") or "knowledge_file"
        if kind == "protocol_pdf":
            return self._extract_protocol_text(file_path)
        if kind in {"protocol_text", "protocol_markdown"}:
            return file_path.read_text(encoding="utf-8")
        raise ValueError(f"Asset kind {kind} does not support source text extraction")

    def _text_sha1(self, text: str) -> str:
        return hashlib.sha1((text or "").encode("utf-8")).hexdigest()

    def _normalize_manual_label(
        self,
        asset_id: str,
        payload: dict[str, Any],
        *,
        existing: Optional[dict[str, Any]] = None,
    ) -> dict[str, Any]:
        now = self._now_iso()
        text = str((payload or {}).get("text") or "").strip()
        if not text:
            raise ValueError("Manual label text is required")
        kind = str((payload or {}).get("kind") or "observation").strip().lower()
        if kind not in {"sign", "service", "observation"}:
            raise ValueError(f"Unsupported manual label kind: {kind}")
        concept_ref = str((payload or {}).get("concept_ref") or "").strip()
        concept_label = str((payload or {}).get("concept_label") or "").strip()
        label_id = str((payload or {}).get("label_id") or "").strip()
        if not label_id:
            entropy = json.dumps(
                [
                    asset_id,
                    kind,
                    text,
                    concept_ref,
                    concept_label,
                    now,
                ],
                ensure_ascii=False,
            )
            label_id = f"label_{hashlib.sha1(entropy.encode('utf-8')).hexdigest()[:12]}"
        mention_id = f"manual::{asset_id}::{label_id}"
        normalized = {
            "label_id": label_id,
            "mention_id": mention_id,
            "kind": kind,
            "text": text,
            "concept_ref": concept_ref,
            "concept_label": concept_label,
            "note": str((payload or {}).get("note") or "").strip(),
            "source_chunk_id": str((payload or {}).get("source_chunk_id") or "").strip(),
            "source_page": self._coerce_int((payload or {}).get("source_page")),
            "start_offset": self._coerce_int((payload or {}).get("start_offset")),
            "end_offset": self._coerce_int((payload or {}).get("end_offset")),
            "manual_status": str((payload or {}).get("manual_status") or "active").strip() or "active",
            "created_at": (existing or {}).get("created_at") or now,
            "updated_at": now,
        }
        return normalized

    def _coerce_int(self, value: Any) -> Optional[int]:
        try:
            if value in (None, ""):
                return None
            return int(value)
        except (TypeError, ValueError):
            return None

    def _default_review_state(self) -> dict[str, Any]:
        return {
            "needs_review": False,
            "status": "clear",
            "pending_reason": None,
            "last_impact_report_at": None,
            "last_review_items": [],
        }

    def _mark_asset_for_review(self, asset: dict[str, Any], *, reason: str) -> None:
        review = asset.setdefault("review", self._default_review_state())
        review["needs_review"] = True
        review["status"] = "needs_review"
        review["pending_reason"] = reason
        review.setdefault("last_review_items", [])
        asset["updated_at"] = self._now_iso()

    def _default_ingest_state(self, kind: str) -> dict[str, Any]:
        return {
            "supported": kind in SUPPORTED_DIRECT_INGEST_KINDS,
            "latest_run_id": None,
            "latest_status": None,
            "last_ingested_at": None,
            "last_error": None,
        }

    def _default_asset_config(self, kind: str, source_type: Optional[str], managed: bool) -> dict[str, Any]:
        return {
            "source_type": source_type or "BYT",
            "preferred_namespace": SERVICE_TABLE_DEFAULT_NAMESPACE if kind == "service_table" else "ontology_v2",
            "preferred_disease_name": "",
            "split_strategy": "auto" if kind == "protocol_pdf" else "single",
            "auto_ingest": bool(managed and kind in SUPPORTED_DIRECT_INGEST_KINDS),
        }

    def _managed_dir_for_kind(self, kind: str) -> Path:
        mapping = {
            "protocol_pdf": KNOWLEDGE_PROTOCOL_PDFS_DIR,
            "protocol_text": KNOWLEDGE_PROTOCOL_TEXTS_DIR,
            "protocol_markdown": KNOWLEDGE_PROTOCOL_TEXTS_DIR,
            "insurance_rulebook": KNOWLEDGE_INSURANCE_RULES_DIR,
            "benefit_table": KNOWLEDGE_BENEFIT_TABLES_DIR,
            "legal_document": KNOWLEDGE_LEGAL_DOCS_DIR,
            "service_table": KNOWLEDGE_SERVICE_TABLES_DIR,
            "symptom_table": KNOWLEDGE_SYMPTOM_TABLES_DIR,
            "benchmark_bundle": KNOWLEDGE_BENCHMARKS_DIR,
            "testcase_bundle": KNOWLEDGE_BENCHMARKS_DIR,
            "experience_memory": KNOWLEDGE_MEMORY_DIR,
        }
        return mapping.get(kind, KNOWLEDGE_MISC_DIR)

    def _managed_root_key_for_kind(self, kind: str) -> str:
        mapping = {
            "protocol_pdf": "managed_protocol_pdfs",
            "protocol_text": "managed_protocol_texts",
            "protocol_markdown": "managed_protocol_texts",
            "insurance_rulebook": "managed_insurance_rulebooks",
            "benefit_table": "managed_benefit_tables",
            "legal_document": "managed_legal_documents",
            "service_table": "managed_service_tables",
            "symptom_table": "managed_symptom_tables",
            "benchmark_bundle": "managed_benchmarks",
            "testcase_bundle": "managed_benchmarks",
            "experience_memory": "managed_memory",
        }
        return mapping.get(kind, "managed_misc")

    def _default_domain_for_kind(self, kind: str) -> str:
        mapping = {
            "protocol_pdf": "protocols",
            "protocol_text": "protocols",
            "protocol_markdown": "protocols",
            "insurance_rulebook": "insurance",
            "benefit_table": "insurance",
            "legal_document": "legal",
            "service_table": "taxonomy",
            "symptom_table": "taxonomy",
            "benchmark_bundle": "benchmark",
            "testcase_bundle": "benchmark",
            "experience_memory": "memory",
        }
        return mapping.get(kind, "misc")

    def _default_suffix_for_kind(self, kind: str) -> str:
        mapping = {
            "protocol_pdf": ".pdf",
            "protocol_text": ".txt",
            "protocol_markdown": ".md",
            "insurance_rulebook": ".pdf",
            "benefit_table": ".xlsx",
            "legal_document": ".pdf",
            "service_table": ".xlsx",
            "symptom_table": ".xlsx",
        }
        return mapping.get(kind, ".bin")

    def _asset_id(self, root_key: str, file_path: Path, root_path: Path) -> str:
        normalized = self._relative_to_root(file_path, root_path)
        digest = hashlib.sha1(f"{root_key}::{normalized}".encode("utf-8")).hexdigest()[:14]
        return f"asset_{digest}"

    def _relative_to_root(self, file_path: Path, root_path: Path) -> str:
        try:
            relative = file_path.resolve().relative_to(root_path.resolve())
            return relative.as_posix()
        except Exception:
            return file_path.name

    def _build_version_payload(self, file_path: Path, spec: dict[str, Any], root_path: Path) -> dict[str, Any]:
        stat = file_path.stat()
        sha1 = self._file_sha1(file_path)
        version_id = hashlib.sha1(
            f"{file_path.resolve()}::{sha1}::{int(stat.st_mtime)}".encode("utf-8")
        ).hexdigest()[:14]
        return {
            "version_id": version_id,
            "sha1": sha1,
            "size_bytes": stat.st_size,
            "modified_at": datetime.fromtimestamp(stat.st_mtime).isoformat(timespec="seconds"),
            "source_path": str(file_path),
            "source_relative_path": self._relative_to_root(file_path, root_path),
            "source_root_key": spec["key"],
            "recorded_at": self._now_iso(),
        }

    def _file_sha1(self, file_path: Path) -> str:
        digest = hashlib.sha1()
        with open(file_path, "rb") as handle:
            while True:
                chunk = handle.read(1024 * 1024)
                if not chunk:
                    break
                digest.update(chunk)
        return digest.hexdigest()

    def _iter_candidate_files(self, root_path: Path):
        for candidate in root_path.rglob("*"):
            if not candidate.is_file():
                continue
            if any(part in IGNORED_DIR_NAMES for part in candidate.parts):
                continue
            if candidate.suffix.lower() not in SCANNED_FILE_EXTENSIONS:
                continue
            yield candidate

    def _infer_asset_profile(self, file_path: Path, spec: dict[str, Any]) -> tuple[str, str, list[str]]:
        ext = file_path.suffix.lower()
        normalized = self._ascii_fold(str(file_path))
        tags: list[str] = []
        domain = spec.get("domain") or "misc"
        kind = spec.get("default_kind") or "knowledge_file"

        if "extract_protoc" in normalized or "phac do" in normalized or "protocol" in normalized:
            domain = "protocols"
            if ext == ".pdf":
                kind = "protocol_pdf"
            elif ext in {".txt", ".md", ".markdown"}:
                kind = "protocol_text" if ext == ".txt" else "protocol_markdown"
            tags.append("phac_do")

        if "06_insurance" in normalized or "quyen loi" in normalized or "loai tru" in normalized:
            domain = "insurance"
            if ext in {".xlsx", ".xls", ".csv"}:
                kind = "benefit_table"
            elif "quy tac" in normalized or "hop dong" in normalized or ext == ".pdf":
                kind = "insurance_rulebook"
            else:
                kind = "insurance_pack"
            tags.append("bao_hiem")

        if "phap ly" in normalized or "quy tac" in normalized:
            tags.append("quy_tac")

        if (
            "service_table" in normalized
            or "dich vu" in normalized
            or "danh muc dich vu" in normalized
            or "bang gia dich vu" in normalized
        ):
            domain = "taxonomy"
            if ext in {".xlsx", ".xls", ".csv"}:
                kind = "service_table"
            tags.append("dich_vu")

        if (
            "symptom_table" in normalized
            or "trieu chung" in normalized
            or "dau hieu" in normalized
            or "symptom" in normalized
        ):
            domain = "taxonomy"
            if ext in {".xlsx", ".xls", ".csv"}:
                kind = "symptom_table"
            tags.append("trieu_chung")

        if "datatest" in normalized or "benchmark" in normalized:
            domain = "benchmark"
            if ext in {".json", ".jsonl"}:
                kind = "testcase_bundle"
            else:
                kind = "benchmark_source_doc"
            tags.append("benchmark")

        if "experience_memory" in normalized:
            domain = "memory"
            kind = "experience_memory"
            tags.append("memory")

        if file_path.parent == KNOWLEDGE_PROTOCOL_PDFS_DIR:
            domain = "protocols"
            kind = "protocol_pdf"
        elif file_path.parent == KNOWLEDGE_PROTOCOL_TEXTS_DIR:
            domain = "protocols"
            kind = "protocol_text" if ext == ".txt" else "protocol_markdown"
        elif file_path.parent == KNOWLEDGE_INSURANCE_RULES_DIR:
            domain = "insurance"
            kind = "insurance_rulebook"
        elif file_path.parent == KNOWLEDGE_BENEFIT_TABLES_DIR:
            domain = "insurance"
            kind = "benefit_table"
        elif file_path.parent == KNOWLEDGE_LEGAL_DOCS_DIR:
            domain = "legal"
            kind = "legal_document"
        elif file_path.parent == KNOWLEDGE_SERVICE_TABLES_DIR:
            domain = "taxonomy"
            kind = "service_table"
        elif file_path.parent == KNOWLEDGE_SYMPTOM_TABLES_DIR:
            domain = "taxonomy"
            kind = "symptom_table"

        if "hospital" in normalized or "vinmec" in normalized or "tam anh" in normalized:
            tags.append("benh_vien")
        else:
            tags.append("byt")

        return domain, kind, sorted(set(tags))

    def _infer_source_type(self, file_path: Path) -> str:
        normalized = self._ascii_fold(str(file_path))
        if "vinmec" in normalized or "tam anh" in normalized or "hospital" in normalized:
            return "hospital"
        return "BYT"

    def _trace_terms_for_asset(self, asset: dict[str, Any]) -> list[str]:
        candidates = [
            asset.get("managed_relative_path") or "",
            asset.get("source_relative_path") or "",
            Path(asset.get("source_path") or "").name,
            Path(asset.get("source_path") or "").stem,
        ]
        result: list[str] = []
        for candidate in candidates:
            raw_value = str(candidate or "").replace("\\", "/").lower()
            folded_value = self._ascii_fold(str(candidate or ""))
            for value in (raw_value, folded_value):
                if len(value) < 5:
                    continue
                if value not in result:
                    result.append(value)
        return result[:8]

    def _ascii_fold(self, text: str) -> str:
        raw = str(text or "").replace("\\", "/").lower()
        raw = raw.replace("đ", "d").replace("Đ", "d")
        normalized = unicodedata.normalize("NFKD", raw)
        return "".join(ch for ch in normalized if not unicodedata.combining(ch))

    def _now_iso(self) -> str:
        return datetime.now().isoformat(timespec="seconds")
