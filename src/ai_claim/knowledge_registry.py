from __future__ import annotations

import hashlib
import json
import re
import shutil
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .settings import SETTINGS


SAFE_FILENAME_RE = re.compile(r"[^A-Za-z0-9._\-\u00C0-\u1EF9 ]+")


def _sha1_file(path: Path) -> str:
    digest = hashlib.sha1()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


@dataclass(slots=True)
class KnowledgeRegistry:
    project_root: Path
    config: dict[str, Any]
    registry_file: Path
    versions_dir: Path

    @classmethod
    def create(cls, project_root: Path, config: dict[str, Any]) -> "KnowledgeRegistry":
        registry_file = project_root / "data" / "runtime" / "knowledge_registry.json"
        versions_dir = project_root / "data" / "runtime" / "knowledge_versions"
        registry_file.parent.mkdir(parents=True, exist_ok=True)
        versions_dir.mkdir(parents=True, exist_ok=True)
        if not registry_file.exists():
            registry_file.write_text(
                json.dumps({"version": 1, "assets": []}, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
        return cls(project_root=project_root, config=config, registry_file=registry_file, versions_dir=versions_dir)

    def _roots_by_key(self) -> dict[str, dict[str, Any]]:
        return {str(item["key"]): item for item in self.config.get("roots", [])}

    def _sanitize_filename(self, filename: str) -> str:
        cleaned = SAFE_FILENAME_RE.sub("_", Path(filename).name).strip().strip(".")
        return cleaned or "upload.bin"

    def _validate_root_and_filename(self, root_key: str, filename: str, size_bytes: int | None = None) -> tuple[dict[str, Any], str]:
        roots = self._roots_by_key()
        if root_key not in roots:
            raise KeyError(f"Unknown root key: {root_key}")
        root = roots[root_key]
        safe_name = self._sanitize_filename(filename)
        accepted = set(str(item).lower() for item in root.get("accepted_types", []))
        suffix = Path(safe_name).suffix.lower().lstrip(".")
        if "dir" in accepted:
            raise ValueError(f"Root {root_key} chi nhan workspace/directory, khong nhan upload file truc tiep")
        if accepted and suffix not in accepted:
            raise ValueError(
                f"File type .{suffix or 'unknown'} khong hop le cho root {root_key}. "
                f"Accepted: {sorted(accepted)}"
            )
        if size_bytes is not None and size_bytes > SETTINGS.max_upload_bytes:
            raise ValueError(
                f"File vuot qua gioi han {SETTINGS.max_upload_bytes} bytes cho upload production"
            )
        return root, safe_name

    def _duplicate_groups(self, assets: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
        groups: dict[str, list[dict[str, Any]]] = {}
        for asset in assets:
            digest = str(asset.get("sha1") or "")
            if not digest:
                continue
            groups.setdefault(digest, []).append(asset)
        return {digest: items for digest, items in groups.items() if len(items) > 1}

    def load(self) -> dict[str, Any]:
        return json.loads(self.registry_file.read_text(encoding="utf-8"))

    def save(self, payload: dict[str, Any]) -> None:
        self.registry_file.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    def scan(self) -> dict[str, Any]:
        payload = self.load()
        assets: list[dict[str, Any]] = []
        known_by_path = {item["path"]: item for item in payload.get("assets", [])}
        for root in self.config.get("roots", []):
            root_path = self.project_root / root["path"]
            root_path.mkdir(parents=True, exist_ok=True)
            for path in sorted(root_path.rglob("*")):
                if not path.is_file():
                    continue
                if path.name == "CLAUDE.md":
                    continue
                if "__pycache__" in path.parts:
                    continue
                rel_path = str(path.relative_to(self.project_root))
                previous = known_by_path.get(rel_path, {})
                digest = _sha1_file(path)
                asset_id = previous.get("asset_id") or f"asset_{digest[:16]}"
                version = previous.get("version", 0) + (1 if previous.get("sha1") and previous.get("sha1") != digest else 0)
                assets.append(
                    {
                        "asset_id": asset_id,
                        "root_key": root["key"],
                        "path": rel_path,
                        "filename": path.name,
                        "extension": path.suffix.lower(),
                        "graph_target": root.get("graph_target"),
                        "sha1": digest,
                        "size_bytes": path.stat().st_size,
                        "version": max(version, 1),
                        "history": list(previous.get("history", [])),
                        "updated_at": datetime.now(timezone.utc).isoformat(),
                        "impact_hint": self._impact_hint(root["key"], rel_path),
                        "status": "cataloged",
                    }
                )
        duplicates = self._duplicate_groups(assets)
        duplicate_count_by_sha = {digest: len(items) for digest, items in duplicates.items()}
        for asset in assets:
            digest = str(asset.get("sha1") or "")
            asset["duplicate_group_size"] = duplicate_count_by_sha.get(digest, 1)
            asset["is_duplicate_content"] = digest in duplicates
        new_payload = {"version": payload.get("version", 1), "assets": assets}
        self.save(new_payload)
        return new_payload

    def list_assets(
        self,
        *,
        root_key: str | None = None,
        limit: int | None = None,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        assets = list(self.load().get("assets", []))
        if root_key:
            assets = [asset for asset in assets if asset.get("root_key") == root_key]
        assets.sort(key=lambda item: str(item.get("updated_at") or ""), reverse=True)
        offset = max(int(offset), 0)
        if limit is None:
            return assets[offset:]
        return assets[offset : offset + max(int(limit), 0)]

    def get_asset(self, asset_id: str) -> dict[str, Any] | None:
        for asset in self.list_assets():
            if asset.get("asset_id") == asset_id:
                return asset
        return None

    def find_asset(self, *, root_key: str, filename: str) -> dict[str, Any] | None:
        candidates = [
            asset
            for asset in self.list_assets()
            if asset.get("root_key") == root_key and asset.get("filename") == filename
        ]
        if not candidates:
            return None
        candidates.sort(key=lambda item: item.get("updated_at") or "", reverse=True)
        return candidates[0]

    def _impact_hint(self, root_key: str, rel_path: str) -> dict[str, Any]:
        if root_key == "protocols":
            return {
                "may_affect": ["medical graph", "disease-service links", "differential reasoning"],
                "human_review": True,
            }
        if root_key in {"insurance_rules", "benefit_tables", "legal_documents"}:
            return {
                "may_affect": ["insurance graph", "clause grounding", "payment rules"],
                "human_review": True,
            }
        if root_key in {"service_tables", "symptom_tables"}:
            return {
                "may_affect": ["canonical mapping", "service/sign alias resolution"],
                "human_review": True,
            }
        if root_key == "diseases":
            return {
                "may_affect": ["disease workspace memory", "reasoning hints", "feedback policy", "benchmark guidance"],
                "human_review": True,
            }
        if root_key == "adjuster_notes":
            return {
                "may_affect": ["memory", "review policy", "exception handling"],
                "human_review": False,
            }
        return {
            "may_affect": ["unknown"],
            "human_review": True,
        }

    def register_upload(self, root_key: str, filename: str, content: bytes) -> dict[str, Any]:
        root, safe_name = self._validate_root_and_filename(root_key, filename, len(content))
        target_dir = self.project_root / root["path"]
        target_dir.mkdir(parents=True, exist_ok=True)
        target_path = target_dir / safe_name
        if target_path.exists():
            self._snapshot_existing_version(target_path)
        target_path.write_bytes(content)
        return self.scan()

    def register_existing_file(self, root_key: str, source_path: Path) -> dict[str, Any]:
        source_path = Path(source_path)
        if not source_path.exists():
            raise FileNotFoundError(source_path)
        root, safe_name = self._validate_root_and_filename(root_key, source_path.name, source_path.stat().st_size)
        target_dir = self.project_root / root["path"]
        target_dir.mkdir(parents=True, exist_ok=True)
        target_path = target_dir / safe_name
        if source_path.resolve() != target_path.resolve():
            if target_path.exists():
                self._snapshot_existing_version(target_path)
            shutil.copy2(source_path, target_path)
        return self.scan()

    def root_summary(self) -> dict[str, Any]:
        assets = self.list_assets()
        roots = self._roots_by_key()
        by_root: dict[str, dict[str, Any]] = {}
        for key, root in roots.items():
            by_root[key] = {
                "root_key": key,
                "graph_target": root.get("graph_target"),
                "accepted_types": root.get("accepted_types", []),
                "asset_count": 0,
                "duplicate_content_assets": 0,
                "human_review_count": 0,
            }
        for asset in assets:
            root_key = str(asset.get("root_key") or "")
            row = by_root.get(root_key)
            if not row:
                continue
            row["asset_count"] += 1
            if asset.get("is_duplicate_content"):
                row["duplicate_content_assets"] += 1
            if (asset.get("impact_hint") or {}).get("human_review"):
                row["human_review_count"] += 1
        return {
            "roots": list(by_root.values()),
            "duplicate_groups": [
                {
                    "sha1": digest,
                    "count": len(items),
                    "paths": [str(item.get("path")) for item in items],
                }
                for digest, items in self._duplicate_groups(assets).items()
            ],
        }

    def _snapshot_existing_version(self, target_path: Path) -> None:
        if not target_path.exists():
            return
        payload = self.load()
        rel_path = str(target_path.relative_to(self.project_root))
        for asset in payload.get("assets", []):
            if asset.get("path") != rel_path:
                continue
            digest = _sha1_file(target_path)
            version = int(asset.get("version", 1) or 1)
            asset_id = str(asset.get("asset_id") or f"asset_{digest[:16]}")
            snapshot_dir = self.versions_dir / asset_id
            snapshot_dir.mkdir(parents=True, exist_ok=True)
            snapshot_name = f"v{version:03d}_{digest[:12]}_{target_path.name}"
            snapshot_path = snapshot_dir / snapshot_name
            if not snapshot_path.exists():
                shutil.copy2(target_path, snapshot_path)
            history = list(asset.get("history", []))
            history.append(
                {
                    "version": version,
                    "sha1": digest,
                    "snapshot_path": str(snapshot_path.relative_to(self.project_root)),
                    "captured_at": datetime.now(timezone.utc).isoformat(),
                    "size_bytes": target_path.stat().st_size,
                }
            )
            asset["history"] = history
            self.save(payload)
            return
