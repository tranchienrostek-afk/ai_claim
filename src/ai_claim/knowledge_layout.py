from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any


DISEASE_WORKSPACE_TEMPLATE = """# {disease_name}

Workspace benh nay dung de quan tri toan bo tri thuc lien quan:

- protocols/
- notes/
- skills/
- versions/
- benchmarks/
- feedback/

Nguyen tac:

- Moi protocol moi phai co version.
- Moi note thẩm định phai truy vet duoc tac gia va ngay gio.
- Moi thay doi lon phai co impact report.
"""


@dataclass(slots=True)
class KnowledgeLayout:
    raw: dict[str, Any]
    project_root: Path

    @classmethod
    def from_file(cls, project_root: Path, path: Path) -> "KnowledgeLayout":
        return cls(json.loads(path.read_text(encoding="utf-8")), project_root)

    def root_items(self) -> list[dict[str, Any]]:
        return list(self.raw.get("roots", []))

    def ensure_roots(self) -> list[dict[str, Any]]:
        results: list[dict[str, Any]] = []
        for item in self.root_items():
            full_path = self.project_root / item["path"]
            full_path.mkdir(parents=True, exist_ok=True)
            results.append(
                {
                    "key": item["key"],
                    "path": str(full_path),
                    "graph_target": item.get("graph_target"),
                    "accepted_types": item.get("accepted_types", []),
                }
            )
        return results

    def create_disease_workspace(self, disease_key: str, disease_name: str) -> dict[str, Any]:
        disease_root = self.project_root / "data" / "knowledge" / "diseases" / disease_key
        for child in ["protocols", "notes", "skills", "versions", "benchmarks", "feedback"]:
            (disease_root / child).mkdir(parents=True, exist_ok=True)
        guide_path = disease_root / "CLAUDE.md"
        if not guide_path.exists():
            guide_path.write_text(
                DISEASE_WORKSPACE_TEMPLATE.format(disease_name=disease_name),
                encoding="utf-8",
            )
        return {
            "disease_key": disease_key,
            "disease_name": disease_name,
            "path": str(disease_root),
            "guide_path": str(guide_path),
        }

