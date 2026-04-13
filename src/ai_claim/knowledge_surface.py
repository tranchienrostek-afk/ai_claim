from __future__ import annotations

import json
import math
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any


TOKEN_RE = re.compile(r"[A-Za-zÀ-ỹ0-9_]+", re.UNICODE)
TEXT_EXTENSIONS = {".md", ".txt", ".json", ".jsonl"}


def _tokenize(text: str) -> list[str]:
    return [token.lower() for token in TOKEN_RE.findall(text)]


def _safe_read_text(path: Path) -> str:
    if path.suffix.lower() not in TEXT_EXTENSIONS:
        return ""
    try:
        return path.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        return path.read_text(encoding="utf-8", errors="ignore")


def _snippet(text: str, query: str, max_chars: int = 400) -> str:
    lower = text.lower()
    query_tokens = _tokenize(query)
    index = -1
    for token in query_tokens:
        index = lower.find(token.lower())
        if index >= 0:
            break
    if index < 0:
        return text[:max_chars].strip()
    start = max(0, index - max_chars // 3)
    end = min(len(text), start + max_chars)
    return text[start:end].strip()


@dataclass(slots=True)
class KnowledgeSurface:
    project_root: Path
    config: dict[str, Any]

    def _root_map(self) -> dict[str, Path]:
        return {
            item["key"]: self.project_root / item["path"]
            for item in self.config.get("roots", [])
        }

    def _iter_paths(
        self,
        root_key: str | None = None,
        disease_key: str | None = None,
    ) -> list[Path]:
        root_map = self._root_map()
        roots: list[Path] = []
        if root_key:
            root = root_map.get(root_key)
            if root:
                roots.append(root)
        else:
            roots.extend(root_map.values())
        if disease_key:
            disease_root = self.project_root / "data" / "knowledge" / "diseases" / disease_key
            if disease_root.exists():
                roots.append(disease_root)
        paths: list[Path] = []
        for root in roots:
            if not root.exists():
                continue
            for path in root.rglob("*"):
                if path.is_file() and path.name != "CLAUDE.md":
                    paths.append(path)
        return sorted(set(paths))

    def search(
        self,
        query: str,
        limit: int = 8,
        root_key: str | None = None,
        disease_key: str | None = None,
    ) -> dict[str, Any]:
        query_tokens = _tokenize(query)
        if not query_tokens:
            return {"query": query, "hits": []}

        scored_hits: list[dict[str, Any]] = []
        for path in self._iter_paths(root_key=root_key, disease_key=disease_key):
            text = _safe_read_text(path)
            if not text:
                continue
            tokens = _tokenize(text)
            if not tokens:
                continue
            token_set = set(tokens)
            overlap = sum(1 for token in query_tokens if token in token_set)
            if overlap == 0:
                continue
            unique_overlap = len({token for token in query_tokens if token in token_set})
            density = overlap / max(len(tokens), 1)
            score = (unique_overlap * 10.0) + math.log10(max(len(text), 10)) + (density * 1000.0)
            rel_path = path.relative_to(self.project_root)
            scored_hits.append(
                {
                    "path": str(rel_path),
                    "filename": path.name,
                    "score": round(score, 3),
                    "matched_tokens": sorted({token for token in query_tokens if token in token_set}),
                    "snippet": _snippet(text, query),
                }
            )

        scored_hits.sort(key=lambda item: (-item["score"], item["path"]))
        return {
            "query": query,
            "root_key": root_key or "",
            "disease_key": disease_key or "",
            "hits": scored_hits[: max(1, min(int(limit), 20))],
        }

    def read(self, relative_path: str) -> dict[str, Any]:
        target = (self.project_root / relative_path).resolve()
        if not str(target).startswith(str(self.project_root.resolve())):
            raise ValueError("Path nam ngoai project root")
        if not target.exists() or not target.is_file():
            raise FileNotFoundError(relative_path)
        if target.suffix.lower() not in TEXT_EXTENSIONS:
            raise ValueError("Chi ho tro doc text/json files trong knowledge surface")
        text = _safe_read_text(target)
        preview = text[:2000]
        payload: dict[str, Any] = {
            "path": str(target.relative_to(self.project_root)),
            "size_bytes": target.stat().st_size,
            "preview": preview,
        }
        if target.suffix.lower() == ".json":
            try:
                payload["parsed"] = json.loads(text)
            except json.JSONDecodeError:
                pass
        return payload
