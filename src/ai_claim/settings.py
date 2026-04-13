from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


def _load_env_file(path: Path) -> None:
    if not path.exists():
        return
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


@dataclass(slots=True)
class Settings:
    project_root: Path
    data_dir: Path
    configs_dir: Path
    docs_dir: Path
    knowledge_dir: Path
    runtime_dir: Path
    static_dir: Path
    pathway_api_base_url: str
    azure_proxy_base_url: str
    router_base_url: str
    max_upload_bytes: int
    azure_openai_endpoint: str
    azure_openai_api_key: str
    azure_openai_api_version: str
    azure_openai_chat_deployment: str
    azure_openai_embedding_deployment: str

    @classmethod
    def load(cls) -> "Settings":
        project_root = Path(__file__).resolve().parents[2]
        _load_env_file(project_root / ".env.local")
        return cls(
            project_root=project_root,
            data_dir=project_root / "data",
            configs_dir=project_root / "configs",
            docs_dir=project_root / "docs",
            knowledge_dir=project_root / "data" / "knowledge",
            runtime_dir=project_root / "data" / "runtime",
            static_dir=project_root / "src" / "ai_claim" / "static",
            pathway_api_base_url=os.getenv("PATHWAY_API_BASE_URL", "http://localhost:9600"),
            azure_proxy_base_url=os.getenv("AZURE_PROXY_BASE_URL", "http://127.0.0.1:8009"),
            router_base_url=os.getenv("ROUTER_BASE_URL", "http://127.0.0.1:20128"),
            max_upload_bytes=int(os.getenv("AI_CLAIM_MAX_UPLOAD_BYTES", str(50 * 1024 * 1024))),
            azure_openai_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT", ""),
            azure_openai_api_key=os.getenv("AZURE_OPENAI_API_KEY", ""),
            azure_openai_api_version=os.getenv("AZURE_OPENAI_API_VERSION", "2024-10-21"),
            azure_openai_chat_deployment=os.getenv("AZURE_OPENAI_CHAT_DEPLOYMENT", ""),
            azure_openai_embedding_deployment=os.getenv("AZURE_OPENAI_EMBEDDING_DEPLOYMENT", ""),
        )


SETTINGS = Settings.load()
