from __future__ import annotations

import json
import sys
from pathlib import Path

from fastapi.testclient import TestClient

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.ai_claim.main import app


def _ensure_agent_claude_samples() -> tuple[Path, Path]:
    prompt_path = PROJECT_ROOT / "configs" / "agent_claude_sample_prompt.txt"
    mcp_path = PROJECT_ROOT / "configs" / "agent_claude_sample_mcp_config.json"
    if not prompt_path.exists():
        prompt_path.write_text(
            "Ban la sample prompt de kiem tra route tao launch spec trong ai_claim.\n",
            encoding="utf-8",
        )
    if not mcp_path.exists():
        mcp_path.write_text(
            json.dumps({"mcpServers": {"pathway-neo4j": {"command": "python", "args": ["launch_pathway_neo4j_mcp.py"]}}}, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
    return prompt_path, mcp_path


def main() -> None:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    prompt_path, mcp_path = _ensure_agent_claude_samples()
    client = TestClient(app)
    outputs = {}
    outputs["health"] = client.get("/health").json()
    outputs["domain_policy"] = client.get("/api/domain-policy").json()
    outputs["knowledge_layout"] = client.get("/api/knowledge-layout").json()
    outputs["knowledge_scan"] = client.post("/api/knowledge/scan").json()
    outputs["knowledge_root_summary"] = client.get("/api/knowledge/root-summary").json()
    outputs["knowledge_surface"] = client.get(
        "/api/knowledge/surface/search",
        params={"query": "Meniere MRI ENG", "disease_key": "H81_0_meniere"},
    ).json()
    outputs["ingest_support_matrix"] = client.get("/api/ingest/support-matrix").json()
    outputs["upload"] = client.post(
        "/api/knowledge/upload",
        data={"root_key": "adjuster_notes"},
        files={"file": ("smoke_note.txt", b"smoke note for registry", "text/plain")},
    ).json()
    outputs["assets"] = client.get("/api/knowledge/assets").json()
    outputs["pathway_knowledge_bootstrap"] = client.get("/api/pathway/knowledge/bootstrap").json()
    outputs["architecture"] = client.get("/api/architecture").json()
    outputs["system_status"] = client.get("/api/system/status").json()
    outputs["production_readiness"] = client.get("/api/production-readiness").json()
    outputs["mapping_key_audit"] = client.get("/api/neo4j/mapping-key-audit").json()
    outputs["agent_launch_spec"] = client.get(
        "/api/agent-launch-spec",
        params={
            "prompt_file": str(prompt_path),
            "mcp_config_file": str(mcp_path),
            "model": "sonnet",
        },
    ).json()
    outputs["benchmark"] = client.get(
        "/api/benchmark/summary",
        params={
            "run_dir": str(
                PROJECT_ROOT.parent
                / "pathway"
                / "notebooklm"
                / "data"
                / "duel_runs"
                / "pathway_vs_agent_claude"
                / "20260409_224555_duel_meniere_001"
            )
        },
    ).json()
    outputs["neo4j_health"] = client.get("/api/neo4j/health").json()
    print(json.dumps(outputs, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
