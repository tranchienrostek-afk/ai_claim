from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from .pathway_client import PathwayClient
from .reasoning_agent import AzureReasoningAgent
from .settings import SETTINGS


def _json_dump(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


@dataclass(slots=True)
class LiveDuelRunner:
    project_root: Path
    pathway_client: PathwayClient

    @classmethod
    def create(cls) -> "LiveDuelRunner":
        return cls(
            project_root=SETTINGS.project_root,
            pathway_client=PathwayClient(base_url=SETTINGS.pathway_api_base_url),
        )

    def _build_summary(
        self,
        case_packet: dict[str, Any],
        azure_result: dict[str, Any],
        pathway_medical: dict[str, Any],
        pathway_adjudicate: dict[str, Any],
        pathway_health: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        azure_payload = azure_result.get("result", {}) or {}
        pathway_medical_payload = pathway_medical.get("payload", {}) or {}
        pathway_adjudicate_payload = pathway_adjudicate.get("payload", {}) or {}
        medical_metrics = self.pathway_client.summarize_medical_metrics(pathway_medical)
        adjudicate_metrics = self.pathway_client.summarize_adjudication_metrics(pathway_adjudicate)
        return {
            "case_id": case_packet.get("case_id", ""),
            "participants": {
                "ai_claim_azure": {
                    "duration_ms": azure_result.get("duration_ms"),
                    "llm_call_count": azure_result.get("llm_call_count"),
                    "tool_call_count": azure_result.get("tool_call_count"),
                    "tool_call_breakdown": azure_result.get("tool_call_breakdown", {}),
                    "usage": azure_result.get("usage", {}),
                    "claim_level_decision": azure_payload.get("claim_level_decision"),
                    "needs_human_review": azure_payload.get("needs_human_review"),
                    "line_result_count": len(azure_payload.get("line_results", []) or []),
                },
                "pathway": {
                    "medical_duration_ms": pathway_medical.get("duration_ms"),
                    "adjudicate_duration_ms": pathway_adjudicate.get("duration_ms"),
                    "medical_reasoning_trace_steps": len(pathway_medical_payload.get("reasoning_trace", []) or []),
                    "adjudicate_result_count": len(pathway_adjudicate_payload.get("results", []) or []),
                    "medical_mode": pathway_medical_payload.get("mode"),
                    "claim_summary_vi": pathway_adjudicate_payload.get("summary_vi", ""),
                    "medical_metrics": medical_metrics,
                    "adjudicate_metrics": adjudicate_metrics,
                    "graph_health": (pathway_health or {}).get("payload", {}),
                },
            },
            "reasoning_gap_vi": [
                "ai_claim_azure co telemetry tong toan phien: LLM calls, tool calls, token usage va duration.",
                "Pathway trong duel live da duoc tong hop them o muc verification plan, evidence ledger, decision breakdown va graph health.",
                "Raw token usage va raw Neo4j query count tu runtime Pathway van chua co trong response production.",
            ],
        }

    def _build_report(self, summary: dict[str, Any]) -> str:
        azure = summary["participants"]["ai_claim_azure"]
        pathway = summary["participants"]["pathway"]
        lines = [
            f"# Live duel: {summary['case_id']}",
            "",
            "## ai_claim Azure",
            f"- Duration ms: `{azure['duration_ms']}`",
            f"- LLM calls: `{azure['llm_call_count']}`",
            f"- Tool calls: `{azure['tool_call_count']}`",
            f"- Tokens: `{azure['usage']}`",
            f"- Claim decision: `{azure['claim_level_decision']}`",
            "",
            "## Pathway",
            f"- Medical duration ms: `{pathway['medical_duration_ms']}`",
            f"- Adjudicate duration ms: `{pathway['adjudicate_duration_ms']}`",
            f"- Medical reasoning trace steps: `{pathway['medical_reasoning_trace_steps']}`",
            f"- Claim summary: `{pathway['claim_summary_vi']}`",
            f"- Medical metrics: `{pathway['medical_metrics']}`",
            f"- Adjudicate metrics: `{pathway['adjudicate_metrics']}`",
            f"- Graph health: `{pathway['graph_health']}`",
            "",
            "## Nhan xet",
        ]
        lines.extend(f"- {item}" for item in summary["reasoning_gap_vi"])
        return "\n".join(lines)

    def run_case(self, case_packet: dict[str, Any]) -> dict[str, Any]:
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        case_id = str(case_packet.get("case_id") or "case").strip() or "case"
        run_dir = self.project_root / "data" / "duel_runs" / f"{stamp}_{case_id}"
        run_dir.mkdir(parents=True, exist_ok=True)

        _json_dump(run_dir / "case_packet.json", case_packet)

        agent = AzureReasoningAgent.from_settings()
        try:
            azure_result = agent.run_case(case_packet)
        finally:
            agent.toolkit.close()

        pathway_medical = self.pathway_client.run_medical_reasoning(case_packet)
        pathway_adjudicate = self.pathway_client.run_adjudication(case_packet)
        pathway_health = self.pathway_client.graph_operating_health()

        summary = self._build_summary(case_packet, azure_result, pathway_medical, pathway_adjudicate, pathway_health)
        report = self._build_report(summary)

        _json_dump(run_dir / "ai_claim_azure_result.json", azure_result)
        _json_dump(run_dir / "pathway_medical_result.json", pathway_medical)
        _json_dump(run_dir / "pathway_adjudicate_result.json", pathway_adjudicate)
        _json_dump(run_dir / "pathway_graph_health.json", pathway_health)
        _json_dump(run_dir / "summary.json", summary)
        (run_dir / "report.md").write_text(report, encoding="utf-8")

        return {
            "run_dir": str(run_dir),
            "summary": summary,
            "report_path": str(run_dir / "report.md"),
        }
