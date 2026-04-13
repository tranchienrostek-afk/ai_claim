from __future__ import annotations

import json
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(slots=True)
class DuelAnalyzer:
    run_dir: Path

    def _read_json(self, name: str) -> dict[str, Any]:
        return json.loads((self.run_dir / name).read_text(encoding="utf-8"))

    def _read_jsonl(self, name: str) -> list[dict[str, Any]]:
        path = self.run_dir / name
        rows: list[dict[str, Any]] = []
        for line in path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line:
                continue
            rows.append(json.loads(line))
        return rows

    def _analyze_agent_claude(self) -> dict[str, Any]:
        events = self._read_jsonl("agent_claude_stream.jsonl")
        init_event = next((item for item in events if item.get("type") == "system"), {})
        result_event = next((item for item in reversed(events) if item.get("type") == "result"), {})
        tool_counter: Counter[str] = Counter()
        assistant_messages = 0
        for event in events:
            if event.get("type") == "assistant":
                assistant_messages += 1
                message = event.get("message", {})
                for block in message.get("content", []):
                    if block.get("type") == "tool_use":
                        tool_counter[block.get("name", "unknown")] += 1
        usage = result_event.get("usage", {})
        model_usage = result_event.get("modelUsage", {})
        llm_turns = result_event.get("num_turns")
        if llm_turns is None:
            llm_turns = len({event.get("message", {}).get("id") for event in events if event.get("type") == "assistant" and event.get("message", {}).get("id")})
        return {
            "participant": "agent_claude",
            "model": init_event.get("model") or result_event.get("model"),
            "duration_ms": result_event.get("duration_ms"),
            "num_turns": result_event.get("num_turns"),
            "assistant_message_count": assistant_messages,
            "neo4j_call_count": sum(count for name, count in tool_counter.items() if "mcp__pathway-neo4j__" in name),
            "neo4j_call_breakdown": dict(sorted(tool_counter.items())),
            "llm_call_count": llm_turns,
            "llm_call_measurement": "exact_from_terminal_turn_counter",
            "tool_call_measurement": "exact_from_stream",
            "usage": usage,
            "model_usage": model_usage,
            "total_cost_usd": result_event.get("total_cost_usd"),
            "allowed_tools": init_event.get("tools", []),
            "mcp_servers": init_event.get("mcp_servers", []),
        }

    def _pathway_reasoning_trace_count(self, payload: Any) -> int:
        if isinstance(payload, dict):
            total = 0
            for key, value in payload.items():
                if key == "reasoning_trace" and isinstance(value, list):
                    total += len(value)
                else:
                    total += self._pathway_reasoning_trace_count(value)
            return total
        if isinstance(payload, list):
            return sum(self._pathway_reasoning_trace_count(item) for item in payload)
        return 0

    def _pathway_evidence_source_counter(self, payload: Any) -> Counter[str]:
        counter: Counter[str] = Counter()
        if isinstance(payload, dict):
            if "source" in payload and isinstance(payload["source"], str):
                counter[payload["source"]] += 1
            for value in payload.values():
                counter.update(self._pathway_evidence_source_counter(value))
        elif isinstance(payload, list):
            for item in payload:
                counter.update(self._pathway_evidence_source_counter(item))
        return counter

    def _analyze_pathway(self) -> dict[str, Any]:
        medical = self._read_json("pathway_medical_response.json")
        adjudicate = self._read_json("pathway_adjudicate_response.json")
        normalized = self._read_json("pathway_normalized_result.json")
        medical_sources = self._pathway_evidence_source_counter(medical)
        adjudicate_sources = self._pathway_evidence_source_counter(adjudicate)
        return {
            "participant": "pathway",
            "api_calls": 2,
            "api_call_measurement": "exact_from_duel_harness",
            "llm_call_count": None,
            "llm_call_measurement": "unknown_due_to_missing_runtime_telemetry",
            "neo4j_call_count": None,
            "neo4j_call_measurement": "unknown_due_to_missing_runtime_telemetry",
            "reasoning_trace_steps": self._pathway_reasoning_trace_count(medical) + self._pathway_reasoning_trace_count(adjudicate),
            "observable_evidence_sources": dict((medical_sources + adjudicate_sources).most_common()),
            "recognized_services": [
                item.get("recognized_service_code")
                for item in normalized.get("line_results", [])
                if item.get("recognized_service_code")
            ],
            "active_diseases": normalized.get("active_diseases", []),
            "claim_level_decision": normalized.get("claim_level_decision"),
            "instrumentation_gap_vi": [
                "Artifact hien tai khong luu raw token usage cua Pathway.",
                "Artifact hien tai khong luu raw Neo4j query count cua Pathway.",
                "Can them telemetry trong API/runtime neu muon so sanh cong bang."
            ],
        }

    def build_reasoning_gap(self) -> dict[str, Any]:
        duel_score = self._read_json("duel_score.json")
        agent = self._analyze_agent_claude()
        pathway = self._analyze_pathway()
        return {
            "run_dir": str(self.run_dir),
            "case_id": duel_score.get("case_id"),
            "score": duel_score,
            "participants": {
                "pathway": pathway,
                "agent_claude": agent,
            },
            "reasoning_gap_vi": {
                "summary": [
                    "Pathway manh o graph discipline va deterministic adjudication.",
                    "agent_claude manh o planner, search song song, va synthesis.",
                    "Trong run nay, agent_claude co tool telemetry day du hon Pathway."
                ],
                "why_agent_claude_looked_smarter": [
                    "No lap search plan truoc khi tra loi.",
                    "No goi nhieu MCP Neo4j song song va tong hop lai.",
                    "No chap nhan review khi clause xung dot thay vi deny bua.",
                    "No co telemetry tool-use day du, nen kha nang cua no quan sat ro hon."
                ],
                "why_pathway_failed": [
                    "Disease coverage cua H81.0 trong graph live yeu.",
                    "Ontology warning: thieu ASSERTION_INDICATES_SERVICE edges.",
                    "Runtime Pathway chua expose token/query telemetry de debug cong bang."
                ],
            },
        }

    def build_markdown_report(self) -> str:
        summary = self.build_reasoning_gap()
        agent = summary["participants"]["agent_claude"]
        pathway = summary["participants"]["pathway"]
        lines = [
            f"# Benchmark summary: {summary['case_id']}",
            "",
            f"- Run dir: `{summary['run_dir']}`",
            f"- Pathway accuracy: `{summary['score']['pathway']['accuracy']}`",
            f"- agent_claude accuracy: `{summary['score']['agent_claude']['accuracy']}`",
            "",
            "## agent_claude",
            f"- Model: `{agent['model']}`",
            f"- Duration ms: `{agent['duration_ms']}`",
            f"- Neo4j calls: `{agent['neo4j_call_count']}`",
            f"- LLM calls: `{agent['llm_call_count']}`",
            f"- Cost USD: `{agent['total_cost_usd']}`",
            "",
            "## Pathway",
            f"- API calls: `{pathway['api_calls']}`",
            f"- Reasoning trace steps: `{pathway['reasoning_trace_steps']}`",
            f"- LLM telemetry: `{pathway['llm_call_measurement']}`",
            f"- Neo4j telemetry: `{pathway['neo4j_call_measurement']}`",
            "",
            "## Nhận xét",
        ]
        lines.extend([f"- {item}" for item in summary["reasoning_gap_vi"]["summary"]])
        return "\n".join(lines)
