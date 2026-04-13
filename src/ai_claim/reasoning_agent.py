from __future__ import annotations

import json
import re
import time
from collections import Counter
from dataclasses import dataclass
from typing import Any

from .azure_openai_backend import AzureOpenAIBackend, AzureOpenAIConfig
from .knowledge_surface import KnowledgeSurface
from .neo4j_toolkit import Neo4jToolkit
from .settings import SETTINGS


SYSTEM_PROMPT = """
Ban la medical-insurance reasoning agent bac cao, chuyen gia ve "Con duong kinh nghiem".

Nhiem vu:
1. TOI GIAN SUY LUAN: Khong di vong vo. Dung "Con duong kinh nghiem" de xac dinh ngay cac node quan trong (Protocol, Service, Exclusion).
2. TACH RIENG TANG: Tach biet ro rang tang Y khoa (Medical) va tang Bao hiem (Insurance).
3. BANG CHUNG CHI TIET: Moi ket luan phai co "Evidence Trace" cuc ky chi tiet (trich dan dieu khoan, chi so can lam sang, ma dich vu) de tham dinh vien co the trace va audit.
4. TOI UU HOA TIM KIEM: Chi tim nhung gi thuc su can thiet de ra quyet dinh. Neu da du bang chung, hay vao ket luan ngay.
5. TRA VE JSON: Output cuoi cung phai la JSON hop le.

JSON format:
{
  "case_id": "",
  "reasoning_path": "Mo ta tom tat con duong suy luan toi uu da di qua",
  "active_diseases": [{"name": "", "basis": "Bang chung y khoa chi tiet"}],
  "line_results": [
    {
      "line_no": 1,
      "service_name_raw": "",
      "medical_decision": "approve|deny|review|uncertain",
      "insurance_decision": "approve|deny|partial_pay|review|uncertain",
      "final_decision": "approve|deny|partial_pay|review|uncertain",
      "reasoning": "Giai thich ngan gon nhung du y",
      "evidence": ["Trich dan tung bang chung, tung dong trong phac do hoac hop dong"]
    }
  ],
  "claim_level_decision": "approve|deny|review",
  "claim_level_reasoning": "Tong hop ly do cuoi cung",
  "needs_human_review": true
}
"""


TOOL_SPECS = [
    {
        "type": "function",
        "function": {
            "name": "graph_health",
            "description": "Lay tinh trang tong quat cua graph y te, insurance, ontology.",
            "parameters": {"type": "object", "properties": {}, "additionalProperties": False},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "list_recent_ci_diseases",
            "description": "Lay danh sach benh gan day trong claims graph.",
            "parameters": {
                "type": "object",
                "properties": {"limit": {"type": "integer"}},
                "additionalProperties": False,
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "query_ci_disease_snapshot",
            "description": "Lay snapshot cua mot benh voi signs va services.",
            "parameters": {
                "type": "object",
                "properties": {
                    "disease_id": {"type": "string"},
                    "disease_name": {"type": "string"},
                    "limit": {"type": "integer"},
                },
                "additionalProperties": False,
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "query_disease_services",
            "description": "Lay danh sach dich vu lien quan den benh.",
            "parameters": {
                "type": "object",
                "properties": {
                    "disease_name": {"type": "string"},
                    "icd_code": {"type": "string"},
                },
                "additionalProperties": False,
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "query_contract_stats",
            "description": "Thong tin tong hop cua hop dong bao hiem.",
            "parameters": {
                "type": "object",
                "properties": {"contract_id": {"type": "string"}},
                "required": ["contract_id"],
                "additionalProperties": False,
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "query_benefits_for_contract",
            "description": "Lay benefit cua hop dong, co the loc theo ten benefit.",
            "parameters": {
                "type": "object",
                "properties": {
                    "contract_id": {"type": "string"},
                    "benefit_name": {"type": "string"},
                },
                "required": ["contract_id"],
                "additionalProperties": False,
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "query_exclusions_by_contract",
            "description": "Lay exclusion cua hop dong.",
            "parameters": {
                "type": "object",
                "properties": {"contract_id": {"type": "string"}},
                "required": ["contract_id"],
                "additionalProperties": False,
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "query_service_exclusions",
            "description": "Tim exclusion lien quan den service.",
            "parameters": {
                "type": "object",
                "properties": {
                    "service_code": {"type": "string"},
                    "service_name": {"type": "string"},
                    "contract_id": {"type": "string"},
                },
                "additionalProperties": False,
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "query_clinical_service_info",
            "description": "Lay thong tin y khoa cua service theo code.",
            "parameters": {
                "type": "object",
                "properties": {"service_code": {"type": "string"}},
                "required": ["service_code"],
                "additionalProperties": False,
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "trace_service_evidence",
            "description": "Trace evidence cho service voi disease va contract.",
            "parameters": {
                "type": "object",
                "properties": {
                    "service_name": {"type": "string"},
                    "disease_id": {"type": "string"},
                    "contract_id": {"type": "string"},
                },
                "required": ["service_name"],
                "additionalProperties": False,
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "search_knowledge_surface",
            "description": "Tim trong knowledge surface noi bo cua ai_claim: notes, feedback, benchmark, protocol text, huong dan theo benh.",
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {"type": "string"},
                    "limit": {"type": "integer"},
                    "root_key": {"type": "string"},
                    "disease_key": {"type": "string"},
                },
                "required": ["query"],
                "additionalProperties": False,
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "read_knowledge_asset",
            "description": "Doc noi dung mot file text/json trong knowledge surface khi can xem ky hon.",
            "parameters": {
                "type": "object",
                "properties": {
                    "relative_path": {"type": "string"},
                },
                "required": ["relative_path"],
                "additionalProperties": False,
            },
        },
    },
]


def _slug(text: str) -> str:
    normalized = (
        text.lower()
        .replace("é", "e")
        .replace("è", "e")
        .replace("ê", "e")
        .replace("ë", "e")
        .replace("á", "a")
        .replace("à", "a")
        .replace("ả", "a")
        .replace("ã", "a")
        .replace("ạ", "a")
        .replace("í", "i")
        .replace("ì", "i")
        .replace("ị", "i")
        .replace("ó", "o")
        .replace("ò", "o")
        .replace("ô", "o")
        .replace("ơ", "o")
        .replace("ú", "u")
        .replace("ù", "u")
        .replace("ư", "u")
    )
    return re.sub(r"[^a-z0-9]+", "_", normalized).strip("_")


@dataclass(slots=True)
class AzureReasoningAgent:
    backend: AzureOpenAIBackend
    toolkit: Neo4jToolkit
    knowledge_surface: KnowledgeSurface

    @classmethod
    def from_settings(cls) -> "AzureReasoningAgent":
        backend = AzureOpenAIBackend(
            AzureOpenAIConfig(
                endpoint=SETTINGS.azure_openai_endpoint,
                api_key=SETTINGS.azure_openai_api_key,
                api_version=SETTINGS.azure_openai_api_version,
                chat_deployment=SETTINGS.azure_openai_chat_deployment,
            )
        )
        toolkit = Neo4jToolkit()
        knowledge_surface = KnowledgeSurface(
            project_root=SETTINGS.project_root,
            config=json.loads((SETTINGS.configs_dir / "knowledge_roots.json").read_text(encoding="utf-8")),
        )
        return cls(backend=backend, toolkit=toolkit, knowledge_surface=knowledge_surface)

    def _derive_disease_key(self, case_packet: dict[str, Any]) -> str:
        disease_key = str(case_packet.get("disease_key") or "").strip()
        if disease_key:
            return disease_key

        candidates: list[str] = []
        candidates.append(str(case_packet.get("case_id") or ""))
        for item in case_packet.get("known_diseases", []) or []:
            candidates.append(str(item))
        for item in case_packet.get("suspected_diseases", []) or []:
            candidates.append(str(item))
        if "clinical_context" in case_packet:
            admission_reason = str(case_packet.get("clinical_context", {}).get("admission_reason") or "")
            candidates.append(admission_reason)
            for item in case_packet.get("clinical_context", {}).get("symptoms", []) or []:
                candidates.append(str(item))
        blob = " ".join(candidates)
        if "meniere" in _slug(blob):
            return "H81_0_meniere"
        return ""

    def _seed_workspace_context(self, case_packet: dict[str, Any], disease_key: str) -> dict[str, Any]:
        if not disease_key:
            return {}
        query = disease_key
        if disease_key == "H81_0_meniere":
            query = "Meniere MRI ENG thinh luc"
        return self.knowledge_surface.search(
            query=query,
            disease_key=disease_key,
            limit=4,
        )

    def _call_tool(self, name: str, arguments: dict[str, Any]) -> Any:
        if name == "graph_health":
            return self.toolkit.graph_health()
        if name == "list_recent_ci_diseases":
            return self.toolkit.list_recent_ci_diseases(arguments.get("limit", 10))
        if name == "query_ci_disease_snapshot":
            return self.toolkit.query_ci_disease_snapshot(
                disease_id=arguments.get("disease_id", ""),
                disease_name=arguments.get("disease_name", ""),
                limit=arguments.get("limit", 10),
            )
        if name == "query_disease_services":
            return self.toolkit.query_disease_services(
                disease_name=arguments.get("disease_name", ""),
                icd_code=arguments.get("icd_code", ""),
            )
        if name == "query_contract_stats":
            return self.toolkit.query_contract_stats(arguments["contract_id"])
        if name == "query_benefits_for_contract":
            return self.toolkit.query_benefits_for_contract(
                arguments["contract_id"],
                arguments.get("benefit_name", ""),
            )
        if name == "query_exclusions_by_contract":
            return self.toolkit.query_exclusions_by_contract(arguments["contract_id"])
        if name == "query_service_exclusions":
            return self.toolkit.query_service_exclusions(
                service_code=arguments.get("service_code", ""),
                service_name=arguments.get("service_name", ""),
                contract_id=arguments.get("contract_id", ""),
            )
        if name == "query_clinical_service_info":
            return self.toolkit.query_clinical_service_info(arguments["service_code"])
        if name == "trace_service_evidence":
            return self.toolkit.trace_service_evidence(
                service_name=arguments["service_name"],
                disease_id=arguments.get("disease_id", ""),
                contract_id=arguments.get("contract_id", ""),
            )
        if name == "search_knowledge_surface":
            return self.knowledge_surface.search(
                query=arguments["query"],
                limit=arguments.get("limit", 6),
                root_key=arguments.get("root_key") or None,
                disease_key=arguments.get("disease_key") or None,
            )
        if name == "read_knowledge_asset":
            return self.knowledge_surface.read(arguments["relative_path"])
        raise KeyError(f"Unknown tool: {name}")

    def run_case(self, case_packet: dict[str, Any], max_turns: int = 10) -> dict[str, Any]:
        if not self.backend.is_configured():
            raise RuntimeError("Azure OpenAI chua cau hinh.")
        try:
            from openai import AzureOpenAI
        except ImportError as exc:
            raise RuntimeError("Thu vien openai chua san sang.") from exc

        disease_key = self._derive_disease_key(case_packet)
        workspace_seed_hits = self._seed_workspace_context(case_packet, disease_key)

        # Determine base_url: use 9router if configured AND reachable, else direct Azure
        effective_base_url = None
        router_url = SETTINGS.router_base_url
        if router_url:
            import httpx
            try:
                with httpx.Client(timeout=2.0) as probe:
                    probe.get(f"{router_url.rstrip('/')}/v1/models")
                effective_base_url = f"{router_url.rstrip('/')}/v1"
            except Exception:
                pass  # router unreachable, fall back to direct Azure

        client = AzureOpenAI(
            api_key=self.backend.config.api_key,
            api_version=self.backend.config.api_version,
            azure_endpoint=self.backend.config.endpoint,
            base_url=effective_base_url,
        )
        messages: list[dict[str, Any]] = [
            {"role": "system", "content": SYSTEM_PROMPT},
            {
                "role": "user",
                "content": json.dumps(
                    {
                        "case_packet": case_packet,
                        "disease_workspace_hint": disease_key,
                        "workspace_seed_hits": workspace_seed_hits,
                    },
                    ensure_ascii=False,
                ),
            },
        ]
        ledger: list[dict[str, Any]] = []
        tool_counter: Counter[str] = Counter()
        started_at = time.perf_counter()
        llm_call_count = 0
        usage_totals = {
            "prompt_tokens": 0,
            "completion_tokens": 0,
            "total_tokens": 0,
        }

        for _ in range(max_turns):
            response = client.chat.completions.create(
                model=self.backend.config.chat_deployment,
                messages=messages,
                tools=TOOL_SPECS,
                tool_choice="auto",
                temperature=0.1,
                max_tokens=4096,
                response_format={"type": "json_object"},
            )
            llm_call_count += 1
            usage_totals["prompt_tokens"] += int(getattr(response.usage, "prompt_tokens", 0) or 0)
            usage_totals["completion_tokens"] += int(getattr(response.usage, "completion_tokens", 0) or 0)
            usage_totals["total_tokens"] += int(getattr(response.usage, "total_tokens", 0) or 0)
            choice = response.choices[0]
            message = choice.message
            tool_calls = getattr(message, "tool_calls", None) or []
            if tool_calls:
                assistant_tool_message = {
                    "role": "assistant",
                    "content": message.content or "",
                    "tool_calls": [
                        {
                            "id": tool.id,
                            "type": "function",
                            "function": {
                                "name": tool.function.name,
                                "arguments": tool.function.arguments,
                            },
                        }
                        for tool in tool_calls
                    ],
                }
                messages.append(assistant_tool_message)
                for tool in tool_calls:
                    arguments = json.loads(tool.function.arguments or "{}")
                    result = self._call_tool(tool.function.name, arguments)
                    tool_counter[tool.function.name] += 1
                    ledger.append(
                        {
                            "tool": tool.function.name,
                            "arguments": arguments,
                            "result_preview": result
                            if isinstance(result, (dict, list, str, int, float, bool, type(None)))
                            else str(result),
                        }
                    )
                    messages.append(
                        {
                            "role": "tool",
                            "tool_call_id": tool.id,
                            "content": json.dumps(result, ensure_ascii=False),
                        }
                    )
                continue

            final_content = message.content or "{}"
            try:
                parsed = json.loads(final_content)
            except json.JSONDecodeError:
                parsed = {
                    "case_id": case_packet.get("case_id", ""),
                    "claim_level_decision": "review",
                    "claim_level_reasoning": final_content,
                    "needs_human_review": True,
                }
            return {
                "result": parsed,
                "tool_ledger": ledger,
                "usage": usage_totals,
                "llm_call_count": llm_call_count,
                "tool_call_count": int(sum(tool_counter.values())),
                "tool_call_breakdown": dict(sorted(tool_counter.items())),
                "duration_ms": round((time.perf_counter() - started_at) * 1000, 1),
                "disease_workspace_hint": disease_key,
            }
        raise RuntimeError("Agent vuot qua so turn toi da.")
