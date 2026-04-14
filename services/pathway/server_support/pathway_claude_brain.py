from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Optional

from server_support.claude_bridge import bridge
from server_support.claude_runtime import DEFAULT_MODEL, ClaudeCliError, ClaudeCliRuntime, env_int
from server_support.ontology_v2_inspector_store import DEFAULT_NAMESPACE, OntologyV2InspectorStore
from server_support.paths import DATATEST_CASES_DIR
from server_support.testcase_trace_runner import list_available_testcase_jsons


BRAIN_SCHEMA_VERSION = "pathway_claude_brain.v1"
DEFAULT_BRAIN_SYSTEM_PROMPT = (
    "Ban la bo nao Claude Code cho Pathway. "
    "Nhiem vu cua ban la giup Pathway hieu dung nhiem vu, lap ke hoach truy xuat tri thuc, "
    "nhin ro ontology dang co gi, hieu testcase neu co, va CHI duoc ket luan tu bang chung duoc cung cap. "
    "Neu bang chung khong du, phai noi ro khong du bang chung va de xuat cac task truy xuat tiep theo. "
    "Khong duoc noi giong nhu da biet khi he thong chua lay du tri thuc. "
    "Tra ve DUY NHAT 1 JSON object hop le, khong markdown, khong code fence, khong giai thich ngoai JSON."
)
MAX_BRAIN_REPAIRS = env_int("PATHWAY_CLAUDE_BRAIN_MAX_REPAIRS", 1)
MAX_CASE_FILES = env_int("PATHWAY_CLAUDE_BRAIN_MAX_CASE_FILES", 12)
MAX_CONTEXT_ITEMS = env_int("PATHWAY_CLAUDE_BRAIN_MAX_CONTEXT_ITEMS", 10)


def _strip_code_fences(raw_text: str) -> str:
    stripped = raw_text.strip()
    if not stripped.startswith("```"):
        return stripped

    lines = stripped.splitlines()
    if lines and lines[0].strip().startswith("```"):
        lines = lines[1:]
    if lines and lines[-1].strip() == "```":
        lines = lines[:-1]
    return "\n".join(lines).strip()


def _extract_json_payload(raw_text: str) -> Dict[str, Any]:
    stripped = _strip_code_fences(raw_text)
    decoder = json.JSONDecoder()

    for candidate in (stripped, stripped.removeprefix("json").lstrip()):
        if not candidate:
            continue
        try:
            payload = json.loads(candidate)
        except json.JSONDecodeError:
            payload = None
        if isinstance(payload, dict):
            return payload

    for index, char in enumerate(stripped):
        if char != "{":
            continue
        try:
            payload, _ = decoder.raw_decode(stripped[index:])
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict):
            return payload

    raise ClaudeCliError("Claude Brain tra ve noi dung khong parse duoc thanh JSON object.")


def _require_text(value: Any, field_name: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise ClaudeCliError(f"Thieu truong bat buoc: {field_name}")
    return text


def _normalize_string_list(value: Any) -> List[str]:
    if value is None:
        return []
    if isinstance(value, str):
        text = value.strip()
        return [text] if text else []
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    raise ClaudeCliError("Gia tri phai la chuoi hoac danh sach chuoi.")


def _normalize_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in {"true", "1", "yes", "y"}:
        return True
    if text in {"false", "0", "no", "n"}:
        return False
    return default


def _normalize_confidence(value: Any) -> str:
    text = str(value or "medium").strip().lower()
    return text if text in {"low", "medium", "high"} else "medium"


def _brain_example() -> str:
    return json.dumps(
        {
            "schema_version": BRAIN_SCHEMA_VERSION,
            "request_type": "clinical_qna",
            "mission": "Tra loi cau hoi lam sang dua tren tri thuc Pathway dang co.",
            "understanding": "Nguoi dung can cau tra loi co bang chung, khong suy dien vo can cu.",
            "grounded": True,
            "confidence": "medium",
            "used_evidence_ids": ["chunk:abc123", "assertion:def456"],
            "reasoning_plan": [
                "Xac dinh cau hoi dang hoi ve benh gi, muc tieu gi.",
                "Doi chieu evidence da truy xuat voi ontology va assertion lien quan.",
                "Chi ket luan nhung diem duoc evidence ho tro truc tiep.",
            ],
            "task_plan": [
                "Task 1: Xac dinh mission va pham vi cau hoi.",
                "Task 2: Kiem tra evidence va ontology snapshot.",
                "Task 3: Tra loi hoac tra ve khoang trong tri thuc.",
            ],
            "knowledge_access_plan": [
                "Neu thieu bang chung, truy them RawChunk va ProtocolAssertion trong namespace ontology_v2.",
                "Neu cau hoi la testcase, doc file testcase roi moi ket luan.",
            ],
            "ontology_understanding": [
                "Ontology hien luu DiseaseEntity, RawChunk, RawSignMention, RawServiceMention, RawObservationMention, ProtocolAssertion, ProtocolDiseaseSummary.",
            ],
            "testcase_understanding": [
                "Neu can testcase, uu tien doc data_test_*.json hoac kich_ban_*.json lien quan.",
            ],
            "caveats": [
                "Khong duoc dung tri thuc ben ngoai packet de khang dinh mot ket luan khong co trong evidence.",
            ],
            "answer": "Cau tra loi co co so tu evidence da truy xuat.",
        },
        ensure_ascii=False,
        indent=2,
    )


def _normalize_brain_payload(raw_payload: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(raw_payload, dict):
        raise ClaudeCliError("Claude Brain phai tra ve mot JSON object.")

    normalized = {
        "schema_version": BRAIN_SCHEMA_VERSION,
        "request_type": _require_text(raw_payload.get("request_type") or "clinical_qna", "request_type"),
        "mission": _require_text(raw_payload.get("mission"), "mission"),
        "understanding": _require_text(raw_payload.get("understanding"), "understanding"),
        "grounded": _normalize_bool(raw_payload.get("grounded"), False),
        "confidence": _normalize_confidence(raw_payload.get("confidence")),
        "used_evidence_ids": _normalize_string_list(raw_payload.get("used_evidence_ids")),
        "reasoning_plan": _normalize_string_list(raw_payload.get("reasoning_plan")),
        "task_plan": _normalize_string_list(raw_payload.get("task_plan")),
        "knowledge_access_plan": _normalize_string_list(raw_payload.get("knowledge_access_plan")),
        "ontology_understanding": _normalize_string_list(raw_payload.get("ontology_understanding")),
        "testcase_understanding": _normalize_string_list(raw_payload.get("testcase_understanding")),
        "caveats": _normalize_string_list(raw_payload.get("caveats")),
        "answer": _require_text(raw_payload.get("answer"), "answer"),
    }
    return normalized


class PathwayClaudeBrainRunner(ClaudeCliRuntime):
    def __init__(self, ontology_store: Optional[OntologyV2InspectorStore] = None, binary: str = "claude") -> None:
        super().__init__(binary=binary)
        self.ontology_store = ontology_store or OntologyV2InspectorStore()

    def _history_summary(self, history: Optional[List[Dict[str, str]]]) -> List[Dict[str, str]]:
        if not history:
            return []
        rows: List[Dict[str, str]] = []
        for item in history[-3:]:
            rows.append(
                {
                    "question": str(item.get("q") or "").strip()[:300],
                    "answer_preview": str(item.get("a") or "").strip()[:300],
                }
            )
        return rows

    def _context_summary(self, context_nodes: Optional[List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        for node in (context_nodes or [])[:MAX_CONTEXT_ITEMS]:
            node_id = node.get("block_id") or node.get("id")
            rows.append(
                {
                    "node_id": f"chunk:{node_id}" if node_id and not str(node_id).startswith(("chunk:", "assertion:")) else node_id,
                    "title": node.get("title"),
                    "disease_name": node.get("disease_name"),
                    "source": node.get("source"),
                    "section_path": node.get("section_path"),
                    "preview": (node.get("expanded_description") or node.get("description") or "")[:500],
                }
            )
        return rows

    def _datatest_snapshot(self) -> Dict[str, Any]:
        cases: List[Dict[str, Any]] = []
        if DATATEST_CASES_DIR.exists():
            for path in sorted(DATATEST_CASES_DIR.glob("*.json"))[:MAX_CASE_FILES]:
                cases.append(
                    {
                        "name": path.name,
                        "relative_path": str(path),
                        "size_bytes": path.stat().st_size,
                    }
                )
        return {
            "available": DATATEST_CASES_DIR.exists(),
            "folder": str(DATATEST_CASES_DIR),
            "files": cases,
        }

    def _ontology_snapshot(self, disease_name: Optional[str]) -> Dict[str, Any]:
        try:
            bootstrap = self.ontology_store.bootstrap()
        except Exception as exc:
            return {
                "available": False,
                "error": str(exc),
                "namespace": DEFAULT_NAMESPACE,
            }

        payload: Dict[str, Any] = {
            "available": True,
            "namespace": bootstrap.get("active_namespace") or DEFAULT_NAMESPACE,
            "summary": bootstrap.get("summary") or {},
            "namespaces": (bootstrap.get("namespaces") or [])[:5],
            "sample_diseases": (bootstrap.get("diseases") or [])[:8],
        }

        if not disease_name:
            return payload

        diseases = bootstrap.get("diseases") or []
        selected = None
        disease_name_lower = disease_name.lower()
        for item in diseases:
            item_name = str(item.get("disease_name") or "").lower()
            item_id = str(item.get("disease_id") or "").lower()
            if disease_name_lower in item_name or disease_name_lower in item_id:
                selected = item
                break

        if not selected:
            return payload

        try:
            graph = self.ontology_store.disease_graph(payload["namespace"], selected["disease_id"])
        except Exception as exc:
            payload["focused_disease_error"] = str(exc)
            return payload

        payload["focused_disease"] = {
            "disease_id": graph.get("disease_id"),
            "disease_name": graph.get("disease_name"),
            "summary_text": ((graph.get("summary") or {}).get("summary_text") or "")[:800],
            "key_signs": ((graph.get("summary") or {}).get("key_signs") or [])[:10],
            "key_services": ((graph.get("summary") or {}).get("key_services") or [])[:10],
            "chunk_titles": [item.get("section_title") for item in (graph.get("chunks") or [])[:8]],
            "sign_mentions": [item.get("mention_text") for item in (graph.get("sign_mentions") or [])[:8]],
            "service_mentions": [item.get("mention_text") for item in (graph.get("service_mentions") or [])[:8]],
            "observation_mentions": [item.get("mention_text") for item in (graph.get("observation_mentions") or [])[:8]],
            "assertions": [item.get("assertion_text") for item in (graph.get("assertions") or [])[:8]],
        }
        return payload

    def _build_request_packet(
        self,
        *,
        question: str,
        disease_name: Optional[str],
        context_nodes: List[Dict[str, Any]],
        history: Optional[List[Dict[str, str]]],
    ) -> Dict[str, Any]:
        ontology_snapshot = self._ontology_snapshot(disease_name)
        testcase_snapshot = {
            "trace_json_files": list_available_testcase_jsons()[:8],
            "datatest_cases": self._datatest_snapshot(),
        }
        evidence = {
            "retrieved_context": self._context_summary(context_nodes),
            "ontology_snapshot": ontology_snapshot,
            "testcase_snapshot": testcase_snapshot,
        }
        state = {
            "question": question,
            "disease_name": disease_name,
            "history_summary": self._history_summary(history),
            "retrieved_context_count": len(context_nodes or []),
        }
        response_contract = {
            "schema_version": BRAIN_SCHEMA_VERSION,
            "required_fields": [
                "request_type",
                "mission",
                "understanding",
                "grounded",
                "confidence",
                "reasoning_plan",
                "task_plan",
                "knowledge_access_plan",
                "answer",
            ],
            "rules": [
                "Khong ket luan neu evidence khong du.",
                "Neu grounded=false thi answer phai noi ro khoang trong tri thuc va next actions.",
                "Task plan phai cho thay Pathway can lam gi de lay tri thuc.",
                "Ontology understanding phai noi ro ontology hien luu gi neu packet co thong tin ve ontology.",
                "Neu testcase lien quan, testcase understanding phai noi ro can doc file nao va doc de lam gi.",
            ],
        }
        return bridge.build_request_packet(
            mode="ask_claude_brain",
            objective=question,
            context=(
                "Day la mot luot hoi dap Pathway. Hay bien Pathway thanh he thong co kha nang "
                "hieu nhiem vu, lap ke hoach, va chi ket luan tu tri thuc dang co."
            ),
            state=state,
            evidence=evidence,
            response_contract=response_contract,
            metadata={
                "disease_name": disease_name,
                "context_count": len(context_nodes or []),
            },
        )

    def _build_prompt(
        self,
        *,
        question: str,
        request_packet: Dict[str, Any],
        context_nodes: List[Dict[str, Any]],
        disease_name: Optional[str],
    ) -> str:
        bridge_context = bridge.render_request_packet(request_packet)
        evidence_digest = self._render_evidence_digest(
            context_nodes=context_nodes,
            disease_name=disease_name,
        )
        return "\n\n".join(
            [
                f"Cau hoi nguoi dung:\n{question}",
                bridge_context,
                evidence_digest,
                (
                    "Yeu cau bat buoc:\n"
                    "- Truoc khi tra loi, phai hieu ro mission, loai request, va Pathway can truy xuat tri thuc tu dau.\n"
                    "- Neu packet cho thay tri thuc hien co khong du, phai noi ro khong du bang chung.\n"
                    "- Khong duoc dung tri thuc ben ngoai packet de khang dinh mot dieu packet chua ho tro.\n"
                    "- reasoning_plan phai mo ta cach suy luan grounded.\n"
                    "- task_plan phai cho thay cac task Pathway can tao de giai bai toan.\n"
                    "- knowledge_access_plan phai chi ro can lay tri thuc tu ontology, testcase, hoac chunk/assertion nao.\n"
                    "- Chi tra ve DUY NHAT 1 JSON object hop le.\n"
                    f"- schema_version phai la {BRAIN_SCHEMA_VERSION}.\n"
                    "- JSON mau tham khao:\n"
                    f"{_brain_example()}"
                ),
            ]
        )

    def _render_evidence_digest(
        self,
        *,
        context_nodes: List[Dict[str, Any]],
        disease_name: Optional[str],
    ) -> str:
        lines = ["Evidence digest thuc te (uu tien hon suy doan tong quat):"]

        context_rows = self._context_summary(context_nodes)
        if context_rows:
            lines.append("- Retrieved context:")
            for row in context_rows[:6]:
                lines.append(
                    f"  - {row.get('node_id')}: {row.get('title') or '-'} | "
                    f"disease={row.get('disease_name') or '-'} | preview={row.get('preview') or '-'}"
                )
        else:
            lines.append("- Retrieved context: (khong co chunk nao duoc truy xuat)")

        ontology = self._ontology_snapshot(disease_name)
        if ontology.get("available"):
            summary = ontology.get("summary") or {}
            lines.append(
                "- Ontology snapshot: "
                f"namespace={ontology.get('namespace')} | "
                f"diseases={summary.get('diseases', 0)} | "
                f"chunks={summary.get('chunks', 0)} | "
                f"assertions={summary.get('assertions', 0)} | "
                f"sign_mentions={summary.get('sign_mentions', 0)} | "
                f"service_mentions={summary.get('service_mentions', 0)}"
            )
            sample_names = [str(item.get("disease_name") or "") for item in (ontology.get("sample_diseases") or [])[:8]]
            if sample_names:
                lines.append(f"- Ontology sample diseases: {', '.join(name for name in sample_names if name)}")
            focused = ontology.get("focused_disease") or {}
            if focused:
                lines.append(
                    f"- Focused disease: {focused.get('disease_name')} ({focused.get('disease_id')})"
                )
                if focused.get("summary_text"):
                    lines.append(f"  - Summary text: {focused.get('summary_text')}")
                if focused.get("chunk_titles"):
                    lines.append(f"  - Chunk titles: {', '.join(str(item) for item in focused.get('chunk_titles')[:6] if item)}")
                if focused.get("key_services"):
                    lines.append(f"  - Key services: {', '.join(str(item) for item in focused.get('key_services')[:8] if item)}")
                if focused.get("assertions"):
                    lines.append(f"  - Assertions: {' | '.join(str(item) for item in focused.get('assertions')[:5] if item)}")
        else:
            lines.append(f"- Ontology snapshot unavailable: {ontology.get('error') or 'unknown error'}")

        datatest = self._datatest_snapshot()
        if datatest.get("files"):
            lines.append(
                "- Datatest cases: "
                + ", ".join(str(item.get("name")) for item in datatest.get("files", [])[:8] if item.get("name"))
            )
        trace_files = list_available_testcase_jsons()[:8]
        if trace_files:
            lines.append(
                "- Trace testcase files: "
                + ", ".join(str(item.get("name")) for item in trace_files if item.get("name"))
            )

        return "\n".join(lines)

    def _build_repair_prompt(self, *, base_prompt: str, raw_content: str, format_error: str) -> str:
        return "\n\n".join(
            [
                base_prompt,
                "Noi dung vua tra ve KHONG hop le. Sua lai ngay theo dung schema JSON.",
                f"Loi dinh dang: {format_error}",
                f"Noi dung khong hop le:\n{raw_content}",
                "Nhac lai: chi tra ve 1 JSON object hop le, khong markdown, khong code fence, khong van ban ben ngoai JSON.",
            ]
        )

    def run_ask(
        self,
        *,
        question: str,
        disease_name: Optional[str],
        context_nodes: List[Dict[str, Any]],
        history: Optional[List[Dict[str, str]]] = None,
        model: Optional[str] = None,
        timeout: int = 240,
    ) -> Dict[str, Any]:
        request_packet = self._build_request_packet(
            question=question,
            disease_name=disease_name,
            context_nodes=context_nodes,
            history=history,
        )
        prompt = self._build_prompt(
            question=question,
            request_packet=request_packet,
            context_nodes=context_nodes,
            disease_name=disease_name,
        )
        current_prompt = prompt
        raw_content = ""
        repair_attempts = 0

        while True:
            result = self.invoke(
                current_prompt,
                system_prompt=DEFAULT_BRAIN_SYSTEM_PROMPT,
                model=model or DEFAULT_MODEL,
                timeout=timeout,
                tools="",
            )
            raw_content = result["content"]

            try:
                structured = _normalize_brain_payload(_extract_json_payload(raw_content))
                break
            except ClaudeCliError as exc:
                if repair_attempts >= MAX_BRAIN_REPAIRS:
                    raise
                repair_attempts += 1
                current_prompt = self._build_repair_prompt(
                    base_prompt=prompt,
                    raw_content=raw_content,
                    format_error=str(exc),
                )

        claude_status = self.status()
        bridge_trace = bridge.persist_interaction(
            mode="ask_claude_brain",
            request_packet=request_packet,
            response_payload=structured,
            prompt_preview=prompt,
            system_prompt_preview=DEFAULT_BRAIN_SYSTEM_PROMPT,
            model=model or DEFAULT_MODEL,
            claude_status=claude_status,
            tags={
                "request_type": structured.get("request_type"),
                "grounded": structured.get("grounded"),
            },
        )

        trace_steps = [
            {
                "phase": "Claude Brain mission",
                "detail": {
                    "request_type": structured.get("request_type"),
                    "mission": structured.get("mission"),
                    "understanding": structured.get("understanding"),
                },
                "ms": result.get("duration_ms"),
            },
            {
                "phase": "Claude Brain plan",
                "detail": {
                    "reasoning_plan": structured.get("reasoning_plan"),
                    "task_plan": structured.get("task_plan"),
                    "knowledge_access_plan": structured.get("knowledge_access_plan"),
                },
            },
            {
                "phase": "Claude Brain groundedness",
                "detail": {
                    "grounded": structured.get("grounded"),
                    "confidence": structured.get("confidence"),
                    "used_evidence_ids": structured.get("used_evidence_ids"),
                    "caveats": structured.get("caveats"),
                },
            },
        ]

        verification = {
            "engine": "claude_brain",
            "grounded": structured.get("grounded"),
            "confidence": structured.get("confidence"),
            "used_evidence_ids": structured.get("used_evidence_ids"),
            "reasoning_plan": structured.get("reasoning_plan"),
            "task_plan": structured.get("task_plan"),
            "knowledge_access_plan": structured.get("knowledge_access_plan"),
            "ontology_understanding": structured.get("ontology_understanding"),
            "testcase_understanding": structured.get("testcase_understanding"),
            "caveats": structured.get("caveats"),
        }

        return {
            "answer": structured.get("answer"),
            "structured": structured,
            "trace": {"steps": trace_steps},
            "verification": verification,
            "bridge_trace": bridge_trace,
            "claude_status": claude_status,
            "repair_attempts": repair_attempts,
        }
