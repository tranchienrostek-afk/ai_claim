import asyncio
import json
import logging
import queue
import shutil
import subprocess
import sys
import threading
import unicodedata
from contextlib import redirect_stderr, redirect_stdout
from datetime import datetime
from pathlib import Path
from typing import List, Optional

from fastapi import FastAPI, File, Form, HTTPException, UploadFile, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.requests import Request

from academic_agent import AcademicAgent
from server_support.api_models import (
    AdjudicationItem,
    AskResponse,
    ClaimAdjudicateRequest,
    ClaimAdjudicateResponse,
    ClaudeDecisionGateRequest,
    ClaudeDecisionGateResponse,
    ClaudeDecisionGateResult,
    ClaudeDuetStructuredTurn,
    ClaudeDuetRequest,
    ClaudeDuetResponse,
    ClaudeDuetTurn,
    PipelineDecisionControlRequest,
    PipelineDecisionControlResponse,
    CrawlRequest,
    QuestionRequest,
    ReasoningTraceEntry,
    Source,
    TraceStep,
)
from server_support.adjudication.models import (
    MedicalReasoningLineResult,
    MedicalReasoningRequest,
    MedicalReasoningResponse,
    MultiAgentAdjudicateRequest,
    MultiAgentAdjudicateResponse,
)
from server_support.claude_bridge import get_bridge_status, record_bridge_feedback
from server_support.claude_decision import ClaudeDecisionGateRunner
from server_support.claude_duet import ClaudeCliError, ClaudeCliUnavailableError, ClaudeDuetRunner
from server_support.pathway_claude_brain import PathwayClaudeBrainRunner
from server_support.claude_workspace_memory import refresh_runtime_memory, runtime_memory_status
from server_support.claims_insights_graph_store import ClaimsInsightsGraphStore
from server_support.ontology_v2_inspector_store import DEFAULT_NAMESPACE, OntologyV2InspectorStore
from server_support.pathway_data_architecture import PathwayDataArchitectureStore
from server_support.pathway_knowledge_excel import PathwayKnowledgeExcelBridge
from server_support.pathway_knowledge_text import PathwayKnowledgeTextBridge
from server_support.pathway_graph_operating import PathwayGraphOperatingStore
from server_support.pathway_knowledge_registry import PathwayKnowledgeRegistryStore
from server_support.paths import (
    BASE_DIR,
    CRAWLER_SCRIPT,
    LEGACY_CRAWLER_SCRIPT,
    LOGS_DIR,
    MEDICAL_PIPELINE_SCRIPTS_DIR,
    ROOT_DIR,
    RUNS_DIR,
    TEMPLATES_DIR,
    UPLOADS_DIR,
    KNOWLEDGE_EXCEL_VIEWS_DIR,
)
from server_support.pdf_catalog import DiseasePdfCatalog
from server_support.pipeline_store import PipelineLogCapture, PipelineRunStore
from server_support.session_store import SessionStore


LOGS_DIR.mkdir(parents=True, exist_ok=True)
logging.basicConfig(
    filename=str(LOGS_DIR / "server.log"),
    level=logging.DEBUG,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("api_server")


app = FastAPI(title="Antigravity Clinical Knowledge Engine")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)
app.mount("/pdfs", StaticFiles(directory=str(BASE_DIR)), name="pdfs")
app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")

templates = Jinja2Templates(directory=str(TEMPLATES_DIR))
pipeline_store = PipelineRunStore(RUNS_DIR)
session_store = SessionStore(max_turns=20)
pdf_catalog = DiseasePdfCatalog(BASE_DIR)
claims_insights_graph_store = ClaimsInsightsGraphStore(
    BASE_DIR / "workspaces" / "claims_insights" / "demo" / "disease_graph_explorer_data.json"
)
ontology_v2_inspector_store = OntologyV2InspectorStore()
pathway_data_architecture_store = PathwayDataArchitectureStore(ontology_v2_inspector_store)
graph_operating_store = PathwayGraphOperatingStore(
    ontology_v2_inspector_store,
    claims_insights_graph_store,
    pathway_data_architecture_store,
)
knowledge_registry_store = PathwayKnowledgeRegistryStore(ontology_v2_inspector_store)
knowledge_text_bridge = PathwayKnowledgeTextBridge(ontology_v2_inspector_store, knowledge_registry_store)
knowledge_excel_bridge = PathwayKnowledgeExcelBridge(
    ontology_v2_inspector_store,
    knowledge_registry_store,
    knowledge_text_bridge=knowledge_text_bridge,
)
claude_duet_runner = ClaudeDuetRunner()
claude_decision_runner = ClaudeDecisionGateRunner()
pathway_claude_brain_runner = PathwayClaudeBrainRunner(ontology_v2_inspector_store)
FINAL_RUN_STATUSES = {
    "completed",
    "error",
    "paused_for_human_review",
    "aborted_by_decision_gate",
    "aborted_by_human_review",
}

agent: Optional[AcademicAgent] = None


def _get_agent() -> AcademicAgent:
    global agent
    if agent is None:
        print("[AGENT] Lazy initialization triggered...")
        agent = AcademicAgent()
    return agent


def _normalize_page(raw_page) -> int:
    try:
        return int(raw_page)
    except (TypeError, ValueError):
        return 1


def _ascii_fold(text: Optional[str]) -> str:
    raw = str(text or "").lower().replace("đ", "d").replace("Đ", "d")
    return "".join(ch for ch in unicodedata.normalize("NFKD", raw) if not unicodedata.combining(ch))


def _parse_iso_dt(raw_value: Optional[str]) -> Optional[datetime]:
    if not raw_value:
        return None
    try:
        parsed = datetime.fromisoformat(raw_value)
    except ValueError:
        return None
    if parsed.tzinfo is not None:
        parsed = parsed.replace(tzinfo=None)
    return parsed


def _is_running_run_stale(run: dict, now: Optional[datetime] = None) -> bool:
    if run.get("status") != "running":
        return False
    now = now or datetime.now()
    last_seen = _parse_iso_dt(run.get("start_time"))
    for entry in run.get("logs", [])[-5:]:
        parsed = _parse_iso_dt(entry.get("timestamp"))
        if parsed and (last_seen is None or parsed > last_seen):
            last_seen = parsed
    if last_seen is None:
        return True
    if (now - last_seen).total_seconds() >= 5 * 60:
        return True
    started_at = _parse_iso_dt(run.get("start_time"))
    if started_at and (now - started_at).total_seconds() >= 6 * 3600:
        return True
    return False


def _build_sources(context_nodes: list, fallback_disease_name: Optional[str] = None) -> List[Source]:
    ag = _get_agent()
    sources: List[Source] = []
    for node in context_nodes or []:
        node_disease = node.get("disease_name") or fallback_disease_name or ""
        sources.append(
            Source(
                title=node["title"],
                url=node.get("url", ""),
                source=node.get("source", "BYT Protocol"),
                page=_normalize_page(node.get("page_number", 1)),
                pdf_file=pdf_catalog.find_pdf_for_disease(node_disease, ag),
                disease_name=node.get("disease_name"),
                source_type=node.get("source_type"),
                hospital_name=node.get("hospital_name"),
                node_id=node.get("block_id") or node.get("id"),
            )
        )
    return sources


def _build_trace_steps(raw_trace: dict) -> List[TraceStep]:
    trace_steps: List[TraceStep] = []
    for step in raw_trace.get("steps", []):
        detail = step.get("detail")
        if isinstance(detail, str):
            detail = {"info": detail}
        trace_steps.append(
            TraceStep(
                step=step.get("phase", step.get("step", "")),
                detail=detail,
                duration_ms=step.get("ms", step.get("duration_ms")),
            )
        )
    return trace_steps


def _build_agentic_history(history: list) -> list:
    agentic_history = []
    for idx in range(0, len(history) - 1, 2):
        if history[idx].get("role") == "user" and idx + 1 < len(history):
            agentic_history.append(
                {
                    "q": history[idx]["content"],
                    "a": history[idx + 1].get("content", ""),
                }
            )
    return agentic_history


def _resolve_disease_name_for_request(ag: AcademicAgent, question: str, explicit_disease: Optional[str] = None) -> Optional[str]:
    disease_name = explicit_disease or ag.resolve_disease_name(question)
    if disease_name:
        return disease_name

    try:
        bootstrap = ontology_v2_inspector_store.bootstrap()
    except Exception:
        return None

    question_lower = (question or "").lower()
    question_folded = _ascii_fold(question)
    for item in bootstrap.get("diseases") or []:
        name = str(item.get("disease_name") or "").strip()
        if name and (name.lower() in question_lower or _ascii_fold(name) in question_folded):
            return name
    return None


def _claims_insights_bundle_bootstrap() -> dict:
    bundle = claims_insights_graph_store.load_bundle()
    return {
        "source": "bundle-fallback",
        "stats": bundle.get("stats", {}),
        "disease_index": bundle.get("disease_index", []),
    }


def _claims_insights_bundle_graph(disease_id: str) -> Optional[dict]:
    bundle = claims_insights_graph_store.load_bundle()
    graph = (bundle.get("graphs") or {}).get(disease_id)
    return graph


def _ensure_orchestrator_import_path() -> None:
    path_str = str(MEDICAL_PIPELINE_SCRIPTS_DIR)
    if path_str not in sys.path:
        sys.path.insert(0, path_str)


def _make_run_id(filename: str) -> str:
    stem = Path(filename).stem.replace(" ", "_")
    return f"{stem}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"


def _reserve_upload_path(filename: str) -> Path:
    UPLOADS_DIR.mkdir(exist_ok=True)
    original = Path(filename or "upload.pdf")
    stem = original.stem or "upload"
    suffix = original.suffix or ".pdf"
    candidate = UPLOADS_DIR / f"{stem}{suffix}"
    if not candidate.exists():
        return candidate

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    candidate = UPLOADS_DIR / f"{stem}_{timestamp}{suffix}"
    counter = 1
    while candidate.exists():
        candidate = UPLOADS_DIR / f"{stem}_{timestamp}_{counter}{suffix}"
        counter += 1
    return candidate


def _create_pipeline_run(
    run_id: str,
    filename: str,
    hospital_name: Optional[str] = None,
    **extra_fields,
) -> None:
    pipeline_store.create_run(
        run_id,
        {
            "id": run_id,
            "status": "running",
            "filename": filename,
            "start_time": datetime.now().isoformat(),
            "hospital_name": hospital_name,
            "logs": [],
            **extra_fields,
        },
    )


def _infer_run_kind(run: dict) -> str:
    explicit = (run.get("kind") or "").strip()
    if explicit:
        return explicit

    run_id = str(run.get("id") or "")
    filename = str(run.get("filename") or "")
    result = run.get("result") or {}

    if run_id.startswith("ontology_v2_") or result.get("namespace") or result.get("disease_name"):
        return "ontology_ingest"
    if run_id.startswith("testcase_trace_") or result.get("aggregate") or result.get("graph_namespace"):
        return "testcase_trace"
    if run_id.startswith("crawl_") or filename.lower().startswith("crawl:"):
        return "crawl"
    return "pipeline"


def _summarize_run_for_dashboard(run: dict) -> dict:
    result = run.get("result") or {}
    summary = result.get("summary") or {}
    aggregate = result.get("aggregate") or {}
    kind = _infer_run_kind(run)
    namespace = (
        result.get("namespace")
        or result.get("graph_namespace")
        or run.get("namespace")
        or run.get("graph_namespace")
    )
    disease_name = result.get("disease_name") or run.get("disease_name")
    if kind == "ontology_ingest":
        namespace = namespace or DEFAULT_NAMESPACE
        if not disease_name:
            raw_name = str(run.get("filename") or run.get("id") or "")
            disease_name = Path(raw_name).stem or None
    decision_gate = _extract_decision_gate_state(run)
    return {
        "id": run.get("id"),
        "name": run.get("filename", "Unknown"),
        "status": run.get("status", "unknown"),
        "is_stale": _is_running_run_stale(run),
        "start_time": run.get("start_time"),
        "kind": kind,
        "namespace": namespace,
        "disease_name": disease_name,
        "summary": {
            "accuracy": summary.get("accuracy") or summary.get("final_accuracy") or "0%",
            "total_questions": summary.get("total_questions")
            or summary.get("total_questions_tested")
            or 0,
            "service_label_accuracy": aggregate.get("service_label_accuracy"),
            "case_count": aggregate.get("case_count"),
            "service_line_count": aggregate.get("service_line_count"),
        },
        "decision_gate": decision_gate,
    }


def _safe_ontology_bootstrap(namespace: Optional[str] = None) -> dict:
    try:
        return ontology_v2_inspector_store.bootstrap(pdf_catalog.list_pdfs(), namespace=namespace)
    except Exception as exc:
        logger.warning("Ontology V2 bootstrap fallback: %s", exc)
        active_namespace = namespace or DEFAULT_NAMESPACE
        return {
            "source": "neo4j-unavailable",
            "error": str(exc),
            "neo4j_uri": "",
            "namespaces": [{"namespace": active_namespace, "disease_count": 0, "chunk_count": 0}],
            "default_namespace": active_namespace,
            "active_namespace": active_namespace,
            "summary": {
                "diseases": 0,
                "chunks": 0,
                "sign_mentions": 0,
                "service_mentions": 0,
                "observation_mentions": 0,
                "assertions": 0,
                "summaries": 0,
            },
            "diseases": [],
            "pdfs": pdf_catalog.list_pdfs(),
        }


def _safe_data_architecture_bootstrap() -> dict:
    try:
        return pathway_data_architecture_store.bootstrap()
    except Exception as exc:
        logger.warning("Pathway data architecture bootstrap fallback: %s", exc)
        return {
            "schema_version": "pathway_data_architecture.v1",
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "mission": "",
            "principles": [],
            "logical_layers": [],
            "question_families": [],
            "operating_contract": {},
            "summary": {
                "domain_count": 0,
                "surface_count": 0,
                "existing_surface_count": 0,
                "missing_required_count": 0,
                "warning_count": 1,
                "ontology_namespace_count": 0,
            },
            "domains": [],
            "warnings": [str(exc)],
            "documentation": {},
            "storage_roots": {
                "base_dir": str(BASE_DIR),
            },
            "ontology_context": {
                "available": False,
                "error": str(exc),
                "active_namespace": None,
                "namespaces": [],
            },
        }


def _safe_graph_operating_bootstrap() -> dict:
    try:
        return graph_operating_store.bootstrap()
    except Exception as exc:
        logger.warning("Graph operating bootstrap fallback: %s", exc)
        return {
            "source": "neo4j-unavailable",
            "mission": "",
            "capabilities": [],
            "domains": {},
            "operating_contract": {},
            "error": str(exc),
        }


def _safe_knowledge_registry_bootstrap(refresh: bool = False) -> dict:
    try:
        return knowledge_registry_store.bootstrap(refresh=refresh)
    except Exception as exc:
        logger.warning("Knowledge registry bootstrap fallback: %s", exc)
        return {
            "schema_version": "pathway_knowledge_registry.v1",
            "mission": "",
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "last_sync_at": None,
            "managed_roots": [],
            "summary": {
                "asset_count": 0,
                "domain_count": 0,
                "kind_count": 0,
                "ready_for_ingest_count": 0,
                "graph_ready_count": 0,
                "needs_review_count": 0,
            },
            "counts": {"domains": {}, "kinds": {}},
            "recent_assets": [],
            "last_sync_summary": {"error": str(exc)},
        }


def _start_knowledge_asset_ingest(
    asset_id: str,
    *,
    namespace: str = DEFAULT_NAMESPACE,
    split_strategy: str = "auto",
    preferred_disease_name: Optional[str] = None,
    source_type: Optional[str] = None,
) -> dict:
    asset = knowledge_registry_store.get_asset(asset_id)
    if asset is None:
        raise KeyError(f"Knowledge asset not found: {asset_id}")

    filename_for_run = asset.get("title") or Path(asset.get("source_path") or asset_id).name
    run_id = _make_run_id(f"knowledge_{filename_for_run}")
    _create_pipeline_run(
        run_id,
        filename_for_run,
        kind="knowledge_ingest",
        asset_id=asset_id,
        knowledge_domain=asset.get("domain"),
        knowledge_kind=asset.get("kind"),
        namespace=namespace,
    )
    knowledge_registry_store.mark_ingest_started(asset_id, run_id)

    def run_pipeline_bg():
        capture = PipelineLogCapture(run_id, sys.stdout, pipeline_store)
        try:
            with redirect_stdout(capture), redirect_stderr(capture):
                print(
                    f"[KNOWLEDGE] Starting ingest asset_id={asset_id} kind={asset.get('kind')} "
                    f"namespace={namespace} split={split_strategy}"
                )
                result = knowledge_registry_store.ingest_asset(
                    asset_id,
                    namespace=namespace,
                    split_strategy=split_strategy,
                    preferred_disease_name=preferred_disease_name,
                    source_type=source_type,
                )
                knowledge_text_bridge.apply_manual_labels_to_graph(asset_id)
                result["asset_id"] = asset_id
                result["namespace"] = namespace
                pipeline_store.mark_completed(run_id, result=result)
                knowledge_registry_store.record_ingest_result(asset_id, run_id, result)
                pdf_catalog.invalidate()
                print(f"[KNOWLEDGE] Completed asset_id={asset_id}")
        except Exception as exc:
            print(f"[KNOWLEDGE] Error asset_id={asset_id}: {exc}")
            pipeline_store.mark_error(run_id, str(exc))
            knowledge_registry_store.record_ingest_error(asset_id, run_id, str(exc))

    threading.Thread(target=run_pipeline_bg, daemon=True).start()
    return {
        "run_id": run_id,
        "status": "running",
        "asset_id": asset_id,
        "namespace": namespace,
        "split_strategy": split_strategy,
    }


def _load_json_file(path: Path) -> Optional[dict]:
    try:
        with open(path, "r", encoding="utf-8") as handle:
            return json.load(handle)
    except (OSError, json.JSONDecodeError):
        return None


def _record_bridge_feedback_safe(interaction_id: Optional[str], payload: dict) -> None:
    if not interaction_id:
        return
    try:
        record_bridge_feedback(interaction_id, payload)
    except Exception as exc:
        logger.warning("Claude bridge feedback write failed for %s: %s", interaction_id, exc)


def _decision_gate_actions_for_checkpoint(checkpoint: Optional[str]) -> List[str]:
    if checkpoint == "post_design":
        return ["continue_to_ingestion", "abort_run"]
    if checkpoint == "post_test":
        return ["accept_current_result", "run_optimization", "abort_run"]
    return []


def _extract_decision_gate_state(run: dict) -> Optional[dict]:
    result = run.get("result") or {}
    events = list(result.get("decision_gate_events") or [])
    if not events:
        return None

    latest = events[-1]
    decision = latest.get("decision") or {}
    checkpoint = latest.get("checkpoint")
    status = run.get("status")
    pending_operator_action = status == "paused_for_human_review"
    human_events = list(result.get("human_decision_events") or run.get("human_decision_events") or [])
    last_human_event = human_events[-1] if human_events else None

    return {
        "checkpoint": checkpoint,
        "recommended_action": decision.get("recommended_action"),
        "confidence": decision.get("confidence"),
        "reasoning": decision.get("reasoning"),
        "risks": decision.get("risks") or [],
        "suggested_changes": decision.get("suggested_changes") or [],
        "next_owner": decision.get("next_owner"),
        "next_step": decision.get("next_step"),
        "stop_signal": decision.get("stop_signal"),
        "needs_human_review": decision.get("needs_human_review", False),
        "pending_operator_action": pending_operator_action,
        "allowed_actions": (
            _decision_gate_actions_for_checkpoint(checkpoint) if pending_operator_action else []
        ),
        "last_human_event": last_human_event,
        "bridge_trace": latest.get("bridge_trace"),
    }


def _testcase_trace_bootstrap_payload() -> dict:
    from server_support.testcase_trace_runner import list_available_testcase_jsons

    pipeline_store.load_from_disk()
    recent_runs = []
    for run in pipeline_store.iter_runs():
        if _infer_run_kind(run) != "testcase_trace":
            continue
        result = run.get("result") or {}
        aggregate = result.get("aggregate") or {}
        recent_runs.append(
            {
                "id": run.get("id"),
                "status": run.get("status"),
                "filename": run.get("filename"),
                "start_time": run.get("start_time"),
                "graph_namespace": result.get("graph_namespace") or run.get("graph_namespace") or DEFAULT_NAMESPACE,
                "aggregate": aggregate,
                "kind": "testcase_trace",
            }
        )
    recent_runs.sort(key=lambda item: item.get("start_time") or "", reverse=True)
    return {
        "json_files": list_available_testcase_jsons(),
        "recent_runs": recent_runs[:20],
        "default_graph_namespace": DEFAULT_NAMESPACE,
    }


def _platform_bootstrap_payload() -> dict:
    pipeline_store.load_from_disk()
    ontology = _safe_ontology_bootstrap()
    data_architecture = _safe_data_architecture_bootstrap()
    graph_operating = _safe_graph_operating_bootstrap()
    knowledge_registry = _safe_knowledge_registry_bootstrap()
    trace = _testcase_trace_bootstrap_payload()
    raw_runs = list(pipeline_store.iter_runs())
    runs = sorted(
        [_summarize_run_for_dashboard(run) for run in raw_runs],
        key=lambda item: item.get("start_time") or "",
        reverse=True,
    )
    now = datetime.now()
    running_runs = [run for run in raw_runs if run.get("status") == "running"]
    stale_running_count = sum(1 for run in running_runs if _is_running_run_stale(run, now=now))
    running_count = len(running_runs)
    active_running_count = max(running_count - stale_running_count, 0)
    error_count = sum(1 for item in runs if item.get("status") == "error")
    completed_count = sum(1 for item in runs if item.get("status") == "completed")
    paused_review_count = sum(1 for item in runs if item.get("status") == "paused_for_human_review")
    aborted_count = sum(1 for item in runs if item.get("status") in {"aborted_by_decision_gate", "aborted_by_human_review"})
    return {
        "platform": {
            "name": "Hệ thống Quản trị Tri thức Bảo hiểm Pathway",
            "api_port": 9600,
            "neo4j_browser_port": 7475,
            "neo4j_bolt_port": 7688,
        },
        "health": {
            "api": {"status": "healthy", "port": 9600},
            "neo4j": {
                "status": "connected" if ontology.get("source") == "neo4j" else "degraded",
                "uri": ontology.get("neo4j_uri", ""),
                "source": ontology.get("source", "unknown"),
            },
        },
        "overview": {
            "pipeline_run_count": len(runs),
            "running_count": running_count,
            "active_running_count": active_running_count,
            "stale_running_count": stale_running_count,
            "error_count": error_count,
            "completed_count": completed_count,
            "paused_review_count": paused_review_count,
            "aborted_count": aborted_count,
            "pdf_count": len(ontology.get("pdfs") or []),
            "namespace_count": len(ontology.get("namespaces") or []),
            "testcase_json_count": len(trace.get("json_files") or []),
            "recent_trace_run_count": len(trace.get("recent_runs") or []),
            "data_domain_count": (data_architecture.get("summary") or {}).get("domain_count", 0),
            "data_surface_count": (data_architecture.get("summary") or {}).get("surface_count", 0),
            "data_warning_count": (data_architecture.get("summary") or {}).get("warning_count", 0),
            "graph_operating_domain_count": len((graph_operating.get("domains") or {}).keys()),
            "knowledge_asset_count": (knowledge_registry.get("summary") or {}).get("asset_count", 0),
            "knowledge_graph_ready_count": (knowledge_registry.get("summary") or {}).get("graph_ready_count", 0),
        },
        "modules": [
            {
                "id": "overview",
                "label": "Tổng quan Nghiệp vụ",
                "description": "Báo cáo tổng hợp, tình trạng xử lý và thông số vận hành hệ thống",
                "url": "",
            },
            {
                "id": "knowledge",
                "label": "Tri thá»©c Ä‘iá»u phá»‘i",
                "description": "Quáº£n trá»‹ toÃ n bá»™ file tri thá»©c, config ingest, version vÃ  reverse trace tá»« graph vá» nguá»“n",
                "url": "/knowledge/registry",
            },
            {
                "id": "forge",
                "label": "Thiết lập Đồ thị",
                "description": "Công cụ tạo lập cấu trúc từ văn bản, phân tích tương quan và mở rộng dữ liệu",
                "url": "/ontology-v2/cinematic",
            },
            {
                "id": "ontology",
                "label": "Quản lý Bệnh lý",
                "description": "Giám sát tài liệu y khoa, phân đoạn dữ liệu và chuẩn hóa phác đồ trên Neo4j",
                "url": "/ontology-v2/inspector",
            },
            {
                "id": "trace",
                "label": "Truy vết Nghiệp vụ",
                "description": "Đối soát kịch bản, lập kế hoạch chi tiết và kiểm tra logic xử lý",
                "url": "/claims-insights/testcase-trace",
            },
            {
                "id": "explorer",
                "label": "Bản đồ Bệnh lý",
                "description": "Khám phá mối liên hệ giữa các bệnh, triệu chứng và nguồn gốc dữ liệu",
                "url": "/claims-insights/explorer",
            },
            {
                "id": "legacy",
                "label": "Hỏi đáp Truyền thống",
                "description": "Giao diện hỗ trợ cũ, dùng để đối chiếu và sử dụng song song",
                "url": "/legacy-chat",
            },
            {
                "id": "claude_duet",
                "label": "Phòng Phân tích Đối chiếu",
                "description": "Tư vấn chuyên gia đa chiều, phản biện và phân tích logic bồi thường",
                "url": "/claude-duet",
            },
            {
                "id": "mapping_audit",
                "label": "Kiểm soát Mapping",
                "description": "Đối soát và chuẩn hóa mã dịch vụ y tế, triệu chứng từ kết quả trích xuất AI",
                "url": "/claims-insights/mapping-audit",
            },
        ],
        "data_architecture": {
            "schema_version": data_architecture.get("schema_version"),
            "mission": data_architecture.get("mission"),
            "operating_contract": data_architecture.get("operating_contract") or {},
            "summary": data_architecture.get("summary") or {},
            "warnings": (data_architecture.get("warnings") or [])[:8],
            "domains": [
                {
                    "id": item.get("id"),
                    "label": item.get("label"),
                    "surface_count": item.get("surface_count", 0),
                    "existing_surface_count": item.get("existing_surface_count", 0),
                    "missing_required_count": item.get("missing_required_count", 0),
                    "neo4j_namespaces": item.get("neo4j_namespaces") or [],
                }
                for item in (data_architecture.get("domains") or [])
            ],
        },
        "graph_operating": {
            "mission": graph_operating.get("mission"),
            "capabilities": graph_operating.get("capabilities") or [],
            "domains": graph_operating.get("domains") or {},
            "operating_contract": graph_operating.get("operating_contract") or {},
        },
        "knowledge_registry": knowledge_registry,
        "ontology": {
            "active_namespace": ontology.get("active_namespace"),
            "default_namespace": ontology.get("default_namespace"),
            "summary": ontology.get("summary") or {},
            "namespaces": ontology.get("namespaces") or [],
            "diseases": (ontology.get("diseases") or [])[:12],
            "pdfs": (ontology.get("pdfs") or [])[:12],
        },
        "trace": {
            "default_graph_namespace": trace.get("default_graph_namespace", DEFAULT_NAMESPACE),
            "json_files": (trace.get("json_files") or [])[:12],
            "recent_runs": (trace.get("recent_runs") or [])[:12],
        },
        "pipeline_runs": runs[:20],
    }


@app.on_event("startup")
async def startup_event():
    try:
        print("[STARTUP] API Server starting up...")
        print("[STARTUP] API Server starting up... (Agent will lazy load on first request)")
        pipeline_store.load_from_disk()
        pathway_data_architecture_store.ensure_layout()
        knowledge_registry_store.ensure_layout()
        knowledge_registry_store.bootstrap()
        pdf_catalog.get_map(_get_agent())
        print("[STARTUP] Startup tasks completed successfully.")
    except Exception as exc:
        logger.exception("Startup error")
        print(f"[STARTUP] CRITICAL ERROR during initialization: {exc}")
        print("[STARTUP] Server will start anyway; agent will retry on first request.")


@app.get("/")
async def index():
    return RedirectResponse(url="/dashboard")


@app.get("/legacy-chat")
async def legacy_index(request: Request):
    return templates.TemplateResponse(request, "index.html", {"request": request})


@app.get("/claude-duet")
async def claude_duet_redirect():
    return RedirectResponse(url="/static/claims_insights/claude_duet_lab.html")


@app.get("/dashboard")
async def unified_dashboard_redirect():
    return RedirectResponse(url="/static/claims_insights/platform_dashboard.html")


@app.get("/knowledge/registry")
async def knowledge_registry_redirect():
    return RedirectResponse(url="/static/claims_insights/knowledge_registry.html")


@app.get("/claims-insights/explorer")
async def claims_insights_explorer_redirect():
    return RedirectResponse(url="/static/claims_insights/neo4j_disease_explorer.html")


@app.get("/ontology-v2/inspector")
async def ontology_v2_inspector_redirect():
    return RedirectResponse(url="/static/claims_insights/ontology_v2_pdf_inspector.html")


@app.get("/ontology-v2/cinematic")
async def ontology_v2_cinematic_redirect():
    return RedirectResponse(url="/static/claims_insights/ontology_v2_graph_cinematic.html")


@app.get("/claims-insights/testcase-trace")
async def claims_insights_testcase_trace_redirect():
    return RedirectResponse(url="/static/claims_insights/claims_adjudication_trace.html")


@app.get("/claims-insights/mapping-audit")
async def claims_insights_mapping_audit_redirect():
    return RedirectResponse(url="/static/claims_insights/sign_mapping_audit.html")


@app.get("/api/claims-insights/mapping-audit/summary")
async def mapping_audit_summary():
    summary_path = Path("workspaces/claims_insights/05_reference/signs/sign_mapping_review/sign_mapping_summary.json")
    if not summary_path.exists():
        raise HTTPException(status_code=404, detail="Sign mapping summary not found. Run build_sign_mapping_review_queue.py first.")
    return json.loads(summary_path.read_text(encoding="utf-8"))


@app.get("/api/claims-insights/mapping-audit/grouped-review")
async def mapping_audit_grouped_review(
    status: Optional[str] = None,
    page: int = 1,
    page_size: int = 50,
):
    review_path = Path("workspaces/claims_insights/05_reference/signs/sign_mapping_review/sign_mapping_grouped_review.json")
    if not review_path.exists():
        raise HTTPException(status_code=404, detail="Grouped review data not found.")
    rows = json.loads(review_path.read_text(encoding="utf-8"))
    if status:
        allowed = set(s.strip().lower() for s in status.split(","))
        rows = [r for r in rows if any(k.lower() in allowed for k in (r.get("status_counter") or {}).keys())]
    total = len(rows)
    start = (page - 1) * page_size
    end = start + page_size
    return {
        "total": total,
        "page": page,
        "page_size": page_size,
        "rows": rows[start:end],
    }


@app.get("/health")
async def health():
    return {"status": "healthy", "agent": "AcademicAgent", "port": 9600}


@app.get("/api/insurance/neo4j-status")
async def insurance_neo4j_status():
    """Check Neo4j connectivity for insurance contract agent."""
    try:
        from server_support.adjudication.adjudicator_agent import NEO4J_CONTRACT_AVAILABLE
        return {
            "status": "healthy",
            "neo4j_contract_agent": NEO4J_CONTRACT_AVAILABLE,
            "neo4j_uri": "bolt://localhost:7688"
        }
    except ImportError:
        return {
            "status": "unavailable",
            "neo4j_contract_agent": False,
            "error": "Contract agent module not found"
        }


@app.get("/api/claude/status")
async def claude_status():
    payload = claude_duet_runner.status()
    payload["bridge"] = get_bridge_status()
    return payload


@app.get("/api/claude/bridge/status")
async def claude_bridge_status():
    return get_bridge_status()


@app.get("/api/claude/memory/status")
async def claude_memory_status():
    return runtime_memory_status()


@app.post("/api/claude/memory/refresh")
async def claude_memory_refresh():
    try:
        return refresh_runtime_memory(trigger="manual_api")
    except Exception as exc:
        logger.exception("Claude runtime memory refresh failed")
        raise HTTPException(status_code=500, detail=str(exc))


@app.post("/api/claude/decision-gate", response_model=ClaudeDecisionGateResponse)
async def run_claude_decision_gate(request: ClaudeDecisionGateRequest):
    try:
        result = claude_decision_runner.decide(
            workflow=request.workflow,
            checkpoint=request.checkpoint,
            objective=request.objective,
            context=request.context,
            state=request.state,
            candidate_actions=request.candidate_actions,
            model=request.model,
            system_prompt=request.system_prompt,
            max_output_chars=request.max_output_chars,
        )
    except ClaudeCliUnavailableError as exc:
        raise HTTPException(status_code=503, detail=str(exc))
    except ClaudeCliError as exc:
        logger.error("Claude decision gate failed: %s", exc)
        raise HTTPException(status_code=500, detail=str(exc))
    except Exception as exc:
        logger.exception("Unexpected Claude decision gate error")
        raise HTTPException(status_code=500, detail=str(exc))

    return ClaudeDecisionGateResponse(
        decision=ClaudeDecisionGateResult(**result["decision"]),
        raw_content=result.get("raw_content"),
        duration_ms=result.get("duration_ms"),
        repair_attempts=result.get("repair_attempts", 0),
        claude_status=result.get("claude_status"),
        bridge_trace=result.get("bridge_trace"),
    )


@app.get("/api/platform/bootstrap")
async def platform_bootstrap():
    try:
        return _platform_bootstrap_payload()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/api/data-architecture/bootstrap")
async def data_architecture_bootstrap():
    return _safe_data_architecture_bootstrap()


@app.get("/api/graph-operating/bootstrap")
async def graph_operating_bootstrap():
    return _safe_graph_operating_bootstrap()


@app.get("/api/graph-operating/search")
async def graph_operating_search(q: str, domains: Optional[str] = None, limit: int = 12):
    try:
        return graph_operating_store.search(q, domains=domains, limit=limit)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/api/graph-operating/trace-service")
async def graph_operating_trace_service(
    service_code: str = "",
    service_name: str = "",
    disease_id: str = "",
    contract_id: str = "",
    ontology_namespace: Optional[str] = None,
):
    try:
        return graph_operating_store.trace_service(
            service_code=service_code,
            service_name=service_name,
            disease_id=disease_id,
            contract_id=contract_id,
            ontology_namespace=ontology_namespace,
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/api/graph-operating/health")
async def graph_operating_health():
    try:
        return graph_operating_store.health()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/api/graph-operating/report")
async def graph_operating_report():
    try:
        return graph_operating_store.report()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/api/knowledge/bootstrap")
async def knowledge_registry_bootstrap(refresh: bool = False):
    return _safe_knowledge_registry_bootstrap(refresh=refresh)


@app.post("/api/knowledge/sync")
async def knowledge_registry_sync(auto_ingest: bool = False, namespace: str = DEFAULT_NAMESPACE):
    summary = knowledge_registry_store.sync_known_sources()
    started_runs: list[dict] = []
    if auto_ingest:
        for asset_id in summary.get("new_asset_ids") or []:
            asset = knowledge_registry_store.get_asset(asset_id)
            if not asset:
                continue
            if not (asset.get("config") or {}).get("auto_ingest"):
                continue
            if not (asset.get("ingest") or {}).get("supported"):
                continue
            try:
                started_runs.append(
                    _start_knowledge_asset_ingest(
                        asset_id,
                        namespace=namespace,
                        split_strategy=(asset.get("config") or {}).get("split_strategy") or "auto",
                        preferred_disease_name=(asset.get("config") or {}).get("preferred_disease_name") or None,
                        source_type=(asset.get("config") or {}).get("source_type") or None,
                    )
                )
            except Exception as exc:
                logger.warning("Knowledge auto ingest failed for %s: %s", asset_id, exc)
    return {
        "status": "ok",
        "sync_summary": summary,
        "started_runs": started_runs,
    }


@app.get("/api/knowledge/assets")
async def knowledge_registry_assets(
    domain: Optional[str] = None,
    kind: Optional[str] = None,
    query: Optional[str] = None,
    limit: int = 200,
):
    return {
        "assets": knowledge_registry_store.list_assets(
            domain=domain,
            kind=kind,
            query=query,
            limit=limit,
        )
    }


@app.get("/api/knowledge/assets/{asset_id}")
async def knowledge_registry_asset_detail(asset_id: str):
    asset = knowledge_registry_store.get_asset(asset_id)
    if asset is None:
        raise HTTPException(status_code=404, detail="Asset not found")
    return asset


@app.post("/api/knowledge/assets/{asset_id}/config")
async def knowledge_registry_asset_config(asset_id: str, request: Request):
    payload = await request.json()
    try:
        return knowledge_registry_store.update_asset_config(asset_id, payload or {})
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc))


@app.get("/api/knowledge/assets/{asset_id}/graph-trace")
async def knowledge_registry_asset_trace(asset_id: str):
    try:
        return knowledge_registry_store.graph_trace(asset_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/api/knowledge/assets/{asset_id}/impact-report")
async def knowledge_registry_asset_impact_report(asset_id: str):
    try:
        return knowledge_registry_store.impact_report(asset_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/api/knowledge/assets/{asset_id}/text-workspace")
async def knowledge_registry_asset_text_workspace(
    asset_id: str,
    refresh_source: bool = False,
):
    try:
        return knowledge_text_bridge.get_asset_text_view(asset_id, refresh_source=refresh_source)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.post("/api/knowledge/assets/{asset_id}/text-extract")
async def knowledge_registry_asset_text_extract(asset_id: str):
    try:
        return knowledge_text_bridge.get_asset_text_view(asset_id, refresh_source=True)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.post("/api/knowledge/assets/{asset_id}/text-save")
async def knowledge_registry_asset_text_save(asset_id: str, request: Request):
    payload = await request.json()
    try:
        return knowledge_text_bridge.save_asset_text_view(
            asset_id,
            str((payload or {}).get("content") or ""),
            note=str((payload or {}).get("note") or ""),
        )
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/api/knowledge/assets/{asset_id}/manual-labels")
async def knowledge_registry_asset_manual_labels(asset_id: str):
    try:
        return {
            "asset_id": asset_id,
            "manual_labels": knowledge_registry_store.list_manual_labels(asset_id),
        }
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.post("/api/knowledge/assets/{asset_id}/text-labels")
async def knowledge_registry_asset_text_label_upsert(asset_id: str, request: Request):
    payload = await request.json()
    try:
        return knowledge_text_bridge.upsert_manual_label(asset_id, payload or {})
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.delete("/api/knowledge/assets/{asset_id}/text-labels/{label_id}")
async def knowledge_registry_asset_text_label_delete(asset_id: str, label_id: str):
    try:
        return knowledge_text_bridge.delete_manual_label(asset_id, label_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.post("/api/knowledge/assets/{asset_id}/excel-export")
async def knowledge_registry_asset_excel_export(asset_id: str):
    try:
        return knowledge_excel_bridge.export_asset_workbook(asset_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.post("/api/knowledge/assets/{asset_id}/excel-sync")
async def knowledge_registry_asset_excel_sync(
    asset_id: str,
    file: UploadFile = File(...),
):
    workbook_name = Path(file.filename or "knowledge_sync.xlsx").name
    workbook_path = KNOWLEDGE_EXCEL_VIEWS_DIR / workbook_name
    if workbook_path.exists():
        workbook_path = KNOWLEDGE_EXCEL_VIEWS_DIR / f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_{workbook_name}"
    with open(workbook_path, "wb") as handle:
        shutil.copyfileobj(file.file, handle)
    try:
        return knowledge_excel_bridge.import_asset_workbook(asset_id, workbook_path)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.post("/api/knowledge/upload")
async def knowledge_registry_upload(
    file: UploadFile = File(...),
    kind: str = Form("protocol_pdf"),
    domain: str = Form("protocols"),
    title: Optional[str] = Form(None),
    source_type: Optional[str] = Form(None),
    auto_ingest: bool = Form(False),
    namespace: str = Form(DEFAULT_NAMESPACE),
):
    target_path = knowledge_registry_store.reserve_managed_path(file.filename, kind)
    with open(target_path, "wb") as handle:
        shutil.copyfileobj(file.file, handle)
    asset = knowledge_registry_store.register_managed_file(
        target_path,
        kind=kind,
        domain=domain,
        title=title,
        source_type=source_type,
    )
    started_run = None
    if auto_ingest and (asset.get("ingest") or {}).get("supported"):
        started_run = _start_knowledge_asset_ingest(
            asset["asset_id"],
            namespace=namespace,
            split_strategy=(asset.get("config") or {}).get("split_strategy") or "auto",
            preferred_disease_name=(asset.get("config") or {}).get("preferred_disease_name") or None,
            source_type=source_type or (asset.get("config") or {}).get("source_type") or None,
        )
    return {
        "status": "ok",
        "asset": asset,
        "started_run": started_run,
    }


@app.post("/api/knowledge/assets/{asset_id}/ingest")
async def knowledge_registry_asset_ingest(
    asset_id: str,
    namespace: str = Form(DEFAULT_NAMESPACE),
    split_strategy: str = Form("auto"),
    preferred_disease_name: Optional[str] = Form(None),
    source_type: Optional[str] = Form(None),
):
    try:
        return _start_knowledge_asset_ingest(
            asset_id,
            namespace=namespace,
            split_strategy=split_strategy,
            preferred_disease_name=preferred_disease_name,
            source_type=source_type,
        )
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/api/claims-insights/explorer/bootstrap")
async def claims_insights_explorer_bootstrap():
    try:
        return claims_insights_graph_store.bootstrap()
    except Exception as exc:
        logger.warning("Claims insights bootstrap fell back to bundle: %s", exc)
        if claims_insights_graph_store.bundle_exists():
            return _claims_insights_bundle_bootstrap()
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/api/claims-insights/explorer/graph/{disease_id}")
async def claims_insights_explorer_graph(disease_id: str):
    try:
        graph = claims_insights_graph_store.disease_graph(disease_id)
        if graph:
            return {"source": "neo4j", "disease_id": disease_id, "graph": graph}
    except Exception as exc:
        logger.warning("Claims insights graph fell back to bundle for %s: %s", disease_id, exc)

    if claims_insights_graph_store.bundle_exists():
        graph = _claims_insights_bundle_graph(disease_id)
        if graph:
            return {"source": "bundle-fallback", "disease_id": disease_id, "graph": graph}

    raise HTTPException(status_code=404, detail=f"Disease graph not found for {disease_id}")


@app.get("/api/ontology-v2/inspector/bootstrap")
async def ontology_v2_inspector_bootstrap(namespace: Optional[str] = None):
    return _safe_ontology_bootstrap(namespace=namespace)


@app.get("/api/ontology-v2/inspector/disease/{disease_id}")
async def ontology_v2_inspector_disease(disease_id: str, namespace: str = DEFAULT_NAMESPACE):
    try:
        return ontology_v2_inspector_store.disease_graph(namespace, disease_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.post("/api/ontology-v2/ingest")
async def ontology_v2_ingest_one(
    disease_name: str = Form(...),
    namespace: str = Form(DEFAULT_NAMESPACE),
    existing_pdf_path: Optional[str] = Form(None),
    skip_first_page: bool = Form(False),
    file: Optional[UploadFile] = File(None),
):
    if not disease_name.strip():
        raise HTTPException(status_code=400, detail="disease_name is required")

    pdf_path: Optional[Path] = None
    filename_for_run = disease_name.strip()

    if file is not None and file.filename:
        pdf_path = _reserve_upload_path(file.filename)
        with open(pdf_path, "wb") as handle:
            shutil.copyfileobj(file.file, handle)
        filename_for_run = pdf_path.name
    elif existing_pdf_path:
        candidate = Path(existing_pdf_path)
        if not candidate.is_absolute():
            candidate = BASE_DIR / existing_pdf_path
        if not candidate.exists() or not candidate.is_file():
            raise HTTPException(status_code=400, detail=f"PDF not found: {existing_pdf_path}")
        pdf_path = candidate
        filename_for_run = candidate.name
    else:
        raise HTTPException(status_code=400, detail="Provide either file or existing_pdf_path")

    run_id = _make_run_id(f"ontology_v2_{filename_for_run}")
    _create_pipeline_run(
        run_id,
        filename_for_run,
        kind="ontology_ingest",
        namespace=namespace,
        disease_name=disease_name.strip(),
    )

    def run_pipeline_bg():
        capture = PipelineLogCapture(run_id, sys.stdout, pipeline_store)
        pipeline = None
        try:
            with redirect_stdout(capture), redirect_stderr(capture):
                from ontology_v2_ingest import OntologyV2Ingest

                pipeline = OntologyV2Ingest(namespace=namespace)
                print(f"[ONTOLOGY_V2] Starting ingest for disease={disease_name} namespace={namespace}")
                result = pipeline.run(
                    pdf_path=str(pdf_path),
                    disease_name=disease_name.strip(),
                    skip_first_page=bool(skip_first_page),
                )
                result["namespace"] = namespace
                result["pdf_path"] = str(pdf_path)
                pipeline_store.mark_completed(run_id, result=result)
                pdf_catalog.invalidate()
                print(f"[ONTOLOGY_V2] Completed ingest for {disease_name}")
        except Exception as exc:
            print(f"[ONTOLOGY_V2] Error: {exc}")
            pipeline_store.mark_error(run_id, str(exc))
        finally:
            if pipeline is not None:
                try:
                    pipeline.close()
                except Exception:
                    logger.warning("Ontology V2 pipeline close failed", exc_info=True)

    threading.Thread(target=run_pipeline_bg, daemon=True).start()
    return {
        "run_id": run_id,
        "status": "running",
        "namespace": namespace,
        "disease_name": disease_name.strip(),
        "pdf_path": str(pdf_path),
    }


@app.get("/api/claims-insights/testcase-trace/bootstrap")
async def claims_insights_testcase_trace_bootstrap():
    try:
        return _testcase_trace_bootstrap_payload()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.post("/api/claims-insights/testcase-trace")
async def claims_insights_testcase_trace(
    graph_namespace: str = Form("ontology_v2"),
    existing_json_path: Optional[str] = Form(None),
    file: Optional[UploadFile] = File(None),
):
    input_path: Optional[Path] = None
    filename_for_run = "testcase.json"

    if file is not None and file.filename:
        input_path = _reserve_upload_path(file.filename)
        with open(input_path, "wb") as handle:
            shutil.copyfileobj(file.file, handle)
        filename_for_run = input_path.name
    elif existing_json_path:
        candidate = Path(existing_json_path)
        if not candidate.is_absolute():
            candidate = BASE_DIR / existing_json_path
        if not candidate.exists() or not candidate.is_file():
            raise HTTPException(status_code=400, detail=f"JSON not found: {existing_json_path}")
        input_path = candidate
        filename_for_run = candidate.name
    else:
        raise HTTPException(status_code=400, detail="Provide either file or existing_json_path")

    run_id = _make_run_id(f"testcase_trace_{filename_for_run}")
    pipeline_store.create_run(
        run_id,
        {
            "id": run_id,
            "kind": "testcase_trace",
            "graph_namespace": graph_namespace,
            "status": "running",
            "filename": filename_for_run,
            "start_time": datetime.now().isoformat(),
            "logs": [],
        },
    )

    def run_pipeline_bg():
        capture = PipelineLogCapture(run_id, sys.stdout, pipeline_store)
        try:
            with redirect_stdout(capture), redirect_stderr(capture):
                from server_support.testcase_trace_runner import TRACE_RUNS_DIR, run_testcase_trace_batch

                output_dir = TRACE_RUNS_DIR / run_id
                print(
                    f"[TRACE_UI] Starting testcase trace for {filename_for_run} "
                    f"(namespace={graph_namespace})"
                )
                result = run_testcase_trace_batch(
                    input_path=input_path,
                    output_dir=output_dir,
                    graph_namespace=graph_namespace,
                    log=print,
                )
                result["graph_namespace"] = graph_namespace
                result["input_file"] = str(input_path)
                pipeline_store.mark_completed(run_id, result=result)
                print(f"[TRACE_UI] Completed testcase trace for {filename_for_run}")
        except Exception as exc:
            print(f"[TRACE_UI] Error: {exc}")
            pipeline_store.mark_error(run_id, str(exc))

    threading.Thread(target=run_pipeline_bg, daemon=True).start()
    return {
        "run_id": run_id,
        "status": "running",
        "graph_namespace": graph_namespace,
        "input_file": str(input_path),
    }


@app.get("/api/stats")
async def get_stats():
    try:
        with _get_agent().driver.session() as session:
            stats = {}
            stats["diseases"] = session.run(
                "MATCH (d:Disease)<-[:ABOUT_DISEASE]-(:Chunk) RETURN count(DISTINCT d) AS c"
            ).single()["c"]
            stats["chunks"] = session.run("MATCH (c:Chunk) RETURN count(c) AS c").single()["c"]
            stats["protocols"] = session.run(
                "MATCH (p:Protocol)-[:COVERS_DISEASE]->(:Disease) RETURN count(DISTINCT p) AS c"
            ).single()["c"]

            c1 = session.run("MATCH (n:Hospital) RETURN count(n) AS c").single()["c"]
            c2 = session.run(
                """
                MATCH (n:Chunk)
                WHERE n.hospital_name IS NOT NULL AND n.hospital_name <> ''
                RETURN count(DISTINCT n.hospital_name) AS c
                """
            ).single()["c"]
            c3 = session.run(
                """
                MATCH (p:Protocol)
                WHERE p.hospital_name IS NOT NULL AND p.hospital_name <> ''
                RETURN count(DISTINCT p.hospital_name) AS c
                """
            ).single()["c"]
            c4 = session.run(
                "MATCH (n:Chunk {source_type: 'hospital'}) RETURN count(DISTINCT n.disease_name) AS c"
            ).single()["c"]
            c5 = session.run("MATCH (n:Chunk) RETURN count(DISTINCT n.source_file) AS c").single()["c"]
            stats["hospitals"] = max(c1, c2, c3, c4, c5)
            if stats["hospitals"] == 0 and stats["chunks"] > 0:
                stats["hospitals"] = 1

            stats["entities"] = session.run(
                """
                MATCH (n)
                WHERE n:Drug OR n:Symptom OR n:LabTest OR n:Procedure OR n:Complication
                RETURN count(n) AS c
                """
            ).single()["c"]
            return stats
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/api/diseases")
async def list_diseases():
    try:
        ag = _get_agent()
        pdf_map = pdf_catalog.get_map(ag)
        with ag.driver.session() as session:
            result = session.run(
                """
                MATCH (d:Disease)<-[:ABOUT_DISEASE]-(c:Chunk)
                OPTIONAL MATCH (p:Protocol)-[:COVERS_DISEASE]->(d)
                RETURN d.name AS name,
                       d.icd_code AS icd_code,
                       coalesce(d.aliases, []) AS aliases,
                       count(c) AS chunks,
                       p.name AS protocol
                ORDER BY d.name
                """
            )
            diseases = []
            seen = set()
            for record in result:
                name = record["name"]
                if name in seen:
                    continue
                seen.add(name)
                diseases.append(
                    {
                        "name": name,
                        "icd_code": record["icd_code"],
                        "aliases": record["aliases"],
                        "chunks": record["chunks"],
                        "protocol": record["protocol"],
                        "pdf_file": pdf_map.get(name),
                    }
                )
            return diseases
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/api/protocols")
async def list_protocols():
    try:
        with _get_agent().driver.session() as session:
            result = session.run(
                """
                MATCH (p:Protocol)-[:COVERS_DISEASE]->(d:Disease)<-[:ABOUT_DISEASE]-(:Chunk)
                RETURN p.name AS name, count(DISTINCT d) AS disease_count
                ORDER BY p.name
                """
            )
            protocols = [
                {"name": record["name"], "disease_count": record["disease_count"]}
                for record in result
            ]
            orphan_count = session.run(
                """
                MATCH (d:Disease)<-[:ABOUT_DISEASE]-(:Chunk)
                WHERE NOT (d)<-[:COVERS_DISEASE]-(:Protocol)
                RETURN count(DISTINCT d) AS orphan_count
                """
            ).single()["orphan_count"]
            if orphan_count > 0:
                protocols.append({"name": "Phác đồ đơn lẻ", "disease_count": orphan_count})
            return protocols
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/api/hospitals")
async def get_hospitals():
    try:
        query = """
        MATCH (h:Hospital) RETURN h.name as name, h.location as location
        UNION
        MATCH (n:Chunk) WHERE n.hospital_name IS NOT NULL
        RETURN DISTINCT n.hospital_name as name, 'Metadata' as location
        """
        with _get_agent().driver.session() as session:
            result = session.run(query)
            return [record.data() for record in result]
    except Exception as exc:
        logger.warning("Error fetching hospitals: %s", exc)
        return []


@app.get("/api/pdfs")
async def list_pdfs():
    return pdf_catalog.list_pdfs()
@app.post("/api/adjudicate", response_model=ClaimAdjudicateResponse)
async def adjudicate_claim_api(request: ClaimAdjudicateRequest):
    try:
        ag = _get_agent()
        session_id, history = session_store.get_or_create(request.session_id)
        
        # Save request to history
        session_store.append(session_id, "user", f"[Hồ sơ Claim] {request.claim_text}")
        
        result = ag.adjudicate_claim(
            claim_text=request.claim_text,
            disease_name=request.disease_name
        )
        
        reasoning_node_ids = []
        if "context_nodes" in result:
            reasoning_node_ids = [n["id"] for n in result.get("context_nodes", [])]
            
        # Parse items to AdjudicationItem
        items = []
        for item_data in result.get("items", []):
            items.append(AdjudicationItem(
                service_name=item_data.get("service_name", "Unknown"),
                status=item_data.get("status", "Need Review"),
                reason=item_data.get("reason", "")
            ))
            
        summary = result.get("summary", "")
        session_store.append(session_id, "assistant", f"[Kết quả Thẩm định] {summary}")
        
        # Build reasoning trace entries
        raw_trace = result.get("reasoning_trace", [])
        reasoning_trace = [
            ReasoningTraceEntry(
                phase=t.get("phase", "unknown"),
                action=t.get("action", ""),
                node_ids=t.get("node_ids"),
                edge_keys=t.get("edge_keys"),
                details=t.get("details"),
                duration_ms=t.get("duration_ms"),
            )
            for t in raw_trace
        ]

        return ClaimAdjudicateResponse(
            items=items,
            summary=summary,
            reasoning_node_ids=reasoning_node_ids,
            reasoning_trace=reasoning_trace,
        )
    except Exception as e:
        logger.error(f"Error in adjudicate_claim: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


# ---------------------------------------------------------------------------
# Multiagent Adjudication v2
# ---------------------------------------------------------------------------
_multiagent_adjudicator = None
_ontology_clinical_reasoner = None


def _get_multiagent_adjudicator(use_neo4j: bool = None):
    global _multiagent_adjudicator
    if _multiagent_adjudicator is None:
        from server_support.adjudication.adjudicator_agent import AdjudicatorAgent, NEO4J_CONTRACT_AVAILABLE
        _multiagent_adjudicator = AdjudicatorAgent(use_neo4j=use_neo4j)
        logger.info(f"Multiagent adjudicator initialized (Neo4j contract agent: {NEO4J_CONTRACT_AVAILABLE})")
    return _multiagent_adjudicator


def _get_ontology_clinical_reasoner():
    global _ontology_clinical_reasoner
    if _ontology_clinical_reasoner is None:
        from server_support.adjudication.ontology_reasoner import OntologyClinicalReasoner
        _ontology_clinical_reasoner = OntologyClinicalReasoner()
    return _ontology_clinical_reasoner


@app.post("/api/adjudicate/v2", response_model=MultiAgentAdjudicateResponse)
async def adjudicate_claim_v2(request: MultiAgentAdjudicateRequest):
    try:
        adjudicator = _get_multiagent_adjudicator()
        return adjudicator.adjudicate_claim(request)
    except Exception as e:
        logger.error(f"Error in adjudicate_claim_v2: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


# ── Unified Knowledge Search endpoint ──

@app.post("/api/knowledge/search")
async def unified_knowledge_search(request: dict):
    """Search across ALL knowledge layers: chunks, assertions, summaries,
    entity mentions, claims insights, experience memory, graph traversal.

    Request body:
        query: str (required)
        intent: str (optional, default "general")
        disease_name: str (optional)
        entities: list[{name, type}] (optional)
        top_k: int (optional, default 12)
    """
    try:
        agent = _get_agent()
        query = request.get("query", "")
        if not query:
            raise HTTPException(status_code=400, detail="query is required")

        results, trace = agent.unified.retrieve(
            query=query,
            intent=request.get("intent", "general"),
            disease_name=request.get("disease_name"),
            entities=request.get("entities", []),
            top_k=request.get("top_k", 12)
        )

        return {
            "results": [r.to_context_dict() for r in results],
            "trace": trace,
            "total": len(results),
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in unified_knowledge_search: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/medical/reason-services", response_model=MedicalReasoningResponse)
async def reason_medical_services(request: MedicalReasoningRequest):
    try:
        reasoner = _get_ontology_clinical_reasoner()
        service_lines = list(request.service_lines)
        for line in service_lines:
            if request.symptoms:
                line.symptoms = list(line.symptoms) + [item for item in request.symptoms if item not in line.symptoms]
            if not line.medical_history and request.medical_history:
                line.medical_history = request.medical_history
            if not line.admission_reason and request.admission_reason:
                line.admission_reason = request.admission_reason
            if not line.diagnosis_text and request.known_diseases:
                line.diagnosis_text = "; ".join(request.known_diseases)

        case_context = reasoner.prepare_case(service_lines)
        line_results: list[MedicalReasoningLineResult] = []
        decision_counts: dict[str, int] = {}

        for line in service_lines:
            result = reasoner.assess_line(line, case_context)
            decision = str(result.get("decision") or "uncertain")
            decision_counts[decision] = decision_counts.get(decision, 0) + 1
            service_info = result.get("service_info") or {}
            line_results.append(
                MedicalReasoningLineResult(
                    service_name_raw=line.service_name_raw,
                    recognized_service_code=str(service_info.get("service_code") or ""),
                    recognized_canonical_name=str(service_info.get("canonical_name") or ""),
                    medical_decision=decision,
                    medical_confidence=float(result.get("confidence") or 0.0),
                    medical_reasoning_vi=str(result.get("reasoning_vi") or ""),
                    ontology_matches=list(result.get("matches") or []),
                    ontology_meta=dict(result.get("meta") or {}),
                    verification_plan=list(result.get("verification_plan") or []),
                    evidence_ledger=list(result.get("evidence_ledger") or []),
                    coverage_gaps=list(result.get("coverage_gaps") or []),
                    audit_summary=dict(result.get("audit_summary") or {}),
                    reasoning_trace=list(result.get("reasoning_trace") or []),
                )
            )

        approve_count = decision_counts.get("approve", 0)
        deny_count = decision_counts.get("deny", 0)
        uncertain_count = decision_counts.get("uncertain", 0)
        review_count = decision_counts.get("review", 0)
        total = len(line_results)
        summary_vi = (
            f"Medical reasoning cho {total} dich vu: "
            f"{approve_count} hop ly, {deny_count} khong hop ly, "
            f"{uncertain_count} chua du bang chung, {review_count} can xem xet."
        )

        return MedicalReasoningResponse(
            case_id=request.case_id,
            mode=str(case_context.get("mode") or "sign_inference"),
            summary_vi=summary_vi,
            input_signs=list(case_context.get("input_signs") or []),
            disease_hints=list(case_context.get("disease_hints") or []),
            top_hypotheses=list(case_context.get("top_hypotheses") or []),
            active_diseases=list(case_context.get("active_diseases") or []),
            verification_plan=list(case_context.get("verification_plan") or []),
            evidence_ledger=list(case_context.get("evidence_ledger") or []),
            coverage_gaps=list(case_context.get("coverage_gaps") or []),
            audit_summary=dict(case_context.get("audit_summary") or {}),
            results=line_results,
            reasoning_trace=list(case_context.get("reasoning_trace") or []),
            meta={
                "decision_counts": decision_counts,
                "coverage_ratio": case_context.get("coverage_ratio"),
            },
        )
    except Exception as e:
        logger.error(f"Error in reason_medical_services: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/ask", response_model=AskResponse)
async def ask_question(request: QuestionRequest):
    try:
        ag = _get_agent()
        session_id, history = session_store.get_or_create(request.session_id)

        if request.deep_reasoning:
            session_store.append(session_id, "user", request.question)
            disease_name = _resolve_disease_name_for_request(ag, request.question, request.disease_name)
            search_mode = "enhanced"
            source_priority = None
            if disease_name:
                if request.hospital_name:
                    context_nodes, source_priority = ag.priority_search(
                        request.question,
                        disease_name,
                        hospital_name=request.hospital_name,
                        top_k=request.top_k,
                    )
                    search_mode = f"priority:{disease_name}:{request.hospital_name}"
                else:
                    context_nodes = ag.scoped_search(request.question, disease_name, top_k=request.top_k)
                    search_mode = f"scoped:{disease_name}"
            else:
                context_nodes = ag.enhanced_search(request.question, top_k=request.top_k)

            if pathway_claude_brain_runner.is_available():
                try:
                    brain_result = pathway_claude_brain_runner.run_ask(
                        question=request.question,
                        disease_name=disease_name,
                        context_nodes=context_nodes or [],
                        history=_build_agentic_history(history),
                    )
                    answer = brain_result.get("answer", "")
                    session_store.append(session_id, "assistant", answer)
                    reasoning_node_ids = [
                        str(node.get("block_id") or node.get("id"))
                        for node in (context_nodes or [])
                        if (node.get("block_id") or node.get("id"))
                    ]
                    return AskResponse(
                        answer=answer,
                        sources=_build_sources(context_nodes or [], fallback_disease_name=disease_name),
                        disease_detected=disease_name,
                        search_mode=f"claude_brain:{search_mode}",
                        source_priority=source_priority,
                        hospital_name=request.hospital_name,
                        trace=_build_trace_steps(brain_result.get("trace") or {}),
                        verification=brain_result.get("verification"),
                        session_id=session_id,
                        reasoning_node_ids=reasoning_node_ids,
                    )
                except ClaudeCliError:
                    logger.exception("Claude Brain failed; falling back to legacy agentic reasoning")

            try:
                result = ag.agentic_ask(
                    request.question,
                    history=_build_agentic_history(history),
                    max_reflect=2,
                )
            except Exception as exc:
                logger.exception("Deep reasoning failed")
                result = {
                    "answer": f"Deep reasoning error: {exc}",
                    "context": [],
                    "trace": {},
                    "verification": None,
                    "disease_detected": None,
                }

            answer = result.get("answer", "")
            session_store.append(session_id, "assistant", answer)
            
            context_nodes = result.get("context") or context_nodes or []
            reasoning_node_ids = [
                str(node.get("block_id") or node.get("id"))
                for node in context_nodes
                if (node.get("block_id") or node.get("id"))
            ]

            return AskResponse(
                answer=answer,
                sources=_build_sources(
                    context_nodes,
                    fallback_disease_name=result.get("disease_detected") or disease_name,
                ),
                disease_detected=result.get("disease_detected") or disease_name,
                search_mode=f"agentic_react:{search_mode}",
                trace=_build_trace_steps(result.get("trace") or {}),
                verification=result.get("verification"),
                session_id=session_id,
                reasoning_node_ids=reasoning_node_ids,
            )

        import time as _time

        session_store.append(session_id, "user", request.question)
        ask_trace: list[ReasoningTraceEntry] = []

        # Step 1: Resolve disease
        t0 = _time.time()
        disease_name = _resolve_disease_name_for_request(ag, request.question, request.disease_name)
        dt = round((_time.time() - t0) * 1000, 1)
        if disease_name:
            ask_trace.append(ReasoningTraceEntry(
                phase="disease_resolve",
                action=f"Xác định bệnh: {disease_name}",
                node_ids=[f"disease:{disease_name}"],
                duration_ms=dt,
            ))
        else:
            ask_trace.append(ReasoningTraceEntry(
                phase="disease_resolve",
                action="Không xác định được bệnh cụ thể → tìm kiếm toàn cục",
                duration_ms=dt,
            ))

        search_mode = "enhanced"
        source_priority = None

        # Step 2: Search
        t0 = _time.time()
        if disease_name:
            if request.hospital_name:
                context_nodes, source_priority = ag.priority_search(
                    request.question,
                    disease_name,
                    hospital_name=request.hospital_name,
                    top_k=request.top_k,
                )
                search_mode = f"priority:{disease_name}:{request.hospital_name}"
            else:
                context_nodes = ag.scoped_search(request.question, disease_name, top_k=request.top_k)
                search_mode = f"scoped:{disease_name}"
        else:
            context_nodes = ag.enhanced_search(request.question, top_k=request.top_k)
        dt = round((_time.time() - t0) * 1000, 1)

        context_nodes = context_nodes or []
        found_node_ids = [
            f"chunk:{node['block_id']}" if node.get("block_id") else None
            for node in context_nodes
        ]
        found_node_ids = [n for n in found_node_ids if n]
        ask_trace.append(ReasoningTraceEntry(
            phase="graph_search",
            action=f"Vector + Fulltext search ({search_mode}) → {len(context_nodes)} kết quả",
            node_ids=found_node_ids[:15],
            details={
                "titles": [n.get("title", "?") for n in context_nodes[:6]],
                "total": len(context_nodes),
            },
            duration_ms=dt,
        ))

        # Step 3: LLM generation
        t0 = _time.time()
        answer = ag.generate_academic_response(
            request.question,
            style=request.style,
            context_nodes=context_nodes,
        )
        dt = round((_time.time() - t0) * 1000, 1)
        session_store.append(session_id, "assistant", answer)

        ask_trace.append(ReasoningTraceEntry(
            phase="llm_generate",
            action=f"LLM tổng hợp câu trả lời từ {len(context_nodes)} nguồn",
            duration_ms=dt,
        ))

        reasoning_node_ids = [
            str(node.get("block_id") or node.get("id"))
            for node in context_nodes
            if (node.get("block_id") or node.get("id"))
        ]

        return AskResponse(
            answer=answer,
            sources=_build_sources(context_nodes, fallback_disease_name=disease_name),
            disease_detected=disease_name,
            search_mode=search_mode,
            source_priority=source_priority,
            hospital_name=request.hospital_name,
            session_id=session_id,
            reasoning_node_ids=reasoning_node_ids,
            reasoning_trace=ask_trace,
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.post("/api/claude/duet", response_model=ClaudeDuetResponse)
async def run_claude_duet(request: ClaudeDuetRequest):
    try:
        result = claude_duet_runner.run_duet(
            topic=request.topic,
            context=request.context,
            turns=request.turns,
            model=request.model,
            agent_a_name=request.agent_a_name,
            agent_a_prompt=request.agent_a_prompt,
            agent_b_name=request.agent_b_name,
            agent_b_prompt=request.agent_b_prompt,
            max_output_chars=request.max_output_chars,
        )
    except ClaudeCliUnavailableError as exc:
        raise HTTPException(status_code=503, detail=str(exc))
    except ClaudeCliError as exc:
        logger.error("Claude duet failed: %s", exc)
        raise HTTPException(status_code=500, detail=str(exc))
    except Exception as exc:
        logger.exception("Unexpected Claude duet error")
        raise HTTPException(status_code=500, detail=str(exc))

    transcript = [
        ClaudeDuetTurn(
            index=turn["index"],
            speaker=turn["speaker"],
            role=turn["role"],
            content=turn["content"],
            raw_content=turn.get("raw_content"),
            structured=ClaudeDuetStructuredTurn(**turn["structured"]) if turn.get("structured") else None,
            duration_ms=turn.get("duration_ms"),
            repair_attempts=turn.get("repair_attempts", 0),
        )
        for turn in result.get("transcript", [])
    ]
    return ClaudeDuetResponse(
        topic=result["topic"],
        model=result["model"],
        turns=result["turns"],
        final_output=result["final_output"],
        final_structured_output=(
            ClaudeDuetStructuredTurn(**result["final_structured_output"])
            if result.get("final_structured_output")
            else None
        ),
        transcript=transcript,
        schema_version=result.get("schema_version", "claude_duet_turn.v1"),
        claude_status=result.get("claude_status"),
        bridge_trace=result.get("bridge_trace"),
    )


@app.websocket("/ws/pipeline/{run_id}")
async def pipeline_ws(websocket: WebSocket, run_id: str):
    await websocket.accept()
    listener = pipeline_store.add_listener(run_id)
    for message in pipeline_store.get_logs(run_id):
        await websocket.send_json(message)
    try:
        while True:
            try:
                message = await asyncio.get_event_loop().run_in_executor(None, listener.get, True, 1.0)
                await websocket.send_json(message)
            except queue.Empty:
                run = pipeline_store.get_run(run_id) or {}
                if run.get("status") in FINAL_RUN_STATUSES:
                    await websocket.send_json({"type": "complete", "status": run.get("status")})
                    break
                await asyncio.sleep(0.1)
    except WebSocketDisconnect:
        pass
    finally:
        pipeline_store.remove_listener(run_id, listener)


@app.post("/api/ingest")
async def ingest_pdf(
    file: UploadFile = File(...),
    test_file: Optional[UploadFile] = File(None),
    target_accuracy: float = Form(0.85),
    max_workers: int = Form(10),
    max_optimize_iterations: int = Form(10),
    hospital_name: Optional[str] = Form(None),
):
    pdf_path = _reserve_upload_path(file.filename)
    with open(pdf_path, "wb") as handle:
        shutil.copyfileobj(file.file, handle)

    test_path = None
    if test_file:
        test_path = _reserve_upload_path(test_file.filename)
        with open(test_path, "wb") as handle:
            shutil.copyfileobj(test_file.file, handle)

    run_id = _make_run_id(file.filename)
    _create_pipeline_run(
        run_id,
        file.filename,
        hospital_name=hospital_name,
        kind="pipeline",
        input_pdf_path=str(pdf_path),
        test_file_path=str(test_path) if test_path else None,
        target_accuracy=target_accuracy,
        max_workers=max_workers,
        max_optimize_iterations=max_optimize_iterations,
    )

    def run_pipeline_bg():
        capture = PipelineLogCapture(run_id, sys.stdout, pipeline_store)
        try:
            with redirect_stdout(capture), redirect_stderr(capture):
                _ensure_orchestrator_import_path()
                from orchestrator import run_pipeline

                print(f"[PIPELINE] Starting Pipeline for {file.filename} (Run: {run_id})")
                result = run_pipeline(
                    pdf_path=str(pdf_path),
                    test_file=str(test_path) if test_path else None,
                    target_accuracy=target_accuracy,
                    max_workers=max_workers,
                    max_optimize_iterations=max_optimize_iterations,
                )
                terminal_status = (result or {}).get("status") if isinstance(result, dict) else None
                if terminal_status in (
                    "paused_for_human_review",
                    "aborted_by_decision_gate",
                    "aborted_by_human_review",
                ):
                    pipeline_store.update_run(run_id, status=terminal_status, result=result)
                else:
                    pipeline_store.mark_completed(run_id, result=result)
                pdf_catalog.invalidate()
                print(f"[PIPELINE] Completed for {file.filename}")
        except Exception as exc:
            print(f"[PIPELINE] Error: {exc}")
            pipeline_store.mark_error(run_id, str(exc))

    threading.Thread(target=run_pipeline_bg, daemon=True).start()
    return {"run_id": run_id, "status": "running"}


@app.get("/api/ingest/{run_id}")
async def get_ingest_status(run_id: str):
    if not pipeline_store.has_run(run_id):
        pipeline_store.load_from_disk()
        if not pipeline_store.has_run(run_id):
            raise HTTPException(status_code=404, detail="Run not found")
    return pipeline_store.get_run(run_id)


@app.post("/api/ingest/{run_id}/control", response_model=PipelineDecisionControlResponse)
async def control_ingest_run(run_id: str, request: PipelineDecisionControlRequest):
    if not pipeline_store.has_run(run_id):
        pipeline_store.load_from_disk()
        if not pipeline_store.has_run(run_id):
            raise HTTPException(status_code=404, detail="Run not found")

    run = pipeline_store.get_run(run_id)
    if not run:
        raise HTTPException(status_code=404, detail="Run not found")

    decision_gate_state = _extract_decision_gate_state(run)
    if not decision_gate_state or run.get("status") != "paused_for_human_review":
        raise HTTPException(status_code=409, detail="Run is not waiting for human review")

    allowed_actions = decision_gate_state.get("allowed_actions") or []
    if request.action not in allowed_actions:
        raise HTTPException(
            status_code=400,
            detail=f"Action {request.action} is not allowed for checkpoint {decision_gate_state.get('checkpoint')}",
        )

    checkpoint = decision_gate_state.get("checkpoint")
    bridge_trace = decision_gate_state.get("bridge_trace") or {}
    bridge_interaction_id = bridge_trace.get("interaction_id")
    result = run.get("result") or {}
    run_dir_raw = result.get("run_directory")
    if not run_dir_raw:
        raise HTTPException(status_code=409, detail="Run does not expose a resumable run_directory")

    run_dir = Path(run_dir_raw)
    if not run_dir.exists():
        raise HTTPException(status_code=409, detail=f"Run directory not found: {run_dir}")

    target_accuracy = float(run.get("target_accuracy") or 0.85)
    max_optimize_iterations = int(run.get("max_optimize_iterations") or 3)
    test_file_path = run.get("test_file_path") or None

    _ensure_orchestrator_import_path()
    from orchestrator import resume_pipeline

    pipeline_store.record_log(
        run_id,
        f"[DecisionGateControl] checkpoint={checkpoint} action={request.action}",
    )
    _record_bridge_feedback_safe(
        bridge_interaction_id,
        {
            "event": "operator_action_selected",
            "run_id": run_id,
            "checkpoint": checkpoint,
            "selected_action": request.action,
            "note": request.note,
            "status_before": run.get("status"),
        },
    )

    if request.action == "abort_run":
        try:
            summary = resume_pipeline(
                run_dir=run_dir,
                checkpoint=checkpoint,
                action=request.action,
                test_file=test_file_path,
                target_accuracy=target_accuracy,
                max_optimize_iterations=max_optimize_iterations,
                note=request.note,
            )
        except Exception as exc:
            logger.exception("Decision gate abort failed")
            raise HTTPException(status_code=500, detail=str(exc))

        terminal_status = (
            summary.get("status")
            if isinstance(summary, dict)
            else "aborted_by_human_review"
        )
        pipeline_store.update_run(
            run_id,
            status=terminal_status,
            result=summary,
            human_decision_events=(summary or {}).get("human_decision_events", []),
            error=None,
        )
        _record_bridge_feedback_safe(
            bridge_interaction_id,
            {
                "event": "operator_outcome",
                "run_id": run_id,
                "checkpoint": checkpoint,
                "selected_action": request.action,
                "terminal_status": terminal_status,
            },
        )
        return PipelineDecisionControlResponse(
            run_id=run_id,
            status=terminal_status,
            selected_action=request.action,
            checkpoint=checkpoint,
            message="Run da duoc operator dung lai.",
            allowed_actions=allowed_actions,
        )

    pipeline_store.update_run(run_id, status="running", error=None)

    def resume_pipeline_bg():
        capture = PipelineLogCapture(run_id, sys.stdout, pipeline_store)
        try:
            with redirect_stdout(capture), redirect_stderr(capture):
                result = resume_pipeline(
                    run_dir=run_dir,
                    checkpoint=checkpoint,
                    action=request.action,
                    test_file=test_file_path,
                    target_accuracy=target_accuracy,
                    max_optimize_iterations=max_optimize_iterations,
                    note=request.note,
                )
                if not isinstance(result, dict):
                    raise RuntimeError("resume_pipeline did not return a summary")
                terminal_status = result.get("status") or "completed"
                pipeline_store.update_run(
                    run_id,
                    status=terminal_status,
                    result=result,
                    human_decision_events=result.get("human_decision_events", []),
                    error=None,
                )
                _record_bridge_feedback_safe(
                    bridge_interaction_id,
                    {
                        "event": "operator_outcome",
                        "run_id": run_id,
                        "checkpoint": checkpoint,
                        "selected_action": request.action,
                        "terminal_status": terminal_status,
                    },
                )
                pdf_catalog.invalidate()
                print(f"[DecisionGateControl] Resume complete for {run_id} -> {terminal_status}")
        except Exception as exc:
            _record_bridge_feedback_safe(
                bridge_interaction_id,
                {
                    "event": "operator_outcome",
                    "run_id": run_id,
                    "checkpoint": checkpoint,
                    "selected_action": request.action,
                    "terminal_status": "error",
                    "error": str(exc),
                },
            )
            print(f"[DecisionGateControl] Error: {exc}")
            pipeline_store.mark_error(run_id, str(exc))

    threading.Thread(target=resume_pipeline_bg, daemon=True).start()
    return PipelineDecisionControlResponse(
        run_id=run_id,
        status="running",
        selected_action=request.action,
        checkpoint=checkpoint,
        message="Da tiep tuc pipeline tu decision gate.",
        allowed_actions=allowed_actions,
    )


@app.get("/api/pipeline-runs")
async def list_pipeline_runs():
    pipeline_store.load_from_disk()
    runs = []
    for run in pipeline_store.iter_runs():
        result = run.get("result") or {}
        summary = result.get("summary") or {}
        runs.append(
            {
                "id": run.get("id"),
                "name": run.get("filename", "Unknown"),
                "status": run.get("status", "unknown"),
                "start_time": run.get("start_time"),
                "summary": {
                    "accuracy": summary.get("accuracy") or summary.get("final_accuracy") or "0%",
                    "total_questions": summary.get("total_questions")
                    or summary.get("total_questions_tested")
                    or 0,
                },
            }
        )
    return sorted(runs, key=lambda item: item["start_time"] or "", reverse=True)


@app.post("/api/crawl")
async def trigger_crawl(req: CrawlRequest):
    run_id = f"crawl_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    _create_pipeline_run(run_id, f"Crawl: {req.query or 'All'}")

    def run_crawl_bg():
        try:
            script_path = CRAWLER_SCRIPT if CRAWLER_SCRIPT.exists() else LEGACY_CRAWLER_SCRIPT
            process = subprocess.Popen(
                [sys.executable, str(script_path)],
                cwd=str(ROOT_DIR),
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
            )
            capture = PipelineLogCapture(run_id, sys.stdout, pipeline_store)
            for line in process.stdout or []:
                capture.write(line)
            process.wait()
            if process.returncode == 0:
                pipeline_store.mark_completed(run_id)
            else:
                pipeline_store.mark_error(run_id, f"Crawler exited with code {process.returncode}")
        except Exception as exc:
            pipeline_store.mark_error(run_id, str(exc))

    threading.Thread(target=run_crawl_bg, daemon=True).start()
    return {"status": "started", "run_id": run_id}


if __name__ == "__main__":
    import uvicorn

    try:
        print("Starting server on http://0.0.0.0:9600")
        uvicorn.run(app, host="0.0.0.0", port=9600, log_level="debug")
    except Exception as exc:
        print(f"Uvicorn error: {exc}")
        logger.exception("Uvicorn failed")
