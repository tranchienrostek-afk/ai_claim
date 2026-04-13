from __future__ import annotations

import json
from pathlib import Path

import httpx
from fastapi import FastAPI, File, Form, HTTPException, UploadFile
from fastapi.responses import FileResponse, HTMLResponse

from .azure_openai_backend import AzureOpenAIBackend, AzureOpenAIConfig
from .benchmark_analyzer import DuelAnalyzer
from .domain_policy import DomainPolicy
from .ingest_compiler import IngestCompiler
from .knowledge_layout import KnowledgeLayout
from .knowledge_registry import KnowledgeRegistry
from .knowledge_surface import KnowledgeSurface
from .live_duel_runner import LiveDuelRunner
from .neo4j_toolkit import Neo4jToolkit
from .pathway_knowledge_bridge import PathwayKnowledgeBridge
from .reasoning_agent import AzureReasoningAgent
from .settings import SETTINGS


app = FastAPI(title="ai_claim", version="0.1.0")


def _load_domain_policy() -> DomainPolicy:
    return DomainPolicy.from_file(SETTINGS.configs_dir / "domain_policy.json")


def _load_knowledge_layout() -> KnowledgeLayout:
    return KnowledgeLayout.from_file(
        SETTINGS.project_root,
        SETTINGS.configs_dir / "knowledge_roots.json",
    )


def _load_registry() -> KnowledgeRegistry:
    config = (SETTINGS.configs_dir / "knowledge_roots.json").read_text(encoding="utf-8")
    return KnowledgeRegistry.create(SETTINGS.project_root, json.loads(config))


def _load_knowledge_surface() -> KnowledgeSurface:
    config = json.loads((SETTINGS.configs_dir / "knowledge_roots.json").read_text(encoding="utf-8"))
    return KnowledgeSurface(project_root=SETTINGS.project_root, config=config)


def _load_pathway_bridge() -> PathwayKnowledgeBridge:
    return PathwayKnowledgeBridge(base_url=SETTINGS.pathway_api_base_url)


def _load_ingest_compiler() -> IngestCompiler:
    return IngestCompiler(registry=_load_registry(), pathway=_load_pathway_bridge())


def _http_health(url: str, timeout_seconds: float = 5.0) -> dict[str, object]:
    try:
        with httpx.Client(timeout=timeout_seconds) as client:
            response = client.get(url)
            return {
                "status": "up" if response.is_success else "error",
                "status_code": response.status_code,
                "url": url,
                "body_preview": response.text[:500],
            }
    except Exception as exc:
        return {
            "status": "down",
            "url": url,
            "error": str(exc),
        }


@app.get("/health")
def health() -> dict[str, object]:
    backend = AzureOpenAIBackend(
        AzureOpenAIConfig(
            endpoint=SETTINGS.azure_openai_endpoint,
            api_key=SETTINGS.azure_openai_api_key,
            api_version=SETTINGS.azure_openai_api_version,
            chat_deployment=SETTINGS.azure_openai_chat_deployment,
        )
    )
    
    # Check dependencies with short timeout
    neo4j = neo4j_health()
    pathway = _http_health(f"{SETTINGS.pathway_api_base_url.rstrip('/')}/health", timeout_seconds=2.0)
    azure_proxy = _http_health(f"{SETTINGS.azure_proxy_base_url.rstrip('/')}/health", timeout_seconds=2.0)
    
    # Core deps: Neo4j + Pathway + Azure configured. Proxy/router are optional.
    core_ok = (
        neo4j.get("status") == "ready"
        and pathway.get("status") == "up"
        and backend.is_configured()
    )

    return {
        "status": "ok" if core_ok else "degraded",
        "dependencies": {
            "neo4j": neo4j.get("status"),
            "pathway": pathway.get("status"),
            "azure_proxy": azure_proxy.get("status"),
        },
        "azure_openai_configured": backend.is_configured(),
        "project_root": str(SETTINGS.project_root),
        "pathway_api_base_url": SETTINGS.pathway_api_base_url,
        "router_base_url": SETTINGS.router_base_url,
        "azure_proxy_base_url": SETTINGS.azure_proxy_base_url,
    }


@app.get("/")
def index() -> FileResponse:
    return FileResponse(SETTINGS.static_dir / "dashboard.html")


@app.get("/api/domain-policy")
def domain_policy() -> dict[str, object]:
    policy = _load_domain_policy()
    return policy.raw


@app.get("/api/knowledge-layout")
def knowledge_layout() -> dict[str, object]:
    layout = _load_knowledge_layout()
    return {
        "version": layout.raw.get("version"),
        "roots": layout.ensure_roots(),
    }


@app.get("/api/knowledge/assets")
def knowledge_assets(limit: int = 100, offset: int = 0, root_key: str = "") -> dict[str, object]:
    registry = _load_registry()
    filtered_assets = registry.list_assets(
        root_key=root_key or None,
        limit=max(1, min(int(limit), 500)),
        offset=max(int(offset), 0),
    )
    total_assets = len(registry.list_assets(root_key=root_key or None))
    return {
        "assets": filtered_assets,
        "total": total_assets,
        "limit": max(1, min(int(limit), 500)),
        "offset": max(int(offset), 0),
        "root_key": root_key or "",
    }


@app.get("/api/knowledge/assets/{asset_id}")
def knowledge_asset_detail(asset_id: str) -> dict[str, object]:
    registry = _load_registry()
    asset = registry.get_asset(asset_id)
    if asset is None:
        raise HTTPException(status_code=404, detail="Khong tim thay asset")
    return asset


@app.get("/api/knowledge/root-summary")
def knowledge_root_summary() -> dict[str, object]:
    registry = _load_registry()
    return registry.root_summary()


@app.post("/api/knowledge/scan")
def knowledge_scan() -> dict[str, object]:
    registry = _load_registry()
    return registry.scan()


@app.get("/api/knowledge/surface/search")
def knowledge_surface_search(query: str, root_key: str = "", disease_key: str = "", limit: int = 8) -> dict[str, object]:
    surface = _load_knowledge_surface()
    return surface.search(
        query=query,
        root_key=root_key or None,
        disease_key=disease_key or None,
        limit=limit,
    )


@app.get("/api/knowledge/surface/read")
def knowledge_surface_read(path: str) -> dict[str, object]:
    surface = _load_knowledge_surface()
    try:
        return surface.read(path)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/api/knowledge/upload")
async def knowledge_upload(
    root_key: str = Form(...),
    file: UploadFile = File(...),
) -> dict[str, object]:
    registry = _load_registry()
    content = await file.read()
    try:
        return registry.register_upload(root_key=root_key, filename=file.filename, content=content)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.get("/api/ingest/support-matrix")
def ingest_support_matrix() -> dict[str, object]:
    compiler = _load_ingest_compiler()
    return compiler.support_matrix()


@app.get("/api/pathway/knowledge/bootstrap")
def pathway_knowledge_bootstrap() -> dict[str, object]:
    bridge = _load_pathway_bridge()
    return bridge.bootstrap()


@app.get("/api/pathway/knowledge/assets")
def pathway_knowledge_assets(limit: int = 100) -> dict[str, object]:
    bridge = _load_pathway_bridge()
    return bridge.list_assets(limit=limit)


@app.post("/api/knowledge/bridge-upload")
async def knowledge_bridge_upload(
    root_key: str = Form(...),
    file: UploadFile = File(...),
    auto_ingest: bool = Form(False),
    namespace: str = Form("ontology_v2"),
    source_type: str = Form(""),
    wait_for_completion: bool = Form(False),
) -> dict[str, object]:
    compiler = _load_ingest_compiler()
    content = await file.read()
    try:
        return compiler.upload_and_bridge(
            root_key=root_key,
            filename=file.filename,
            content=content,
            auto_ingest=auto_ingest,
            namespace=namespace,
            source_type=source_type or None,
            title=file.filename,
            wait_for_completion=wait_for_completion,
        )
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/api/knowledge/assets/{asset_id}/bridge")
def knowledge_bridge_existing_asset(
    asset_id: str,
    auto_ingest: bool = Form(False),
    namespace: str = Form("ontology_v2"),
    source_type: str = Form(""),
    wait_for_completion: bool = Form(False),
) -> dict[str, object]:
    compiler = _load_ingest_compiler()
    try:
        return compiler.bridge_existing_asset(
            asset_id=asset_id,
            auto_ingest=auto_ingest,
            namespace=namespace,
            source_type=source_type or None,
            wait_for_completion=wait_for_completion,
        )
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.get("/api/pathway/knowledge/assets/{asset_id}/impact-report")
def pathway_asset_impact_report(asset_id: str) -> dict[str, object]:
    bridge = _load_pathway_bridge()
    try:
        return bridge.impact_report(asset_id)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc


@app.get("/api/pathway/knowledge/assets/{asset_id}/graph-trace")
def pathway_asset_graph_trace(asset_id: str) -> dict[str, object]:
    bridge = _load_pathway_bridge()
    try:
        return bridge.graph_trace(asset_id)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc


@app.get("/api/pathway/knowledge/assets/{asset_id}/text-workspace")
def pathway_asset_text_workspace(asset_id: str) -> dict[str, object]:
    bridge = _load_pathway_bridge()
    try:
        return bridge.text_workspace(asset_id)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc


@app.get("/api/pathway/runs/{run_id}")
def pathway_run_status(run_id: str) -> dict[str, object]:
    bridge = _load_pathway_bridge()
    try:
        return bridge.get_run_status(run_id)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc


@app.post("/api/knowledge/disease-workspace")
def knowledge_disease_workspace(disease_key: str = Form(...), disease_name: str = Form(...)) -> dict[str, object]:
    layout = _load_knowledge_layout()
    return layout.create_disease_workspace(disease_key, disease_name)


@app.get("/api/architecture")
def architecture() -> dict[str, object]:
    return {
        "docs": [
            "01_chenh_lech_tu_duy_pathway_va_agent_claude.md",
            "02_tieu_chi_ingest_neo4j.md",
            "03_kien_truc_muc_tieu_ai_claim.md",
            "04_version_feedback_memory.md",
            "05_luong_quyet_dinh_y_te_bao_hiem.md",
            "06_pathway_hay_agent_cho_ingest.md",
            "07_domain_lock_va_azure.md",
            "08_trang_thai_trien_khai.md",
            "09_goi_ai_claim_len_web.md",
        ],
        "modules": [
            "benchmark_analyzer.py",
            "domain_policy.py",
            "knowledge_layout.py",
            "knowledge_registry.py",
            "azure_openai_backend.py",
            "knowledge_surface.py",
            "pathway_client.py",
            "pathway_knowledge_bridge.py",
            "ingest_compiler.py",
            "live_duel_runner.py",
            "reasoning_agent.py",
        ],
        "runtime_layers": [
            "knowledge_surface",
            "ingest_compiler",
            "graph_core",
            "graph_operating_layer",
            "reasoning_runtime",
            "human_review",
        ],
    }


@app.get("/api/agent-launch-spec")
def agent_launch_spec(prompt_file: str, mcp_config_file: str, model: str = "sonnet") -> dict[str, object]:
    policy = _load_domain_policy()
    return policy.build_agent_claude_launch_spec(
        Path(prompt_file),
        Path(mcp_config_file),
        model=model,
        project_root=SETTINGS.project_root,
    )


@app.get("/api/neo4j/health")
def neo4j_health() -> dict[str, object]:
    toolkit = Neo4jToolkit()
    try:
        return {
            "status": "ready",
            "summary": toolkit.graph_health(),
        }
    except Exception as exc:
        return {
            "status": "unavailable",
            "error": str(exc),
        }
    finally:
        toolkit.close()


@app.get("/api/neo4j/disease-service-coverage")
def neo4j_disease_service_coverage(disease_name: str = "", icd_code: str = "") -> dict[str, object]:
    """Check disease-service coverage in the graph for benchmark readiness."""
    toolkit = Neo4jToolkit()
    try:
        services = toolkit.query_disease_services(disease_name=disease_name, icd_code=icd_code)
        snapshot = toolkit.query_ci_disease_snapshot(
            disease_name=disease_name,
            disease_id=f"disease:{icd_code}" if icd_code else "",
        )
        return {
            "query": {"disease_name": disease_name, "icd_code": icd_code},
            "disease_services": services,
            "ci_snapshot": snapshot,
            "coverage_summary": {
                "has_service_links": bool(any(r.get("service_codes") for r in services)),
                "service_count": sum(len(r.get("service_codes", [])) for r in services),
                "source": services[0].get("_source", "primary") if services else "none",
                "has_ci_signs": bool(snapshot.get("signs")),
                "ci_sign_count": len(snapshot.get("signs", [])),
            },
        }
    except Exception as exc:
        return {"status": "error", "error": str(exc)}
    finally:
        toolkit.close()


@app.get("/api/neo4j/benchmark-coverage")
def neo4j_benchmark_coverage() -> dict[str, object]:
    """Check graph coverage for all known benchmark diseases."""
    toolkit = Neo4jToolkit()
    BENCHMARK_DISEASES = [
        ("H81.0", "Bệnh Ménière"),
        ("J18.9", "Viêm phổi"),
        ("J06.9", "Viêm mũi họng cấp tính"),
        ("J01.9", "Viêm mũi xoang cấp tính"),
        ("H66.3", "Viêm tai giữa mạn tính có cholesteatoma"),
        ("A15.0", "Lao phổi"),
        ("K35", "Viêm ruột thừa"),
        ("E11", "Đái tháo đường type 2"),
    ]
    try:
        results = []
        for icd, name in BENCHMARK_DISEASES:
            services = toolkit.query_disease_services(icd_code=icd)
            svc_count = sum(len(r.get("service_codes", [])) for r in services)
            source = services[0].get("_source", "primary") if services else "none"
            results.append({
                "icd_code": icd,
                "disease_name": name,
                "service_count": svc_count,
                "source": source,
                "ready": svc_count >= 3,
            })
        ready_count = sum(1 for r in results if r["ready"])
        return {
            "diseases": results,
            "summary": {
                "total": len(results),
                "ready": ready_count,
                "not_ready": len(results) - ready_count,
            },
        }
    except Exception as exc:
        return {"status": "error", "error": str(exc)}
    finally:
        toolkit.close()


@app.get("/api/neo4j/mapping-key-audit")
def neo4j_mapping_key_audit() -> dict[str, object]:
    toolkit = Neo4jToolkit()
    try:
        return toolkit.mapping_key_audit()
    except Exception as exc:
        return {
            "status": "unavailable",
            "error": str(exc),
        }
    finally:
        toolkit.close()


@app.get("/api/system/status")
def system_status() -> dict[str, object]:
    neo4j = neo4j_health()
    pathway = _http_health(f"{SETTINGS.pathway_api_base_url.rstrip('/')}/health")
    azure_proxy = _http_health(f"{SETTINGS.azure_proxy_base_url.rstrip('/')}/health")
    router = _http_health(f"{SETTINGS.router_base_url.rstrip('/')}/v1/models")
    return {
        "ai_claim": health(),
        "pathway": pathway,
        "neo4j": neo4j,
        "azure_proxy": azure_proxy,
        "router": router,
    }


@app.get("/api/production-readiness")
def production_readiness() -> dict[str, object]:
    support = _load_ingest_compiler().support_matrix()
    root_summary = _load_registry().root_summary()
    mapping_audit = neo4j_mapping_key_audit()
    system = system_status()
    direct_roots = [
        row["root_key"]
        for row in support.get("rows", [])
        if row.get("pathway_direct_ingest")
    ]
    catalog_only_roots = [
        row["root_key"]
        for row in support.get("rows", [])
        if row.get("pathway_catalog") and not row.get("pathway_direct_ingest")
    ]
    checklist = {
        "ai_claim_server_ready": system["ai_claim"]["status"] == "ok",
        "pathway_api_reachable": system["pathway"]["status"] == "up",
        "neo4j_reachable": system["neo4j"].get("status") == "ready",
        "no_graph_key_duplicates": not mapping_audit.get("has_duplicates", True),
        "direct_ingest_roots_present": len(direct_roots) > 0,
    }
    optional = {
        "router_reachable": system["router"]["status"] == "up",
        "azure_proxy_reachable": system["azure_proxy"]["status"] == "up",
    }
    return {
        "checklist": checklist,
        "optional": optional,
        "direct_ingest_roots": direct_roots,
        "catalog_only_roots": catalog_only_roots,
        "root_summary": root_summary,
        "mapping_key_audit": mapping_audit,
        "system_status": system,
        "notes_vi": [
            "Neu root nam trong catalog_only thi van quan tri duoc tren dashboard, nhung ingest vao graph hien dang di qua Pathway bridge thay vi engine local first-class.",
            "Mapper duplicate key audit hien uu tien cac key canonic quan trong: CIService, CanonicalService, CISign, CIDisease, InsuranceContract, Benefit.",
            "router va azure_proxy la optional — reasoning agent fallback truc tiep ve Azure khi chung down.",
        ],
    }


@app.get("/api/benchmark/summary")
def benchmark_summary(run_dir: str) -> dict[str, object]:
    run_path = Path(run_dir)
    if not run_path.exists():
        raise HTTPException(status_code=404, detail="Run dir khong ton tai")
    analyzer = DuelAnalyzer(run_path)
    return analyzer.build_reasoning_gap()


@app.get("/api/benchmark/report")
def benchmark_report(run_dir: str) -> HTMLResponse:
    run_path = Path(run_dir)
    if not run_path.exists():
        raise HTTPException(status_code=404, detail="Run dir khong ton tai")
    analyzer = DuelAnalyzer(run_path)
    return HTMLResponse(f"<pre>{analyzer.build_markdown_report()}</pre>")


@app.post("/api/reasoning/run")
def run_reasoning(case_packet: dict[str, object]) -> dict[str, object]:
    try:
        agent = AzureReasoningAgent.from_settings()
        try:
            return agent.run_case(case_packet)
        except Exception as exc:
            return {
                "status": "failed",
                "error": str(exc),
                "hint": "Kiem tra Azure OpenAI env vars va ket noi Neo4j.",
            }
        finally:
            agent.toolkit.close()
    except Exception as exc:
        raise HTTPException(
            status_code=424,
            detail=f"Dịch vụ hỗ trợ suy luận (Neo4j/Azure) không khả thi: {exc}"
        )


@app.post("/api/duel/run")
def run_live_duel(case_packet: dict[str, object]) -> dict[str, object]:
    if "case_file" in case_packet:
        case_path = Path(str(case_packet["case_file"]))
        if not case_path.exists():
            raise HTTPException(status_code=404, detail="Case file khong ton tai")
        try:
            case_packet = json.loads(case_path.read_text(encoding="utf-8"))
        except Exception as exc:
            raise HTTPException(status_code=400, detail=f"Lỗi đọc file JSON: {exc}")

    try:
        runner = LiveDuelRunner.create()
        return runner.run_case(case_packet)
    except Exception as exc:
        raise HTTPException(
            status_code=424,
            detail=f"Không thể chạy Live Duel do lỗi hạ tầng (Neo4j/Pathway): {exc}"
        )


@app.get("/dashboard")
def dashboard() -> FileResponse:
    return FileResponse(SETTINGS.static_dir / "dashboard.html")


@app.get("/management")
def management() -> FileResponse:
    return FileResponse(SETTINGS.static_dir / "management.html")
