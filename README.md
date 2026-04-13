# ai_claim

`ai_claim` la project doc lap, cloud-ready, de dong goi toan bo bai toan:

- ingest tri thuc y te, bao hiem, phap ly vao Neo4j;
- benchmark va giai thich chenh lech suy luan giua `Pathway` va `agent_claude`;
- khoa domain cho agent suy luan chi con y te + bao hiem;
- chuyen runtime sang Azure OpenAI thay vi GLM/Claude native;
- quan tri tri thuc theo file, version, note, feedback, memory;
- cung cap UI/UX dashboard de quan ly toan bo qua trinh.

## Muc tieu

Project nay khong co gang "copy Pathway" hoac "copy agent_claude" 1:1. Muc tieu la:

1. Rut ra phan manh nhat cua `Pathway`: data architecture, ingest, Neo4j graph, evidence, audit.
2. Rut ra phan manh nhat cua `agent_claude`: planner, search strategy, synthesis, decision trace.
3. Hop nhat 2 huong do thanh mot he thong `AI Claim Reasoning Platform` doc lap.

## Cau truc

- `docs/`: kien truc, tieu chi ingest, versioning, memory, benchmark notes.
- `docs/handoffs/`: bao cao ban giao, router, MCP, benchmark test.
- `configs/`: domain policy, knowledge roots, Azure OpenAI mau.
- `configs/agent_claude_router.env.example`: env mau de `agent_claude` di qua `9router`.
- `src/ai_claim/`: source code backend, analyzer, runtime policy.
- `scripts/`: script bootstrap, analyze benchmark, khoi dong local.
- `data/knowledge/`: kho tri thuc de dua len cloud.
- `data/runtime/`: cache, manifest, logs, report sinh ra trong runtime.
- `data/duel_runs/`: artifact so sanh live giua `ai_claim` va `Pathway`.
- `data/imported_runs/`: copy cac run quan trong tu workspace ngoai de giu lai trong chinh folder nay.

## Khoi dong nhanh

### 1. Tao knowledge tree

```powershell
cd d:\desktop_folder\01_claudecodeleak\ai_claim
python scripts\bootstrap_knowledge_tree.py
```

### 2. Phan tich run benchmark da co

```powershell
cd d:\desktop_folder\01_claudecodeleak\ai_claim
python scripts\analyze_duel_run.py --run-dir d:\desktop_folder\01_claudecodeleak\pathway\notebooklm\data\duel_runs\pathway_vs_agent_claude\20260409_224555_duel_meniere_001
```

### 3. Chay API local

```powershell
cd d:\desktop_folder\01_claudecodeleak\ai_claim
uvicorn src.ai_claim.main:app --reload --host 0.0.0.0 --port 9780
```

Sau do mo:

- `http://localhost:9780/health`
- `http://localhost:9780/dashboard`

### 4. Smoke test nhanh

```powershell
cd d:\desktop_folder\01_claudecodeleak\ai_claim
python scripts\smoke_test.py
```

### 5. Chay reasoning bang Azure OpenAI

```powershell
cd d:\desktop_folder\01_claudecodeleak\ai_claim
copy configs\azure_openai.env.example .env.local
# set cac bien AZURE_OPENAI_* truoc
python scripts\run_reasoning_case.py --case-file data\benchmarks\sample_case_meniere.json
```

### 6. Chay live duel `ai_claim` vs `Pathway`

```powershell
cd d:\desktop_folder\01_claudecodeleak\ai_claim
python scripts\run_live_duel.py --case-file data\benchmarks\sample_case_meniere.json
```

Artifact se duoc luu vao `data/duel_runs/`.

### 7. Gom du lieu ngoai vao trong `ai_claim`

```powershell
cd d:\desktop_folder\01_claudecodeleak\ai_claim
python scripts\import_external_context.py
```

Script nay copy:

- report handoff quan trong vao `docs/handoffs/`
- benchmark cases ngoai vao `data/benchmarks/`
- duel runs moi nhat vao `data/imported_runs/`

### 8. Build de dua len web

```powershell
cd d:\desktop_folder\01_claudecodeleak\ai_claim
docker build -t ai-claim-web .
docker run --rm -p 9780:9780 --env-file .env.local ai-claim-web
```

Tai lieu chi tiet nam o `docs/09_goi_ai_claim_len_web.md`.

### 9. Kiem tra stack production

```powershell
cd d:\desktop_folder\01_claudecodeleak\ai_claim
python scripts\check_runtime_stack.py
```

### 10. Chay agent_claude qua 9router

- Dung env mau: `configs/agent_claude_router.env.example`
- Route health co the xem ngay tren:
  - `/api/system/status`
  - `/api/production-readiness`

## Trang thai hien tai

Ban scaffold nay da co:

- benchmark analyzer doc duoc artifact Pathway vs agent_claude;
- reasoning-gap explainer va scorecard tong hop;
- domain policy de khoa `agent_claude` vao pham vi Neo4j y te/bao hiem;
- Azure OpenAI adapter o muc backend;
- Azure reasoning agent loop co tool calling voi Neo4j toolkit va knowledge surface;
- knowledge registry co scan/upload/catalog version theo file va snapshot lich su version;
- knowledge tree + `CLAUDE.md` templates cho protocols, rules, benefits, services, symptoms, diseases, notes;
- disease workspace memory cho benchmark/feedback/notes;
- ingest support matrix de biet root nao direct ingest duoc vao Pathway, root nao moi catalog;
- Pathway bridge de upload mot lan vao local registry + Pathway knowledge registry, doc impact report, graph trace va text workspace;
- live duel runner goi ca `Pathway` API va `ai_claim` runtime, luu artifact trong `data/duel_runs/`;
- structured reasoning output theo line-level medical / insurance / final decision, co tong hop:
  - `duration_ms`
  - `llm_call_count`
  - `tool_call_count`
  - `tool_call_breakdown`
  - `usage.prompt_tokens/completion_tokens/total_tokens`
- Pathway observable metrics:
  - `medical_metrics`
  - `adjudicate_metrics`
  - `graph_health`
- FastAPI bootstrap + dashboard HTML.

Phan chua co day du 100%:

- runtime tool-using loop thay the hoan toan `agent_claude` release binary;
- ingest thuc chien toan bo PDF/Excel/rulebook trong `ai_claim` theo 1 pipeline duy nhat van chua xong; hien da co bridge sang Pathway va support matrix ro hon;
- telemetry raw token/query tu phia Pathway van chua co;
- adjudication engine day du nhu production cho moi benh va moi hop dong van can lam giau them graph/data.

Nhung day da la bo khung source code doc lap can thiet de day len cloud va phat trien tiep.
