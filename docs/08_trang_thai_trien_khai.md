# Trang thai trien khai `ai_claim`

## Da hoan thanh trong source code

### 1. Project doc lap, san sang dua len cloud

- `README.md`
- `pyproject.toml`
- `configs/`
- `docs/`
- `src/ai_claim/`
- `scripts/`
- `data/knowledge/`

### 2. Phan tich chenh lech Pathway vs `agent_claude`

- Co benchmark analyzer doc run duel cu.
- Rut duoc:
  - thoi gian chay;
  - token input/output/cache cua `agent_claude`;
  - so luot Neo4j MCP;
  - so turn agent;
  - reasoning gap summary.

### 3. Domain lock cho `agent_claude`

- Allowlist built-in tools.
- Allowlist MCP Neo4j.
- Denylist edit/web/agent/skill tools.
- Launch spec generator voi `--bare`, `--allowedTools`, `--disallowedTools`, `--mcp-config`, `--add-dir`.

### 4. Azure OpenAI path

- Adapter backend.
- `.env.local` loading.
- Sample env file.
- Reasoning runtime khoi tao tu Azure config.

### 5. Knowledge operating skeleton

- Knowledge roots.
- Disease workspace scaffold.
- Registry scan.
- Upload file vao root.
- Version theo SHA1 file.
- Snapshot lich su version vao `data/runtime/knowledge_versions`.
- Impact hint theo loai tri thuc.
- Knowledge surface search/read cho notes, benchmark, feedback, workspace memory.

### 6. Bridge tu `ai_claim` sang Pathway

- Support matrix cho tung root:
  - root nao co direct ingest;
  - root nao chi catalog;
  - root nao moi o local registry.
- Pathway knowledge bridge:
  - bootstrap;
  - list assets;
  - impact report;
  - graph trace;
  - text workspace;
  - run status.
- Upload file mot lan:
  - vao local registry cua `ai_claim`;
  - dong thoi day sang Pathway knowledge registry.

### 7. UI/UX bootstrap

- Dashboard hien thi:
  - health;
  - domain policy;
  - knowledge roots;
  - ingest support matrix;
  - benchmark summary;
  - knowledge registry scan;
  - upload file;
  - bridge upload vao Pathway;
  - Pathway asset trace;
  - asset catalog;
  - knowledge surface search;
  - live duel runner.

### 8. Runtime reasoning skeleton

- Neo4j toolkit query truc tiep.
- Azure reasoning agent loop voi tool calling.
- Sample case Meniere.
- API `/api/reasoning/run`.
- API `/api/knowledge/surface/search`.
- API `/api/duel/run`.
- Live duel runner luu artifact vao `data/duel_runs/`.
- Tong hop them metric co the quan sat tu Pathway:
  - verification plan count;
  - evidence ledger count;
  - decision breakdown;
  - graph health.

## Da chay that

- `python scripts/bootstrap_knowledge_tree.py`
- `python scripts/analyze_duel_run.py --run-dir D:\desktop_folder\01_claudecodeleak\pathway\notebooklm\data\duel_runs\pathway_vs_agent_claude\20260409_224555_duel_meniere_001`
- `python scripts/smoke_test.py`
- `python scripts/run_reasoning_case.py --case-file data\benchmarks\sample_case_meniere.json`
- `python scripts/run_live_duel.py --case-file data\benchmarks\sample_case_meniere.json`
- live bridge upload qua API `ai_claim -> Pathway`

## Ket qua xac minh live ngay 2026-04-10

### 1. Docker / Neo4j / Pathway

- `pathway-neo4j-1` len `healthy`.
- `pathway-api-1` len `healthy`.
- `ai_claim` ket noi duoc `bolt://localhost:7688`.
- `neo4j_health` tra `ready`.

### 2. Azure OpenAI

- `ai_claim/.env.local` da duoc nap.
- `smoke_test.py` xac nhan `azure_openai_configured = true`.
- `run_reasoning_case.py` da goi live Azure OpenAI thanh cong.

### 3. Reasoning case live

- Case mau: `meniere_demo_001`.
- Runtime da tra ve:
  - `result`
  - `tool_ledger`
  - `usage.prompt_tokens`
  - `usage.completion_tokens`
  - `usage.total_tokens`
- Runtime da tan dung `knowledge surface` noi bo trong `data/knowledge/diseases/H81_0_meniere`.
- Ket qua live:
  - `Do thinh luc don am -> approve`
  - `ENG -> approve`
  - `MRI so nao - xuong da -> partial_pay`
  - `Sieu am o bung tong quat -> deny`
- Claim-level decision cua `ai_claim` ra `partial_pay`.

### 4. Live duel run

- Run duel moi nhat:
  - `D:\desktop_folder\01_claudecodeleak\ai_claim\data\duel_runs\20260410_115649_meniere_demo_001`
- `ai_claim_azure` hien co telemetry tong toan phien:
  - `duration_ms = 21663.6`
  - `llm_call_count = 7`
  - `tool_call_count = 9`
  - `total_tokens = 77513`
- `Pathway` trong cung run nay da duoc tong hop them:
  - `medical_metrics`
  - `adjudicate_metrics`
  - `graph_health`
- `Pathway` live van tra claim summary deny ca 4 dich vu.

### 5. Bridge upload / ingest live

- Bridge upload catalog-only thanh cong:
  - local asset id: `asset_0e75847dbae284c5`
  - Pathway asset id: `asset_0777a4873130a2`
- Bridge upload + auto ingest thanh cong:
  - Pathway asset id: `asset_4495d421ea25a9`
  - run id: `knowledge_ai_claim_bridge_ingest_protocol_20260410_045612`
  - final run status: `completed`
- Sau upload, `text_workspace` cua Pathway da co noi dung va truy cap duoc tu `ai_claim`.

## Nhung gi da xong that su

- Nen tang `ai_claim` doc lap da dung xong.
- Domain lock cho `agent_claude` da co va sinh launch spec duoc.
- Azure runtime da chay that.
- Neo4j runtime da chay that.
- Benchmark analyzer da chay that.
- Knowledge tree, registry, scan, upload da chay that.
- Version snapshot cho asset local da co.
- Knowledge surface search/read da chay that.
- Pathway bridge upload/catalog/impact/text trace da chay that.
- Live duel runner da chay that.
- Dashboard va API bootstrap da chay that.

## Nhung gi chua the noi la hoan tat 100%

### 1. Telemetry cong bang cho Pathway

- Artifact hien tai van chua co raw token usage cua Pathway.
- Artifact hien tai van chua co raw Neo4j query count cua Pathway.
- Vi vay phan so sanh dinh luong cong bang tuyet doi giua Pathway va `agent_claude` van con thieu telemetry tu phia Pathway.

### 2. Chuyen toan bo nghiep vu Pathway sang runtime moi

- Khung kien truc, domain lock, Azure runtime, Neo4j toolkit, Pathway bridge va dashboard da co.
- Nhung chua the noi toan bo business flow production cua Pathway da duoc di tru het sang `ai_claim`.

### 3. Chat luong reasoning phu thuoc graph live

- Runtime moi da chay duoc va da biet dung disease workspace memory de bu graph coverage.
- Nhung graph live van con mong; neu khong co note/feedback/workspace memory thi Pathway va ca agent van bi tran boi du lieu Neo4j hien co.
- Day la thieu hut du lieu/graph coverage, khong phai loi bootstrap runtime.

## Ket luan

`ai_claim` hien da vuot qua giai doan scaffold. Du an da co source code doc lap, co runtime Azure chay that, co Neo4j chay that, co dashboard, co registry, co benchmark analyzer, co knowledge surface memory, co Pathway bridge va co live duel runner.

Neu hoi "da hoan thanh toan bo moi task trong multi_task.md chua" thi cau tra loi trung thuc hien tai la:

- Chua hoan tat 100% o muc nghiep vu cuoi cung.

Nhung phan con lai chu yeu la:

- bo sung telemetry Pathway de so sanh cong bang;
- di tru sau hon business flow tu Pathway sang `ai_claim`;
- lam giau graph coverage de reasoning case ra quyet dinh tot hon tren du lieu that.
