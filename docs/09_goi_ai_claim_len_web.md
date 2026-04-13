# Gói `ai_claim` lên web

## Mục tiêu

Folder `ai_claim` sau khi gom lại cần đủ 4 nhóm thành phần:

- source backend và dashboard
- cấu hình môi trường mẫu
- knowledge tree và benchmark artifacts tối thiểu
- tài liệu handoff để người vận hành trên web không phải mở repo khác

## Những gì đã được đưa vào trong `ai_claim`

### 1. Source và runtime

- `src/ai_claim/`: backend, duel runner, bridge, reasoning, Neo4j toolkit
- `scripts/`: bootstrap, smoke test, run duel, import external context
- `Dockerfile`: đóng gói app để đưa lên web/container
- `.dockerignore`: loại bỏ file runtime cục bộ và secret local

### 2. Cấu hình deploy

- `configs/app.env.example`: toàn bộ biến môi trường cần cho:
  - `PATHWAY_API_BASE_URL`
  - `NEO4J_URI`
  - `NEO4J_USER`
  - `NEO4J_PASSWORD`
  - `AZURE_OPENAI_*`
- `configs/agent_claude_router.env.example`: env mẫu để `agent_claude` đi qua `9router`
- `configs/agent_claude_sample_prompt.txt`
- `configs/agent_claude_sample_mcp_config.json`

### 3. Dữ liệu và benchmark tối thiểu

- `data/knowledge/`: knowledge tree để thả tài liệu
- `data/benchmarks/`: case và benchmark nội bộ
- `data/imported_runs/`: các run duel được copy về từ workspace ngoài để giữ bằng chứng tại chỗ

### 4. Tài liệu handoff

- `docs/handoffs/report_9router.md`
- `docs/handoffs/report_pathway_agent_claude.md`
- `docs/handoffs/report_test_9router_hc_insurance.md`

## Cách gom dữ liệu ngoài vào `ai_claim`

Chạy:

```powershell
cd d:\desktop_folder\01_claudecodeleak\ai_claim
python scripts\import_external_context.py
```

Script này sẽ copy:

- các report handoff quan trọng
- benchmark case `Ménière` và `Pneumonia`
- các duel run mới nhất từ `pathway/notebooklm/data/duel_runs/pathway_vs_agent_claude/`

## Cách chạy local trước khi đẩy lên web

```powershell
cd d:\desktop_folder\01_claudecodeleak\ai_claim
copy configs\app.env.example .env.local
python scripts\bootstrap_knowledge_tree.py
python scripts\smoke_test.py
python scripts\check_runtime_stack.py
uvicorn src.ai_claim.main:app --host 0.0.0.0 --port 9780
```

## Cách build container

```powershell
cd d:\desktop_folder\01_claudecodeleak\ai_claim
docker build -t ai-claim-web .
docker run --rm -p 9780:9780 --env-file .env.local ai-claim-web
```

## Điều cần nhớ khi đưa lên web

- `ai_claim` hiện đã tự chứa đủ source và docs để deploy.
- Nhưng nếu dùng:
  - `Pathway bridge`
  - `live duel với Pathway`
  - `impact report / graph trace`
  thì môi trường web vẫn phải có `Pathway API` sống ở `PATHWAY_API_BASE_URL`.
- Nếu chỉ dùng:
  - `dashboard`
  - `knowledge registry local`
  - `Azure reasoning`
  - `Neo4j toolkit`
  thì `ai_claim` có thể chạy như app độc lập với Azure + Neo4j.
