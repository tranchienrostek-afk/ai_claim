# PRD — AI Claim Dashboard Phase 2: Hien thi nang luc he thong

**Version**: 1.0  
**Date**: 2026-04-14  
**Status**: Draft  
**Author**: AI Claim Engineering

---

## 1. Boi canh va van de

### 1.1 Hien trang

He thong AI Claim so huu **68 API endpoints** (Pathway) + **35 endpoints** (ai_claim), **11 reasoning tools**, **17 Neo4j query methods**, he thong adjudication 3 agent voi conflict resolution matrix 9 luat, pipeline ingestion 5 pha, va nhieu thuat toan tien tien (RRF fusion, disease hypothesis scoring, experience memory, canonical resolver).

**Van de**: Dashboard hien tai chi the hien **~30% nang luc** cua backend. Nguoi dung thay duoc system status va basic operations nhung KHONG thay duoc:

- Qua trinh suy luan (reasoning path) cua AI agent
- Bang chung (evidence trace) tu graph va knowledge surface
- Thuat toan phan xu (adjudication) voi conflict resolution
- Chuoi fallback khi truy van disease-service
- Lich su version cua knowledge assets
- Lab result interpretation va disease hypothesis scoring
- Pipeline ingestion 5 pha voi decision gates
- Benchmark 2,091 cases va scoring breakdown
- Contract/benefit/exclusion explorer
- Canonical service resolver va price comparison

### 1.2 He qua

- Stakeholder khong danh gia dung nang luc cua he thong
- Dev team khong co feedback loop de tinh chinh thuat toan
- Operator khong co cong cu de debug khi quyet dinh sai
- Khong co transparency cho quy trinh audit y te/bao hiem

---

## 2. Muc tieu Phase 2

| # | Muc tieu | Do do thanh cong |
|---|----------|-----------------|
| M1 | The hien day du reasoning path cua AI agent | 100% tool calls visible, evidence trace co the click-through |
| M2 | Truc quan hoa adjudication pipeline | 3 agent verdicts + conflict matrix + final decision visible |
| M3 | Mo graph explorer cho disease/contract/benefit | Browse 224 diseases, 7 contracts, 160 benefits tu dashboard |
| M4 | Pipeline ingestion monitoring real-time | WebSocket log stream + 5-phase progress + decision gate UI |
| M5 | Benchmark dashboard thay cho CLI | 2,091 cases viewable, accuracy/precision/recall metrics |
| M6 | Evidence trace end-to-end | Tu claim input → disease match → service check → contract → final decision |

---

## 3. Phan tich nang luc an (Backend vs UI Gap)

### 3.1 Reasoning Agent — Hien thi 20%

**Backend co**:
- 11 tools voi function calling loop (max 10 turns)
- `tool_ledger`: mang cac tool call voi args + result preview
- `reasoning_path`: mo ta tu nhien con duong suy luan
- `evidence_trace`: citations tu medical + insurance
- `tool_call_breakdown`: dem theo ten tool
- `usage`: token counts (prompt/completion/total)
- `duration_ms`: thoi gian thuc hien
- `disease_workspace_hint`: auto-detect disease

**Dashboard hien thi**:
- Chi ket qua cuoi cung (JSON blob)
- Khong co tool ledger inspection
- Khong co reasoning path visualization
- Khong co evidence trace drill-down

### 3.2 Multi-Agent Adjudication — Hien thi 0%

**Backend co**:
- **Clinical Agent**: medical necessity scoring, service-disease matrix, BHYT price reference, ontology-first reasoning
- **Contract Agent**: contract clause evaluation, exclusion matching, cross-contract risk priors, benefit hints
- **Anomaly Agent**: price outlier (3x-5x BHYT), duplicate detection, quantity checks
- **Adjudicator**: 9-rule conflict resolution matrix, bao gom rule #4 "double uncertainty = deny"
- **Canonical Resolver**: service_code → BYT canonical (MAANHXA, price, classification)

**Dashboard hien thi**: Khong co gi. Tat ca logic nay invisible.

### 3.3 Neo4j Graph Explorer — Hien thi 35%

**Backend co** (17 query methods):
- `query_ci_disease_snapshot()` — disease + signs + services (1 query)
- `query_contract_stats()` — contract summary voi benefit/exclusion counts
- `query_benefits_for_contract()` — benefit list voi relevance ranking
- `query_exclusions_by_contract()` — exclusions ordered by usage frequency
- `query_service_exclusions()` — service-level exclusion checks
- `query_clinical_service_info()` — canonical service + related diseases
- `trace_service_evidence()` — combined medical + insurance evidence
- `list_recent_ci_diseases()` — browsable disease list

**Dashboard hien thi**: Chi co health summary + mapping audit + 1 disease-service lookup.

### 3.4 Knowledge Management — Hien thi 60%

**Backend co**:
- SHA1 dedup voi duplicate group detection
- Version history (snapshot moi version voi sha1, path, timestamp)
- Impact hints (moi root co risk assessment rieng)
- Disease workspace creation
- Text search voi token scoring + diacritic normalization

**Dashboard hien thi**: Upload + asset table + search. Thieu version history, diff, rollback, disease workspace UI.

### 3.5 Pipeline Ingestion — Hien thi 10%

**Backend co**:
- 5 pha: PDF Analysis → Pipeline Design → Ingestion → Quality Testing → Self-Improvement
- 2 decision gates (post_design, post_test) voi human/Claude approval
- WebSocket real-time log streaming (`WS /ws/pipeline/{run_id}`)
- Experience memory (hoc tu pipeline runs truoc)
- Safety hooks (block destructive commands)
- LLM judge scoring (0-1.0 accuracy)
- Max 5 optimization iterations voi rollback on accuracy drop

**Dashboard hien thi**: Hau nhu khong co (chi co run button trong Pathway dashboard, khong co trong ai_claim).

### 3.6 Benchmark System — Hien thi 15%

**Backend co**:
- 2,091 gold-labeled service lines
- Accuracy/precision/recall/F1/specificity/balanced_accuracy metrics
- Per-service-line results (predicted vs gold, resolution_rule, confidence)
- Live duel: ai_claim vs Pathway side-by-side
- Gap analysis: strengths/weaknesses per system
- Tool call breakdown comparison

**Dashboard hien thi**: Chi co run_dir input + JSON output. Khong co visual metrics, khong co case-by-case inspection.

---

## 4. Thiet ke giai phap — 10 Module moi

### Module 1: Reasoning Inspector (M1)

**Muc dich**: Truc quan hoa toan bo qua trinh suy luan cua AI agent.

**UI Components**:
- **Timeline view**: Moi tool call la 1 node tren timeline (icon theo tool type)
- **Tool detail panel**: Click vao node → hien args, response, duration
- **Reasoning path narrative**: Markdown render cua `reasoning_path`
- **Evidence sidebar**: Click-through tu evidence_trace → source document/graph node
- **Token meter**: Bieu do usage (prompt vs completion tokens)
- **Turn counter**: Hien so LLM calls va tool calls

**API su dung**: `POST /api/reasoning/run` (da co day du data trong response)

**Wireframe**:
```
┌─────────────────────────────────────────────────────┐
│  REASONING INSPECTOR                                │
├──────────┬──────────────────────────────────────────┤
│ Timeline │  Tool Detail                             │
│          │  ┌────────────────────────────────────┐  │
│ ● graph  │  │ Tool: query_ci_disease_snapshot    │  │
│   health │  │ Args: {"disease_name": "Meniere"}  │  │
│          │  │ Duration: 342ms                    │  │
│ ● query  │  │ Result: 8 signs, 12 services      │  │
│   disease│  │ ──────────────────────────────     │  │
│          │  │ [View Full Response]               │  │
│ ● query  │  └────────────────────────────────────┘  │
│   benefits                                          │
│          │  Evidence Trace                           │
│ ● trace  │  ┌────────────────────────────────────┐  │
│   evidence  │ Medical: 3 sources                 │  │
│          │  │ Insurance: 2 sources               │  │
│ ● search │  │ Knowledge: 1 source                │  │
│   surface│  └────────────────────────────────────┘  │
│          │                                          │
│ RESULT   │  Tokens: ████████░░ 2,847 / 4,096       │
│ approve  │  LLM Calls: 4  Tool Calls: 5            │
│ conf:0.87│  Duration: 11,542ms                      │
└──────────┴──────────────────────────────────────────┘
```

**Do phuc tap**: Medium  
**Phu thuoc**: Khong can backend moi — data da co trong response hien tai

---

### Module 2: Adjudication Trace (M2)

**Muc dich**: Hien thi 3-agent pipeline va conflict resolution matrix.

**UI Components**:
- **3-column verdict panel**: Clinical | Contract | Anomaly — moi cot hien decision + confidence + evidence
- **Conflict resolution flow**: Flowchart hien rule nao duoc apply (highlight rule #)
- **Final decision banner**: APPROVE/DENY/REVIEW/PARTIAL voi confidence bar
- **Rule explanation**: Tooltip giai thich tai sao rule X duoc chon
- **Price comparison**: BHYT reference price vs claimed price (tu canonical resolver)
- **Exclusion flags**: List cac exclusion patterns matched

**API can bo sung**:
- `POST /api/adjudicate/v2/trace` — Tra ve per-agent verdicts + resolution_rule applied (Pathway side)
- Hoac: mo rong `POST /api/reasoning/run` de include adjudication breakdown

**Wireframe**:
```
┌────────────────────────────────────────────────────────┐
│  ADJUDICATION TRACE — Claim #CLM-2024-001              │
├──────────────┬──────────────┬──────────────┬───────────┤
│  CLINICAL    │  CONTRACT    │  ANOMALY     │  FINAL    │
│  ✓ approve   │  ? uncertain │  ✓ approve   │  ✗ DENY  │
│  conf: 0.72  │  conf: 0.30  │  conf: 0.85  │  conf:0.7│
│              │              │              │           │
│  Evidence:   │  Evidence:   │  Evidence:   │  Rule #6  │
│  - Matrix ✓  │  - No clause │  - Price OK  │  Double   │
│  - Ontology ✓│  - No benefit│  - No dups   │  Uncert.  │
│  - BHYT ref  │    match     │  - Qty OK    │  = Deny   │
├──────────────┴──────────────┴──────────────┴───────────┤
│  Conflict Resolution Flow:                              │
│  Rule 1 (hard deny) → skip                              │
│  Rule 2 (clinical deny) → skip                          │
│  ...                                                    │
│  Rule 6 (double uncertainty) → ★ MATCHED               │
│    clinical=approve(0.72) + contract=uncertain(0.30)    │
│    → DENY (confidence override)                         │
└────────────────────────────────────────────────────────┘
```

**Do phuc tap**: High  
**Phu thuoc**: Can Pathway API extension hoac ai_claim wrapping logic

---

### Module 3: Graph Explorer (M3)

**Muc dich**: Browse toan bo Neo4j graph tu dashboard.

**UI Components**:
- **Disease browser**: List 224 diseases voi search/filter, click → detail panel
- **Disease detail**: Signs (frequency), Services (coverage %), related diseases
- **Contract browser**: 7 contracts voi benefit/exclusion counts
- **Benefit lookup**: Search by name, filter by contract
- **Exclusion explorer**: Ordered by usage, voi reason text
- **Service info**: Canonical name, BYT price, classification, related diseases
- **Evidence trace**: Service → medical support + insurance support (tu `trace_service_evidence`)
- **Fallback indicator**: Badge "primary" hoac "hypothesis_seed" cho disease-service results

**API su dung** (tat ca da co):
- `GET /api/neo4j/health`
- `list_recent_ci_diseases` (qua reasoning) hoac can endpoint moi
- `query_ci_disease_snapshot` → can expose as `GET /api/neo4j/disease/{id}/snapshot`
- `query_contract_stats` → can expose as `GET /api/neo4j/contract/{id}/stats`
- `query_benefits_for_contract` → `GET /api/neo4j/contract/{id}/benefits`
- `query_exclusions_by_contract` → `GET /api/neo4j/contract/{id}/exclusions`
- `trace_service_evidence` → `GET /api/neo4j/service/{code}/evidence`

**API can bo sung** (6 endpoints wrapper tu neo4j_toolkit):
```python
GET /api/neo4j/diseases                        # list_recent_ci_diseases
GET /api/neo4j/diseases/{id}/snapshot           # query_ci_disease_snapshot
GET /api/neo4j/contracts/{id}/stats             # query_contract_stats
GET /api/neo4j/contracts/{id}/benefits          # query_benefits_for_contract
GET /api/neo4j/contracts/{id}/exclusions        # query_exclusions_by_contract
GET /api/neo4j/services/{code}/evidence         # trace_service_evidence
```

**Do phuc tap**: Medium  
**Phu thuoc**: 6 endpoint wrappers trong main.py

---

### Module 4: Pipeline Monitor (M4)

**Muc dich**: Real-time monitoring cua 5-phase ingestion pipeline.

**UI Components**:
- **Phase progress bar**: 5 steps voi status (pending/running/done/failed)
- **Decision gate dialog**: Khi pipeline pause, hien button: Continue / Optimize / Abort
- **Log stream**: WebSocket feed real-time (terminal-style)
- **Artifact inspector**: Click vao phase → xem output (analysis.json, pipeline_config.json, test_report.json)
- **Accuracy gauge**: Test accuracy 0-100% voi target line (80%)
- **Optimization tracker**: So iterations (0-5), accuracy trend chart

**API su dung**:
- `POST /api/ingest` (Pathway) → bat dau pipeline
- `WS /ws/pipeline/{run_id}` (Pathway) → real-time logs
- `GET /api/ingest/{run_id}` (Pathway) → poll status
- `POST /api/ingest/{run_id}/control` (Pathway) → human decision

**API proxy can bo sung** (ai_claim wrapping Pathway):
```python
POST /api/pipeline/start                        # proxy to Pathway /api/ingest
GET  /api/pipeline/{run_id}/status              # proxy to Pathway /api/ingest/{run_id}
POST /api/pipeline/{run_id}/control             # proxy to Pathway /api/ingest/{run_id}/control
WS   /ws/pipeline/{run_id}                      # proxy WebSocket
GET  /api/pipeline/runs                         # proxy to Pathway /api/pipeline-runs
```

**Wireframe**:
```
┌─────────────────────────────────────────────────┐
│  PIPELINE MONITOR — Run #20260414_083000        │
├─────────────────────────────────────────────────┤
│  ● Analysis → ● Design → ◐ Ingest → ○ Test → ○ Opt │
│                           ▲ running              │
├──────────────────────────┬──────────────────────┤
│  Live Logs               │  Artifacts           │
│  ┌────────────────────┐  │  analysis.json ✓     │
│  │ [08:30:12] Phase 2 │  │  pipeline_config ✓   │
│  │ [08:30:15] Chunk.. │  │  ingestion_result ◐  │
│  │ [08:30:18] Entity..│  │  test_report ○       │
│  │ [08:30:22] 847/... │  │  optimization ○      │
│  └────────────────────┘  │                      │
├──────────────────────────┴──────────────────────┤
│  Decision Gate: POST_DESIGN                      │
│  [Continue to Ingestion] [Abort Run]             │
└─────────────────────────────────────────────────┘
```

**Do phuc tap**: High  
**Phu thuoc**: WebSocket proxy, Pathway API access

---

### Module 5: Benchmark Dashboard (M5)

**Muc dich**: Visual benchmark results thay cho CLI.

**UI Components**:
- **Summary cards**: Accuracy %, Precision, Recall, F1, Balanced Accuracy
- **Confusion matrix**: 2x2 grid (APPROVE/DENY predicted vs gold)
- **Per-case table**: Scrollable list voi predicted vs gold, resolution_rule, confidence, status (correct/wrong)
- **Filter**: By decision type, by confidence range, by resolution rule
- **Duel comparison**: Side-by-side ai_claim vs Pathway scores
- **Gap analysis panel**: Strengths/weaknesses tu benchmark_analyzer

**API can bo sung**:
```python
POST /api/benchmark/run                         # trigger benchmark (wraps benchmark_runner)
GET  /api/benchmark/results                     # latest benchmark results
GET  /api/benchmark/cases?decision=&rule=&page= # paginated case list
GET  /api/benchmark/confusion-matrix            # 2x2 matrix counts
```

**Do phuc tap**: Medium  
**Phu thuoc**: benchmark_runner.py integration, JSONL parsing

---

### Module 6: Evidence Trace Viewer (M6)

**Muc dich**: End-to-end trace tu claim → decision voi moi link co the click.

**UI Components**:
- **Claim input summary**: Patient, diagnosis, services requested
- **Disease match panel**: ICD code → CIDisease node (primary vs fallback)
- **Service checklist**: Moi service line → medical necessity check + insurance check
- **Evidence links**: Click → xem Neo4j node/relationship hoac knowledge file
- **Decision chain**: Clinical → Contract → Anomaly → Final (voi confidence bars)
- **Export**: PDF report cho audit purposes

**API su dung**: Ket hop `trace_service_evidence` + `reasoning/run` output

**Do phuc tap**: High  
**Phu thuoc**: Modules 1, 2, 3

---

### Module 7: Contract & Benefit Explorer (M3 sub)

**Muc dich**: Browse insurance graph domain.

**UI Components**:
- **Contract list**: 7 contracts voi product_name, insurer, mode, benefit_count, exclusion_count
- **Benefit table**: Searchable, voi major_section, subsection, canonical_name
- **Exclusion table**: Ordered by usage, voi reason text, linked ExclusionReason
- **Service-benefit bridge**: Show CIService → FALLS_UNDER_BENEFIT connections
- **Plan coverage**: ContractPlan → COVERS_BENEFIT drill-down

**Do phuc tap**: Low-Medium  
**Phu thuoc**: 3 endpoint wrappers (contract_stats, benefits, exclusions)

---

### Module 8: Knowledge Version History (M4 sub)

**Muc dich**: Version tracking va diff cho knowledge assets.

**UI Components**:
- **Version timeline**: Moi version la 1 dot voi timestamp, sha1, size
- **Diff viewer**: So sanh 2 versions (text diff)
- **Rollback button**: Restore previous version
- **Impact badge**: Hien impact_hint cua root (affects medical graph, insurance graph, etc.)
- **Duplicate group panel**: Click → list tat ca files cung SHA1

**API can bo sung**:
```python
GET /api/knowledge/assets/{id}/history          # version list voi sha1, path, timestamp
GET /api/knowledge/assets/{id}/versions/{v}/content  # read specific version
POST /api/knowledge/assets/{id}/rollback?version=    # restore old version
```

**Do phuc tap**: Medium  
**Phu thuoc**: knowledge_registry.py da co versioning logic

---

### Module 9: Disease Workspace Manager

**Muc dich**: Quan ly disease-specific directories cho reasoning enrichment.

**UI Components**:
- **Disease workspace list**: Directories trong `data/knowledge/diseases/`
- **Create workspace button**: Input disease_key + disease_name
- **Workspace content**: Files trong workspace (notes, benchmarks, feedback)
- **Link to reasoning**: "Run reasoning on this disease" shortcut

**API su dung**: `POST /api/knowledge/disease-workspace` (da co)

**Do phuc tap**: Low  
**Phu thuoc**: Khong can backend moi

---

### Module 10: Lab Result & Hypothesis Viewer

**Muc dich**: Hien thi disease hypothesis scoring va lab result interpretation.

**UI Components**:
- **Hypothesis table**: Disease name, ICD, status (active/ruled_out/differential), composite score
- **Score breakdown**: sign_score, service_score, lab_support, lab_exclusion, memory_prior, graph_context
- **Supporting evidence**: matched signs, matched services, graph snippets
- **Lab results panel**: Lab values vs reference ranges, abnormality flags
- **Disease ranking**: Sorted by composite score, visual bars

**API can bo sung**:
```python
POST /api/reasoning/hypothesis                  # run disease_hypothesis_engine on case
POST /api/reasoning/lab-interpret               # run lab_result_interpreter on lab values
```

**Do phuc tap**: High  
**Phu thuoc**: Pipeline modules (disease_hypothesis_engine.py, lab_result_interpreter.py)

---

## 5. Do uu tien va Roadmap

### Phase 2A — Core Visibility (2-3 tuan)

| # | Module | Do uu tien | Ly do |
|---|--------|-----------|-------|
| 1 | Reasoning Inspector | **P0** | Gia tri cao nhat — hien thi AI dang "nghi gi" |
| 2 | Graph Explorer | **P0** | 224 diseases + 7 contracts hien khong browsable |
| 3 | Contract & Benefit Explorer | **P1** | Supplement cho Graph Explorer |
| 4 | Disease Workspace Manager | **P2** | Quick win, backend da co |

**Backend changes**: 6 endpoint wrappers trong main.py  
**Frontend changes**: 3 views moi trong dashboard.html

### Phase 2B — Adjudication & Evidence (3-4 tuan)

| # | Module | Do uu tien | Ly do |
|---|--------|-----------|-------|
| 5 | Adjudication Trace | **P0** | Transparency cho quy trinh phan xu |
| 6 | Evidence Trace Viewer | **P1** | End-to-end audit trail |
| 7 | Lab Result & Hypothesis | **P1** | Hien thi thuat toan phuc tap nhat |

**Backend changes**: Pathway API extension hoac ai_claim wrapping  
**Frontend changes**: 2 views moi + data panels

### Phase 2C — Operations & Monitoring (2-3 tuan)

| # | Module | Do uu tien | Ly do |
|---|--------|-----------|-------|
| 8 | Pipeline Monitor | **P0** | 5-phase pipeline hien khong co UI |
| 9 | Benchmark Dashboard | **P1** | Thay CLI bang visual metrics |
| 10 | Knowledge Version History | **P2** | Enhancement cho document management |

**Backend changes**: WebSocket proxy, benchmark API, version history endpoints  
**Frontend changes**: 2 views moi + WebSocket integration

---

## 6. Yeu cau ky thuat

### 6.1 Backend (ai_claim main.py)

**Endpoints moi can them** (tong 15):

```
# Graph Explorer (6)
GET /api/neo4j/diseases
GET /api/neo4j/diseases/{id}/snapshot
GET /api/neo4j/contracts/{id}/stats
GET /api/neo4j/contracts/{id}/benefits
GET /api/neo4j/contracts/{id}/exclusions
GET /api/neo4j/services/{code}/evidence

# Pipeline Proxy (5)
POST /api/pipeline/start
GET  /api/pipeline/{run_id}/status
POST /api/pipeline/{run_id}/control
WS   /ws/pipeline/{run_id}
GET  /api/pipeline/runs

# Benchmark (2)
POST /api/benchmark/run
GET  /api/benchmark/cases

# Knowledge (2)
GET  /api/knowledge/assets/{id}/history
POST /api/knowledge/assets/{id}/rollback
```

### 6.2 Frontend (dashboard.html)

**Views moi**: 4 tabs bo sung hoac sub-panels trong existing views:
1. Reasoning Inspector → sub-panel trong view Reasoning
2. Graph Explorer → thay the view Graph hien tai
3. Adjudication Trace → sub-panel trong view Reasoning
4. Pipeline Monitor → view moi hoac sub-panel trong view Documents

### 6.3 Dependencies

- WebSocket support trong ai_claim (hien chua co — can them `websockets` dependency)
- Pathway API accessible tu ai_claim container (da co qua Docker network)
- D3.js hoac Chart.js cho visualization (co the dung CDN)

---

## 7. Metrics thanh cong

| Metric | Hien tai | Muc tieu Phase 2 |
|--------|---------|-------------------|
| Backend capabilities visible in UI | ~30% | 80%+ |
| Endpoints exercised by dashboard | 15/35 (43%) | 30/50 (60%) |
| Adjudication transparency | 0% | 100% (3 agents + matrix) |
| Graph browsing | 1 query type | 8+ query types |
| Pipeline monitoring | No UI | Full 5-phase + decision gates |
| Benchmark inspection | CLI only | Visual dashboard |
| Evidence trace depth | JSON blob | Click-through per-source |
| Knowledge version tracking | Hidden | Timeline + diff + rollback |

---

## 8. Rui ro va mitigation

| Rui ro | Impact | Mitigation |
|--------|--------|------------|
| Pathway API khong stable trong Docker | High | Health check + retry + graceful degradation |
| Dashboard qua nang (single HTML file) | Medium | Component hoa bang Web Components hoac tach file |
| WebSocket proxy phuc tap | Medium | Bat dau voi polling, upgrade sau |
| Benchmark data qua lon (2,091 cases) | Low | Server-side pagination + lazy load |
| Neo4j query cham voi graph lon | Low | Query limit + caching |

---

## 9. Definition of Done

- [ ] Moi module co unit test cho API endpoints
- [ ] Dashboard load < 3s voi tat ca views
- [ ] Moi tool call trong reasoning co the click → xem detail
- [ ] Adjudication hien thi 3 agent verdicts + rule applied
- [ ] Graph explorer browse duoc 224 diseases
- [ ] Pipeline monitor hien thi 5 phases real-time
- [ ] Benchmark dashboard hien thi accuracy metrics
- [ ] Evidence trace co click-through den source
- [ ] Tat ca views co demo mode fallback (khi backend down)
- [ ] Mobile responsive (sidebar collapse)
