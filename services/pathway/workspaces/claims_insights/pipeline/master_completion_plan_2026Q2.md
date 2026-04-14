# AZINSU Master Completion Plan (2026Q2)

## 1) Muc tieu

Hoan thien toan bo chuoi thuat toan va trien khai van hanh cho bai toan adjudication:

1. Du lieu dau vao duoc chuan hoa + enrich day du.
2. Engine suy luan clinical + contract + anomaly ra quyet dinh co giai thich.
3. Vong lap hoc tu review (onboarding/remap) chay duoc hang ngay.
4. Pipeline co the deploy, monitor, rollback, va audit.

## 2) Baseline hien tai (snapshot)

- Benchmark MVP: `80.81%` (99 dong), confusion `TP=75, FP=18, TN=5, FN=1`.
- Service-Disease Matrix: `1587` links, service-link coverage `11.17%`.
- Contract rules machine-readable: `6` contracts, `30` exclusion items, `9` exclusion groups.
- Observation extraction:
  - `83,922` observation rows
  - node-ready `85.24%`
  - concept-mapped `63.18%`
- Onboarding queue:
  - `4708` raw names can review
  - `2455` new concept/service
  - `1741` service alias gap
  - `512` result parser review

## 3) Pham vi "con do" can hoan tat

### A. Data/Knowledge

1. Phase 2 enrichment LLM dang blocked/khong on dinh.
2. Phase 4 validation + quality gates chua co script production.
3. Coverage matrix disease-service thap (11.17%), chua du de suy luan manh.
4. Observation onboarding/remap da co khung, nhung chua chay curate theo vong sprint.

### B. Reasoning Engine

1. Clinical necessity hien tai van la heuristic fallback, chua temporal-aware day du.
2. Contract eligibility dang o muc abstraction, chua clause-level precedence engine.
3. Unified adjudication chua co gate "high-confidence auto / low-confidence manual" theo policy.

### C. Deployment/Ops

1. Chua co release train ro rang (nightly jobs + artifact versioning).
2. Chua co gate test bat buoc truoc deploy.
3. Chua co monitoring dashboard cho quality drift.

## 4) Definition of Done (DoD)

## 4.1 Data Foundation Done

- Enriched codebook co quality gates tu dong, reject ban ghi loi schema/logic.
- Matrix coverage service-link >= `35%` (moc Q2), co diem evidences ro rang.
- Observation:
  - node-ready >= `92%`
  - concept-mapped >= `80%`
  - queue backlog giam >= `60%` so voi baseline.

## 4.2 Reasoning Done

- Adjudication benchmark 99 dong >= `88%`.
- Precision nhom PAYMENT >= `90%` (giam false positive).
- Co decision trace day du: clinical evidence + clause path + anomaly flags.

## 4.3 Deployment Done

- 1 lenh orchestrator chay end-to-end va tao artifacts versioned.
- Co smoke tests + regression tests + rollback path.
- Co report health sau moi lan chay (coverage, accuracy, drift).

## 5) Lo trinh trien khai (6 sprint)

Moi sprint 1-2 tuan.

### Sprint 1 - Stabilize enrichment core

Muc tieu:
- Giai quyet Phase 2 enrich reliability (khong empty/invalid outputs).
- Hoan tat script validation phase 4.

Deliverables:
1. `03_enrich/enrich_llm_batch.py` hardening:
   - strict JSON schema validation
   - retry/backoff + fail-fast logs
   - checkpoint resume chac chan.
2. Script moi `03_enrich/validate_enrichment.py`:
   - schema checks
   - range sanity checks
   - contradiction checks.
3. Output:
   - `enriched_codebook_p2.json`
   - `enrichment_validation_report.json`.

Gate:
- Khong con batch crash im lang.
- Ty le entry valid >= `95%`.

### Sprint 2 - Observation onboarding execution loop

Muc tieu:
- Bien queue 4708 thanh backlog co uu tien va xu ly theo lo.

Deliverables:
1. Curate top priority overrides theo queue class.
2. Remap batch va do tac dong moi ngay.
3. Tao "onboarding SLA":
   - top 500 raw names xu ly xong.
4. Parser refinement cho `result_parser_review`.

Gate:
- queue item count giam >= `30%`.
- concept-mapped rows tang >= `+10pp`.

### Sprint 3 - Clinical necessity engine v2 (temporal-aware)

Muc tieu:
- Thay fallback heuristic bang scoring engine co logic thoi gian.

Deliverables:
1. Tach module moi:
   - `pipeline/clinical_necessity_v2.py`
2. Inputs:
   - matrix + observation + diagnosis context + temporal phase.
3. Outputs:
   - role-based reasoning (`screening`, `diagnostic`, `rule_out`, `monitoring`)
   - confidence score + evidence path.

Gate:
- Giam FP it nhat `30%` tren benchmark.
- Co test unit cho cac pattern ENT/viral/lab bundles.

### Sprint 4 - Contract clause engine v2

Muc tieu:
- Chuyen tu contract abstraction sang clause retrieval + precedence.

Deliverables:
1. Build clause library chuan:
   - `06_insurance/contract_clause_library.json`
2. Module moi:
   - `pipeline/contract_clause_engine_v2.py`
3. Rule precedence:
   - coverage -> waiting -> exclusion -> sublimit -> copay -> exception.

Gate:
- Moi decision contract co clause trace.
- Khong con "unknown_contract_fallback" cho contracts da onboard.

### Sprint 5 - Unified adjudication v2 + anomaly v2

Muc tieu:
- Hop nhat clinical v2 + contract v2 + anomaly v2 thanh 1 engine production.

Deliverables:
1. `pipeline/adjudication_v2.py` + benchmark runner.
2. Decision bands:
   - auto_approve
   - auto_deny
   - manual_review.
3. Explainability payload chuan cho audit.

Gate:
- Accuracy >= `88%` tren benchmark 99 dong.
- Manual review rate nam trong band muc tieu (de review team van hanh duoc).

### Sprint 6 - Production deployment & guardrails

Muc tieu:
- Chay duoc production mode va co monitoring.

Deliverables:
1. Orchestrator runbook:
   - daily ingestion
   - nightly remap/retrain artifacts
   - benchmark regression.
2. Deployment:
   - dockerized pipeline jobs
   - environment config and secrets guide.
3. Monitoring outputs:
   - quality KPIs JSON/MD after each run
   - drift alerts (coverage drop, confidence drop, FP spike).

Gate:
- 7 ngay chay lien tuc khong vo pipeline.
- Co rollback artifact N-1.

## 6) Song song ky thuat + van hanh

## 6.1 Test strategy bat buoc

- Unit test: parser, mapper, clause evaluator.
- Integration test: enrich -> queue -> remap -> adjudication.
- Regression set:
  - benchmark 99 dong
  - them 1 bo holdout claim set theo tung insurer.

## 6.2 Artifact versioning

- Quy uoc:
  - `output/YYYYMMDD_HHMM/<artifact>.json`
- Luu metadata:
  - git commit
  - script version
  - input snapshot hash.

## 6.3 Release gate

Chi deploy neu dong thoi dat:

1. benchmark khong giam so voi release truoc
2. schema validation pass 100%
3. no critical parser errors
4. queue growth khong tang dot bien.

## 7) Ke hoach 30/60/90 ngay

### 30 ngay

- Dong Sprint 1 + 2.
- Co vong onboarding/remap van hanh that.
- Coverage observation cai thien ro.

### 60 ngay

- Dong Sprint 3 + 4.
- Clinical v2 + Contract v2 co trace.

### 90 ngay

- Dong Sprint 5 + 6.
- Adjudication v2 + deployment + monitoring on.

## 8) Uu tien bat dau ngay (execution order)

1. Hardening `enrich_llm_batch.py` + tao `validate_enrichment.py`.
2. Xu ly top 500 items tu onboarding queue.
3. Bat dau clinical necessity v2 voi bo test hoi suc/ENT/viral.

## 9) Nguyen tac ra quyet dinh trong qua trinh thuc hien

- Khong doi phuc tap lay nhanh: moi buoc deu phai do duoc.
- Khong map bua de "dep so": uu tien dung + audit.
- Khong bo review queue: case mo ho phai vao manual review.
- Khong de model overwrite contract logic: contract clause la ranh gioi cuoi.
