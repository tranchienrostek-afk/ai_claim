# Neo4j Ontology for Insurance Domain

## Overview

This ontology defines the graph model for insurance contract knowledge to support **Problem 2: Contract Compliance** in the Pathway adjudication system.

**Namespace**: `insurance_v1`

---

## Core Node Types

### 1. Insurer (:Insurer)
Nhà bảo hiểm cấp hợp đồng.

**Properties:**
- `insurer_id` (string, UNIQUE) - e.g., "PJICO", "FPT", "BHV"
- `insurer_name` (string) - Tên đầy đủ, e.g., "Công ty Bảo hiểm PJICO"
- `namespace` (string) = "insurance_v1"

**Example:**
```cypher
(:Insurer {insurer_id: "PJICO", insurer_name: "Công ty Bảo hiểm PJICO", namespace: "insurance_v1"})
```

---

### 2. Contract (:Contract)
Hợp đồng bảo hiểm giữa công ty bảo hiểm và khách hàng (FPT, TIN).

**Properties:**
- `contract_id` (string, UNIQUE) - e.g., "FPT-NT-2024", "FPT-NV"
- `contract_name` (string) - e.g., "FPT Ngoại trú 2024"
- `insurer_id` (string) - FK to :Insurer
- `contract_type` (string) - "Ngoại trú", "Nội trú", "Tai nạn", "Sức khỏe cá nhân"
- `effective_date` (date) - Ngày hiệu lực
- `expiry_date` (date) - Ngày hết hạn
- `mode` (string) - "multi_plan", "single_plan"
- `source_file` (string) - Đường dẫn file nguồn
- `namespace` (string) = "insurance_v1"

**Example:**
```cypher
(:Contract {
    contract_id: "FPT-NT-2024",
    contract_name: "FPT Ngoại trú 2024",
    insurer_id: "FPT",
    contract_type: "Ngoại trú",
    mode: "multi_plan",
    source_file: "FPT NT 2024.xlsx",
    namespace: "insurance_v1"
})
```

---

### 3. Plan (:Plan)
Gói bảo hiểm trong hợp đồng (nếu hợp đồng có nhiều gói).

**Properties:**
- `plan_id` (string, UNIQUE) - e.g., "FPT-NT-2024-A", "FPT-NT-2024-B"
- `contract_id` (string) - FK to :Contract
- `plan_name` (string) - e.g., "Gói A", "Gói B", "Mở rộng"
- `plan_type` (string) - "basic", "expanded", "exclusion"
- `co_pay_percent` (int) - Phần trăm đồng chi trả (0, 10, 20)
- `limit_per_visit` (float) - Giới hạn mỗi lần khám (VND)
- `limit_per_year` (float) - Giới hạn mỗi năm (VND)
- `namespace` (string) = "insurance_v1"

**Example:**
```cypher
(:Plan {
    plan_id: "FPT-NT-2024-A",
    contract_id: "FPT-NT-2024",
    plan_name: "Gói A",
    plan_type: "basic",
    co_pay_percent: 20,
    limit_per_visit: 500000,
    limit_per_year: 10000000,
    namespace: "insurance_v1"
})
```

---

### 4. Benefit (:Benefit)
Quyền lợi được bảo hiểm chi trả.

**Properties:**
- `benefit_id` (string, UNIQUE) - e.g., "BEN-001", "BEN-004"
- `benefit_name` (string) - Tên quyền lợi, e.g., "Chi phí điều trị ngoại trú"
- `canonical_name` (string) - Tên chuẩn sau khi diễn giải
- `benefit_type` (string) - "treatment", "examination", "medication", "dental", "pregnancy"
- `description` (string) - Mô tả chi tiết
- `major_section` (string) - e.g., "I. BẢO HIỂM SỨC KHỎE"
- `subsection` (string) - e.g., "1. ĐIỀU TRỊ NGOẠI TRÚ"
- `source_contract_id` (string) - FK to :Contract
- `namespace` (string) = "insurance_v1"

**Example:**
```cypher
(:Benefit {
    benefit_id: "BEN-004",
    benefit_name: "Chi phí điều trị ngoại trú",
    canonical_name: "Chi phí điều trị ngoại trú",
    benefit_type: "treatment",
    major_section: "I. BẢO HIỂM SỨC KHỎE",
    subsection: "1. ĐIỀU TRỊ NGOẠI TRÚ",
    source_contract_id: "FPT-NT-2024",
    namespace: "insurance_v1"
})
```

---

### 5. BenefitCoverage (:BenefitCoverage)
Chi tiết mức bảo hiểm cho từng quyền lợi theo từng gói.

**Properties:**
- `coverage_id` (string, UNIQUE)
- `benefit_id` (string) - FK to :Benefit
- `plan_id` (string) - FK to :Plan
- `coverage_status` (string) - "Bảo hiểm", "Không bảo hiểm", "Mở rộng bảo hiểm", "Giới hạn"
- `coverage_detail` (string) - Chi tiết điều kiện
- `copay_percent` (int) - Phần trăm đồng chi trả
- `limit_amount` (float) - Giới hạn số tiền
- `namespace` (string) = "insurance_v1"

**Example:**
```cypher
(:BenefitCoverage {
    coverage_id: "BEN-004-FPT-NT-2024-A",
    benefit_id: "BEN-004",
    plan_id: "FPT-NT-2024-A",
    coverage_status: "Bảo hiểm",
    copay_percent: 20,
    limit_amount: 500000,
    namespace: "insurance_v1"
})
```

---

### 6. BenefitServiceMapping (:BenefitServiceMapping)
Mapping giữa quyền lợi và dịch vụ cụ thể.

**Properties:**
- `mapping_id` (string, UNIQUE)
- `benefit_id` (string) - FK to :Benefit
- `service_code` (string) - FK to :Service (từ namespace khác)
- `service_name_raw` (string) - Tên dịch vụ gốc
- `allocation_count` (int) - Số lần xuất hiện trong hồ sơ
- `confidence` (float) - Độ tin cậy của mapping (0-1)
- `source_type` (string) - "allocation", "interpretation"
- `namespace` (string) = "insurance_v1"

**Example:**
```cypher
(:BenefitServiceMapping {
    mapping_id: "BEN-004-LAB-BIO-001",
    benefit_id: "BEN-004",
    service_code: "LAB-BIO-001",
    service_name_raw: "Xét nghiệm máu tổng quát",
    allocation_count: 150,
    confidence: 0.85,
    source_type: "allocation",
    namespace: "insurance_v1"
})
```

---

### 7. Exclusion (:Exclusion)
Nhóm loại trừ (category-level).

**Properties:**
- `exclusion_id` (string, UNIQUE) - e.g., "EXC-THUOC", "EXC-CLS"
- `exclusion_name` (string) - Tên nhóm, e.g., "Thuốc", "Cận lâm sàng"
- `exclusion_group` (string) - "Thuốc", "Cận lâm sàng", "Loại trừ - Quyền lợi", "Điều kiện mở rộng"
- `description` (string) - Mô tả
- `namespace` (string) = "insurance_v1"

**Example:**
```cypher
(:Exclusion {
    exclusion_id: "EXC-THUOC",
    exclusion_name: "Loại trừ Thuốc",
    exclusion_group: "Thuốc",
    description: "Các trường hợp thuốc không được bảo hiểm chi trả",
    namespace: "insurance_v1"
})
```

---

### 8. ExclusionReason (:ExclusionReason)
Lý do loại trừ cụ thể (atomic-level).

**Properties:**
- `reason_id` (string, UNIQUE) - e.g., "ma18", "ma06", "ma27"
- `reason_code` (string) - Mã lý do
- `reason_text` (string) - Nội dung lý do, e.g., "Đơn thuốc kê quá 30 ngày theo quy định của BYT"
- `exclusion_id` (string) - FK to :Exclusion
- `process_path` (string) - "Hồ sơ => Bộ Y tế => Hợp đồng"
- `source_note` (string) - Ghi chú nguồn
- `namespace` (string) = "insurance_v1"

**Example:**
```cypher
(:ExclusionReason {
    reason_id: "ma18",
    reason_code: "ma18",
    reason_text: "Đơn thuốc kê quá 30 ngày theo quy định của BYT",
    exclusion_id: "EXC-THUOC",
    process_path: "Hồ sơ => Bộ Y tế => Hợp đồng",
    source_note: "1. Bộ Y Tế: 26/2025/TT-BYT",
    namespace: "insurance_v1"
})
```

---

### 9. ExclusionServiceMapping (:ExclusionServiceMapping)
Mapping giữa lý do loại trừ và dịch vụ.

**Properties:**
- `mapping_id` (string, UNIQUE)
- `reason_id` (string) - FK to :ExclusionReason
- `service_code` (string) - FK to :Service
- `service_name_raw` (string) - Tên dịch vụ gốc
- `claim_count` (int) - Số hồ sơ có mapping này
- `gap_sum_vnd` (float) - Tổng số tiền không được chi trả
- `confidence` (float) - Độ tin cậy của mapping
- `source_type` (string) - "main", "outpatient", "combined"
- `namespace` (string) = "insurance_v1"

**Example:**
```cypher
(:ExclusionServiceMapping {
    mapping_id: "ma18-THUOC-001",
    reason_id: "ma18",
    service_code: "DRUG-001",
    service_name_raw: "Amoxicillin 500mg",
    claim_count: 127,
    gap_sum_vnd: 15600000,
    confidence: 0.92,
    source_type: "main",
    namespace: "insurance_v1"
})
```

---

### 10. Rulebook (:Rulebook)
Tài liệu quy tắc bảo hiểm (PDF).

**Properties:**
- `rulebook_id` (string, UNIQUE) - e.g., "PJICO-711", "BHV-151B"
- `rule_code` (string) - e.g., "711", "151B", "384"
- `display_name` (string) - e.g., "PJICO - Quy tắc BH Sức khỏe 711"
- `insurer_id` (string) - FK to :Insurer
- `source_file` (string) - Đường dẫn PDF
- `page_count` (int) - Số trang
- `text_extractable_pages` (int) - Số trang có thể extract text
- `ocr_status` (string) - "text_layer_present" | "ocr_required"
- `namespace` (string) = "insurance_v1"
- `chunk_id` (string, OPTIONAL) - FK to :Chunk (nếu đã ingest)

**Example:**
```cypher
(:Rulebook {
    rulebook_id: "PJICO-711",
    rule_code: "711",
    display_name: "PJICO - Quy tắc BH Sức khỏe 711",
    insurer_id: "PJICO",
    source_file: "QT 711 - BH SUC KHOE.pdf",
    page_count: 17,
    text_extractable_pages: 0,
    ocr_status: "ocr_required",
    namespace: "insurance_v1"
})
```

---

### 11. RulebookClause (:RulebookClause)
Điều khoản cụ thể trong quy tắc bảo hiểm.

**Properties:**
- `clause_id` (string, UNIQUE)
- `rulebook_id` (string) - FK to :Rulebook
- `chapter` (string) - e.g., "Chương I", "Chương III"
- `section` (string) - e.g., "Khoản 9", "Khoản 17"
- `clause_text` (string) - Nội dung điều khoản
- `exclusion_ids` (list[string]) - List FK to :Exclusion
- `page_number` (int) - Trang trong PDF
- `namespace` (string) = "insurance_v1"

**Example:**
```cypher
(:RulebookClause {
    clause_id: "PJICO-711-CH1-S9",
    rulebook_id: "PJICO-711",
    chapter: "Chương I",
    section: "Khoản 9",
    clause_text: "Các trường hợp không được bảo hiểm chi trả",
    page_number: 3,
    namespace: "insurance_v1"
})
```

---

### 12. InsuranceClaim (:InsuranceClaim)
Hồ sơ bảo hiểm (đã xử lý/loại trừ).

**Properties:**
- `claim_id` (string, UNIQUE)
- `contract_id` (string) - FK to :Contract (nếu có)
- `plan_id` (string, OPTIONAL) - FK to :Plan (nếu có)
- `care_type` (string) - "Ngoại trú", "Nội trú", "Tai nạn"
- `patient_id` (string, OPTIONAL)
- `visit_date` (date, OPTIONAL)
- `requested_amount_vnd` (float) - Số tiền yêu cầu
- `paid_amount_vnd` (float) - Số tiền thực chi trả
- `gap_amount_vnd` (float) - Số tiền không chi trả
- `adjudication_status` (string) - "PAID", "PARTIAL", "REJECTED"
- `namespace` (string) = "insurance_v1"

**Example:**
```cypher
(:InsuranceClaim {
    claim_id: "CLAIM-001",
    contract_id: "FPT-NV",
    care_type: "Ngoại trú",
    requested_amount_vnd: 1500000,
    paid_amount_vnd: 800000,
    gap_amount_vnd: 700000,
    adjudication_status: "PARTIAL",
    namespace: "insurance_v1"
})
```

---

### 13. ClaimExclusion (:ClaimExclusion)
Mapping giữa hồ sơ và lý do loại trừ.

**Properties:**
- `claim_exclusion_id` (string, UNIQUE)
- `claim_id` (string) - FK to :InsuranceClaim
- `reason_id` (string) - FK to :ExclusionReason
- `service_line_id` (string, OPTIONAL) - Dịch vụ cụ thể bị loại trừ
- `gap_amount_vnd` (float) - Số tiền không chi trả vì lý do này
- `namespace` (string) = "insurance_v1"

**Example:**
```cypher
(:ClaimExclusion {
    claim_exclusion_id: "CLAIM-001-ma18",
    claim_id: "CLAIM-001",
    reason_id: "ma18",
    gap_amount_vnd: 500000,
    namespace: "insurance_v1"
})
```

---

## Relationships

### Primary Relationships

| From | To | Relationship | Properties |
|------|-----|-------------|-------------|
| `:Insurer` | `:Contract` | `ISSUES` | `issue_date` |
| `:Insurer` | `:Rulebook` | `PUBLISHES` | `publish_date` |
| `:Contract` | `:Plan` | `HAS_PLAN` | - |
| `:Contract` | `:Benefit` | `COVERS` | - |
| `:Contract` | `:Rulebook` | `REFERENCES` | - |
| `:Contract` | `:Exclusion` | `APPLIES_EXCLUSION` | - |
| `:Plan` | `:BenefitCoverage` | `HAS_COVERAGE` | - |
| `:Benefit` | `:BenefitCoverage` | `HAS_COVERAGE` | - |
| `:BenefitCoverage` | `:Plan` | `FOR_PLAN` | - |
| `:Benefit` | `:BenefitServiceMapping` | `MAPS_TO_SERVICE` | - |
| `:Exclusion` | `:ExclusionReason` | `HAS_REASON` | - |
| `:ExclusionReason` | `:ExclusionServiceMapping` | `EXCLUDES_SERVICE` | - |
| `:Rulebook` | `:RulebookClause` | `CONTAINS_CLAUSE` | `page_range` |
| `:RulebookClause` | `:Exclusion` | `DEFINES_EXCLUSION` | - |
| `:InsuranceClaim` | `:Contract` | `UNDER_CONTRACT` | `submission_date` |
| `:InsuranceClaim` | `:Plan` | `UNDER_PLAN` | - |
| `:InsuranceClaim` | `:ClaimExclusion` | `HAS_EXCLUSION` | - |
| `:ClaimExclusion` | `:ExclusionReason` | `CITED_REASON` | - |
| `:ClaimExclusion` | `:Service` | `AFFECTED_SERVICE` | - |
| `:BenefitServiceMapping` | `:Service` | `REFERENCES_SERVICE` | - |
| `:ExclusionServiceMapping` | `:Service` | `REFERENCES_SERVICE` | - |
| `:Rulebook` | `:Chunk` | `HAS_CHUNK` | (nếu ingest PDF) |

### Cross-Namespace Relationships (linking to clinical domain)

| From | To | Relationship | Description |
|------|-----|-------------|-------------|
| `:BenefitServiceMapping` (insurance_v1) | `:Service` (claims_insights) | `REFERENCES_SERVICE` | Mapping quyền lợi → dịch vụ chuẩn hóa |
| `:ExclusionServiceMapping` (insurance_v1) | `:Service` (claims_insights) | `REFERENCES_SERVICE` | Mapping loại trừ → dịch vụ chuẩn hóa |

---

## Constraints & Indexes

### Constraints (UNIQUE)

```cypher
-- Insurer
CREATE CONSTRAINT IF NOT EXISTS FOR (n:Insurer) REQUIRE n.insurer_id IS UNIQUE;
-- Contract
CREATE CONSTRAINT IF NOT EXISTS FOR (n:Contract) REQUIRE n.contract_id IS UNIQUE;
-- Plan
CREATE CONSTRAINT IF NOT EXISTS FOR (n:Plan) REQUIRE n.plan_id IS UNIQUE;
-- Benefit
CREATE CONSTRAINT IF NOT EXISTS FOR (n:Benefit) REQUIRE n.benefit_id IS UNIQUE;
-- BenefitCoverage
CREATE CONSTRAINT IF NOT EXISTS FOR (n:BenefitCoverage) REQUIRE n.coverage_id IS UNIQUE;
-- BenefitServiceMapping
CREATE CONSTRAINT IF NOT EXISTS FOR (n:BenefitServiceMapping) REQUIRE n.mapping_id IS UNIQUE;
-- Exclusion
CREATE CONSTRAINT IF NOT EXISTS FOR (n:Exclusion) REQUIRE n.exclusion_id IS UNIQUE;
-- ExclusionReason
CREATE CONSTRAINT IF NOT EXISTS FOR (n:ExclusionReason) REQUIRE n.reason_id IS UNIQUE;
-- ExclusionServiceMapping
CREATE CONSTRAINT IF NOT EXISTS FOR (n:ExclusionServiceMapping) REQUIRE n.mapping_id IS UNIQUE;
-- Rulebook
CREATE CONSTRAINT IF NOT EXISTS FOR (n:Rulebook) REQUIRE n.rulebook_id IS UNIQUE;
-- RulebookClause
CREATE CONSTRAINT IF NOT EXISTS FOR (n:RulebookClause) REQUIRE n.clause_id IS UNIQUE;
-- InsuranceClaim
CREATE CONSTRAINT IF NOT EXISTS FOR (n:InsuranceClaim) REQUIRE n.claim_id IS UNIQUE;
-- ClaimExclusion
CREATE CONSTRAINT IF NOT EXISTS FOR (n:ClaimExclusion) REQUIRE n.claim_exclusion_id IS UNIQUE;
```

### Indexes

```cypher
-- Full-text search
CREATE FULLTEXT INDEX IF NOT EXISTS FOR (n:Benefit) ON EACH [n.benefit_name, n.description];
CREATE FULLTEXT INDEX IF NOT EXISTS FOR (n:Exclusion) ON EACH [n.exclusion_name, n.description];
CREATE FULLTEXT INDEX IF NOT EXISTS FOR (n:ExclusionReason) ON EACH [n.reason_text];
CREATE FULLTEXT INDEX IF NOT EXISTS FOR (n:Rulebook) ON EACH [n.display_name];
CREATE FULLTEXT INDEX IF NOT EXISTS FOR (n:RulebookClause) ON EACH [n.clause_text];

-- Lookup indexes
CREATE INDEX IF NOT EXISTS FOR (n:Contract) ON (n.insurer_id);
CREATE INDEX IF NOT EXISTS FOR (n:Plan) ON (n.contract_id);
CREATE INDEX IF NOT EXISTS FOR (n:Benefit) ON (n.source_contract_id);
CREATE INDEX IF NOT EXISTS FOR (n:BenefitCoverage) ON (n.benefit_id);
CREATE INDEX IF NOT EXISTS FOR (n:BenefitCoverage) ON (n.plan_id);
CREATE INDEX IF NOT EXISTS FOR (n:BenefitServiceMapping) ON (n.benefit_id);
CREATE INDEX IF NOT EXISTS FOR (n:BenefitServiceMapping) ON (n.service_code);
CREATE INDEX IF NOT EXISTS FOR (n:ExclusionReason) ON (n.exclusion_id);
CREATE INDEX IF NOT EXISTS FOR (n:ExclusionServiceMapping) ON (n.reason_id);
CREATE INDEX IF NOT EXISTS FOR (n:ExclusionServiceMapping) ON (n.service_code);
CREATE INDEX IF NOT EXISTS FOR (n:RulebookClause) ON (n.rulebook_id);
CREATE INDEX IF NOT EXISTS FOR (n:InsuranceClaim) ON (n.contract_id);
CREATE INDEX IF NOT EXISTS FOR (n:ClaimExclusion) ON (n.claim_id);
CREATE INDEX IF NOT EXISTS FOR (n:ClaimExclusion) ON (n.reason_id);
```

---

## Query Patterns for Adjudication

### Q1: Kiểm tra dịch vụ có được bảo hiểm không

```cypher
// Input: service_code, contract_id, plan_id
// Output: coverage_status, copay_percent, limit_amount

MATCH (c:Contract {contract_id: $contract_id})
MATCH (c)-[:HAS_PLAN]->(p:Plan {plan_id: $plan_id})
MATCH (c)-[:COVERS]->(b:Benefit)
MATCH (b)-[:HAS_COVERAGE]->(bc:BenefitCoverage)-[:FOR_PLAN]->(p)
MATCH (b)-[:MAPS_TO_SERVICE]->(bsm:BenefitServiceMapping)
WHERE bsm.service_code = $service_code
RETURN bc.coverage_status, bc.copay_percent, bc.limit_amount, b.benefit_name
```

### Q2: Kiểm tra dịch vụ có bị loại trừ không

```cypher
// Input: service_code, contract_id
// Output: list of exclusion reasons

MATCH (c:Contract {contract_id: $contract_id})
MATCH (c)-[:APPLIES_EXCLUSION]->(ex:Exclusion)
MATCH (ex)-[:HAS_REASON]->(er:ExclusionReason)
MATCH (er)-[:EXCLUDES_SERVICE]->(esm:ExclusionServiceMapping)
WHERE esm.service_code = $service_code
RETURN er.reason_id, er.reason_text, er.process_path, esm.gap_sum_vnd, esm.claim_count
ORDER BY esm.claim_count DESC
```

### Q3: Full coverage check (service + contract + plan)

```cypher
// Input: service_code, contract_id, plan_id
// Output: comprehensive coverage decision

// Step 1: Check benefit coverage
WITH $service_code AS svc, $contract_id AS contract, $plan_id AS plan
MATCH (c:Contract {contract_id: contract})
MATCH (c)-[:HAS_PLAN]->(p:Plan {plan_id: plan})
MATCH (c)-[:COVERS]->(b:Benefit)
MATCH (b)-[:HAS_COVERAGE]->(bc:BenefitCoverage)-[:FOR_PLAN]->(p)
MATCH (b)-[:MAPS_TO_SERVICE]->(bsm:BenefitServiceMapping)
WHERE bsm.service_code = svc

WITH b, bc, bsm

// Step 2: Check exclusion
OPTIONAL MATCH (c)-[:APPLIES_EXCLUSION]->(ex:Exclusion)
MATCH (ex)-[:HAS_REASON]->(er:ExclusionReason)
MATCH (er)-[:EXCLUDES_SERVICE]->(esm:ExclusionServiceMapping)
WHERE esm.service_code = svc

WITH b, bc, bsm, collect(DISTINCT {reason_id: er.reason_id, reason: er.reason_text}) AS exclusions

RETURN {
    service_code: svc,
    contract_id: contract,
    plan_id: plan,
    benefit: b.benefit_name,
    coverage_status: bc.coverage_status,
    copay_percent: bc.copay_percent,
    limit_amount: bc.limit_amount,
    exclusions: exclusions,
    decision: CASE
        WHEN bc.coverage_status = "Không bảo hiểm" THEN "REJECT"
        WHEN bc.coverage_status = "Giới hạn" AND exclusions = [] THEN "PARTIAL"
        WHEN bc.coverage_status = "Bảo hiểm" AND exclusions = [] THEN "APPROVE"
        WHEN exclusions <> [] THEN "REVIEW"
        ELSE "REVIEW"
    END
} AS result
```

### Q4: Tìm tất cả dịch vụ được bảo hiểm cho một quyền lợi

```cypher
// Input: benefit_id
// Output: list of services

MATCH (b:Benefit {benefit_id: $benefit_id})
MATCH (b)-[:MAPS_TO_SERVICE]->(bsm:BenefitServiceMapping)
MATCH (bsm)-[:REFERENCES_SERVICE]->(s:Service)
RETURN s.service_code, s.canonical_name, bsm.allocation_count, bsm.confidence
ORDER BY bsm.allocation_count DESC
```

### Q5: Tìm lý do loại trừ phổ biến cho một dịch vụ

```cypher
// Input: service_code
// Output: top exclusion reasons

MATCH (er:ExclusionReason)
MATCH (er)-[:EXCLUDES_SERVICE]->(esm:ExclusionServiceMapping)
WHERE esm.service_code = $service_code
RETURN er.reason_id, er.reason_text, er.exclusion_id,
       esm.claim_count, esm.gap_sum_vnd, esm.confidence
ORDER BY esm.claim_count DESC, esm.gap_sum_vnd DESC
LIMIT 10
```

### Q6: Tìm điều khoản quy tắc liên quan đến lý do loại trừ

```cypher
// Input: reason_id
// Output: rulebook clauses

MATCH (er:ExclusionReason {reason_id: $reason_id})
MATCH (er)-[:MEMBER_OF_EXCLUSION]->(ex:Exclusion)
MATCH (rc:RulebookClause)-[:DEFINES_EXCLUSION]->(ex)
MATCH (rc)-[:IN_RULEBOOK]->(rb:Rulebook)
RETURN rb.rulebook_id, rb.display_name, rc.chapter, rc.section, rc.clause_text, rc.page_number
```

### Q7: Full-text search quyền lợi

```cypher
// Input: search query
// Output: matching benefits

CALL db.index.fulltext.queryNodes("benefit_fulltext", $query) YIELD node, score
MATCH (node)-[:COVERED_BY]->(c:Contract)
RETURN node.benefit_id, node.benefit_name, node.description, c.contract_id, score
ORDER BY score DESC
LIMIT 20
```

### Q8: Phân tích hồ sơ bị loại trừ theo lý do

```cypher
// Input: contract_id, date_range
// Output: exclusion statistics

MATCH (c:Contract {contract_id: $contract_id})
MATCH (ic:InsuranceClaim)-[:UNDER_CONTRACT]->(c)
WHERE ic.visit_date >= $from_date AND ic.visit_date <= $to_date
MATCH (ic)-[:HAS_EXCLUSION]->(ce:ClaimExclusion)
MATCH (ce)-[:CITED_REASON]->(er:ExclusionReason)
RETURN er.reason_id, er.reason_text, er.exclusion_id,
       count(ic) AS claim_count,
       sum(ce.gap_amount_vnd) AS total_gap_vnd
ORDER BY claim_count DESC, total_gap_vnd DESC
```

---

## Schema Visual Summary

```
:Insurer (PJICO, FPT, BHV, ...)
    ├── ISSUES → :Contract (FPT-NT-2024, FPT-NV, ...)
    │       ├── HAS_PLAN → :Plan (Gói A, Gói B, ...)
    │       │       └── HAS_COVERAGE → :BenefitCoverage
    │       ├── COVERS → :Benefit
    │       │       ├── HAS_COVERAGE → :BenefitCoverage
    │       │       └── MAPS_TO_SERVICE → :BenefitServiceMapping → :Service
    │       ├── APPLIES_EXCLUSION → :Exclusion
    │       │       └── HAS_REASON → :ExclusionReason
    │       │               └── EXCLUDES_SERVICE → :ExclusionServiceMapping → :Service
    │       └── REFERENCES → :Rulebook (QT 711, QT 384, ...)
    │               └── CONTAINS_CLAUSE → :RulebookClause
    │                       └── DEFINES_EXCLUSION → :Exclusion
    │
    └── PUBLISHES → :Rulebook

:InsuranceClaim (hồ sơ đã xử lý)
    ├── UNDER_CONTRACT → :Contract
    ├── UNDER_PLAN → :Plan (optional)
    └── HAS_EXCLUSION → :ClaimExclusion
            └── CITED_REASON → :ExclusionReason
            └── AFFECTED_SERVICE → :Service (optional)
```

---

## Migration Strategy

### Phase 1: Core Schema Setup
1. Create all constraints
2. Create all indexes
3. Verify with `SHOW CONSTRAINTS` and `SHOW INDEXES`

### Phase 2: Static Data Ingest
1. `:Insurer` - từ contract_rules.json
2. `:Contract` - từ contract_rules.json
3. `:Plan` - từ contract_rules.json
4. `:Benefit` - từ benefit_contract_knowledge_pack.json
5. `:BenefitCoverage` - từ benefit_contract_knowledge_pack.json
6. `:Exclusion` - từ exclusion_knowledge_pack.json
7. `:ExclusionReason` - từ exclusion_knowledge_pack.json
8. `:Rulebook` - từ rulebook_policy_pack.json

### Phase 3: Mapping Data Ingest
1. `:BenefitServiceMapping` - từ benefit_detail_service_links.jsonl
2. `:ExclusionServiceMapping` - từ exclusion_note_mentions_linked.jsonl + outpatient_exclusion_signals.jsonl

### Phase 4: Claim Data Ingest (optional, for analysis)
1. `:InsuranceClaim` - từ Danh sách hồ sơ loại trừ.xlsx
2. `:ClaimExclusion` - từ claim data

### Phase 5: PDF Ingest
1. `:RulebookClause` - từ rulebook PDFs (OCR + LLM extraction)
2. Link `:Rulebook` → `:Chunk` (sau khi ingest PDFs)

---

## Version: 1.0
**Date**: 2026-04-08
**Author**: Pathway Architecture Team
**Status**: Ready for Implementation
