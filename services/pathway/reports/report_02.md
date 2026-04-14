# Report 02 — Ontology-Driven Knowledge Graph: Kiến trúc & Kế hoạch

## 2026-03-28

## 1. Nguyên tắc cốt lõi

**KHÔNG dùng random graph extraction như GraphRAG.** GraphRAG extract entity ngẫu nhiên từ text → rồi mới discover structure (Leiden communities). Ta làm ngược: **đã có ontology y khoa 4 lớp → dùng LLM extract CHÍNH XÁC vào cấu trúc đã thiết kế.**

| | GraphRAG (bottom-up) | Pathway (top-down, ontology-driven) |
|---|---|---|
| Entity types | LLM tự quyết (org, person, geo...) | Ontology định nghĩa sẵn (ProtocolSign, ClaimSignConcept, TMHInferenceDisease...) |
| Relationships | LLM tự tạo, weight 1-10 | Ontology quy định (PROTO_HAS_SIGN, CI_INDICATES_SERVICE...) |
| Structure | Leiden clustering → emergent communities | 4-layer architecture đã thiết kế |
| Quality | Phụ thuộc LLM, không validate | Validate against ontology schema |

## 2. Những gì ĐÁNG học từ GraphRAG/SurfSense/open-notebook

Không lấy kiến trúc, chỉ lấy **kỹ thuật**:

### 2.1 Multi-pass Gleaning (GraphRAG)
- Hỏi LLM 2+ lần: "Có entity nào bị miss không? (Y/N)" → nếu Y, extract thêm
- **Áp dụng**: Sau khi extract ProtocolSign, ProtocolServiceMention từ một section, hỏi lại LLM kiểm tra miss
- **Impact ước tính**: +15-20% entity recall

### 2.2 Entity Description Summarization (GraphRAG)
- Cùng 1 entity xuất hiện ở nhiều chunks → gộp descriptions thành 1 summary
- **Áp dụng**: ClaimSignConcept xuất hiện trong 10+ sections → tạo `summary_description` property tổng hợp
- **Impact**: Cải thiện embedding quality → retrieval tốt hơn

### 2.3 Hierarchical 2-tier Indexing (SurfSense)
- Layer trên: summaries (recall nhanh), layer dưới: chunks chi tiết (precision cao)
- **Áp dụng**: ProtocolDisease node giữ `summary_embedding` (tổng hợp), Chunk nodes giữ `content_embedding` (chi tiết)
- **Impact**: Search nhanh hơn ở disease level, precision giữ nguyên ở chunk level

### 2.4 Relationship Strength Scoring (GraphRAG)
- Mỗi relationship có weight 1-10 thay vì binary
- **Áp dụng**: `PROTO_HAS_SIGN` thêm `strength` (1-10), `evidence_count`, `section_count`
- **Impact**: Ranking results theo relevance thay vì chỉ có/không

### 2.5 Claim/Covariate Extraction (GraphRAG)
- Extract assertions/facts có status (TRUE/FALSE/SUSPECTED) + temporal bounds
- **Áp dụng**: Map trực tiếp vào `ProtocolAssertion` — luật y khoa extract từ text, có status và evidence
- **Impact**: Đây chính là tầng 3 (Inference) mà ta đang thiếu

## 3. Ontology 4 lớp hiện tại — Inventory

### Layer 1: Protocol Knowledge ✅ (đã có)
```
ProtocolBook ─PROTO_CONTAINS_DISEASE─> ProtocolDisease (65)
ProtocolDisease ─PROTO_HAS_SECTION─> ProtocolSection (879)
ProtocolDisease ─PROTO_HAS_SIGN─> ProtocolSign (1,145)
ProtocolDisease ─PROTO_HAS_SERVICE_MENTION─> ProtocolServiceMention (128)
ProtocolServiceMention ─PROTO_RESOLVES_TO_SERVICE─> ProtocolService (20)
ProtocolService ─PROTO_SERVICE_IN_FAMILY─> ProtocolServiceFamily (6)
ProtocolDisease ─PROTO_MATCHES_ICD─> ProtocolICD (23)
```

### Layer 2: Canonical Concepts ✅ (đã có, cần bổ sung)
```
ClaimSignConcept (532) ─CLAIM_SIGN_HAS_ALIAS─> ClaimSignAlias (565)
ClaimSignConcept ─CLAIM_SIGN_HAS_MODIFIER─> ClaimModifier (7 types)
ClaimSignConcept ─CLAIM_SIGN_OF_DISEASE─> ClaimDisease (167)
```
**Thiếu**: ObservationConcept, DiseaseGroup hierarchy, ServiceConcept canonical

### Layer 3: Inference ⚠️ (có một phần)
```
TMHInferenceDisease (17) ─TMH_DISEASE_EXPECTS_SERVICE─> TMHInferenceService (15)
TMHInferenceService ─TMH_SERVICE_HAS_SIGNAL_SOURCE─> TMHResultSignalSource (23)
TMHResultSignalSource ─TMH_SIGNAL_EVIDENCE_FOR_DISEASE─> TMHInferenceDisease (107 edges)
TMHResultSignalSource ─TMH_SIGNAL_SOURCE_MATCHES_PROFILE─> TMHResultSignalProfile (4 profiles)
```
Signal profiles: `direct_positive_clue`, `negative_exclusion_clue`, `abnormal_supportive_clue`, `narrative_supportive_clue`

**Thiếu**: ProtocolAssertion nodes, DiseaseHypothesis lifecycle, LabFeature matrix

### Layer 4: Claims ✅ (đã có)
```
CIDisease ─CI_HAS_SIGN─> CISign
CIDisease ─CI_INDICATES_SERVICE─> CIService
CIDisease ─CI_HAS_OBSERVATION─> CIObservation
CIService ─CI_SUPPORTS_OBSERVATION─> CIObservation
```
**Thiếu**: ClaimCase, ServiceLine, ReviewDecision graph representation

### Layer 3b: Rules ✅ (đã có)
```
TMHRuleGroup (90) ─TMH_GROUP_HAS_TERM─> TMHTerm (325)
TMHRule (62) ─TMH_RULE_USES_GROUP─> TMHRuleGroup
```

## 4. Gap Analysis — Cần build thêm

### Gap A: ProtocolAssertion (Luật y khoa từ text)
**Ví dụ từ tham chiếu**: "Tất cả viêm mũi họng đỏ cấp có chấm mủ trắng phải điều trị như liên cầu khi chưa có xét nghiệm phân loại"

Node type mới:
```
ProtocolAssertion:
  assertion_id: str
  assertion_text: str          # Nguyên văn từ phác đồ
  assertion_type: str          # "treatment_rule" | "diagnostic_rule" | "contraindication" | "indication"
  status: str                  # "ACTIVE" | "CONDITIONAL" | "DEPRECATED"
  condition_text: str          # Điều kiện áp dụng
  action_text: str             # Hành động
  evidence_level: str          # "strong" | "moderate" | "weak" | "expert_opinion"
  source_section_id: str       # Truy xuất section gốc
  source_page: int
  embedding: [1536]            # Vector cho semantic search
```

Relationships:
```
ProtocolSection ─PROTO_CONTAINS_ASSERTION─> ProtocolAssertion
ProtocolAssertion ─PROTO_ASSERTION_ABOUT_DISEASE─> ProtocolDisease
ProtocolAssertion ─PROTO_ASSERTION_REQUIRES_SIGN─> ProtocolSign
ProtocolAssertion ─PROTO_ASSERTION_INDICATES_SERVICE─> ProtocolService
ProtocolAssertion ─PROTO_ASSERTION_CONTRAINDICATES─> ProtocolService
```

### Gap B: DiseaseHypothesis (Suy luận lâm sàng)
Khi nhận signs từ case → kích hoạt hypotheses → evidence xác nhận/loại trừ

```
DiseaseHypothesis:
  hypothesis_id: str
  disease_id: str
  confidence: float            # 0.0-1.0, updated by evidence
  status: str                  # "active" | "confirmed" | "ruled_out"
  supporting_signs: [str]      # Signs ủng hộ
  opposing_signs: [str]        # Signs phản bác
  required_services: [str]     # Services cần để xác nhận
```

### Gap C: Summary Nodes (Hierarchical 2-tier)
Mỗi ProtocolDisease tạo thêm summary node:

```
ProtocolDiseaseSummary:
  disease_id: str
  summary_text: str            # LLM-generated từ tất cả sections
  key_signs: [str]             # Top signs
  key_services: [str]          # Top services
  key_drugs: [str]             # Top drugs
  differential_diseases: [str] # Bệnh cần phân biệt
  summary_embedding: [1536]    # Vector cho fast retrieval
```

## 5. Extraction Pipeline mới: Ontology-Guided

### Khác biệt với pipeline hiện tại

| Bước | Pipeline hiện tại | Pipeline mới (ontology-guided) |
|------|-------------------|-------------------------------|
| 1. Chunk | Section-aware, fixed size | Section-aware + sentence boundary |
| 2. Extract | Generic entities (Drug, Symptom...) | **Ontology-typed**: ProtocolSign, ProtocolServiceMention, ProtocolAssertion |
| 3. Validate | Không có | **Schema validation** — entity phải match ontology type |
| 4. Canonical | Không có | **Alias resolution** — raw mention → canonical concept |
| 5. Relations | MENTIONS (binary) | **Typed + weighted**: PROTO_HAS_SIGN (strength, evidence_count) |
| 6. Gleaning | 1 pass | **2+ passes**: hỏi LLM kiểm tra miss |
| 7. Summarize | Không có | **Entity summary** + **Disease summary** |
| 8. Assertion | Không có | **Extract ProtocolAssertion** từ treatment/diagnosis sections |

### Flow chi tiết

```
PDF
 ├─ Phase 1: Document Analysis (giữ nguyên)
 │   └─ Detect disease, ICD, domain, source
 │
 ├─ Phase 2: Ontology-Guided Config (MỚI)
 │   ├─ Load ontology schema cho domain (TMH, Internal_Medicine, Oncology...)
 │   ├─ Load existing canonical concepts (ClaimSignConcept, ProtocolService...)
 │   └─ Generate extraction prompt WITH ontology context
 │
 ├─ Phase 3: Section-Aware Chunking (cải thiện)
 │   ├─ Detect section types: "Triệu chứng", "Chẩn đoán", "Điều trị", "Xét nghiệm"
 │   └─ Tag mỗi chunk với section_type → guide extraction
 │
 ├─ Phase 4: Ontology-Typed Extraction (MỚI)
 │   ├─ Per section_type, extract đúng entity types:
 │   │   ├─ "Triệu chứng" → ProtocolSign + ClaimModifier
 │   │   ├─ "Chẩn đoán" → ProtocolSign + ProtocolAssertion (diagnostic rules)
 │   │   ├─ "Điều trị" → ProtocolServiceMention + ProtocolAssertion (treatment rules)
 │   │   └─ "Xét nghiệm" → ProtocolServiceMention + TMHResultSignalSource
 │   ├─ Multi-pass gleaning: "Có entity nào bị miss? (Y/N)"
 │   └─ Validate against ALLOWED node types
 │
 ├─ Phase 5: Canonical Resolution (MỚI)
 │   ├─ Match extracted signs → existing ClaimSignConcept (fuzzy match)
 │   ├─ Match extracted services → existing ProtocolService (alias lookup)
 │   ├─ Create new canonical entries nếu chưa có
 │   └─ Track mapping confidence + provenance
 │
 ├─ Phase 6: Assertion Extraction (MỚI)
 │   ├─ Scan treatment + diagnosis sections
 │   ├─ LLM extract clinical rules: condition → action
 │   ├─ Classify: treatment_rule | diagnostic_rule | contraindication | indication
 │   └─ Link to ProtocolSign + ProtocolService
 │
 ├─ Phase 7: Summary Generation (MỚI — từ SurfSense/GraphRAG)
 │   ├─ Per-disease summary: gộp tất cả sections → 1 summary text + embedding
 │   ├─ Per-entity summary: gộp descriptions từ nhiều chunks
 │   └─ Differential diagnosis list
 │
 └─ Phase 8: Neo4j Ingest (cải thiện)
     ├─ MERGE nodes với đúng ontology labels
     ├─ MERGE relationships với weight + evidence_count
     ├─ Build hierarchy (NEXT_CHUNK, HAS_CHILD)
     └─ Create vector indexes cho summary nodes
```

## 6. Ví dụ cụ thể: "Viêm mũi họng cấp" → Graph

Dựa trên [tài liệu tham chiếu](D:/desktop_folder/12_Claude_Code/pathway/notebooklm/data/viêm%20mũi%20họng%20cấp_cảm%20thường.md):

### Nodes sẽ extract:

**Layer 1 — Protocol:**
```cypher
(:ProtocolDisease {disease_id: "viem_mui_hong_cap", icd10: "J00", disease_title: "Viêm mũi họng cấp tính"})
(:ProtocolSection {section_type: "trieu_chung", body_preview: "Sốt đột ngột 38-39°C..."})
(:ProtocolSection {section_type: "dieu_tri", body_preview: "Kháng sinh: Peniciline V..."})
```

**Layer 2 — Canonical Concepts:**
```cypher
(:ClaimSignConcept {canonical_label: "Sốt"})
  -[:CLAIM_SIGN_HAS_MODIFIER]->(:ClaimModifier {modifier_type: "severity", modifier_label: "38-39°C, lên tới 40°C ở trẻ em"})
(:ClaimSignConcept {canonical_label: "Đau họng"})
  -[:CLAIM_SIGN_HAS_MODIFIER]->(:ClaimModifier {modifier_type: "course", modifier_label: "khô rát, nuốt đau nhói lan lên tai"})
(:ClaimSignConcept {canonical_label: "Niêm mạc họng đỏ"})
  -[:CLAIM_SIGN_HAS_MODIFIER]->(:ClaimModifier {modifier_type: "course", modifier_label: "xuất tiết"})
(:ClaimSignConcept {canonical_label: "Amiđan sưng to"})
  -[:CLAIM_SIGN_HAS_MODIFIER]->(:ClaimModifier {modifier_type: "course", modifier_label: "sung huyết, có chấm mủ trắng/bựa trắng"})
```

**Layer 3 — Inference:**
```cypher
// Hypothesis activation
(:DiseaseHypothesis {disease: "Nhiễm virus (cảm thường)", status: "active"})
(:DiseaseHypothesis {disease: "Bội nhiễm vi khuẩn", status: "active"})

// Evidence chains
(:TMHResultSignalSource {concept_name: "Bạch cầu giảm, nhiều lympho"})
  -[:TMH_SIGNAL_EVIDENCE_FOR_DISEASE {profile: "abnormal_supportive_clue"}]->
  (:TMHInferenceDisease {disease_name: "Nhiễm virus"})

(:TMHResultSignalSource {concept_name: "Bạch cầu tăng, đa nhân trung tính"})
  -[:TMH_SIGNAL_EVIDENCE_FOR_DISEASE {profile: "abnormal_supportive_clue"}]->
  (:TMHInferenceDisease {disease_name: "Bội nhiễm vi khuẩn"})

(:TMHResultSignalSource {concept_name: "ASLO tăng cao"})
  -[:TMH_SIGNAL_EVIDENCE_FOR_DISEASE {profile: "direct_positive_clue"}]->
  (:TMHInferenceDisease {disease_name: "Liên cầu bê-ta tan huyết nhóm A"})
```

**Layer 1 — ProtocolAssertion (MỚI):**
```cypher
(:ProtocolAssertion {
  assertion_type: "treatment_rule",
  assertion_text: "Tất cả viêm mũi họng đỏ cấp có chấm mủ trắng hay bựa trắng trên bề mặt amiđan đều phải điều trị như viêm mũi họng đỏ cấp do liên cầu khi chưa có xét nghiệm phân loại vi khuẩn hay virus",
  condition_text: "chấm mủ trắng/bựa trắng trên amiđan AND chưa có XN phân loại",
  action_text: "điều trị như do liên cầu",
  status: "ACTIVE"
})
  -[:PROTO_ASSERTION_REQUIRES_SIGN]->(:ProtocolSign {sign_text: "chấm mủ trắng trên amiđan"})
  -[:PROTO_ASSERTION_INDICATES_SERVICE]->(:ProtocolService {service_name: "Kháng sinh nhóm Peniciline"})
```

**Layer 4 — Claims (quyết định thẩm định):**
```cypher
// Nếu case: chẩn đoán "cảm thường" + kê kháng sinh phổ rộng + KHÔNG có bằng chứng bội nhiễm
(:ReviewDecision {
  decision: "REVIEW",
  reason: "Kháng sinh phổ rộng liều cao ngày đầu, thiếu bằng chứng bội nhiễm vi khuẩn",
  assertion_id: "pa_001",  // Trỏ về ProtocolAssertion
  evidence_gap: "Thiếu CBC hoặc ASLO"
})
```

## 7. So sánh với GraphRAG — Tại sao ontology-driven tốt hơn cho medical domain

| Tiêu chí | GraphRAG (random) | Ontology-driven (ta) |
|----------|-------------------|---------------------|
| **Precision** | LLM hallucinate entity types | Chỉ extract vào types đã định nghĩa |
| **Consistency** | Cùng 1 concept, LLM đặt tên khác nhau | Canonical resolution → 1 concept duy nhất |
| **Explainability** | "Community 5 gồm entities X,Y,Z" | "ProtocolAssertion PA-001 từ phác đồ BYT" |
| **Medical safety** | Không validate logic y khoa | Validate assertion conditions + evidence |
| **Audit trail** | Source text references | Section → page → assertion → evidence chain |
| **Claims integration** | Không có | ReviewDecision linked to ProtocolAssertion |

## 8. Kế hoạch triển khai

### Phase A: Ontology Schema (1-2 ngày)
- [ ] Define ProtocolAssertion node type + relationships
- [ ] Define DiseaseHypothesis lifecycle
- [ ] Define ProtocolDiseaseSummary node type
- [ ] Viết JSON schema cho validation

### Phase B: Extraction Pipeline (2-3 ngày)
- [ ] Section-type detection (triệu chứng/chẩn đoán/điều trị/xét nghiệm)
- [ ] Ontology-typed extraction prompts per section_type
- [ ] Multi-pass gleaning (2 passes)
- [ ] Canonical resolution (sign → ClaimSignConcept, service → ProtocolService)

### Phase C: Assertion Extraction (1-2 ngày)
- [ ] LLM extract clinical rules từ treatment/diagnosis sections
- [ ] Classify assertion types
- [ ] Link assertions to signs + services

### Phase D: Summary Generation (1 ngày)
- [ ] Per-disease summary generation
- [ ] Per-entity description summarization
- [ ] Summary embeddings + vector index

### Phase E: Validation & Testing (1-2 ngày)
- [ ] Schema validation trước Neo4j ingest
- [ ] Comparison: ontology-driven vs current pipeline
- [ ] Benchmark trên TMH 65 diseases

## 9. Những gì KHÔNG lấy từ GraphRAG

- ❌ Leiden community detection → ta đã có disease groups
- ❌ Generic entity types (org, person, geo) → ta có ontology cụ thể
- ❌ Random relationship discovery → ta có typed relationships
- ❌ Map-reduce global search → ta có scoped_search + priority_search
- ❌ Parquet output → ta dùng Neo4j trực tiếp

---

## 10. NGUYÊN TẮC KIẾN TRÚC BẬC CAO: Dual Representation with Provenance

> **Không được chọn giữa "text thô" và "ID canonical". Phải giữ cả hai, rồi nối chúng bằng mapping có confidence và provenance.**

### 10.1 Vấn đề: Canonical hoá xong là mất ngữ nghĩa gốc

Nếu chỉ đưa vào Neo4j nodes canonical (`Disease/Service/Sign/LabFeature` đã ID-hoá), ta **mất**:
- Cách nói gốc của bác sĩ/người bệnh
- Từ đồng nghĩa thực tế
- Ngữ cảnh câu
- Điều kiện áp dụng
- Provenance để audit

Pipeline sai: `text → extract → canonical node → xong`

Pipeline đúng: **Lưu song song 2 bản sự thật**

### 10.2 Mô hình 6 lớp node

```
┌─────────────────────────────────────────────────────────┐
│  Layer 0: Raw Text                                      │
│  ┌──────────┐                                           │
│  │ RawChunk │ ← text gốc từ PDF, giữ nguyên            │
│  └────┬─────┘                                           │
│       │ :MENTIONS                                       │
│  ┌────▼──────────┐                                      │
│  │ RawMention    │ ← mention bóc từ text, giữ ngữ cảnh │
│  └────┬──────────┘                                      │
│       │ :MAPS_TO {confidence, method, status}           │
├───────┼─────────────────────────────────────────────────┤
│  Layer 1: Alias                                         │
│  ┌────▼──────┐                                          │
│  │ Alias     │ ← từ đồng nghĩa, không mất synonym     │
│  └────┬──────┘                                          │
│       │ :ALIAS_OF                                       │
├───────┼─────────────────────────────────────────────────┤
│  Layer 2: Canonical                                     │
│  ┌────▼──────┐                                          │
│  │ Concept   │ ← ID chuẩn để suy luận                  │
│  └────┬──────┘                                          │
│       │ :USED_IN                                        │
├───────┼─────────────────────────────────────────────────┤
│  Layer 3: Logic                                         │
│  ┌────▼──────────┐                                      │
│  │ Assertion     │ ← luật y khoa: condition → action    │
│  └───────────────┘                                      │
├─────────────────────────────────────────────────────────┤
│  Layer 4: Acceleration                                  │
│  ┌───────────┐                                          │
│  │ Summary   │ ← tăng tốc retrieval, KHÔNG phải truth  │
│  └───────────┘                                          │
└─────────────────────────────────────────────────────────┘
```

### 10.3 Áp dụng cụ thể vào domain y khoa

**Raw Layer** (giữ nguyên ngữ nghĩa gốc):
```
RawChunk         ← ProtocolSection body text
RawSignMention   ← "ộc máu mũi ồ ạt", "sốt đột ngột 38-39°C"
RawServiceMention ← "cắt amiđan", "XN công thức máu"
RawObservationMention ← "bạch cầu giảm, nhiều lympho"
```

**Alias Layer** (synonym không biến mất, trở thành graph):
```
ClaimSignAlias       ← "ộc máu mũi ồ ạt" :ALIAS_OF "chảy máu mũi"
ServiceAlias         ← "XN CTM" :ALIAS_OF "Tổng phân tích tế bào máu"
ObservationAlias     ← "BC tăng" :ALIAS_OF "Bạch cầu tăng"
```

**Canonical Layer** (ID chuẩn để suy luận):
```
ClaimSignConcept     ← {sign_id: "epistaxis", canonical_label: "Chảy máu mũi"}
ProtocolService      ← {service_code: "CBC", service_name: "Tổng phân tích tế bào máu"}
ObservationConcept   ← {concept_code: "WBC_HIGH", concept_name: "Bạch cầu tăng"}
DiseaseEntity        ← {disease_id: "J00", disease_name: "Viêm mũi họng cấp tính"}
```

**Assertion Layer** (logic y khoa):
```
ProtocolAssertion    ← {condition: "mủ trắng trên amiđan AND chưa XN", action: "điều trị như liên cầu"}
```

**Summary Layer** (tăng tốc, KHÔNG thay thế raw):
```
ProtocolDiseaseSummary ← tổng hợp từ tất cả sections, cho fast retrieval
SectionSummary         ← tóm tắt per section
```

### 10.4 Ví dụ: "Chảy máu mũi" — 3 lớp biểu diễn

Khi text gốc là:
- `"ộc máu mũi ồ ạt"`
- `"chảy máu mũi tự cầm"`
- `"epistaxis"`

Graph giữ được CẢ 3 lớp:

```cypher
// Raw mentions (ngữ cảnh gốc)
(:RawChunk {body: "...ộc máu mũi ồ ạt, phải nhét bấc..."})
  -[:MENTIONS]->
  (:RawSignMention {text: "ộc máu mũi ồ ạt", context: "cấp cứu, phải nhét bấc"})

// Alias (synonym trở thành node)
(:ClaimSignAlias {alias_label: "ộc máu mũi ồ ạt"})
  -[:ALIAS_OF]->
  (:ClaimSignConcept {sign_id: "epistaxis", canonical_label: "Chảy máu mũi"})

// Mapping có confidence
(:RawSignMention {text: "ộc máu mũi ồ ạt"})
  -[:MAPS_TO {confidence: 0.92, method: "fuzzy_match", status: "probable"}]->
  (:ClaimSignConcept {sign_id: "epistaxis"})
```

Truy ngược hoàn toàn:
```
ReviewDecision → ProtocolAssertion → ProtocolSection → RawChunk → exact text span
```

### 10.5 Quy tắc Canonical Resolution — Có trạng thái, không ép sạch

Mapping status **PHẢI** có:
| Status | Ý nghĩa | Hành động |
|--------|----------|-----------|
| `exact` | Text gốc = canonical (hoặc alias đã xác minh) | Dùng ngay |
| `probable` | Fuzzy match confidence > 0.8, 1 candidate | Dùng, nhưng giữ raw |
| `ambiguous` | Nhiều candidates, không chắc | Giữ RawMention, treo review |
| `unknown` | Không match canonical nào | Giữ RawMention, tạo candidate mới |

**Không bao giờ ép map bừa.** Nếu chưa chắc → giữ `RawMention` và treo review.

### 10.6 Quy tắc Summary — Lớp tăng tốc, KHÔNG phải lớp chân lý

| Node | Dùng cho | KHÔNG dùng cho |
|------|----------|----------------|
| `ProtocolDiseaseSummary` | Recall nhanh, semantic retrieval | Source of truth, audit |
| `SectionSummary` | Query-time context enrichment | Thay thế ProtocolSection |
| Entity summary description | Embedding quality improvement | Thay thế raw mentions |

**Không bao giờ**: summary thay raw, chunk thay section, canonical thay mention.

### 10.7 Provenance Chain — Truy ngược bắt buộc

Mọi quyết định phải truy ngược được đến text gốc:

```
ReviewDecision          "REJECT: kháng sinh phổ rộng thiếu bằng chứng"
  └── ProtocolAssertion   "có mủ trắng → điều trị như liên cầu"
       └── ProtocolSection  section_type: "dieu_tri", page: 5
            └── RawChunk     "Peniciline V 1MUI x 2 lần/ngày..."
                 └── RawServiceMention  "Peniciline V"
                      └── MAPS_TO {confidence: 1.0, status: "exact"}
                           └── ProtocolService  {service_code: "J01CE01"}
```

### 10.8 Quan hệ cốt lõi (Core Relationships)

```
// Text → Mention
RawChunk -[:MENTIONS]-> RawSignMention
RawChunk -[:MENTIONS]-> RawServiceMention
RawChunk -[:MENTIONS]-> RawObservationMention

// Mention → Concept (mapping có confidence)
RawSignMention -[:MAPS_TO {confidence, method, status}]-> ClaimSignConcept
RawServiceMention -[:MAPS_TO {confidence, method, status}]-> ProtocolService
RawObservationMention -[:MAPS_TO {confidence, method, status}]-> ObservationConcept

// Alias → Concept
ClaimSignAlias -[:ALIAS_OF]-> ClaimSignConcept
ServiceAlias -[:ALIAS_OF]-> ProtocolService
ObservationAlias -[:ALIAS_OF]-> ObservationConcept

// Assertion → Concept
ProtocolAssertion -[:REQUIRES_SIGN]-> ClaimSignConcept
ProtocolAssertion -[:INDICATES_SERVICE]-> ProtocolService
ProtocolAssertion -[:ABOUT_DISEASE]-> DiseaseEntity
ProtocolAssertion -[:CONTRAINDICATES]-> ProtocolService

// Summary → Disease (acceleration only)
ProtocolDiseaseSummary -[:SUMMARIZES]-> ProtocolDisease

// Provenance
ProtocolSection -[:HAS_CHUNK]-> RawChunk
ProtocolSection -[:CONTAINS_ASSERTION]-> ProtocolAssertion
```

## 11. Tổng kết

### Triết lý

Dùng LLM như **công cụ extract chính xác** vào cấu trúc ontology đã thiết kế, KHÔNG phải như **công cụ discover** cấu trúc ngẫu nhiên. GraphRAG cho ta kỹ thuật (gleaning, summarization, strength scoring), ontology cho ta cấu trúc (4 layers, typed nodes, typed relationships).

### Nguyên tắc kiến trúc

**Dual representation with provenance**: Một thực thể luôn có ít nhất 4 lớp:
1. Text gốc (RawChunk)
2. Mention bóc từ text (RawMention)
3. Canonical concept (Concept)
4. Assertion/evidence dùng để suy luận (Assertion)

Đó mới là cách:
- Không mất synonym
- Không mất ngữ cảnh
- Vẫn suy luận chuẩn
- Vẫn scale được
- Vẫn audit được

### Kết quả kỳ vọng

Từ 1 PDF phác đồ → extract chính xác vào 6 lớp (RawChunk → RawMention → Alias → Concept → Assertion → Summary) → validate against schema → canonical resolution có trạng thái → summary cho retrieval → sẵn sàng cho claims reasoning với full provenance chain.
