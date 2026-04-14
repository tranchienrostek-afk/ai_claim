# System Architecture — Antigravity Bio-Medical Knowledge Graph-RAG

Hệ thống chuyển đổi PDF phác đồ y tế Việt Nam → Knowledge Graph (Neo4j) → Agentic Q&A với ReAct reasoning, trích dẫn APA/MLA, và tự cải tiến pipeline.

**Trạng thái hiện tại (2026-03):**
- 2,222 chunks | 73+ bệnh | 393 protocols | 2,216 embeddings
- Dữ liệu: Sốt xuất huyết Dengue (489), Ung thư vú BYT+Vinmec (447), TMH 63 bệnh (~1,479), Viêm gan B (85), Thận mạn (87), COPD (65), Suy tim (60), Huyết khối TM (76), COVID-19 (8)
- Benchmark: **93.8% accuracy** (80 câu hỏi ung thư vú, 20 workers)

---

## TỔNG QUAN KIẾN TRÚC

```
┌───────────────────────────────────────────────────────────────────────────────────┐
│                         ANTIGRAVITY BIO-MEDICAL ENGINE                           │
│                                                                                   │
│  ┌─────────────┐   ┌──────────────────┐   ┌────────────────┐   ┌──────────────┐  │
│  │  PDF Upload  │──▶│ Universal Ingest  │──▶│    Neo4j KG    │──▶│ Agentic RAG  │  │
│  │  (Web/CLI)   │   │ (20 workers)     │   │  (2222 chunks) │   │ (ReAct Loop) │  │
│  └─────────────┘   └──────────────────┘   └────────────────┘   └──────────────┘  │
│         │                    │                      │                    │         │
│         │           ┌───────┴────────┐     ┌───────┴────────┐   ┌──────┴───────┐ │
│         │           │  Multi-Disease  │     │ Experience     │   │  API Server  │ │
│         │           │  Splitter       │     │ Memory (Neo4j) │   │  (port 9600) │ │
│         │           └────────────────┘     └────────────────┘   └──────────────┘ │
│         │                                                              │          │
│         └──────────────────────────────────────────────────────────────┘          │
│                              Web Dashboard (4 tabs)                               │
└───────────────────────────────────────────────────────────────────────────────────┘
```

---

## PHẦN A: INGESTION PIPELINE (PDF → Neo4j)

### A1. Pipeline Tổng Thể

```
PDF File
  │
  ├── [Phase 1] DocumentAnalyzer (LLM)
  │     Sample pages → detect disease, ICD, domain, source_type, hospital
  │     Output: DocumentProfile (Pydantic)
  │
  ├── [Phase 2] PipelineConfigurator (LLM)
  │     Generate entity types, ontology, extraction prompts
  │     Output: PipelineConfig (entity_types, extraction_prompt, ontology_rules)
  │
  └── [Phase 3] UniversalIngest (20 workers parallel)
        ├── Text Extract (PyMuPDF)
        ├── OCR Cleanup (NFC + line joins)
        ├── Semantic Chunking (heading-based, max 3000 chars)
        ├── ThreadPoolExecutor(max_workers=20):
        │     Mỗi worker:
        │     ├── Entity Extraction (LLM, gpt-4o-mini)
        │     ├── Embedding (ada-002, 1536-dim)
        │     └── Neo4j MERGE (worker-local session, thread-safe)
        └── Build hierarchy (NEXT_CHUNK + HAS_CHILD links)
```

### A2. Trích Xuất Text

**File:** `v2_ingest.py:extract_text()` / `universal_ingest.py`

```
PDF (PyMuPDF/fitz)
  │
  ├── fitz.open(pdf_path)
  ├── Mỗi trang: page.get_text("blocks")
  │     → Trả về list of blocks: (x0, y0, x1, y1, text, block_no, type)
  ├── Sort blocks theo tọa độ: top→bottom, left→right
  └── Gộp thành text với page markers: "--- Page N ---"
```

### A3. Làm Sạch OCR

**File:** `v2_ingest.py:clean_ocr_text()`

```
Raw OCR text
  ├── 1. unicodedata.normalize("NFC")     → Ghép tổ hợp dấu decomposed
  ├── 2. Strip control chars              → Loại bỏ \x00-\x08, \x0b, ...
  ├── 3. Fix line joins                   → Nối dòng bị OCR cắt sai
  └── 4. Collapse spaces                  → Nhiều space → 1, 3+ blank → 2
```

### A4. Semantic Chunking

**File:** `v2_ingest.py:SemanticChunker`

```
Cleaned text
  │
  ├── Skip page 1 (legal header BYT)
  │
  ├── Detect headings bằng regex:
  │     ┌─────────────────────────────────┬──────────┐
  │     │ Pattern                         │ Level    │
  │     ├─────────────────────────────────┼──────────┤
  │     │ ^[IVX]+\.\s*[A-ZĐÀ]            │ section  │
  │     │ ^\d+\.\s+[A-Za-z]              │ section  │
  │     │ ^\d+\.\d+\.?\s+                │ subsect  │
  │     │ ^[a-z]\)\s+                    │ paragr   │
  │     │ ^[-•+]\s+                      │ paragr   │
  │     │ ^Bảng\s+\d+|^Phụ lục          │ section  │
  │     │ ^Lưu đồ\s+\d+                 │ section  │
  │     └─────────────────────────────────┴──────────┘
  │
  ├── Maintain section_path stack (breadcrumb):
  │     VD: "I. ĐẠI CƯƠNG > 1. Định nghĩa > 1.1. Viêm gan B cấp"
  │
  ├── Post-processing:
  │     ├── Split chunks > 3000 chars tại sentence boundary
  │     └── Merge chunks < 100 chars vào chunk trước
  │
  └── Output: list[dict]
        {chunk_id, content, title, level, section_path, page_number, parent_context}
```

### A5. LLM Entity Extraction (Universal)

**File:** `universal_ingest.py:extract_entities()`

```
Mỗi semantic chunk
  │
  ├── PipelineConfig cung cấp:
  │     - entity_types: ["Drug", "LabTest", "Disease", "Symptom", "Stage", ...]
  │     - extraction_prompt: prompt tùy chỉnh theo bệnh
  │     - suggested_entities: danh sách gợi ý
  │
  ├── Prompt → Azure OpenAI (gpt-4o-mini), temperature=0.0
  │     "Extract medical entities from this text..."
  │
  └── Output: [{name: "Trastuzumab", type: "Drug"}, {name: "HER2", type: "Biomarker"}, ...]
```

### A6. Parallel Chunk Processing (20 Workers)

**File:** `universal_ingest.py` — ThreadPoolExecutor

```python
def _process_one_chunk(idx_chunk):
    i, chunk = idx_chunk
    entities = self.extract_entities(chunk['content'], chunk['section_path'], config)
    embed_text = f"{chunk['title']}\n{chunk.get('parent_context', '')}\n{chunk['content']}"
    embedding = self.get_embedding(embed_text)       # ada-002 → 1536-dim
    with self.driver.session() as worker_session:     # Thread-safe: mỗi worker 1 session
        self.ingest_chunk(worker_session, chunk, config, entities, embedding)

with ThreadPoolExecutor(max_workers=20) as executor:
    futures = {executor.submit(_process_one_chunk, (i, c)): i for i, c in enumerate(chunks)}
    for future in as_completed(futures):
        # thread-safe stats update với threading.Lock
```

**Hiệu suất:** 435 chunks / ~5 phút (BYT ung thư vú 60 trang)

### A7. Multi-Disease PDF Splitter

**File:** `multi_disease_ingest.py`

```
Multi-disease PDF (VD: TMH 63 bệnh)
  │
  ├── TOC parsing (Vietnamese heading patterns)
  ├── Split → từng section per disease
  ├── ThreadPoolExecutor(max_workers=10-15)
  │     Mỗi worker: UniversalIngest instance riêng (thread safety)
  ├── Skip diseases đã ingest (resume support)
  └── Output: 63 diseases, ~1,479 chunks
```

### A8. Neo4j Ingestion (MERGE)

```
Mỗi chunk → Cypher MERGE:
  │
  ├── MERGE (:Chunk {chunk_id}) SET content, title, section_path, disease_name,
  │     page_number, embedding[1536], source_type, hospital_name, block_id
  │
  ├── MERGE (:Disease {name}) SET icd_code, aliases
  ├── MERGE (:Protocol {name}) SET source_type, hospital_name
  │
  ├── CREATE (chunk)-[:ABOUT_DISEASE]->(disease)
  ├── CREATE (protocol)-[:HAS_BLOCK]->(chunk)
  ├── CREATE (protocol)-[:COVERS_DISEASE]->(disease)
  │
  ├── For each entity:
  │     MERGE (:Drug/:LabTest/:Symptom/:Stage/:Biomarker {name})
  │     CREATE (chunk)-[:MENTIONS]->(entity)
  │
  └── Post-ingest:
        ├── BUILD NEXT_CHUNK links (sequential order)
        ├── BUILD HAS_CHILD links (section hierarchy)
        └── BUILD FROM_HOSPITAL links
```

---

## PHẦN B: QUERY PIPELINE (Câu hỏi → Câu trả lời)

### B1. Request Flow

```
Client (Browser Dashboard)
  │
  ├── POST /api/ask
  │     {question, deep_reasoning, session_id, hospital_name, top_k, style}
  │
  └── FastAPI Router (api_server.py:9600)
        │
        ├── SessionStore: get/create session (max 20 turns)
        │
        ├── [if deep_reasoning=true]
        │     └── agentic_ask() → ReAct Loop (Phần B3)
        │
        └── [normal mode]
              ├── resolve_disease_name(query) → Disease routing
              ├── priority_search() / scoped_search() / enhanced_search()
              └── generate_academic_response() → APA/MLA citations
```

### B2. Search Engine (6 Algorithms)

#### Algorithm 1: Reciprocal Rank Fusion (RRF)

```
Vector results (ranked by cosine similarity)  ──┐
                                                  ├── RRF Merge ──▶ Final ranking
Fulltext results (ranked by BM25 score)        ──┘

RRF Score = Σ 1/(k + rank_i)    # k=60, scale-invariant, rank-based
```

- **Tại sao không dùng max(score)?** Vector score (0-1 cosine) và BM25 score (0-∞ Lucene) có scale khác nhau → so sánh trực tiếp bị bias.
- **RRF:** Chỉ dùng thứ hạng, không cần normalize score → merge công bằng.

#### Algorithm 2: Context Window Expansion

```
Cypher đã fetch prev_block_content + next_block_content
  │
  └── _expand_context(chunk):
        "[Ngữ cảnh trước] {prev[:500]}"
        "{chunk.description}"
        "[Ngữ cảnh sau] {next[:500]}"
        → expanded_description (đưa vào LLM prompt)
```

- **Chi phí bằng 0:** Dùng data đã fetch sẵn từ NEXT_CHUNK traversal
- **Tăng context:** LLM thấy nhiều hơn → câu trả lời đầy đủ hơn

#### Algorithm 3: BM25 Title Boosting

```
_build_boosted_fulltext_query(query, disease):
  │
  └── "title:{query}^3 OR content:{query}"
       Title boost 3x → heading matches ưu tiên hơn body text
```

#### Algorithm 4: Query Expansion (Aliases + ICD)

```
Khi search với disease_name:
  │
  ├── MATCH (d:Disease {name: disease_name})
  │     RETURN d.aliases, d.icd_code
  │
  └── Append to fulltext query:
        "content:{alias1}^0.5 OR content:{alias2}^0.5 OR content:{icd_code}^0.5"
```

VD: "Ung thư vú" → thêm "C50", "Breast cancer", "ung thư tuyến vú"

#### Algorithm 5: Adaptive Retrieval Strategy

```python
_STRATEGY_MAP = {
    "lookup":           {"graph": False, "rerank": False, "sub_query_threshold": 3},
    "general":          {"graph": False, "rerank": False, "sub_query_threshold": 5},
    "dosage":           {"graph": False, "rerank": False, "sub_query_threshold": 3},
    "procedure":        {"graph": False, "rerank": True,  "sub_query_threshold": 5},
    "diagnosis":        {"graph": True,  "rerank": True,  "sub_query_threshold": 5},
    "contraindication": {"graph": True,  "rerank": True,  "sub_query_threshold": 3},
    "compare":          {"graph": True,  "rerank": True,  "sub_query_threshold": 3},
}
```

- **Tiết kiệm LLM calls:** lookup/dosage bỏ qua graph + rerank
- **Chính xác hơn:** contraindication/compare bật graph traversal + LLM rerank

#### Algorithm 6: Confidence-gated Response

```
Confidence từ reason_and_verify():
  │
  ├── < 0.5  → ⚠️ "Không đủ thông tin để trả lời chính xác" + warning
  ├── 0.5-0.8 → Trả lời + disclaimer "Mức độ tin cậy: trung bình"
  └── ≥ 0.8  → Trả lời bình thường (an toàn y khoa)
```

### B3. Disease Routing

**File:** `medical_agent.py:resolve_disease_name()`

```
User query: "ung thu vu giai đoạn 2"
  │
  ├── [Step 1] Extract keywords, remove stop words
  │     → candidates: ["ung thu vu giai đoạn 2", "ung thu vu", ...]
  │
  ├── [Step 2] Cypher CONTAINS match (exact Unicode)
  │     MATCH (d:Disease)<-[:ABOUT_DISEASE]-(:Chunk)
  │     WHERE toLower(d.name) CONTAINS toLower($keyword)
  │     → Coverage threshold: ≥40%
  │
  ├── [Step 3] Fallback: Diacritical-stripped fuzzy match
  │     _strip_diacritics("ung thư vú") → "ung thu vu"
  │     _strip_diacritics("Ung thư vú") → "ung thu vu"
  │     → Match! Return "Ung thư vú"
  │
  └── Output: "Ung thư vú" hoặc None (→ enhanced_search unscoped)
```

**Xử lý edge cases:**
- ASCII input: "ung thu vu" → ✓ match
- Typo dấu: "ung thứ vú" → ✓ match (diacritical strip)
- Không có bệnh: → None → enhanced_search cross-disease

### B4. Search Modes

```
┌────────────────────────────────┬──────────────────────────────────────────────────┐
│ Mode                           │ Khi nào dùng                                     │
├────────────────────────────────┼──────────────────────────────────────────────────┤
│ scoped_search(disease)         │ Disease detected → Vector+Fulltext filtered      │
│ priority_search(disease, hosp) │ Hospital specified → 3-tier: hosp→BYT→other      │
│ enhanced_search()              │ No disease found → V1+V2 cross-disease fusion     │
│ agentic_ask() [Deep Reasoning] │ deep_reasoning=true → Full ReAct loop            │
└────────────────────────────────┴──────────────────────────────────────────────────┘
```

#### Scoped Search (V2)

```
scoped_search(query, disease, top_k=8)
  │
  ├── _scoped_vector_search()
  │     CALL db.index.vector.queryNodes('chunk_vector_index', 120, $qv)
  │     WHERE chunk.disease_name = $disease
  │     + Optional: source_type/hospital_name filter
  │
  ├── scoped_fulltext_search()
  │     CALL db.index.fulltext.queryNodes('chunk_fulltext', $boosted_query)
  │     WHERE node.disease_name = $disease
  │
  ├── _reciprocal_rank_fusion([vector, fulltext])
  │
  └── Fallback: nếu 0 results → graph_rag_search() V1
```

#### Priority Search (Hospital-tiered)

```
priority_search(query, disease, hospital_name="Vinmec")
  │
  ├── Tier 1: scoped_search(source_type="hospital", hospital_name="Vinmec")
  │     → Nếu có kết quả → return ("hospital")
  │
  ├── Tier 2: scoped_search(source_type="BYT")
  │     → Nếu có kết quả → return ("BYT")
  │
  └── Tier 3: scoped_search() (tất cả nguồn)
        → return ("other")
```

### B5. Agentic RAG — ReAct Loop (Deep Reasoning)

**File:** `medical_agent.py:agentic_ask()`

```
agentic_ask(question, history, max_reflect=2)
  │
  ├── Step 1: 🧠 Preprocess (Understand)
  │     LLM phân tích → intent, entities, disease_hint, sub_queries,
  │                       needs_graph_traversal, needs_verification
  │
  ├── Step 2: 🔍 Plan & Search (Act)
  │     Adaptive strategy based on intent:
  │     ├── Disease routing → resolve_disease_name()
  │     ├── Primary search → scoped/enhanced
  │     ├── Sub-query expansion (nếu primary < threshold)
  │     ├── Graph traversal (nếu strategy.graph=true)
  │     └── LLM Rerank (nếu strategy.rerank=true)
  │
  ├── Step 3: 💬 Generate Answer
  │     academic_agent.generate_academic_response() với context
  │
  ├── Step 4: 🔬 Verify (Reflect)
  │     LLM tự kiểm tra:
  │     - Mâu thuẫn với phác đồ?
  │     - Liều lượng/chống chỉ định chính xác?
  │     - Bỏ sót thông tin quan trọng?
  │     - Suy diễn ngoài phác đồ?
  │     → {is_safe, confidence, issues, needs_more_search, correction}
  │
  ├── [Loop] Nếu không safe / confidence < 0.7:
  │     ├── Tìm bổ sung (additional_query)
  │     ├── Apply correction nếu có
  │     └── Quay lại Step 3 (max 2 iterations)
  │
  └── Step 5: Confidence-gated Response
        ├── < 0.5 → Warning + unverified content
        ├── 0.5-0.8 → Answer + disclaimer
        └── ≥ 0.8 → Normal answer
```

**Output trace (hiển thị trên Dashboard):**
```json
{
  "steps": [
    {"phase": "🧠 Phân tích câu hỏi", "detail": "Intent: dosage | Disease: Ung thư vú", "ms": 1200},
    {"phase": "🔍 Truy xuất đa bước", "detail": "Tìm được 8 đoạn trong 2100ms", "ms": 2100},
    {"phase": "💬 Sinh câu trả lời (lần 1)", "detail": "Dựa trên 8 nguồn | 2500 ký tự", "ms": 3000},
    {"phase": "🔬 Kiểm chứng (lần 1)", "detail": "Confidence: 85% | Safe: ✓", "ms": 1500}
  ],
  "final_confidence": 0.85,
  "iterations": 1
}
```

### B6. Response Generation

**File:** `academic_agent.py:generate_academic_response()`

```
Context nodes + Question
  │
  ├── _expand_context() cho mỗi node       ← Context Window Expansion
  │
  ├── LLM Metadata Extraction (cho citations):
  │     "Extract author, year, institution from title+snippet"
  │     → {author: "Bộ Y tế", year: 2020, institution: "Cục Quản lý KCB"}
  │
  ├── Format Citation (APA/MLA):
  │     APA: "Bộ Y tế. (2020). Phác đồ điều trị ung thư vú. Medical Protocol."
  │     MLA: "Bộ Y tế. \"Phác đồ điều trị ung thư vú.\" Medical Protocol, 2020."
  │
  ├── Build Context String:
  │     SOURCE [1]: Citation + expanded_description
  │     SOURCE [2]: ...
  │
  └── LLM Generate (gpt-5-mini, temp=0.1):
        System: "Bạn là chuyên gia nghiên cứu y khoa..."
        → Markdown response with [1], [2] citations
```

---

## PHẦN C: NEO4J GRAPH MODEL

### C1. Clinical Data Nodes

```
(:Chunk)  — PDF-ingested content (V2, primary)
  Properties: chunk_id, content, title, section_path, disease_name,
              page_number, embedding[1536], source_type, hospital_name, block_id

(:Disease) — Disease entity
  Properties: name, icd_code, aliases[], category

(:Protocol) — Umbrella node per PDF/document
  Properties: name, source_type ("BYT"|"hospital"), hospital_name

(:Hospital) — Healthcare facility
  Properties: name, location

(:Drug), (:Symptom), (:LabTest), (:Procedure), (:Complication),
(:Stage), (:Biomarker) — Extracted medical entities

(:Block) — Web-crawled content (V1, legacy)
  Properties: title, content, embedding, page_number, id

(:Entity) — V1 generic entity (legacy)
(:OntologyClass) — Schema definitions
```

### C2. Key Relationships

```
(:Chunk)-[:ABOUT_DISEASE]->(:Disease)           # Disease scoping for search
(:Chunk)-[:NEXT_CHUNK]->(:Chunk)                # Sequential reading order
(:Chunk)-[:HAS_CHILD]->(:Chunk)                 # Section hierarchy
(:Chunk)-[:MENTIONS]->(:Drug|:Symptom|...)      # Entity linking
(:Protocol)-[:COVERS_DISEASE]->(:Disease)       # Multi-disease PDF grouping
(:Protocol)-[:HAS_BLOCK]->(:Chunk)              # Protocol → chunks
(:Protocol)-[:FROM_HOSPITAL]->(:Hospital)       # Hospital source
(:Disease)-[:CLASSIFIED_AS]->(:ICD_Category)    # ICD classification
```

### C3. Indexes

```
chunk_vector_index     — Cosine 1536-dim on :Chunk.embedding      (primary)
chunk_fulltext         — Lucene on :Chunk.content, :Chunk.title    (primary)
block_vector_index     — Cosine 1536-dim on :Block.embedding       (V1 legacy)
block_fulltext         — Lucene on :Block.title, :Block.content    (V1 legacy)
experience_vector_index — Cosine 1536-dim on :Experience.embedding (learning)
experience_fulltext    — Lucene on :Experience.search_text         (learning)
clinical_vector_index  — Legacy on :Page.embedding                 (web crawl)
```

### C4. Experience Memory Nodes

```
(:Experience) — Unified vector-searchable entry point
  Properties: experience_id, type, embedding[1536], search_text

(:OntologyTemplate) — Successful ontology configs per disease
  Properties: template_id, disease_name, medical_domain, entity_types_json, accuracy_score

(:PipelineRunLog) — Execution history
  Properties: run_id, pdf_name, strategy, accuracy_pct, passed

(:OptimizationLesson) — Error patterns and fixes
  Properties: lesson_id, error_analysis, strategies_applied, accuracy_before

(:SystemPromptVersion) — Prompt evolution
  Properties: prompt_id, prompt_text, prompt_hash, accuracy_score, is_best

Relationships:
(:Experience)-[:HAS_TEMPLATE]->(:OntologyTemplate)
(:Experience)-[:HAS_RUN]->(:PipelineRunLog)
(:Experience)-[:HAS_LESSON]->(:OptimizationLesson)
(:Experience)-[:HAS_PROMPT]->(:SystemPromptVersion)
(:PipelineRunLog)-[:USED_TEMPLATE]->(:OntologyTemplate)
(:PipelineRunLog)-[:USED_PROMPT]->(:SystemPromptVersion)
```

### C5. Full Graph Diagram

```
                              ┌─────────────────────────────────────────────────────┐
                              │                    NEO4J DATABASE                    │
                              │                                                     │
                              │  V2 Subgraph (Primary — 73+ diseases)              │
                              │  ┌─────────────────────────────────────────────┐    │
                              │  │ Protocol──HAS_BLOCK──▶Chunk──ABOUT_DISEASE─▶│    │
                              │  │  2222 chunks │   Disease──CLASSIFIED_AS──▶  │    │
                              │  │              │   ICD_Category               │    │
                              │  │         NEXT_CHUNK  │                      │    │
                              │  │         HAS_CHILD   ├──MENTIONS──▶Drug     │    │
                              │  │                     ├──MENTIONS──▶LabTest  │    │
                              │  │                     ├──MENTIONS──▶Symptom  │    │
                              │  │                     ├──MENTIONS──▶Stage    │    │
                              │  │                     └──MENTIONS──▶Biomarker│    │
                              │  │   chunk_vector_index │ chunk_fulltext      │    │
                              │  └─────────────────────────────────────────────┘    │
                              │                                                     │
                              │  Experience Memory (Learning System)                │
                              │  ┌─────────────────────────────────────────────┐    │
                              │  │ Experience──HAS_TEMPLATE──▶OntologyTemplate │    │
                              │  │           ├──HAS_RUN──▶PipelineRunLog       │    │
                              │  │           ├──HAS_LESSON──▶OptimizationLesson│    │
                              │  │           └──HAS_PROMPT──▶SystemPromptVer   │    │
                              │  │   experience_vector_index                   │    │
                              │  └─────────────────────────────────────────────┘    │
                              │                                                     │
                              │  Shared Ontology                                    │
                              │    ICD_Chapter──▶ICD_Category──▶Disease             │
                              │    Drug, LabTest, Stage, Biomarker (shared)         │
                              └─────────────────────────────────────────────────────┘
                                         ▲                    │
                                         │                    │
              ┌──────────────────────────┤                    ├────────────────────────────┐
              │ INGESTION                │                    │                QUERY       │
              │                          │                    │                            │
   ┌──────────┴──────────┐    ┌──────────┴──────────┐    ┌───┴───────────┐    ┌───────────┴────────┐
   │  universal_ingest   │    │  multi_disease_     │    │ medical_agent │    │  academic_agent    │
   │                     │    │  ingest             │    │               │    │  (extends medical) │
   │ • 3-phase auto      │    │                     │    │ • 6 algorithms│    │                    │
   │ • 20 workers        │    │ • TOC parsing       │    │ • RRF merge   │    │ • Metadata extract │
   │ • LLM analyze PDF   │    │ • Per-disease split │    │ • ReAct loop  │    │ • APA/MLA citation │
   │ • LLM config pipe   │    │ • 10-15 workers     │    │ • Confidence  │    │ • Context expansion│
   │ • Thread-safe       │    │ • Resume support    │    │ • Diacritic   │    │                    │
   └─────────────────────┘    └─────────────────────┘    └───────────────┘    └────────────────────┘
                                                                                        │
                                                                              ┌─────────┴─────────┐
                                                                              │   api_server.py   │
                                                                              │   FastAPI :9600   │
                                                                              │                   │
                                                                              │ POST /api/ask     │
                                                                              │ POST /api/ingest  │
                                                                              │ WS /ws/pipeline/* │
                                                                              │ GET /api/stats    │
                                                                              │ GET /api/diseases │
                                                                              │ GET /api/protocols│
                                                                              │ GET /api/hospitals│
                                                                              │ GET /api/pdfs     │
                                                                              │ POST /api/crawl   │
                                                                              └─────────┬─────────┘
                                                                                        │
                                                                              ┌─────────┴─────────┐
                                                                              │   Web Dashboard   │
                                                                              │                   │
                                                                              │ ┌───┬───┬───┬───┐ │
                                                                              │ │Q&A│Ing│His│Crw│ │
                                                                              │ │   │est│tor│ler│ │
                                                                              │ │   │   │y  │   │ │
                                                                              │ ├───┴───┴───┴───┤ │
                                                                              │ │ PDF Viewer    │ │
                                                                              │ │ + Citations   │ │
                                                                              │ │ + Stats       │ │
                                                                              │ └───────────────┘ │
                                                                              └───────────────────┘
```

---

## PHẦN D: API SERVER

### D1. Endpoints

**File:** `api_server.py` — FastAPI, port **9600**

```
┌────────────────────────────────┬─────────────────────────────────────────────────┐
│ Endpoint                       │ Chức năng                                       │
├────────────────────────────────┼─────────────────────────────────────────────────┤
│ GET  /                         │ Web Dashboard (Jinja2 template)                 │
│ GET  /health                   │ Health check                                    │
│ POST /api/ask                  │ Q&A (deep_reasoning, session_id, hospital_name) │
│ POST /api/ingest               │ Upload PDF + test → 5-phase pipeline (bg thread)│
│ WS   /ws/pipeline/{run_id}     │ Real-time log streaming                         │
│ GET  /api/stats                │ Diseases, chunks, protocols, hospitals, entities │
│ GET  /api/diseases             │ List all diseases with chunk counts              │
│ GET  /api/protocols            │ List protocols with disease counts               │
│ GET  /api/hospitals            │ List hospitals                                   │
│ GET  /api/pdfs                 │ List PDF files                                   │
│ GET  /api/pipeline-runs        │ Pipeline history                                 │
│ GET  /api/ingest/{run_id}      │ Pipeline status by run_id                        │
│ POST /api/crawl                │ Trigger VN web crawler                           │
└────────────────────────────────┴─────────────────────────────────────────────────┘
```

### D2. Server Support Modules

```
server_support/
├── api_models.py        # Pydantic models: QuestionRequest, AskResponse, Source, TraceStep
├── paths.py             # Path constants: BASE_DIR, UPLOADS_DIR, RUNS_DIR, etc.
├── pdf_catalog.py       # DiseasePdfCatalog: map disease → PDF file for viewer
├── pipeline_store.py    # PipelineRunStore: run history, log capture, WebSocket listeners
└── session_store.py     # SessionStore: multi-turn sessions, max 20 turns
```

### D3. Session Memory

```
POST /api/ask {session_id: "abc123", question: "..."}
  │
  ├── SessionStore.get_or_create("abc123")
  │     → [existing history or new empty list]
  │
  ├── Append user message to session
  ├── Generate answer (with history context)
  ├── Append assistant message to session
  │
  └── Max 20 turns per session (FIFO eviction)

Session format: [{"role": "user", "content": "..."}, {"role": "assistant", "content": "..."}]
Agentic conversion: [{"q": "user msg", "a": "assistant msg"}] for preprocess_query
```

---

## PHẦN E: AUTONOMOUS PIPELINE ORCHESTRATOR

**File:** `medical_pipeline_agent/scripts/orchestrator.py`

```
5-Phase Autonomous Pipeline:
  │
  ├── Phase 0: PDF Analysis
  │     PyMuPDF extract, language detect, TOC parse
  │     Classify: single-disease / multi-disease
  │
  ├── Phase 1: Pipeline Design
  │     Choose strategy (universal_ingest vs multi_disease_ingest)
  │     Skip existing diseases, configure workers
  │
  ├── Phase 2: Ingestion
  │     Execute chosen pipeline (parallel workers)
  │
  ├── Phase 3: Quality Testing
  │     LLM-as-judge scores Q&A (0/0.25/0.5/0.75/1.0)
  │     Auto-generate test if none provided
  │
  └── Phase 4: Self-Improvement
        LLM failure analysis → fix strategies
        (rechunk, enrich, alias, prompt improvement)
        → Re-test → rollback if accuracy drops
        Max 5 iterations, 3 consecutive drops = stop

Usage:
  python orchestrator.py "protocol.pdf" --test-file "test.json" \
    --target-accuracy 0.85 --max-optimize 3
```

### Experience Memory (Learning System)

**File:** `medical_pipeline_agent/scripts/experience_memory.py`

```
query_before_run(disease, domain):
  → Similar templates, past runs, lessons, best prompts
  → Informs pipeline configuration

save_after_run(run_result):
  → OntologyTemplate, PipelineRunLog, OptimizationLesson, SystemPromptVersion
  → All with 1536-dim embeddings for future RAG retrieval

CLI: python experience_memory.py [stats|templates|query|backfill]
```

---

## PHẦN F: TESTING & EVALUATION

### F1. Benchmark Runner

**File:** `test_runner.py` — Parallel benchmark with LLM-as-judge

```
ParallelClinicalRunner(max_workers=20)
  │
  ├── Load test file (JSON):
  │     Supports Vietnamese keys: cau_hoi, dap_an, phan_loai
  │     Supports English keys: question, answer, topic, scenario
  │
  ├── ThreadPoolExecutor(20 workers):
  │     Mỗi câu: agent.scoped_search() → generate_response() → LLM judge
  │
  ├── LLM-as-Judge scoring:
  │     ┌─────────┬────────────────────────────────────┐
  │     │ Score   │ Meaning                             │
  │     ├─────────┼────────────────────────────────────┤
  │     │ 1.0     │ Hoàn toàn chính xác                │
  │     │ 0.75    │ Đúng cơ bản, thiếu chi tiết nhỏ    │
  │     │ 0.5     │ Đúng một phần, thiếu nội dung chính│
  │     │ 0.25    │ Sai phần lớn hoặc lạc đề           │
  │     │ 0.0     │ Hoàn toàn sai hoặc không trả lời   │
  │     └─────────┴────────────────────────────────────┘
  │
  └── Output: accuracy %, per-category breakdown, XLSX report
```

### F2. Benchmark Results

```
┌─────────────────────────────────────────┬────────┬──────────┬───────────┐
│ Dataset                                 │ Câu hỏi│ Accuracy │ Workers   │
├─────────────────────────────────────────┼────────┼──────────┼───────────┤
│ Ung thư vú BYT (80 câu, 4 test files)  │   80   │  93.8%   │ 20        │
│ Viêm gan vi rút B (50 câu)             │   50   │  97.5%   │ 20        │
│ Sốt xuất huyết Dengue (V1)             │   -    │   -      │ -         │
└─────────────────────────────────────────┴────────┴──────────┴───────────┘
```

---

## PHẦN G: WEB DASHBOARD

**File:** `templates/index.html` — 4 tabs

```
┌─────────────────────────────────────────────────────────────────────┐
│  🏥 Antigravity Clinical Knowledge Engine                          │
├──────┬──────────┬──────────────┬─────────┬─────────────────────────┤
│ Q&A  │ Pipeline │ History/Eval │ Crawler │                         │
│ Chat │ Ingest   │              │         │                         │
├──────┴──────────┴──────────────┴─────────┤   Stats Sidebar         │
│                                          │   • Bệnh: 73+           │
│  ┌───────────────────┐ ┌──────────────┐  │   • Chunks: 2,222      │
│  │   Chat Area       │ │ PDF Viewer   │  │   • Protocols: 8       │
│  │                   │ │              │  │   • Hospitals: 1       │
│  │  [Deep Reasoning] │ │  Click [1]   │  │   • Entities: 500+    │
│  │   toggle          │ │  → jump to   │  │                        │
│  │                   │ │  source page │  │                        │
│  │  Reasoning Trace  │ │              │  │                        │
│  │  (expandable)     │ │              │  │                        │
│  │                   │ │              │  │                        │
│  │  Source cards     │ │              │  │                        │
│  │  with page badges│ │              │  │                        │
│  └───────────────────┘ └──────────────┘  │                        │
└──────────────────────────────────────────┴────────────────────────┘
```

**Features:**
- **Deep Reasoning toggle** — enables ReAct agentic loop, shows reasoning trace
- **PDF Viewer** — clickable citation [1][2] → auto-scroll to source page
- **Real-time Pipeline** — WebSocket log streaming during ingestion
- **Session Memory** — multi-turn conversation context
- **Hospital Filter** — priority search for hospital-specific protocols

---

## PHẦN H: INFRASTRUCTURE

```
┌─────────────────────────────────────────────┐
│             Docker Compose                   │
│                                             │
│  ┌─────────────────────────────────────┐    │
│  │  neo4j_pathway (neo4j:5-community)  │    │
│  │  • Bolt: localhost:7688 (→7687)     │    │
│  │  • Browser: localhost:7475 (→7474)  │    │
│  │  • Auth: neo4j / password123        │    │
│  └─────────────────────────────────────┘    │
└─────────────────────────────────────────────┘

External Services:
  • Azure OpenAI (Chat): gpt-5-mini (MODEL2)
  • Azure OpenAI (Embeddings): text-embedding-ada-002
  • Embedding dim: 1536

Environment (.env):
  • AZURE_OPENAI_ENDPOINT, AZURE_OPENAI_API_KEY
  • AZURE_EMBEDDINGS_ENDPOINT, AZURE_EMBEDDINGS_API_KEY
  • AZURE_OPENAI_API_VERSION
  • MODEL2 (chat model)
  • NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD
```

---

## PHẦN I: FILE STRUCTURE

```
notebooklm/
├── api_server.py                  # FastAPI server (port 9600)
├── medical_agent.py               # Agentic RAG Engine (6 algorithms + ReAct loop)
├── academic_agent.py              # Citation formatting (APA/MLA)
├── universal_ingest.py            # Single-disease PDF → Neo4j (20 workers parallel)
├── multi_disease_ingest.py        # Multi-disease splitter + parallel ingest
├── v2_ingest.py                   # Semantic chunker + entity extraction
├── test_runner.py                 # Parallel benchmark with LLM judge (20 workers)
├── run_benchmark_all.py           # Multi-file benchmark runner
├── rag_setup.py                   # Vector index + embedding setup
├── ontology_setup.py              # Neo4j ontology schema
├── runtime_env.py                 # Environment loader
├── .env                           # Azure OpenAI + Neo4j credentials
├── templates/index.html           # Dashboard UI (Vietnamese)
├── server_support/
│   ├── api_models.py              # Pydantic models
│   ├── paths.py                   # Path constants
│   ├── pdf_catalog.py             # Disease → PDF mapping
│   ├── pipeline_store.py          # Pipeline run history
│   └── session_store.py           # Multi-turn session store
├── medical_pipeline_agent/
│   ├── scripts/
│   │   ├── orchestrator.py        # 5-phase pipeline orchestrator
│   │   └── experience_memory.py   # Persistent learning system
│   ├── hooks/
│   │   ├── safety_check.py        # Command validation
│   │   └── audit_log.py           # Audit trail
│   └── CLAUDE.md                  # Pipeline agent rules
├── data/
│   ├── pipeline_runs/             # Output per pipeline run
│   ├── uploads/                   # Uploaded PDFs
│   ├── datatest/                  # Test datasets + reports
│   ├── extracted_text/
│   └── reports/
└── config/ingest_configs/         # Saved configs per disease
```

---

## PHẦN J: KEY CONVENTIONS

| Convention | Value |
|---|---|
| Neo4j Bolt | `bolt://localhost:7688` (Docker-mapped, not 7687) |
| API Server Port | **9600** |
| Embedding Model | `text-embedding-ada-002` (1536-dim) |
| Chat Model | `MODEL2` env var (default: `gpt-5-mini`) |
| UI Language | Vietnamese |
| Disease Routing | `resolve_disease_name()` → 40% coverage + diacritical fallback |
| Search Merge | Reciprocal Rank Fusion (k=60) |
| Hospital Priority | 3-tier: hospital → BYT → other |
| Deep Reasoning | `agentic_ask()` ReAct loop, max 2 reflect iterations |
| Confidence Gate | <0.5 refuse, 0.5-0.8 warn, ≥0.8 normal |
| Session Memory | Max 20 turns per session |
| Parallel Workers | 20 (ingest) / 20 (benchmark) |
| Benchmark Scoring | LLM-as-judge, 5-tier (0/0.25/0.5/0.75/1.0) |
| Pipeline Runs | Saved to `data/pipeline_runs/{timestamp}_{pdf}/` |
| Experience Memory | Every run saves learnings to Neo4j |
| Self-improvement | Max 5 iterations, rollback on accuracy drop |
