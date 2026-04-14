---
name: Pipeline Patterns
description: "Use when deciding between single-disease and multi-disease ingestion strategies, configuring chunk sizes, choosing between TOC-based and heading-based splitting, or adapting pipelines to new PDF formats. Triggers on 'choose pipeline', 'single vs multi disease', 'TOC parsing', 'chunk strategy', 'PDF splitting'."
version: 1.0.0
---

# Pipeline Patterns for Medical PDF Ingestion

## Decision Matrix: Which Pipeline?

| Signal | Single-Disease | Multi-Disease |
|--------|---------------|---------------|
| TOC with page numbers | - | YES |
| One disease name in title | YES | - |
| Multiple disease headings | - | YES |
| < 50 pages | Likely | Possible |
| > 100 pages | Unlikely | Likely |
| "Phác đồ" + single disease | YES | - |
| "Hướng dẫn chẩn đoán điều trị" + specialty | - | YES |

## Single-Disease Pipeline (universal_ingest.py)

**When**: PDF covers one disease/condition
**Entry point**: `UniversalIngest.auto_ingest(pdf_path)`

### 7-Phase Pipeline:
1. **Extract text** — PyMuPDF with page markers (`--- Page N ---`)
2. **Chunk text** — Semantic chunking (heading-aware, 500-1500 chars)
3. **Generate embeddings** — text-embedding-ada-002 (1536 dim)
4. **Extract entities** — LLM identifies Drug, Symptom, LabTest, etc.
5. **Create Neo4j nodes** — Chunk, Disease, entity nodes
6. **Create relationships** — ABOUT_DISEASE, NEXT_CHUNK, HAS_CHILD
7. **Create indexes** — chunk_vector_index, chunk_fulltext

### Key Parameter: `pre_extracted_text`
If calling from multi-disease pipeline, pass pre-sliced text to avoid re-reading PDF 63 times.

## Multi-Disease Pipeline (multi_disease_ingest.py)

**When**: PDF covers multiple diseases (e.g., ENT guidelines with 63 diseases)
**Entry point**: `MultiDiseaseIngest.auto_ingest(pdf_path, max_workers=10)`

### Pipeline:
1. **Parse TOC** — Read first 10-15 pages, extract disease names + page numbers
2. **LLM enrich** — Single LLM call to add ICD-10 codes to all diseases
3. **Split text** — Pre-extract text per disease by page boundaries
4. **Parallel ingest** — ThreadPoolExecutor with per-worker UniversalIngest instances
5. **Create Protocol** — Umbrella node linking to all Disease nodes
6. **Skip-already-done** — Query Neo4j to resume interrupted runs

### Thread Safety
Each worker creates its OWN:
- `AzureOpenAI` client (chat)
- `AzureOpenAI` client (embeddings)
- `GraphDatabase.driver` (Neo4j)

NO shared state between workers.

## TOC Parsing Strategies

### Strategy 1: Dotted lines (most common in Vietnamese BYT docs)
```
Pattern: "Disease Name ............ 45"
Regex: r'^(.+?)\s*\.{3,}\s*(\d+)'
```

### Strategy 2: Tabulated TOC
```
Pattern: "1. Disease Name          45"
Regex: r'^\d+\.\s+(.+?)\s{3,}(\d+)'
```

### Strategy 3: Part-grouped TOC (like TMH)
```
PHẦN I: TAI
1. Viêm tai ngoài ............ 13
2. Viêm tai giữa cấp ......... 15

PHẦN II: MŨI XOANG
3. Viêm mũi xoang cấp ........ 25
```
Parse part names for additional grouping metadata.

### Strategy 4: No TOC — Heading-based fallback
```
Scan body text for disease headings:
Pattern: numbered items in UPPERCASE Vietnamese
Filter: exclude sub-sections (ĐỊNH NGHĨA, NGUYÊN NHÂN, etc.)
```

## Chunk Strategy

### Semantic Chunking (default)
- Split on headings (##, numbered sections)
- Maintain parent context (section path)
- Overlap: 50-100 chars between chunks
- Min size: 100 chars, Max size: 2000 chars

### Page-Aware Chunking
- Preserve page number metadata per chunk
- Never split mid-sentence across pages
- Include page markers for citation

## Adapting to New Formats

When encountering an unknown PDF format:
1. Extract first 10 pages of text
2. Send to LLM: "What is the structure of this medical document?"
3. Based on response, choose strategy
4. Run on first 3 diseases as test
5. Verify chunks make sense before full run
