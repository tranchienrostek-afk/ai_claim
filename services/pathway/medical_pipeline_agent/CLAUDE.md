# Medical Pipeline Agent — Rules & Constraints

This agent autonomously ingests medical PDF protocols into a Neo4j knowledge graph.
It MUST follow these rules strictly.

## ABSOLUTE RULES (Never violate)

1. **NEVER delete existing data** in Neo4j. Only ADD new nodes/relationships.
2. **NEVER modify production code** (medical_agent.py, academic_agent.py, api_server.py) without running tests BEFORE and AFTER. If accuracy drops, REVERT immediately.
3. **NEVER commit or push** without explicit user approval.
4. **NEVER expose API keys** or .env contents in logs or output.
5. **ALL pipeline runs MUST be logged** to `data/pipeline_runs/{timestamp}/` with full audit trail.

## ACCURACY THRESHOLDS

- Minimum accuracy to accept ingestion: **80%**
- If accuracy < 80% after optimization: STOP and report to user
- If accuracy drops from previous run: REVERT changes and report
- Maximum self-improvement iterations: **5**

## PIPELINE PHASES (must execute in order)

### Phase 0: PDF Analysis
- Extract text with PyMuPDF
- Detect: language, number of diseases, TOC structure, page count
- Classify: single-disease vs multi-disease
- Output: `analysis.json` in run directory

### Phase 1: Pipeline Design
- Choose strategy: `universal_ingest.py` (single) or `multi_disease_ingest.py` (multi)
- Configure: entity types, chunk strategy, worker count
- Output: `pipeline_config.json` in run directory

### Phase 2: Ingestion
- Execute chosen pipeline
- Log all chunks created, diseases found, entities extracted
- Output: `ingestion_result.json` in run directory

### Phase 3: Quality Testing
- Generate test questions from ingested content (if no test file provided)
- Run test suite with LLM judge
- Score each question 0-1.0
- Output: `test_report.json` in run directory

### Phase 4: Self-Improvement (only if accuracy < target)
- Analyze failure patterns from test report
- Identify: missing chunks, wrong disease routing, weak search
- Apply fixes (re-chunk, add aliases, improve embeddings)
- Re-run tests
- Output: `optimization_log.json`

## FILE STRUCTURE

```
data/pipeline_runs/
└── {YYYY-MM-DD_HH-MM}_{pdf_name}/
    ├── analysis.json         # Phase 0 output
    ├── pipeline_config.json  # Phase 1 output
    ├── ingestion_result.json # Phase 2 output
    ├── test_report.json      # Phase 3 output
    ├── optimization_log.json # Phase 4 output (if needed)
    └── run_summary.json      # Final summary
```

## EXISTING SYSTEM CONTEXT

- Neo4j: bolt://localhost:7688, auth: neo4j/password123
- Embeddings: Azure OpenAI text-embedding-ada-002 (1536 dim)
- Chat: Azure OpenAI (model from .env MODEL2)
- Existing data: 65 diseases, 1479 chunks (DO NOT touch)
- Vector index: chunk_vector_index (cosine, 1536 dim)
- Fulltext index: chunk_fulltext
- Key files: universal_ingest.py, multi_disease_ingest.py, medical_agent.py

## SAFETY HOOKS

- PreToolUse hook validates all Bash commands before execution
- Blocked patterns: `DROP`, `DELETE`, `DETACH DELETE`, `rm -rf`, `git push`
- Neo4j write operations must target ONLY new disease_name namespaces
