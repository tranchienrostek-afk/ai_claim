# NotebookLM Module

This folder was reorganized to reduce root clutter and separate runtime data from source code.

## Current layout

- `api_server.py`, `academic_agent.py`, `medical_agent.py`, `universal_ingest.py`, `v2_ingest.py`
  - Main runtime entry points and ingestion logic.
- `server_support/`
  - API helpers (path constants, session store, pipeline store, pdf catalog).
- `data/`
  - Runtime and benchmark data:
  - `uploads/`, `pipeline_runs/`, `extracted_text/`, `reports/`, `datatest/`.
  - `datatest/cases/` for benchmark inputs, `datatest/reports/` for outputs, `datatest/datasets/` for raw corpora, `datatest/assets/` for visuals, `datatest/source_docs/` for supporting source files.
- `config/ingest_configs/`
  - Generated ingest configs per disease/pipeline run.
- `medical_pipeline_agent/`
  - Pipeline plugin scripts and hooks.
- `workspaces/`
  - Large side workspaces moved out of root (`claims_insights/`, `agent_lab/`).
- `docs/`, `assets/reference_pdfs/`, `logs/`, `scripts/`, `templates/`, `static/`
  - Documentation, reference files, logs, utilities, and web assets.

## Data Architecture Contract

Pathway now has a first-class data architecture standard, similar in spirit to a folder architecture standard for code work.

- Machine-readable spec: `config/pathway_data_architecture_v1.json`
- Human-readable architecture note: `docs/architecture/pathway_data_architecture.md`
- Runtime bootstrap API: `GET /api/data-architecture/bootstrap`
- Platform bootstrap now includes a `data_architecture` summary block at `GET /api/platform/bootstrap`
- Local validation script:
  - `python scripts/utility/validate_pathway_data_architecture.py --summary-only`
  - `python scripts/utility/validate_pathway_data_architecture.py --ensure-layout --fail-on-missing`

This contract is intended to help Pathway understand its own data world:
- medical knowledge surfaces
- claims and contract surfaces
- benchmark and testcase surfaces
- runtime and observability surfaces
- memory and supervision surfaces

## Quick start

```bash
python api_server.py
```

- Dashboard/API: `http://localhost:9600`
- Health: `http://localhost:9600/health`

## Text To Neo4j

Protocol text files can now be ingested through a reasoning-first path that writes structured graph layers into `ontology_v2`.

```bash
cd notebooklm
python batch_text_to_graph.py --dry-run
python batch_text_to_graph.py --only "Benh Meniere" --skip-existing
python batch_text_to_graph.py --folder "Phac do tai mui hong" --engine ontology_v2 --namespace ontology_v2
```

The strengthened default flow is:
- text file -> document profile -> `ontology_v2` ingest -> structured Neo4j graph
- each run also writes a contract snapshot, per-file manifest, and batch report under `data/pipeline_runs/text_to_neo4j/`

## Notes

- API still serves PDFs via `/pdfs/...`.
- Cataloged PDFs are read from:
  - `data/uploads/`
  - `assets/reference_pdfs/`
- Server log path: `logs/server.log`.
- Common scripts:
  - `python scripts/migrations/migrate_add_source_type.py`
  - `python scripts/testing/run_benchmark_all.py`
  - `python scripts/utility/pdf_to_text.py`
