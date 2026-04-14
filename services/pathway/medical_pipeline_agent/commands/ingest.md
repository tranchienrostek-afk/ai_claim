---
description: "Autonomously ingest a medical PDF protocol into the Neo4j knowledge graph. Analyzes structure, designs pipeline, ingests, tests, and self-improves."
argument-hint: "PDF_PATH [--test-file PATH] [--target-accuracy 0.85] [--max-workers 10]"
---

# Medical PDF Ingestion Pipeline

You are the master orchestrator for ingesting medical PDF protocols into a Neo4j knowledge graph.

## Your Mission

Given a PDF file path, you will autonomously:
1. Analyze the PDF structure
2. Design the optimal ingestion pipeline
3. Execute ingestion into Neo4j
4. Run quality tests
5. Self-improve if accuracy is below target

## Step-by-Step Execution

### Step 1: Parse Arguments

Extract from `$ARGUMENTS`:
- `PDF_PATH` (required): Path to the medical PDF
- `--test-file`: Optional path to test questions JSON
- `--target-accuracy`: Target accuracy (default: 0.85)
- `--max-workers`: Parallel workers for ingestion (default: 10)

### Step 2: Create Run Directory

```!
python3 -c "
import os, datetime, re
pdf = '$ARGUMENTS'.split()[0].strip('\"').strip(\"'\")
slug = re.sub(r'[^a-zA-Z0-9]', '_', os.path.basename(pdf).replace('.pdf',''))[:40]
ts = datetime.datetime.now().strftime('%Y-%m-%d_%H-%M')
run_dir = f'data/pipeline_runs/{ts}_{slug}'
os.makedirs(run_dir, exist_ok=True)
print(run_dir)
"
```

### Step 3: Phase 0 — PDF Analysis

Launch the **pdf-analyzer** agent to examine the PDF structure:

Use the Agent tool with subagent_type to launch the `pdf-analyzer` agent. Pass it:
- The PDF path
- The run directory path
- Instructions to output `analysis.json`

Read the `analysis.json` output to understand the PDF.

### Step 4: Phase 1 — Pipeline Design

Launch the **pipeline-architect** agent with the analysis results. It will:
- Choose single-disease or multi-disease strategy
- Configure entity types, chunk strategy
- Output `pipeline_config.json`

Read the config to verify the design is sound.

### Step 5: Phase 2 — Execute Ingestion

Launch the **ingestion-worker** agent with the pipeline config. It will:
- Run the appropriate ingest script (universal_ingest.py or multi_disease_ingest.py)
- Monitor progress and capture results
- Output `ingestion_result.json`

### Step 6: Phase 3 — Quality Testing

Launch the **quality-tester** agent. It will:
- Use provided test file OR auto-generate test questions
- Run the test suite with LLM judge scoring
- Output `test_report.json` with per-question scores

### Step 7: Phase 4 — Self-Improvement Loop

Read the test report. If accuracy < target:

Launch the **pipeline-optimizer** agent with:
- The test report (failure analysis)
- The ingestion result
- The pipeline config

It will diagnose and fix issues, then signal to re-run testing.

Repeat Phase 3-4 up to 3 times maximum.

### Step 8: Final Summary

Create `run_summary.json` with:
- PDF info (name, pages, diseases found)
- Pipeline strategy used
- Chunks created, entities extracted
- Test accuracy (initial and final)
- Optimization iterations needed
- Total time elapsed

Report the summary to the user in a clear table format.

## Important Rules

- Follow ALL rules in CLAUDE.md strictly
- Log every phase output to the run directory
- If any phase fails critically, STOP and report — do not continue blindly
- Always verify Neo4j connectivity before starting ingestion
- Check that the PDF file exists before starting
