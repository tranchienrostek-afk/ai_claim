"""
Pipeline Orchestrator — Python entry point for the Medical Pipeline Agent.
Called by the web API when a user uploads a PDF.

Usage:
    python orchestrator.py "path/to/medical.pdf" [--test-file test.json] [--target-accuracy 0.85] [--max-workers 10]

This runs the full 5-phase pipeline:
  Phase 0: PDF Analysis
  Phase 1: Pipeline Design
  Phase 2: Ingestion
  Phase 3: Quality Testing
  Phase 4: Self-Improvement (if needed)
"""
import os
import sys
import json
import time
import argparse
import re
from pathlib import Path
from datetime import datetime

# Fix Windows encoding (skip if stdout has been redirected/captured)
if sys.stdout.encoding and sys.stdout.encoding != 'utf-8' and hasattr(sys.stdout, 'buffer'):
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

# Resolve paths
SCRIPT_DIR = Path(__file__).parent
PLUGIN_DIR = SCRIPT_DIR.parent
NOTEBOOKLM_DIR = PLUGIN_DIR.parent
sys.path.insert(0, str(NOTEBOOKLM_DIR))

from dotenv import load_dotenv
load_dotenv(NOTEBOOKLM_DIR / '.env')


def create_run_dir(pdf_path: str) -> Path:
    """Create timestamped run directory."""
    slug = re.sub(r'[^a-zA-Z0-9]', '_', Path(pdf_path).stem)[:40]
    ts = datetime.now().strftime('%Y-%m-%d_%H-%M')
    run_dir = NOTEBOOKLM_DIR / 'data' / 'pipeline_runs' / f'{ts}_{slug}'
    run_dir.mkdir(parents=True, exist_ok=True)
    return run_dir


def _write_json(path: Path, payload: dict) -> None:
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def _read_json(path: Path, default=None):
    if not path.exists():
        return default
    with open(path, 'r', encoding='utf-8') as f:
        return json.load(f)


def _decision_gate_enabled() -> bool:
    raw = str(os.getenv("PATHWAY_DECISION_GATE_ENABLED", "1")).strip().lower()
    return raw not in {"0", "false", "no", "off"}


def _record_decision_gate(run_dir: Path, checkpoint: str, payload: dict) -> dict:
    decision = dict(payload.get("decision") or {})
    saved = {
        "checkpoint": checkpoint,
        "decision": decision,
        "raw_content": payload.get("raw_content"),
        "duration_ms": payload.get("duration_ms"),
        "repair_attempts": payload.get("repair_attempts", 0),
        "claude_status": payload.get("claude_status"),
        "bridge_trace": payload.get("bridge_trace"),
    }
    _write_json(run_dir / f"decision_gate_{checkpoint}.json", saved)
    return saved


def _log_decision_gate(checkpoint: str, payload: dict) -> None:
    decision = payload.get("decision") or {}
    print(
        f"[DecisionGate] checkpoint={checkpoint} "
        f"action={decision.get('recommended_action', '?')} "
        f"confidence={decision.get('confidence', '?')} "
        f"stop={decision.get('stop_signal', '?')} "
        f"proceed={decision.get('proceed', '?')}"
    )
    if decision.get("reasoning"):
        print(f"[DecisionGate] reasoning: {decision['reasoning']}")
    for risk in (decision.get("risks") or [])[:3]:
        print(f"[DecisionGate] risk: {risk}")
    if decision.get("next_step"):
        print(f"[DecisionGate] next: {decision['next_step']}")


def _build_run_summary(
    *,
    run_dir: Path,
    analysis: dict,
    config: dict | None = None,
    ingestion: dict | None = None,
    test_report: dict | None = None,
    iteration: int = 0,
    elapsed: int = 0,
    status: str = "completed",
    decision_gate_events: list | None = None,
    human_decision_events: list | None = None,
) -> dict:
    config = config or {}
    ingestion = ingestion or {}
    test_report = test_report or {}
    return {
        "status": status,
        "pdf_name": analysis.get('pdf_name'),
        "pages": analysis.get('pages'),
        "classification": analysis.get('classification'),
        "diseases_detected": analysis.get('disease_count_estimate'),
        "diseases_ingested": config.get('disease_count_to_process', 0),
        "diseases_skipped": config.get('disease_count_skipped', 0),
        "total_chunks": ingestion.get('total_chunks', 0),
        "strategy": config.get('strategy', analysis.get('recommended_strategy')),
        "test_accuracy_pct": test_report.get('accuracy_pct', 0),
        "test_passed": test_report.get('passed', False),
        "optimization_iterations": iteration,
        "total_duration_seconds": elapsed,
        "run_directory": str(run_dir),
        "decision_gate_events": decision_gate_events or [],
        "human_decision_events": human_decision_events or [],
    }


def _record_human_decision(run_dir: Path, checkpoint: str, action: str, note: str | None = None) -> list:
    events = _load_human_decision_events(run_dir)
    event = {
        "timestamp": datetime.now().isoformat(),
        "checkpoint": checkpoint,
        "action": action,
        "note": note or "",
    }
    events.append(event)
    _write_json(run_dir / 'human_decision_events.json', {"events": events})
    return events


def _load_human_decision_events(run_dir: Path) -> list:
    raw = _read_json(run_dir / 'human_decision_events.json', {})
    if isinstance(raw, dict):
        return list(raw.get("events") or [])
    if isinstance(raw, list):
        return list(raw)
    return []


def _save_experience_memory(memory, summary: dict, config: dict, test_report: dict, run_dir: Path, iteration: int) -> None:
    if not memory:
        return

    try:
        opt_logs = []
        for i in range(1, iteration + 1):
            log_file = run_dir / f'optimization_log_iter{i}.json'
            if log_file.exists():
                with open(log_file, 'r', encoding='utf-8') as f:
                    opt_logs.append(json.load(f))

        ingest_config_data = None
        ingest_configs_dir = NOTEBOOKLM_DIR / 'config' / 'ingest_configs'
        if ingest_configs_dir.exists():
            configs = sorted(ingest_configs_dir.glob('*.json'), key=lambda p: p.stat().st_mtime, reverse=True)
            if configs:
                try:
                    with open(configs[0], 'r', encoding='utf-8') as f:
                        ingest_config_data = json.load(f)
                except Exception:
                    ingest_config_data = None

        final_prompt = None
        prompt_file = run_dir / 'improved_system_prompt.txt'
        if prompt_file.exists():
            final_prompt = prompt_file.read_text(encoding='utf-8').strip()

        memory.save_after_run(
            run_summary=summary,
            config=config,
            test_report=test_report,
            optimization_logs=opt_logs,
            ingest_config=ingest_config_data,
            system_prompt_text=final_prompt,
        )
        print(f"  [MEMORY] Experience saved to Neo4j graph")
    except Exception as e:
        print(f"  [MEMORY] Error saving experience: {e}")
    finally:
        memory.close()


def _complete_after_test(
    *,
    run_dir: Path,
    analysis: dict,
    config: dict,
    ingestion: dict,
    test_report: dict,
    target_accuracy: float,
    max_optimize_iterations: int,
    test_file: str | None,
    start_time: float,
    previous_elapsed: int = 0,
    decision_gate=None,
    decision_gate_events: list | None = None,
    human_decision_events: list | None = None,
    memory=None,
    run_post_test_gate: bool = True,
) -> dict:
    decision_gate_events = list(decision_gate_events or [])
    human_decision_events = list(human_decision_events or [])

    if run_post_test_gate and decision_gate:
        try:
            test_gate_result = decision_gate.decide(
                workflow="medical_ingest_pipeline",
                checkpoint="post_test",
                objective=(
                    "Quyet dinh co nen chap nhan ket qua hien tai, tiep tuc toi self-improvement, "
                    "tam dung cho human review, hay huy run."
                ),
                context="Checkpoint nay xay ra sau quality testing va truoc vong optimization.",
                state={
                    "strategy": config.get("strategy"),
                    "disease_count_to_process": config.get("disease_count_to_process"),
                    "total_chunks": ingestion.get("total_chunks", 0),
                    "accuracy_pct": test_report.get("accuracy_pct", 0),
                    "target_accuracy_pct": test_report.get("target_accuracy_pct", target_accuracy * 100),
                    "passed": test_report.get("passed", False),
                    "total_questions": test_report.get("total_questions", 0),
                    "failure_analysis": test_report.get("failure_analysis", {}),
                },
                candidate_actions=[
                    "accept_current_result",
                    "run_optimization",
                    "pause_for_human_review",
                    "abort_run",
                ],
            )
            saved_gate = _record_decision_gate(run_dir, "post_test", test_gate_result)
            decision_gate_events.append(saved_gate)
            _log_decision_gate("post_test", saved_gate)

            gate_action = saved_gate["decision"].get("recommended_action")
            if gate_action in {"pause_for_human_review", "abort_run"}:
                status = "paused_for_human_review" if gate_action == "pause_for_human_review" else "aborted_by_decision_gate"
                summary = _build_run_summary(
                    run_dir=run_dir,
                    analysis=analysis,
                    config=config,
                    ingestion=ingestion,
                    test_report=test_report,
                    iteration=0,
                    elapsed=previous_elapsed + int(time.time() - start_time),
                    status=status,
                    decision_gate_events=decision_gate_events,
                    human_decision_events=human_decision_events,
                )
                _write_json(run_dir / 'run_summary.json', summary)
                print(f"\n  [DecisionGate] Pipeline {status}")
                return summary

            if gate_action == "accept_current_result":
                print("  [DecisionGate] Accepting current test result and skipping optimization loop")
                max_optimize_iterations = 0
        except Exception as e:
            print(f"  [DecisionGate] post_test failed: {e}")

    iteration = 0
    prev_strategies = []
    best_accuracy = test_report.get('accuracy_pct', 0)
    best_test_report = test_report
    best_prompt = None
    consecutive_drops = 0

    prompt_file = run_dir / 'improved_system_prompt.txt'
    if prompt_file.exists():
        best_prompt = prompt_file.read_text(encoding='utf-8').strip()

    with open(run_dir / 'best_test_report.json', 'w', encoding='utf-8') as f:
        json.dump(best_test_report, f, ensure_ascii=False, indent=2)

    while not test_report.get('passed', False) and iteration < max_optimize_iterations:
        iteration += 1
        current_acc = test_report.get('accuracy_pct', 0)
        print(f"\n  --- Optimization iteration {iteration}/{max_optimize_iterations} ---")
        print(f"  Current: {current_acc}%  Best: {best_accuracy}%  Target: {target_accuracy*100}%")

        phase4_optimize(config, test_report, run_dir, iteration=iteration, prev_strategies=prev_strategies)
        test_report = phase3_test(config, run_dir, test_file, target_accuracy)
        new_acc = test_report.get('accuracy_pct', 0)
        print(f"  After iteration {iteration}: {new_acc}% (best: {best_accuracy}%)")

        if new_acc >= target_accuracy * 100:
            print(f"  TARGET REACHED! {new_acc}% >= {target_accuracy*100}%")
            best_accuracy = new_acc
            best_test_report = test_report
            if prompt_file.exists():
                best_prompt = prompt_file.read_text(encoding='utf-8').strip()
            break

        if new_acc > best_accuracy:
            best_accuracy = new_acc
            best_test_report = test_report
            consecutive_drops = 0
            if prompt_file.exists():
                best_prompt = prompt_file.read_text(encoding='utf-8').strip()
            with open(run_dir / 'best_test_report.json', 'w', encoding='utf-8') as f:
                json.dump(best_test_report, f, ensure_ascii=False, indent=2)
            print(f"  NEW BEST: {best_accuracy}%")
        else:
            consecutive_drops += 1
            print(f"  NO IMPROVEMENT (drop #{consecutive_drops})")
            if best_prompt and prompt_file.exists():
                with open(prompt_file, 'w', encoding='utf-8') as f:
                    f.write(best_prompt)
                print(f"  ROLLBACK: Restored best prompt ({len(best_prompt)} chars)")
            test_report = best_test_report

        if consecutive_drops >= 3:
            print(f"  STOPPING: 3 consecutive drops, reverting to best result ({best_accuracy}%)")
            test_report = best_test_report
            break

    if best_accuracy > test_report.get('accuracy_pct', 0):
        test_report = best_test_report
    if best_prompt:
        with open(prompt_file, 'w', encoding='utf-8') as f:
            f.write(best_prompt)
    with open(run_dir / 'test_report.json', 'w', encoding='utf-8') as f:
        json.dump(test_report, f, ensure_ascii=False, indent=2)

    elapsed = previous_elapsed + int(time.time() - start_time)
    summary = _build_run_summary(
        run_dir=run_dir,
        analysis=analysis,
        config=config,
        ingestion=ingestion,
        test_report=test_report,
        iteration=iteration,
        elapsed=elapsed,
        status="completed",
        decision_gate_events=decision_gate_events,
        human_decision_events=human_decision_events,
    )

    _write_json(run_dir / 'run_summary.json', summary)
    _save_experience_memory(memory, summary, config, test_report, run_dir, iteration)

    print(f"\n{'#'*60}")
    print(f"  PIPELINE COMPLETE")
    print(f"  Accuracy: {test_report.get('accuracy_pct', 0)}%")
    print(f"  Passed target: {'YES' if test_report.get('passed', False) else 'NO'}")
    print(f"  Optimizations: {iteration}")
    print(f"  Duration: {elapsed}s")
    print(f"  Run dir: {run_dir}")
    print(f"{'#'*60}\n")

    return summary


def phase0_analyze(pdf_path: str, run_dir: Path) -> dict:
    """Phase 0: Analyze PDF structure with Vietnamese quality assessment."""
    print(f"\n{'='*60}")
    print(f"  PHASE 0: PDF ANALYSIS")
    print(f"{'='*60}")

    import fitz
    # Import quality assessment from vision reader
    try:
        import sys as _sys
        _sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
        from server_support.pdf_vision_reader import _assess_text_quality
    except ImportError:
        _assess_text_quality = None

    doc = fitz.open(pdf_path)
    pages = len(doc)
    total_chars = sum(len(page.get_text()) for page in doc)

    # Language detection
    sample = " ".join(doc[i].get_text()[:500] for i in range(min(5, pages)))
    has_vietnamese = any(c in sample for c in "ắằẳẵặấầẩẫậốồổỗộứừửữự")

    # Vietnamese text quality assessment (detect garbled fonts)
    quality_scores = []
    if _assess_text_quality and pages > 0:
        sample_indices = [min(2, pages - 1)]
        if pages > 10:
            sample_indices.append(pages // 2)
        if pages > 20:
            sample_indices.append(pages * 3 // 4)
        for idx in sample_indices:
            text = doc[idx].get_text("text")
            score = _assess_text_quality(text)
            quality_scores.append(score)
    avg_quality = sum(quality_scores) / len(quality_scores) if quality_scores else 1.0
    has_garbled_font = avg_quality < 0.4

    # TOC detection: scan first 15 pages for table of contents patterns
    toc_pages = []
    diseases_detected = []
    toc_pattern = re.compile(r'^(.+?)\s*\.{3,}\s*(\d+)', re.MULTILINE)
    # Full Vietnamese character set (including all diacritical marks on plain vowels)
    _VN_UPPER = r'A-ZĐÀÁẢÃẠĂẮẰẲẴẶÂẤẦẨẪẬÈÉẺẼẸÊẾỀỂỄỆÌÍỈĨỊÒÓỎÕỌÔỐỒỔỖỘƠỚỜỞỠỢÙÚỦŨỤƯỨỪỬỮỰỲÝỶỸỴ'
    _VN_LOWER = r'a-zđàáảãạăắằẳẵặâấầẩẫậèéẻẽẹêếềểễệìíỉĩịòóỏõọôốồổỗộơớờởỡợùúủũụưứừửữựỳýỷỹỵ'
    numbered_pattern = re.compile(rf'^\d+\.\s+([{_VN_UPPER}][{_VN_UPPER}{_VN_LOWER}\s\-\(\),\./:]+)', re.MULTILINE)

    for i in range(min(15, pages)):
        text = doc[i].get_text()
        matches = toc_pattern.findall(text)
        if len(matches) >= 3:  # At least 3 TOC entries on one page
            toc_pages.append(i + 1)
            for name, page_num in matches:
                name = name.strip().rstrip('.')
                # Filter out sub-sections
                skip_words = ['ĐỊNH NGHĨA', 'NGUYÊN NHÂN', 'TRIỆU CHỨNG', 'CHẨN ĐOÁN',
                              'ĐIỀU TRỊ', 'PHÒNG BỆNH', 'TIÊN LƯỢNG', 'BIẾN CHỨNG',
                              'TÀI LIỆU', 'MỤC LỤC', 'PHỤ LỤC', 'PHẦN']
                if any(sw in name.upper() for sw in skip_words):
                    continue
                if len(name) > 5:
                    diseases_detected.append({
                        "name": name,
                        "start_page": int(page_num)
                    })

    # If no TOC found, try heading-based detection
    if not diseases_detected:
        for i in range(pages):
            text = doc[i].get_text()
            for match in numbered_pattern.finditer(text):
                name = match.group(1).strip()
                if len(name) > 5 and len(name) < 100:
                    diseases_detected.append({
                        "name": name,
                        "start_page": i + 1
                    })

    # Deduplicate
    seen = set()
    unique_diseases = []
    for d in diseases_detected:
        if d['name'] not in seen:
            seen.add(d['name'])
            unique_diseases.append(d)
    diseases_detected = unique_diseases

    # Classification
    if len(diseases_detected) <= 1:
        classification = "single_disease"
    elif len(diseases_detected) <= 5:
        classification = "few_diseases"
    else:
        classification = "multi_disease"

    # Determine strategy
    if classification == "multi_disease":
        strategy = "multi_disease_ingest"
    else:
        strategy = "universal_ingest"

    doc.close()

    # Determine if vision OCR is needed
    needs_ocr = total_chars < pages * 100  # scanned/empty pages
    needs_vision = has_garbled_font  # Vietnamese font garbling

    analysis = {
        "pdf_path": str(pdf_path),
        "pdf_name": Path(pdf_path).name,
        "pages": pages,
        "total_chars": total_chars,
        "avg_chars_per_page": total_chars // max(pages, 1),
        "language": "vi" if has_vietnamese else "en",
        "needs_ocr": needs_ocr,
        "needs_vision": needs_vision,
        "text_quality": round(avg_quality, 2),
        "has_garbled_font": has_garbled_font,
        "classification": classification,
        "disease_count_estimate": len(diseases_detected),
        "toc_found": len(toc_pages) > 0,
        "toc_pages": toc_pages,
        "diseases_detected": diseases_detected[:100],  # Cap at 100
        "recommended_strategy": strategy,
    }

    with open(run_dir / 'analysis.json', 'w', encoding='utf-8') as f:
        json.dump(analysis, f, ensure_ascii=False, indent=2)

    print(f"  Pages: {pages}")
    print(f"  Chars: {total_chars:,}")
    print(f"  Language: {analysis['language']}")
    print(f"  Text quality: {avg_quality:.2f}" + (" ⚠ GARBLED FONT" if has_garbled_font else ""))
    if needs_vision:
        print(f"  [!] Vision OCR recommended — garbled Vietnamese fonts detected")
    print(f"  Classification: {classification}")
    print(f"  Diseases detected: {len(diseases_detected)}")
    print(f"  TOC found: {analysis['toc_found']} (pages: {toc_pages})")
    print(f"  Strategy: {strategy}")

    return analysis


def phase1_design(analysis: dict, run_dir: Path, max_workers: int) -> dict:
    """Phase 1: Design pipeline configuration."""
    print(f"\n{'='*60}")
    print(f"  PHASE 1: PIPELINE DESIGN")
    print(f"{'='*60}")

    from neo4j import GraphDatabase
    driver = GraphDatabase.driver(os.getenv('NEO4J_URI', 'bolt://localhost:7688'), auth=(os.getenv('NEO4J_USER', 'neo4j'), os.getenv('NEO4J_PASSWORD', 'password123')))

    # Check for existing diseases
    diseases_to_skip = []
    diseases_to_ingest = []

    with driver.session() as s:
        for d in analysis.get('diseases_detected', []):
            r = s.run(
                "MATCH (c:Chunk {disease_name: $name}) RETURN count(c) AS c",
                name=d['name']
            )
            count = r.single()['c']
            if count > 0:
                diseases_to_skip.append(d['name'])
                print(f"  SKIP: {d['name']} ({count} chunks already exist)")
            else:
                diseases_to_ingest.append(d)

    driver.close()

    # Worker count
    n_diseases = len(diseases_to_ingest)
    if n_diseases <= 1:
        workers = 1
    elif n_diseases <= 10:
        workers = min(5, max_workers)
    elif n_diseases <= 50:
        workers = min(10, max_workers)
    else:
        workers = min(15, max_workers)

    config = {
        "strategy": analysis['recommended_strategy'],
        "pdf_path": analysis['pdf_path'],
        "max_workers": workers,
        "diseases_to_skip": diseases_to_skip,
        "diseases_to_ingest": [d['name'] for d in diseases_to_ingest],
        "disease_count_to_process": len(diseases_to_ingest),
        "disease_count_skipped": len(diseases_to_skip),
        "embedding_model": "text-embedding-ada-002",
        "needs_vision": analysis.get("needs_vision", False),
        "needs_ocr": analysis.get("needs_ocr", False),
        "text_quality": analysis.get("text_quality", 1.0),
    }

    with open(run_dir / 'pipeline_config.json', 'w', encoding='utf-8') as f:
        json.dump(config, f, ensure_ascii=False, indent=2)

    print(f"  Strategy: {config['strategy']}")
    print(f"  To ingest: {len(diseases_to_ingest)} diseases")
    print(f"  To skip: {len(diseases_to_skip)} (already done)")
    print(f"  Workers: {workers}")

    return config


def phase2_ingest(config: dict, run_dir: Path) -> dict:
    """Phase 2: Execute ingestion."""
    print(f"\n{'='*60}")
    print(f"  PHASE 2: INGESTION")
    print(f"{'='*60}")

    pdf_path = config['pdf_path']
    start_time = time.time()

    if config['disease_count_to_process'] == 0:
        print("  Nothing to ingest — all diseases already exist")
        result = {"status": "skipped", "reason": "all_diseases_exist", "total_chunks": 0}
        with open(run_dir / 'ingestion_result.json', 'w', encoding='utf-8') as f:
            json.dump(result, f, ensure_ascii=False, indent=2)
        return result

    # Determine if vision OCR is needed (garbled fonts or scanned pages)
    force_vision = config.get('needs_vision', False) or config.get('needs_ocr', False)
    if force_vision:
        print(f"  [!] Vision OCR enabled for text extraction")

    if config['strategy'] == 'multi_disease_ingest':
        from multi_disease_ingest import MultiDiseaseIngest
        print(f"  Running multi-disease ingest with {config['max_workers']} workers...")
        try:
            results = MultiDiseaseIngest.auto_ingest(
                pdf_path, max_workers=config['max_workers'],
                force_vision=force_vision,
            )
            total_chunks = sum(r.get('chunks', 0) for r in results if isinstance(r, dict))
            errors = [r for r in results if isinstance(r, dict) and r.get('status') == 'error']
            result = {
                "status": "success",
                "diseases_processed": len(results) - len(errors),
                "diseases_failed": len(errors),
                "total_chunks": total_chunks,
                "duration_seconds": int(time.time() - start_time),
                "errors": [str(e) for e in errors[:10]],
                "vision_ocr_used": force_vision,
            }
        except Exception as e:
            result = {"status": "error", "error": str(e), "duration_seconds": int(time.time() - start_time)}

    else:
        from universal_ingest import UniversalIngest
        print(f"  Running single-disease ingest...")
        try:
            ingest_result = UniversalIngest.auto_ingest(pdf_path)
            result = {
                "status": "success",
                "diseases_processed": 1,
                "total_chunks": ingest_result.get('chunks', 0),
                "duration_seconds": int(time.time() - start_time),
                "vision_ocr_used": force_vision,
            }
        except Exception as e:
            result = {"status": "error", "error": str(e), "duration_seconds": int(time.time() - start_time)}

    with open(run_dir / 'ingestion_result.json', 'w', encoding='utf-8') as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    print(f"  Status: {result['status']}")
    print(f"  Chunks created: {result.get('total_chunks', 0)}")
    print(f"  Duration: {result.get('duration_seconds', 0)}s")

    return result


def phase3_test(config: dict, run_dir: Path, test_file: str = None, target_accuracy: float = 0.85) -> dict:
    """Phase 3: Quality testing."""
    print(f"\n{'='*60}")
    print(f"  PHASE 3: QUALITY TESTING")
    print(f"{'='*60}")

    from medical_agent import MedicalAgent
    from openai import AzureOpenAI
    from concurrent.futures import ThreadPoolExecutor, as_completed

    agent = MedicalAgent()
    judge_client = AzureOpenAI(
        azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT").strip(),
        api_key=os.getenv("AZURE_OPENAI_API_KEY").strip(),
        api_version=os.getenv("AZURE_OPENAI_API_VERSION").strip()
    )
    judge_model = os.getenv("MODEL2", "gpt-5-mini").strip()

    # Load or generate test data
    if test_file and os.path.exists(test_file):
        print(f"  Using provided test file: {test_file}")
        with open(test_file, 'r', encoding='utf-8') as f:
            test_data = json.load(f)
    else:
        print("  Auto-generating test questions from ingested data...")
        test_data = _auto_generate_tests(agent, config)

    if not test_data:
        print("  WARNING: No test data available")
        report = {"total_questions": 0, "accuracy_pct": 0, "results": []}
        with open(run_dir / 'test_report.json', 'w', encoding='utf-8') as f:
            json.dump(report, f, ensure_ascii=False, indent=2)
        agent.close()
        return report

    # Load improved system prompt if exists from Phase 4
    system_prompt = """Bạn là chuyên gia Y khoa hàng đầu. Nhiệm vụ: trả lời câu hỏi lâm sàng DỰA TRÊN phác đồ điều trị được cung cấp.

QUY TẮC BẮT BUỘC:
1. Trả lời CHÍNH XÁC và CỤ THỂ dựa trên nội dung phác đồ. Nêu rõ tên thuốc, liều lượng, số đợt, phương pháp.
2. KHÔNG trả lời mơ hồ, chung chung hoặc yêu cầu thêm thông tin khi phác đồ đã nêu rõ.
3. Khi phác đồ nêu chống chỉ định → khẳng định dứt khoát (VD: "Không có chỉ định vì..." thay vì "Cần cân nhắc...").
4. Trích dẫn cụ thể từ phác đồ: tên mục, giai đoạn, phác đồ cụ thể.
5. Nếu có nhiều lựa chọn → liệt kê theo thứ tự ưu tiên của phác đồ.
6. Trả lời bằng tiếng Việt, ngắn gọn, đi thẳng vào trọng tâm."""
    improved_prompt_file = run_dir / 'improved_system_prompt.txt'
    if improved_prompt_file.exists():
        try:
            system_prompt = improved_prompt_file.read_text(encoding='utf-8').strip()
            print(f"  Using improved system prompt ({len(system_prompt)} chars)")
        except Exception:
            pass

    print(f"  Running {len(test_data)} test questions...")

    # Pre-compute: diseases known from ingestion config → force routing
    known_diseases = config.get('diseases_to_ingest', [])
    _diseases_with_chunks = set()  # Cache for chunk count verification

    def process_one(item, idx):
        topic = item.get('topic', item.get('chu_de', ''))
        scenario = item.get('scenario', '')
        question = item.get('question', item.get('cau_hoi', ''))
        ground_truth = item.get('answer', item.get('dap_an', ''))

        # ── FIX 1: Include topic in search query (was lost before) ──
        full_query = f"{topic} {scenario} {question}".strip()

        # ── FIX 2: Disease routing with chunk verification + fallback ──
        disease = agent.resolve_disease_name(topic) if topic else None
        if not disease:
            disease = agent.resolve_disease_name(full_query)

        # Verify the resolved disease actually has chunks
        # If it resolved to an overview/umbrella node with 0 chunks, fall through
        if disease and disease not in _diseases_with_chunks:
            try:
                with agent.driver.session() as s:
                    r = s.run("MATCH (c:Chunk {disease_name: $d}) RETURN count(c) AS cnt", d=disease)
                    cnt = r.single()['cnt']
                    if cnt > 0:
                        _diseases_with_chunks.add(disease)
                    else:
                        print(f"  [{idx+1}] Disease '{disease}' has 0 chunks, falling through to known_diseases")
                        disease = None
            except Exception:
                pass

        if not disease:
            # Fallback strategy: find the disease with most chunks from known_diseases or Neo4j
            # Step 1: Try known_diseases with chunk verification
            if len(known_diseases) == 1:
                disease = known_diseases[0]
            elif known_diseases:
                topic_lower = topic.lower()
                for kd in known_diseases:
                    if kd.lower() in topic_lower or topic_lower in kd.lower():
                        disease = kd
                        break

            # Step 2: Verify chosen disease has chunks, or find best alternative from Neo4j
            if disease:
                try:
                    with agent.driver.session() as s:
                        r = s.run("MATCH (c:Chunk {disease_name: $d}) RETURN count(c) AS cnt", d=disease)
                        if r.single()['cnt'] == 0:
                            disease = None  # fall through to Neo4j lookup
                except Exception:
                    pass

            # Step 3: If still no disease with chunks, query Neo4j for all diseases
            if not disease:
                try:
                    with agent.driver.session() as s:
                        r = s.run("""
                            MATCH (c:Chunk)-[:ABOUT_DISEASE]->(d:Disease)
                            RETURN d.name AS name, count(c) AS cnt
                            ORDER BY cnt DESC LIMIT 10
                        """)
                        all_db_diseases = [(rec['name'], rec['cnt']) for rec in r]
                        if all_db_diseases:
                            # Try to match by keyword overlap with topic/query
                            for db_name, cnt in all_db_diseases:
                                if db_name.lower() in full_query.lower() or full_query.lower() in db_name.lower():
                                    disease = db_name
                                    break
                            if not disease:
                                # Default to disease with most chunks
                                disease = all_db_diseases[0][0]
                            _diseases_with_chunks.add(disease)
                except Exception:
                    if known_diseases:
                        disease = known_diseases[0]

        # ── FIX 3: Search with full context, use scoped when disease known ──
        if disease:
            context = agent.scoped_search(full_query, disease, top_k=8)
            # If scoped search returns too few results, supplement with enhanced
            if len(context) < 3:
                extra = agent.enhanced_search(full_query, top_k=5, disease_name=disease)
                seen_ids = {c.get('block_id') for c in context}
                for e in extra:
                    if e.get('block_id') not in seen_ids:
                        context.append(e)
                        if len(context) >= 8:
                            break
            mode = f"scoped:{disease}"
        else:
            context = agent.enhanced_search(full_query, top_k=8)
            mode = "enhanced"

        # ── FIX 4: Include topic context in the QA prompt ──
        ctx_str = "\n".join(f"[{i+1}] {c['title']}: {c['description'][:500]}" for i, c in enumerate(context))
        clinical_context = f"Chủ đề lâm sàng: {topic}\n" if topic else ""
        try:
            resp = agent.chat_client.chat.completions.create(
                model=agent.model,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": f"{clinical_context}Ngữ cảnh phác đồ:\n{ctx_str}\n\nCâu hỏi: {question}"}
                ],
            )
            ai_answer = resp.choices[0].message.content
        except Exception as e:
            ai_answer = f"ERROR: {e}"

        # Judge
        try:
            judge_resp = judge_client.chat.completions.create(
                model=judge_model,
                messages=[{"role": "user", "content": f"""
Chấm điểm Y khoa. Trả về JSON {{"score": 0-1.0, "reason": "..."}}
Câu hỏi: {question}
Đáp án chuẩn: {ground_truth}
AI trả lời: {ai_answer}
"""}],
                response_format={"type": "json_object"}
            )
            judge = json.loads(judge_resp.choices[0].message.content)
            score = float(judge.get('score', 0))
            reason = judge.get('reason', '')
        except Exception as e:
            score, reason = 0.0, f"Judge error: {e}"

        print(f"  [{idx+1}/{len(test_data)}] {score} | {mode} | {question[:50]}...")
        return {
            "id": item.get('id', idx+1),
            "topic": topic,
            "question": question,
            "ground_truth": ground_truth,
            "ai_answer": ai_answer,
            "score": score,
            "reason": reason,
            "disease_detected": disease,
            "search_mode": mode,
            "n_chunks": len(context),
        }

    results = []
    test_workers = min(len(test_data), 20)  # Parallelize ALL test questions
    with ThreadPoolExecutor(max_workers=test_workers) as executor:
        futures = {executor.submit(process_one, item, i): i for i, item in enumerate(test_data)}
        for future in as_completed(futures):
            try:
                results.append(future.result())
            except Exception as e:
                results.append({"score": 0, "error": str(e)})

    results.sort(key=lambda x: x.get('id', 0))
    total_score = sum(r.get('score', 0) for r in results)
    accuracy = (total_score / len(results)) * 100 if results else 0

    # Failure analysis
    failures = {
        "wrong_routing": [r for r in results if r.get('score', 0) < 0.5 and r.get('disease_detected') is None],
        "low_score": [r for r in results if 0 < r.get('score', 0) < 0.5],
        "zero_score": [r for r in results if r.get('score', 0) == 0],
    }

    report = {
        "total_questions": len(results),
        "total_score": total_score,
        "accuracy_pct": round(accuracy, 1),
        "target_accuracy_pct": target_accuracy * 100,
        "passed": accuracy >= target_accuracy * 100,
        "results": results,
        "failure_analysis": {k: len(v) for k, v in failures.items()},
    }

    with open(run_dir / 'test_report.json', 'w', encoding='utf-8') as f:
        json.dump(report, f, ensure_ascii=False, indent=2)

    agent.close()

    print(f"\n  Accuracy: {accuracy:.1f}% (target: {target_accuracy*100}%)")
    print(f"  {'PASSED' if report['passed'] else 'FAILED — optimization needed'}")

    return report


def _auto_generate_tests(agent, config) -> list:
    """Generate test questions from ingested disease data."""
    from neo4j import GraphDatabase
    driver = GraphDatabase.driver(os.getenv('NEO4J_URI', 'bolt://localhost:7688'), auth=(os.getenv('NEO4J_USER', 'neo4j'), os.getenv('NEO4J_PASSWORD', 'password123')))

    test_data = []
    diseases_to_test = config.get('diseases_to_ingest', [])[:10]  # Max 10 diseases

    with driver.session() as s:
        for disease_name in diseases_to_test:
            # Get sample chunk content
            r = s.run("""
                MATCH (c:Chunk {disease_name: $name})
                RETURN c.title AS title, c.content AS content
                ORDER BY c.page_number LIMIT 3
            """, name=disease_name)
            chunks = [{"title": rec['title'], "content": rec['content'][:300]} for rec in r]

            if not chunks:
                continue

            # Generate question using LLM
            chunk_text = "\n".join(f"{c['title']}: {c['content']}" for c in chunks)
            try:
                resp = agent.chat_client.chat.completions.create(
                    model=agent.model,
                    messages=[{"role": "user", "content": f"""
Dựa trên nội dung y khoa sau về bệnh "{disease_name}", tạo 2 câu hỏi lâm sàng kèm đáp án ngắn gọn.
Trả về JSON: [{{"topic": "...", "scenario": "...", "question": "...", "answer": "..."}}]

Nội dung:
{chunk_text}
"""}],
                    response_format={"type": "json_object"}
                )
                generated = json.loads(resp.choices[0].message.content)
                if isinstance(generated, dict) and 'questions' in generated:
                    generated = generated['questions']
                if isinstance(generated, list):
                    for i, q in enumerate(generated[:2]):
                        q['id'] = len(test_data) + 1
                        q['topic'] = disease_name
                        test_data.append(q)
            except Exception as e:
                print(f"  Failed to generate questions for {disease_name}: {e}")

    driver.close()
    return test_data


def _get_experience_lessons(config: dict) -> str:
    """Query experience memory for lessons from similar diseases (used by Phase 4)."""
    try:
        from experience_memory import ExperienceMemory
        mem = ExperienceMemory()
        diseases = config.get('diseases_to_process', config.get('diseases_to_ingest', []))
        disease_name = diseases[0] if diseases else 'unknown'
        lessons = mem.find_relevant_lessons(disease_name, "General", top_k=3)
        mem.close()
        if lessons:
            parts = []
            for l in lessons:
                parts.append(
                    f"- Disease '{l.get('disease_name', '?')}': "
                    f"{l.get('error_analysis', '')[:200]} "
                    f"Fixed by: {l.get('strategies_applied', '[]')}"
                )
            return "\n".join(parts)
    except Exception:
        pass
    return "(No past experience available)"


def phase4_optimize(config: dict, test_report: dict, run_dir: Path,
                    iteration: int = 1, prev_strategies: list = None) -> dict:
    """Phase 4: LLM-driven self-improvement based on test failure analysis."""
    print(f"\n{'='*60}")
    print(f"  PHASE 4: SELF-IMPROVEMENT (iteration {iteration})")
    print(f"{'='*60}")

    from neo4j import GraphDatabase
    from openai import AzureOpenAI

    driver = GraphDatabase.driver(
        os.getenv('NEO4J_URI', 'bolt://localhost:7688'),
        auth=(os.getenv('NEO4J_USER', 'neo4j'), os.getenv('NEO4J_PASSWORD', 'password123'))
    )
    llm = AzureOpenAI(
        azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT").strip(),
        api_key=os.getenv("AZURE_OPENAI_API_KEY").strip(),
        api_version=os.getenv("AZURE_OPENAI_API_VERSION").strip()
    )
    llm_model = os.getenv("MODEL2", "gpt-5-mini").strip()

    results = test_report.get('results', [])
    accuracy = test_report.get('accuracy_pct', 0)
    prev_strategies = prev_strategies or []
    fixes = []

    # ── Step 1: Gather failure details ──
    failures = [r for r in results if r.get('score', 1) < 0.75]
    successes = [r for r in results if r.get('score', 1) >= 0.75]

    failure_summary = []
    for f in failures:
        failure_summary.append({
            "question": f.get('question', '')[:200],
            "expected": f.get('ground_truth', '')[:200],
            "got": f.get('ai_answer', '')[:200],
            "score": f.get('score', 0),
            "reason": f.get('reason', '')[:200],
            "search_mode": f.get('search_mode', ''),
            "n_chunks": f.get('n_chunks', 0),
            "disease_detected": f.get('disease_detected'),
        })

    # ── Step 2: Get current chunk stats ──
    chunk_stats = {}
    with driver.session() as s:
        for disease in config.get('diseases_to_ingest', []):
            r = s.run("MATCH (c:Chunk {disease_name: $d}) RETURN count(c) AS cnt, avg(size(c.content)) AS avg_len",
                       d=disease)
            rec = r.single()
            chunk_stats[disease] = {"count": rec['cnt'], "avg_content_length": int(rec['avg_len'] or 0)}

    # ── Step 3: LLM analyzes failures and proposes strategies ──
    analysis_prompt = f"""You are a Medical Knowledge Graph RAG optimization agent.

CURRENT STATE:
- Accuracy: {accuracy}% (target: {test_report.get('target_accuracy_pct', 85)}%)
- Total questions: {len(results)}, passed: {len(successes)}, failed: {len(failures)}
- Chunk statistics per disease: {json.dumps(chunk_stats, ensure_ascii=False)}
- PDF: {config.get('pdf_path', 'unknown')}

PREVIOUSLY TRIED STRATEGIES (do NOT repeat): {json.dumps(prev_strategies, ensure_ascii=False)}

FAILED QUESTIONS:
{json.dumps(failure_summary, ensure_ascii=False, indent=1)}

AVAILABLE STRATEGIES (choose 1-3 that best address the failures):
1. "rechunk_smaller" — DESTRUCTIVE: Delete existing chunks and re-ingest with smaller chunk size. Only use ONCE as first strategy, NEVER after enrichments. Useful when chunks are too large.
2. "rechunk_overlap" — DESTRUCTIVE: Re-ingest with overlapping chunks. Only use ONCE, NEVER after enrichments. Useful when answers span chunk boundaries.
3. "add_disease_aliases" — NON-DESTRUCTIVE: Add alternative disease names/aliases for better routing
4. "fix_embeddings" — NON-DESTRUCTIVE: Re-generate embeddings for chunks with missing/poor vectors
5. "enrich_chunks" — NON-DESTRUCTIVE: Use LLM to add summary/keywords metadata to each chunk for better search
6. "improve_system_prompt" — NON-DESTRUCTIVE: Change the QA system prompt to guide LLM to be more specific and protocol-aligned
7. "add_contextual_chunks" — NON-DESTRUCTIVE: Generate synthetic summary chunks that aggregate key facts across sections

CRITICAL RULES:
- Prefer NON-DESTRUCTIVE strategies over destructive ones
- rechunk_smaller and rechunk_overlap count as the SAME strategy — only ONE rechunk allowed total
- After rechunk, always follow with enrich_chunks + fix_embeddings
- If accuracy is already above 50%, DO NOT rechunk — focus on enrich/prompt/contextual strategies

PAST EXPERIENCE FROM SIMILAR DISEASES (if any):
{_get_experience_lessons(config)}

Respond with JSON:
{{
  "analysis": "2-3 sentence root cause analysis of why these questions failed",
  "strategies": [
    {{
      "name": "<strategy_name>",
      "params": {{}},
      "reasoning": "why this will help"
    }}
  ]
}}"""

    try:
        resp = llm.chat.completions.create(
            model=llm_model,
            messages=[{"role": "user", "content": analysis_prompt}],
            response_format={"type": "json_object"}
        )
        plan = json.loads(resp.choices[0].message.content)
        print(f"  LLM Analysis: {plan.get('analysis', 'N/A')}")
    except Exception as e:
        print(f"  LLM analysis failed: {e}")
        plan = {"analysis": "LLM call failed", "strategies": []}

    strategies = plan.get('strategies', [])
    if not strategies:
        print("  No strategies proposed — will try rechunk_smaller as default")
        strategies = [{"name": "rechunk_smaller", "params": {"chunk_size": 400}, "reasoning": "default fallback"}]

    # ── Step 4: Execute strategies — parallel where possible ──
    from concurrent.futures import ThreadPoolExecutor, as_completed

    tried_names = [s.get('name') for s in prev_strategies]
    rechunk_tried = any(n in tried_names for n in ['rechunk_smaller', 'rechunk_overlap'])

    # Strategies that can only run ONCE (repeating adds no value or causes harm)
    once_only = {'rechunk_smaller', 'rechunk_overlap', 'add_disease_aliases', 'fix_embeddings', 'enrich_chunks'}
    # Strategies that CAN repeat (they read current state and improve incrementally)
    repeatable = {'improve_system_prompt', 'add_contextual_chunks'}

    # Separate into: rechunk (must run first, alone) and non-destructive (can parallel)
    rechunk_strats = []
    parallel_strats = []
    for strat in strategies:
        sname = strat.get('name', '')
        if sname in tried_names and sname in once_only:
            print(f"  SKIP: already tried '{sname}' (once-only)")
            continue
        if sname in ('rechunk_smaller', 'rechunk_overlap') and rechunk_tried:
            print(f"  SKIP: rechunk already done")
            continue
        if sname in ('rechunk_smaller', 'rechunk_overlap'):
            rechunk_strats.append(strat)
        else:
            parallel_strats.append(strat)

    # Run rechunk first if needed (sequential, only one)
    for strat in rechunk_strats[:1]:
        sname = strat.get('name', '')
        params = strat.get('params', {})
        print(f"\n  STRATEGY [sequential]: {sname}")
        try:
            if sname == "rechunk_smaller":
                fix = _strategy_rechunk(config, driver, params.get('chunk_size', 400))
            else:
                fix = _strategy_rechunk(config, driver, params.get('chunk_size', 500), overlap=params.get('overlap', 100))
            fixes.append(fix)
            prev_strategies.append(strat)
        except Exception as e:
            print(f"  STRATEGY FAILED: {sname}: {e}")
            fixes.append({"strategy": sname, "status": "error", "error": str(e)})

    # Run non-destructive strategies in PARALLEL
    def run_strategy(strat):
        sname = strat.get('name', '')
        params = strat.get('params', {})
        print(f"\n  STRATEGY [parallel]: {sname}")
        if sname == "add_disease_aliases":
            return sname, _strategy_add_aliases(failures, driver, llm, llm_model)
        elif sname == "fix_embeddings":
            return sname, _strategy_fix_embeddings(config, driver)
        elif sname == "enrich_chunks":
            return sname, _strategy_enrich_chunks(config, driver, llm, llm_model)
        elif sname == "improve_system_prompt":
            return sname, _strategy_improve_prompt(failures, run_dir, llm, llm_model)
        elif sname == "add_contextual_chunks":
            return sname, _strategy_contextual_chunks(config, driver, llm, llm_model)
        else:
            print(f"  UNKNOWN strategy: {sname}")
            return sname, {"status": "unknown"}

    if parallel_strats:
        with ThreadPoolExecutor(max_workers=min(len(parallel_strats), 5)) as ex:
            futures = {ex.submit(run_strategy, s): s for s in parallel_strats}
            for future in as_completed(futures):
                strat = futures[future]
                try:
                    sname, fix = future.result()
                    fixes.append(fix)
                    prev_strategies.append(strat)
                except Exception as e:
                    sname = strat.get('name', '?')
                    print(f"  STRATEGY FAILED: {sname}: {e}")
                    fixes.append({"strategy": sname, "status": "error", "error": str(e)})

    driver.close()

    optimization_log = {
        "iteration": iteration,
        "accuracy_before": accuracy,
        "analysis": plan.get('analysis', ''),
        "fixes_applied": fixes,
        "total_fixes": len([f for f in fixes if f.get('status') != 'error']),
        "strategies_tried": [s.get('name') for s in prev_strategies],
    }

    log_file = run_dir / f'optimization_log_iter{iteration}.json'
    with open(log_file, 'w', encoding='utf-8') as f:
        json.dump(optimization_log, f, ensure_ascii=False, indent=2)
    # Also save as the main optimization_log.json
    with open(run_dir / 'optimization_log.json', 'w', encoding='utf-8') as f:
        json.dump(optimization_log, f, ensure_ascii=False, indent=2)

    print(f"  Applied {optimization_log['total_fixes']} fixes this iteration")
    return optimization_log


# ── Optimization Strategies ──

def _strategy_rechunk(config, driver, chunk_size=400, overlap=0):
    """ADDITIVE re-chunking: keep existing chunks, add smaller overlapping versions.
    This preserves the original data while adding more granular retrieval targets."""
    import fitz
    from openai import AzureOpenAI

    pdf_path = config['pdf_path']
    diseases = config.get('diseases_to_ingest', [])

    embed_client = AzureOpenAI(
        azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT").strip(),
        api_key=os.getenv("AZURE_OPENAI_API_KEY").strip(),
        api_version=os.getenv("AZURE_OPENAI_API_VERSION").strip()
    )
    embed_model = os.getenv("EMBEDDING_MODEL", "text-embedding-ada-002").strip()

    # Extract full text from PDF
    doc = fitz.open(pdf_path)
    full_text = "\n".join(page.get_text() for page in doc)
    doc.close()

    # Split into smaller overlapping chunks
    words = full_text.split()
    chunk_words = chunk_size // 5  # ~5 chars per word
    overlap_words = max(overlap // 5, chunk_words // 4)
    step = max(chunk_words - overlap_words, 1)

    mini_chunks = []
    for i in range(0, len(words), step):
        chunk_text = " ".join(words[i:i + chunk_words])
        if len(chunk_text.strip()) > 50:
            mini_chunks.append(chunk_text)

    print(f"    Created {len(mini_chunks)} mini-chunks (size~{chunk_size}, overlap~{overlap})")

    # Use first disease name for these chunks
    disease_name = diseases[0] if diseases else "unknown"
    # Parallel embedding generation for mini-chunks
    from concurrent.futures import ThreadPoolExecutor

    def embed_mini(args):
        idx, text = args
        chunk_id = f"rechunk_{disease_name}_{idx}"
        try:
            emb_resp = embed_client.embeddings.create(model=embed_model, input=text[:8000])
            return chunk_id, idx, text, emb_resp.data[0].embedding
        except Exception:
            return None

    total_added = 0
    with ThreadPoolExecutor(max_workers=min(len(mini_chunks), 15)) as ex:
        for result in ex.map(embed_mini, enumerate(mini_chunks)):
            if result:
                chunk_id, idx, text, embedding = result
                with driver.session() as s:
                    s.run("""
                        MERGE (c:Chunk {chunk_id: $id})
                        SET c.disease_name = $disease, c.title = $title, c.content = $content,
                            c.embedding = $emb, c.chunk_type = 'rechunk_overlap', c.source_type = 'BYT'
                        WITH c
                        MERGE (d:Disease {name: $disease})
                        MERGE (c)-[:ABOUT_DISEASE]->(d)
                    """, id=chunk_id, disease=disease_name,
                         title=f"Rechunked segment {idx+1}", content=text, emb=embedding)
                    total_added += 1

    print(f"    Added {total_added} overlapping mini-chunks (kept original chunks)")
    return {
        "strategy": "rechunk_additive",
        "status": "success",
        "chunk_size": chunk_size,
        "overlap": overlap,
        "added": total_added,
    }


def _strategy_add_aliases(failures, driver, llm, llm_model):
    """Use LLM to generate disease aliases and add them."""
    added = []
    with driver.session() as s:
        # Get all disease names
        r = s.run("MATCH (d:Disease)<-[:ABOUT_DISEASE]-(:Chunk) RETURN DISTINCT d.name AS name")
        all_diseases = [rec['name'] for rec in r]

    if not all_diseases:
        return {"strategy": "add_disease_aliases", "status": "skipped", "reason": "no diseases found"}

    # Use LLM to generate aliases
    try:
        resp = llm.chat.completions.create(
            model=llm_model,
            messages=[{"role": "user", "content": f"""
For each Vietnamese medical disease name, generate 3-5 aliases (Vietnamese and English).
Return JSON: {{"aliases": {{"disease_name": ["alias1", "alias2", ...]}}}}

Diseases: {json.dumps(all_diseases, ensure_ascii=False)}
"""}],
            response_format={"type": "json_object"}
        )
        alias_map = json.loads(resp.choices[0].message.content).get('aliases', {})
    except Exception as e:
        return {"strategy": "add_disease_aliases", "status": "error", "error": str(e)}

    with driver.session() as s:
        for disease, aliases in alias_map.items():
            if disease in all_diseases and isinstance(aliases, list):
                s.run(
                    "MATCH (d:Disease {name: $name}) SET d.aliases = $aliases",
                    name=disease, aliases=[a.lower() for a in aliases]
                )
                added.append({"disease": disease, "aliases": aliases})
                print(f"    Added aliases for {disease}: {aliases}")

    return {"strategy": "add_disease_aliases", "status": "success", "added": added}


def _strategy_fix_embeddings(config, driver):
    """Re-generate embeddings for chunks with null embeddings — parallelized."""
    from concurrent.futures import ThreadPoolExecutor
    from openai import AzureOpenAI
    embed_client = AzureOpenAI(
        azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT").strip(),
        api_key=os.getenv("AZURE_OPENAI_API_KEY").strip(),
        api_version=os.getenv("AZURE_OPENAI_API_VERSION").strip()
    )
    embed_model = os.getenv("EMBEDDING_MODEL", "text-embedding-ada-002").strip()

    with driver.session() as s:
        r = s.run("MATCH (c:Chunk) WHERE c.embedding IS NULL RETURN c.chunk_id AS id, c.content AS content LIMIT 100")
        chunks_to_fix = [dict(rec) for rec in r]

    if not chunks_to_fix:
        print("    No missing embeddings")
        return {"strategy": "fix_embeddings", "status": "skipped", "fixed": 0}

    def embed_one(chunk):
        try:
            resp = embed_client.embeddings.create(model=embed_model, input=chunk['content'][:8000])
            return chunk['id'], resp.data[0].embedding
        except Exception:
            return None

    fixed = 0
    with ThreadPoolExecutor(max_workers=min(len(chunks_to_fix), 15)) as ex:
        for result in ex.map(embed_one, chunks_to_fix):
            if result:
                cid, emb = result
                with driver.session() as s:
                    s.run("MATCH (c:Chunk {chunk_id: $id}) SET c.embedding = $emb", id=cid, emb=emb)
                fixed += 1

    print(f"    Fixed {fixed} missing embeddings")
    return {"strategy": "fix_embeddings", "status": "success", "fixed": fixed}


def _strategy_enrich_chunks(config, driver, llm, llm_model):
    """Add LLM-generated summary and keywords to each chunk — parallelized."""
    from concurrent.futures import ThreadPoolExecutor, as_completed
    diseases = config.get('diseases_to_ingest', [])
    all_chunks = []

    with driver.session() as s:
        for disease in diseases:
            r = s.run(
                "MATCH (c:Chunk {disease_name: $d}) WHERE c.keywords IS NULL RETURN c.chunk_id AS id, c.title AS title, c.content AS content LIMIT 50",
                d=disease
            )
            all_chunks.extend([dict(rec) for rec in r])

    if not all_chunks:
        return {"strategy": "enrich_chunks", "status": "skipped", "enriched": 0}

    def enrich_one(chunk):
        try:
            resp = llm.chat.completions.create(
                model=llm_model,
                messages=[{"role": "user", "content": f"""Extract medical keywords and a one-line summary from this clinical text.
Return JSON: {{"keywords": ["kw1", "kw2", ...], "summary": "..."}}

Title: {chunk['title']}
Content: {chunk['content'][:1500]}"""}],
                response_format={"type": "json_object"}
            )
            meta = json.loads(resp.choices[0].message.content)
            return chunk['id'], meta.get('keywords', []), meta.get('summary', '')
        except Exception:
            return None

    enriched = 0
    with ThreadPoolExecutor(max_workers=min(len(all_chunks), 15)) as ex:
        for result in ex.map(enrich_one, all_chunks):
            if result:
                cid, kw, summary = result
                with driver.session() as s:
                    s.run("MATCH (c:Chunk {chunk_id: $id}) SET c.keywords = $kw, c.summary = $s",
                          id=cid, kw=kw, s=summary)
                enriched += 1

    print(f"    Enriched {enriched} chunks with keywords/summary")
    return {"strategy": "enrich_chunks", "status": "success", "enriched": enriched}


def _strategy_improve_prompt(failures, run_dir, llm, llm_model):
    """Analyze failures to generate an improved system prompt for QA.
    Reads the CURRENT prompt from file (if exists) so improvements are incremental."""
    failure_examples = []
    for f in failures[:8]:  # More examples for better analysis
        failure_examples.append({
            "q": f.get('question', '')[:200],
            "expected": f.get('ground_truth', '')[:200],
            "got": f.get('ai_answer', '')[:200],
            "judge_reason": f.get('reason', '')[:200],
        })

    # Read the CURRENT system prompt (not hardcoded baseline!)
    prompt_file = run_dir / 'improved_system_prompt.txt'
    current_prompt = "Bạn là chuyên gia Y khoa. Trả lời đầy đủ dựa trên ngữ cảnh phác đồ."
    if prompt_file.exists():
        try:
            current_prompt = prompt_file.read_text(encoding='utf-8').strip()
        except Exception:
            pass

    try:
        resp = llm.chat.completions.create(
            model=llm_model,
            messages=[{"role": "user", "content": f"""
You are optimizing a medical QA system prompt. The system retrieves clinical protocol chunks and answers questions.
Below are examples where the system FAILED with the CURRENT prompt.

CURRENT SYSTEM PROMPT (keep what works, fix what doesn't):
---
{current_prompt}
---

FAILURES (questions the system got wrong):
{json.dumps(failure_examples, ensure_ascii=False, indent=1)}

COMMON FAILURE PATTERNS TO ADDRESS:
- AI says "không có trong phác đồ" when the protocol DOES contain the answer
- AI gives vague/general answers instead of extracting specific protocol content
- AI asks for more information instead of answering from available context
- AI cites external guidelines instead of the provided protocol

Write an IMPROVED Vietnamese system prompt (max 400 words) that:
1. Keeps the effective parts of the current prompt
2. Fixes the failure patterns above
3. Instructs the LLM to ALWAYS try to answer from context, even partially
4. When context mentions related concepts, the LLM should connect them to answer
5. NEVER respond with "không có trong phác đồ" unless the context is truly empty

Return JSON: {{"improved_prompt": "..."}}
"""}],
            response_format={"type": "json_object"}
        )
        result = json.loads(resp.choices[0].message.content)
        new_prompt = result.get('improved_prompt', '')

        if not new_prompt:
            return {"strategy": "improve_system_prompt", "status": "skipped", "reason": "empty prompt"}

        # Save the improved prompt
        with open(prompt_file, 'w', encoding='utf-8') as f:
            f.write(new_prompt)

        print(f"    Generated improved system prompt ({len(new_prompt)} chars)")
        return {"strategy": "improve_system_prompt", "status": "success", "prompt_length": len(new_prompt)}
    except Exception as e:
        return {"strategy": "improve_system_prompt", "status": "error", "error": str(e)}


def _strategy_contextual_chunks(config, driver, llm, llm_model):
    """Generate synthetic summary chunks — parallelized embedding generation."""
    from concurrent.futures import ThreadPoolExecutor
    from openai import AzureOpenAI
    embed_client = AzureOpenAI(
        azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT").strip(),
        api_key=os.getenv("AZURE_OPENAI_API_KEY").strip(),
        api_version=os.getenv("AZURE_OPENAI_API_VERSION").strip()
    )
    embed_model = os.getenv("EMBEDDING_MODEL", "text-embedding-ada-002").strip()

    diseases = config.get('diseases_to_ingest', [])
    created = 0

    for disease in diseases:
        with driver.session() as s:
            r = s.run(
                "MATCH (c:Chunk {disease_name: $d}) RETURN c.title AS title, c.content AS content ORDER BY c.page_number",
                d=disease
            )
            all_chunks = [dict(rec) for rec in r]

        if len(all_chunks) < 2:
            continue

        all_text = "\n\n".join(f"## {c['title']}\n{c['content'][:800]}" for c in all_chunks[:15])

        try:
            resp = llm.chat.completions.create(
                model=llm_model,
                messages=[{"role": "user", "content": f"""
From this clinical protocol about "{disease}", create 5-8 comprehensive summary sections.
Each section should aggregate key clinical decision points that a clinician would need.
Focus on: indications, contraindications, drug combinations, dosages, staging-specific actions.

Return JSON: {{"summaries": [{{"title": "...", "content": "..."}}]}}

Protocol content:
{all_text}
"""}],
                response_format={"type": "json_object"}
            )
            summaries = json.loads(resp.choices[0].message.content).get('summaries', [])

            # Parallel embedding generation — use timestamp to avoid overwriting previous synthetics
            import hashlib
            ts_suffix = hashlib.md5(str(time.time()).encode()).hexdigest()[:6]
            def embed_and_store(args):
                i, summary = args
                chunk_id = f"synthetic_{disease}_{i}_{ts_suffix}"
                text = f"{summary['title']}: {summary['content']}"[:8000]
                emb_resp = embed_client.embeddings.create(model=embed_model, input=text)
                return chunk_id, summary['title'], summary['content'], emb_resp.data[0].embedding

            with ThreadPoolExecutor(max_workers=min(len(summaries), 10)) as ex:
                for chunk_id, title, content, embedding in ex.map(embed_and_store, enumerate(summaries)):
                    with driver.session() as s:
                        s.run("""
                            MERGE (c:Chunk {chunk_id: $id})
                            SET c.disease_name = $disease, c.title = $title, c.content = $content,
                                c.embedding = $emb, c.chunk_type = 'synthetic_summary',
                                c.source_type = 'BYT'
                            WITH c
                            MERGE (d:Disease {name: $disease})
                            MERGE (c)-[:ABOUT_DISEASE]->(d)
                        """, id=chunk_id, disease=disease, title=title,
                             content=content, emb=embedding)
                        created += 1
                        print(f"    Created synthetic chunk: {title[:60]}")

        except Exception as e:
            print(f"    Failed to create summaries for {disease}: {e}")

    print(f"    Created {created} synthetic summary chunks")
    return {"strategy": "add_contextual_chunks", "status": "success", "created": created}


def run_pipeline(pdf_path: str, test_file: str = None, target_accuracy: float = 0.85,
                 max_workers: int = 10, max_optimize_iterations: int = 3):
    """Main pipeline orchestrator."""
    start_time = time.time()

    print(f"\n{'#'*60}")
    print(f"  MEDICAL PIPELINE AGENT — AUTONOMOUS INGESTION")
    print(f"  PDF: {Path(pdf_path).name}")
    print(f"  Target accuracy: {target_accuracy*100}%")
    print(f"{'#'*60}")

    # Verify PDF exists
    if not os.path.exists(pdf_path):
        print(f"\nERROR: PDF not found: {pdf_path}")
        return

    # Create run directory
    run_dir = create_run_dir(pdf_path)
    print(f"\n  Run directory: {run_dir}")

    # Initialize Experience Memory
    memory = None
    experience = None
    experience_advice_summary = None
    decision_gate = None
    decision_gate_events = []
    try:
        from experience_memory import ExperienceMemory
        memory = ExperienceMemory()
        print(f"  [MEMORY] Experience Memory initialized")
    except Exception as e:
        print(f"  [MEMORY] Experience Memory unavailable: {e} (continuing without)")

    if _decision_gate_enabled():
        try:
            from server_support.claude_decision import ClaudeDecisionGateRunner

            decision_gate = ClaudeDecisionGateRunner()
            print("  [DecisionGate] Claude decision gate initialized")
        except Exception as e:
            print(f"  [DecisionGate] Unavailable: {e} (continuing without)")

    # Phase 0: Analyze
    analysis = phase0_analyze(pdf_path, run_dir)

    if analysis.get('needs_ocr'):
        print("\n  [!] PDF needs OCR — scanned/empty pages detected. Vision OCR will be used.")
    if analysis.get('has_garbled_font'):
        print("  [!] Garbled Vietnamese fonts detected (quality={:.2f}). Vision OCR will be used.".format(
            analysis.get('text_quality', 0)))

    # Query Experience Memory before pipeline design
    if memory:
        try:
            diseases = analysis.get('diseases_detected', [])
            disease_hint = diseases[0] if isinstance(diseases, list) and diseases else str(diseases)
            if isinstance(disease_hint, dict):
                disease_hint = disease_hint.get('name', str(disease_hint))
            experience = memory.query_before_run(
                disease_name=disease_hint,
                medical_domain="General",
                classification=analysis.get('classification', 'single_disease'),
                pdf_name=analysis.get('pdf_name', ''),
            )
            # Save experience advice for audit trail
            with open(run_dir / 'experience_advice.json', 'w', encoding='utf-8') as f:
                # Serialize safely (skip non-serializable fields)
                advice_safe = {
                    "has_template": bool(experience.get('best_template')),
                    "similar_runs": len(experience.get('similar_runs', [])),
                    "lessons": len(experience.get('relevant_lessons', [])),
                    "has_prompt": bool(experience.get('best_prompt')),
                    "recommendations": experience.get('recommendations', ''),
                }
                experience_advice_summary = dict(advice_safe)
                json.dump(advice_safe, f, ensure_ascii=False, indent=2)

            # Apply best prompt from experience (if exists and no prompt already present)
            prompt_file = run_dir / 'improved_system_prompt.txt'
            if experience.get('best_prompt') and not prompt_file.exists():
                best_prompt_text = experience['best_prompt'].get('prompt_text', '')
                if best_prompt_text:
                    with open(prompt_file, 'w', encoding='utf-8') as f:
                        f.write(best_prompt_text)
                    print(f"  [MEMORY] Applied best prompt from experience (acc={experience['best_prompt'].get('accuracy_score', '?')}%)")

            if experience.get('recommendations'):
                print(f"\n  [MEMORY] Recommendations from past experience:")
                for line in experience['recommendations'].split('\n')[:5]:
                    if line.strip():
                        print(f"    {line.strip()}")
                print()
        except Exception as e:
            print(f"  [MEMORY] Error querying experience: {e}")

    # Phase 1: Design
    config = phase1_design(analysis, run_dir, max_workers)

    if decision_gate:
        try:
            design_gate_result = decision_gate.decide(
                workflow="medical_ingest_pipeline",
                checkpoint="post_design",
                objective="Quyet dinh co nen bat dau ingestion ngay, tam dung cho human review, hay huy run.",
                context=(
                    "Checkpoint nay xay ra sau khi PDF da duoc phan tich, experience memory da duoc tra cuu, "
                    "va pipeline config da duoc lap nhung chua ghi them du lieu moi vao graph."
                ),
                state={
                    "analysis": {
                        "pdf_name": analysis.get("pdf_name"),
                        "pages": analysis.get("pages"),
                        "classification": analysis.get("classification"),
                        "disease_count_estimate": analysis.get("disease_count_estimate"),
                        "recommended_strategy": analysis.get("recommended_strategy"),
                        "needs_ocr": analysis.get("needs_ocr"),
                    },
                    "config": {
                        "strategy": config.get("strategy"),
                        "disease_count_to_process": config.get("disease_count_to_process"),
                        "disease_count_skipped": config.get("disease_count_skipped"),
                        "max_workers": config.get("max_workers"),
                    },
                    "experience_advice": experience_advice_summary,
                },
                candidate_actions=[
                    "continue_to_ingestion",
                    "pause_for_human_review",
                    "abort_run",
                ],
            )
            saved_gate = _record_decision_gate(run_dir, "post_design", design_gate_result)
            decision_gate_events.append(saved_gate)
            _log_decision_gate("post_design", saved_gate)

            gate_action = saved_gate["decision"].get("recommended_action")
            if gate_action in {"pause_for_human_review", "abort_run"}:
                status = "paused_for_human_review" if gate_action == "pause_for_human_review" else "aborted_by_decision_gate"
                summary = _build_run_summary(
                    run_dir=run_dir,
                    analysis=analysis,
                    config=config,
                    iteration=0,
                    elapsed=int(time.time() - start_time),
                    status=status,
                    decision_gate_events=decision_gate_events,
                )
                _write_json(run_dir / 'run_summary.json', summary)
                print(f"\n  [DecisionGate] Pipeline {status}")
                return summary
        except Exception as e:
            print(f"  [DecisionGate] post_design failed: {e}")

    # Phase 2: Ingest
    ingestion = phase2_ingest(config, run_dir)

    if ingestion.get('status') == 'error':
        print(f"\nERROR: Ingestion failed: {ingestion.get('error')}")
        return

    # Phase 3: Test
    test_report = phase3_test(config, run_dir, test_file, target_accuracy)
    return _complete_after_test(
        run_dir=run_dir,
        analysis=analysis,
        config=config,
        ingestion=ingestion,
        test_report=test_report,
        target_accuracy=target_accuracy,
        max_optimize_iterations=max_optimize_iterations,
        test_file=test_file,
        start_time=start_time,
        decision_gate=decision_gate,
        decision_gate_events=decision_gate_events,
        memory=memory,
    )

    if decision_gate:
        try:
            test_gate_result = decision_gate.decide(
                workflow="medical_ingest_pipeline",
                checkpoint="post_test",
                objective=(
                    "Quyet dinh co nen chap nhan ket qua hien tai, tiep tuc toi self-improvement, "
                    "tam dung cho human review, hay huy run."
                ),
                context="Checkpoint nay xay ra sau quality testing va truoc vong optimization.",
                state={
                    "strategy": config.get("strategy"),
                    "disease_count_to_process": config.get("disease_count_to_process"),
                    "total_chunks": ingestion.get("total_chunks", 0),
                    "accuracy_pct": test_report.get("accuracy_pct", 0),
                    "target_accuracy_pct": test_report.get("target_accuracy_pct", target_accuracy * 100),
                    "passed": test_report.get("passed", False),
                    "total_questions": test_report.get("total_questions", 0),
                    "failure_analysis": test_report.get("failure_analysis", {}),
                },
                candidate_actions=[
                    "accept_current_result",
                    "run_optimization",
                    "pause_for_human_review",
                    "abort_run",
                ],
            )
            saved_gate = _record_decision_gate(run_dir, "post_test", test_gate_result)
            decision_gate_events.append(saved_gate)
            _log_decision_gate("post_test", saved_gate)

            gate_action = saved_gate["decision"].get("recommended_action")
            if gate_action in {"pause_for_human_review", "abort_run"}:
                status = "paused_for_human_review" if gate_action == "pause_for_human_review" else "aborted_by_decision_gate"
                summary = _build_run_summary(
                    run_dir=run_dir,
                    analysis=analysis,
                    config=config,
                    ingestion=ingestion,
                    test_report=test_report,
                    iteration=0,
                    elapsed=int(time.time() - start_time),
                    status=status,
                    decision_gate_events=decision_gate_events,
                )
                _write_json(run_dir / 'run_summary.json', summary)
                print(f"\n  [DecisionGate] Pipeline {status}")
                return summary

            if gate_action == "accept_current_result":
                print("  [DecisionGate] Accepting current test result and skipping optimization loop")
                max_optimize_iterations = 0
        except Exception as e:
            print(f"  [DecisionGate] post_test failed: {e}")

    # Phase 4: Self-improve (loop with rollback)
    iteration = 0
    prev_strategies = []
    best_accuracy = test_report.get('accuracy_pct', 0)
    best_test_report = test_report
    best_prompt = None  # Track the prompt that produced the best result
    consecutive_drops = 0

    # Save initial best prompt if exists
    prompt_file = run_dir / 'improved_system_prompt.txt'
    if prompt_file.exists():
        best_prompt = prompt_file.read_text(encoding='utf-8').strip()

    # Save initial best test report
    with open(run_dir / 'best_test_report.json', 'w', encoding='utf-8') as f:
        json.dump(best_test_report, f, ensure_ascii=False, indent=2)

    while not test_report.get('passed', False) and iteration < max_optimize_iterations:
        iteration += 1
        current_acc = test_report.get('accuracy_pct', 0)
        print(f"\n  --- Optimization iteration {iteration}/{max_optimize_iterations} ---")
        print(f"  Current: {current_acc}%  Best: {best_accuracy}%  Target: {target_accuracy*100}%")

        opt_log = phase4_optimize(config, test_report, run_dir,
                                  iteration=iteration, prev_strategies=prev_strategies)

        # Re-test after applying fixes
        test_report = phase3_test(config, run_dir, test_file, target_accuracy)
        new_acc = test_report.get('accuracy_pct', 0)
        print(f"  After iteration {iteration}: {new_acc}% (best: {best_accuracy}%)")

        if new_acc >= target_accuracy * 100:
            print(f"  TARGET REACHED! {new_acc}% >= {target_accuracy*100}%")
            best_accuracy = new_acc
            best_test_report = test_report
            if prompt_file.exists():
                best_prompt = prompt_file.read_text(encoding='utf-8').strip()
            break

        if new_acc > best_accuracy:
            best_accuracy = new_acc
            best_test_report = test_report
            consecutive_drops = 0
            # Save the prompt that produced this best result
            if prompt_file.exists():
                best_prompt = prompt_file.read_text(encoding='utf-8').strip()
            # Save best test report to file
            with open(run_dir / 'best_test_report.json', 'w', encoding='utf-8') as f:
                json.dump(best_test_report, f, ensure_ascii=False, indent=2)
            print(f"  NEW BEST: {best_accuracy}%")
        else:
            consecutive_drops += 1
            print(f"  NO IMPROVEMENT (drop #{consecutive_drops})")
            # ROLLBACK: Restore the best prompt so next iteration builds on what worked
            if best_prompt and prompt_file.exists():
                with open(prompt_file, 'w', encoding='utf-8') as f:
                    f.write(best_prompt)
                print(f"  ROLLBACK: Restored best prompt ({len(best_prompt)} chars)")
            # Use best report for next analysis so LLM sees best failures, not degraded ones
            test_report = best_test_report

        # Stop if 3 consecutive drops — strategies are exhausted
        if consecutive_drops >= 3:
            print(f"  STOPPING: 3 consecutive drops, reverting to best result ({best_accuracy}%)")
            test_report = best_test_report
            break

    # Ensure final report reflects the best accuracy achieved
    if best_accuracy > test_report.get('accuracy_pct', 0):
        test_report = best_test_report
    # Restore best prompt for final state
    if best_prompt:
        with open(prompt_file, 'w', encoding='utf-8') as f:
            f.write(best_prompt)
    # Save final best test report as test_report.json
    with open(run_dir / 'test_report.json', 'w', encoding='utf-8') as f:
        json.dump(test_report, f, ensure_ascii=False, indent=2)

    # Final Summary
    elapsed = int(time.time() - start_time)
    summary = _build_run_summary(
        run_dir=run_dir,
        analysis=analysis,
        config=config,
        ingestion=ingestion,
        test_report=test_report,
        iteration=iteration,
        elapsed=elapsed,
        status="completed",
        decision_gate_events=decision_gate_events,
    )

    _write_json(run_dir / 'run_summary.json', summary)

    # Save experience to memory
    if memory:
        try:
            # Collect optimization logs
            opt_logs = []
            for i in range(1, iteration + 1):
                log_file = run_dir / f'optimization_log_iter{i}.json'
                if log_file.exists():
                    with open(log_file, 'r', encoding='utf-8') as f:
                        opt_logs.append(json.load(f))

            # Read ingest config (latest for this disease)
            ingest_config_data = None
            ingest_configs_dir = NOTEBOOKLM_DIR / 'config' / 'ingest_configs'
            if ingest_configs_dir.exists():
                configs = sorted(ingest_configs_dir.glob('*.json'), key=lambda p: p.stat().st_mtime, reverse=True)
                if configs:
                    try:
                        with open(configs[0], 'r', encoding='utf-8') as f:
                            ingest_config_data = json.load(f)
                    except:
                        pass

            # Read final system prompt
            final_prompt = None
            if prompt_file.exists():
                final_prompt = prompt_file.read_text(encoding='utf-8').strip()

            memory.save_after_run(
                run_summary=summary,
                config=config,
                test_report=test_report,
                optimization_logs=opt_logs,
                ingest_config=ingest_config_data,
                system_prompt_text=final_prompt,
            )
            print(f"  [MEMORY] Experience saved to Neo4j graph")
        except Exception as e:
            print(f"  [MEMORY] Error saving experience: {e}")
        finally:
            memory.close()

    print(f"\n{'#'*60}")
    print(f"  PIPELINE COMPLETE")
    print(f"{'#'*60}")
    print(f"  PDF:            {summary['pdf_name']}")
    print(f"  Classification: {summary['classification']}")
    print(f"  Diseases:       {summary['diseases_ingested']} ingested, {summary['diseases_skipped']} skipped")
    print(f"  Chunks:         {summary['total_chunks']}")
    print(f"  Test accuracy:  {summary['test_accuracy_pct']}%")
    print(f"  Passed:         {'YES' if summary['test_passed'] else 'NO'}")
    print(f"  Optimizations:  {summary['optimization_iterations']}")
    print(f"  Duration:       {elapsed}s")
    print(f"  Run dir:        {run_dir}")
    print(f"{'#'*60}\n")

    return summary


def resume_pipeline(
    run_dir: str | Path,
    checkpoint: str,
    action: str,
    test_file: str | None = None,
    target_accuracy: float = 0.85,
    max_optimize_iterations: int = 3,
    note: str | None = None,
):
    """Resume a paused run from a decision-gate checkpoint."""
    resume_started_at = time.time()
    run_dir = Path(run_dir)

    if not run_dir.exists():
        raise FileNotFoundError(f"Run directory not found: {run_dir}")

    analysis = _read_json(run_dir / 'analysis.json', {}) or {}
    config = _read_json(run_dir / 'pipeline_config.json', {}) or {}
    prior_summary = _read_json(run_dir / 'run_summary.json', {}) or {}
    decision_gate_events = list(prior_summary.get("decision_gate_events") or [])
    previous_elapsed = int(prior_summary.get("total_duration_seconds") or 0)
    human_decision_events = _record_human_decision(run_dir, checkpoint, action, note)

    print(f"\n{'#'*60}")
    print(f"  RESUME PIPELINE FROM DECISION GATE")
    print(f"  Run dir: {run_dir}")
    print(f"  Checkpoint: {checkpoint}")
    print(f"  Human action: {action}")
    if note:
        print(f"  Note: {note}")
    print(f"{'#'*60}")

    memory = None
    try:
        from experience_memory import ExperienceMemory
        memory = ExperienceMemory()
        print(f"  [MEMORY] Experience Memory initialized for resume")
    except Exception as e:
        print(f"  [MEMORY] Experience Memory unavailable during resume: {e}")

    if checkpoint == "post_design":
        if action not in {"continue_to_ingestion", "abort_run"}:
            raise ValueError(f"Invalid action for {checkpoint}: {action}")
        if action == "abort_run":
            summary = _build_run_summary(
                run_dir=run_dir,
                analysis=analysis,
                config=config,
                iteration=0,
                elapsed=previous_elapsed,
                status="aborted_by_human_review",
                decision_gate_events=decision_gate_events,
                human_decision_events=human_decision_events,
            )
            _write_json(run_dir / 'run_summary.json', summary)
            if memory:
                memory.close()
            return summary

        ingestion = phase2_ingest(config, run_dir)
        if ingestion.get('status') == 'error':
            summary = _build_run_summary(
                run_dir=run_dir,
                analysis=analysis,
                config=config,
                ingestion=ingestion,
                iteration=0,
                elapsed=previous_elapsed + int(time.time() - resume_started_at),
                status="error",
                decision_gate_events=decision_gate_events,
                human_decision_events=human_decision_events,
            )
            _write_json(run_dir / 'run_summary.json', summary)
            if memory:
                memory.close()
            return summary

        decision_gate = None
        if _decision_gate_enabled():
            try:
                from server_support.claude_decision import ClaudeDecisionGateRunner

                decision_gate = ClaudeDecisionGateRunner()
                print("  [DecisionGate] Claude decision gate re-initialized for resume")
            except Exception as e:
                print(f"  [DecisionGate] Resume gate unavailable: {e} (continuing without)")

        test_report = phase3_test(config, run_dir, test_file, target_accuracy)
        return _complete_after_test(
            run_dir=run_dir,
            analysis=analysis,
            config=config,
            ingestion=ingestion,
            test_report=test_report,
            target_accuracy=target_accuracy,
            max_optimize_iterations=max_optimize_iterations,
            test_file=test_file,
            start_time=resume_started_at,
            previous_elapsed=previous_elapsed,
            decision_gate=decision_gate,
            decision_gate_events=decision_gate_events,
            human_decision_events=human_decision_events,
            memory=memory,
            run_post_test_gate=True,
        )

    if checkpoint == "post_test":
        if action not in {"accept_current_result", "run_optimization", "abort_run"}:
            raise ValueError(f"Invalid action for {checkpoint}: {action}")

        ingestion = _read_json(run_dir / 'ingestion_result.json', {}) or {}
        test_report = _read_json(run_dir / 'test_report.json', {}) or {}

        if action == "abort_run":
            summary = _build_run_summary(
                run_dir=run_dir,
                analysis=analysis,
                config=config,
                ingestion=ingestion,
                test_report=test_report,
                iteration=0,
                elapsed=previous_elapsed,
                status="aborted_by_human_review",
                decision_gate_events=decision_gate_events,
                human_decision_events=human_decision_events,
            )
            _write_json(run_dir / 'run_summary.json', summary)
            if memory:
                memory.close()
            return summary

        effective_iterations = 0 if action == "accept_current_result" else max_optimize_iterations
        return _complete_after_test(
            run_dir=run_dir,
            analysis=analysis,
            config=config,
            ingestion=ingestion,
            test_report=test_report,
            target_accuracy=target_accuracy,
            max_optimize_iterations=effective_iterations,
            test_file=test_file,
            start_time=resume_started_at,
            previous_elapsed=previous_elapsed,
            decision_gate=None,
            decision_gate_events=decision_gate_events,
            human_decision_events=human_decision_events,
            memory=memory,
            run_post_test_gate=False,
        )

    if memory:
        memory.close()
    raise ValueError(f"Unsupported checkpoint: {checkpoint}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Medical PDF Pipeline Orchestrator")
    parser.add_argument("pdf_path", help="Path to medical PDF")
    parser.add_argument("--test-file", help="Path to test questions JSON", default=None)
    parser.add_argument("--target-accuracy", type=float, default=0.85, help="Target accuracy (0-1)")
    parser.add_argument("--max-workers", type=int, default=10, help="Max parallel workers")
    parser.add_argument("--max-optimize", type=int, default=3, help="Max optimization iterations")
    args = parser.parse_args()

    run_pipeline(
        pdf_path=args.pdf_path,
        test_file=args.test_file,
        target_accuracy=args.target_accuracy,
        max_workers=args.max_workers,
        max_optimize_iterations=args.max_optimize,
    )
