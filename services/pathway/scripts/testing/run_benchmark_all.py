"""
Benchmark runner — chạy tất cả test files song song 20 workers.
Usage: python scripts/testing/run_benchmark_all.py
"""
import json
import sys
import time
from pathlib import Path

NOTEBOOKLM_DIR = Path(__file__).resolve().parents[2]
SCRIPT_DIR = Path(__file__).resolve().parent
if str(NOTEBOOKLM_DIR) not in sys.path:
    sys.path.insert(0, str(NOTEBOOKLM_DIR))
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from test_runner import ParallelClinicalRunner
from runtime_env import load_notebooklm_env
from server_support.paths import (
    DATATEST_CASES_DIR,
    DATATEST_REPORTS_DIR,
    ensure_datatest_layout,
)

load_notebooklm_env()

TEST_FILES = [
    "data_test_22_01.json",
    "data_test_22_02.json",
    "data_test_23.json",
    "data_test_25.json",
]
MAX_WORKERS = 20

def main():
    ensure_datatest_layout()
    runner = ParallelClinicalRunner(max_workers=MAX_WORKERS)
    overall_start = time.time()
    all_results = {}

    try:
        for tf in TEST_FILES:
            tf_path = DATATEST_CASES_DIR / tf
            if not tf_path.exists():
                print(f"[SKIP] {tf} not found")
                continue

            with open(tf_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            n_questions = len(data)

            out_name = tf.replace(".json", "_report.xlsx")
            out_path = DATATEST_REPORTS_DIR / out_name

            print(f"\n{'='*60}")
            print(f"BENCHMARK: {tf} ({n_questions} questions, {MAX_WORKERS} workers)")
            print(f"{'='*60}")

            t0 = time.time()
            accuracy = runner.run_benchmark(str(tf_path), str(out_path), limit=n_questions)
            elapsed = time.time() - t0

            all_results[tf] = {
                "questions": n_questions,
                "accuracy": accuracy,
                "time_sec": round(elapsed, 1),
                "avg_sec_per_q": round(elapsed / n_questions, 1),
            }
            print(f"  => {tf}: {accuracy:.1f}% in {elapsed:.0f}s ({elapsed/n_questions:.1f}s/q)")

        # Summary
        total_time = time.time() - overall_start
        print(f"\n{'='*60}")
        print(f"TONG KET BENCHMARK ({len(all_results)} files, {MAX_WORKERS} workers)")
        print(f"{'='*60}")
        total_q = 0
        total_score = 0
        for tf, r in all_results.items():
            print(f"  {tf}: {r['accuracy']:.1f}% ({r['questions']}q, {r['time_sec']}s)")
            total_q += r['questions']
            total_score += r['accuracy'] * r['questions'] / 100

        if total_q > 0:
            overall_acc = (total_score / total_q) * 100
            print(f"\n  OVERALL: {overall_acc:.1f}% ({total_q} questions)")
        print(f"  TOTAL TIME: {total_time:.0f}s")

        # Save summary
        summary_path = DATATEST_REPORTS_DIR / "benchmark_summary_byt_utv.json"
        with open(summary_path, 'w', encoding='utf-8') as f:
            json.dump(all_results, f, ensure_ascii=False, indent=2)
        print(f"\n  Summary saved: {summary_path}")

    finally:
        runner.agent.close()

if __name__ == "__main__":
    main()
