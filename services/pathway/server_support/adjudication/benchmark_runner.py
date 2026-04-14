"""Benchmark runner for the multiagent adjudication system.

Loads the gold benchmark (2,091 labeled service lines) and runs each through
the AdjudicatorAgent to measure accuracy, precision, recall, and F1.

Usage:
    python -m server_support.adjudication.benchmark_runner
    python -m server_support.adjudication.benchmark_runner --limit 100
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any

# Resolve paths
_THIS_DIR = Path(__file__).resolve().parent
_NOTEBOOKLM_DIR = _THIS_DIR.parent.parent
_GOLD_DIR = _NOTEBOOKLM_DIR / "workspaces" / "claims_insights" / "08_manual_gold_benchmark"
_GOLD_INPUTS = _GOLD_DIR / "data" / "gold_service_lines_input.jsonl"
_GOLD_LABELS = _GOLD_DIR / "labels" / "gold_service_lines_labels.jsonl"
_OUTPUT_DIR = _GOLD_DIR / "outputs"


def load_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with path.open(encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows


def _decision_to_payment_status(decision: str) -> str:
    """Map multiagent final_decision -> PAYMENT / REJECT for benchmark comparison."""
    if decision == "approve":
        return "PAYMENT"
    if decision in ("deny", "partial_pay"):
        return "REJECT"
    # "review" -> treat as REJECT for benchmark (conservative)
    return "REJECT"


def run_benchmark(limit: int | None = None) -> dict[str, Any]:
    """Run the gold benchmark and return a report dict."""
    from .adjudicator_agent import AdjudicatorAgent
    from .models import MultiAgentAdjudicateRequest, ServiceLineInput

    print(f"Loading gold benchmark from {_GOLD_INPUTS} ...")
    inputs = load_jsonl(_GOLD_INPUTS)
    labels = load_jsonl(_GOLD_LABELS)

    # Build label lookup by benchmark_id
    label_by_id: dict[str, dict[str, Any]] = {}
    for label in labels:
        bid = label.get("benchmark_id", "")
        if bid:
            label_by_id[bid] = label

    # Join inputs with labels
    joined: list[dict[str, Any]] = []
    for inp in inputs:
        bid = inp.get("benchmark_id", "")
        label = label_by_id.get(bid)
        if label:
            joined.append({**inp, **label})

    if limit:
        joined = joined[:limit]

    print(f"Benchmark dataset: {len(joined)} service lines (limit={limit})")

    # Initialize adjudicator
    print("Initializing AdjudicatorAgent ...")
    t0_init = time.time()
    adjudicator = AdjudicatorAgent()
    init_ms = round((time.time() - t0_init) * 1000, 1)
    print(f"Initialization took {init_ms:.0f}ms")

    # Run adjudication
    print("Running adjudication ...")
    t0_run = time.time()

    tp = fp = tn = fn = 0  # PAYMENT=positive, REJECT=negative
    decision_counts = Counter()
    resolution_rule_counts = Counter()
    scored_rows: list[dict[str, Any]] = []
    fp_rows: list[dict[str, Any]] = []
    fn_rows: list[dict[str, Any]] = []

    for i, row in enumerate(joined):
        line = ServiceLineInput(
            service_name_raw=row.get("service_name_raw", ""),
            diagnosis_text=row.get("diagnosis_text", ""),
            primary_icd=row.get("icd_hint", ""),
            contract_id=row.get("contract_name", ""),
            insurer=row.get("insurer", ""),
            cost_vnd=float(row.get("amount_vnd", 0) or 0),
        )

        request = MultiAgentAdjudicateRequest(
            claim_id=row.get("claim_id", ""),
            service_lines=[line],
            contract_id=row.get("contract_name", ""),
            insurer=row.get("insurer", ""),
        )

        response = adjudicator.adjudicate_claim(request)
        result = response.results[0] if response.results else None

        predicted_decision = result.final_decision if result else "review"
        predicted_status = _decision_to_payment_status(predicted_decision)
        gold_status = row.get("gold_payment_status", "REJECT")
        correct = predicted_status == gold_status

        decision_counts[predicted_decision] += 1
        if result:
            resolution_rule_counts[result.resolution_rule] += 1

        # Confusion matrix (PAYMENT = positive)
        if gold_status == "PAYMENT" and predicted_status == "PAYMENT":
            tp += 1
        elif gold_status == "REJECT" and predicted_status == "PAYMENT":
            fp += 1
            fp_rows.append({
                "benchmark_id": row.get("benchmark_id"),
                "service_name_raw": row.get("service_name_raw"),
                "diagnosis_text": row.get("diagnosis_text"),
                "predicted": predicted_decision,
                "gold": gold_status,
                "resolution_rule": result.resolution_rule if result else "",
                "confidence": result.confidence if result else 0,
            })
        elif gold_status == "REJECT" and predicted_status == "REJECT":
            tn += 1
        elif gold_status == "PAYMENT" and predicted_status == "REJECT":
            fn += 1
            fn_rows.append({
                "benchmark_id": row.get("benchmark_id"),
                "service_name_raw": row.get("service_name_raw"),
                "diagnosis_text": row.get("diagnosis_text"),
                "predicted": predicted_decision,
                "gold": gold_status,
                "resolution_rule": result.resolution_rule if result else "",
                "confidence": result.confidence if result else 0,
            })

        scored_rows.append({
            "benchmark_id": row.get("benchmark_id"),
            "service_name_raw": row.get("service_name_raw"),
            "gold_status": gold_status,
            "predicted_decision": predicted_decision,
            "predicted_status": predicted_status,
            "correct": correct,
            "resolution_rule": result.resolution_rule if result else "",
            "confidence": result.confidence if result else 0,
        })

        if (i + 1) % 200 == 0:
            elapsed = time.time() - t0_run
            acc = (tp + tn) / (i + 1)
            print(f"  [{i+1}/{len(joined)}] accuracy={acc:.2%} elapsed={elapsed:.1f}s")

    run_ms = round((time.time() - t0_run) * 1000, 1)
    total = tp + fp + tn + fn

    # Metrics
    accuracy = (tp + tn) / total if total else 0
    precision = tp / (tp + fp) if (tp + fp) else 0
    recall = tp / (tp + fn) if (tp + fn) else 0
    f1 = 2 * precision * recall / (precision + recall) if (precision + recall) else 0
    specificity = tn / (tn + fp) if (tn + fp) else 0
    balanced_accuracy = (recall + specificity) / 2

    report = {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "dataset": str(_GOLD_INPUTS),
        "total_rows": total,
        "limit": limit,
        "init_ms": init_ms,
        "run_ms": run_ms,
        "confusion_matrix": {"TP": tp, "FP": fp, "TN": tn, "FN": fn},
        "metrics": {
            "accuracy": round(accuracy, 4),
            "precision_payment": round(precision, 4),
            "recall_payment": round(recall, 4),
            "f1_payment": round(f1, 4),
            "specificity_reject": round(specificity, 4),
            "balanced_accuracy": round(balanced_accuracy, 4),
        },
        "decision_distribution": dict(decision_counts),
        "resolution_rule_distribution": dict(resolution_rule_counts.most_common(20)),
        "false_positive_count": len(fp_rows),
        "false_negative_count": len(fn_rows),
    }

    return report, scored_rows, fp_rows, fn_rows


def main() -> None:
    parser = argparse.ArgumentParser(description="Multiagent Gold Benchmark Runner")
    parser.add_argument("--limit", type=int, default=None, help="Limit number of rows to process")
    parser.add_argument("--save", action="store_true", help="Save results to output files")
    args = parser.parse_args()

    report, scored_rows, fp_rows, fn_rows = run_benchmark(limit=args.limit)

    # Print report
    print("\n" + "=" * 70)
    print("MULTIAGENT GOLD BENCHMARK REPORT")
    print("=" * 70)
    cm = report["confusion_matrix"]
    metrics = report["metrics"]
    print(f"Total rows:       {report['total_rows']}")
    print(f"Confusion matrix: TP={cm['TP']}  FP={cm['FP']}  TN={cm['TN']}  FN={cm['FN']}")
    print(f"Accuracy:         {metrics['accuracy']:.2%}")
    print(f"Precision (PAY):  {metrics['precision_payment']:.2%}")
    print(f"Recall (PAY):     {metrics['recall_payment']:.2%}")
    print(f"F1 (PAY):         {metrics['f1_payment']:.2%}")
    print(f"Specificity (REJ):{metrics['specificity_reject']:.2%}")
    print(f"Balanced Acc:     {metrics['balanced_accuracy']:.2%}")
    print(f"\nDecision distribution: {report['decision_distribution']}")
    print(f"Top resolution rules:")
    for rule, count in list(report["resolution_rule_distribution"].items())[:10]:
        print(f"  {rule}: {count}")
    print(f"\nFalse Positives: {report['false_positive_count']}")
    print(f"False Negatives: {report['false_negative_count']}")
    print(f"Run time: {report['run_ms']:.0f}ms (init: {report['init_ms']:.0f}ms)")

    if args.save:
        _OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        report_path = _OUTPUT_DIR / "multiagent_benchmark_report.json"
        report_path.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
        print(f"\nReport saved to {report_path}")

        scored_path = _OUTPUT_DIR / "multiagent_scored_rows.jsonl"
        with scored_path.open("w", encoding="utf-8") as f:
            for row in scored_rows:
                f.write(json.dumps(row, ensure_ascii=False) + "\n")
        print(f"Scored rows saved to {scored_path}")


if __name__ == "__main__":
    main()
