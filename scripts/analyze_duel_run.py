from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.ai_claim.benchmark_analyzer import DuelAnalyzer
from src.ai_claim.settings import SETTINGS


def main() -> None:
    parser = argparse.ArgumentParser(description="Analyze a Pathway vs agent_claude duel run.")
    parser.add_argument("--run-dir", required=True, help="Path to duel run directory")
    parser.add_argument(
        "--output-dir",
        default=str(SETTINGS.data_dir / "benchmarks"),
        help="Where to write summary artifacts",
    )
    args = parser.parse_args()

    run_dir = Path(args.run_dir)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    analyzer = DuelAnalyzer(run_dir)
    summary = analyzer.build_reasoning_gap()
    report = analyzer.build_markdown_report()

    safe_name = run_dir.name
    summary_path = output_dir / f"{safe_name}_summary.json"
    report_path = output_dir / f"{safe_name}_report.md"
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    report_path.write_text(report, encoding="utf-8")

    print(json.dumps({
        "run_dir": str(run_dir),
        "summary_file": str(summary_path),
        "report_file": str(report_path),
        "agent_cost_usd": summary["participants"]["agent_claude"]["total_cost_usd"],
        "agent_neo4j_calls": summary["participants"]["agent_claude"]["neo4j_call_count"],
    }, ensure_ascii=False))


if __name__ == "__main__":
    main()
