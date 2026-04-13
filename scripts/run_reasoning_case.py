from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

from src.ai_claim.reasoning_agent import AzureReasoningAgent


def main() -> None:
    parser = argparse.ArgumentParser(description="Run ai_claim Azure reasoning agent on a case packet.")
    parser.add_argument(
        "--case-file",
        default=str(PROJECT_ROOT / "data" / "benchmarks" / "sample_case_meniere.json"),
    )
    args = parser.parse_args()
    case_packet = json.loads(Path(args.case_file).read_text(encoding="utf-8"))
    agent = AzureReasoningAgent.from_settings()
    try:
        result = agent.run_case(case_packet)
    finally:
        agent.toolkit.close()
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
