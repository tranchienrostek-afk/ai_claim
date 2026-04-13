from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.ai_claim.domain_policy import DomainPolicy
from src.ai_claim.settings import SETTINGS


def main() -> None:
    parser = argparse.ArgumentParser(description="Emit a restricted agent_claude launch spec.")
    parser.add_argument("--prompt-file", required=True)
    parser.add_argument("--mcp-config-file", required=True)
    parser.add_argument("--model", default="sonnet")
    args = parser.parse_args()

    policy = DomainPolicy.from_file(SETTINGS.configs_dir / "domain_policy.json")
    spec = policy.build_agent_claude_launch_spec(
        Path(args.prompt_file),
        Path(args.mcp_config_file),
        model=args.model,
        project_root=SETTINGS.project_root,
    )
    print(json.dumps(spec, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
