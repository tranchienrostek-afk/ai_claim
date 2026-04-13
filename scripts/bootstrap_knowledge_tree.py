from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.ai_claim.knowledge_layout import KnowledgeLayout
from src.ai_claim.settings import SETTINGS


def main() -> None:
    parser = argparse.ArgumentParser(description="Bootstrap ai_claim knowledge tree.")
    parser.add_argument("--disease-key", help="Optional disease workspace key")
    parser.add_argument("--disease-name", help="Optional disease name")
    args = parser.parse_args()

    layout = KnowledgeLayout.from_file(
        SETTINGS.project_root,
        SETTINGS.configs_dir / "knowledge_roots.json",
    )
    result = {
        "roots": layout.ensure_roots(),
    }
    if args.disease_key and args.disease_name:
        result["disease_workspace"] = layout.create_disease_workspace(
            args.disease_key,
            args.disease_name,
        )
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
