from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parents[2]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from server_support.pathway_data_architecture import PathwayDataArchitectureStore


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Validate and inspect the Pathway data architecture contract."
    )
    parser.add_argument(
        "--ensure-layout",
        action="store_true",
        help="Create any configured directories that are marked create_if_missing before validating.",
    )
    parser.add_argument(
        "--summary-only",
        action="store_true",
        help="Print a compact summary instead of the full bootstrap payload.",
    )
    parser.add_argument(
        "--fail-on-missing",
        action="store_true",
        help="Exit with code 1 if required surfaces are missing.",
    )
    parser.add_argument(
        "--fail-on-warning",
        action="store_true",
        help="Exit with code 1 if any warning is present.",
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()
    store = PathwayDataArchitectureStore()

    layout_result = None
    if args.ensure_layout:
        layout_result = store.ensure_layout()

    payload = store.bootstrap()
    output = payload
    if args.summary_only:
        output = {
            "schema_version": payload.get("schema_version"),
            "generated_at": payload.get("generated_at"),
            "summary": payload.get("summary") or {},
            "warnings": payload.get("warnings") or [],
            "domains": [
                {
                    "id": item.get("id"),
                    "surface_count": item.get("surface_count", 0),
                    "existing_surface_count": item.get("existing_surface_count", 0),
                    "missing_required_count": item.get("missing_required_count", 0),
                }
                for item in (payload.get("domains") or [])
            ],
        }
    if layout_result is not None:
        output = {
            "layout": layout_result,
            "result": output,
        }

    print(json.dumps(output, ensure_ascii=False, indent=2))

    missing_required = int((payload.get("summary") or {}).get("missing_required_count") or 0)
    warning_count = int((payload.get("summary") or {}).get("warning_count") or 0)
    if args.fail_on_missing and missing_required > 0:
        return 1
    if args.fail_on_warning and warning_count > 0:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
