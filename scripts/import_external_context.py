from __future__ import annotations

import shutil
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
WORKSPACE_ROOT = PROJECT_ROOT.parent


def _copy_file(src: Path, dst: Path) -> None:
    if not src.exists():
        return
    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, dst)


def _copy_tree(src: Path, dst: Path) -> None:
    if not src.exists():
        return
    if dst.exists():
        shutil.rmtree(dst)
    shutil.copytree(src, dst)


def main() -> None:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")

    handoff_dir = PROJECT_ROOT / "docs" / "handoffs"
    benchmark_dir = PROJECT_ROOT / "data" / "benchmarks"
    imported_runs_dir = PROJECT_ROOT / "data" / "imported_runs"

    report_sources = {
        WORKSPACE_ROOT / "report_9router.md": handoff_dir / "report_9router.md",
        WORKSPACE_ROOT / "report_pathway_agent_claude.md": handoff_dir / "report_pathway_agent_claude.md",
        WORKSPACE_ROOT / "report_test_9router_hc_insurance.md": handoff_dir / "report_test_9router_hc_insurance.md",
    }

    benchmark_sources = {
        WORKSPACE_ROOT
        / "pathway"
        / "notebooklm"
        / "workspaces"
        / "claims_insights"
        / "07_architecture"
        / "09_pathway_vs_agent_claude_case_meniere.json": benchmark_dir / "duel_case_meniere.json",
        WORKSPACE_ROOT
        / "pathway"
        / "notebooklm"
        / "workspaces"
        / "claims_insights"
        / "07_architecture"
        / "10_benchmark_case_pneumonia.json": benchmark_dir / "duel_case_pneumonia.json",
    }

    run_sources = {
        WORKSPACE_ROOT
        / "pathway"
        / "notebooklm"
        / "data"
        / "duel_runs"
        / "pathway_vs_agent_claude"
        / "20260413_094909_duel_meniere_001": imported_runs_dir / "20260413_094909_duel_meniere_001",
        WORKSPACE_ROOT
        / "pathway"
        / "notebooklm"
        / "data"
        / "duel_runs"
        / "pathway_vs_agent_claude"
        / "20260413_095219_duel_pneumonia_002": imported_runs_dir / "20260413_095219_duel_pneumonia_002",
    }

    copied_files: list[str] = []
    copied_dirs: list[str] = []

    for src, dst in report_sources.items():
        _copy_file(src, dst)
        if dst.exists():
            copied_files.append(str(dst.relative_to(PROJECT_ROOT)))

    for src, dst in benchmark_sources.items():
        _copy_file(src, dst)
        if dst.exists():
            copied_files.append(str(dst.relative_to(PROJECT_ROOT)))

    for src, dst in run_sources.items():
        _copy_tree(src, dst)
        if dst.exists():
            copied_dirs.append(str(dst.relative_to(PROJECT_ROOT)))

    print("Copied files:")
    for item in copied_files:
        print(f"- {item}")
    print("Copied directories:")
    for item in copied_dirs:
        print(f"- {item}")


if __name__ == "__main__":
    main()
