from __future__ import annotations

import subprocess
import sys
from pathlib import Path


PROJECT_DIR = Path(__file__).parent.parent

BHYT_REFERENCE_SCRIPT = PROJECT_DIR / "05_reference" / "bhyt" / "build_bhyt_reference_pack.py"
PROTOCOL_EXCEL_SCRIPT = PROJECT_DIR / "05_reference" / "phac_do" / "build_protocol_excel_pack.py"
PROTOCOL_TEXT_SCRIPT = PROJECT_DIR / "05_reference" / "phac_do" / "build_protocol_text_pack.py"
PHASE3_SCRIPT = PROJECT_DIR / "03_enrich" / "enrich_icd_correlation.py"
OBSERVATION_SCRIPT = PROJECT_DIR / "05_observations" / "extract_lab_observations.py"
OBSERVATION_INPUT = PROJECT_DIR / "04_reports" / "ket_qua_xet_nghiem.json"
OBSERVATION_QUEUE_SCRIPT = PROJECT_DIR / "05_observations" / "build_observation_onboarding_queue.py"
OBSERVATION_OUTPUT = PROJECT_DIR / "05_observations" / "lab_observations.jsonl"
EXCLUSION_SIGNAL_SCRIPT = PROJECT_DIR / "06_insurance" / "extract_exclusion_claim_signals.py"
EXCLUSION_LINK_SCRIPT = PROJECT_DIR / "06_insurance" / "link_exclusion_note_mentions.py"
EXCLUSION_KNOWLEDGE_SCRIPT = PROJECT_DIR / "06_insurance" / "build_exclusion_knowledge_pack.py"
BENEFIT_KNOWLEDGE_SCRIPT = PROJECT_DIR / "06_insurance" / "build_benefit_contract_knowledge_pack.py"
RULEBOOK_KNOWLEDGE_SCRIPT = PROJECT_DIR / "06_insurance" / "build_rulebook_policy_pack.py"
CONTRACT_SCRIPT = PROJECT_DIR / "06_insurance" / "build_contract_rules.py"
MVP_SCRIPT = PROJECT_DIR / "pipeline" / "adjudication_mvp.py"


def run_step(label: str, cmd: list[str]) -> None:
    print(f"\n=== {label} ===")
    print(" ".join(cmd))
    completed = subprocess.run(cmd, cwd=PROJECT_DIR, check=False)
    if completed.returncode != 0:
        raise SystemExit(f"Step failed ({label}) with exit code {completed.returncode}")


def main() -> None:
    python = sys.executable
    if BHYT_REFERENCE_SCRIPT.exists():
        run_step("Phase 1b: BHYT Reference Pack", [python, str(BHYT_REFERENCE_SCRIPT)])
    if PROTOCOL_EXCEL_SCRIPT.exists():
        run_step("Phase 2b: Protocol Excel Pack", [python, str(PROTOCOL_EXCEL_SCRIPT)])
    if PROTOCOL_TEXT_SCRIPT.exists():
        run_step("Phase 2c: Protocol Text Pack", [python, str(PROTOCOL_TEXT_SCRIPT)])
    run_step("Phase 3: ICD Correlation", [python, str(PHASE3_SCRIPT)])
    if OBSERVATION_SCRIPT.exists() and OBSERVATION_INPUT.exists():
        run_step("Phase 5: Observation Extraction", [python, str(OBSERVATION_SCRIPT)])
    if OBSERVATION_QUEUE_SCRIPT.exists() and OBSERVATION_OUTPUT.exists():
        run_step("Phase 5b: Observation Onboarding Queue", [python, str(OBSERVATION_QUEUE_SCRIPT)])
    if EXCLUSION_SIGNAL_SCRIPT.exists():
        run_step("Phase 6a: Exclusion Claim Signals", [python, str(EXCLUSION_SIGNAL_SCRIPT)])
    if EXCLUSION_LINK_SCRIPT.exists():
        run_step("Phase 6b: Exclusion Mention Linking", [python, str(EXCLUSION_LINK_SCRIPT)])
    if EXCLUSION_KNOWLEDGE_SCRIPT.exists():
        run_step("Phase 6c: Exclusion Knowledge Pack", [python, str(EXCLUSION_KNOWLEDGE_SCRIPT)])
    if BENEFIT_KNOWLEDGE_SCRIPT.exists():
        run_step("Phase 6d: Benefit Contract Knowledge Pack", [python, str(BENEFIT_KNOWLEDGE_SCRIPT)])
    if RULEBOOK_KNOWLEDGE_SCRIPT.exists():
        run_step("Phase 6e: Rulebook Policy Pack", [python, str(RULEBOOK_KNOWLEDGE_SCRIPT)])
    run_step("Contract Rules Build", [python, str(CONTRACT_SCRIPT)])
    run_step("Adjudication MVP Benchmark", [python, str(MVP_SCRIPT), "--benchmark"])
    print("\nAll steps completed.")


if __name__ == "__main__":
    main()
