from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import unicodedata
import urllib.error
import urllib.request
import uuid
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple


ROOT_DIR = Path(__file__).resolve().parents[4]
NOTEBOOKLM_DIR = ROOT_DIR / "pathway" / "notebooklm"
DEFAULT_CASE_PATH = (
    NOTEBOOKLM_DIR
    / "workspaces"
    / "claims_insights"
    / "07_architecture"
    / "09_pathway_vs_agent_claude_case_meniere.json"
)
DEFAULT_AGENT_ENV_PATH = ROOT_DIR / "agent_claude" / ".env"
DEFAULT_CLAUDE_SCRIPT = Path(r"C:\Users\Admin\AppData\Roaming\npm\claude.ps1")
DEFAULT_OUTPUT_ROOT = NOTEBOOKLM_DIR / "data" / "duel_runs" / "pathway_vs_agent_claude"
DEFAULT_PATHWAY_BASE_URL = "http://localhost:9600"
DEFAULT_MODEL = os.getenv("DUEL_AGENT_CLAUDE_MODEL", "sonnet")
DEFAULT_TIMEOUT_SECONDS = int(os.getenv("DUEL_AGENT_CLAUDE_TIMEOUT_SECONDS", "900"))


ALLOWED_MCP_TOOLS = [
    "mcp__pathway-neo4j__graph_operating_search",
    "mcp__pathway-neo4j__query_ci_disease_snapshot",
    "mcp__pathway-neo4j__trace_service_evidence",
    "mcp__pathway-neo4j__query_contracts_by_insurer",
    "mcp__pathway-neo4j__query_benefits_for_contract",
    "mcp__pathway-neo4j__query_service_exclusions",
]


AGENT_OUTPUT_SCHEMA = {
    "type": "object",
    "required": [
        "participant",
        "case_id",
        "active_diseases",
        "differentials",
        "line_results",
        "claim_level_decision",
        "claim_level_reasoning",
    ],
    "properties": {
        "participant": {"type": "string"},
        "case_id": {"type": "string"},
        "active_diseases": {
            "type": "array",
            "items": {"type": "string"},
        },
        "differentials": {
            "type": "array",
            "items": {"type": "string"},
        },
        "line_results": {
            "type": "array",
            "items": {
                "type": "object",
                "required": [
                    "line_no",
                    "service_name_raw",
                    "medical_decision",
                    "insurance_decision",
                    "final_decision",
                    "reasoning",
                    "evidence",
                ],
                "properties": {
                    "line_no": {"type": "integer"},
                    "service_name_raw": {"type": "string"},
                    "medical_decision": {"type": "string"},
                    "insurance_decision": {"type": "string"},
                    "final_decision": {"type": "string"},
                    "service_role": {"type": "string"},
                    "reasoning": {"type": "string"},
                    "evidence": {
                        "type": "array",
                        "items": {"type": "string"},
                    },
                },
            },
        },
        "claim_level_decision": {"type": "string"},
        "claim_level_reasoning": {"type": "string"},
    },
}


@dataclass
class CommandResult:
    stdout: bytes
    stderr: bytes
    returncode: int


def strip_accents(text: str) -> str:
    normalized = unicodedata.normalize("NFKD", text or "")
    return "".join(ch for ch in normalized if not unicodedata.combining(ch)).lower()


def json_dump(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def text_dump(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def read_json(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def load_env_file(path: Path) -> Dict[str, str]:
    env: Dict[str, str] = {}
    if not path.exists():
        return env
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        env[key.strip()] = value.strip().strip('"').strip("'")
    return env


def http_post_json(url: str, payload: Dict[str, Any], timeout_seconds: int = 120) -> Dict[str, Any]:
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    request = urllib.request.Request(
        url,
        data=body,
        headers={"Content-Type": "application/json; charset=utf-8"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
            response_body = response.read().decode("utf-8")
            return json.loads(response_body)
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="ignore")
        raise RuntimeError(f"HTTP {exc.code} for {url}: {detail}") from exc


def derive_claim_level_decision(line_results: Iterable[Dict[str, Any]]) -> str:
    finals = [str(item.get("final_decision") or "").lower() for item in line_results]
    if "partial_pay" in finals:
        return "partial_pay"
    if "review" in finals:
        return "review"
    if finals and all(decision == "deny" for decision in finals):
        return "deny"
    if finals and all(decision == "approve" for decision in finals):
        return "approve"
    return "review"


def pathway_medical_request(case: Dict[str, Any]) -> Dict[str, Any]:
    insurance = case["insurance_context"]
    clinical = case["clinical_context"]
    lines = []
    for line in case["service_lines"]:
        lines.append(
            {
                "service_name_raw": line["service_name_raw"],
                "contract_id": insurance["contract_id"],
                "insurer": insurance["insurer"],
                "symptoms": clinical["symptoms"],
                "medical_history": clinical["medical_history"],
                "admission_reason": clinical["admission_reason"],
                "cost_vnd": line["cost_vnd"],
            }
        )
    return {
        "case_id": case["case_id"],
        "known_diseases": [],
        "symptoms": clinical["symptoms"],
        "medical_history": clinical["medical_history"],
        "admission_reason": clinical["admission_reason"],
        "service_lines": lines,
    }


def pathway_adjudicate_request(case: Dict[str, Any]) -> Dict[str, Any]:
    insurance = case["insurance_context"]
    clinical = case["clinical_context"]
    lines = []
    for line in case["service_lines"]:
        lines.append(
            {
                "service_name_raw": line["service_name_raw"],
                "contract_id": insurance["contract_id"],
                "insurer": insurance["insurer"],
                "symptoms": clinical["symptoms"],
                "medical_history": clinical["medical_history"],
                "admission_reason": clinical["admission_reason"],
                "cost_vnd": line["cost_vnd"],
            }
        )
    return {
        "claim_id": case["case_id"],
        "contract_id": insurance["contract_id"],
        "insurer": insurance["insurer"],
        "known_diseases": [],
        "symptoms": clinical["symptoms"],
        "medical_history": clinical["medical_history"],
        "admission_reason": clinical["admission_reason"],
        "service_lines": lines,
    }


def normalize_pathway(
    case: Dict[str, Any],
    medical_response: Dict[str, Any],
    adjudicate_response: Dict[str, Any],
) -> Dict[str, Any]:
    medical_results = {
        strip_accents(item.get("service_name_raw", "")): item
        for item in medical_response.get("results", [])
    }
    normalized_lines: List[Dict[str, Any]] = []
    for index, adjudicated in enumerate(adjudicate_response.get("results", []), start=1):
        service_name = adjudicated.get("service_name") or case["service_lines"][index - 1]["service_name_raw"]
        key = strip_accents(service_name)
        medical_line = medical_results.get(key, {})
        medical_decision = str(
            medical_line.get("medical_decision")
            or adjudicated.get("medical_decision")
            or "uncertain"
        ).lower()
        final_decision = str(adjudicated.get("final_decision") or "review").lower()
        if medical_decision == "deny":
            insurance_decision = "deny"
        else:
            insurance_decision = final_decision
        normalized_lines.append(
            {
                "line_no": index,
                "service_name_raw": service_name,
                "recognized_service_code": adjudicated.get("recognized_service_code", ""),
                "recognized_canonical_name": adjudicated.get("recognized_canonical_name", ""),
                "medical_decision": medical_decision,
                "insurance_decision": insurance_decision,
                "final_decision": final_decision,
                "medical_reasoning_vi": medical_line.get("medical_reasoning_vi", ""),
                "final_reasoning_vi": adjudicated.get("reasoning_vi", ""),
                "medical_confidence": medical_line.get("medical_confidence"),
                "final_confidence": adjudicated.get("confidence"),
            }
        )

    active_diseases = [
        item.get("name") or item.get("disease_name") or item.get("label") or ""
        for item in medical_response.get("active_diseases", [])
        if (item.get("name") or item.get("disease_name") or item.get("label"))
    ]
    top_hypotheses = [
        item.get("name") or item.get("disease_name") or item.get("label") or ""
        for item in medical_response.get("top_hypotheses", [])
        if (item.get("name") or item.get("disease_name") or item.get("label"))
    ]
    differential_candidates = []
    normalized_active = {strip_accents(item) for item in active_diseases}
    for hypothesis in top_hypotheses:
        if strip_accents(hypothesis) not in normalized_active:
            differential_candidates.append(hypothesis)

    return {
        "participant": "pathway",
        "case_id": case["case_id"],
        "active_diseases": active_diseases,
        "differentials": differential_candidates[:5],
        "line_results": normalized_lines,
        "claim_level_decision": derive_claim_level_decision(normalized_lines),
        "claim_level_reasoning": adjudicate_response.get("summary_vi", ""),
    }


def build_agent_prompt(case: Dict[str, Any]) -> str:
    case_packet = {
        "case_id": case["case_id"],
        "patient": case["patient"],
        "clinical_context": case["clinical_context"],
        "insurance_context": case["insurance_context"],
        "service_lines": case["service_lines"],
    }
    packet_text = json.dumps(case_packet, ensure_ascii=False, separators=(",", ":"))
    return (
        "Ban dang thi doc lap voi Pathway. Khong co quyen doc file workspace, khong co quyen xem output cua Pathway. "
        "Chi duoc dung cac MCP tool Neo4j da cap.\n\n"
        "Chi tra ve JSON hop le, khong markdown, khong chu thich ngoai JSON.\n"
        "Bat buoc co cac key:\n"
        "- participant\n"
        "- case_id\n"
        "- active_diseases\n"
        "- differentials\n"
        "- line_results: moi phan tu gom line_no, service_name_raw, medical_decision, insurance_decision, final_decision, service_role, reasoning, evidence\n"
        "- claim_level_decision\n"
        "- claim_level_reasoning\n\n"
        "Quy tac:\n"
        "- Chi dung bang chung tu MCP Neo4j tools.\n"
        "- Neu dich vu hop ly ve y khoa nhung vuot han muc chi tra thi insurance_decision va final_decision phai la partial_pay.\n"
        "- Neu khong du bang chung, dung uncertain hoac review thay vi doan.\n\n"
        f"Case packet JSON:{packet_text}"
    )


def ensure_claude_script(path: Path) -> Path:
    if path.exists():
        return path
    fallback_candidates = [
        Path(r"C:\Users\Admin\.local\bin\claude.exe"),
        Path(r"C:\Users\Admin\AppData\Roaming\npm\claude.cmd"),
        Path(r"C:\Users\Admin\AppData\Roaming\npm\claude"),
    ]
    for candidate in fallback_candidates:
        if candidate.exists():
            return candidate
    raise FileNotFoundError(f"Khong tim thay Claude CLI script tai {path}")


def create_mcp_config(run_dir: Path) -> Path:
    launcher_path = NOTEBOOKLM_DIR / "scripts" / "testing" / "launch_pathway_neo4j_mcp.py"
    payload = {
        "mcpServers": {
            "pathway-neo4j": {
                "command": sys.executable,
                "args": [str(launcher_path)],
            }
        }
    }
    config_path = run_dir / "agent_claude_mcp_config.json"
    json_dump(config_path, payload)
    return config_path


def run_powershell_command(
    command: List[str],
    *,
    env: Dict[str, str],
    cwd: Path,
    input_bytes: Optional[bytes] = None,
    timeout_seconds: int,
) -> CommandResult:
    process = subprocess.run(
        command,
        input=input_bytes,
        capture_output=True,
        cwd=str(cwd),
        env=env,
        timeout=timeout_seconds,
    )
    return CommandResult(
        stdout=process.stdout or b"",
        stderr=process.stderr or b"",
        returncode=process.returncode,
    )


def decode_output(raw: bytes) -> str:
    return raw.decode("utf-8", errors="ignore")


def powershell_quote(text: str) -> str:
    return text.replace("'", "''")


def disease_name_items(items: Iterable[Any]) -> List[str]:
    names: List[str] = []
    for item in items:
        if isinstance(item, str):
            names.append(item)
            continue
        if isinstance(item, dict):
            for key in ("disease_name", "name", "label", "icd10"):
                value = item.get(key)
                if isinstance(value, str) and value.strip():
                    names.append(value)
                    break
    return names


def extract_json_object(raw_text: str) -> Dict[str, Any]:
    cleaned = raw_text.strip()
    if cleaned.startswith("```"):
        first_newline = cleaned.find("\n")
        cleaned = cleaned[first_newline + 1 :] if first_newline >= 0 else cleaned
        if cleaned.endswith("```"):
            cleaned = cleaned[:-3].strip()
    try:
        return json.loads(cleaned)
    except json.JSONDecodeError:
        start = cleaned.find("{")
        end = cleaned.rfind("}")
        if start >= 0 and end > start:
            return json.loads(cleaned[start : end + 1])
        raise


def parse_stream_json(stream_text: str) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    events: List[Dict[str, Any]] = []
    final_payload: Dict[str, Any] = {}
    for raw_line in stream_text.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            continue
        events.append(event)
        if event.get("type") == "result" and event.get("subtype") == "success":
            result_text = event.get("result", "")
            if isinstance(result_text, str) and result_text.strip():
                final_payload = extract_json_object(result_text)
    return events, final_payload


def extract_model_from_events(events: List[Dict[str, Any]]) -> Optional[str]:
    for event in events:
        if event.get("type") == "system" and event.get("subtype") == "init":
            model = event.get("model") or event.get("cliVersion")
            if model:
                return str(model)
        if event.get("type") == "assistant" and event.get("model"):
            return str(event["model"])
    return None


def run_agent_claude(
    case: Dict[str, Any],
    run_dir: Path,
    *,
    agent_env_path: Path,
    model: str,
    timeout_seconds: int,
) -> Dict[str, Any]:
    prompt = build_agent_prompt(case)
    prompt_path = run_dir / "agent_claude_prompt.txt"
    text_dump(prompt_path, prompt)

    env = os.environ.copy()
    env.update(load_env_file(agent_env_path))
    env["PYTHONIOENCODING"] = "utf-8"
    env.setdefault("CLAUDE_CODE_DISABLE_AUTOUPDATER", "1")
    env.setdefault("NO_COLOR", "1")

    claude_script = ensure_claude_script(DEFAULT_CLAUDE_SCRIPT)
    mcp_config_path = create_mcp_config(run_dir)
    tool_entries = ", ".join(f"'{powershell_quote(tool)}'" for tool in ALLOWED_MCP_TOOLS)
    ps_script = (
        "$OutputEncoding = [Console]::OutputEncoding = [System.Text.UTF8Encoding]::new($false); "
        f"$tools = @({tool_entries}); "
        "$args = @("
        "'-p','--bare','--model',"
        f"'{powershell_quote(model)}',"
        "'--permission-mode','bypassPermissions','--mcp-config',"
        f"'{powershell_quote(str(mcp_config_path))}',"
        "'--strict-mcp-config','--allowedTools'"
        ") + $tools + @('--output-format','stream-json','--verbose'); "
        f"$prompt = Get-Content -LiteralPath '{powershell_quote(str(prompt_path))}' -Raw; "
        f"& '{powershell_quote(str(claude_script))}' @args $prompt"
    )
    command = [
        "powershell",
        "-NoProfile",
        "-ExecutionPolicy",
        "Bypass",
        "-Command",
        ps_script,
    ]

    result = run_powershell_command(
        command,
        env=env,
        cwd=ROOT_DIR,
        input_bytes=None,
        timeout_seconds=timeout_seconds,
    )
    stdout_text = decode_output(result.stdout)
    stderr_text = decode_output(result.stderr)
    text_dump(run_dir / "agent_claude_stream.jsonl", stdout_text)
    text_dump(run_dir / "agent_claude_stderr.log", stderr_text)
    json_dump(
        run_dir / "agent_claude_command.json",
        {
            "command": command,
            "powershell_script": ps_script,
            "model": model,
            "allowed_tools": ALLOWED_MCP_TOOLS,
            "mcp_config": str(mcp_config_path),
            "cwd": str(ROOT_DIR),
            "agent_env_path": str(agent_env_path),
            "returncode": result.returncode,
        },
    )
    if result.returncode != 0:
        raise RuntimeError(f"agent_claude exited with code {result.returncode}: {stderr_text.strip() or stdout_text.strip()}")

    events, final_payload = parse_stream_json(stdout_text)
    if not final_payload:
        raise RuntimeError("Khong parse duoc JSON ket qua cua agent_claude tu stream-json.")

    actual_model = extract_model_from_events(events)
    if actual_model:
        final_payload["_actual_model"] = actual_model
    self_reported = final_payload.get("participant", "unknown")
    if actual_model and actual_model != self_reported:
        final_payload["_model_mismatch"] = f"self_reported={self_reported}, actual={actual_model}"
        final_payload["participant"] = f"agent_claude ({actual_model})"

    json_dump(run_dir / "agent_claude_events.json", events)
    json_dump(run_dir / "agent_claude_result.json", final_payload)
    return final_payload


def run_pathway(case: Dict[str, Any], run_dir: Path, base_url: str) -> Dict[str, Any]:
    medical_request = pathway_medical_request(case)
    adjudicate_request = pathway_adjudicate_request(case)
    json_dump(run_dir / "pathway_medical_request.json", medical_request)
    json_dump(run_dir / "pathway_adjudicate_request.json", adjudicate_request)

    medical_response = http_post_json(f"{base_url}/api/medical/reason-services", medical_request)
    adjudicate_response = http_post_json(f"{base_url}/api/adjudicate/v2", adjudicate_request)
    json_dump(run_dir / "pathway_medical_response.json", medical_response)
    json_dump(run_dir / "pathway_adjudicate_response.json", adjudicate_response)

    normalized = normalize_pathway(case, medical_response, adjudicate_response)
    json_dump(run_dir / "pathway_normalized_result.json", normalized)
    return normalized


DECISION_EQUIVALENCE: Dict[str, str] = {
    # --- canonical labels ---
    "partial_pay": "partial_pay",
    "partial_review": "partial_pay",
    "partial": "partial_pay",
    "approve": "approve",
    "payment": "approve",
    "accept": "approve",
    "deny": "deny",
    "reject": "deny",
    "review": "review",
    "uncertain": "uncertain",
    # --- agent_claude medical-decision labels ---
    "medically_necessary": "approve",
    "medically_reasonable": "approve",
    "medically_indicated": "approve",
    "indicated": "approve",
    "not_indicated": "deny",
    "not_medically_indicated": "deny",
    "not_medically_necessary": "deny",
    "contraindicated": "deny",
    "conditional_indication": "review",
    "conditionally_indicated": "review",
    # --- agent_claude insurance-decision labels ---
    "pay": "approve",
    "pay_full": "approve",
    "covered": "approve",
    "not_covered": "deny",
}


ICD_DISEASE_ALIASES: Dict[str, List[str]] = {
    "H81.0": ["meniere", "benh meniere", "bệnh meniere", "meniere disease"],
    "H81.1": ["bppv", "chong mat do vi tri", "benign paroxysmal positional vertigo"],
    "H81.2": ["viem than kinh doi", "vestibular neuritis"],
    "H83.0": ["viem maze", "labyrinthitis"],
    "J18": ["viem phoi", "pneumonia", "viem phoi cong dong", "community acquired pneumonia"],
    "J18.9": ["viem phoi", "pneumonia", "viem phoi cong dong", "community acquired pneumonia", "cap"],
    "J20": ["viem phe quan", "bronchitis", "viem phe quan cap"],
    "J20.9": ["viem phe quan", "bronchitis", "viem phe quan cap"],
    "K35": ["viem ruot thua", "appendicitis"],
    "E11": ["dai thao duong type 2", "diabetes mellitus type 2"],
    "A15": ["lao phoi", "tuberculosis", "tb"],
}


def normalize_decision(raw: str) -> str:
    key = strip_accents(raw).strip().lower().replace(" ", "_").replace("-", "_")
    return DECISION_EQUIVALENCE.get(key, key)


def soft_disease_match(expected_name: str, submitted_items: Iterable[Any]) -> Tuple[bool, str]:
    expected_norm = strip_accents(expected_name)
    submitted_names = disease_name_items(submitted_items)

    for name in submitted_names:
        name_norm = strip_accents(name)
        if expected_norm == name_norm:
            return True, "exact"
        if expected_norm in name_norm or name_norm in expected_norm:
            return True, "substring"

    for item in submitted_items:
        icd = ""
        if isinstance(item, dict):
            icd = strip_accents(item.get("icd_code", "") or item.get("icd10", "") or "")
        if icd:
            aliases = ICD_DISEASE_ALIASES.get(icd.upper(), [])
            for alias in aliases:
                if strip_accents(alias) in expected_norm or expected_norm in strip_accents(alias):
                    return True, f"icd:{icd}"

    for name in submitted_names:
        name_tokens = set(strip_accents(name).split())
        expected_tokens = set(expected_norm.split())
        if expected_tokens and name_tokens:
            overlap = len(expected_tokens & name_tokens)
            if overlap >= max(1, len(expected_tokens) * 0.6):
                return True, "token_overlap"

    return False, "none"


def soft_decision_match(observed_raw: str, expected_raw: str) -> Tuple[bool, str]:
    obs = normalize_decision(observed_raw)
    exp = normalize_decision(expected_raw)
    if obs == exp:
        return True, "exact" if strip_accents(observed_raw) == strip_accents(expected_raw) else "equivalent"
    return False, "mismatch"


def score_submission(case: Dict[str, Any], submission: Dict[str, Any]) -> Dict[str, Any]:
    gold = case["gold"]
    gold_lines = {item["line_no"]: item for item in gold["service_expectations"]}
    submitted_lines = {int(item["line_no"]): item for item in submission.get("line_results", [])}

    disease_hit = False
    disease_match_type = "none"
    for expected in gold.get("expected_active_diseases", []):
        hit, match_type = soft_disease_match(expected, submission.get("active_diseases", []))
        if hit:
            disease_hit = True
            disease_match_type = match_type
            break

    differential_hits = 0
    differential_total = len(gold.get("expected_differentials", []))
    for expected_diff in gold.get("expected_differentials", []):
        hit, _ = soft_disease_match(expected_diff, submission.get("differentials", []))
        if hit:
            differential_hits += 1

    line_scores = []
    medical_matches = 0
    insurance_matches = 0
    final_matches = 0
    line_all_matches = 0
    for line_no, expected in sorted(gold_lines.items()):
        observed = submitted_lines.get(line_no, {})
        med_match, med_type = soft_decision_match(
            observed.get("medical_decision", ""), expected["medical_decision"]
        )
        ins_match, ins_type = soft_decision_match(
            observed.get("insurance_decision", ""), expected["insurance_decision"]
        )
        fin_match, fin_type = soft_decision_match(
            observed.get("final_decision", ""), expected["final_decision"]
        )
        if med_match:
            medical_matches += 1
        if ins_match:
            insurance_matches += 1
        if fin_match:
            final_matches += 1
        all_match = med_match and ins_match and fin_match
        if all_match:
            line_all_matches += 1
        line_scores.append(
            {
                "line_no": line_no,
                "medical_match": med_match,
                "medical_match_type": med_type,
                "insurance_match": ins_match,
                "insurance_match_type": ins_type,
                "final_match": fin_match,
                "final_match_type": fin_type,
                "all_match": all_match,
                "expected": expected,
                "observed": observed,
            }
        )

    claim_match, claim_match_type = soft_decision_match(
        submission.get("claim_level_decision", ""),
        gold.get("expected_claim_level_decision", ""),
    )

    line_total = len(gold_lines)
    disease_score = 1.0 if disease_hit else 0.0
    medical_score = round(medical_matches / line_total, 4) if line_total else 0.0
    insurance_score = round(insurance_matches / line_total, 4) if line_total else 0.0
    final_score = round(final_matches / line_total, 4) if line_total else 0.0
    claim_score = 1.0 if claim_match else 0.0

    total_checks = line_total + 2
    total_hits = line_all_matches + (1 if disease_hit else 0) + (1 if claim_match else 0)
    accuracy = round(total_hits / total_checks, 4) if total_checks else 0.0

    weighted = round(
        0.15 * disease_score
        + 0.35 * medical_score
        + 0.30 * final_score
        + 0.20 * claim_score,
        4,
    )

    return {
        "participant": submission.get("participant", "unknown"),
        "case_id": case["case_id"],
        "disease_match": disease_hit,
        "disease_match_type": disease_match_type,
        "differential_hits": differential_hits,
        "differential_total": differential_total,
        "claim_level_match": claim_match,
        "claim_level_match_type": claim_match_type,
        "line_all_matches": line_all_matches,
        "line_total": line_total,
        "sub_scores": {
            "disease_inference": disease_score,
            "line_medical": medical_score,
            "line_insurance": insurance_score,
            "line_final": final_score,
            "claim_level": claim_score,
        },
        "total_hits": total_hits,
        "total_checks": total_checks,
        "accuracy": accuracy,
        "weighted_score": weighted,
        "line_scores": line_scores,
    }


def render_sub_scores(score: Dict[str, Any]) -> str:
    sub = score.get("sub_scores", {})
    return (
        f"  - disease_inference: {sub.get('disease_inference', 0)} (match_type={score.get('disease_match_type', 'none')})\n"
        f"  - line_medical: {sub.get('line_medical', 0)}\n"
        f"  - line_insurance: {sub.get('line_insurance', 0)}\n"
        f"  - line_final: {sub.get('line_final', 0)}\n"
        f"  - claim_level: {sub.get('claim_level', 0)} (match_type={score.get('claim_level_match_type', 'none')})\n"
        f"  - differentials: {score.get('differential_hits', 0)}/{score.get('differential_total', 0)}"
    )


def build_report(
    case: Dict[str, Any],
    pathway_result: Dict[str, Any],
    agent_result: Dict[str, Any],
    pathway_score: Dict[str, Any],
    agent_score: Dict[str, Any],
    run_dir: Path,
) -> str:
    def render_lines(lines: List[Dict[str, Any]]) -> str:
        rendered = []
        for item in lines:
            rendered.append(
                f"- L{item['line_no']}: {item.get('service_name_raw','')} | medical={item.get('medical_decision','')} | "
                f"insurance={item.get('insurance_decision','')} | final={item.get('final_decision','')}"
            )
        return "\n".join(rendered)

    def render_line_detail(line_scores: List[Dict[str, Any]]) -> str:
        rendered = []
        for ls in line_scores:
            markers = []
            for field in ("medical", "insurance", "final"):
                matched = ls.get(f"{field}_match", False)
                mtype = ls.get(f"{field}_match_type", "")
                symbol = "OK" if matched else "MISS"
                markers.append(f"{field}={symbol}" + (f"({mtype})" if mtype and matched else ""))
            rendered.append(f"  - L{ls['line_no']}: {' | '.join(markers)}")
        return "\n".join(rendered)

    winner = "hoa"
    pw = pathway_score.get("weighted_score", pathway_score.get("accuracy", 0))
    aw = agent_score.get("weighted_score", agent_score.get("accuracy", 0))
    if pw > aw:
        winner = "pathway"
    elif aw > pw:
        winner = "agent_claude"

    return (
        f"# Duel Report: {case['case_id']}\n\n"
        f"- Run dir: `{run_dir}`\n"
        f"- Winner: **{winner}**\n"
        f"- Pathway: accuracy={pathway_score['accuracy']} weighted={pathway_score.get('weighted_score', 'N/A')} "
        f"({pathway_score['total_hits']}/{pathway_score['total_checks']})\n"
        f"- agent_claude: accuracy={agent_score['accuracy']} weighted={agent_score.get('weighted_score', 'N/A')} "
        f"({agent_score['total_hits']}/{agent_score['total_checks']})\n\n"
        "## Gold\n\n"
        f"- Expected active disease: {', '.join(case['gold'].get('expected_active_diseases', []))}\n"
        f"- Expected differentials: {', '.join(case['gold'].get('expected_differentials', []))}\n"
        f"- Expected claim decision: {case['gold'].get('expected_claim_level_decision', '')}\n\n"
        "## Pathway\n\n"
        f"- Active diseases: {', '.join(disease_name_items(pathway_result.get('active_diseases', []))) or '(rong)'}\n"
        f"- Claim level decision: {pathway_result.get('claim_level_decision', '')}\n"
        f"{render_lines(pathway_result.get('line_results', []))}\n\n"
        "### Pathway Sub-Scores\n\n"
        f"{render_sub_scores(pathway_score)}\n\n"
        "### Pathway Line Detail\n\n"
        f"{render_line_detail(pathway_score.get('line_scores', []))}\n\n"
        "## agent_claude\n\n"
        f"- Active diseases: {', '.join(disease_name_items(agent_result.get('active_diseases', []))) or '(rong)'}\n"
        f"- Claim level decision: {agent_result.get('claim_level_decision', '')}\n"
        f"{render_lines(agent_result.get('line_results', []))}\n\n"
        "### agent_claude Sub-Scores\n\n"
        f"{render_sub_scores(agent_score)}\n\n"
        "### agent_claude Line Detail\n\n"
        f"{render_line_detail(agent_score.get('line_scores', []))}\n\n"
        "## File outputs\n\n"
        "- `pathway_medical_request.json`\n"
        "- `pathway_medical_response.json`\n"
        "- `pathway_adjudicate_request.json`\n"
        "- `pathway_adjudicate_response.json`\n"
        "- `agent_claude_prompt.txt`\n"
        "- `agent_claude_stream.jsonl`\n"
        "- `agent_claude_stderr.log`\n"
        "- `agent_claude_result.json`\n"
        "- `duel_score.json`\n"
    )


def make_run_dir(case_id: str, output_root: Path) -> Path:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    run_dir = output_root / f"{timestamp}_{case_id}"
    run_dir.mkdir(parents=True, exist_ok=False)
    return run_dir


def run_single_duel(
    case: Dict[str, Any],
    case_path: Path,
    output_root: Path,
    pathway_base_url: str,
    agent_env_path: Path,
    model: str,
    timeout_seconds: int,
) -> Dict[str, Any]:
    run_dir = make_run_dir(case["case_id"], output_root)
    json_dump(run_dir / "case_input.json", case)
    json_dump(
        run_dir / "runner_meta.json",
        {
            "case_path": str(case_path),
            "pathway_base_url": pathway_base_url,
            "agent_env_path": str(agent_env_path),
            "model": model,
            "timeout_seconds": timeout_seconds,
            "run_id": str(uuid.uuid4()),
            "sealed_rules": [
                "Pathway chi nhan request qua API va khong doc output agent_claude",
                "agent_claude chi nhan case packet + Neo4j MCP, khong doc file workspace va khong goi Pathway API",
            ],
        },
    )

    pathway_result = run_pathway(case, run_dir, pathway_base_url.rstrip("/"))
    agent_result = run_agent_claude(
        case,
        run_dir,
        agent_env_path=agent_env_path,
        model=model,
        timeout_seconds=timeout_seconds,
    )
    pathway_score = score_submission(case, pathway_result)
    agent_score = score_submission(case, agent_result)

    duel_score = {
        "case_id": case["case_id"],
        "pathway": pathway_score,
        "agent_claude": agent_score,
    }
    json_dump(run_dir / "duel_score.json", duel_score)
    report = build_report(case, pathway_result, agent_result, pathway_score, agent_score, run_dir)
    text_dump(run_dir / "duel_report.md", report)
    return {"run_dir": str(run_dir), "score": duel_score}


def build_leaderboard(results: List[Dict[str, Any]], output_path: Path) -> str:
    pathway_totals: Dict[str, float] = {
        "disease_inference": 0, "line_medical": 0, "line_insurance": 0,
        "line_final": 0, "claim_level": 0, "weighted": 0, "accuracy": 0,
    }
    agent_totals: Dict[str, float] = dict(pathway_totals)
    n = len(results)
    for r in results:
        ps = r["score"]["pathway"]
        acs = r["score"]["agent_claude"]
        for key in ("disease_inference", "line_medical", "line_insurance", "line_final", "claim_level"):
            pathway_totals[key] += ps.get("sub_scores", {}).get(key, 0)
            agent_totals[key] += acs.get("sub_scores", {}).get(key, 0)
        pathway_totals["weighted"] += ps.get("weighted_score", 0)
        pathway_totals["accuracy"] += ps.get("accuracy", 0)
        agent_totals["weighted"] += acs.get("weighted_score", 0)
        agent_totals["accuracy"] += acs.get("accuracy", 0)

    def avg(totals: Dict[str, float]) -> Dict[str, float]:
        return {k: round(v / n, 4) for k, v in totals.items()} if n else totals

    pa = avg(pathway_totals)
    aa = avg(agent_totals)

    lines = [
        "# Benchmark Leaderboard",
        f"\nCases: {n}\n",
        "| Metric | Pathway | agent_claude |",
        "|--------|---------|-------------|",
    ]
    for key in ("accuracy", "weighted", "disease_inference", "line_medical", "line_insurance", "line_final", "claim_level"):
        lines.append(f"| {key} | {pa[key]} | {aa[key]} |")

    lines.append("\n## Per-Case Results\n")
    for r in results:
        case_id = r["score"]["case_id"]
        pw = r["score"]["pathway"].get("weighted_score", 0)
        aw = r["score"]["agent_claude"].get("weighted_score", 0)
        winner_icon = "P" if pw > aw else ("A" if aw > pw else "=")
        lines.append(f"- `{case_id}`: Pathway={pw} vs agent_claude={aw} [{winner_icon}]")

    report = "\n".join(lines)
    text_dump(output_path, report)
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description="Run a sealed duel: Pathway vs agent_claude on the same Neo4j case.")
    parser.add_argument("--case", default=str(DEFAULT_CASE_PATH), help="Single case JSON or directory of case JSONs")
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--pathway-base-url", default=DEFAULT_PATHWAY_BASE_URL)
    parser.add_argument("--agent-env", default=str(DEFAULT_AGENT_ENV_PATH))
    parser.add_argument("--model", default=DEFAULT_MODEL)
    parser.add_argument("--timeout-seconds", type=int, default=DEFAULT_TIMEOUT_SECONDS)
    parser.add_argument("--rescore", help="Rescore an existing run directory (skip running Pathway/agent_claude)")
    args = parser.parse_args()

    output_root = Path(args.output_root)
    agent_env_path = Path(args.agent_env)

    if args.rescore:
        rescore_dir = Path(args.rescore)
        case = read_json(rescore_dir / "case_input.json")
        for participant in ("pathway", "agent_claude"):
            result_path = rescore_dir / f"{participant}_normalized_result.json"
            if not result_path.exists():
                result_path = rescore_dir / f"{participant}_result.json"
            if result_path.exists():
                submission = read_json(result_path)
                new_score = score_submission(case, submission)
                print(f"\n=== {participant} rescored ===")
                print(json.dumps(new_score, ensure_ascii=False, indent=2))
        return

    case_path = Path(args.case)
    if case_path.is_dir():
        case_files = sorted(case_path.glob("*.json"))
        if not case_files:
            print(f"No case JSON files found in {case_path}")
            return
        all_results: List[Dict[str, Any]] = []
        for cf in case_files:
            case = read_json(cf)
            print(f"\n=== Running duel: {case['case_id']} ===")
            try:
                result = run_single_duel(
                    case, cf, output_root, args.pathway_base_url,
                    agent_env_path, args.model, args.timeout_seconds,
                )
                all_results.append(result)
                print(json.dumps({"case_id": case["case_id"], "run_dir": result["run_dir"]}, ensure_ascii=False))
            except Exception as exc:
                print(f"FAILED {case['case_id']}: {exc}")
        if all_results:
            leaderboard = build_leaderboard(all_results, output_root / "leaderboard.md")
            print(f"\n{leaderboard}")
            json_dump(output_root / "leaderboard.json", all_results)
    else:
        case = read_json(case_path)
        result = run_single_duel(
            case, case_path, output_root, args.pathway_base_url,
            agent_env_path, args.model, args.timeout_seconds,
        )
        print(json.dumps({"run_dir": result["run_dir"], "score_file": str(Path(result["run_dir"]) / "duel_score.json")}, ensure_ascii=False))


if __name__ == "__main__":
    main()
