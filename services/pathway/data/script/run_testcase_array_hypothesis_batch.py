from __future__ import annotations

import json
import re
import sys
import unicodedata
from datetime import datetime
from pathlib import Path
from typing import Any


BASE_DIR = Path(__file__).resolve().parent
NOTEBOOKLM_DIR = BASE_DIR.parents[1]
PIPELINE_DIR = NOTEBOOKLM_DIR / "workspaces" / "claims_insights" / "pipeline"
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))
if str(PIPELINE_DIR) not in sys.path:
    sys.path.insert(0, str(PIPELINE_DIR))

from disease_hypothesis_engine import DiseaseHypothesisEngine  # noqa: E402
from test_kich_ban_json_case import (  # noqa: E402
    as_text,
    collect_patient_context,
    collect_sign_sets,
    collect_structured_signs,
)


GENERIC_DISEASE_TOKENS = {
    "benh",
    "hoi",
    "chung",
    "da",
    "ly",
    "nguy",
    "kich",
    "cap",
    "man",
    "tinh",
    "do",
    "test",
    "onto",
    "medical",
}


def now_iso() -> str:
    return datetime.now().astimezone().isoformat()


def load_cases(path: Path) -> list[dict[str, Any]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if isinstance(payload, dict):
        return [payload]
    return []


def ascii_fold(value: Any) -> str:
    text = as_text(value).lower().replace("đ", "d").replace("Đ", "d")
    normalized = unicodedata.normalize("NFD", text)
    stripped = "".join(ch for ch in normalized if unicodedata.category(ch) != "Mn")
    stripped = re.sub(r"[^a-z0-9 ]+", " ", stripped)
    return re.sub(r"\s+", " ", stripped).strip()


def important_disease_tokens(value: Any) -> list[str]:
    tokens: list[str] = []
    for token in ascii_fold(value).split():
        if len(token) <= 2 or token in GENERIC_DISEASE_TOKENS:
            continue
        tokens.append(token)
    return tokens


def hypothesis_hit_flags(expected_disease: str, hypotheses: list[dict[str, Any]]) -> dict[str, Any]:
    expected_tokens = important_disease_tokens(expected_disease)

    def score_item(item: dict[str, Any]) -> float:
        predicted_tokens = set(important_disease_tokens(item.get("disease_name")))
        if not expected_tokens or not predicted_tokens:
            return 0.0
        overlap = set(expected_tokens) & predicted_tokens
        return len(overlap) / max(len(set(expected_tokens)), 1)

    scored = [score_item(item) for item in hypotheses]
    top1_score = scored[0] if scored else 0.0
    top3_score = max(scored[:3]) if scored else 0.0
    return {
        "expected_disease": expected_disease,
        "expected_tokens": expected_tokens,
        "top1_score": round(top1_score, 4),
        "top3_score": round(top3_score, 4),
        "top1_hit": top1_score >= 0.6,
        "top3_hit": top3_score >= 0.6,
    }


def build_case_report(case_payload: dict[str, Any], input_path: Path, index: int, engine: DiseaseHypothesisEngine) -> dict[str, Any]:
    case_level = ((case_payload.get("du_lieu_labeling_mau") or {}).get("case_level") or {})
    service_rows = ((case_payload.get("du_lieu_labeling_mau") or {}).get("service_lines") or [])
    sign_sets = collect_sign_sets(case_payload)
    patient_context = collect_patient_context(case_payload)
    structured_signs = collect_structured_signs(case_payload)
    hypothesis_payload = engine.infer(
        signs=sign_sets.get("chief_plus_findings") or sign_sets.get("all_case_signs") or [],
        structured_signs=structured_signs,
        patient_context=patient_context,
        observed_services=service_rows,
        specialty=as_text(case_level.get("specialty")),
        top_k=8,
    )
    expected_disease = as_text(case_level.get("main_disease_name_vi")) or as_text(
        ((case_payload.get("cau_chuyen_y_khoa") or {}).get("chan_doan_cuoi_cung"))
    )
    hit_flags = hypothesis_hit_flags(expected_disease, hypothesis_payload.get("hypotheses") or [])
    return {
        "generated_at": now_iso(),
        "input_json": str(input_path),
        "case_index": index,
        "case_level": case_level,
        "story_title": as_text(((case_payload.get("cau_chuyen_y_khoa") or {}).get("tieu_de"))),
        "expected_disease": expected_disease,
        "patient_context": patient_context,
        "sign_sets": sign_sets,
        "structured_signs": structured_signs,
        "service_lines": service_rows,
        "hypothesis_result": hypothesis_payload,
        "hit_flags": hit_flags,
    }


def write_case_outputs(report: dict[str, Any], output_dir: Path) -> dict[str, str]:
    request_id = as_text((report.get("case_level") or {}).get("request_id")) or f"case_{report['case_index']:03d}"
    safe_id = re.sub(r"[^A-Za-z0-9_-]+", "_", request_id)
    json_path = output_dir / f"{safe_id}_hypothesis_report.json"
    md_path = output_dir / f"{safe_id}_hypothesis_report.md"
    json_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    hypotheses = (report.get("hypothesis_result") or {}).get("hypotheses") or []
    hit_flags = report.get("hit_flags") or {}
    lines = [
        f"# {safe_id} Disease Hypothesis Report",
        "",
        f"- Input: `{report['input_json']}`",
        f"- Story title: `{report['story_title']}`",
        f"- Expected disease: `{report['expected_disease']}`",
        f"- Top-1 hit: `{hit_flags.get('top1_hit')}`",
        f"- Top-3 hit: `{hit_flags.get('top3_hit')}`",
        "",
        "## Top Hypotheses",
        "",
    ]
    for item in hypotheses[:5]:
        lines.append(
            f"- `{item.get('icd10')}` {item.get('disease_name')} | status `{item.get('status')}` | "
            f"score `{item.get('score')}` | confidence `{item.get('confidence')}`"
        )
        if item.get("matched_services"):
            lines.append(f"  matched_services: `{'; '.join(item.get('matched_services')[:4])}`")
        if item.get("memory_recommendations"):
            lines.append(f"  memory: `{'; '.join(item.get('memory_recommendations')[:2])}`")
        if item.get("graph_context_snippets"):
            lines.append(f"  graph_context: `{'; '.join(item.get('graph_context_snippets')[:2])}`")
    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return {"json": str(json_path), "md": str(md_path)}


def build_case_summary(report: dict[str, Any]) -> dict[str, Any]:
    hypotheses = ((report.get("hypothesis_result") or {}).get("hypotheses") or [])
    top1 = hypotheses[0] if hypotheses else {}
    return {
        "request_id": as_text((report.get("case_level") or {}).get("request_id")),
        "testcase_title": as_text((report.get("case_level") or {}).get("testcase_title")),
        "expected_disease": report.get("expected_disease"),
        "service_count": len(report.get("service_lines") or []),
        "top1_disease": as_text(top1.get("disease_name")),
        "top1_status": as_text(top1.get("status")),
        "top1_score": float(top1.get("score") or 0.0),
        "top1_confidence": float(top1.get("confidence") or 0.0),
        "top1_memory_match_count": int(top1.get("memory_match_count") or 0),
        "top1_memory_prior_score": float(top1.get("memory_prior_score") or 0.0),
        "top1_graph_context_match_count": int(top1.get("graph_context_match_count") or 0),
        "top1_graph_context_score": float(top1.get("graph_context_score") or 0.0),
        "top1_hit": bool((report.get("hit_flags") or {}).get("top1_hit")),
        "top3_hit": bool((report.get("hit_flags") or {}).get("top3_hit")),
        "matched_service_count_top1": len(top1.get("matched_services") or []),
    }


def build_batch_for_file(input_path: Path, output_dir: Path) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    engine = DiseaseHypothesisEngine()
    cases = load_cases(input_path)
    case_summaries: list[dict[str, Any]] = []

    for index, case_payload in enumerate(cases, start=1):
        report = build_case_report(case_payload, input_path, index, engine)
        paths = write_case_outputs(report, output_dir)
        summary = build_case_summary(report)
        summary["report_json"] = paths["json"]
        summary["report_md"] = paths["md"]
        case_summaries.append(summary)

    aggregate = {
        "evaluated_case_count": len(case_summaries),
        "top1_hit_count": sum(1 for item in case_summaries if item["top1_hit"]),
        "top3_hit_count": sum(1 for item in case_summaries if item["top3_hit"]),
        "avg_top1_confidence": round(
            sum(item["top1_confidence"] for item in case_summaries) / max(len(case_summaries), 1),
            4,
        ),
        "cases_with_memory_support": sum(1 for item in case_summaries if item["top1_memory_match_count"] > 0),
        "cases_with_graph_context": sum(1 for item in case_summaries if item["top1_graph_context_match_count"] > 0),
        "top1_with_matched_service_count": sum(1 for item in case_summaries if item["matched_service_count_top1"] > 0),
    }

    payload = {
        "generated_at": now_iso(),
        "input_file": str(input_path),
        "aggregate": aggregate,
        "cases": case_summaries,
    }

    summary_json = output_dir / "batch_summary.json"
    summary_md = output_dir / "batch_summary.md"
    summary_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    lines = [
        f"# Disease Hypothesis Batch Summary: {input_path.name}",
        "",
        f"- Evaluated cases: `{aggregate['evaluated_case_count']}`",
        f"- Top-1 hit count: `{aggregate['top1_hit_count']}`",
        f"- Top-3 hit count: `{aggregate['top3_hit_count']}`",
        f"- Avg top-1 confidence: `{aggregate['avg_top1_confidence']}`",
        f"- Cases with memory support on top-1: `{aggregate['cases_with_memory_support']}`",
        f"- Cases with graph context on top-1: `{aggregate['cases_with_graph_context']}`",
        f"- Top-1 with matched service evidence: `{aggregate['top1_with_matched_service_count']}`",
        "",
        "## Case Table",
        "",
    ]
    for item in case_summaries:
        lines.append(
            f"- `{item['request_id']}` | expected `{item['expected_disease']}` | top1 `{item['top1_disease']}` "
            f"| status `{item['top1_status']}` | top1_hit `{item['top1_hit']}` | matched_service `{item['matched_service_count_top1']}` "
            f"| memory `{item['top1_memory_match_count']}` | graph `{item['top1_graph_context_match_count']}`"
        )
    summary_md.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return {"summary_json": str(summary_json), "summary_md": str(summary_md)}


def main() -> None:
    if len(sys.argv) < 2:
        raise SystemExit("Usage: python run_testcase_array_hypothesis_batch.py <input_json> [output_dir]")
    input_path = Path(sys.argv[1]).resolve()
    if not input_path.exists():
        raise SystemExit(f"Input file not found: {input_path}")
    if len(sys.argv) >= 3:
        output_dir = Path(sys.argv[2]).resolve()
    else:
        output_dir = input_path.parent / f"{input_path.stem}_hypothesis_batch"
    outputs = build_batch_for_file(input_path, output_dir)
    print(f"Batch summary JSON: {outputs['summary_json']}")
    print(f"Batch summary MD: {outputs['summary_md']}")


if __name__ == "__main__":
    main()
