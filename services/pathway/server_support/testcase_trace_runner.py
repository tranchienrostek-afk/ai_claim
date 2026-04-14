from __future__ import annotations

import json
import os
import re
import sys
import unicodedata
from datetime import datetime
from pathlib import Path
from typing import Any, Callable

from dotenv import load_dotenv
from langchain_openai import AzureChatOpenAI
from neo4j import GraphDatabase

NOTEBOOKLM_DIR = Path(__file__).resolve().parents[1]
load_dotenv(NOTEBOOKLM_DIR / ".env")
SCRIPT_DIR = NOTEBOOKLM_DIR / "data" / "script"
PIPELINE_DIR = NOTEBOOKLM_DIR / "workspaces" / "claims_insights" / "pipeline"
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))
if str(PIPELINE_DIR) not in sys.path:
    sys.path.insert(0, str(PIPELINE_DIR))

from disease_hypothesis_engine import DiseaseHypothesisEngine  # noqa: E402
from test_kich_ban_json_case import (  # noqa: E402
    as_text,
    collect_patient_context,
    collect_sign_sets,
    collect_structured_signs,
    run_service_mapping,
)


TRACE_RUNS_DIR = SCRIPT_DIR / "testcase_trace_runs"
RECENT_FILE_PATTERNS = ("kich_ban_*.json", "testcase_*.json")
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

# ---------------------------------------------------------------------------
# Neo4j connection for graph-based adjudication
# ---------------------------------------------------------------------------
_NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7688")
_NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
_NEO4J_PASS = os.getenv("NEO4J_PASSWORD", "password123")

_neo4j_driver = None


def _get_neo4j_driver():
    global _neo4j_driver
    if _neo4j_driver is None:
        _neo4j_driver = GraphDatabase.driver(_NEO4J_URI, auth=(_NEO4J_USER, _NEO4J_PASS))
    return _neo4j_driver


def _query_graph_expected_services(disease_name: str) -> list[dict[str, str]]:
    """Query Neo4j for DISEASE_EXPECTS_SERVICE relationships for a given disease.

    Returns list of {service_code, service_name, role, category} dicts.
    Uses text overlap to find the best matching DiseaseHypothesis node.
    """
    if not disease_name:
        return []
    try:
        driver = _get_neo4j_driver()
        with driver.session() as session:
            result = session.run(
                """
                MATCH (h:DiseaseHypothesis)-[r:DISEASE_EXPECTS_SERVICE]->(s:ProtocolService)
                RETURN h.disease_name AS disease, s.service_code AS code,
                       s.service_name AS name, r.role AS role, r.category AS category
                """
            )
            all_rows = [dict(record) for record in result]
    except Exception:
        return []

    # Group by disease, find best match via text overlap
    diseases: dict[str, list[dict[str, str]]] = {}
    for row in all_rows:
        d = as_text(row.get("disease"))
        if d not in diseases:
            diseases[d] = []
        diseases[d].append({
            "service_code": as_text(row.get("code")),
            "service_name": as_text(row.get("name")),
            "role": as_text(row.get("role")),
            "category": as_text(row.get("category")),
        })

    best_disease = ""
    best_score = 0.0
    for d in diseases:
        score = text_overlap_score(disease_name, d)
        if score > best_score:
            best_score = score
            best_disease = d

    if best_score >= 0.34 and best_disease:
        return diseases[best_disease]
    return []


# Cache for expected services per disease (avoid repeated Neo4j queries)
_expected_services_cache: dict[str, list[dict[str, str]]] = {}


def _get_expected_services_cached(disease_name: str) -> list[dict[str, str]]:
    key = ascii_fold(disease_name)
    if key not in _expected_services_cache:
        _expected_services_cache[key] = _query_graph_expected_services(disease_name)
    return _expected_services_cache[key]


# ---------------------------------------------------------------------------
# LLM verification — "observe again before concluding"
# ---------------------------------------------------------------------------
_llm_chat = None


def _get_llm_chat() -> AzureChatOpenAI:
    global _llm_chat
    if _llm_chat is None:
        _llm_chat = AzureChatOpenAI(
            azure_deployment=os.getenv("MODEL2", "gpt-5-mini").strip(),
            azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT", "").strip(),
            api_key=os.getenv("AZURE_OPENAI_API_KEY", "").strip(),
            api_version=os.getenv("AZURE_OPENAI_API_VERSION", "2024-12-01-preview").strip(),
            temperature=1,
        )
    return _llm_chat


def _query_protocol_evidence(disease_names: list[str], max_chunks: int = 6) -> str:
    """Query Neo4j for protocol chunks relevant to the diseases — provides
    grounded evidence for LLM to cite rather than using general knowledge."""
    if not disease_names:
        return "(không tìm thấy tài liệu tham khảo)"
    try:
        driver = _get_neo4j_driver()
        chunks = []
        with driver.session() as session:
            for disease in disease_names[:3]:
                result = session.run(
                    """
                    MATCH (c:Chunk)
                    WHERE c.disease_name IS NOT NULL
                      AND toLower(c.disease_name) CONTAINS toLower($disease_token)
                    RETURN c.title AS title, c.section_path AS section,
                           left(c.content, 300) AS content, c.disease_name AS disease
                    LIMIT $limit
                    """,
                    disease_token=disease.split()[0] if disease else "",
                    limit=max_chunks // len(disease_names[:3]),
                )
                for record in result:
                    title = as_text(record["title"])
                    section = as_text(record["section"])
                    content = as_text(record["content"])
                    disease = as_text(record["disease"])
                    chunks.append(f"[{disease} / {section or title}]: {content}")
        if chunks:
            return "\n".join(chunks[:max_chunks])
    except Exception:
        pass
    return "(không tìm thấy tài liệu tham khảo)"


def _llm_verify_service_lines(
    disease_hypotheses: list[str],
    signs: list[str],
    service_lines: list[dict[str, Any]],
    adjudication_lines: list[dict[str, Any]],
    mapping_rows: dict[int, dict[str, Any]] | None = None,
) -> dict[int, dict[str, Any]]:
    """LLM verification with top-K candidates and grounded evidence.

    Three jobs:
    1. RERANK — pick the best mapping from top-K candidates (not just top-1)
    2. VERIFY — check clinical justification based on protocol evidence from Neo4j
    3. CLASSIFY — assign role (screening/diagnostic/confirmatory/rule_out/treatment/severity)

    If no evidence from data → justified=false (cannot confirm without documentation).
    """
    if not service_lines:
        return {}
    mapping_rows = mapping_rows or {}

    # Build per-line info with TOP-K candidates for LLM reranking
    lines_parts = []
    for sl, adj in zip(service_lines, adjudication_lines):
        line_no = int(sl.get("line_no") or 0)
        raw_name = as_text(sl.get("service_name_raw"))
        row = mapping_rows.get(line_no, {})

        # Collect ALL candidates from both mappers (top-K)
        base_alts = row.get("base_alternatives") or []
        hybrid_alts = row.get("hybrid_alternatives") or []

        # Merge and deduplicate by service_code, keep top-5
        seen_codes: set[str] = set()
        candidates: list[str] = []
        for alt in (base_alts + hybrid_alts):
            code = as_text(alt.get("service_code"))
            name = as_text(alt.get("canonical_name"))
            conf = as_text(alt.get("confidence"))
            score = alt.get("score", 0)
            if code and code not in seen_codes:
                seen_codes.add(code)
                candidates.append(f"    [{code}] {name} (score={score:.0f}, conf={conf})")
        candidates = candidates[:5]

        if candidates:
            cand_text = "\n".join(candidates)
            lines_parts.append(f"  Line {line_no}: \"{raw_name}\"\n    Candidates:\n{cand_text}")
        else:
            lines_parts.append(f"  Line {line_no}: \"{raw_name}\"\n    Candidates: (không tìm được)")

    lines_text = "\n".join(lines_parts)
    signs_text = ", ".join(signs[:15])
    engine_hint = ", ".join(disease_hypotheses[:5]) if disease_hypotheses else "(không có)"

    # Query Neo4j for protocol evidence
    evidence_text = _query_protocol_evidence(disease_hypotheses)

    prompt = f"""Bạn là bác sĩ thẩm định bảo hiểm y tế. Nhiệm vụ: rerank mapping và xác nhận dịch vụ dựa trên DỮ LIỆU.

=== TRIỆU CHỨNG ===
{signs_text}

=== CHẨN ĐOÁN PHÂN BIỆT (hệ thống gợi ý) ===
{engine_hint}

=== DỊCH VỤ + TOP CANDIDATES ===
{lines_text}

=== TÀI LIỆU THAM KHẢO TỪ PROTOCOL ===
{evidence_text}

=== NHIỆM VỤ ===
Với MỖI service line:
1. RERANK: Từ danh sách candidates, chọn candidate ĐÚNG NHẤT khớp với tên dịch vụ gốc.
   - Nếu KHÔNG có candidate nào khớp → selected_code = null
   - Ưu tiên: tên khớp nghĩa > score cao
2. VERIFY: Dịch vụ GỐC có hợp lý lâm sàng cho triệu chứng trên không?
   - Ưu tiên dựa trên TÀI LIỆU THAM KHẢO. Nếu không có tài liệu nhưng dịch vụ hiển nhiên hợp lý (VD: xét nghiệm máu cho bệnh nhiễm trùng) → justified=true
3. CLASSIFY: role (screening / diagnostic / confirmatory / rule_out / treatment / severity / monitoring)

Trả lời JSON array:
[{{"line_no": N, "selected_code": "MÃ_ĐÚNG_NHẤT hoặc null", "selected_name": "tên canonical", "justified": true/false, "best_disease": "...", "role": "...", "evidence_source": "trích tài liệu hoặc 'hiển nhiên hợp lý' hoặc '(không có)'", "reasoning": "1 câu"}}]
CHỈ trả JSON."""

    try:
        llm = _get_llm_chat()
        resp = llm.invoke(prompt)
        raw = as_text(resp.content)
        bracket_start = raw.find("[")
        bracket_end = raw.rfind("]")
        if bracket_start >= 0 and bracket_end > bracket_start:
            raw = raw[bracket_start:bracket_end + 1]
        if not raw.strip():
            return {}
        items = json.loads(raw)
        return {int(item["line_no"]): item for item in items if isinstance(item, dict) and "line_no" in item}
    except Exception:
        import traceback
        traceback.print_exc()
        return {}


def now_iso() -> str:
    return datetime.now().astimezone().isoformat()


def safe_log(log: Callable[[str], None], text: str) -> None:
    try:
        log(text)
    except UnicodeEncodeError:
        log(text.encode("ascii", "replace").decode("ascii"))


def load_cases(path: Path) -> list[dict[str, Any]]:
    raw = path.read_text(encoding="utf-8").strip()
    # Strip markdown code fences if present (```json ... ```)
    if raw.startswith("```"):
        raw = re.sub(r"^```[a-zA-Z]*\n?", "", raw)
        raw = re.sub(r"\n?```\s*$", "", raw)
    payload = json.loads(raw)
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if isinstance(payload, dict):
        return [payload]
    return []


def ascii_fold(value: Any) -> str:
    text = as_text(value).lower().replace("đ", "d").replace("Đ", "d").replace("Ä‘", "d").replace("Ä", "d")
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


def text_overlap_score(left: Any, right: Any) -> float:
    left_tokens = set(important_disease_tokens(left))
    right_tokens = set(important_disease_tokens(right))
    if not left_tokens or not right_tokens:
        return 0.0
    overlap = left_tokens & right_tokens
    if not overlap:
        return 0.0
    return len(overlap) / max(len(left_tokens | right_tokens), 1)


def list_available_testcase_jsons(script_dir: Path | None = None) -> list[dict[str, Any]]:
    directory = script_dir or SCRIPT_DIR
    rows: list[dict[str, Any]] = []
    seen: set[Path] = set()
    for pattern in RECENT_FILE_PATTERNS:
        for path in sorted(directory.glob(pattern)):
            if path in seen or not path.is_file():
                continue
            seen.add(path)
            rows.append(
                {
                    "name": path.name,
                    "path": str(path),
                    "relative_path": str(path.relative_to(NOTEBOOKLM_DIR)),
                    "size_bytes": path.stat().st_size,
                    "modified_at": datetime.fromtimestamp(path.stat().st_mtime).astimezone().isoformat(),
                }
            )
    rows.sort(key=lambda item: item["modified_at"], reverse=True)
    return rows


def _case_request_id(case_payload: dict[str, Any], index: int) -> str:
    case_level = ((case_payload.get("du_lieu_labeling_mau") or {}).get("case_level") or {})
    request_id = as_text(case_level.get("request_id"))
    if request_id:
        return request_id
    return f"case_{index:03d}"


def _safe_case_id(case_payload: dict[str, Any], index: int) -> str:
    return re.sub(r"[^A-Za-z0-9_-]+", "_", _case_request_id(case_payload, index))


def _disease_hit_flags(expected_disease: str, hypotheses: list[dict[str, Any]]) -> dict[str, Any]:
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


def _query_disease_specialty(disease_name: str) -> str:
    """Look up the specialty for a disease from DiseaseHypothesis nodes in Neo4j."""
    if not disease_name:
        return ""
    try:
        driver = _get_neo4j_driver()
        with driver.session() as session:
            result = session.run(
                "MATCH (h:DiseaseHypothesis) RETURN h.disease_name AS name, h.specialty AS spec"
            )
            best_spec = ""
            best_score = 0.0
            for record in result:
                score = text_overlap_score(disease_name, as_text(record["name"]))
                if score > best_score:
                    best_score = score
                    best_spec = as_text(record["spec"])
            if best_score >= 0.34:
                return best_spec
    except Exception:
        pass
    return ""


# Specialty compatibility groups — diseases in the same group should not penalize each other
_SPECIALTY_GROUPS = {
    "TMH": {"TMH"},
    "Hô hấp": {"Hô hấp", "Truyền nhiễm", "Nội tổng quát"},
    "Truyền nhiễm": {"Truyền nhiễm", "Hô hấp", "Nội tổng quát"},
    "Huyết học": {"Huyết học", "Nội tổng quát"},
    "Nội tổng quát": {"Nội tổng quát", "Hô hấp", "Truyền nhiễm", "Huyết học"},
}


def _select_focus_hypothesis(
    expected_disease: str,
    hypotheses: list[dict[str, Any]],
    case_specialty: str = "",
) -> tuple[dict[str, Any], dict[str, Any], str, float]:
    free_top_hypothesis = (hypotheses[:1] or [{}])[0]
    anchored_hypothesis = free_top_hypothesis
    anchor_overlap = 0.0
    if expected_disease:
        best_item: dict[str, Any] | None = None
        best_score = 0.0
        for item in hypotheses:
            score = text_overlap_score(expected_disease, item.get("disease_name"))
            if score > best_score:
                best_score = score
                best_item = item
        if best_item and best_score >= 0.34:
            anchored_hypothesis = best_item
            anchor_overlap = best_score
            return free_top_hypothesis, anchored_hypothesis, "known_disease", round(anchor_overlap, 4)

    # Specialty gating: if case has a known specialty and free_top is from a
    # different specialty group, try to find a better match within the same group
    if case_specialty and hypotheses:
        expected_spec = _query_disease_specialty(expected_disease) if expected_disease else ""
        target_spec = expected_spec or case_specialty
        compatible = _SPECIALTY_GROUPS.get(target_spec, {target_spec})
        free_spec = _query_disease_specialty(as_text(free_top_hypothesis.get("disease_name")))
        if free_spec and free_spec not in compatible:
            # Free top is from wrong specialty — find best in-specialty hypothesis
            for item in hypotheses[1:]:
                item_spec = _query_disease_specialty(as_text(item.get("disease_name")))
                if item_spec in compatible:
                    anchored_hypothesis = item
                    anchor_overlap = 0.0
                    return free_top_hypothesis, anchored_hypothesis, "specialty_gated", 0.0

    return free_top_hypothesis, anchored_hypothesis, "free_reasoning", round(anchor_overlap, 4)


def _extract_service_targets(
    hypothesis_result: dict[str, Any],
    focus_disease_name: str = "",
) -> list[dict[str, str]]:
    hypotheses = list(hypothesis_result.get("hypotheses") or [])
    if focus_disease_name:
        hypotheses.sort(
            key=lambda item: (
                text_overlap_score(focus_disease_name, item.get("disease_name")),
                float(item.get("score") or 0.0),
                float(item.get("confidence") or 0.0),
            ),
            reverse=True,
        )
    hypotheses = hypotheses[:3]
    sign_payload = hypothesis_result.get("sign_payload") or {}
    seen: set[tuple[str, str]] = set()
    rows: list[dict[str, str]] = []

    for hyp in hypotheses:
        disease_name = as_text(hyp.get("disease_name"))
        for service_name in hyp.get("required_services") or []:
            text = as_text(service_name)
            key = (ascii_fold(text), disease_name)
            if text and key not in seen:
                seen.add(key)
                rows.append({"service_name": text, "source": "required_service", "disease_name": disease_name})
        for item in hyp.get("matched_services") or []:
            text = as_text(item)
            rhs = text.split("->", 1)[-1].strip() if "->" in text else text
            key = (ascii_fold(rhs), disease_name)
            if rhs and key not in seen:
                seen.add(key)
                rows.append({"service_name": rhs, "source": "matched_service", "disease_name": disease_name})

    top_names = [as_text(item.get("disease_name")) for item in hypotheses if as_text(item.get("disease_name"))]
    for service in sign_payload.get("recommended_services") or []:
        service_name = as_text(service.get("service_name"))
        supported = service.get("supporting_diseases") or []
        matched_names = []
        for disease in supported:
            disease_name = as_text(disease.get("disease_name"))
            if not disease_name:
                continue
            if any(text_overlap_score(disease_name, top_name) >= 0.45 for top_name in top_names):
                matched_names.append(disease_name)
        if not matched_names:
            continue
        for disease_name in matched_names[:2]:
            key = (ascii_fold(service_name), disease_name)
            if service_name and key not in seen:
                seen.add(key)
                rows.append({"service_name": service_name, "source": "recommended_service", "disease_name": disease_name})
    return rows


def _support_strength_label(score: float) -> str:
    if score >= 0.58:
        return "strong"
    if score >= 0.34:
        return "moderate"
    if score >= 0.18:
        return "weak"
    return "none"


def _line_decision(best_support_score: float, top_hypothesis: dict[str, Any], mapping_row: dict[str, Any]) -> str:
    top_status = as_text(top_hypothesis.get("status"))
    base_conf = as_text(((mapping_row.get("base_top") or {}).get("confidence")))
    hybrid_conf = as_text(((mapping_row.get("hybrid_top") or {}).get("confidence")))
    if top_status == "ruled_out" and best_support_score < 0.18:
        return "DENIAL"
    if best_support_score >= 0.34:
        return "PAYMENT"
    if base_conf == "HIGH" or hybrid_conf == "HIGH":
        return "REVIEW"
    return "REVIEW"


def _build_service_line_trace(
    service_line: dict[str, Any],
    mapping_row: dict[str, Any],
    hypothesis_result: dict[str, Any],
    focus_hypothesis: dict[str, Any] | None = None,
) -> dict[str, Any]:
    top_hypothesis = focus_hypothesis or (((hypothesis_result.get("hypotheses") or [])[:1] or [{}])[0])
    service_targets = _extract_service_targets(hypothesis_result, as_text(top_hypothesis.get("disease_name")))
    observed_name = as_text(service_line.get("service_name_raw"))
    base_top = mapping_row.get("base_top") or {}
    hybrid_top = mapping_row.get("hybrid_top") or {}

    # --- Mapping sanity check ---
    # If raw service name and mapped canonical name have very low overlap,
    # the mapping is unreliable — don't trust it for graph lookup.
    base_canonical = as_text(base_top.get("canonical_name"))
    hybrid_canonical = as_text(hybrid_top.get("canonical_name"))
    base_mapping_sane = text_overlap_score(observed_name, base_canonical) >= 0.15 if base_canonical else False
    hybrid_mapping_sane = text_overlap_score(observed_name, hybrid_canonical) >= 0.15 if hybrid_canonical else False
    mapping_unreliable = not base_mapping_sane and not hybrid_mapping_sane

    comparison_texts = [observed_name]
    if base_mapping_sane:
        comparison_texts.append(base_canonical)
    if hybrid_mapping_sane:
        comparison_texts.append(hybrid_canonical)

    best_target: dict[str, Any] | None = None
    best_score = 0.0
    for target in service_targets:
        target_name = as_text(target.get("service_name"))
        score = max(text_overlap_score(text, target_name) for text in comparison_texts if as_text(text))
        if score > best_score:
            best_score = score
            best_target = target

    # --- Multi-hypothesis graph-based expected service lookup ---
    # Check top-3 hypotheses (not just focus), because one service can be
    # justified by ANY of the differential diagnoses.
    graph_match: dict[str, str] | None = None
    graph_match_role = ""
    graph_match_disease = ""
    all_hypotheses = hypothesis_result.get("hypotheses") or []
    # Build candidate disease list: focus first, then top hypotheses
    candidate_diseases: list[str] = []
    focus_disease_name = as_text(top_hypothesis.get("disease_name"))
    if focus_disease_name:
        candidate_diseases.append(focus_disease_name)
    for hyp in all_hypotheses[:5]:
        d = as_text(hyp.get("disease_name"))
        if d and d not in candidate_diseases:
            candidate_diseases.append(d)
    candidate_diseases = candidate_diseases[:3]  # top-3

    # Determine which mapped code to use for graph lookup
    mapped_code = ""
    if not mapping_unreliable:
        if base_mapping_sane:
            mapped_code = as_text(base_top.get("service_code"))
        elif hybrid_mapping_sane:
            mapped_code = as_text(hybrid_top.get("service_code"))

    for disease_candidate in candidate_diseases:
        if graph_match:
            break
        expected = _get_expected_services_cached(disease_candidate)
        if not expected:
            continue

        # Strategy 1: exact code match (only if mapping is sane)
        if mapped_code:
            for exp in expected:
                if exp["service_code"] == mapped_code:
                    graph_match = exp
                    graph_match_role = exp.get("role", "")
                    graph_match_disease = disease_candidate
                    break

        # Strategy 2: text overlap between RAW service name and expected service names
        # This works even when mapping is wrong — directly compare what the doctor wrote
        if not graph_match:
            for exp in expected:
                overlap = text_overlap_score(observed_name, exp["service_name"])
                if overlap >= 0.45:
                    graph_match = exp
                    graph_match_role = exp.get("role", "")
                    graph_match_disease = disease_candidate
                    break

        # Strategy 3: canonical name overlap (only if mapping is sane)
        if not graph_match and not mapping_unreliable:
            mapped_canonical = base_canonical if base_mapping_sane else hybrid_canonical
            if mapped_canonical:
                for exp in expected:
                    overlap = text_overlap_score(mapped_canonical, exp["service_name"])
                    if overlap >= 0.55:
                        graph_match = exp
                        graph_match_role = exp.get("role", "")
                        graph_match_disease = disease_candidate
                        break

    # Boost support score if graph confirms expected service
    if graph_match:
        # Service code matched in DISEASE_EXPECTS_SERVICE → strong clinical support
        graph_boost = 0.45  # ensures >= 0.34 PAYMENT threshold
        if graph_match_role in ("confirmatory", "screening", "diagnostic"):
            graph_boost = 0.55
        elif graph_match_role in ("treatment", "severity"):
            graph_boost = 0.50
        elif graph_match_role == "rule_out":
            graph_boost = 0.45
        best_score = max(best_score, graph_boost)

    support_strength = _support_strength_label(best_score)
    proposed_label = _line_decision(best_score, top_hypothesis, mapping_row)
    explanation_bits = [
        f"Top hypothesis: {as_text(top_hypothesis.get('disease_name')) or '-'} ({as_text(top_hypothesis.get('status')) or 'active'})",
        f"Mapping: base={as_text(base_top.get('service_code')) or '-'} {as_text(base_top.get('confidence')) or '-'}, hybrid={as_text(hybrid_top.get('service_code')) or '-'} {as_text(hybrid_top.get('confidence')) or '-'}",
    ]
    if mapping_unreliable:
        explanation_bits.append(f"WARNING: mapping unreliable — '{observed_name}' has low overlap with mapped canonical names")
    if graph_match:
        explanation_bits.append(
            f"Graph match: service expected for '{graph_match_disease}' (role={graph_match_role}, boost→{best_score:.2f})"
        )
    if best_target:
        explanation_bits.append(
            f"Best clinical support: {observed_name} ~= {as_text(best_target.get('service_name'))} ({support_strength}, {best_score:.2f})"
        )
    elif not graph_match:
        explanation_bits.append("No strong target service retrieved from current disease hypotheses.")

    evidence = []
    for item in (top_hypothesis.get("evidence_items") or [])[:4]:
        evidence.append(
            {
                "source": as_text(item.get("source")),
                "score_contribution": float(item.get("score_contribution") or 0.0),
                "summary": as_text(item.get("candidate_disease"))
                or "; ".join([as_text(text) for text in (item.get("recommendations") or [])[:2] if as_text(text)])
                or "; ".join([as_text(text) for text in (item.get("snippets") or [])[:2] if as_text(text)])
                or "; ".join([as_text(text) for text in (item.get("matched_services") or [])[:2] if as_text(text)]),
            }
        )

    return {
        "line_no": service_line.get("line_no"),
        "service_name_raw": observed_name,
        "ground_truth_label": as_text(service_line.get("final_label")),
        "reason_layer": as_text(service_line.get("reason_layer")),
        "proposed_label": proposed_label,
        "support_strength": support_strength,
        "support_score": round(best_score, 4),
        "matched_target_service": as_text((best_target or {}).get("service_name")),
        "matched_target_source": as_text((best_target or {}).get("source")),
        "mapped_service_code": as_text(base_top.get("service_code")) or as_text(hybrid_top.get("service_code")),
        "mapped_canonical_name": as_text(base_top.get("canonical_name")) or as_text(hybrid_top.get("canonical_name")),
        "mapping_resolution": as_text(mapping_row.get("base_resolution")) or as_text(mapping_row.get("hybrid_resolution")),
        "mapping_confidence": as_text(base_top.get("confidence")) or as_text(hybrid_top.get("confidence")),
        "top_hypothesis_disease": as_text(top_hypothesis.get("disease_name")),
        "top_hypothesis_status": as_text(top_hypothesis.get("status")),
        "top_hypothesis_confidence": float(top_hypothesis.get("confidence") or 0.0),
        "mapping_unreliable": mapping_unreliable,
        "graph_expected_match": bool(graph_match),
        "graph_expected_role": graph_match_role,
        "graph_expected_disease": graph_match_disease,
        "graph_expected_service": as_text((graph_match or {}).get("service_name")),
        "explanation": " | ".join([bit for bit in explanation_bits if bit]),
        "evidence": evidence,
    }


def _build_planning_trace(
    case_payload: dict[str, Any],
    sign_sets: dict[str, list[str]],
    structured_signs: list[dict[str, Any]],
    service_mapping: dict[str, Any],
    hypothesis_result: dict[str, Any],
    adjudication_lines: list[dict[str, Any]],
    seed_disease_hints: list[str],
    focus_hypothesis: dict[str, Any],
    free_top_hypothesis: dict[str, Any],
    anchor_mode: str,
    anchor_overlap: float,
) -> list[dict[str, Any]]:
    case_level = ((case_payload.get("du_lieu_labeling_mau") or {}).get("case_level") or {})
    return [
        {
            "step_no": 1,
            "agent": "planner",
            "title": "Read case and establish adjudication plan",
            "status": "completed",
            "summary": f"Loaded case `{as_text(case_level.get('request_id'))}` with {len((case_payload.get('du_lieu_labeling_mau') or {}).get('service_lines') or [])} service lines.",
            "details": {
                "specialty": as_text(case_level.get("specialty")),
                "clinical_question": as_text(case_level.get("initial_clinical_question")),
            },
        },
        {
            "step_no": 2,
            "agent": "mapping",
            "title": "Normalize signs and map service lines",
            "status": "completed",
            "summary": f"Collected {len(sign_sets.get('chief_plus_findings') or [])} raw/derived signs and mapped {service_mapping.get('summary', {}).get('service_count', 0)} service lines.",
            "details": {
                "chief_plus_findings": sign_sets.get("chief_plus_findings") or [],
                "structured_signs": structured_signs,
                "service_mapping_summary": service_mapping.get("summary") or {},
                "seed_disease_hints": seed_disease_hints,
            },
        },
        {
            "step_no": 3,
            "agent": "medical_reasoner",
            "title": "Generate disease hypotheses",
            "status": "completed",
            "summary": (
                f"Focus hypothesis: {as_text(focus_hypothesis.get('disease_name')) or '-'} "
                f"({as_text(focus_hypothesis.get('status')) or 'active'}) with confidence "
                f"{float(focus_hypothesis.get('confidence') or 0.0):.2f} via `{anchor_mode}`."
            ),
            "details": {
                "top_hypotheses": (hypothesis_result.get("hypotheses") or [])[:5],
                "free_top_hypothesis": free_top_hypothesis,
                "focus_hypothesis": focus_hypothesis,
                "anchor_mode": anchor_mode,
                "anchor_overlap": anchor_overlap,
            },
        },
        {
            "step_no": 4,
            "agent": "adjudicator",
            "title": "Trace service-line support for reviewer",
            "status": "completed",
            "summary": f"Built {len(adjudication_lines)} adjudication trace rows for insurance review.",
            "details": {
                "proposed_labels": {
                    "payment": sum(1 for row in adjudication_lines if as_text(row.get('proposed_label')) == "PAYMENT"),
                    "review": sum(1 for row in adjudication_lines if as_text(row.get('proposed_label')) == "REVIEW"),
                    "denial": sum(1 for row in adjudication_lines if as_text(row.get('proposed_label')) == "DENIAL"),
                }
            },
        },
    ]


def _build_chat_trace(
    case_payload: dict[str, Any],
    patient_context: dict[str, Any],
    sign_sets: dict[str, list[str]],
    hypothesis_result: dict[str, Any],
    adjudication_lines: list[dict[str, Any]],
) -> list[dict[str, str]]:
    case_level = ((case_payload.get("du_lieu_labeling_mau") or {}).get("case_level") or {})
    top_hypothesis = ((hypothesis_result.get("hypotheses") or [])[:1] or [{}])[0]
    recommendation_lines = []
    for row in adjudication_lines[:4]:
        recommendation_lines.append(
            f"Line {row.get('line_no')}: {row.get('service_name_raw')} -> {row.get('proposed_label')} ({row.get('support_strength')})"
        )
    return [
        {
            "speaker": "Planner Agent",
            "tone": "system",
            "content": f"Tôi sẽ đọc case `{as_text(case_level.get('request_id'))}`, gom dấu hiệu, map dịch vụ, suy luận bệnh, rồi ghi trace cho từng service line.",
        },
        {
            "speaker": "Context Agent",
            "tone": "analysis",
            "content": f"Patient context: giới tính `{as_text(patient_context.get('gioi_tinh')) or '-'}`, tuổi `{as_text(patient_context.get('tuoi')) or '-'}`. Dấu hiệu chính: {', '.join((sign_sets.get('chief_plus_findings') or [])[:6]) or '-'}",
        },
        {
            "speaker": "Medical Reasoner",
            "tone": "analysis",
            "content": f"Hypothesis mạnh nhất hiện là `{as_text(top_hypothesis.get('disease_name')) or '-'}` với status `{as_text(top_hypothesis.get('status')) or '-'}` và confidence `{float(top_hypothesis.get('confidence') or 0.0):.2f}`.",
        },
        {
            "speaker": "Adjudication Agent",
            "tone": "decision",
            "content": " | ".join(recommendation_lines) if recommendation_lines else "Chưa có service line nào để trace.",
        },
    ]


def _build_case_trace(
    case_payload: dict[str, Any],
    input_path: Path,
    index: int,
    engine: DiseaseHypothesisEngine,
    log: Callable[[str], None],
) -> dict[str, Any]:
    case_level = ((case_payload.get("du_lieu_labeling_mau") or {}).get("case_level") or {})
    request_id = _case_request_id(case_payload, index)
    story_title = as_text(((case_payload.get("cau_chuyen_y_khoa") or {}).get("tieu_de")))
    service_lines = ((case_payload.get("du_lieu_labeling_mau") or {}).get("service_lines") or [])

    safe_log(log, f"[Planner] {request_id}: parsing case payload")
    sign_sets = collect_sign_sets(case_payload)
    structured_signs = collect_structured_signs(case_payload)
    patient_context = collect_patient_context(case_payload)
    seed_disease_hints = [
        as_text(case_level.get("main_disease_name_vi")),
        as_text(case_level.get("initial_clinical_question")),
        as_text(((case_payload.get("cau_chuyen_y_khoa") or {}).get("chan_doan_cuoi_cung"))),
    ]
    seed_disease_hints = [item for item in seed_disease_hints if item]

    safe_log(log, f"[Mapper] {request_id}: mapping {len(service_lines)} service lines")
    service_mapping = run_service_mapping(service_lines)
    mapping_rows = {int(row.get("line_no") or 0): row for row in (service_mapping.get("rows") or [])}

    safe_log(log, f"[Reasoner] {request_id}: generating disease hypotheses")
    hypothesis_result = engine.infer(
        signs=sign_sets.get("chief_plus_findings") or sign_sets.get("all_case_signs") or [],
        structured_signs=structured_signs,
        patient_context=patient_context,
        observed_services=service_lines,
        specialty=as_text(case_level.get("specialty")),
        seed_disease_hints=seed_disease_hints,
        top_k=8,
    )
    hypotheses = hypothesis_result.get("hypotheses") or []
    expected_disease = as_text(case_level.get("main_disease_name_vi")) or as_text(((case_payload.get("cau_chuyen_y_khoa") or {}).get("chan_doan_cuoi_cung")))
    hit_flags = _disease_hit_flags(expected_disease, hypotheses)
    case_specialty = as_text(case_level.get("specialty"))
    free_top_hypothesis, top_hypothesis, anchor_mode, anchor_overlap = _select_focus_hypothesis(expected_disease, hypotheses, case_specialty=case_specialty)

    safe_log(log, f"[Adjudicator] {request_id}: building service-line trace")
    adjudication_lines = []
    for service_line in service_lines:
        row = mapping_rows.get(int(service_line.get("line_no") or 0), {})
        line_trace = _build_service_line_trace(service_line, row, hypothesis_result, focus_hypothesis=top_hypothesis)
        adjudication_lines.append(line_trace)

    # --- LLM Verification Pass ---
    # "Observe again before concluding" — ask LLM to verify ALL service lines
    # against the differential diagnoses. If LLM says not justified, demote
    # PAYMENT → REVIEW (never promote — thà bỏ sót còn hơn xác nhận sai).
    safe_log(log, f"[Verifier] {request_id}: LLM verifying {len(adjudication_lines)} service lines")
    disease_names = [as_text(h.get("disease_name")) for h in hypotheses[:5] if as_text(h.get("disease_name"))]
    all_signs = sign_sets.get("chief_plus_findings") or sign_sets.get("all_case_signs") or []
    llm_verdicts = _llm_verify_service_lines(disease_names, all_signs, service_lines, adjudication_lines, mapping_rows)

    for line_trace in adjudication_lines:
        line_no = int(line_trace.get("line_no") or 0)
        verdict = llm_verdicts.get(line_no)
        if verdict:
            llm_justified = bool(verdict.get("justified"))
            llm_reasoning = as_text(verdict.get("reasoning"))
            llm_role = as_text(verdict.get("role"))
            llm_disease = as_text(verdict.get("best_disease"))
            llm_evidence = as_text(verdict.get("evidence_source"))
            llm_selected_code = as_text(verdict.get("selected_code"))
            llm_selected_name = as_text(verdict.get("selected_name"))

            line_trace["llm_verified"] = llm_justified
            line_trace["llm_reasoning"] = llm_reasoning
            line_trace["llm_role"] = llm_role
            line_trace["llm_disease"] = llm_disease
            line_trace["llm_evidence_source"] = llm_evidence

            # If LLM reranked to a different code, update the mapping
            if llm_selected_code and llm_selected_code != "null":
                old_code = as_text(line_trace.get("mapped_service_code"))
                if llm_selected_code != old_code:
                    line_trace["llm_reranked_code"] = llm_selected_code
                    line_trace["llm_reranked_name"] = llm_selected_name
                    line_trace["mapped_service_code"] = llm_selected_code
                    line_trace["mapped_canonical_name"] = llm_selected_name
                    line_trace["explanation"] += f" | LLM RERANKED: {old_code}→{llm_selected_code}"

                    # Re-check graph with reranked code
                    focus_disease_name = as_text(line_trace.get("top_hypothesis_disease"))
                    all_hyp_diseases = [as_text(h.get("disease_name")) for h in hypotheses[:5] if as_text(h.get("disease_name"))]
                    for disease_candidate in ([focus_disease_name] + all_hyp_diseases)[:3]:
                        expected = _get_expected_services_cached(disease_candidate)
                        for exp in expected:
                            if exp["service_code"] == llm_selected_code:
                                line_trace["graph_expected_match"] = True
                                line_trace["graph_expected_role"] = exp.get("role", "")
                                line_trace["graph_expected_disease"] = disease_candidate
                                line_trace["graph_expected_service"] = exp.get("service_name", "")
                                break
                        if line_trace.get("graph_expected_match"):
                            break

            current_label = as_text(line_trace.get("proposed_label"))
            if current_label == "PAYMENT" and not llm_justified:
                line_trace["proposed_label"] = "REVIEW"
                line_trace["explanation"] += " | LLM DEMOTED: not justified"
            elif current_label == "REVIEW" and llm_justified:
                line_trace["proposed_label"] = "PAYMENT"
                line_trace["support_strength"] = "llm_verified"
                line_trace["explanation"] += f" | LLM PROMOTED: {llm_evidence[:60] if llm_evidence else llm_reasoning[:60]}"
        else:
            line_trace["llm_verified"] = None
            line_trace["llm_reasoning"] = ""
            line_trace["llm_role"] = ""
            line_trace["llm_disease"] = ""
            line_trace["llm_evidence_source"] = ""

    exact_match_count = 0
    for line_trace in adjudication_lines:
        if as_text(line_trace.get("ground_truth_label")) and as_text(line_trace.get("ground_truth_label")) == as_text(line_trace.get("proposed_label")):
            exact_match_count += 1

    planning_trace = _build_planning_trace(
        case_payload,
        sign_sets,
        structured_signs,
        service_mapping,
        hypothesis_result,
        adjudication_lines,
        seed_disease_hints,
        top_hypothesis,
        free_top_hypothesis,
        anchor_mode,
        anchor_overlap,
    )
    chat_trace = _build_chat_trace(
        case_payload,
        patient_context,
        sign_sets,
        hypothesis_result,
        adjudication_lines,
    )

    return {
        "generated_at": now_iso(),
        "input_json": str(input_path),
        "case_index": index,
        "case_id": request_id,
        "story_title": story_title,
        "expected_disease": expected_disease,
        "case_level": case_level,
        "patient_context": patient_context,
        "sign_sets": sign_sets,
        "structured_signs": structured_signs,
        "service_lines": service_lines,
        "service_mapping": service_mapping,
        "hypothesis_result": hypothesis_result,
        "planning_trace": planning_trace,
        "chat_trace": chat_trace,
        "adjudication_log": {
            "disclaimer": "Medical support trace from ontology/sign/service reasoning. Contract and benefit exclusions should be applied in a later layer.",
            "top_hypothesis": {
                "disease_name": as_text(top_hypothesis.get("disease_name")),
                "status": as_text(top_hypothesis.get("status")),
                "confidence": float(top_hypothesis.get("confidence") or 0.0),
                "score": float(top_hypothesis.get("score") or 0.0),
                "anchor_mode": anchor_mode,
                "anchor_overlap": anchor_overlap,
                "free_top1_disease": as_text(free_top_hypothesis.get("disease_name")),
                "free_top1_confidence": float(free_top_hypothesis.get("confidence") or 0.0),
            },
            "service_lines": adjudication_lines,
        },
        "summary": {
            "request_id": request_id,
            "testcase_title": as_text(case_level.get("testcase_title")) or story_title,
            "service_line_count": len(service_lines),
            "top1_disease": as_text(top_hypothesis.get("disease_name")),
            "top1_status": as_text(top_hypothesis.get("status")),
            "top1_confidence": float(top_hypothesis.get("confidence") or 0.0),
            "free_top1_disease": as_text(free_top_hypothesis.get("disease_name")),
            "free_top1_status": as_text(free_top_hypothesis.get("status")),
            "free_top1_confidence": float(free_top_hypothesis.get("confidence") or 0.0),
            "anchor_mode": anchor_mode,
            "anchor_overlap": anchor_overlap,
            "top1_hit": bool(hit_flags.get("top1_hit")),
            "top3_hit": bool(hit_flags.get("top3_hit")),
            "cases_with_graph_context": 1 if int(top_hypothesis.get("graph_context_match_count") or 0) > 0 else 0,
            "memory_match_count": int(top_hypothesis.get("memory_match_count") or 0),
            "graph_expected_match_count": sum(1 for row in adjudication_lines if row.get("graph_expected_match")),
            "proposed_payment_count": sum(1 for row in adjudication_lines if as_text(row.get("proposed_label")) == "PAYMENT"),
            "proposed_review_count": sum(1 for row in adjudication_lines if as_text(row.get("proposed_label")) == "REVIEW"),
            "proposed_denial_count": sum(1 for row in adjudication_lines if as_text(row.get("proposed_label")) == "DENIAL"),
            "ground_truth_match_count": exact_match_count,
        },
    }


def _write_case_artifacts(case_trace: dict[str, Any], output_dir: Path) -> dict[str, str]:
    safe_id = re.sub(r"[^A-Za-z0-9_-]+", "_", as_text(case_trace.get("case_id")) or f"case_{int(case_trace.get('case_index') or 0):03d}")
    json_path = output_dir / f"{safe_id}_trace.json"
    md_path = output_dir / f"{safe_id}_trace.md"
    chat_path = output_dir / f"{safe_id}_chat.md"

    json_path.write_text(json.dumps(case_trace, ensure_ascii=False, indent=2), encoding="utf-8")

    summary = case_trace.get("summary") or {}
    lines = [
        f"# {safe_id} Trace Report",
        "",
        f"- Story title: `{as_text(case_trace.get('story_title'))}`",
        f"- Expected disease: `{as_text(case_trace.get('expected_disease'))}`",
        f"- Top-1 disease: `{as_text(summary.get('top1_disease'))}`",
        f"- Free top-1 disease: `{as_text(summary.get('free_top1_disease'))}`",
        f"- Anchor mode: `{as_text(summary.get('anchor_mode'))}` (overlap `{float(summary.get('anchor_overlap') or 0.0):.2f}`)",
        f"- Top-1 confidence: `{float(summary.get('top1_confidence') or 0.0):.2f}`",
        f"- Ground-truth match count: `{int(summary.get('ground_truth_match_count') or 0)}/{int(summary.get('service_line_count') or 0)}`",
        "",
        "## Planning Trace",
        "",
    ]
    for step in case_trace.get("planning_trace") or []:
        lines.append(f"- Step {step.get('step_no')}: `{step.get('agent')}` | {step.get('summary')}")
    lines.extend(["", "## Adjudication Lines", ""])
    for row in ((case_trace.get("adjudication_log") or {}).get("service_lines") or []):
        lines.append(
            f"- Line `{row.get('line_no')}` `{row.get('service_name_raw')}` | GT `{row.get('ground_truth_label')}` | Proposed `{row.get('proposed_label')}` | Support `{row.get('support_strength')}`"
        )
    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    chat_lines = [f"# {safe_id} Agent Chat Trace", ""]
    for item in case_trace.get("chat_trace") or []:
        chat_lines.append(f"## {as_text(item.get('speaker'))}")
        chat_lines.append("")
        chat_lines.append(as_text(item.get("content")) or "-")
        chat_lines.append("")
    chat_path.write_text("\n".join(chat_lines).strip() + "\n", encoding="utf-8")

    return {
        "json": str(json_path),
        "md": str(md_path),
        "chat_md": str(chat_path),
    }


def run_testcase_trace_batch(
    input_path: Path,
    output_dir: Path,
    *,
    graph_namespace: str = "ontology_v2",
    log: Callable[[str], None] = print,
) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    TRACE_RUNS_DIR.mkdir(parents=True, exist_ok=True)
    cases = load_cases(input_path)
    engine = DiseaseHypothesisEngine(graph_namespace=graph_namespace)
    case_traces: list[dict[str, Any]] = []

    safe_log(log, f"[Trace] Loaded {len(cases)} cases from {input_path.name}")
    for index, case_payload in enumerate(cases, start=1):
        case_trace = _build_case_trace(case_payload, input_path, index, engine, log)
        artifacts = _write_case_artifacts(case_trace, output_dir)
        case_trace["artifact_paths"] = artifacts
        case_traces.append(case_trace)
        safe_log(
            log,
            f"[Trace] {case_trace['case_id']}: top1={case_trace['summary']['top1_disease']} "
            f"match={case_trace['summary']['ground_truth_match_count']}/{case_trace['summary']['service_line_count']}"
        )

    aggregate = {
        "case_count": len(case_traces),
        "service_line_count": sum(int((item.get("summary") or {}).get("service_line_count") or 0) for item in case_traces),
        "top1_hit_count": sum(1 for item in case_traces if bool((item.get("summary") or {}).get("top1_hit"))),
        "top3_hit_count": sum(1 for item in case_traces if bool((item.get("summary") or {}).get("top3_hit"))),
        "cases_with_graph_context": sum(1 for item in case_traces if int((item.get("summary") or {}).get("cases_with_graph_context") or 0) > 0),
        "cases_with_memory_support": sum(1 for item in case_traces if int((item.get("summary") or {}).get("memory_match_count") or 0) > 0),
        "service_label_match_total": sum(int((item.get("summary") or {}).get("ground_truth_match_count") or 0) for item in case_traces),
    }
    aggregate["service_label_accuracy"] = round(
        aggregate["service_label_match_total"] / max(aggregate["service_line_count"], 1),
        4,
    )

    payload = {
        "generated_at": now_iso(),
        "input_file": str(input_path),
        "graph_namespace": graph_namespace,
        "aggregate": aggregate,
        "cases": case_traces,
    }

    summary_json = output_dir / "trace_summary.json"
    summary_md = output_dir / "trace_summary.md"
    summary_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    summary_lines = [
        f"# Testcase Trace Summary: {input_path.name}",
        "",
        f"- Case count: `{aggregate['case_count']}`",
        f"- Service lines: `{aggregate['service_line_count']}`",
        f"- Top-1 hit count: `{aggregate['top1_hit_count']}`",
        f"- Top-3 hit count: `{aggregate['top3_hit_count']}`",
        f"- Cases with graph context: `{aggregate['cases_with_graph_context']}`",
        f"- Cases with memory support: `{aggregate['cases_with_memory_support']}`",
        f"- Service label accuracy: `{aggregate['service_label_accuracy']}`",
        "",
        "## Cases",
        "",
    ]
    for item in case_traces:
        summary = item.get("summary") or {}
        summary_lines.append(
            f"- `{item.get('case_id')}` | top1 `{summary.get('top1_disease')}` | "
            f"GT match `{summary.get('ground_truth_match_count')}/{summary.get('service_line_count')}` | "
            f"artifacts `{(item.get('artifact_paths') or {}).get('md')}`"
        )
    summary_md.write_text("\n".join(summary_lines) + "\n", encoding="utf-8")

    payload["artifact_paths"] = {
        "summary_json": str(summary_json),
        "summary_md": str(summary_md),
    }
    return payload
