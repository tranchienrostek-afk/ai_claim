from __future__ import annotations

import argparse
import json
import re
import unicodedata
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from adjudication_mvp import AdjudicationMVP
from lab_result_interpreter import summarize_lab_result_signals
from lab_result_to_disease_inference import LabResultDiseaseInferenceEngine
from protocol_semantic_support import ProtocolSemanticSupportEngine
from sign_to_service_engine import SignToServiceEngine
from tmh_step1_support import TMHStep1SupportEngine


PROJECT_DIR = Path(__file__).parent.parent
DEFAULT_INPUT_JSONL = PROJECT_DIR / "09_unified_story_testcase" / "label_gt_mined" / "data" / "all_service_lines_input.jsonl"
DEFAULT_OUTPUT_JSONL = PROJECT_DIR / "09_unified_story_testcase" / "step1_clinical_necessity" / "clinical_necessity_scored.jsonl"
DEFAULT_SUMMARY_JSON = PROJECT_DIR / "09_unified_story_testcase" / "step1_clinical_necessity" / "clinical_necessity_summary.json"
DEFAULT_CASESET_JSONL = PROJECT_DIR / "09_unified_story_testcase" / "parallel_merged" / "data" / "unified_case_testset.jsonl"
DEFAULT_OBSERVATION_JSONL = PROJECT_DIR / "05_observations" / "lab_observations.jsonl"

GENERIC_SIGNS = {
    "",
    "kham",
    "kham benh",
    "kham suc khoe",
    "tai kham",
    "xet nghiem",
    "can lam sang",
    "chan doan",
    "theo doi",
    "nhap vien",
    "yeu cau",
    "theo yeu cau",
    "nguoi benh yeu cau",
    "benh nhan yeu cau",
    "bat thuong suc khoe",
    "suc khoe",
    "tham kham",
    "den kham",
    "vao vien",
    "kham tong quat",
}

GENERIC_SIGN_SUBSTRINGS = {
    "yeu cau",
    "kham suc khoe",
    "bat thuong suc khoe",
    "kham tong quat",
}

SYMPTOM_HINT_TOKENS = {
    "au",
    "bung",
    "chay",
    "choang",
    "chong",
    "dam",
    "dau",
    "dom",
    "ho",
    "khan",
    "kho",
    "mat",
    "met",
    "mui",
    "ngua",
    "nguc",
    "non",
    "oi",
    "phoi",
    "sot",
    "sung",
    "tao",
    "tho",
    "tieu",
    "tuc",
    "xuat",
}

SCREENING_SERVICE_MARKERS = {
    "cholesterol",
    "triglycerid",
    "hdl",
    "ldl",
    "hbsag",
    "hbsab",
    "anti hbs",
    "anti hcv",
    "hcv",
    "hiv",
    "hpv",
}

SCREENING_CONTEXT_EXEMPTIONS = {
    "gan",
    "viem gan",
    "virus",
    "roi loan chuyen hoa lipid",
    "tang lipid",
    "lipoprotein",
    "mo mau",
    "dai thao duong",
    "tieu duong",
    "dtd",
    "dtd",
    "benh mach vanh",
    "nguy co tim mach",
    "tim mach",
    "tang huyet ap",
    "tha",
    "rllm",
    "nhoi mau nao",
    "dot quy",
    "tai bien",
    "co tu cung",
    "phu khoa",
    "thai san",
    "mang thai",
    "thai ky",
    "san khoa",
}

SERVICE_MATCH_STOPWORDS = {
    "",
    "dinh",
    "do",
    "hoat",
    "do",
    "luong",
    "phan",
    "tich",
    "test",
    "nhanh",
    "mau",
    "may",
    "theo",
    "yeu",
    "cau",
    "thuong",
    "bang",
    "tu",
    "dong",
    "xet",
    "nghiem",
}

PANEL_OBSERVATION_ALIASES = [
    {
        "service_markers": {"dien giai", "na k cl", "na k ci", "na, k, cl", "na, k, ci", "ion do"},
        "observation_aliases": {"na", "na+", "natri", "k", "k+", "kali", "cl", "ci", "cl-", "clorua"},
    },
    {
        "service_markers": {"tong phan tich nuoc tieu", "nuoc tieu"},
        "observation_aliases": {
            "ph",
            "protein",
            "glucose",
            "ketone",
            "nitrite",
            "bach cau",
            "hong cau",
            "leukocyte",
            "ery",
            "sg",
            "specific gravity",
            "bilirubin",
            "urobilinogen",
            "blood",
        },
    },
    {
        "service_markers": {"tong phan tich te bao mau", "te bao mau ngoai vi", "cong thuc mau", "huyet hoc"},
        "observation_aliases": {
            "wbc",
            "rbc",
            "hgb",
            "hb",
            "hct",
            "plt",
            "mcv",
            "mch",
            "mchc",
            "neu",
            "neut",
            "lym",
            "mono",
            "eos",
            "baso",
            "rdw",
            "mpv",
            "pct",
            "pdw",
        },
    },
]


@dataclass
class Step1Decision:
    decision: str
    confidence: float
    reason: str
    service_role: str
    linked_conditions: list[str]
    evidence_flags: list[str]


@dataclass
class LabSupportAssessment:
    supported: bool
    support_level: str
    observed: bool
    match_count: int
    best_match_score: float
    positive_count: int
    abnormal_count: int
    narrative_count: int
    negative_count: int
    normal_count: int
    has_positive_signal: bool
    has_negative_signal: bool
    has_abnormal_signal: bool
    has_conflicting_signals: bool
    matched_observations: list[dict[str, Any]]


@dataclass
class ResultDiseaseAssessment:
    supported: bool
    support_level: str
    best_score: float
    matched_primary_icd: bool
    matched_diagnosis: bool
    evidence_count: int
    top_diseases: list[dict[str, Any]]


def load_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with path.open(encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows


def write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")


def as_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    return str(value).strip()


def strip_diacritics(text: str) -> str:
    normalized = unicodedata.normalize("NFD", as_text(text))
    return "".join(char for char in normalized if unicodedata.category(char) != "Mn")


def clean_phrase(text: str) -> str:
    phrase = as_text(text)
    phrase = re.sub(r"\s+", " ", phrase)
    return phrase.strip(" .;,:-")


def normalize_phrase(text: str) -> str:
    phrase = strip_diacritics(clean_phrase(text)).lower()
    phrase = re.sub(r"[^a-z0-9 ]+", " ", phrase)
    phrase = re.sub(r"\s+", " ", phrase).strip()
    return phrase


def tokenize_service_match(text: str) -> set[str]:
    tokens: set[str] = set()
    for token in normalize_phrase(text).split():
        if token in SERVICE_MATCH_STOPWORDS or len(token) <= 1:
            continue
        tokens.add(token)
    return tokens


def split_segments(text: str) -> list[str]:
    if not text:
        return []
    pieces = re.split(r"[;\n|]+|(?<=\w)\.(?=\s|$)", text)
    segments: list[str] = []
    for piece in pieces:
        clean = clean_phrase(piece)
        if clean:
            segments.append(clean)
    return segments


def extract_medical_history_segments(text: str) -> list[str]:
    if not text:
        return []
    segments: list[str] = []
    for piece in re.split(r"[.;\n|]+", text):
        clean = clean_phrase(piece)
        if clean:
            segments.append(clean)
    return segments


def is_generic_sign(normalized: str) -> bool:
    if not normalized:
        return True
    if normalized in GENERIC_SIGNS:
        return True
    return any(fragment in normalized for fragment in GENERIC_SIGN_SUBSTRINGS)


def looks_like_clinical_sign(normalized: str) -> bool:
    if not normalized:
        return False
    tokens = normalized.split()
    if any(token in SYMPTOM_HINT_TOKENS for token in tokens):
        return True
    if len(tokens) >= 3 and any(len(token) >= 5 for token in tokens):
        return True
    return False


def extract_sign_candidates(case_row: dict[str, Any]) -> list[str]:
    claim = case_row.get("claim_info_merged", {}) or {}
    admission = case_row.get("admission_context", {}) or {}
    medical_history = as_text(admission.get("medical_history_enriched")) or as_text(claim.get("medical_history"))

    raw_texts = [
        as_text(claim.get("symptom")),
        as_text(claim.get("admission_reason")),
        as_text(admission.get("visit_reason_enriched")),
    ]
    generic_or_empty = [text for text in raw_texts if not text or is_generic_sign(normalize_phrase(text))]
    should_expand_history = len(generic_or_empty) == len(raw_texts)
    if medical_history and should_expand_history:
        raw_texts.extend(extract_medical_history_segments(medical_history)[:6])

    seen: set[str] = set()
    signs: list[str] = []

    for raw_text in raw_texts:
        for segment in split_segments(raw_text):
            normalized = normalize_phrase(segment)
            if not normalized or is_generic_sign(normalized):
                continue
            if len(normalized) < 3 or len(normalized) > 80:
                continue
            if not looks_like_clinical_sign(normalized):
                continue
            if normalized in seen:
                continue
            seen.add(normalized)
            signs.append(segment)

    return signs


def build_case_sign_lookup(path: Path) -> dict[str, list[str]]:
    lookup: dict[str, list[str]] = {}
    for row in load_jsonl(path):
        message_hash_id = as_text(row.get("message_hash_id"))
        if not message_hash_id:
            continue
        signs = extract_sign_candidates(row)
        if signs:
            lookup[message_hash_id] = signs
    return lookup


def observation_signature(row: dict[str, Any]) -> tuple[str, ...]:
    return (
        as_text(row.get("service_raw_name")),
        as_text(row.get("service_code")),
        as_text(row.get("service_canonical_name")),
        as_text(row.get("observation_concept_code")),
        as_text(row.get("observation_concept_name")),
        as_text(row.get("observation_node_code")),
        as_text(row.get("result_flag")),
        as_text(row.get("polarity")),
        as_text(row.get("abnormality")),
        as_text(row.get("result_raw")),
        as_text(row.get("reference_range_raw")),
    )


def build_case_observation_lookup(path: Path) -> dict[str, dict[str, Any]]:
    lookup: dict[str, dict[str, Any]] = {}
    if not path.exists():
        return lookup
    seen_by_case: dict[str, set[tuple[str, ...]]] = defaultdict(set)
    with path.open(encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            row = json.loads(line)
            message_hash_id = as_text(row.get("message_id_hash"))
            if not message_hash_id:
                continue
            compact = {
                "service_raw_name": as_text(row.get("service_raw_name")),
                "service_code": as_text(row.get("service_code")),
                "service_canonical_name": as_text(row.get("service_canonical_name")),
                "observation_concept_code": as_text(row.get("observation_concept_code")),
                "observation_concept_name": as_text(row.get("observation_concept_name")),
                "observation_node_code": as_text(row.get("observation_node_code")),
                "result_flag": as_text(row.get("result_flag")),
                "polarity": as_text(row.get("polarity")),
                "abnormality": as_text(row.get("abnormality")),
                "result_raw": as_text(row.get("result_raw")),
                "reference_range_raw": as_text(row.get("reference_range_raw")),
            }
            signature = observation_signature(compact)
            if signature in seen_by_case[message_hash_id]:
                continue
            seen_by_case[message_hash_id].add(signature)

            raw_texts = [
                compact["service_raw_name"],
                compact["service_canonical_name"],
                compact["observation_concept_name"],
            ]
            normalized_texts = {normalize_phrase(text) for text in raw_texts if normalize_phrase(text)}
            tokens = set()
            for text in raw_texts:
                tokens.update(tokenize_service_match(text))
            compact["_normalized_texts"] = sorted(normalized_texts)
            compact["_tokens"] = sorted(tokens)

            case_entry = lookup.setdefault(
                message_hash_id,
                {
                    "observations": [],
                    "by_service_code": defaultdict(list),
                    "by_normalized_text": defaultdict(list),
                    "by_token": defaultdict(list),
                },
            )
            obs_idx = len(case_entry["observations"])
            case_entry["observations"].append(compact)
            if compact["service_code"]:
                case_entry["by_service_code"][compact["service_code"]].append(obs_idx)
            for normalized in compact["_normalized_texts"]:
                case_entry["by_normalized_text"][normalized].append(obs_idx)
            for token in compact["_tokens"]:
                case_entry["by_token"][token].append(obs_idx)
    return lookup


class ClinicalNecessityStep1Engine:
    def __init__(
        self,
        case_input_path: Path = DEFAULT_CASESET_JSONL,
        observation_input_path: Path = DEFAULT_OBSERVATION_JSONL,
    ) -> None:
        self.mvp = AdjudicationMVP()
        self.sign_engine = SignToServiceEngine()
        self.result_disease_engine = LabResultDiseaseInferenceEngine()
        self.protocol_semantic_engine = ProtocolSemanticSupportEngine()
        self.tmh_step1_engine = TMHStep1SupportEngine()
        self.case_sign_lookup = build_case_sign_lookup(case_input_path)
        self.case_observation_lookup = build_case_observation_lookup(observation_input_path)
        self._service_cache: dict[str, dict[str, Any]] = {}
        self._clinical_cache: dict[tuple[str, str, str], Any] = {}
        self._sign_inference_cache: dict[tuple[str, ...], dict[str, Any]] = {}
        self._lab_support_cache: dict[tuple[str, str, str], LabSupportAssessment] = {}
        self._result_disease_cache: dict[tuple[str, str, str, str], ResultDiseaseAssessment] = {}

    @staticmethod
    def _has_direct_final_diagnosis_support(clinical: Any) -> bool:
        if not getattr(clinical, "justified", False):
            return False
        linked_conditions = getattr(clinical, "linked_conditions", []) or []
        if linked_conditions:
            return True
        reason = as_text(getattr(clinical, "reason", ""))
        return "Matched service-disease matrix" in reason

    @staticmethod
    def _is_screening_like_without_context(
        service_name: str,
        service_info: dict[str, Any],
        diagnosis: str,
        initial_signs: list[str],
        final_diagnosis_supported: bool,
        sign_supported: bool,
    ) -> bool:
        if final_diagnosis_supported or sign_supported or initial_signs:
            return False
        service_text = normalize_phrase(
            as_text(service_info.get("canonical_name")) or service_name
        )
        diagnosis_text = normalize_phrase(diagnosis)
        if not service_text:
            return False
        has_screening_marker = any(marker in service_text for marker in SCREENING_SERVICE_MARKERS)
        if not has_screening_marker:
            return False
        if any(exemption in diagnosis_text for exemption in SCREENING_CONTEXT_EXEMPTIONS):
            return False
        return True

    @staticmethod
    def _panel_aliases_for_service(service_name: str, service_info: dict[str, Any]) -> set[str]:
        service_text = normalize_phrase(as_text(service_info.get("canonical_name")) or service_name)
        aliases: set[str] = set()
        if not service_text:
            return aliases
        for rule in PANEL_OBSERVATION_ALIASES:
            if any(marker in service_text for marker in rule["service_markers"]):
                aliases.update(rule["observation_aliases"])
        return aliases

    @staticmethod
    def _service_to_observation_match_score(
        service_name: str,
        service_info: dict[str, Any],
        observation: dict[str, Any],
        panel_aliases: set[str] | None = None,
    ) -> float:
        service_code = as_text(service_info.get("service_code"))
        if service_code and service_code == as_text(observation.get("service_code")):
            return 1.0

        service_texts = [
            service_name,
            as_text(service_info.get("canonical_name")),
        ]
        observation_texts = [
            as_text(observation.get("service_raw_name")),
            as_text(observation.get("service_canonical_name")),
            as_text(observation.get("observation_concept_name")),
        ]
        observation_normalized = set(observation.get("_normalized_texts") or [])
        observation_tokens = set(observation.get("_tokens") or [])

        if panel_aliases and (panel_aliases & observation_tokens or panel_aliases & observation_normalized):
            return 0.82

        best_score = 0.0
        for service_text in service_texts:
            service_norm = normalize_phrase(service_text)
            service_tokens = tokenize_service_match(service_text)
            if not service_norm:
                continue
            for observation_text in observation_texts:
                observation_norm = normalize_phrase(observation_text)
                if not observation_norm:
                    continue
                if service_norm == observation_norm:
                    best_score = max(best_score, 0.98)
                    continue
                if min(len(service_norm), len(observation_norm)) >= 5 and (
                    service_norm in observation_norm or observation_norm in service_norm
                ):
                    best_score = max(best_score, 0.9)
                    continue
                overlap = service_tokens & observation_tokens
                if not overlap:
                    continue
                coverage = len(overlap) / max(len(service_tokens), 1)
                reverse_coverage = len(overlap) / max(len(observation_tokens), 1)
                score = 0.35 + 0.35 * coverage + 0.3 * reverse_coverage
                best_score = max(best_score, score)
        return round(best_score, 4)

    def _assess_lab_support(self, message_hash_id: str, service_name: str, service_info: dict[str, Any]) -> LabSupportAssessment:
        cache_key = (
            message_hash_id,
            as_text(service_info.get("service_code")),
            normalize_phrase(as_text(service_info.get("canonical_name")) or service_name),
        )
        cached = self._lab_support_cache.get(cache_key)
        if cached is not None:
            return cached

        case_entry = self.case_observation_lookup.get(message_hash_id)
        if not case_entry:
            result = LabSupportAssessment(
                supported=False,
                support_level="none",
                observed=False,
                match_count=0,
                best_match_score=0.0,
                positive_count=0,
                abnormal_count=0,
                narrative_count=0,
                negative_count=0,
                normal_count=0,
                has_positive_signal=False,
                has_negative_signal=False,
                has_abnormal_signal=False,
                has_conflicting_signals=False,
                matched_observations=[],
            )
            self._lab_support_cache[cache_key] = result
            return result

        observations = case_entry.get("observations", [])
        candidate_indices: set[int] = set()
        service_code = as_text(service_info.get("service_code"))
        if service_code:
            candidate_indices.update(case_entry.get("by_service_code", {}).get(service_code, []))

        service_texts = [service_name, as_text(service_info.get("canonical_name"))]
        for service_text in service_texts:
            service_norm = normalize_phrase(service_text)
            if service_norm:
                candidate_indices.update(case_entry.get("by_normalized_text", {}).get(service_norm, []))

        token_hits: Counter[int] = Counter()
        service_tokens = set()
        for service_text in service_texts:
            service_tokens.update(tokenize_service_match(service_text))
        for token in service_tokens:
            for idx in case_entry.get("by_token", {}).get(token, []):
                token_hits[idx] += 1
        min_token_hits = 1 if len(service_tokens) <= 2 else 2
        for idx, hit_count in token_hits.items():
            if hit_count >= min_token_hits:
                candidate_indices.add(idx)

        panel_aliases = self._panel_aliases_for_service(service_name, service_info)
        for alias in panel_aliases:
            candidate_indices.update(case_entry.get("by_normalized_text", {}).get(alias, []))
            for idx in case_entry.get("by_token", {}).get(alias, []):
                candidate_indices.add(idx)

        if not candidate_indices:
            candidate_indices = set(range(len(observations)))

        matched: list[dict[str, Any]] = []
        for idx in sorted(candidate_indices):
            observation = observations[idx]
            match_score = self._service_to_observation_match_score(
                service_name,
                service_info,
                observation,
                panel_aliases=panel_aliases,
            )
            if match_score < 0.58:
                continue
            enriched = dict(observation)
            enriched["match_score"] = match_score
            matched.append(enriched)

        matched.sort(key=lambda item: item.get("match_score", 0.0), reverse=True)
        matched = matched[:6]
        if not matched:
            result = LabSupportAssessment(
                supported=False,
                support_level="none",
                observed=False,
                match_count=0,
                best_match_score=0.0,
                positive_count=0,
                abnormal_count=0,
                narrative_count=0,
                negative_count=0,
                normal_count=0,
                has_positive_signal=False,
                has_negative_signal=False,
                has_abnormal_signal=False,
                has_conflicting_signals=False,
                matched_observations=[],
            )
            self._lab_support_cache[cache_key] = result
            return result

        signal_summary = summarize_lab_result_signals(matched)
        best_match_score = float(matched[0].get("match_score") or 0.0)

        result = LabSupportAssessment(
            supported=signal_summary.supported,
            support_level=signal_summary.support_level,
            observed=signal_summary.observed,
            match_count=len(matched),
            best_match_score=round(best_match_score, 4),
            positive_count=signal_summary.positive_count,
            abnormal_count=signal_summary.abnormal_count,
            narrative_count=signal_summary.narrative_count,
            negative_count=signal_summary.negative_count,
            normal_count=signal_summary.normal_count,
            has_positive_signal=signal_summary.has_positive_signal,
            has_negative_signal=signal_summary.has_negative_signal,
            has_abnormal_signal=signal_summary.has_abnormal_signal,
            has_conflicting_signals=signal_summary.has_conflicting_signals,
            matched_observations=[
                {
                    "service_raw_name": item.get("service_raw_name"),
                    "observation_concept_name": item.get("observation_concept_name"),
                    "result_flag": item.get("result_flag"),
                    "polarity": item.get("polarity"),
                    "abnormality": item.get("abnormality"),
                    "result_raw": item.get("result_raw"),
                    "match_score": item.get("match_score"),
                }
                for item in matched
            ],
        )
        self._lab_support_cache[cache_key] = result
        return result

    @staticmethod
    def _disease_name_matches_diagnosis(disease_name: str, diagnosis: str) -> bool:
        disease_norm = normalize_phrase(disease_name)
        diagnosis_norm = normalize_phrase(diagnosis)
        if not disease_norm or not diagnosis_norm:
            return False
        if disease_norm in diagnosis_norm or diagnosis_norm in disease_norm:
            return True
        disease_tokens = {token for token in disease_norm.split() if len(token) >= 3}
        diagnosis_tokens = {token for token in diagnosis_norm.split() if len(token) >= 3}
        if len(disease_tokens) < 2 or len(diagnosis_tokens) < 2:
            return False
        overlap = disease_tokens & diagnosis_tokens
        return len(overlap) >= 2 and len(overlap) / max(len(disease_tokens), 1) >= 0.5

    def _assess_result_disease_support(
        self,
        lab_support: LabSupportAssessment,
        diagnosis: str,
        primary_icd: str,
        service_info: dict[str, Any],
    ) -> ResultDiseaseAssessment:
        cache_key = (
            normalize_phrase(as_text(service_info.get("service_code")) or as_text(service_info.get("canonical_name"))),
            normalize_phrase(diagnosis),
            as_text(primary_icd),
            json.dumps(lab_support.matched_observations, ensure_ascii=False, sort_keys=True),
        )
        cached = self._result_disease_cache.get(cache_key)
        if cached is not None:
            return cached

        if not lab_support.observed or not lab_support.matched_observations:
            result = ResultDiseaseAssessment(
                supported=False,
                support_level="none",
                best_score=0.0,
                matched_primary_icd=False,
                matched_diagnosis=False,
                evidence_count=0,
                top_diseases=[],
            )
            self._result_disease_cache[cache_key] = result
            return result

        inferred = self.result_disease_engine.infer(lab_support.matched_observations, top_k=5)
        top_diseases: list[dict[str, Any]] = []
        matched_primary_icd = False
        matched_diagnosis = False
        best_score = 0.0

        for item in inferred:
            disease_row = {
                "disease_key": item.disease_key,
                "disease_name": item.disease_name,
                "icd10": item.icd10,
                "score": item.score,
                "support_score": item.support_score,
                "exclusion_score": item.exclusion_score,
                "evidence_count": item.evidence_count,
                "strongest_level": item.strongest_level,
                "evidences": item.evidences[:3],
            }
            top_diseases.append(disease_row)
            best_score = max(best_score, float(item.score))
            if primary_icd and as_text(item.icd10) == as_text(primary_icd) and item.score > 0:
                matched_primary_icd = True
            if self._disease_name_matches_diagnosis(item.disease_name, diagnosis) and item.score > 0:
                matched_diagnosis = True

        if matched_primary_icd and best_score >= 1.0:
            support_level = "strong"
            supported = True
        elif matched_primary_icd or (matched_diagnosis and best_score >= 0.8):
            support_level = "moderate"
            supported = True
        elif top_diseases:
            support_level = "observed"
            supported = False
        else:
            support_level = "none"
            supported = False

        result = ResultDiseaseAssessment(
            supported=supported,
            support_level=support_level,
            best_score=round(best_score, 4),
            matched_primary_icd=matched_primary_icd,
            matched_diagnosis=matched_diagnosis,
            evidence_count=sum(len(item["evidences"]) for item in top_diseases),
            top_diseases=top_diseases,
        )
        self._result_disease_cache[cache_key] = result
        return result

    def _infer_from_signs(self, signs: list[str]) -> dict[str, Any] | None:
        if not signs:
            return None
        cache_key = tuple(normalize_phrase(sign) for sign in signs if normalize_phrase(sign))
        if not cache_key:
            return None
        cached = self._sign_inference_cache.get(cache_key)
        if cached is not None:
            return cached
        inferred = self.sign_engine.infer_from_signs(signs)
        self._sign_inference_cache[cache_key] = inferred
        return inferred

    @staticmethod
    def _assess_sign_support(service_info: dict[str, Any], sign_inference: dict[str, Any] | None) -> dict[str, Any]:
        if not sign_inference:
            return {
                "support_level": "none",
                "supported": False,
                "score": 0.0,
                "service_rank": None,
                "matched_service": None,
                "matched_signs": [],
                "suspected_diseases": [],
            }

        service_code = as_text(service_info.get("service_code"))
        recommended = sign_inference.get("recommended_services") or []
        if not service_code:
            return {
                "support_level": "none",
                "supported": False,
                "score": 0.0,
                "service_rank": None,
                "matched_service": None,
                "matched_signs": sign_inference.get("matched_signs", [])[:4],
                "suspected_diseases": sign_inference.get("suspected_diseases", [])[:5],
            }

        matched_service = None
        for idx, service in enumerate(recommended):
            if as_text(service.get("service_code")) == service_code:
                matched_service = dict(service)
                matched_service["service_rank"] = idx + 1
                break

        if matched_service is None:
            return {
                "support_level": "none",
                "supported": False,
                "score": 0.0,
                "service_rank": None,
                "matched_service": None,
                "matched_signs": sign_inference.get("matched_signs", [])[:4],
                "suspected_diseases": sign_inference.get("suspected_diseases", [])[:5],
            }

        top_score = float((recommended[0] or {}).get("score") or 0.0) if recommended else 0.0
        raw_score = float(matched_service.get("score") or 0.0)
        relative_score = raw_score / top_score if top_score > 0 else 0.0
        rank = matched_service["service_rank"]

        if rank <= 8 and relative_score >= 0.35:
            support_level = "strong"
            supported = True
        elif rank <= 12 or relative_score >= 0.2:
            support_level = "moderate"
            supported = True
        else:
            support_level = "weak"
            supported = False

        return {
            "support_level": support_level,
            "supported": supported,
            "score": round(raw_score, 4),
            "relative_score": round(relative_score, 4),
            "service_rank": rank,
            "matched_service": matched_service,
            "matched_signs": sign_inference.get("matched_signs", [])[:4],
            "suspected_diseases": sign_inference.get("suspected_diseases", [])[:5],
        }

    def assess_row(self, row: dict[str, Any]) -> dict[str, Any]:
        service_name = str(row.get("service_name_raw") or "")
        diagnosis = str(row.get("diagnosis_text_enriched") or "")
        icd_code = str(row.get("primary_icd") or "")
        row_service_info = row.get("recognized_service")
        service_info = (
            row_service_info
            if isinstance(row_service_info, dict) and row_service_info.get("service_code")
            else None
        )
        if service_info is None:
            service_info = self._service_cache.get(service_name)
        if service_info is None:
            service_info = self.mvp.recognize_service(service_name)
            self._service_cache[service_name] = service_info

        clinical_key = (service_name, diagnosis, icd_code)
        clinical = self._clinical_cache.get(clinical_key)
        if clinical is None:
            clinical = self.mvp.assess_clinical_necessity(
                service_name=service_name,
                service_info=service_info,
                diagnosis=diagnosis,
                icd_code=icd_code,
            )
            self._clinical_cache[clinical_key] = clinical

        message_hash_id = as_text(row.get("message_hash_id"))
        initial_signs = self.case_sign_lookup.get(message_hash_id, [])
        chief_complaint = as_text(row.get("chief_complaint"))
        specialty = as_text(row.get("specialty"))
        sign_inference = self._infer_from_signs(initial_signs)
        sign_support = self._assess_sign_support(service_info, sign_inference)
        lab_support = self._assess_lab_support(message_hash_id, service_name, service_info)
        result_disease_support = self._assess_result_disease_support(
            lab_support=lab_support,
            diagnosis=diagnosis,
            primary_icd=icd_code,
            service_info=service_info,
        )
        protocol_semantic_support = self.protocol_semantic_engine.assess(
            service_name=service_name,
            service_info=service_info,
            icd_code=icd_code,
            diagnosis_text=diagnosis,
        )
        tmh_support = self.tmh_step1_engine.assess(
            service_name=service_name,
            diagnosis_text=diagnosis,
            primary_icd=icd_code,
            chief_complaint=chief_complaint,
            initial_signs=initial_signs,
            specialty=specialty,
        )

        evidence_flags: list[str] = []
        mapping_status = as_text(service_info.get("mapping_status")) or (
            "probable" if service_info.get("service_code") else "unknown"
        )
        if row.get("has_icd_context"):
            evidence_flags.append("has_icd_context")
        if row.get("has_admission_context"):
            evidence_flags.append("has_admission_context")
        if row.get("has_lab_docs"):
            evidence_flags.append("has_lab_docs")
        if row.get("has_lab_observations"):
            evidence_flags.append("has_lab_observations")
        if mapping_status:
            evidence_flags.append(f"service_mapping_{mapping_status}")
        if service_info.get("service_code"):
            evidence_flags.append("service_recognized")
        if initial_signs:
            evidence_flags.append("has_initial_signs")
        if sign_support["supported"]:
            evidence_flags.append("supported_by_initial_signs")
        elif sign_support["support_level"] == "weak":
            evidence_flags.append("weak_sign_support")
        if lab_support.observed:
            evidence_flags.append("matched_lab_result")
        if lab_support.supported:
            evidence_flags.append("supported_by_lab_results")
        if result_disease_support.top_diseases:
            evidence_flags.append("matched_result_disease_inference")
        if result_disease_support.supported:
            evidence_flags.append("supported_by_result_disease")
        if protocol_semantic_support.supported:
            evidence_flags.append("matched_protocol_semantic")
            evidence_flags.append("supported_by_protocol_semantic")
        if tmh_support.supported:
            evidence_flags.append("supported_by_tmh_structured")
        if tmh_support.unsupported:
            evidence_flags.append("blocked_by_tmh_structured")

        final_diagnosis_supported = self._has_direct_final_diagnosis_support(clinical)
        has_structured_context = bool(
            row.get("has_admission_context")
            or row.get("has_lab_docs")
            or row.get("has_lab_observations")
            or initial_signs
        )
        fallback_clinical = bool(clinical.justified and not final_diagnosis_supported)
        screening_without_context = self._is_screening_like_without_context(
            service_name=service_name,
            service_info=service_info,
            diagnosis=diagnosis,
            initial_signs=initial_signs,
            final_diagnosis_supported=final_diagnosis_supported,
            sign_supported=sign_support["supported"],
        )

        if final_diagnosis_supported or sign_support["supported"] or lab_support.supported or result_disease_support.supported or protocol_semantic_support.supported or tmh_support.supported:
            if final_diagnosis_supported and sign_support["supported"]:
                medical_necessity_status = "supported_by_both"
                medical_reason = (
                    f"Supported by final diagnosis and initial signs. Diagnosis reasoning: {clinical.reason}"
                )
                decision = "JUSTIFIED"
                confidence = max(float(clinical.confidence), 0.8)
                if lab_support.supported:
                    medical_reason += " Lab result also provided supportive evidence."
                    confidence = max(confidence, 0.84)
                if protocol_semantic_support.supported:
                    medical_reason += " Protocol semantic retrieval also matched the disease paraclinical section."
                    confidence = max(confidence, 0.86)
            elif final_diagnosis_supported:
                medical_necessity_status = "supported_by_final_diagnosis"
                medical_reason = clinical.reason
                decision = "JUSTIFIED" if clinical.confidence >= 0.75 else "UNCERTAIN"
                confidence = float(clinical.confidence)
                if lab_support.supported:
                    medical_reason += " Lab result matched the service and was abnormal/positive."
                    decision = "JUSTIFIED"
                    confidence = max(confidence, 0.8)
                if protocol_semantic_support.supported:
                    top_hit = protocol_semantic_support.parent_hits[0]
                    medical_reason += (
                        f" Protocol semantic retrieval matched section '{top_hit['section_title']}'"
                        f" for ICD {top_hit['icd10']}."
                    )
                    confidence = max(confidence, 0.82)
            elif lab_support.supported:
                medical_necessity_status = "supported_by_lab_results"
                medical_reason = (
                    f"Matched lab result(s) for this service with support level {lab_support.support_level}: "
                    f"{lab_support.positive_count} positive, {lab_support.abnormal_count} abnormal, "
                    f"{lab_support.negative_count} negative, {lab_support.narrative_count} narrative."
                )
                decision = "JUSTIFIED" if lab_support.support_level == "strong" else "UNCERTAIN"
                confidence = 0.82 if lab_support.support_level == "strong" else 0.72
                if protocol_semantic_support.supported:
                    medical_reason += " Protocol semantic retrieval also found a matching paraclinical section."
                    confidence = max(confidence, 0.78)
            elif result_disease_support.supported:
                medical_necessity_status = "supported_by_result_disease"
                top_hit = result_disease_support.top_diseases[0] if result_disease_support.top_diseases else {}
                medical_reason = (
                    f"Lab result signals support disease inference toward '{top_hit.get('disease_name', '')}'"
                    f" ({top_hit.get('icd10', '')}) with score {result_disease_support.best_score}."
                )
                decision = "JUSTIFIED" if result_disease_support.support_level == "strong" else "UNCERTAIN"
                confidence = 0.8 if result_disease_support.support_level == "strong" else 0.7
                if result_disease_support.matched_primary_icd:
                    medical_reason += " Top inferred disease matches the claim primary ICD."
                    confidence = max(confidence, 0.82)
                elif result_disease_support.matched_diagnosis:
                    medical_reason += " Top inferred disease text also aligns with diagnosis text."
                    confidence = max(confidence, 0.75)
            elif protocol_semantic_support.supported:
                medical_necessity_status = "supported_by_protocol_semantic"
                top_hit = protocol_semantic_support.parent_hits[0]
                medical_reason = (
                    f"Protocol semantic retrieval matched section '{top_hit['section_title']}' "
                    f"for disease '{top_hit['disease_name'] or top_hit['disease_title']}'"
                    f" with score {protocol_semantic_support.best_score}."
                )
                decision = "JUSTIFIED" if protocol_semantic_support.support_level == "strong" else "UNCERTAIN"
                confidence = 0.78 if protocol_semantic_support.support_level == "strong" else 0.68
            elif tmh_support.supported:
                medical_necessity_status = "supported_by_tmh_structured"
                medical_reason = tmh_support.reason
                decision = "JUSTIFIED" if tmh_support.support_level == "strong" else "UNCERTAIN"
                confidence = 0.8 if tmh_support.support_level == "strong" else 0.72
            else:
                medical_necessity_status = "supported_by_initial_signs"
                sign_reason_parts = []
                if sign_support["matched_signs"]:
                    sign_reason_parts.append(
                        "Initial signs matched: "
                        + ", ".join(item.get("input_sign") or "" for item in sign_support["matched_signs"][:3] if item.get("input_sign"))
                    )
                if sign_support["matched_service"]:
                    sign_reason_parts.append(
                        f"Service ranked #{sign_support['service_rank']} in sign-based recommendation set"
                    )
                medical_reason = "; ".join(part for part in sign_reason_parts if part) or "Supported by sign-to-service inference."
                decision = "JUSTIFIED" if sign_support["support_level"] == "strong" else "UNCERTAIN"
                confidence = max(float(clinical.confidence), 0.72 if sign_support["support_level"] == "strong" else 0.58)
                if lab_support.supported:
                    medical_reason += " Lab result matched the same service and was supportive."
                    confidence = max(confidence, 0.76)
                if protocol_semantic_support.supported:
                    medical_reason += " Protocol semantic retrieval also supported the same service."
                    confidence = max(confidence, 0.78)

            step1 = Step1Decision(
                decision=decision,
                confidence=round(confidence, 3),
                reason=medical_reason,
                service_role=clinical.role,
                linked_conditions=clinical.linked_conditions,
                evidence_flags=evidence_flags,
            )
        elif tmh_support.unsupported and not final_diagnosis_supported and not sign_support["supported"] and not lab_support.supported:
            unsupported_reason = tmh_support.reason
            step1 = Step1Decision(
                decision="UNJUSTIFIED",
                confidence=0.82 if tmh_support.support_level == "strong" else 0.72,
                reason=unsupported_reason,
                service_role=clinical.role,
                linked_conditions=clinical.linked_conditions,
                evidence_flags=evidence_flags + ["tmh_structured_contraindication"],
            )
            medical_necessity_status = "not_medically_supported"
        elif screening_without_context:
            unsupported_reason = (
                "Screening-like service has no support from final diagnosis or initial signs."
            )
            step1 = Step1Decision(
                decision="UNJUSTIFIED",
                confidence=0.78,
                reason=unsupported_reason,
                service_role=clinical.role,
                linked_conditions=clinical.linked_conditions,
                evidence_flags=evidence_flags + ["screening_without_context"],
            )
            medical_necessity_status = "not_medically_supported"
        elif has_structured_context or fallback_clinical or sign_support["support_level"] == "weak" or lab_support.observed:
            uncertain_reason = clinical.reason
            if fallback_clinical:
                uncertain_reason = (
                    "No direct service-disease matrix support from final diagnosis; kept as uncertain because some clinical context exists."
                )
            if lab_support.observed and not lab_support.supported:
                uncertain_reason = (
                    "Matched lab result exists for this service, but result pattern is normal/negative and does not strongly support medical necessity."
                )
            elif not uncertain_reason:
                uncertain_reason = "Clinical context exists but no direct support from final diagnosis or initial signs."
            step1 = Step1Decision(
                decision="UNCERTAIN",
                confidence=round(max(float(clinical.confidence), 0.45), 3),
                reason=uncertain_reason,
                service_role=clinical.role,
                linked_conditions=clinical.linked_conditions,
                evidence_flags=evidence_flags,
            )
            medical_necessity_status = "uncertain"
        else:
            unsupported_reason = clinical.reason or "No support from final diagnosis, initial signs, or nearby clinical context."
            step1 = Step1Decision(
                decision="UNJUSTIFIED",
                confidence=round(float(clinical.confidence), 3),
                reason=unsupported_reason,
                service_role=clinical.role,
                linked_conditions=clinical.linked_conditions,
                evidence_flags=evidence_flags,
            )
            medical_necessity_status = "not_medically_supported"

        return {
            "benchmark_id": row.get("benchmark_id"),
            "message_hash_id": row.get("message_hash_id"),
            "claim_line_id": row.get("claim_line_id"),
            "so_hoso_boithuong": row.get("so_hoso_boithuong"),
            "service_name_raw": service_name,
            "amount_vnd": row.get("amount_vnd"),
            "diagnosis_text_enriched": diagnosis,
            "primary_icd": icd_code,
            "initial_signs": initial_signs,
            "recognized_service": service_info,
            "step1_clinical_necessity": {
                "decision": step1.decision,
                "confidence": step1.confidence,
                "reason": step1.reason,
                "service_role": step1.service_role,
                "linked_conditions": step1.linked_conditions,
                "evidence_flags": step1.evidence_flags,
                "medical_necessity_status": medical_necessity_status,
                "support_by_final_diagnosis": {
                    "supported": bool(clinical.justified),
                    "confidence": round(float(clinical.confidence), 3),
                    "reason": clinical.reason,
                    "linked_conditions": clinical.linked_conditions,
                },
                "support_by_initial_signs": {
                    "supported": sign_support["supported"],
                    "support_level": sign_support["support_level"],
                    "score": sign_support["score"],
                    "relative_score": sign_support.get("relative_score", 0.0),
                    "service_rank": sign_support["service_rank"],
                    "matched_signs": sign_support["matched_signs"],
                    "suspected_diseases": sign_support["suspected_diseases"],
                },
                "support_by_lab_results": {
                    "supported": lab_support.supported,
                    "support_level": lab_support.support_level,
                    "observed": lab_support.observed,
                    "match_count": lab_support.match_count,
                    "best_match_score": lab_support.best_match_score,
                    "positive_count": lab_support.positive_count,
                    "abnormal_count": lab_support.abnormal_count,
                    "narrative_count": lab_support.narrative_count,
                    "negative_count": lab_support.negative_count,
                    "normal_count": lab_support.normal_count,
                    "has_positive_signal": lab_support.has_positive_signal,
                    "has_negative_signal": lab_support.has_negative_signal,
                    "has_abnormal_signal": lab_support.has_abnormal_signal,
                    "has_conflicting_signals": lab_support.has_conflicting_signals,
                    "matched_observations": lab_support.matched_observations,
                },
                "support_by_result_disease": {
                    "supported": result_disease_support.supported,
                    "support_level": result_disease_support.support_level,
                    "best_score": result_disease_support.best_score,
                    "matched_primary_icd": result_disease_support.matched_primary_icd,
                    "matched_diagnosis": result_disease_support.matched_diagnosis,
                    "evidence_count": result_disease_support.evidence_count,
                    "top_diseases": result_disease_support.top_diseases,
                },
                "support_by_protocol_semantic": {
                    "supported": protocol_semantic_support.supported,
                    "support_level": protocol_semantic_support.support_level,
                    "best_score": protocol_semantic_support.best_score,
                    "retrieval_modes": protocol_semantic_support.retrieval_modes,
                    "parent_hits": protocol_semantic_support.parent_hits,
                },
                "support_by_tmh_structured": {
                    "supported": tmh_support.supported,
                    "unsupported": tmh_support.unsupported,
                    "support_level": tmh_support.support_level,
                    "reason": tmh_support.reason,
                    "source": tmh_support.source,
                    "matched_disease": tmh_support.matched_disease,
                    "matched_service": tmh_support.matched_service,
                },
            },
        }

    def score_jsonl(self, input_path: Path, output_path: Path, summary_path: Path) -> dict[str, Any]:
        rows = load_jsonl(input_path)
        scored_rows = [self.assess_row(row) for row in rows]
        write_jsonl(output_path, scored_rows)

        counter = Counter(row["step1_clinical_necessity"]["decision"] for row in scored_rows)
        medical_status_counter = Counter(row["step1_clinical_necessity"]["medical_necessity_status"] for row in scored_rows)
        input_flag_counts = {
            "has_icd_context": sum(1 for row in rows if row.get("has_icd_context")),
            "has_admission_context": sum(1 for row in rows if row.get("has_admission_context")),
            "has_lab_docs": sum(1 for row in rows if row.get("has_lab_docs")),
            "has_lab_observations": sum(1 for row in rows if row.get("has_lab_observations")),
        }
        result_like_keys = sorted({key for row in rows for key in row.keys() if key.startswith("result")})
        summary = {
            "generated_at": datetime.now().astimezone().isoformat(),
            "input_path": str(input_path),
            "output_path": str(output_path),
            "total_rows": len(scored_rows),
            "input_flag_counts": input_flag_counts,
            "rows_with_extracted_signs": sum(1 for row in scored_rows if row.get("initial_signs")),
            "input_result_like_keys": result_like_keys,
            "decision_distribution": dict(counter),
            "decision_distribution_pct": {
                key: round(100.0 * value / max(len(scored_rows), 1), 2) for key, value in counter.items()
            },
            "medical_necessity_status_distribution": dict(medical_status_counter),
            "medical_necessity_status_distribution_pct": {
                key: round(100.0 * value / max(len(scored_rows), 1), 2) for key, value in medical_status_counter.items()
            },
            "rows_supported_by_initial_signs": sum(
                1 for row in scored_rows if row["step1_clinical_necessity"]["support_by_initial_signs"]["supported"]
            ),
            "rows_supported_by_lab_results": sum(
                1 for row in scored_rows if row["step1_clinical_necessity"]["support_by_lab_results"]["supported"]
            ),
            "rows_supported_by_result_disease": sum(
                1 for row in scored_rows if row["step1_clinical_necessity"]["support_by_result_disease"]["supported"]
            ),
            "rows_supported_by_protocol_semantic": sum(
                1 for row in scored_rows if row["step1_clinical_necessity"]["support_by_protocol_semantic"]["supported"]
            ),
            "rows_supported_by_tmh_structured": sum(
                1 for row in scored_rows if row["step1_clinical_necessity"]["support_by_tmh_structured"]["supported"]
            ),
            "rows_not_medically_supported": medical_status_counter.get("not_medically_supported", 0),
            "notes": [
                "This module answers only step 1: whether a service appears clinically justified.",
                "It does not yet apply contract clauses, coverage rules, copay, waiting period, or exclusion logic.",
                "Step 1 now reads observation-level lab results when they can be matched back to the service line.",
                "Lab result signals can also be converted into disease evidence updates through a Neo4j-ready result-to-disease graph.",
                "Sign-to-service reasoning is now integrated when initial signs can be recovered from the canonical unified case set.",
                "Protocol PDF semantic retrieval is used only as supporting evidence and does not replace matrix-based reasoning.",
                "TMH structured support can add support or contraindication signals from the parsed ENT protocol bundle.",
            ],
        }
        summary_path.parent.mkdir(parents=True, exist_ok=True)
        summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
        return summary


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Step 1 clinical necessity scorer")
    parser.add_argument("--input-jsonl", type=Path, default=DEFAULT_INPUT_JSONL)
    parser.add_argument("--output-jsonl", type=Path, default=DEFAULT_OUTPUT_JSONL)
    parser.add_argument("--summary-json", type=Path, default=DEFAULT_SUMMARY_JSON)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    engine = ClinicalNecessityStep1Engine()
    summary = engine.score_jsonl(args.input_jsonl, args.output_jsonl, args.summary_json)
    print(f"Total rows: {summary['total_rows']}")
    print(f"Decision distribution: {summary['decision_distribution']}")
    print(f"Summary JSON: {args.summary_json}")


if __name__ == "__main__":
    main()
