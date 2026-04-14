from __future__ import annotations

import argparse
import json
import math
import re
import unicodedata
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from sign_phrase_decomposer import SignPhraseDecomposer, repair_text

PROJECT_DIR = Path(__file__).parent.parent
GRAPH_DATA_PATH = PROJECT_DIR / "demo" / "disease_graph_explorer_data.json"
SIGN_CONCEPT_CATALOG_PATH = PROJECT_DIR / "05_reference" / "signs" / "sign_concept_catalog.json"
ONTOLOGY_PROFILE_PATH = PROJECT_DIR / "05_reference" / "signs" / "tmh_ontology_disease_profiles.json"
DEFAULT_EXPORT_DIR = PROJECT_DIR / "09_unified_story_testcase" / "step1_sign_to_service"

MAX_SIGN_MATCHES = 8
MAX_DISEASES = 12
MAX_SERVICES = 20
MAX_SUPPORT_ITEMS = 6
MAX_CATALOG_SIGNS = 120

GENERIC_SIGN_TOKENS = {
    "",
    "benh",
    "kham",
    "benhnhan",
    "nguoi",
    "tinh",
    "tot",
    "thang",
    "hien",
    "tai",
    "khong",
    "co",
    "gi",
    "dac",
    "biet",
    "binh",
    "thuong",
    "the",
    "trang",
    "vien",
    "vao",
    "den",
    "ly",
    "do",
}

DEFAULT_SCENARIOS = [
    {
        "scenario_id": "sign_sot_ho_kho_tho",
        "title": "Sot + ho + kho tho",
        "signs": ["sot", "ho", "kho tho"],
    },
    {
        "scenario_id": "sign_dau_hong_sot",
        "title": "Dau hong + sot",
        "signs": ["dau hong", "sot"],
    },
    {
        "scenario_id": "sign_dau_bung_day_bung",
        "title": "Dau bung + day bung",
        "signs": ["dau bung", "day bung"],
    },
    {
        "scenario_id": "sign_sot_xuat_huyet_dau_nhuc",
        "title": "Sot + xuat huyet + dau nhuc",
        "signs": ["sot", "xuat huyet", "dau nhuc"],
    },
    {
        "scenario_id": "sign_dau_nguc_choang_vang",
        "title": "Dau nguc + choang vang",
        "signs": ["dau nguc", "chong mat"],
    },
]


@dataclass
class SignMatch:
    sign_id: str
    sign_label: str
    normalized_key: str
    matched_alias: str
    match_score: float
    match_type: str
    support_cases: int
    disease_links: dict[str, int]


def as_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    return str(value).strip()


def strip_diacritics(text: str) -> str:
    normalized = unicodedata.normalize("NFD", as_text(text))
    return "".join(char for char in normalized if unicodedata.category(char) != "Mn")


def normalize_text(text: str) -> str:
    normalized = strip_diacritics(repair_text(text)).lower()
    normalized = re.sub(r"[^a-z0-9 ]+", " ", normalized)
    normalized = re.sub(r"\s+", " ", normalized).strip()
    return normalized


def tokenize_sign(text: str) -> list[str]:
    tokens: list[str] = []
    for token in normalize_text(text).split():
        if token.isdigit() or token in GENERIC_SIGN_TOKENS:
            continue
        if len(token) >= 2:
            tokens.append(token)
    return tokens


def has_contiguous_token_sequence(container: list[str], candidate: list[str]) -> bool:
    if not container or not candidate or len(candidate) > len(container):
        return False
    width = len(candidate)
    for start in range(0, len(container) - width + 1):
        if container[start : start + width] == candidate:
            return True
    return False


def pick_best_label(existing: str, candidate: str) -> str:
    if existing and len(existing) >= len(candidate):
        return existing
    return candidate or existing


def top_counter(counter: Counter[str], limit: int) -> list[dict[str, Any]]:
    return [{"name": key, "count": value} for key, value in counter.most_common(limit)]


class SignToServiceEngine:
    def __init__(
        self,
        graph_path: Path = GRAPH_DATA_PATH,
        sign_catalog_path: Path = SIGN_CONCEPT_CATALOG_PATH,
        ontology_profile_path: Path = ONTOLOGY_PROFILE_PATH,
    ) -> None:
        self.graph_path = graph_path
        self.sign_catalog_path = sign_catalog_path
        self.ontology_profile_path = ontology_profile_path
        self.bundle = json.loads(graph_path.read_text(encoding="utf-8"))
        self.disease_index = {item["disease_id"]: item for item in self.bundle.get("disease_index", [])}
        self.graphs = self.bundle.get("graphs", {})
        self.sign_phrase_decomposer = SignPhraseDecomposer()
        self.sign_catalog = self._load_or_build_sign_catalog()
        self.profile_catalog = self._load_profile_catalog()
        self.profile_index = {item["disease_id"]: item for item in self.profile_catalog}

    def _load_or_build_sign_catalog(self) -> dict[str, dict[str, Any]]:
        if self.sign_catalog_path.exists():
            payload = json.loads(self.sign_catalog_path.read_text(encoding="utf-8"))
            concepts = payload.get("concepts") or []
            relationships = payload.get("relationships", {}).get("sign_disease") or []
            disease_links_by_sign: dict[str, Counter[str]] = defaultdict(Counter)
            for row in relationships:
                sign_id = as_text(row.get("sign_id"))
                disease_id = as_text(row.get("disease_id"))
                if sign_id and disease_id:
                    disease_links_by_sign[sign_id][disease_id] += int(row.get("support_cases") or 0)

            sign_catalog: dict[str, dict[str, Any]] = {}
            for row in concepts:
                sign_id = as_text(row.get("sign_id"))
                if not sign_id:
                    continue
                aliases = row.get("aliases") or []
                alias_entries = []
                alias_keys = set()
                token_pool = set(row.get("tokens") or [])
                for alias in aliases:
                    alias_label = as_text(alias.get("alias_label"))
                    normalized_alias = as_text(alias.get("normalized_alias")) or normalize_text(alias_label)
                    alias_tokens = set(tokenize_sign(alias_label))
                    alias_entries.append(
                        {
                            "alias_label": alias_label,
                            "normalized_alias": normalized_alias,
                            "tokens": alias_tokens,
                            "label_support_cases": int(alias.get("label_support_cases") or 0),
                            "label_frequency": int(alias.get("label_frequency") or 0),
                        }
                    )
                    if normalized_alias:
                        alias_keys.add(normalized_alias)
                    token_pool.update(alias_tokens)
                sign_catalog[sign_id] = {
                    "sign_id": sign_id,
                    "sign_label": as_text(row.get("canonical_label")),
                    "normalized_key": as_text(row.get("normalized_key")),
                    "tokens": token_pool,
                    "support_cases": int(row.get("support_cases") or 0),
                    "disease_links": disease_links_by_sign.get(sign_id, Counter()),
                    "aliases": alias_entries,
                    "alias_keys": alias_keys,
                }
            return sign_catalog

        return self._build_sign_catalog_from_graph()

    def _load_profile_catalog(self) -> list[dict[str, Any]]:
        if not self.ontology_profile_path.exists():
            return []
        payload = json.loads(self.ontology_profile_path.read_text(encoding="utf-8"))
        profiles = payload.get("profiles") or []
        processed: list[dict[str, Any]] = []
        for row in profiles:
            disease_id = as_text(row.get("disease_id"))
            if not disease_id:
                continue
            concepts = []
            for concept in row.get("sign_concepts") or []:
                aliases = [normalize_text(item) for item in concept.get("aliases") or [] if normalize_text(item)]
                canonical = as_text(concept.get("canonical"))
                canonical_norm = normalize_text(canonical)
                if canonical_norm and canonical_norm not in aliases:
                    aliases.append(canonical_norm)
                concepts.append(
                    {
                        "canonical": canonical,
                        "weight": float(concept.get("weight") or 1.0),
                        "aliases": aliases,
                    }
                )
            modifiers = []
            for clue in row.get("modifier_clues") or []:
                aliases = [normalize_text(item) for item in clue.get("aliases") or [] if normalize_text(item)]
                if not aliases:
                    continue
                modifiers.append(
                    {
                        "label": as_text(clue.get("label")) or aliases[0],
                        "weight": float(clue.get("weight") or 1.0),
                        "aliases": aliases,
                    }
                )
            demographics = []
            for clue in row.get("demographic_clues") or []:
                demographics.append(
                    {
                        "label": as_text(clue.get("label")) or "demographic",
                        "weight": float(clue.get("weight") or 1.0),
                        "sex": normalize_text(clue.get("sex")),
                        "min_age": clue.get("min_age"),
                        "max_age": clue.get("max_age"),
                    }
                )
            services = []
            for service in row.get("services") or []:
                service_name = as_text(service.get("service_name"))
                if not service_name:
                    continue
                services.append(
                    {
                        "service_code": as_text(service.get("service_code")),
                        "service_name": service_name,
                        "weight": float(service.get("weight") or 1.0),
                        "role": as_text(service.get("role")) or "diagnostic",
                    }
                )
            processed.append(
                {
                    "profile_id": as_text(row.get("profile_id")),
                    "disease_id": disease_id,
                    "disease_name": as_text(row.get("disease_name")),
                    "specialty": as_text(row.get("specialty")),
                    "disease_aliases": [normalize_text(item) for item in row.get("disease_aliases") or [] if normalize_text(item)],
                    "sign_concepts": concepts,
                    "modifier_clues": modifiers,
                    "demographic_clues": demographics,
                    "services": services,
                }
            )
        return processed

    def _build_sign_catalog_from_graph(self) -> dict[str, dict[str, Any]]:
        sign_catalog: dict[str, dict[str, Any]] = {}
        for disease_id, graph in self.graphs.items():
            for sign in graph.get("signs", []):
                normalized_key = normalize_text(sign.get("label"))
                if not normalized_key:
                    continue
                sign_id = f"SIGN-{normalized_key.replace(' ', '_').upper()}"
                entry = sign_catalog.setdefault(
                    sign_id,
                    {
                        "sign_id": sign_id,
                        "sign_label": "",
                        "normalized_key": normalized_key,
                        "tokens": set(),
                        "support_cases": 0,
                        "disease_links": Counter(),
                        "aliases": [],
                        "alias_keys": {normalized_key},
                    },
                )
                entry["sign_label"] = pick_best_label(entry["sign_label"], as_text(sign.get("label")))
                entry["tokens"].update(tokenize_sign(sign.get("label")))
                support_cases = int(sign.get("support_cases") or 0)
                entry["support_cases"] += support_cases
                entry["disease_links"][disease_id] += support_cases
                entry["aliases"].append(
                    {
                        "alias_label": as_text(sign.get("label")),
                        "normalized_alias": normalized_key,
                        "tokens": set(tokenize_sign(sign.get("label"))),
                        "label_support_cases": support_cases,
                        "label_frequency": 1,
                    }
                )

        return sign_catalog

    def _match_single_fragment(self, sign_text: str) -> list[SignMatch]:
        normalized = normalize_text(sign_text)
        token_list = tokenize_sign(sign_text)
        tokens = set(token_list)
        matches: list[SignMatch] = []

        for sign_entry in self.sign_catalog.values():
            candidate_norm = sign_entry["normalized_key"]
            best_match_score = 0.0
            best_match_type = ""
            best_alias = sign_entry["sign_label"]

            alias_entries = sign_entry.get("aliases") or [
                {
                    "alias_label": sign_entry["sign_label"],
                    "normalized_alias": candidate_norm,
                    "tokens": sign_entry["tokens"],
                    "label_support_cases": int(sign_entry["support_cases"]),
                }
            ]

            for alias in alias_entries:
                alias_label = as_text(alias.get("alias_label")) or sign_entry["sign_label"]
                alias_norm = as_text(alias.get("normalized_alias")) or normalize_text(alias_label)
                alias_token_list = [token for token in alias_norm.split() if token and token not in GENERIC_SIGN_TOKENS]
                alias_tokens = set(alias_token_list)
                match_score = 0.0
                match_type = ""

                if normalized and normalized == alias_norm:
                    match_score = 1.0
                    match_type = "exact"
                elif token_list and alias_token_list and len(token_list) >= 2 and len(alias_token_list) >= 2 and (
                    has_contiguous_token_sequence(alias_token_list, token_list)
                    or has_contiguous_token_sequence(token_list, alias_token_list)
                ):
                    min_width = min(len(token_list), len(alias_token_list))
                    match_score = 0.94 if min_width >= 2 else 0.82
                    match_type = "token_subsequence"
                elif tokens and alias_tokens:
                    overlap = tokens & alias_tokens
                    if len(overlap) >= 2:
                        coverage = len(overlap) / max(len(tokens), 1)
                        candidate_coverage = len(overlap) / max(len(alias_tokens), 1)
                        jaccard = len(overlap) / max(len(tokens | alias_tokens), 1)
                        match_score = 0.35 + 0.35 * coverage + 0.15 * candidate_coverage + 0.15 * jaccard
                        match_type = "multi_token_overlap"
                    elif len(tokens) == 1 and len(alias_tokens) == 1 and overlap:
                        token = next(iter(overlap))
                        if len(token) >= 3:
                            match_score = 0.74
                            match_type = "single_token_overlap"

                if match_score > best_match_score:
                    best_match_score = match_score
                    best_match_type = match_type
                    best_alias = alias_label

            if best_match_score < 0.55:
                continue

            matches.append(
                SignMatch(
                    sign_id=sign_entry["sign_id"],
                    sign_label=sign_entry["sign_label"],
                    normalized_key=candidate_norm,
                    matched_alias=best_alias,
                    match_score=round(best_match_score, 4),
                    match_type=best_match_type,
                    support_cases=int(sign_entry["support_cases"]),
                    disease_links=dict(sign_entry["disease_links"]),
                )
            )

        matches.sort(key=lambda item: (item.match_score, item.support_cases, item.sign_label), reverse=True)
        return matches[:MAX_SIGN_MATCHES]

    def _match_single_sign(self, sign_text: str) -> list[SignMatch]:
        fragments = self.sign_phrase_decomposer.decompose(sign_text)
        if not fragments:
            fragments = [normalize_text(sign_text)]

        best_by_sign_id: dict[str, SignMatch] = {}
        for fragment in fragments:
            for match in self._match_single_fragment(fragment):
                adjusted_score = match.match_score
                adjusted_type = match.match_type
                if normalize_text(fragment) != normalize_text(sign_text) and len(tokenize_sign(fragment)) >= 2:
                    adjusted_score = round(min(1.0, adjusted_score + 0.03), 4)
                    adjusted_type = f"decomposed_{match.match_type}"
                current = best_by_sign_id.get(match.sign_id)
                candidate = SignMatch(
                    sign_id=match.sign_id,
                    sign_label=match.sign_label,
                    normalized_key=match.normalized_key,
                    matched_alias=match.matched_alias,
                    match_score=adjusted_score,
                    match_type=adjusted_type,
                    support_cases=match.support_cases,
                    disease_links=match.disease_links,
                )
                if current is None or candidate.match_score > current.match_score:
                    best_by_sign_id[match.sign_id] = candidate

        matches = sorted(
            best_by_sign_id.values(),
            key=lambda item: (item.match_score, item.support_cases, item.sign_label),
            reverse=True,
        )
        return matches[:MAX_SIGN_MATCHES]

    @staticmethod
    def _alias_hits(evidence_texts: list[str], aliases: list[str]) -> tuple[bool, str]:
        for alias in aliases:
            if not alias:
                continue
            for text in evidence_texts:
                if not text:
                    continue
                if alias == text or alias in text or text in alias:
                    return True, alias
        return False, ""

    def _profile_supports(
        self,
        signs: list[str],
        structured_signs: list[dict[str, Any]] | None = None,
        patient_context: dict[str, Any] | None = None,
    ) -> tuple[dict[str, float], dict[str, list[dict[str, Any]]], dict[str, list[dict[str, Any]]]]:
        evidence_texts = [normalize_text(item) for item in signs if normalize_text(item)]
        structured_signs = structured_signs or []
        for item in structured_signs:
            concept = normalize_text(item.get("concept"))
            if concept:
                evidence_texts.append(concept)
            for modifier in item.get("modifiers") or []:
                modifier_norm = normalize_text(modifier)
                if modifier_norm:
                    evidence_texts.append(modifier_norm)

        evidence_texts = [item for item in evidence_texts if item]
        profile_scores: dict[str, float] = defaultdict(float)
        profile_supports: dict[str, list[dict[str, Any]]] = defaultdict(list)
        profile_services: dict[str, list[dict[str, Any]]] = defaultdict(list)

        normalized_sex = normalize_text((patient_context or {}).get("gioi_tinh") or (patient_context or {}).get("sex"))
        age_value = (patient_context or {}).get("tuoi")
        try:
            age_number = int(age_value) if age_value is not None and str(age_value).strip() != "" else None
        except Exception:
            age_number = None

        for profile in self.profile_catalog:
            disease_id = profile["disease_id"]
            score = 0.0
            supports: list[dict[str, Any]] = []

            for concept in profile.get("sign_concepts") or []:
                matched, alias = self._alias_hits(evidence_texts, concept.get("aliases") or [])
                if not matched:
                    continue
                weight = float(concept.get("weight") or 1.0)
                score += weight
                supports.append(
                    {
                        "input_sign": concept.get("canonical"),
                        "sign_id": f"{profile['profile_id']}::{normalize_text(concept.get('canonical'))}",
                        "matched_sign": concept.get("canonical"),
                        "matched_alias": alias,
                        "match_type": "ontology_profile_concept",
                        "match_score": round(min(1.0, 0.6 + (weight / 5.0)), 4),
                        "support_cases": max(1, int(round(weight))),
                    }
                )

            for clue in profile.get("modifier_clues") or []:
                matched, alias = self._alias_hits(evidence_texts, clue.get("aliases") or [])
                if not matched:
                    continue
                weight = float(clue.get("weight") or 1.0)
                score += weight
                supports.append(
                    {
                        "input_sign": clue.get("label"),
                        "sign_id": f"{profile['profile_id']}::{normalize_text(clue.get('label'))}",
                        "matched_sign": clue.get("label"),
                        "matched_alias": alias,
                        "match_type": "ontology_profile_modifier",
                        "match_score": round(min(1.0, 0.62 + (weight / 5.0)), 4),
                        "support_cases": max(1, int(round(weight))),
                    }
                )

            for clue in profile.get("demographic_clues") or []:
                sex_ok = True
                if clue.get("sex"):
                    sex_ok = normalized_sex == clue.get("sex")
                age_ok = True
                if age_number is not None and clue.get("min_age") is not None:
                    age_ok = age_ok and age_number >= int(clue["min_age"])
                if age_number is not None and clue.get("max_age") is not None:
                    age_ok = age_ok and age_number <= int(clue["max_age"])
                if not sex_ok or not age_ok:
                    continue
                weight = float(clue.get("weight") or 1.0)
                score += weight
                supports.append(
                    {
                        "input_sign": clue.get("label"),
                        "sign_id": f"{profile['profile_id']}::demographic",
                        "matched_sign": clue.get("label"),
                        "matched_alias": clue.get("label"),
                        "match_type": "ontology_profile_demographic",
                        "match_score": round(min(1.0, 0.55 + (weight / 6.0)), 4),
                        "support_cases": max(1, int(round(weight))),
                    }
                )

            if score <= 0:
                continue

            profile_scores[disease_id] += score
            profile_supports[disease_id].extend(supports)
            for service in profile.get("services") or []:
                profile_services[disease_id].append(service)

        return profile_scores, profile_supports, profile_services

    def match_sign_text(self, sign_text: str, limit: int = MAX_SIGN_MATCHES) -> list[dict[str, Any]]:
        return [
            {
                "sign_id": item.sign_id,
                "sign_label": item.sign_label,
                "normalized_key": item.normalized_key,
                "matched_alias": item.matched_alias,
                "match_score": item.match_score,
                "match_type": item.match_type,
                "support_cases": item.support_cases,
                "linked_disease_count": len(item.disease_links),
                "top_disease_ids": [disease_id for disease_id, _ in Counter(item.disease_links).most_common(5)],
            }
            for item in self._match_single_sign(sign_text)[:limit]
        ]

    @staticmethod
    def _disease_support_boost(support_cases: int) -> float:
        return math.log1p(max(support_cases, 0))

    @staticmethod
    def _service_role_boost(roles: list[str]) -> float:
        lowered = {normalize_text(role) for role in roles}
        if "diagnostic" in lowered:
            return 1.0
        if "rule out" in lowered or "rule_out" in lowered:
            return 0.95
        if "confirmatory" in lowered:
            return 0.9
        if "monitoring" in lowered:
            return 0.8
        if "screening" in lowered:
            return 0.72
        return 0.78

    def infer_from_signs(
        self,
        signs: list[str],
        top_diseases: int = MAX_DISEASES,
        top_services: int = MAX_SERVICES,
        structured_signs: list[dict[str, Any]] | None = None,
        patient_context: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        normalized_inputs = [as_text(sign) for sign in signs if as_text(sign)]
        matched_signs: list[dict[str, Any]] = []
        disease_scores: dict[str, float] = defaultdict(float)
        disease_support_by_sign: dict[str, list[dict[str, Any]]] = defaultdict(list)

        for raw_sign in normalized_inputs:
            sign_matches = self._match_single_sign(raw_sign)
            matched_signs.append(
                {
                    "input_sign": raw_sign,
                    "matches": [
                        {
                            "sign_id": match.sign_id,
                            "sign_label": match.sign_label,
                            "matched_alias": match.matched_alias,
                            "normalized_key": match.normalized_key,
                            "match_score": match.match_score,
                            "match_type": match.match_type,
                            "support_cases": match.support_cases,
                        }
                        for match in sign_matches
                    ],
                }
            )
            for match in sign_matches:
                for disease_id, support_cases in match.disease_links.items():
                    disease_scores[disease_id] += match.match_score * self._disease_support_boost(support_cases)
                    disease_support_by_sign[disease_id].append(
                        {
                            "input_sign": raw_sign,
                            "sign_id": match.sign_id,
                            "matched_sign": match.sign_label,
                            "matched_alias": match.matched_alias,
                            "match_type": match.match_type,
                            "match_score": round(match.match_score, 4),
                            "support_cases": support_cases,
                        }
                    )

        profile_scores, profile_supports, profile_services = self._profile_supports(
            normalized_inputs,
            structured_signs=structured_signs,
            patient_context=patient_context,
        )
        for disease_id, profile_score in profile_scores.items():
            disease_scores[disease_id] += profile_score
            disease_support_by_sign[disease_id].extend(profile_supports.get(disease_id, []))

        ranked_diseases = sorted(
            disease_scores.items(),
            key=lambda item: (
                item[1],
                self.disease_index.get(item[0], {}).get("case_count", 0),
                self.disease_index.get(item[0], {}).get("disease_name", self.profile_index.get(item[0], {}).get("disease_name", "")),
            ),
            reverse=True,
        )[:top_diseases]

        disease_results: list[dict[str, Any]] = []
        service_scores: dict[str, float] = defaultdict(float)
        service_support: dict[str, dict[str, Any]] = {}

        max_case_support = max(
            (
                int(service.get("case_support") or 0)
                for graph in self.graphs.values()
                for service in graph.get("services", [])
            ),
            default=1,
        )

        for disease_id, disease_score in ranked_diseases:
            disease_stub = self.disease_index.get(disease_id, {})
            profile_stub = self.profile_index.get(disease_id, {})
            graph = self.graphs.get(disease_id, {})
            support_items = disease_support_by_sign.get(disease_id, [])
            disease_result = {
                "disease_id": disease_id,
                "icd10": disease_stub.get("icd10"),
                "disease_name": disease_stub.get("disease_name") or profile_stub.get("disease_name"),
                "case_count": disease_stub.get("case_count", 0),
                "score": round(disease_score, 4),
                "supporting_signs": support_items[:MAX_SUPPORT_ITEMS],
            }
            disease_results.append(disease_result)

            for service in graph.get("services", []):
                service_code = as_text(service.get("service_code"))
                if not service_code:
                    continue
                case_support = int(service.get("case_support") or 0)
                case_support_norm = case_support / max(max_case_support, 1)
                max_score = float(service.get("max_score") or 0.0)
                evidence_hits = (
                    int(service.get("guideline_hits") or 0)
                    + int(service.get("protocol_excel_hits") or 0)
                    + int(service.get("statistical_hits") or 0)
                )
                role_boost = self._service_role_boost(service.get("roles") or [])
                service_weight = (0.45 * case_support_norm) + (0.25 * max_score) + (0.15 * min(evidence_hits / 5.0, 1.0)) + (0.15 * role_boost)
                contribution = disease_score * max(service_weight, 0.1)
                service_scores[service_code] += contribution

                bucket = service_support.setdefault(
                    service_code,
                    {
                        "service_code": service_code,
                        "service_name": service.get("label"),
                        "category_code": service.get("category_code"),
                        "category_name": service.get("category_name"),
                        "roles": service.get("roles", []),
                        "evidences": service.get("evidences", []),
                        "supporting_diseases": [],
                    },
                )
                bucket["supporting_diseases"].append(
                    {
                        "disease_id": disease_id,
                        "icd10": disease_stub.get("icd10"),
                        "disease_name": disease_stub.get("disease_name") or profile_stub.get("disease_name"),
                        "disease_score": round(disease_score, 4),
                        "service_case_support": case_support,
                        "service_max_score": max_score,
                    }
                )

            for service in profile_services.get(disease_id, []):
                service_code = as_text(service.get("service_code"))
                service_name = as_text(service.get("service_name"))
                if not service_name:
                    continue
                service_key = service_code or f"profile::{disease_id}::{normalize_text(service_name)}"
                service_weight = float(service.get("weight") or 1.0)
                contribution = disease_score * max(service_weight, 0.1)
                service_scores[service_key] += contribution
                bucket = service_support.setdefault(
                    service_key,
                    {
                        "service_code": service_code,
                        "service_name": service_name,
                        "category_code": "",
                        "category_name": "",
                        "roles": [as_text(service.get("role")) or "diagnostic"],
                        "evidences": ["ontology_profile"],
                        "supporting_diseases": [],
                    },
                )
                bucket["supporting_diseases"].append(
                    {
                        "disease_id": disease_id,
                        "icd10": disease_stub.get("icd10"),
                        "disease_name": disease_stub.get("disease_name") or profile_stub.get("disease_name"),
                        "disease_score": round(disease_score, 4),
                        "service_case_support": 0,
                        "service_max_score": service_weight,
                    }
                )

        ranked_services = sorted(
            service_scores.items(),
            key=lambda item: (item[1], service_support[item[0]]["service_name"]),
            reverse=True,
        )[:top_services]

        service_results = []
        for service_code, service_score in ranked_services:
            bucket = service_support[service_code]
            supporting_diseases = sorted(
                bucket["supporting_diseases"],
                key=lambda item: (item["disease_score"], item["service_case_support"], item["disease_name"]),
                reverse=True,
            )[:MAX_SUPPORT_ITEMS]
            service_results.append(
                {
                    "service_code": service_code,
                    "service_name": bucket["service_name"],
                    "category_code": bucket["category_code"],
                    "category_name": bucket["category_name"],
                    "score": round(service_score, 4),
                    "roles": bucket["roles"],
                    "evidences": bucket["evidences"],
                    "supporting_diseases": supporting_diseases,
                }
            )

        return {
            "input_signs": normalized_inputs,
            "matched_signs": matched_signs,
            "suspected_diseases": disease_results,
            "recommended_services": service_results,
        }

    def build_sign_catalog(self, limit: int = MAX_CATALOG_SIGNS) -> list[dict[str, Any]]:
        ranked_signs = sorted(
            self.sign_catalog.values(),
            key=lambda item: (item["support_cases"], len(item["disease_links"]), item["sign_label"]),
            reverse=True,
        )[:limit]

        catalog: list[dict[str, Any]] = []
        for item in ranked_signs:
            inference = self.infer_from_signs([item["sign_label"]], top_diseases=8, top_services=12)
            catalog.append(
                {
                    "sign_id": item["sign_id"],
                    "sign_label": item["sign_label"],
                    "normalized_key": item["normalized_key"],
                    "support_cases": item["support_cases"],
                    "linked_disease_count": len(item["disease_links"]),
                    "alias_count": len(item.get("aliases") or []),
                    "aliases": [alias.get("alias_label") for alias in (item.get("aliases") or [])[:12]],
                    "top_diseases": inference["suspected_diseases"],
                    "top_services": inference["recommended_services"],
                }
            )
        return catalog

    def export_bundle(self, output_dir: Path) -> dict[str, Path]:
        output_dir.mkdir(parents=True, exist_ok=True)
        summary_path = output_dir / "sign_to_service_summary.json"
        catalog_path = output_dir / "sign_to_service_catalog.json"
        examples_path = output_dir / "sign_to_service_examples.json"
        readme_path = output_dir / "README.md"

        catalog = self.build_sign_catalog()
        examples = [
            {
                "scenario_id": scenario["scenario_id"],
                "title": scenario["title"],
                "signs": scenario["signs"],
                "inference": self.infer_from_signs(scenario["signs"]),
            }
            for scenario in DEFAULT_SCENARIOS
        ]
        summary = {
            "generated_at": datetime.now().astimezone().isoformat(),
            "graph_source": str(self.graph_path),
            "sign_catalog_source": str(self.sign_catalog_path) if self.sign_catalog_path.exists() else "inline_from_graph",
            "catalog_sign_count": len(catalog),
            "scenario_count": len(examples),
            "stats": self.bundle.get("stats", {}),
            "notes": [
                "This module implements Step-1 sign-to-service reasoning only.",
                "It does not decide insurance payment or contract eligibility.",
                "It generates two medical lists indirectly: suspected diseases and recommended services from initial signs.",
            ],
        }

        summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
        catalog_path.write_text(json.dumps(catalog, ensure_ascii=False, indent=2), encoding="utf-8")
        examples_path.write_text(json.dumps(examples, ensure_ascii=False, indent=2), encoding="utf-8")

        readme_lines = [
            "# Step 1 - Sign to Service Engine",
            "",
            f"- Generated at: `{summary['generated_at']}`",
            f"- Graph source: `{self.graph_path}`",
            f"- Catalog signs: `{len(catalog)}`",
            f"- Example scenarios: `{len(examples)}`",
            "",
            "## What this module does",
            "",
            "- Reads the existing disease -> sign and disease -> service knowledge already built from claims + matrix.",
            "- Infers a set of suspected diseases from initial signs.",
            "- Expands those diseases into a ranked set of medically plausible services.",
            "- Supports Step 1 clinical reasoning only, before any insurance contract logic.",
            "",
            "## Output files",
            "",
            f"- `{catalog_path.name}`: top signs with linked diseases and recommended services.",
            f"- `{examples_path.name}`: worked examples for multi-sign scenarios.",
            f"- `{summary_path.name}`: generation metadata and stats.",
        ]
        readme_path.write_text("\n".join(readme_lines) + "\n", encoding="utf-8")

        return {
            "summary": summary_path,
            "catalog": catalog_path,
            "examples": examples_path,
            "readme": readme_path,
        }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Infer medically plausible services from initial signs")
    parser.add_argument("--graph-json", type=Path, default=GRAPH_DATA_PATH)
    parser.add_argument("--sign-catalog-json", type=Path, default=SIGN_CONCEPT_CATALOG_PATH)
    parser.add_argument("--export-dir", type=Path, default=DEFAULT_EXPORT_DIR)
    parser.add_argument("--signs", nargs="*", default=None, help="Optional sign list for direct query")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    engine = SignToServiceEngine(graph_path=args.graph_json, sign_catalog_path=args.sign_catalog_json)

    if args.signs:
        payload = engine.infer_from_signs(args.signs)
        print(json.dumps(payload, ensure_ascii=False, indent=2))
        return

    paths = engine.export_bundle(args.export_dir)
    print(f"Summary: {paths['summary']}")
    print(f"Catalog: {paths['catalog']}")
    print(f"Examples: {paths['examples']}")
    print(f"README: {paths['readme']}")


if __name__ == "__main__":
    main()
