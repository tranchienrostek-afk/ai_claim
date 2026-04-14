from __future__ import annotations

import argparse
import json
import math
import re
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import pandas as pd
from rapidfuzz import fuzz, process

from auto_review import strip_noise, to_skeleton
from chuan_hoa_dich_vu import classify_category, extract_discriminator, normalize_service_name
from service_family_mapper import ServiceFamilyMapper


DATA_DIR = Path(__file__).parent
DEFAULT_CODEBOOK_PATH = DATA_DIR / "service_codebook.json"
DEFAULT_REVIEW_SUMMARY_PATH = DATA_DIR / "review_summary.json"
DEFAULT_MAPPING_POLICY_PATH = DATA_DIR / "service_mapping_policy.json"
DEFAULT_OUTPUT_PATH = DATA_DIR / "mapped_service_codes.xlsx"
FUZZY_PRESELECT_LIMIT = 120
MIN_PRESELECT_SCORE = 55.0


SIGNAL_SYNONYMS = {
    "analytes": {
        "alt": ("alt", "gpt", "sgpt", "alt (gpt)", "alt(gpt)", "alt (sgpt)", "alt(sgpt)"),
        "ast": ("ast", "got", "sgot", "ast (got)", "ast(got)", "ast (sgot)", "ast(sgot)"),
        "ggt": ("ggt", "gama gt", "gamma gt"),
        "amylase": ("amylase",),
        "ldh": ("ldh",),
        "ck": ("ck", "cpk", "ck-mb"),
        "troponin": ("troponin", "troponin i", "troponin t"),
        "creatinin": ("creatinin", "creatinine", "creatinnie"),
        "ure": ("urê", "ure", "urea"),
        "glucose": ("glucose",),
        "bilirubin": ("bilirubin",),
        "cholesterol": ("cholesterol",),
        "triglycerid": ("triglycerid",),
        "hdl": ("hdl", "hdl-c"),
        "ldl": ("ldl", "ldl-c"),
        "albumin": ("albumin",),
        "protein": ("protein", "protein toàn phần"),
        "ferritin": ("ferritin",),
        "transferrin": ("transferrin", "transferin"),
        "crp": ("crp", "c-reactive"),
        "hba1c": ("hba1c", "hbalc"),
        "rf": ("rf",),
        "tsh": ("tsh",),
        "ft3": ("ft3", "ft 3"),
        "ft4": ("ft4", "ft 4"),
        "fsh": ("fsh",),
        "lh": ("lh",),
        "amh": ("amh",),
        "prolactin": ("prolactin",),
        "ige": ("ige",),
        "hbsag": ("hbsag",),
        "hbc": ("hbc",),
        "hcv": ("hcv", "anti-hcv"),
        "hiv": ("hiv",),
        "cmv": ("cmv",),
        "hsv": ("hsv", "hsv 1+2"),
        "influenza": ("influenza", "cúm ab", "cúm a/b"),
        "rsv": ("rsv",),
        "adeno": ("adeno",),
        "dengue": ("dengue",),
        "toxocara": ("toxocara",),
        "norovirus": ("norovirus",),
        "rotavirus": ("rotavirus",),
        "covid": ("covid", "sars", "sars-cov-2"),
        "vi_sinh": ("vi sinh",),
        "sinh_hoa": ("sinh hóa", "hóa sinh"),
        "mien_dich": ("miễn dịch",),
        "huyet_hoc": ("huyết học",),
    },
    "body_parts": {
        "nguc": ("ngực", "lồng ngực", "tim phổi"),
        "bung": ("bụng", "ổ bụng"),
        "cot_song_co": ("cột sống cổ",),
        "cot_song_nguc": ("cột sống ngực",),
        "cot_song_that_lung": ("cột sống thắt lưng",),
        "khung_chau": ("khung chậu", "cùng chậu", "khớp cùng chậu"),
        "ban_ngon_tay": ("bàn ngón tay",),
        "ban_tay": ("bàn tay",),
        "ban_chan": ("bàn chân",),
        "co_tay": ("cổ tay",),
        "co_chan": ("cổ chân",),
        "cang_tay": ("cẳng tay",),
        "cang_chan": ("cẳng chân",),
        "khuyu_tay": ("khuỷu tay",),
        "dui": ("đùi",),
        "khop_goi": ("khớp gối",),
        "khop_vai": ("khớp vai",),
        "khop_hang": ("khớp háng", "khớp hàng"),
        "xoang": ("xoang",),
        "so": ("sọ",),
        "ham_mat": ("hàm mặt",),
        "tuyen_giap": ("tuyến giáp",),
        "tuyen_vu": ("tuyến vú", "vú"),
        "tu_cung": ("tử cung",),
        "tim": ("tim",),
        "da_day": ("dạ dày",),
        "thuc_quan": ("thực quản",),
        "dai_trang": ("đại tràng",),
        "truc_trang": ("trực tràng",),
        "tai_mui_hong": ("tai mũi họng",),
        "thanh_quan": ("thanh quản",),
        "dong_mach_canh": ("động mạch cảnh",),
        "chi_duoi": ("chi dưới",),
    },
    "modalities": {
        "x_quang": ("x-quang", "xquang", "x quang", "xq", "chụp x"),
        "sieu_am": ("siêu âm",),
        "ct": ("ct", "clvt", "cắt lớp vi tính", "cone beam"),
        "mri": ("mri", "cộng hưởng từ"),
        "noi_soi": ("nội soi",),
        "dien_tim": ("điện tim", "điện tâm đồ", "ecg", "ekg"),
        "dien_nao": ("điện não", "eeg"),
        "ho_hap": ("chức năng hô hấp", "phế dung"),
        "holter": ("holter",),
        "doppler": ("doppler",),
    },
    "specimens": {
        "mau": ("máu", "huyết thanh", "huyết tương", "máu ngoại vi"),
        "nuoc_tieu": ("nước tiểu", "niệu"),
        "dam": ("đờm",),
        "phan": ("phân",),
        "dich_am_dao": ("dịch âm đạo",),
        "mo_benh_hoc": ("mô bệnh học",),
    },
}


def _compile_pattern(term: str) -> re.Pattern[str]:
    return re.compile(r"(?<!\w)" + re.escape(term) + r"(?!\w)", flags=re.IGNORECASE)


COMPILED_SIGNAL_PATTERNS = {
    group: {
        canonical: [_compile_pattern(alias) for alias in aliases]
        for canonical, aliases in terms.items()
    }
    for group, terms in SIGNAL_SYNONYMS.items()
}


@dataclass(frozen=True)
class QueryFeatures:
    raw_text: str
    cleaned_name: str
    stripped_name: str
    skeleton: str
    category_code: str
    category_name: str
    discriminator: str
    signals: dict[str, set[str]]
    family_id: str | None
    family_status: str
    family_confidence: str


@dataclass(frozen=True)
class VariantEntry:
    service_code: str
    canonical_name: str
    category_code: str
    category_name: str
    cleaned_name: str
    stripped_name: str
    skeleton: str
    discriminator: str
    occurrences: int
    signals: dict[str, set[str]]
    family_id: str | None


@dataclass
class ScoredMatch:
    service_code: str
    canonical_name: str
    category_code: str
    category_name: str
    matched_variant: str
    score: float
    confidence: str
    reasons: list[str]
    conflicts: list[str]
    metrics: dict[str, float]


class ServiceTextMapper:
    def __init__(
        self,
        codebook_path: Path = DEFAULT_CODEBOOK_PATH,
        review_summary_path: Path = DEFAULT_REVIEW_SUMMARY_PATH,
        mapping_policy_path: Path = DEFAULT_MAPPING_POLICY_PATH,
    ) -> None:
        self.codebook_path = codebook_path
        self.review_summary_path = review_summary_path
        self.mapping_policy_path = mapping_policy_path
        self.blacklist = self._load_blacklist(review_summary_path)
        self.blacklist_by_variant: dict[str, set[str]] = defaultdict(set)
        for service_code, cleaned_variant in self.blacklist:
            self.blacklist_by_variant[cleaned_variant].add(service_code)
        self.family_mapper = ServiceFamilyMapper()
        self.mapping_policy = self._load_mapping_policy(mapping_policy_path)
        self.alias_supplements = self.mapping_policy.get("alias_supplements") or []
        self.family_only_aliases = self._build_family_only_aliases(self.mapping_policy.get("family_only_aliases") or [])
        self.entries: list[VariantEntry] = []
        self.by_cleaned: dict[str, list[int]] = defaultdict(list)
        self.by_stripped: dict[str, list[int]] = defaultdict(list)
        self.by_skeleton: dict[str, list[int]] = defaultdict(list)
        self.by_category: dict[str, set[int]] = defaultdict(set)
        self.by_family: dict[str, set[int]] = defaultdict(set)
        self.by_signal: dict[str, dict[str, set[int]]] = {
            group: defaultdict(set) for group in SIGNAL_SYNONYMS
        }
        self._load_entries()
        self.unique_names = list(self.by_cleaned.keys())

    def _load_mapping_policy(self, mapping_policy_path: Path) -> dict:
        if not mapping_policy_path.exists():
            return {}
        with mapping_policy_path.open(encoding="utf-8") as handle:
            return json.load(handle)

    def _family_id_for_entry(
        self,
        *,
        service_code: str,
        canonical_name: str,
        explicit_family_id: str | None = None,
    ) -> str | None:
        if explicit_family_id:
            return explicit_family_id
        for row in self.alias_supplements:
            if row.get("service_code") == service_code and row.get("family_id"):
                return str(row["family_id"])
        family_result = self.family_mapper.score_text(canonical_name, top_k=1)
        if family_result.get("mapping_status") in {"exact", "probable", "ambiguous"}:
            return str(family_result.get("family_id") or "") or None
        return None

    def _build_family_only_aliases(self, rows: list[dict]) -> dict[str, dict[str, str]]:
        alias_map: dict[str, dict[str, str]] = {}
        for row in rows:
            family_id = str(row.get("family_id") or "").strip()
            if not family_id:
                continue
            family_label = str(row.get("family_label") or family_id).strip()
            for alias in row.get("aliases") or []:
                cleaned_alias, _ = normalize_service_name(alias)
                if cleaned_alias:
                    alias_map[cleaned_alias] = {"family_id": family_id, "family_label": family_label}
        return alias_map

    def _load_blacklist(self, review_summary_path: Path) -> set[tuple[str, str]]:
        if not review_summary_path.exists():
            return set()

        with review_summary_path.open(encoding="utf-8") as handle:
            summary = json.load(handle)

        blacklist = set()
        for item in summary.get("wrong_merges", []):
            service_code = item.get("service_code")
            wrong_variant = item.get("wrong_variant", "")
            normalized_variant, _ = normalize_service_name(wrong_variant)
            blacklist.add((service_code, normalized_variant))
        return blacklist

    def _load_entries(self) -> None:
        with self.codebook_path.open(encoding="utf-8") as handle:
            payload = json.load(handle)

        seen_pairs = set()
        for cluster in payload.get("codebook", []):
            service_code = cluster["service_code"]
            canonical_name = cluster["canonical_name"]
            category_code = cluster["category_code"]
            category_name = cluster["category_name"]
            family_id = self._family_id_for_entry(service_code=service_code, canonical_name=canonical_name)

            variants = list(cluster.get("variants", []))
            if canonical_name not in {v["cleaned_name"] for v in variants}:
                variants.append(
                    {
                        "cleaned_name": canonical_name,
                        "fuzzy_score": 100,
                        "occurrences": cluster.get("total_occurrences", 1),
                    }
                )

            for variant in variants:
                cleaned_name, _ = normalize_service_name(variant["cleaned_name"])
                if (service_code, cleaned_name) in self.blacklist:
                    continue
                pair_key = (service_code, cleaned_name)
                if pair_key in seen_pairs:
                    continue
                seen_pairs.add(pair_key)

                entry = self._build_entry(
                    service_code=service_code,
                    canonical_name=canonical_name,
                    category_code=category_code,
                    category_name=category_name,
                    cleaned_name=cleaned_name,
                    occurrences=int(variant.get("occurrences", 1)),
                    family_id=family_id,
                )
                self._register_entry(entry)

        for row in self.alias_supplements:
            service_code = str(row.get("service_code") or "").strip()
            if not service_code:
                continue
            family_id = self._family_id_for_entry(
                service_code=service_code,
                canonical_name=str(row.get("canonical_name") or ""),
                explicit_family_id=str(row.get("family_id") or "").strip() or None,
            )
            for alias in row.get("aliases") or []:
                cleaned_name, _ = normalize_service_name(alias)
                if not cleaned_name or (service_code, cleaned_name) in seen_pairs:
                    continue
                seen_pairs.add((service_code, cleaned_name))
                entry = self._build_entry(
                    service_code=service_code,
                    canonical_name=str(row.get("canonical_name") or alias),
                    category_code=str(row.get("category_code") or classify_category(cleaned_name)[0]),
                    category_name=str(row.get("category_name") or classify_category(cleaned_name)[1]),
                    cleaned_name=cleaned_name,
                    occurrences=int(row.get("occurrences") or 1),
                    family_id=family_id,
                )
                self._register_entry(entry)

    def _build_entry(
        self,
        service_code: str,
        canonical_name: str,
        category_code: str,
        category_name: str,
        cleaned_name: str,
        occurrences: int,
        family_id: str | None,
    ) -> VariantEntry:
        stripped_name = strip_noise(cleaned_name)
        skeleton = to_skeleton(stripped_name or cleaned_name)
        discriminator = extract_discriminator(cleaned_name)
        signals = extract_signal_tokens(cleaned_name)
        return VariantEntry(
            service_code=service_code,
            canonical_name=canonical_name,
            category_code=category_code,
            category_name=category_name,
            cleaned_name=cleaned_name,
            stripped_name=stripped_name,
            skeleton=skeleton,
            discriminator=discriminator,
            occurrences=occurrences,
            signals=signals,
            family_id=family_id,
        )

    def _register_entry(self, entry: VariantEntry) -> None:
        idx = len(self.entries)
        self.entries.append(entry)
        self.by_cleaned[entry.cleaned_name].append(idx)
        self.by_stripped[entry.stripped_name].append(idx)
        self.by_skeleton[entry.skeleton].append(idx)
        self.by_category[entry.category_code].add(idx)
        if entry.family_id:
            self.by_family[entry.family_id].add(idx)
        for group, tokens in entry.signals.items():
            for token in tokens:
                self.by_signal[group][token].add(idx)

    def build_query(self, raw_text: str) -> QueryFeatures:
        cleaned_name, _ = normalize_service_name(raw_text)
        stripped_name = strip_noise(cleaned_name)
        skeleton = to_skeleton(stripped_name or cleaned_name)
        category_code, category_name = classify_category(cleaned_name)
        discriminator = extract_discriminator(cleaned_name)
        signals = extract_signal_tokens(cleaned_name)
        family_result = self.family_mapper.score_text(raw_text, top_k=1)
        return QueryFeatures(
            raw_text=raw_text,
            cleaned_name=cleaned_name,
            stripped_name=stripped_name,
            skeleton=skeleton,
            category_code=category_code,
            category_name=category_name,
            discriminator=discriminator,
            signals=signals,
            family_id=family_result.get("family_id"),
            family_status=str(family_result.get("mapping_status") or "unknown"),
            family_confidence=str(family_result.get("family_confidence") or "unknown"),
        )

    def _candidate_ids(self, query: QueryFeatures) -> set[int]:
        candidate_ids: set[int] = set()

        candidate_ids.update(self.by_cleaned.get(query.cleaned_name, []))
        candidate_ids.update(self.by_stripped.get(query.stripped_name, []))
        candidate_ids.update(self.by_skeleton.get(query.skeleton, []))

        if query.category_code in self.by_category:
            candidate_ids.update(self.by_category[query.category_code])
        if query.family_id and query.family_status in {"exact", "probable", "ambiguous"}:
            candidate_ids.update(self.by_family.get(query.family_id, set()))

        for group, tokens in query.signals.items():
            for token in tokens:
                candidate_ids.update(self.by_signal[group].get(token, set()))

        for scorer in (fuzz.token_sort_ratio, fuzz.token_set_ratio):
            matches = process.extract(
                query.cleaned_name,
                self.unique_names,
                scorer=scorer,
                limit=FUZZY_PRESELECT_LIMIT,
                score_cutoff=MIN_PRESELECT_SCORE,
            )
            for name, _, _ in matches:
                candidate_ids.update(self.by_cleaned.get(name, []))

        if not candidate_ids:
            candidate_ids.update(range(len(self.entries)))

        return candidate_ids

    def score_text(self, raw_text: str, top_k: int = 3) -> dict:
        query = self.build_query(raw_text)
        candidate_ids = self._candidate_ids(query)
        best_by_code: dict[str, ScoredMatch] = {}

        for idx in candidate_ids:
            entry = self.entries[idx]
            if entry.service_code in self.blacklist_by_variant.get(query.cleaned_name, set()):
                continue
            score, reasons, conflicts, metrics = score_entry(query, entry)
            if score < 45:
                continue

            current = best_by_code.get(entry.service_code)
            if current is None or score > current.score:
                best_by_code[entry.service_code] = ScoredMatch(
                    service_code=entry.service_code,
                    canonical_name=entry.canonical_name,
                    category_code=entry.category_code,
                    category_name=entry.category_name,
                    matched_variant=entry.cleaned_name,
                    score=score,
                    confidence="REVIEW",
                    reasons=reasons,
                    conflicts=conflicts,
                    metrics=metrics,
                )

        suggestions = sorted(best_by_code.values(), key=lambda item: (-item.score, item.service_code))
        suggestions = suggestions[:top_k]

        if suggestions:
            second_score = suggestions[1].score if len(suggestions) > 1 else 0.0
            suggestions[0].confidence = assign_confidence(
                top_match=suggestions[0],
                second_score=second_score,
                query=query,
            )
            for item in suggestions[1:]:
                item.confidence = "ALTERNATIVE"

        family_only = self.family_only_aliases.get(query.cleaned_name)
        mapping_resolution = "coded" if suggestions else ("family_only" if family_only else "unknown")
        if suggestions and suggestions[0].confidence == "REVIEW" and family_only:
            mapping_resolution = "family_only"

        return {
            "input_text": raw_text,
            "cleaned_text": query.cleaned_name,
            "category_hint_code": query.category_code,
            "category_hint_name": query.category_name,
            "family_hint_id": query.family_id,
            "family_hint_status": query.family_status,
            "family_hint_confidence": query.family_confidence,
            "mapping_resolution": mapping_resolution,
            "discriminator": query.discriminator,
            "signals": {group: sorted(tokens) for group, tokens in query.signals.items() if tokens},
            "family_only_hint": family_only,
            "suggestions": [
                {
                    "service_code": item.service_code,
                    "canonical_name": item.canonical_name,
                    "category_code": item.category_code,
                    "category_name": item.category_name,
                    "matched_variant": item.matched_variant,
                    "score": round(min(item.score, 100.0), 2),
                    "confidence": item.confidence,
                    "reasons": item.reasons,
                    "conflicts": item.conflicts,
                    "family_id": item.metrics.get("family_id"),
                }
                for item in suggestions
            ],
        }

    def map_dataframe(self, frame: pd.DataFrame, text_column: str, top_k: int = 3) -> pd.DataFrame:
        mapped_rows = []
        for _, row in frame.iterrows():
            raw_text = "" if pd.isna(row[text_column]) else str(row[text_column])
            result = self.score_text(raw_text, top_k=top_k)
            suggestions = result["suggestions"]
            top = suggestions[0] if suggestions else {}
            alternatives = [
                f'{item["service_code"]}|{item["canonical_name"]}|{min(item["score"], 100.0):.2f}'
                for item in suggestions[1:]
            ]
            mapped_rows.append(
                {
                    **row.to_dict(),
                    "normalized_text": result["cleaned_text"],
                    "category_hint_code": result["category_hint_code"],
                    "category_hint_name": result["category_hint_name"],
                    "family_hint_id": result.get("family_hint_id", ""),
                    "family_hint_status": result.get("family_hint_status", ""),
                    "mapping_resolution": result.get("mapping_resolution", ""),
                    "mapped_service_code": top.get("service_code", ""),
                    "mapped_canonical_name": top.get("canonical_name", ""),
                    "mapped_category_name": top.get("category_name", ""),
                    "mapped_score": min(top.get("score", 0.0), 100.0) if top else "",
                    "mapped_confidence": top.get("confidence", "REVIEW"),
                    "matched_variant": top.get("matched_variant", ""),
                    "matched_reasons": "; ".join(top.get("reasons", [])),
                    "matched_conflicts": "; ".join(top.get("conflicts", [])),
                    "alternative_candidates": " || ".join(alternatives),
                }
            )
        return pd.DataFrame(mapped_rows)


def extract_signal_tokens(text: str) -> dict[str, set[str]]:
    lowered = text.lower()
    extracted: dict[str, set[str]] = {group: set() for group in SIGNAL_SYNONYMS}
    for group, terms in COMPILED_SIGNAL_PATTERNS.items():
        for canonical, patterns in terms.items():
            if any(pattern.search(lowered) for pattern in patterns):
                extracted[group].add(canonical)
    return extracted


def char_ngram_jaccard(left: str, right: str, n: int = 3) -> float:
    def grams(value: str) -> set[str]:
        if not value:
            return set()
        if len(value) <= n:
            return {value}
        return {value[i : i + n] for i in range(len(value) - n + 1)}

    left_grams = grams(left)
    right_grams = grams(right)
    if not left_grams and not right_grams:
        return 1.0
    union = left_grams | right_grams
    if not union:
        return 0.0
    return len(left_grams & right_grams) / len(union)


def weighted_signal_overlap(
    query_signals: dict[str, set[str]],
    entry_signals: dict[str, set[str]],
) -> float:
    weights = {
        "analytes": 0.45,
        "body_parts": 0.25,
        "modalities": 0.15,
        "specimens": 0.15,
    }
    total_weight = 0.0
    score = 0.0
    for group, weight in weights.items():
        query_tokens = query_signals[group]
        entry_tokens = entry_signals[group]
        if not query_tokens and not entry_tokens:
            continue
        total_weight += weight
        if query_tokens and entry_tokens and query_tokens & entry_tokens:
            score += weight
        elif not query_tokens or not entry_tokens:
            score += weight * 0.35
    if total_weight == 0:
        return 0.5
    return score / total_weight


def detect_conflicts(
    query_signals: dict[str, set[str]],
    entry_signals: dict[str, set[str]],
) -> list[str]:
    conflicts = []
    for group in ("analytes", "body_parts", "modalities", "specimens"):
        query_tokens = query_signals[group]
        entry_tokens = entry_signals[group]
        if query_tokens and entry_tokens and not query_tokens & entry_tokens:
            conflicts.append(group)
    return conflicts


def score_entry(query: QueryFeatures, entry: VariantEntry) -> tuple[float, list[str], list[str], dict[str, float]]:
    token_sort = fuzz.token_sort_ratio(query.cleaned_name, entry.cleaned_name)
    token_set = fuzz.token_set_ratio(query.cleaned_name, entry.cleaned_name)
    partial = fuzz.partial_ratio(query.cleaned_name, entry.cleaned_name)
    stripped_ratio = fuzz.ratio(query.stripped_name, entry.stripped_name) if query.stripped_name and entry.stripped_name else 0.0
    skeleton_ratio = fuzz.ratio(query.skeleton, entry.skeleton) if query.skeleton and entry.skeleton else 0.0
    ngram_jaccard = char_ngram_jaccard(query.skeleton, entry.skeleton) * 100
    signal_overlap = weighted_signal_overlap(query.signals, entry.signals) * 100

    score = (
        token_sort * 0.24
        + token_set * 0.14
        + partial * 0.08
        + stripped_ratio * 0.14
        + skeleton_ratio * 0.16
        + ngram_jaccard * 0.12
        + signal_overlap * 0.12
    )

    reasons: list[str] = []
    conflicts = detect_conflicts(query.signals, entry.signals)

    if query.cleaned_name == entry.cleaned_name:
        score = max(score, 99.0)
        reasons.append("exact_cleaned_match")
    if query.stripped_name and query.stripped_name == entry.stripped_name:
        score += 8
        reasons.append("exact_after_noise_strip")
    elif query.stripped_name and entry.stripped_name and (
        query.stripped_name in entry.stripped_name or entry.stripped_name in query.stripped_name
    ):
        score += 4
        reasons.append("same_core_phrase")

    if query.discriminator and query.discriminator == entry.discriminator:
        score += 6
        reasons.append("medical_discriminator_match")

    if query.category_code == entry.category_code and query.category_code != "GEN-OTH":
        score += 3
        reasons.append("same_category_hint")
    elif query.category_code != entry.category_code and query.category_code != "GEN-OTH" and entry.category_code != "GEN-OTH":
        score -= 5

    if query.family_id and entry.family_id:
        if query.family_id == entry.family_id:
            score += 6
            reasons.append("same_service_family")
        elif query.family_status in {"exact", "probable"}:
            score -= 8
            reasons.append("penalty_family_conflict")

    if entry.occurrences > 0:
        score += min(math.log10(entry.occurrences + 1) * 1.5, 2.5)

    penalties = {
        "analytes": 24,
        "body_parts": 18,
        "modalities": 14,
        "specimens": 10,
    }
    for conflict in conflicts:
        score -= penalties[conflict]
        reasons.append(f"penalty_{conflict}_conflict")

    if signal_overlap >= 80:
        reasons.append("medical_signal_overlap_high")
    elif signal_overlap >= 60:
        reasons.append("medical_signal_overlap_good")

    if token_sort >= 95:
        reasons.append("token_sort_very_high")
    elif token_sort >= 88:
        reasons.append("token_sort_high")

    score = max(0.0, score)
    metrics = {
        "token_sort": token_sort,
        "token_set": token_set,
        "partial": partial,
        "stripped_ratio": stripped_ratio,
        "skeleton_ratio": skeleton_ratio,
        "ngram_jaccard": ngram_jaccard,
        "signal_overlap": signal_overlap,
        "family_id": entry.family_id or "",
    }
    return score, reasons, conflicts, metrics


def assign_confidence(top_match: ScoredMatch, second_score: float, query: QueryFeatures) -> str:
    gap = top_match.score - second_score
    exact_like = (
        "exact_cleaned_match" in top_match.reasons
        or "exact_after_noise_strip" in top_match.reasons
    )
    if top_match.conflicts:
        if exact_like and top_match.score >= 90 and gap >= 8:
            return "MEDIUM"
        if top_match.score >= 84 and gap >= 6:
            return "LOW"
        return "REVIEW"

    if exact_like:
        return "HIGH"
    if top_match.score >= 93 and gap >= 4:
        return "HIGH"
    if top_match.score >= 87 and gap >= 2.5:
        return "MEDIUM"
    if top_match.score >= 80:
        return "LOW"
    return "REVIEW"


def load_input_frame(input_path: Path, text_column: str | None) -> tuple[pd.DataFrame, str]:
    suffix = input_path.suffix.lower()
    if suffix == ".txt":
        rows = [line.strip() for line in input_path.read_text(encoding="utf-8").splitlines() if line.strip()]
        frame = pd.DataFrame({"service_text": rows})
        return frame, "service_text"

    if suffix == ".csv":
        frame = pd.read_csv(input_path)
    elif suffix in {".xlsx", ".xls"}:
        frame = pd.read_excel(input_path)
    elif suffix == ".json":
        with input_path.open(encoding="utf-8") as handle:
            payload = json.load(handle)
        if isinstance(payload, list):
            frame = pd.DataFrame(payload)
        else:
            frame = pd.DataFrame(payload.get("data", []))
    else:
        raise ValueError(f"Unsupported input format: {input_path.suffix}")

    selected_column = text_column or frame.columns[0]
    if selected_column not in frame.columns:
        raise ValueError(f"Column '{selected_column}' does not exist in {input_path.name}")
    return frame, selected_column


def dump_single_result(result: dict) -> str:
    return json.dumps(result, ensure_ascii=False, indent=2)


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Map raw service text to a standardized service_code from service_codebook.json."
    )
    parser.add_argument("--text", help="Single service text to map.")
    parser.add_argument("--input", type=Path, help="Input file (.txt, .csv, .xlsx, .json).")
    parser.add_argument("--column", help="Text column for tabular input.")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT_PATH, help="Output file for batch mode.")
    parser.add_argument("--top-k", type=int, default=3, help="Number of suggestions to keep.")
    parser.add_argument("--codebook", type=Path, default=DEFAULT_CODEBOOK_PATH, help="Path to service_codebook.json.")
    parser.add_argument(
        "--review-summary",
        type=Path,
        default=DEFAULT_REVIEW_SUMMARY_PATH,
        help="Path to review_summary.json for known wrong merges.",
    )
    return parser


def main(argv: Iterable[str] | None = None) -> None:
    parser = build_argument_parser()
    args = parser.parse_args(argv)

    if not args.text and not args.input:
        parser.error("Provide --text for a single query or --input for batch mapping.")

    mapper = ServiceTextMapper(
        codebook_path=args.codebook,
        review_summary_path=args.review_summary,
    )

    if args.text:
        result = mapper.score_text(args.text, top_k=args.top_k)
        print(dump_single_result(result))
        return

    frame, text_column = load_input_frame(args.input, args.column)
    mapped = mapper.map_dataframe(frame, text_column=text_column, top_k=args.top_k)

    if args.output.suffix.lower() == ".csv":
        mapped.to_csv(args.output, index=False, encoding="utf-8-sig")
    else:
        mapped.to_excel(args.output, index=False)

    print(f"Mapped {len(mapped):,} rows using column '{text_column}'.")
    print(f"Output saved to: {args.output}")


if __name__ == "__main__":
    main()
