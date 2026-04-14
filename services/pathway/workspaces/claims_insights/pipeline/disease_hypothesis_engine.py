from __future__ import annotations

import math
import re
import unicodedata
from dataclasses import dataclass
from pathlib import Path
from typing import Any

try:
    from case_context_retriever import CaseContextRetriever
    from lab_result_to_disease_inference import DiseaseEvidenceUpdate, LabResultDiseaseInferenceEngine
    from reasoning_experience_memory import ReasoningExperienceMemory
    from sign_to_service_engine import SignToServiceEngine, as_text
except ModuleNotFoundError:  # pragma: no cover
    import sys

    sys.path.append(str(Path(__file__).resolve().parent))
    from case_context_retriever import CaseContextRetriever
    from lab_result_to_disease_inference import DiseaseEvidenceUpdate, LabResultDiseaseInferenceEngine
    from reasoning_experience_memory import ReasoningExperienceMemory
    from sign_to_service_engine import SignToServiceEngine, as_text


def normalize_text(value: Any) -> str:
    text = as_text(value).lower().replace("đ", "d").replace("Đ", "d")
    normalized = unicodedata.normalize("NFD", text)
    stripped = "".join(ch for ch in normalized if unicodedata.category(ch) != "Mn")
    stripped = re.sub(r"[^a-z0-9 ]+", " ", stripped)
    return re.sub(r"\s+", " ", stripped).strip()


def important_tokens(value: Any) -> set[str]:
    generic = {
        "",
        "benh",
        "hoi",
        "chung",
        "cap",
        "man",
        "tinh",
        "do",
        "khong",
        "co",
        "va",
    }
    return {token for token in normalize_text(value).split() if len(token) > 2 and token not in generic}


def text_overlap_score(left: Any, right: Any) -> float:
    left_tokens = important_tokens(left)
    right_tokens = important_tokens(right)
    if not left_tokens or not right_tokens:
        return 0.0
    overlap = left_tokens & right_tokens
    if not overlap:
        return 0.0
    return len(overlap) / max(len(left_tokens | right_tokens), 1)


def unique_terms(values: list[Any]) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for value in values:
        text = as_text(value)
        if not text:
            continue
        key = normalize_text(text)
        if not key or key in seen:
            continue
        seen.add(key)
        ordered.append(text)
    return ordered


def canonical_hypothesis_key(disease_id: str = "", disease_name: str = "", icd10: str = "") -> str:
    if as_text(icd10):
        return f"icd:{normalize_text(icd10)}"
    if as_text(disease_id):
        return f"id:{normalize_text(disease_id)}"
    return f"name:{normalize_text(disease_name)}"


@dataclass
class DiseaseHypothesis:
    hypothesis_key: str
    disease_name: str
    icd10: str
    status: str
    score: float
    confidence: float
    sign_score: float
    service_score: float
    lab_support_score: float
    lab_exclusion_score: float
    memory_prior_score: float
    graph_context_score: float
    graph_context_match_count: int
    memory_match_count: int
    supporting_signs: list[str]
    opposing_signals: list[str]
    matched_services: list[str]
    required_services: list[str]
    memory_recommendations: list[str]
    memory_categories: list[str]
    graph_context_snippets: list[str]
    evidence_items: list[dict[str, Any]]


class DiseaseHypothesisEngine:
    def __init__(
        self,
        sign_engine: SignToServiceEngine | None = None,
        lab_engine: LabResultDiseaseInferenceEngine | None = None,
        experience_memory: ReasoningExperienceMemory | None = None,
        context_retriever: CaseContextRetriever | None = None,
        graph_namespace: str = "ontology_v2",
    ) -> None:
        self.sign_engine = sign_engine or SignToServiceEngine()
        self.lab_engine = lab_engine or LabResultDiseaseInferenceEngine()
        default_memory_path = Path(__file__).resolve().parents[3] / "data" / "script" / "experience_memory" / "reasoning_experience_memory.jsonl"
        self.experience_memory = experience_memory or ReasoningExperienceMemory(default_memory_path)
        self.graph_namespace = graph_namespace
        if context_retriever is not None:
            self.context_retriever = context_retriever
        else:
            try:
                self.context_retriever = CaseContextRetriever(namespace=graph_namespace)
            except Exception:
                self.context_retriever = None

    @staticmethod
    def _ensure_bucket(
        buckets: dict[str, dict[str, Any]],
        *,
        hypothesis_key: str,
        disease_name: str,
        icd10: str,
    ) -> dict[str, Any]:
        bucket = buckets.get(hypothesis_key)
        if bucket is None:
            bucket = {
                "hypothesis_key": hypothesis_key,
                "disease_name": as_text(disease_name),
                "icd10": as_text(icd10),
                "sign_score": 0.0,
                "service_score": 0.0,
                "lab_support_score": 0.0,
                "lab_exclusion_score": 0.0,
                "memory_prior_score": 0.0,
                "graph_context_score": 0.0,
                "graph_context_match_count": 0,
                "memory_match_count": 0,
                "supporting_signs": [],
                "opposing_signals": [],
                "matched_services": [],
                "required_services": [],
                "memory_recommendations": [],
                "memory_categories": [],
                "graph_context_snippets": [],
                "evidence_items": [],
            }
            buckets[hypothesis_key] = bucket
        else:
            if not bucket["disease_name"] and as_text(disease_name):
                bucket["disease_name"] = as_text(disease_name)
            if not bucket["icd10"] and as_text(icd10):
                bucket["icd10"] = as_text(icd10)
        return bucket

    def _collect_sign_evidence(
        self,
        buckets: dict[str, dict[str, Any]],
        *,
        signs: list[str],
        structured_signs: list[dict[str, Any]] | None,
        patient_context: dict[str, Any] | None,
    ) -> tuple[dict[str, list[dict[str, Any]]], dict[str, list[str]], dict[str, Any]]:
        sign_payload = self.sign_engine.infer_from_signs(
            signs,
            top_diseases=12,
            top_services=20,
            structured_signs=structured_signs,
            patient_context=patient_context,
        )
        suspected = sign_payload.get("suspected_diseases") or []
        if not suspected:
            return {}, {}, sign_payload

        max_sign_score = max(float(item.get("score") or 0.0) for item in suspected) or 1.0
        disease_to_services: dict[str, list[dict[str, Any]]] = {}
        for item in suspected:
            hypothesis_key = canonical_hypothesis_key(
                disease_id=as_text(item.get("disease_id")),
                disease_name=as_text(item.get("disease_name")),
                icd10=as_text(item.get("icd10")),
            )
            bucket = self._ensure_bucket(
                buckets,
                hypothesis_key=hypothesis_key,
                disease_name=as_text(item.get("disease_name")),
                icd10=as_text(item.get("icd10")),
            )
            score = float(item.get("score") or 0.0)
            normalized_score = score / max_sign_score
            contribution = 0.7 * normalized_score
            bucket["sign_score"] += contribution
            supporting_signs = [
                as_text(evidence.get("matched_sign")) or as_text(evidence.get("input_sign"))
                for evidence in item.get("supporting_signs") or []
                if as_text(evidence.get("matched_sign")) or as_text(evidence.get("input_sign"))
            ]
            bucket["supporting_signs"].extend(supporting_signs)
            bucket["evidence_items"].append(
                {
                    "source": "sign_engine",
                    "score_contribution": round(contribution, 4),
                    "raw_score": round(score, 4),
                    "supporting_signs": supporting_signs[:8],
                }
            )
            disease_to_services[hypothesis_key] = []

        for service in sign_payload.get("recommended_services") or []:
            service_name = as_text(service.get("service_name"))
            for supported in service.get("supporting_diseases") or []:
                hypothesis_key = canonical_hypothesis_key(
                    disease_id=as_text(supported.get("disease_id")),
                    disease_name=as_text(supported.get("disease_name")),
                    icd10=as_text(supported.get("icd10")),
                )
                disease_to_services.setdefault(hypothesis_key, []).append(service)

        required_services: dict[str, list[str]] = {}
        for hypothesis_key, rows in disease_to_services.items():
            names: list[str] = []
            for row in rows:
                service_name = as_text(row.get("service_name"))
                if service_name and service_name not in names:
                    names.append(service_name)
            required_services[hypothesis_key] = names
        return disease_to_services, required_services, sign_payload

    def _collect_service_overlap_evidence(
        self,
        buckets: dict[str, dict[str, Any]],
        *,
        observed_services: list[dict[str, Any]] | None,
        disease_to_services: dict[str, list[dict[str, Any]]],
    ) -> None:
        if not observed_services:
            return

        observed_names = [as_text(item.get("service_name")) or as_text(item.get("service_name_raw")) for item in observed_services]
        observed_names = [name for name in observed_names if name]
        if not observed_names:
            return

        match_counter: dict[str, int] = {}
        match_details: dict[str, list[str]] = {}

        for hypothesis_key, candidate_services in disease_to_services.items():
            for observed_name in observed_names:
                for row in candidate_services:
                    service_name = as_text(row.get("service_name"))
                    overlap = text_overlap_score(observed_name, service_name)
                    if overlap < 0.34:
                        continue
                    match_counter[hypothesis_key] = match_counter.get(hypothesis_key, 0) + 1
                    match_details.setdefault(hypothesis_key, []).append(f"{observed_name} -> {service_name}")

        max_matches = max(match_counter.values(), default=0)
        if max_matches <= 0:
            return

        for hypothesis_key, count in match_counter.items():
            bucket = buckets.get(hypothesis_key)
            if not bucket:
                continue
            contribution = 0.2 * (count / max_matches)
            bucket["service_score"] += contribution
            bucket["matched_services"].extend(match_details.get(hypothesis_key, []))
            bucket["evidence_items"].append(
                {
                    "source": "service_overlap",
                    "score_contribution": round(contribution, 4),
                    "matched_services": match_details.get(hypothesis_key, [])[:8],
                }
            )

    def _collect_lab_evidence(
        self,
        buckets: dict[str, dict[str, Any]],
        *,
        observations: list[dict[str, Any]] | None,
        case_feature_row: dict[str, Any] | None,
    ) -> None:
        updates: list[DiseaseEvidenceUpdate] = []
        if case_feature_row:
            updates = self.lab_engine.infer_from_case_feature_row(case_feature_row, top_k=12)
        elif observations:
            updates = self.lab_engine.infer(observations, top_k=12)

        if not updates:
            return

        max_support = max((item.support_score for item in updates), default=0.0) or 1.0
        max_exclusion = max((item.exclusion_score for item in updates), default=0.0) or 1.0

        for item in updates:
            hypothesis_key = canonical_hypothesis_key(
                disease_id=as_text(item.disease_key),
                disease_name=as_text(item.disease_name),
                icd10=as_text(item.icd10),
            )
            bucket = self._ensure_bucket(
                buckets,
                hypothesis_key=hypothesis_key,
                disease_name=item.disease_name,
                icd10=item.icd10,
            )
            support_contribution = 0.35 * (item.support_score / max_support) if item.support_score > 0 else 0.0
            exclusion_contribution = 0.45 * (item.exclusion_score / max_exclusion) if item.exclusion_score > 0 else 0.0
            bucket["lab_support_score"] += support_contribution
            bucket["lab_exclusion_score"] += exclusion_contribution
            positive_labels = [
                as_text(evidence.get("observation_concept_name")) or as_text(evidence.get("signal_source_key"))
                for evidence in item.evidences
                if as_text(evidence.get("support_direction")) != "exclude"
            ]
            negative_labels = [
                as_text(evidence.get("observation_concept_name")) or as_text(evidence.get("signal_source_key"))
                for evidence in item.evidences
                if as_text(evidence.get("support_direction")) == "exclude"
            ]
            bucket["supporting_signs"].extend(positive_labels)
            bucket["opposing_signals"].extend(negative_labels)
            bucket["evidence_items"].append(
                {
                    "source": "lab_result",
                    "score_contribution": round(support_contribution - exclusion_contribution, 4),
                    "support_score": round(item.support_score, 4),
                    "exclusion_score": round(item.exclusion_score, 4),
                    "strongest_level": item.strongest_level,
                    "positive_signals": positive_labels[:6],
                    "negative_signals": negative_labels[:6],
                }
            )

    def _collect_memory_evidence(
        self,
        buckets: dict[str, dict[str, Any]],
        *,
        signs: list[str],
        structured_signs: list[dict[str, Any]] | None,
        observed_services: list[dict[str, Any]] | None,
        specialty: str = "",
    ) -> None:
        sign_terms: list[str] = [as_text(item) for item in signs if as_text(item)]
        for item in structured_signs or []:
            concept = as_text(item.get("concept"))
            if concept:
                sign_terms.append(concept)
            sign_terms.extend([as_text(modifier) for modifier in (item.get("modifiers") or []) if as_text(modifier)])

        service_terms = [
            as_text(item.get("service_name")) or as_text(item.get("service_name_raw"))
            for item in observed_services or []
            if as_text(item.get("service_name")) or as_text(item.get("service_name_raw"))
        ]

        for bucket in buckets.values():
            raw_matches = self.experience_memory.query(
                disease_name=as_text(bucket.get("disease_name")),
                specialty=specialty,
                sign_terms=sign_terms,
                service_terms=service_terms,
                scopes=["reasoning", "shared"],
                memory_kinds=["procedural", "semantic", "episodic"],
                min_importance=0.35,
                top_k=6,
            )
            matches = [
                item
                for item in raw_matches
                if text_overlap_score(bucket.get("disease_name"), item.get("disease_name")) >= 0.45
            ]
            if not matches:
                continue

            prior = 0.0
            recommendations: list[str] = []
            categories: list[str] = []
            for match in matches:
                category = as_text(match.get("category"))
                memory_kind = as_text(match.get("memory_kind"))
                recommendation = as_text(match.get("recommendation"))
                if recommendation and recommendation not in recommendations:
                    recommendations.append(recommendation)
                if category and category not in categories:
                    categories.append(category)
                if category == "successful_pattern":
                    prior += 0.05 if memory_kind in {"procedural", "semantic"} else 0.03

            prior = min(prior, 0.1)
            bucket["memory_prior_score"] += prior
            bucket["memory_match_count"] = max(int(bucket.get("memory_match_count") or 0), len(matches))
            bucket["memory_recommendations"].extend([item for item in recommendations if item not in bucket["memory_recommendations"]])
            bucket["memory_categories"].extend([item for item in categories if item not in bucket["memory_categories"]])
            bucket["evidence_items"].append(
                {
                    "source": "experience_memory",
                    "score_contribution": round(prior, 4),
                    "match_count": len(matches),
                    "categories": categories,
                    "recommendations": recommendations[:3],
                }
            )

    def _collect_graph_context_evidence(
        self,
        buckets: dict[str, dict[str, Any]],
        *,
        signs: list[str],
        structured_signs: list[dict[str, Any]] | None,
        observed_services: list[dict[str, Any]] | None,
        seed_disease_hints: list[str] | None = None,
    ) -> None:
        if self.context_retriever is None or not buckets:
            if not seed_disease_hints:
                return

        sign_terms: list[str] = [as_text(item) for item in signs if as_text(item)]
        for item in structured_signs or []:
            concept = as_text(item.get("concept"))
            if concept:
                sign_terms.append(concept)
            sign_terms.extend([as_text(modifier) for modifier in (item.get("modifiers") or []) if as_text(modifier)])

        service_terms = [
            as_text(item.get("service_name")) or as_text(item.get("service_name_raw"))
            for item in observed_services or []
            if as_text(item.get("service_name")) or as_text(item.get("service_name_raw"))
        ]

        base_terms = unique_terms(sign_terms + service_terms)

        normalized_hint_map: dict[str, str] = {}
        for hint in seed_disease_hints or []:
            text = as_text(hint)
            if text:
                normalized_hint_map[normalize_text(text)] = text

        if normalized_hint_map:
            for hint in normalized_hint_map.values():
                hypothesis_key = canonical_hypothesis_key(disease_name=hint, icd10="")
                self._ensure_bucket(
                    buckets,
                    hypothesis_key=hypothesis_key,
                    disease_name=hint,
                    icd10="",
                )

        for bucket in buckets.values():
            disease_name = as_text(bucket.get("disease_name"))
            query_parts = [disease_name]
            query_parts.extend(base_terms[:10])
            query_text = " | ".join([part for part in query_parts if part])
            if not query_text:
                continue

            try:
                retrieved = self.context_retriever.retrieve(
                    query_text=query_text,
                    namespace=self.graph_namespace,
                    disease_hint=disease_name,
                    top_k_summary=4,
                    top_k_assertion=6,
                    top_k_chunk=6,
                )
            except Exception:
                continue

            candidates = [
                item
                for item in retrieved.get("candidate_diseases") or []
                if text_overlap_score(disease_name, item.get("disease_name")) >= 0.45
            ]
            if not candidates:
                continue

            best_candidate = candidates[0]
            best_score = float(best_candidate.get("retrieval_score") or 0.0)
            top_score = max(float(item.get("retrieval_score") or 0.0) for item in retrieved.get("candidate_diseases") or [best_candidate]) or 1.0
            overlap_with_hint = max(
                [text_overlap_score(disease_name, hint_text) for hint_text in normalized_hint_map.values()] or [0.0]
            )
            contribution = 0.16 * (best_score / top_score)
            if overlap_with_hint >= 0.45:
                contribution += 0.08

            disease_id = as_text(best_candidate.get("disease_id"))
            matching_summaries = [
                item for item in (retrieved.get("summary_hits") or [])
                if as_text(item.get("disease_id")) == disease_id
            ]
            matching_assertions = [
                item for item in (retrieved.get("assertion_hits") or [])
                if as_text(item.get("disease_id")) == disease_id
            ]
            matching_chunks = [
                item for item in (retrieved.get("chunk_hits") or [])
                if as_text(item.get("disease_id")) == disease_id
            ]

            snippets: list[str] = []
            for item in matching_summaries[:1]:
                summary_text = as_text(item.get("summary_text"))
                if summary_text:
                    snippets.append(f"summary: {summary_text[:140]}")
                for service_name in item.get("key_services") or []:
                    text = as_text(service_name)
                    if text and text not in bucket["required_services"]:
                        bucket["required_services"].append(text)
            for item in matching_assertions[:2]:
                assertion_text = as_text(item.get("assertion_text"))
                if assertion_text:
                    snippets.append(f"assertion: {assertion_text[:140]}")
            for item in matching_chunks[:2]:
                preview = as_text(item.get("body_preview"))
                if preview:
                    snippets.append(f"chunk: {preview[:140]}")

            deduped_snippets = []
            for snippet in snippets:
                text = as_text(snippet)
                if text and text not in deduped_snippets:
                    deduped_snippets.append(text)

            bucket["graph_context_score"] += contribution
            bucket["graph_context_match_count"] = max(
                int(bucket.get("graph_context_match_count") or 0),
                len(matching_summaries) + len(matching_assertions) + len(matching_chunks),
            )
            for snippet in deduped_snippets:
                if snippet not in bucket["graph_context_snippets"]:
                    bucket["graph_context_snippets"].append(snippet)
            bucket["evidence_items"].append(
                {
                    "source": "graph_context_retriever",
                    "score_contribution": round(contribution, 4),
                    "candidate_disease": disease_name,
                    "retrieval_score": round(best_score, 4),
                    "hit_sources": best_candidate.get("hit_sources") or [],
                    "summary_hits": len(matching_summaries),
                    "assertion_hits": len(matching_assertions),
                    "chunk_hits": len(matching_chunks),
                    "snippets": deduped_snippets[:4],
                }
            )

        if not normalized_hint_map:
            return

        seed_buckets = {
            normalize_text(as_text(bucket.get("disease_name"))): bucket
            for bucket in buckets.values()
            if as_text(bucket.get("disease_name"))
        }
        for hint_text in normalized_hint_map.values():
            query_parts = [hint_text]
            query_parts.extend(base_terms[:10])
            query_text = " | ".join([part for part in query_parts if part])
            if not query_text:
                continue
            try:
                retrieved = self.context_retriever.retrieve(
                    query_text=query_text,
                    namespace=self.graph_namespace,
                    disease_hint=hint_text,
                    top_k_summary=4,
                    top_k_assertion=6,
                    top_k_chunk=6,
                )
            except Exception:
                continue

            candidates = [
                item
                for item in (retrieved.get("candidate_diseases") or [])
                if text_overlap_score(hint_text, item.get("disease_name")) >= 0.34
            ]
            if not candidates:
                continue

            best_candidate = candidates[0]
            candidate_name = as_text(best_candidate.get("disease_name")) or hint_text
            hint_key = normalize_text(candidate_name)
            bucket = seed_buckets.get(hint_key)
            if bucket is None:
                hypothesis_key = canonical_hypothesis_key(
                    disease_id=as_text(best_candidate.get("disease_id")),
                    disease_name=candidate_name,
                    icd10="",
                )
                bucket = self._ensure_bucket(
                    buckets,
                    hypothesis_key=hypothesis_key,
                    disease_name=candidate_name,
                    icd10="",
                )
                seed_buckets[hint_key] = bucket

            best_score = float(best_candidate.get("retrieval_score") or 0.0)
            top_score = max(float(item.get("retrieval_score") or 0.0) for item in candidates) or 1.0
            overlap_with_hint = text_overlap_score(hint_text, candidate_name)
            contribution = 0.22 * (best_score / top_score)
            if overlap_with_hint >= 0.45:
                contribution += 0.24
            elif overlap_with_hint >= 0.3:
                contribution += 0.14

            disease_id = as_text(best_candidate.get("disease_id"))
            matching_summaries = [
                item for item in (retrieved.get("summary_hits") or [])
                if as_text(item.get("disease_id")) == disease_id
            ]
            matching_assertions = [
                item for item in (retrieved.get("assertion_hits") or [])
                if as_text(item.get("disease_id")) == disease_id
            ]
            matching_chunks = [
                item for item in (retrieved.get("chunk_hits") or [])
                if as_text(item.get("disease_id")) == disease_id
            ]

            snippets: list[str] = []
            for item in matching_summaries[:1]:
                summary_text = as_text(item.get("summary_text"))
                if summary_text:
                    snippets.append(f"summary: {summary_text[:140]}")
                for service_name in item.get("key_services") or []:
                    text = as_text(service_name)
                    if text and text not in bucket["required_services"]:
                        bucket["required_services"].append(text)
            for item in matching_assertions[:2]:
                assertion_text = as_text(item.get("assertion_text"))
                if assertion_text:
                    snippets.append(f"assertion: {assertion_text[:140]}")
            for item in matching_chunks[:2]:
                preview = as_text(item.get("body_preview"))
                if preview:
                    snippets.append(f"chunk: {preview[:140]}")

            deduped_snippets = []
            for snippet in snippets:
                text = as_text(snippet)
                if text and text not in deduped_snippets:
                    deduped_snippets.append(text)

            bucket["graph_context_score"] += contribution
            bucket["graph_context_match_count"] = max(
                int(bucket.get("graph_context_match_count") or 0),
                len(matching_summaries) + len(matching_assertions) + len(matching_chunks),
            )
            for snippet in deduped_snippets:
                if snippet not in bucket["graph_context_snippets"]:
                    bucket["graph_context_snippets"].append(snippet)
            bucket["evidence_items"].append(
                {
                    "source": "graph_context_seed",
                    "score_contribution": round(contribution, 4),
                    "candidate_disease": candidate_name,
                    "retrieval_score": round(best_score, 4),
                    "hit_sources": best_candidate.get("hit_sources") or [],
                    "summary_hits": len(matching_summaries),
                    "assertion_hits": len(matching_assertions),
                    "chunk_hits": len(matching_chunks),
                    "seed_hint": hint_text,
                    "snippets": deduped_snippets[:4],
                }
            )

    @staticmethod
    def _finalize_bucket(bucket: dict[str, Any]) -> DiseaseHypothesis:
        positive = (
            float(bucket["sign_score"])
            + float(bucket["service_score"])
            + float(bucket["lab_support_score"])
            + float(bucket["memory_prior_score"])
            + float(bucket["graph_context_score"])
        )
        exclusion = float(bucket["lab_exclusion_score"])
        score = positive - exclusion
        confidence = positive / max(positive + exclusion + 0.15, 0.15)
        if exclusion >= max(positive * 1.05, 0.25):
            status = "ruled_out"
        elif positive >= 0.72 and confidence >= 0.6:
            status = "confirmed"
        else:
            status = "active"

        supporting_signs = []
        for item in bucket["supporting_signs"]:
            text = as_text(item)
            if text and text not in supporting_signs:
                supporting_signs.append(text)

        opposing_signals = []
        for item in bucket["opposing_signals"]:
            text = as_text(item)
            if text and text not in opposing_signals:
                opposing_signals.append(text)

        matched_services = []
        for item in bucket["matched_services"]:
            text = as_text(item)
            if text and text not in matched_services:
                matched_services.append(text)

        required_services = []
        for item in bucket["required_services"]:
            text = as_text(item)
            if text and text not in required_services:
                required_services.append(text)

        evidence_items = sorted(
            bucket["evidence_items"],
            key=lambda row: float(row.get("score_contribution") or 0.0),
            reverse=True,
        )

        return DiseaseHypothesis(
            hypothesis_key=bucket["hypothesis_key"],
            disease_name=bucket["disease_name"],
            icd10=bucket["icd10"],
            status=status,
            score=round(score, 4),
            confidence=round(max(0.0, min(1.0, confidence)), 4),
            sign_score=round(float(bucket["sign_score"]), 4),
            service_score=round(float(bucket["service_score"]), 4),
            lab_support_score=round(float(bucket["lab_support_score"]), 4),
            lab_exclusion_score=round(float(bucket["lab_exclusion_score"]), 4),
            memory_prior_score=round(float(bucket["memory_prior_score"]), 4),
            graph_context_score=round(float(bucket["graph_context_score"]), 4),
            graph_context_match_count=int(bucket["graph_context_match_count"] or 0),
            memory_match_count=int(bucket["memory_match_count"] or 0),
            supporting_signs=supporting_signs[:12],
            opposing_signals=opposing_signals[:12],
            matched_services=matched_services[:12],
            required_services=required_services[:12],
            memory_recommendations=[as_text(item) for item in bucket["memory_recommendations"][:5] if as_text(item)],
            memory_categories=[as_text(item) for item in bucket["memory_categories"][:5] if as_text(item)],
            graph_context_snippets=[as_text(item) for item in bucket["graph_context_snippets"][:5] if as_text(item)],
            evidence_items=evidence_items[:12],
        )

    def infer(
        self,
        *,
        signs: list[str],
        structured_signs: list[dict[str, Any]] | None = None,
        patient_context: dict[str, Any] | None = None,
        observed_services: list[dict[str, Any]] | None = None,
        observations: list[dict[str, Any]] | None = None,
        case_feature_row: dict[str, Any] | None = None,
        specialty: str = "",
        seed_disease_hints: list[str] | None = None,
        top_k: int = 8,
    ) -> dict[str, Any]:
        buckets: dict[str, dict[str, Any]] = {}
        disease_to_services, required_services, sign_payload = self._collect_sign_evidence(
            buckets,
            signs=signs,
            structured_signs=structured_signs,
            patient_context=patient_context,
        )
        for hypothesis_key, names in required_services.items():
            if hypothesis_key in buckets:
                buckets[hypothesis_key]["required_services"].extend(names)

        self._collect_service_overlap_evidence(
            buckets,
            observed_services=observed_services,
            disease_to_services=disease_to_services,
        )
        self._collect_lab_evidence(
            buckets,
            observations=observations,
            case_feature_row=case_feature_row,
        )
        self._collect_memory_evidence(
            buckets,
            signs=signs,
            structured_signs=structured_signs,
            observed_services=observed_services,
            specialty=specialty,
        )
        self._collect_graph_context_evidence(
            buckets,
            signs=signs,
            structured_signs=structured_signs,
            observed_services=observed_services,
            seed_disease_hints=seed_disease_hints,
        )

        hypotheses = [self._finalize_bucket(bucket) for bucket in buckets.values()]
        hypotheses.sort(key=lambda item: (item.score, item.confidence, item.disease_name), reverse=True)
        hypotheses = hypotheses[:top_k]

        return {
            "input_signs": [as_text(item) for item in signs if as_text(item)],
            "structured_signs": structured_signs or [],
            "patient_context": patient_context or {},
            "seed_disease_hints": [as_text(item) for item in (seed_disease_hints or []) if as_text(item)],
            "hypotheses": [
                {
                    "hypothesis_key": item.hypothesis_key,
                    "disease_name": item.disease_name,
                    "icd10": item.icd10,
                    "status": item.status,
                    "score": item.score,
                    "confidence": item.confidence,
                    "sign_score": item.sign_score,
                    "service_score": item.service_score,
                    "lab_support_score": item.lab_support_score,
                    "lab_exclusion_score": item.lab_exclusion_score,
                    "memory_prior_score": item.memory_prior_score,
                    "graph_context_score": item.graph_context_score,
                    "graph_context_match_count": item.graph_context_match_count,
                    "memory_match_count": item.memory_match_count,
                    "supporting_signs": item.supporting_signs,
                    "opposing_signals": item.opposing_signals,
                    "matched_services": item.matched_services,
                    "required_services": item.required_services,
                    "memory_recommendations": item.memory_recommendations,
                    "memory_categories": item.memory_categories,
                    "graph_context_snippets": item.graph_context_snippets,
                    "evidence_items": item.evidence_items,
                }
                for item in hypotheses
            ],
            "sign_payload": sign_payload,
        }
