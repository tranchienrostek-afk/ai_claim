from __future__ import annotations

import json
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

try:
    from lab_result_interpreter import classify_lab_result_signal
except ModuleNotFoundError:  # pragma: no cover
    sys.path.append(str(Path(__file__).resolve().parent))
    from lab_result_interpreter import classify_lab_result_signal


PROJECT_DIR = Path(__file__).resolve().parent.parent
DEFAULT_CATALOG_JSON = PROJECT_DIR / "05_reference" / "phac_do" / "tmh_lab_result_disease_catalog.json"


def as_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    return str(value).strip()


@dataclass(frozen=True)
class DiseaseEvidenceUpdate:
    disease_key: str
    disease_name: str
    icd10: str
    score: float
    support_score: float
    exclusion_score: float
    evidence_count: int
    strongest_level: str
    evidences: list[dict[str, Any]]


class LabResultDiseaseInferenceEngine:
    def __init__(self, catalog_path: Path = DEFAULT_CATALOG_JSON):
        payload = json.loads(catalog_path.read_text(encoding="utf-8"))
        self.catalog_path = catalog_path
        self.summary = payload.get("summary") or {}
        self.diseases = {row["entity_key"]: row for row in payload.get("diseases") or []}
        self.signal_sources = {row["source_key"]: row for row in payload.get("signal_sources") or []}
        self.signal_profiles = {row["profile_id"]: row for row in payload.get("signal_profiles") or []}
        self.links_by_source: dict[str, list[dict[str, Any]]] = {}
        for row in (payload.get("relationships") or {}).get("disease_signal") or []:
            self.links_by_source.setdefault(row["signal_source_key"], []).append(row)

    def _service_source_keys(self, observation: dict[str, Any]) -> list[str]:
        service_code = as_text(observation.get("service_code"))
        return [f"service_signal:{service_code}"] if service_code else []

    def _matching_profiles(self, source: dict[str, Any], observation: dict[str, Any]) -> list[dict[str, Any]]:
        signal = classify_lab_result_signal(observation)
        allowed = set(source.get("allowed_profile_ids") or [])
        matches: list[dict[str, Any]] = []
        for profile_id in allowed:
            profile = self.signal_profiles.get(profile_id)
            if not profile:
                continue
            trigger = profile.get("trigger") or {}
            ok = True
            if trigger.get("positive") and not signal["is_positive"]:
                ok = False
            if trigger.get("negative") and not signal["is_negative"]:
                ok = False
            if trigger.get("abnormal") and not signal["is_abnormal"]:
                ok = False
            if trigger.get("narrative") and not signal["is_narrative"]:
                ok = False
            if ok:
                matches.append(profile)
        return matches

    def _matching_profiles_from_feature_bucket(
        self,
        source: dict[str, Any],
        feature_bucket: dict[str, Any],
    ) -> list[dict[str, Any]]:
        allowed = set(source.get("allowed_profile_ids") or [])
        has_positive = int(feature_bucket.get("positive_count", 0)) > 0
        has_negative = int(feature_bucket.get("negative_count", 0)) > 0
        has_abnormal = (
            int(feature_bucket.get("abnormal_count", 0)) > 0
            or int(feature_bucket.get("high_count", 0)) > 0
            or int(feature_bucket.get("low_count", 0)) > 0
        )
        has_narrative = int(feature_bucket.get("narrative_count", 0)) > 0

        matches: list[dict[str, Any]] = []
        for profile_id in allowed:
            profile = self.signal_profiles.get(profile_id)
            if not profile:
                continue
            trigger = profile.get("trigger") or {}
            ok = True
            if trigger.get("positive") and not has_positive:
                ok = False
            if trigger.get("negative") and not has_negative:
                ok = False
            if trigger.get("abnormal") and not has_abnormal:
                ok = False
            if trigger.get("narrative") and not has_narrative:
                ok = False
            if ok:
                matches.append(profile)
        return matches

    def _build_concept_feature_buckets(self, observations: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
        concept_buckets: dict[str, dict[str, Any]] = {}
        for observation in observations:
            concept_code = as_text(observation.get("observation_concept_code"))
            if not concept_code:
                continue
            bucket = concept_buckets.setdefault(
                concept_code,
                {
                    "concept_code": concept_code,
                    "concept_name": as_text(observation.get("observation_concept_name")),
                    "service_codes": set(),
                    "observed_count": 0,
                    "positive_count": 0,
                    "negative_count": 0,
                    "normal_count": 0,
                    "abnormal_count": 0,
                    "high_count": 0,
                    "low_count": 0,
                    "narrative_count": 0,
                },
            )
            signal = classify_lab_result_signal(observation)
            abnormality = as_text(observation.get("abnormality")).lower()
            bucket["observed_count"] += 1
            bucket["positive_count"] += int(signal["is_positive"])
            bucket["negative_count"] += int(signal["is_negative"])
            bucket["normal_count"] += int(signal["is_normal"])
            bucket["abnormal_count"] += int(signal["is_abnormal"])
            bucket["high_count"] += int(abnormality == "high")
            bucket["low_count"] += int(abnormality == "low")
            bucket["narrative_count"] += int(signal["is_narrative"])
            service_code = as_text(observation.get("service_code"))
            if service_code:
                bucket["service_codes"].add(service_code)

        for bucket in concept_buckets.values():
            bucket["service_codes"] = sorted(bucket["service_codes"])
        return concept_buckets

    def _apply_profiles_to_links(
        self,
        *,
        disease_scores: dict[str, dict[str, Any]],
        source_key: str,
        profiles: list[dict[str, Any]],
        evidence_payload: dict[str, Any],
    ) -> None:
        level_rank = {"weak": 1, "moderate": 2, "strong": 3}
        source = self.signal_sources.get(source_key)
        if not source or not profiles:
            return

        for link in self.links_by_source.get(source_key) or []:
            disease = self.diseases.get(link["disease_key"])
            if not disease:
                continue
            bucket = disease_scores.setdefault(
                link["disease_key"],
                {
                    "disease_name": disease.get("disease_name", ""),
                    "icd10": disease.get("icd10", ""),
                    "support_score": 0.0,
                    "exclusion_score": 0.0,
                    "strongest_level": "weak",
                    "evidences": [],
                },
            )
            for profile in profiles:
                magnitude = (
                    float(profile.get("weight", 1.0))
                    * float(link.get("evidence_weight", 1.0))
                    * float(link.get("confidence_weight", 1.0))
                )
                if profile.get("support_direction") == "exclude":
                    bucket["exclusion_score"] += magnitude
                else:
                    bucket["support_score"] += magnitude

                if level_rank.get(profile.get("support_level", "weak"), 1) > level_rank.get(
                    bucket["strongest_level"], 1
                ):
                    bucket["strongest_level"] = profile.get("support_level", "weak")

                bucket["evidences"].append(
                    {
                        "signal_source_key": source_key,
                        "profile_id": profile.get("profile_id"),
                        "support_direction": profile.get("support_direction"),
                        "support_level": profile.get("support_level"),
                        "service_key": link.get("service_key"),
                        "link_mode": link.get("link_mode"),
                        "link_confidence": link.get("link_confidence"),
                        "score_contribution": round(magnitude, 4),
                        **evidence_payload,
                    }
                )

    def _finalize_updates(
        self,
        disease_scores: dict[str, dict[str, Any]],
        top_k: int,
    ) -> list[DiseaseEvidenceUpdate]:
        updates: list[DiseaseEvidenceUpdate] = []
        for disease_key, bucket in disease_scores.items():
            support_score = float(bucket["support_score"])
            exclusion_score = float(bucket["exclusion_score"])
            updates.append(
                DiseaseEvidenceUpdate(
                    disease_key=disease_key,
                    disease_name=bucket["disease_name"],
                    icd10=bucket["icd10"],
                    score=round(support_score - exclusion_score, 4),
                    support_score=round(support_score, 4),
                    exclusion_score=round(exclusion_score, 4),
                    evidence_count=len(bucket["evidences"]),
                    strongest_level=bucket["strongest_level"],
                    evidences=sorted(
                        bucket["evidences"],
                        key=lambda row: (-row["score_contribution"], row["profile_id"]),
                    ),
                )
            )

        updates.sort(key=lambda row: (-row.score, -row.support_score, row.disease_name))
        return updates[:top_k]

    def infer_from_case_feature_row(
        self,
        case_feature_row: dict[str, Any],
        top_k: int = 10,
    ) -> list[DiseaseEvidenceUpdate]:
        disease_scores: dict[str, dict[str, Any]] = {}
        concept_features = case_feature_row.get("concept_features") or {}
        for concept_code, feature_bucket in concept_features.items():
            source_key = f"obs:{concept_code}"
            source = self.signal_sources.get(source_key)
            if not source:
                continue
            profiles = self._matching_profiles_from_feature_bucket(source, feature_bucket)
            if not profiles:
                continue
            self._apply_profiles_to_links(
                disease_scores=disease_scores,
                source_key=source_key,
                profiles=profiles,
                evidence_payload={
                    "evidence_mode": "case_feature",
                    "observation_concept_code": concept_code,
                    "observation_concept_name": as_text(feature_bucket.get("concept_name")),
                    "service_codes": list(feature_bucket.get("service_codes") or []),
                    "result_flag": "",
                    "polarity": "",
                    "abnormality": "",
                    "feature_summary": {
                        "observed_count": int(feature_bucket.get("observed_count", 0)),
                        "positive_count": int(feature_bucket.get("positive_count", 0)),
                        "negative_count": int(feature_bucket.get("negative_count", 0)),
                        "normal_count": int(feature_bucket.get("normal_count", 0)),
                        "abnormal_count": int(feature_bucket.get("abnormal_count", 0)),
                        "high_count": int(feature_bucket.get("high_count", 0)),
                        "low_count": int(feature_bucket.get("low_count", 0)),
                        "narrative_count": int(feature_bucket.get("narrative_count", 0)),
                    },
                },
            )
        return self._finalize_updates(disease_scores, top_k)

    def infer(self, observations: list[dict[str, Any]], top_k: int = 10) -> list[DiseaseEvidenceUpdate]:
        disease_scores: dict[str, dict[str, Any]] = {}

        for observation in observations:
            for source_key in self._service_source_keys(observation):
                source = self.signal_sources.get(source_key)
                if not source:
                    continue
                profiles = self._matching_profiles(source, observation)
                if not profiles:
                    continue
                self._apply_profiles_to_links(
                    disease_scores=disease_scores,
                    source_key=source_key,
                    profiles=profiles,
                    evidence_payload={
                        "evidence_mode": "direct_observation",
                        "observation_concept_code": as_text(observation.get("observation_concept_code")),
                        "observation_concept_name": as_text(observation.get("observation_concept_name")),
                        "service_code": as_text(observation.get("service_code")),
                        "result_flag": as_text(observation.get("result_flag")),
                        "polarity": as_text(observation.get("polarity")),
                        "abnormality": as_text(observation.get("abnormality")),
                    },
                )

        concept_buckets = self._build_concept_feature_buckets(observations)
        for concept_code, feature_bucket in concept_buckets.items():
            source_key = f"obs:{concept_code}"
            source = self.signal_sources.get(source_key)
            if not source:
                continue
            profiles = self._matching_profiles_from_feature_bucket(source, feature_bucket)
            if not profiles:
                continue
            self._apply_profiles_to_links(
                disease_scores=disease_scores,
                source_key=source_key,
                profiles=profiles,
                evidence_payload={
                    "evidence_mode": "case_feature_from_observations",
                    "observation_concept_code": concept_code,
                    "observation_concept_name": as_text(feature_bucket.get("concept_name")),
                    "service_codes": list(feature_bucket.get("service_codes") or []),
                    "result_flag": "",
                    "polarity": "",
                    "abnormality": "",
                    "feature_summary": {
                        "observed_count": int(feature_bucket.get("observed_count", 0)),
                        "positive_count": int(feature_bucket.get("positive_count", 0)),
                        "negative_count": int(feature_bucket.get("negative_count", 0)),
                        "normal_count": int(feature_bucket.get("normal_count", 0)),
                        "abnormal_count": int(feature_bucket.get("abnormal_count", 0)),
                        "high_count": int(feature_bucket.get("high_count", 0)),
                        "low_count": int(feature_bucket.get("low_count", 0)),
                        "narrative_count": int(feature_bucket.get("narrative_count", 0)),
                    },
                },
            )

        return self._finalize_updates(disease_scores, top_k)
