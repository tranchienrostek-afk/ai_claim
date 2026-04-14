from __future__ import annotations

import json
import unicodedata
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from adjudication_mvp import AdjudicationMVP, icd_group, normalize_icd
from sign_to_service_engine import SignToServiceEngine


PROJECT_DIR = Path(__file__).parent.parent
TMH_PROTOCOL_LINKS_PATH = PROJECT_DIR / "05_reference" / "phac_do" / "tmh_protocol_text_service_links.json"


def normalize_text(value: Any) -> str:
    text = str(value or "").strip().lower()
    text = text.replace("đ", "d").replace("Đ", "d")
    normalized = unicodedata.normalize("NFD", text)
    text = "".join(ch for ch in normalized if unicodedata.category(ch) != "Mn")
    return " ".join(text.split())


@dataclass
class EvidenceResult:
    supported: bool
    confidence: float
    reason: str
    evidence_flags: list[str]
    branch: str
    catalog_coverage: bool
    matched_items: list[dict[str, Any]]


class KnownDiseaseComparer:
    def __init__(self, mvp: AdjudicationMVP, protocol_links_path: Path = TMH_PROTOCOL_LINKS_PATH) -> None:
        self.mvp = mvp
        self.protocol_payload = json.loads(protocol_links_path.read_text(encoding="utf-8")) if protocol_links_path.exists() else {"links": []}

        self.protocol_by_icd: dict[str, list[dict[str, Any]]] = {}
        self.matrix_by_icd: dict[str, list[dict[str, Any]]] = {}

        for link in self.protocol_payload.get("links", []):
            icd10 = normalize_icd(link.get("icd10"))
            if icd10:
                self.protocol_by_icd.setdefault(icd10, []).append(link)

        for link in self.mvp.matrix_payload.get("links", []):
            icd10 = normalize_icd(link.get("icd10"))
            if icd10:
                self.matrix_by_icd.setdefault(icd10, []).append(link)

    def _protocol_matches(self, service_code: str, primary_icd: str) -> list[dict[str, Any]]:
        matches: list[dict[str, Any]] = []
        for link in self.protocol_by_icd.get(normalize_icd(primary_icd), []):
            if str(link.get("service_code") or "").strip().upper() == service_code.upper():
                matches.append(
                    {
                        "source": "tmh_protocol",
                        "service_code": service_code,
                        "icd10": normalize_icd(primary_icd),
                        "service_name": str(link.get("service_name") or ""),
                        "disease_name": str(link.get("disease_name") or ""),
                    }
                )
        return matches

    def _matrix_matches(self, service_code: str, primary_icd: str) -> list[dict[str, Any]]:
        matches: list[dict[str, Any]] = []
        icd_full = normalize_icd(primary_icd)
        icd3 = icd_group(primary_icd)
        for link in self.matrix_by_icd.get(icd_full, []):
            if str(link.get("service_code") or "").strip().upper() == service_code.upper():
                matches.append(
                    {
                        "source": "service_disease_matrix",
                        "service_code": service_code,
                        "icd10": icd_full,
                        "service_name": str(link.get("service_name") or ""),
                        "disease_name": str(link.get("disease_name") or ""),
                        "score": float(link.get("score") or 0.0),
                    }
                )
        if not matches and icd3:
            for link in self.mvp.links_by_service.get(service_code, []):
                if str(link.get("icd10_group") or "") == icd3:
                    matches.append(
                        {
                            "source": "service_disease_matrix_group",
                            "service_code": service_code,
                            "icd10": icd3,
                            "service_name": str(link.get("service_name") or ""),
                            "disease_name": str(link.get("disease_name") or ""),
                            "score": float(link.get("score") or 0.0),
                        }
                    )
        return matches

    def _has_catalog_for_icd(self, primary_icd: str) -> bool:
        icd_full = normalize_icd(primary_icd)
        if not icd_full:
            return False
        if self.protocol_by_icd.get(icd_full):
            return True
        if self.matrix_by_icd.get(icd_full):
            return True
        icd3 = icd_group(primary_icd)
        if not icd3:
            return False
        for links in self.mvp.links_by_service.values():
            for link in links:
                if str(link.get("icd10_group") or "") == icd3:
                    return True
        return False

    def compare(self, service_code: str, primary_icd: str) -> EvidenceResult:
        if not service_code or not normalize_icd(primary_icd):
            return EvidenceResult(
                supported=False,
                confidence=0.0,
                reason="Missing service_code or ICD for known-disease comparison.",
                evidence_flags=[],
                branch="known_disease",
                catalog_coverage=False,
                matched_items=[],
            )

        protocol_matches = self._protocol_matches(service_code, primary_icd)
        matrix_matches = self._matrix_matches(service_code, primary_icd)
        coverage = self._has_catalog_for_icd(primary_icd)

        if protocol_matches:
            return EvidenceResult(
                supported=True,
                confidence=0.95,
                reason="Service is explicitly listed in the protocol/paraclinical catalog for the known ICD.",
                evidence_flags=["protocol_icd_support"],
                branch="known_disease",
                catalog_coverage=True,
                matched_items=protocol_matches,
            )
        if matrix_matches:
            best_score = max(float(item.get("score") or 0.0) for item in matrix_matches)
            return EvidenceResult(
                supported=True,
                confidence=round(max(0.72, min(0.9, 0.55 + best_score * 0.35)), 4),
                reason="Service is supported by the service-disease matrix for the known ICD/group.",
                evidence_flags=["matrix_icd_support"],
                branch="known_disease",
                catalog_coverage=True,
                matched_items=matrix_matches,
            )
        if coverage:
            return EvidenceResult(
                supported=False,
                confidence=0.78,
                reason="Known disease catalog exists but this service is not listed for the ICD/group.",
                evidence_flags=["known_disease_catalog_miss"],
                branch="known_disease",
                catalog_coverage=True,
                matched_items=[],
            )
        return EvidenceResult(
            supported=False,
            confidence=0.0,
            reason="No disease catalog coverage found for the ICD, so known-disease comparison cannot conclude.",
            evidence_flags=[],
            branch="known_disease",
            catalog_coverage=False,
            matched_items=[],
        )


class SignBasedComparer:
    def __init__(self, sign_engine: SignToServiceEngine) -> None:
        self.sign_engine = sign_engine
        self.cache: dict[tuple[str, ...], dict[str, Any]] = {}

    def _infer(self, signs: list[str]) -> dict[str, Any]:
        key = tuple(sign for sign in (str(item or "").strip() for item in signs) if sign)
        if key not in self.cache:
            self.cache[key] = self.sign_engine.infer_from_signs(list(key)) if key else {"recommended_services": []}
        return self.cache[key]

    def compare(self, service_code: str, signs: list[str]) -> EvidenceResult:
        cleaned_signs = [str(item or "").strip() for item in signs if str(item or "").strip()]
        if not service_code or not cleaned_signs:
            return EvidenceResult(
                supported=False,
                confidence=0.0,
                reason="Missing service_code or initial signs for sign-based comparison.",
                evidence_flags=[],
                branch="sign_based",
                catalog_coverage=False,
                matched_items=[],
            )

        inference = self._infer(cleaned_signs)
        recommended = inference.get("recommended_services") or []
        if not recommended:
            return EvidenceResult(
                supported=False,
                confidence=0.0,
                reason="No sign-based service recommendations were produced.",
                evidence_flags=[],
                branch="sign_based",
                catalog_coverage=False,
                matched_items=[],
            )

        top_score = float(recommended[0].get("score") or 0.0)
        for index, item in enumerate(recommended, start=1):
            if str(item.get("service_code") or "").strip().upper() != service_code.upper():
                continue
            service_score = float(item.get("score") or 0.0)
            relative = (service_score / top_score) if top_score > 0 else 0.0
            if index <= 5 and relative >= 0.35:
                return EvidenceResult(
                    supported=True,
                    confidence=0.82,
                    reason="Initial signs strongly point to this service via top suspected diseases.",
                    evidence_flags=["sign_service_support_strong"],
                    branch="sign_based",
                    catalog_coverage=True,
                    matched_items=[{"rank": index, "relative_score": round(relative, 4), "service_name": item.get("service_name")}],
                )
            if index <= 10 and relative >= 0.2:
                return EvidenceResult(
                    supported=True,
                    confidence=0.68,
                    reason="Initial signs moderately point to this service via suspected diseases.",
                    evidence_flags=["sign_service_support_moderate"],
                    branch="sign_based",
                    catalog_coverage=True,
                    matched_items=[{"rank": index, "relative_score": round(relative, 4), "service_name": item.get("service_name")}],
                )
            return EvidenceResult(
                supported=False,
                confidence=0.0,
                reason="The service appears in the long tail of sign-based recommendations only.",
                evidence_flags=[],
                branch="sign_based",
                catalog_coverage=True,
                matched_items=[{"rank": index, "relative_score": round(relative, 4), "service_name": item.get("service_name")}],
            )

        return EvidenceResult(
            supported=False,
            confidence=0.7,
            reason="Initial signs produce a service catalog, but this service is outside the recommended set.",
            evidence_flags=["sign_catalog_miss"],
            branch="sign_based",
            catalog_coverage=True,
            matched_items=[],
        )


class CleanMedicalStep1EngineV1:
    def __init__(self) -> None:
        self.mvp = AdjudicationMVP()
        self.sign_engine = SignToServiceEngine()
        self.known_disease = KnownDiseaseComparer(self.mvp)
        self.sign_based = SignBasedComparer(self.sign_engine)

    def map_service(self, service_name: str) -> dict[str, Any]:
        return self.mvp.recognize_service(service_name)

    def assess(
        self,
        *,
        service_name: str,
        primary_icd: str,
        diagnosis_text: str,
        initial_signs: list[str] | None,
    ) -> dict[str, Any]:
        service_info = self.map_service(service_name)
        mapping_resolution = str(service_info.get("mapping_status") or "unknown")
        service_code = str(service_info.get("service_code") or "")
        signs = list(initial_signs or [])

        if mapping_resolution not in {"exact", "probable"} or not service_code:
            return {
                "decision": "UNCERTAIN",
                "medical_necessity_status": "uncertain",
                "confidence": 0.28,
                "reason": "Service mapping is not reliable enough to enter the medical core.",
                "branch_used": "mapping_gate",
                "service_mapping": service_info,
                "known_disease_evidence": None,
                "sign_based_evidence": None,
            }

        known_result: EvidenceResult | None = None
        sign_result: EvidenceResult | None = None

        if normalize_icd(primary_icd):
            known_result = self.known_disease.compare(service_code, primary_icd)
            if known_result.catalog_coverage:
                if known_result.supported:
                    return {
                        "decision": "JUSTIFIED",
                        "medical_necessity_status": "supported_by_known_disease",
                        "confidence": known_result.confidence,
                        "reason": known_result.reason,
                        "branch_used": "known_disease",
                        "service_mapping": service_info,
                        "known_disease_evidence": known_result.__dict__,
                        "sign_based_evidence": None,
                    }
                return {
                    "decision": "UNJUSTIFIED",
                    "medical_necessity_status": "not_medically_supported",
                    "confidence": known_result.confidence,
                    "reason": known_result.reason,
                    "branch_used": "known_disease",
                    "service_mapping": service_info,
                    "known_disease_evidence": known_result.__dict__,
                    "sign_based_evidence": None,
                }

        if signs:
            sign_result = self.sign_based.compare(service_code, signs)
            if sign_result.catalog_coverage:
                if sign_result.supported:
                    return {
                        "decision": "JUSTIFIED",
                        "medical_necessity_status": "supported_by_initial_signs",
                        "confidence": sign_result.confidence,
                        "reason": sign_result.reason,
                        "branch_used": "sign_based",
                        "service_mapping": service_info,
                        "known_disease_evidence": known_result.__dict__ if known_result else None,
                        "sign_based_evidence": sign_result.__dict__,
                    }
                return {
                    "decision": "UNJUSTIFIED",
                    "medical_necessity_status": "not_medically_supported",
                    "confidence": sign_result.confidence,
                    "reason": sign_result.reason,
                    "branch_used": "sign_based",
                    "service_mapping": service_info,
                    "known_disease_evidence": known_result.__dict__ if known_result else None,
                    "sign_based_evidence": sign_result.__dict__,
                }

        return {
            "decision": "UNCERTAIN",
            "medical_necessity_status": "uncertain",
            "confidence": 0.35,
            "reason": "The medical core has insufficient disease/sign catalog coverage to conclude.",
            "branch_used": "coverage_gap",
            "service_mapping": service_info,
            "known_disease_evidence": known_result.__dict__ if known_result else None,
            "sign_based_evidence": sign_result.__dict__ if sign_result else None,
        }
