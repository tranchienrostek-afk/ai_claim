from __future__ import annotations

import json
import re
import unicodedata
from dataclasses import dataclass
from pathlib import Path
from typing import Any


PROJECT_DIR = Path(__file__).parent.parent
PHAC_DO_DIR = PROJECT_DIR / "05_reference" / "phac_do"
TMH_SERVICE_LINKS_PATH = PHAC_DO_DIR / "tmh_protocol_text_service_links.json"
TMH_TEXT_GROUPS_PATH = PHAC_DO_DIR / "tmh_step1_text_groups.json"
TMH_RULE_CATALOG_PATH = PHAC_DO_DIR / "tmh_step1_rule_catalog.json"
MOJIBAKE_HINTS = ("Ãƒ", "Ã„", "Ã…", "Ã†", "Ã‚", "Ã", "Ã¡Âº", "Ã¡Â»", "Ã¡Â¼", "Ã¢")


def repair_text(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    if any(hint in text for hint in MOJIBAKE_HINTS):
        try:
            repaired = text.encode("latin1").decode("utf-8")
            if repaired:
                return repaired
        except Exception:
            pass
    return text


def normalize_text(value: Any) -> str:
    text = repair_text(value).lower()
    text = text.replace("đ", "d").replace("Đ", "d")
    text = text.replace("Ä‘", "d").replace("Ä", "d")
    text = text.replace("Ä‘", "d").replace("Ä", "d")
    normalized = unicodedata.normalize("NFD", text)
    text = "".join(ch for ch in normalized if unicodedata.category(ch) != "Mn")
    text = re.sub(r"[^a-z0-9 ]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def token_set(value: Any) -> set[str]:
    return {token for token in normalize_text(value).split() if len(token) >= 2}


def contains_any(text: str, fragments: set[str]) -> bool:
    return any(fragment in text for fragment in fragments)


def contains_all(text: str, fragments: set[str]) -> bool:
    return all(fragment in text for fragment in fragments)


@dataclass
class TMHSupportAssessment:
    supported: bool
    unsupported: bool
    support_level: str
    reason: str
    source: str
    matched_disease: str
    matched_service: str


def _load_text_groups(path: Path = TMH_TEXT_GROUPS_PATH) -> dict[str, set[str]]:
    if not path.exists():
        raise FileNotFoundError(f"TMH text group catalog not found: {path}")
    payload = json.loads(path.read_text(encoding="utf-8"))
    return {
        str(name): {normalize_text(term) for term in terms if normalize_text(term)}
        for name, terms in (payload.get("groups") or {}).items()
    }


def _load_rule_catalog(path: Path = TMH_RULE_CATALOG_PATH) -> list[dict[str, Any]]:
    if not path.exists():
        raise FileNotFoundError(f"TMH rule catalog not found: {path}")
    payload = json.loads(path.read_text(encoding="utf-8"))
    rules = list(payload.get("rules") or [])
    return sorted(rules, key=lambda rule: int(rule.get("priority", 0)))


class TMHStep1SupportEngine:
    def __init__(
        self,
        service_links_path: Path = TMH_SERVICE_LINKS_PATH,
        text_groups_path: Path = TMH_TEXT_GROUPS_PATH,
        rule_catalog_path: Path = TMH_RULE_CATALOG_PATH,
    ) -> None:
        self.service_links_path = service_links_path
        self.text_groups = _load_text_groups(text_groups_path)
        self.rules = _load_rule_catalog(rule_catalog_path)
        self.links_by_icd: dict[str, list[dict[str, Any]]] = {}
        self.links_by_disease: dict[str, list[dict[str, Any]]] = {}
        if service_links_path.exists():
            payload = json.loads(service_links_path.read_text(encoding="utf-8"))
            for link in payload.get("links", []):
                icd10 = str(link.get("icd10") or "").strip().upper()
                disease_name = normalize_text(link.get("disease_name"))
                if icd10:
                    self.links_by_icd.setdefault(icd10, []).append(link)
                if disease_name:
                    self.links_by_disease.setdefault(disease_name, []).append(link)

    @staticmethod
    def _match_service_marker(service_text: str, markers: set[str]) -> bool:
        return contains_any(service_text, markers)

    @staticmethod
    def _same_service_family(service_text: str, candidate_text: str) -> bool:
        service_tokens = token_set(service_text)
        candidate_tokens = token_set(candidate_text)
        overlap = service_tokens & candidate_tokens
        if not overlap:
            return False
        coverage = len(overlap) / max(len(candidate_tokens), 1)
        return coverage >= 0.5 or len(overlap) >= 2

    def _group_terms(self, group_names: list[str] | None) -> set[str]:
        terms: set[str] = set()
        for group_name in group_names or []:
            terms |= self.text_groups.get(str(group_name), set())
        return terms

    def _inline_terms(self, values: list[str] | None) -> set[str]:
        return {normalize_text(value) for value in (values or []) if normalize_text(value)}

    def _expanded_terms(self, group_names: list[str] | None, inline_terms: list[str] | None) -> set[str]:
        return self._group_terms(group_names) | self._inline_terms(inline_terms)

    def _match_mode(
        self,
        text: str,
        *,
        group_names: list[str] | None,
        inline_terms: list[str] | None,
        mode: str,
    ) -> bool:
        fragments = self._expanded_terms(group_names, inline_terms)
        if not fragments:
            return mode in {"all", "none"}
        if mode == "any":
            return contains_any(text, fragments)
        if mode == "all":
            return contains_all(text, fragments)
        if mode == "none":
            return not contains_any(text, fragments)
        raise ValueError(f"Unsupported matcher mode: {mode}")

    def _scope_constraints_pass(self, text: str, payload: dict[str, Any], scope: str) -> bool:
        if not self._match_mode(
            text,
            group_names=payload.get(f"{scope}_groups_any"),
            inline_terms=payload.get(f"{scope}_terms_any"),
            mode="any",
        ) and (payload.get(f"{scope}_groups_any") or payload.get(f"{scope}_terms_any")):
            return False
        if not self._match_mode(
            text,
            group_names=payload.get(f"{scope}_groups_all"),
            inline_terms=payload.get(f"{scope}_terms_all"),
            mode="all",
        ) and (payload.get(f"{scope}_groups_all") or payload.get(f"{scope}_terms_all")):
            return False
        if not self._match_mode(
            text,
            group_names=payload.get(f"{scope}_groups_none"),
            inline_terms=payload.get(f"{scope}_terms_none"),
            mode="none",
        ) and (payload.get(f"{scope}_groups_none") or payload.get(f"{scope}_terms_none")):
            return False
        return True

    def _matcher_passes(self, matcher: dict[str, Any], context_norm: str, specialty_norm: str) -> bool:
        if matcher.get("context_present") and not context_norm:
            return False
        if matcher.get("context_absent") and context_norm:
            return False
        if not self._scope_constraints_pass(context_norm, matcher, "context"):
            return False
        if not self._scope_constraints_pass(specialty_norm, matcher, "specialty"):
            return False
        return True

    def _rule_matches(self, rule: dict[str, Any], service_text: str, context_norm: str, specialty_norm: str) -> bool:
        if not self._scope_constraints_pass(service_text, rule, "service"):
            return False
        matchers = list(rule.get("matchers") or [{}])
        return any(self._matcher_passes(matcher, context_norm, specialty_norm) for matcher in matchers)

    def _assessment_from_rule(
        self,
        rule: dict[str, Any],
        *,
        diagnosis_text: str,
        service_name: str,
    ) -> TMHSupportAssessment:
        action = str(rule.get("action") or "").strip().lower()
        supported = action == "supported"
        unsupported = action == "unsupported"
        return TMHSupportAssessment(
            supported=supported,
            unsupported=unsupported,
            support_level=str(rule.get("support_level") or "none"),
            reason=str(rule.get("reason") or "TMH rule matched."),
            source=str(rule.get("source") or rule.get("rule_id") or "tmh_rule_catalog"),
            matched_disease=diagnosis_text,
            matched_service=service_name,
        )

    def _protocol_support(self, service_text: str, diagnosis_text: str, primary_icd: str) -> TMHSupportAssessment | None:
        context_norm = normalize_text(diagnosis_text)

        def condition_satisfied(link: dict[str, Any]) -> bool:
            condition_texts = [
                normalize_text(source.get("condition_to_apply"))
                for source in (link.get("sources") or [])
                if str(source.get("condition_to_apply") or "").strip()
            ]
            if not condition_texts:
                return True
            for condition_text in condition_texts:
                condition_tokens = [token for token in condition_text.split() if len(token) >= 4]
                if any(token in context_norm for token in condition_tokens):
                    return True
            return False

        icd_links = self.links_by_icd.get(str(primary_icd or "").strip().upper(), [])
        for link in icd_links:
            candidate_service = normalize_text(link.get("service_name"))
            if not candidate_service:
                continue
            if not condition_satisfied(link):
                continue
            if self._same_service_family(service_text, candidate_service):
                matched_disease = str(link.get("disease_name") or "")
                return TMHSupportAssessment(
                    supported=True,
                    unsupported=False,
                    support_level="strong",
                    reason=(
                        f"TMH protocol paraclinical/treatment link matched service '{link.get('service_name')}'"
                        f" for ICD {primary_icd} ({matched_disease})."
                    ),
                    source="tmh_protocol_service_links",
                    matched_disease=matched_disease,
                    matched_service=str(link.get("service_name") or ""),
                )

        diag_norm = normalize_text(diagnosis_text)
        for disease_norm, links in self.links_by_disease.items():
            if not disease_norm or disease_norm not in diag_norm:
                continue
            for link in links:
                candidate_service = normalize_text(link.get("service_name"))
                if not condition_satisfied(link):
                    continue
                if candidate_service and self._same_service_family(service_text, candidate_service):
                    matched_disease = str(link.get("disease_name") or "")
                    return TMHSupportAssessment(
                        supported=True,
                        unsupported=False,
                        support_level="moderate",
                        reason=(
                            f"TMH protocol matched disease '{matched_disease}' and service '{link.get('service_name')}'."
                        ),
                        source="tmh_protocol_service_links",
                        matched_disease=matched_disease,
                        matched_service=str(link.get("service_name") or ""),
                    )
        return None

    def assess(
        self,
        *,
        service_name: str,
        diagnosis_text: str,
        primary_icd: str,
        chief_complaint: str,
        initial_signs: list[str],
        specialty: str,
    ) -> TMHSupportAssessment:
        service_text = normalize_text(service_name)
        specialty_norm = normalize_text(specialty)
        context_text = " | ".join(
            [
                diagnosis_text,
                chief_complaint,
                specialty,
                " ; ".join(initial_signs),
            ]
        )
        context_norm = normalize_text(context_text)

        protocol = self._protocol_support(service_text, diagnosis_text, primary_icd)
        if protocol is not None:
            return protocol

        for rule in self.rules:
            if self._rule_matches(rule, service_text, context_norm, specialty_norm):
                return self._assessment_from_rule(rule, diagnosis_text=diagnosis_text, service_name=service_name)

        return TMHSupportAssessment(
            supported=False,
            unsupported=False,
            support_level="none",
            reason="No TMH-specific structured support or contradiction detected.",
            source="none",
            matched_disease="",
            matched_service="",
        )
