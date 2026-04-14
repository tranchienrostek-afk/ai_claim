from __future__ import annotations

import argparse
import csv
import json
import re
import unicodedata
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any


PROJECT_DIR = Path(__file__).parent.parent
INSURANCE_DIR = PROJECT_DIR / "06_insurance"
DEFAULT_INPUT_JSONL = PROJECT_DIR / "09_unified_story_testcase" / "step1_clinical_necessity" / "clinical_necessity_scored.jsonl"
DEFAULT_OUTPUT_JSONL = PROJECT_DIR / "09_unified_story_testcase" / "step2_contract_clause" / "contract_clause_scored.jsonl"
DEFAULT_SUMMARY_JSON = PROJECT_DIR / "09_unified_story_testcase" / "step2_contract_clause" / "contract_clause_summary.json"
DEFAULT_EXAMPLES_JSON = PROJECT_DIR / "09_unified_story_testcase" / "step2_contract_clause" / "contract_clause_examples.json"
DEFAULT_CONTRACT_RULES_JSON = INSURANCE_DIR / "contract_rules.json"
DEFAULT_CONTRACT_CLAUSE_SERVICE_JSON = INSURANCE_DIR / "contract_clause_service_catalog.json"
DEFAULT_BENEFIT_PACK_JSON = INSURANCE_DIR / "benefit_contract_knowledge_pack.json"
DEFAULT_EXCLUSION_PACK_JSON = INSURANCE_DIR / "exclusion_knowledge_pack.json"

DIRECT_CONTRACT_KEYS = (
    "contract_name",
    "contract_id",
    "product_id",
    "plan_id",
    "policy_contract",
)

SCREENING_PATTERNS = (
    "tầm soát",
    "tam soat",
    "kiểm tra",
    "kiem tra",
    "screening",
)
NOT_COVERED_PATTERNS = (
    "không thuộc quyền lợi",
    "khong thuoc quyen loi",
    "không bảo hiểm",
    "khong bao hiem",
    "không thuộc phạm vi",
    "khong thuoc pham vi",
)
COPAY_PATTERNS = ("đồng chi trả", "dong chi tra", "copay")
LIMIT_PATTERNS = (
    "vượt quá",
    "vuot qua",
    "hạn mức",
    "han muc",
    "giới hạn",
    "gioi han",
    "một lần khám",
    "mot lan kham",
    "một ngày",
    "mot ngay",
)
DOCUMENT_PATTERNS = (
    "chứng từ",
    "chung tu",
    "hồ sơ",
    "ho so",
    "giấy",
    "giay",
)
WAITING_PERIOD_PATTERNS = (
    "thời gian chờ",
    "thoi gian cho",
    "chờ 365",
    "cho 365",
    "bệnh có sẵn",
    "benh co san",
    "bệnh đặc biệt",
    "benh dac biet",
)
LATE_SUBMISSION_PATTERNS = ("nộp muộn", "nop muon", "quá hạn", "qua han")

OUTPATIENT_DIAGNOSTIC_CATEGORY_PREFIXES = ("LAB", "IMG", "END", "CONS", "PROC")


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
    lowered = strip_diacritics(text).lower().replace("đ", "d").replace("Đ", "d")
    return re.sub(r"\s+", " ", lowered).strip()


def canonical_contract_key(text: str) -> str:
    normalized = normalize_text(text)
    return re.sub(r"[^a-z0-9]+", "", normalized)


def load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


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


def matches_any(text: str, patterns: tuple[str, ...]) -> bool:
    haystack = normalize_text(text)
    return any(pattern in haystack for pattern in patterns)


def classify_atomic_reason(reason: str) -> list[str]:
    flags: list[str] = []
    if matches_any(reason, SCREENING_PATTERNS):
        flags.append("screening")
    if matches_any(reason, NOT_COVERED_PATTERNS):
        flags.append("not_covered")
    if matches_any(reason, COPAY_PATTERNS):
        flags.append("copay")
    if matches_any(reason, LIMIT_PATTERNS):
        flags.append("limit")
    if matches_any(reason, DOCUMENT_PATTERNS):
        flags.append("documents")
    if matches_any(reason, WAITING_PERIOD_PATTERNS):
        flags.append("waiting_period")
    if matches_any(reason, LATE_SUBMISSION_PATTERNS):
        flags.append("late_submission")
    if not flags:
        flags.append("other")
    return flags


def is_category_covered(category_code: str, covered_patterns: list[str]) -> bool:
    category = as_text(category_code)
    if not category or not covered_patterns:
        return False
    for pattern in covered_patterns:
        pattern_text = as_text(pattern)
        if not pattern_text:
            continue
        if pattern_text.endswith("*") and category.startswith(pattern_text[:-1]):
            return True
        if category == pattern_text:
            return True
    return False


def load_contract_attachment_map(path: Path | None) -> dict[str, dict[str, str]]:
    if path is None or not path.exists():
        return {}

    attachments: dict[str, dict[str, str]] = {}
    if path.suffix.lower() == ".jsonl":
        rows = load_jsonl(path)
    elif path.suffix.lower() == ".csv":
        with path.open(encoding="utf-8-sig", newline="") as handle:
            rows = list(csv.DictReader(handle))
    else:
        raise SystemExit(f"Unsupported contract attachment format: {path}")

    for row in rows:
        if not isinstance(row, dict):
            continue
        contract_name = as_text(row.get("contract_name") or row.get("contract_id"))
        insurer = as_text(row.get("insurer"))
        if not contract_name:
            continue
        payload = {
            "contract_name": contract_name,
            "insurer": insurer,
            "source": path.name,
        }
        for key_name in ("message_hash_id", "benchmark_id", "so_hoso_boithuong"):
            join_value = as_text(row.get(key_name))
            if join_value:
                attachments[f"{key_name}:{join_value}"] = payload
    return attachments


@dataclass
class RiskEvidence:
    risk_counts: Counter
    reason_rows: int
    top_reasons: list[dict[str, Any]]
    top_contracts: list[dict[str, Any]]
    matched_contracts: list[str]


class ContractClauseStep2:
    def __init__(
        self,
        contract_rules_path: Path,
        clause_service_catalog_path: Path,
        benefit_pack_path: Path,
        exclusion_pack_path: Path,
        contract_attachment_map: dict[str, dict[str, str]] | None = None,
        default_contract: str | None = None,
    ) -> None:
        self.contract_rules_payload = load_json(contract_rules_path)
        self.clause_service_payload = load_json(clause_service_catalog_path)
        self.benefit_pack_payload = load_json(benefit_pack_path)
        self.exclusion_pack_payload = load_json(exclusion_pack_path)
        self.contract_attachment_map = contract_attachment_map or {}
        self.default_contract = as_text(default_contract)

        self.contract_lookup: dict[str, dict[str, Any]] = {}
        for rule in self.contract_rules_payload.get("contract_rules", []):
            contract_names = {
                as_text(rule.get("contract_id")),
                as_text(rule.get("product")),
                as_text(rule.get("insurer")),
            }
            for contract_name in contract_names:
                if contract_name:
                    self.contract_lookup[canonical_contract_key(contract_name)] = rule

        self.benefit_hints_by_contract = self._build_benefit_hints()
        self.contract_service_profiles, self.cross_contract_profiles = self._build_service_risk_profiles()
        self.exclusion_stats = self.exclusion_pack_payload.get("main_summary", {}).get("stats", {})

    def _build_benefit_hints(self) -> dict[str, dict[str, Any]]:
        hints_by_contract: dict[str, dict[str, Any]] = {}
        for contract in self.benefit_pack_payload.get("contract_catalog", {}).get("contracts", []):
            contract_id = as_text(contract.get("contract_id"))
            key = canonical_contract_key(contract_id)
            if not key:
                continue
            benefit_entries = contract.get("benefit_entries", [])
            counter = Counter()
            sample_labels: list[str] = []
            for entry in benefit_entries:
                label = as_text(entry.get("entry_label"))
                if not label:
                    continue
                label_norm = normalize_text(label)
                if len(sample_labels) < 12:
                    sample_labels.append(label)
                if "dong chi tra" in label_norm:
                    counter["copay"] += 1
                if "gioi han" in label_norm or "han muc" in label_norm:
                    counter["limit"] += 1
                if "khong bao hiem" in label_norm or "khong thuoc" in label_norm:
                    counter["not_covered"] += 1
                if "ngoai tru" in label_norm or "xet nghiem" in label_norm or "chan doan" in label_norm:
                    counter["outpatient_diagnostic"] += 1
                if "noi tru" in label_norm or "vien phi" in label_norm:
                    counter["inpatient"] += 1
            hints_by_contract[key] = {
                "contract_id": contract_id,
                "product_name": as_text(contract.get("product_name")),
                "benefit_hint_counts": dict(counter),
                "sample_labels": sample_labels,
            }
        return hints_by_contract

    def _build_service_risk_profiles(self) -> tuple[dict[str, dict[str, RiskEvidence]], dict[str, RiskEvidence]]:
        per_contract_rows: dict[str, dict[str, Counter]] = defaultdict(lambda: defaultdict(Counter))
        per_contract_reason_texts: dict[str, dict[str, Counter]] = defaultdict(lambda: defaultdict(Counter))
        per_service_contract_rows: dict[str, Counter] = defaultdict(Counter)
        cross_risk_counts: dict[str, Counter] = defaultdict(Counter)
        cross_reason_texts: dict[str, Counter] = defaultdict(Counter)
        matched_contracts: dict[str, set[str]] = defaultdict(set)

        for contract in self.clause_service_payload.get("contracts", []):
            contract_name = as_text(contract.get("contract_name"))
            contract_key = canonical_contract_key(contract_name)
            if not contract_key:
                continue
            for profile in contract.get("reason_profiles", []):
                reason = as_text(profile.get("atomic_reason"))
                reason_rows = int(profile.get("rows") or 0)
                risk_kinds = classify_atomic_reason(reason)
                for service in profile.get("top_services", []):
                    service_code = as_text(service.get("service_code"))
                    if not service_code:
                        continue
                    rows = int(service.get("rows") or 0) or reason_rows
                    for risk_kind in risk_kinds:
                        per_contract_rows[contract_key][service_code][risk_kind] += rows
                        cross_risk_counts[service_code][risk_kind] += rows
                    per_contract_rows[contract_key][service_code]["total"] += rows
                    cross_risk_counts[service_code]["total"] += rows
                    per_contract_reason_texts[contract_key][service_code][reason] += rows
                    cross_reason_texts[service_code][reason] += rows
                    per_service_contract_rows[service_code][contract_name] += rows
                    matched_contracts[service_code].add(contract_name)

        per_contract_profiles: dict[str, dict[str, RiskEvidence]] = defaultdict(dict)
        for contract_key, service_map in per_contract_rows.items():
            for service_code, counter in service_map.items():
                reason_counter = per_contract_reason_texts[contract_key][service_code]
                contract_counter = per_service_contract_rows[service_code]
                per_contract_profiles[contract_key][service_code] = RiskEvidence(
                    risk_counts=counter,
                    reason_rows=int(counter.get("total") or 0),
                    top_reasons=[
                        {"atomic_reason": reason, "rows": rows}
                        for reason, rows in reason_counter.most_common(5)
                    ],
                    top_contracts=[
                        {"contract_name": name, "rows": rows}
                        for name, rows in contract_counter.most_common(5)
                    ],
                    matched_contracts=sorted(matched_contracts.get(service_code, set())),
                )

        cross_profiles: dict[str, RiskEvidence] = {}
        for service_code, counter in cross_risk_counts.items():
            reason_counter = cross_reason_texts[service_code]
            contract_counter = per_service_contract_rows[service_code]
            cross_profiles[service_code] = RiskEvidence(
                risk_counts=counter,
                reason_rows=int(counter.get("total") or 0),
                top_reasons=[
                    {"atomic_reason": reason, "rows": rows}
                    for reason, rows in reason_counter.most_common(5)
                ],
                top_contracts=[
                    {"contract_name": name, "rows": rows}
                    for name, rows in contract_counter.most_common(5)
                ],
                matched_contracts=sorted(matched_contracts.get(service_code, set())),
            )
        return per_contract_profiles, cross_profiles

    def _resolve_contract(self, row: dict[str, Any]) -> dict[str, str]:
        for key_name in DIRECT_CONTRACT_KEYS:
            contract_name = as_text(row.get(key_name))
            if contract_name:
                return {
                    "status": "provided_in_row",
                    "contract_name": contract_name,
                    "insurer": as_text(row.get("insurer")),
                    "source": key_name,
                }

        for key_name in ("message_hash_id", "benchmark_id", "so_hoso_boithuong"):
            key_value = as_text(row.get(key_name))
            if not key_value:
                continue
            attachment = self.contract_attachment_map.get(f"{key_name}:{key_value}")
            if attachment:
                return {
                    "status": "attached_from_mapping",
                    "contract_name": attachment["contract_name"],
                    "insurer": attachment.get("insurer", ""),
                    "source": attachment.get("source", ""),
                }

        if self.default_contract:
            return {
                "status": "default_contract",
                "contract_name": self.default_contract,
                "insurer": "",
                "source": "cli_default_contract",
            }

        return {
            "status": "contract_unknown",
            "contract_name": "",
            "insurer": "",
            "source": "",
        }

    def _find_contract_rule(self, contract_name: str) -> dict[str, Any] | None:
        if not contract_name:
            return None
        return self.contract_lookup.get(canonical_contract_key(contract_name))

    def _select_benefit_hints(self, contract_name: str, category_code: str) -> list[str]:
        contract_key = canonical_contract_key(contract_name)
        hint_payload = self.benefit_hints_by_contract.get(contract_key) or {}
        sample_labels = hint_payload.get("sample_labels", [])
        category = as_text(category_code)
        if category.startswith(OUTPATIENT_DIAGNOSTIC_CATEGORY_PREFIXES):
            filtered = [
                label
                for label in sample_labels
                if any(token in normalize_text(label) for token in ("ngoai tru", "xet nghiem", "chan doan", "kham"))
            ]
            return filtered[:5]
        return sample_labels[:5]

    def _cross_contract_prior(self, service_code: str) -> dict[str, Any]:
        evidence = self.cross_contract_profiles.get(service_code)
        if evidence is None:
            return {
                "available": False,
                "reason_rows": 0,
                "risk_counts": {},
                "dominant_risks": [],
                "top_reasons": [],
                "top_contracts": [],
            }

        dominant_risks = [
            risk_kind
            for risk_kind, rows in evidence.risk_counts.items()
            if risk_kind != "total" and rows >= 5
        ]
        return {
            "available": True,
            "reason_rows": evidence.reason_rows,
            "risk_counts": {
                risk_kind: rows
                for risk_kind, rows in evidence.risk_counts.items()
                if risk_kind != "total"
            },
            "dominant_risks": sorted(dominant_risks),
            "top_reasons": evidence.top_reasons,
            "top_contracts": evidence.top_contracts,
            "matched_contracts": evidence.matched_contracts,
        }

    def _contract_specific_evidence(self, contract_name: str, service_code: str) -> dict[str, Any]:
        contract_key = canonical_contract_key(contract_name)
        evidence = self.contract_service_profiles.get(contract_key, {}).get(service_code)
        if evidence is None:
            return {
                "available": False,
                "reason_rows": 0,
                "risk_counts": {},
                "top_reasons": [],
            }
        return {
            "available": True,
            "reason_rows": evidence.reason_rows,
            "risk_counts": {
                risk_kind: rows
                for risk_kind, rows in evidence.risk_counts.items()
                if risk_kind != "total"
            },
            "top_reasons": evidence.top_reasons,
        }

    def assess_row(self, row: dict[str, Any]) -> dict[str, Any]:
        recognized = row.get("recognized_service") or {}
        service_code = as_text(recognized.get("service_code"))
        service_name = as_text(recognized.get("canonical_name") or row.get("service_name_raw"))
        category_code = as_text(recognized.get("category_code"))
        medical = row.get("step1_clinical_necessity") or {}
        medical_status = as_text(medical.get("medical_necessity_status"))
        contract_resolution = self._resolve_contract(row)
        contract_rule = self._find_contract_rule(contract_resolution["contract_name"])

        cross_contract_prior = self._cross_contract_prior(service_code)
        contract_specific = self._contract_specific_evidence(contract_resolution["contract_name"], service_code)
        benefit_hints = self._select_benefit_hints(contract_resolution["contract_name"], category_code)

        evidence_flags: list[str] = []
        risks = {
            "screening_sensitive": False,
            "not_covered_sensitive": False,
            "copay_sensitive": False,
            "limit_sensitive": False,
            "documents_sensitive": False,
            "waiting_period_sensitive": False,
            "late_submission_sensitive": False,
        }

        if cross_contract_prior.get("available"):
            evidence_flags.append("cross_contract_prior")
        if contract_specific.get("available"):
            evidence_flags.append("contract_specific_service_profile")
        if benefit_hints:
            evidence_flags.append("benefit_hint_available")

        if contract_rule is not None:
            evidence_flags.append("contract_rule_loaded")
            if contract_rule.get("copay_percent"):
                risks["copay_sensitive"] = True
                evidence_flags.append("contract_has_copay")
            if contract_rule.get("sublimit_per_visit"):
                risks["limit_sensitive"] = True
                evidence_flags.append("contract_has_sublimit")
            covered_patterns = contract_rule.get("covered_categories") or []
            if covered_patterns and category_code and not is_category_covered(category_code, covered_patterns):
                evidence_flags.append("category_not_seen_in_contract_patterns")

        for source in (cross_contract_prior, contract_specific):
            risk_counts = source.get("risk_counts") or {}
            if risk_counts.get("screening", 0) >= 5:
                risks["screening_sensitive"] = True
            if risk_counts.get("not_covered", 0) >= 3:
                risks["not_covered_sensitive"] = True
            if risk_counts.get("copay", 0) >= 3:
                risks["copay_sensitive"] = True
            if risk_counts.get("limit", 0) >= 3:
                risks["limit_sensitive"] = True
            if risk_counts.get("documents", 0) >= 2:
                risks["documents_sensitive"] = True
            if risk_counts.get("waiting_period", 0) >= 2:
                risks["waiting_period_sensitive"] = True
            if risk_counts.get("late_submission", 0) >= 2:
                risks["late_submission_sensitive"] = True

        decision = "contract_unknown"
        reason = "No contract attached to this service line; Step 2 emits priors only."

        if contract_rule is not None:
            if risks["not_covered_sensitive"]:
                decision = "contract_review_not_covered_sensitive"
                reason = "Contract/category patterns indicate this service may fall outside covered benefits."
            elif risks["screening_sensitive"] and medical_status in {"uncertain", "not_medically_supported"}:
                decision = "contract_review_screening_sensitive"
                reason = "Historical contract evidence shows screening/tầm soát sensitivity for this service."
            elif any(risks[key] for key in ("waiting_period_sensitive", "late_submission_sensitive", "documents_sensitive")):
                decision = "contract_review_admin_or_clause_sensitive"
                reason = "Administrative or clause-sensitive patterns exist and require contract-side review."
            elif any(risks[key] for key in ("copay_sensitive", "limit_sensitive")):
                decision = "contract_partial_pay_sensitive"
                reason = "Contract evidence suggests copay and/or sublimit handling rather than pure deny."
            else:
                decision = "contract_clear"
                reason = "No strong contract-side deny signal was triggered for the matched contract."
        elif cross_contract_prior.get("available"):
            if risks["screening_sensitive"]:
                decision = "contract_unknown_with_screening_prior"
                reason = "Across historical contracts, this service is frequently associated with screening/tầm soát clauses."
            elif risks["not_covered_sensitive"]:
                decision = "contract_unknown_with_not_covered_prior"
                reason = "Across historical contracts, this service often appears in not-covered exclusion patterns."
            elif any(risks[key] for key in ("copay_sensitive", "limit_sensitive")):
                decision = "contract_unknown_with_partial_pay_prior"
                reason = "Across historical contracts, this service is often subject to copay or limit rules."

        return {
            "decision": decision,
            "reason": reason,
            "contract_resolution": contract_resolution,
            "benefit_hints": benefit_hints,
            "cross_contract_prior": cross_contract_prior,
            "contract_specific_evidence": contract_specific,
            "contract_risk_flags": risks,
            "evidence_flags": evidence_flags,
            "exclusion_reference": {
                "reason_dictionary_size": self.exclusion_stats.get("reason_dictionary_size"),
                "atomic_reason_total": self.exclusion_stats.get("atomic_reason_total"),
            },
        }


def build_examples(rows: list[dict[str, Any]]) -> dict[str, Any]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        step2 = row.get("step2_contract_clause") or {}
        decision = as_text(step2.get("decision")) or "unknown"
        if len(grouped[decision]) >= 5:
            continue
        grouped[decision].append(
            {
                "benchmark_id": row.get("benchmark_id"),
                "service_name_raw": row.get("service_name_raw"),
                "recognized_service": row.get("recognized_service"),
                "step1_status": (row.get("step1_clinical_necessity") or {}).get("medical_necessity_status"),
                "step2": step2,
            }
        )
    return {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "examples_by_decision": grouped,
    }


def build_summary(rows: list[dict[str, Any]], default_contract: str | None) -> dict[str, Any]:
    decision_counts = Counter()
    contract_resolution_counts = Counter()
    risk_flag_counts = Counter()
    prior_service_counts = Counter()
    prior_reason_counts = Counter()

    for row in rows:
        step2 = row.get("step2_contract_clause") or {}
        decision_counts[as_text(step2.get("decision")) or "unknown"] += 1
        contract_resolution = step2.get("contract_resolution") or {}
        contract_resolution_counts[as_text(contract_resolution.get("status")) or "unknown"] += 1
        for risk_name, enabled in (step2.get("contract_risk_flags") or {}).items():
            if enabled:
                risk_flag_counts[risk_name] += 1
        prior = step2.get("cross_contract_prior") or {}
        if prior.get("available"):
            service = as_text(((row.get("recognized_service") or {}).get("canonical_name")) or row.get("service_name_raw"))
            prior_service_counts[service] += 1
            for reason in prior.get("top_reasons") or []:
                prior_reason_counts[as_text(reason.get("atomic_reason"))] += int(reason.get("rows") or 0)

    return {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "default_contract": as_text(default_contract),
        "stats": {
            "total_rows": len(rows),
            "decision_distribution": dict(decision_counts),
            "contract_resolution_distribution": dict(contract_resolution_counts),
            "risk_flag_counts": dict(risk_flag_counts),
        },
        "top_services_with_cross_contract_priors": [
            {"service_name": name, "rows": count}
            for name, count in prior_service_counts.most_common(15)
        ],
        "top_atomic_reasons_from_priors": [
            {"atomic_reason": reason, "rows": rows_count}
            for reason, rows_count in prior_reason_counts.most_common(15)
        ],
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Step 2 contract / benefit / clause reasoning.")
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT_JSONL)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT_JSONL)
    parser.add_argument("--summary", type=Path, default=DEFAULT_SUMMARY_JSON)
    parser.add_argument("--examples", type=Path, default=DEFAULT_EXAMPLES_JSON)
    parser.add_argument("--contract-rules", type=Path, default=DEFAULT_CONTRACT_RULES_JSON)
    parser.add_argument("--clause-service-catalog", type=Path, default=DEFAULT_CONTRACT_CLAUSE_SERVICE_JSON)
    parser.add_argument("--benefit-pack", type=Path, default=DEFAULT_BENEFIT_PACK_JSON)
    parser.add_argument("--exclusion-pack", type=Path, default=DEFAULT_EXCLUSION_PACK_JSON)
    parser.add_argument("--contract-attachments", type=Path, default=None)
    parser.add_argument("--default-contract", default="")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    rows = load_jsonl(args.input)
    attachment_map = load_contract_attachment_map(args.contract_attachments)
    engine = ContractClauseStep2(
        contract_rules_path=args.contract_rules,
        clause_service_catalog_path=args.clause_service_catalog,
        benefit_pack_path=args.benefit_pack,
        exclusion_pack_path=args.exclusion_pack,
        contract_attachment_map=attachment_map,
        default_contract=args.default_contract,
    )

    scored_rows: list[dict[str, Any]] = []
    for row in rows:
        enriched = dict(row)
        enriched["step2_contract_clause"] = engine.assess_row(row)
        scored_rows.append(enriched)

    write_jsonl(args.output, scored_rows)
    args.summary.parent.mkdir(parents=True, exist_ok=True)
    args.summary.write_text(
        json.dumps(build_summary(scored_rows, args.default_contract), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    args.examples.parent.mkdir(parents=True, exist_ok=True)
    args.examples.write_text(
        json.dumps(build_examples(scored_rows), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


if __name__ == "__main__":
    main()
