from __future__ import annotations

import json
import re
import unicodedata
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


PROJECT_DIR = Path(__file__).parent.parent
INSURANCE_DIR = PROJECT_DIR / "06_insurance"

OUTPUT_JSONL_PATH = INSURANCE_DIR / "exclusion_claim_signals.jsonl"
OUTPUT_SUMMARY_PATH = INSURANCE_DIR / "exclusion_claim_signal_summary.json"
OUTPUT_CATALOG_PATH = INSURANCE_DIR / "contract_clause_signal_catalog.json"

CLAUSE_REF_PATTERNS = [
    re.compile(r"(Khoản\s+\d+[^\n.;:]*)", re.IGNORECASE),
    re.compile(r"(Chương\s+[IVXLC]+[^\n.;:]*)", re.IGNORECASE),
    re.compile(r"(Quy tắc bảo hiểm số\s+[^\n.;:]*)", re.IGNORECASE),
    re.compile(r"(Quy tắc số\s+[^\n.;:]*)", re.IGNORECASE),
]
SERVICE_AMOUNT_RE = re.compile(
    r"([A-Za-zÀ-ỹ0-9#%+\-_/().,\s]{2,}?)\s+bằng\s+([0-9][0-9.,\s]*)\s*(?:VNĐ|VND|đồng)",
    re.IGNORECASE,
)
FORMULA_RE = re.compile(
    r"([0-9][0-9.,\s]*)\s*(?:VNĐ|VND)?\s*x\s*(\d{1,3})\s*%\s*=\s*([0-9][0-9.,\s]*)",
    re.IGNORECASE,
)
MENTION_BLOCKLIST = [
    "cong ty bao hiem",
    "pjico",
    "quy khach",
    "quyen loi",
    "chi phi y te thuc te",
    "hop dong",
    "can cu theo",
    "do do",
    "quy tac bao hiem",
    "dinh nghia",
    "ho so yeu cau boi thuong",
]


def find_required_file(filename: str) -> Path:
    matches = sorted(INSURANCE_DIR.rglob(filename))
    if not matches:
        raise FileNotFoundError(f"Missing required file: {filename}")
    return matches[0]


def collapse_spaces(text: object) -> str:
    return " ".join(str(text or "").strip().split())


def strip_diacritics(text: object) -> str:
    base = collapse_spaces(text).lower().replace("đ", "d").replace("Đ", "d")
    nfkd = unicodedata.normalize("NFKD", base)
    return "".join(ch for ch in nfkd if not unicodedata.combining(ch))


def normalize_for_match(text: object) -> str:
    normalized = strip_diacritics(text)
    normalized = normalized.replace("vnđ", "vnd").replace("đ", "d")
    normalized = re.sub(r"\s+", " ", normalized)
    return normalized.strip()


def clean_display_text(text: object) -> str:
    return collapse_spaces(text)


def parse_vnd_amount(value: object) -> int | None:
    if value is None:
        return None
    digits = re.sub(r"[^\d]", "", str(value))
    if not digits:
        return None
    return int(digits)


def parse_date(value: object) -> str | None:
    if value is None or str(value).strip() == "":
        return None
    parsed = pd.to_datetime(value, format="%d/%m/%Y", errors="coerce")
    if pd.isna(parsed):
        parsed = pd.to_datetime(value, errors="coerce")
    if pd.isna(parsed):
        return None
    return parsed.date().isoformat()


def infer_insurer(contract_name: str) -> str:
    key = normalize_for_match(contract_name)
    if "fpt" in key:
        return "FPT"
    if "pjico" in key:
        return "PJICO"
    if "bhv" in key:
        return "BHV"
    if "tcg" in key:
        return "TCGIns"
    if "uic" in key:
        return "UIC"
    if "tin" in key:
        return "TIN"
    return "UNKNOWN"


def split_atomic_reasons(raw_reason: str) -> list[str]:
    text = clean_display_text(raw_reason)
    if not text:
        return []
    return [part for part in (clean_display_text(item) for item in text.split(";")) if part]


def parse_reason_dictionary(path: Path) -> tuple[dict[str, dict[str, Any]], list[dict[str, Any]]]:
    xls = pd.ExcelFile(path)
    df = pd.read_excel(path, sheet_name=xls.sheet_names[0], skiprows=1).dropna(how="all")
    exact_index: dict[str, dict[str, Any]] = {}
    items: list[dict[str, Any]] = []
    for _, row in df.iterrows():
        code = clean_display_text(row.iloc[0])
        reason = clean_display_text(row.iloc[1])
        group = clean_display_text(row.iloc[2])
        process = clean_display_text(row.iloc[3])
        source = clean_display_text(row.iloc[4])
        if not reason:
            continue
        item = {
            "code": code,
            "reason": reason,
            "reason_norm": normalize_for_match(reason),
            "group": group,
            "process_path": process,
            "source_note": source,
        }
        exact_index[item["reason_norm"]] = item
        items.append(item)
    items.sort(key=lambda entry: len(entry["reason_norm"]), reverse=True)
    return exact_index, items


def match_reason_dictionary(
    reason_text: str,
    exact_index: dict[str, dict[str, Any]],
    ranked_items: list[dict[str, Any]],
) -> dict[str, Any] | None:
    normalized = normalize_for_match(reason_text)
    if normalized in exact_index:
        return exact_index[normalized]
    for item in ranked_items:
        item_norm = item["reason_norm"]
        if item_norm and (item_norm in normalized or normalized in item_norm):
            return item
    return None


def extract_clause_refs(note_text: str) -> list[str]:
    refs: list[str] = []
    for pattern in CLAUSE_REF_PATTERNS:
        for match in pattern.findall(note_text):
            ref = clean_display_text(match)
            if ref and ref not in refs:
                refs.append(ref)
    return refs


def extract_service_mentions(note_text: str) -> list[dict[str, Any]]:
    mentions: list[dict[str, Any]] = []
    for raw_name, raw_amount in SERVICE_AMOUNT_RE.findall(note_text):
        name = clean_display_text(raw_name)
        if ":" in name:
            name = name.split(":")[-1].strip()
        name = re.sub(r"^(?:,|và|va)\s+", "", name, flags=re.IGNORECASE)
        name = re.sub(r"^chi phí\s+", "", name, flags=re.IGNORECASE)
        if len(name) < 3:
            continue
        name_norm = normalize_for_match(name)
        if len(name) > 120 or ". " in name:
            continue
        if any(marker in name_norm for marker in MENTION_BLOCKLIST):
            continue
        mentions.append(
            {
                "item_name": name.lstrip("*-: "),
                "amount_vnd": parse_vnd_amount(raw_amount),
            }
        )
    deduped: list[dict[str, Any]] = []
    seen: set[tuple[str, int | None]] = set()
    for item in mentions:
        key = (item["item_name"], item["amount_vnd"])
        if key in seen:
            continue
        seen.add(key)
        deduped.append(item)
    return deduped


def first_amount_for_unit(note_norm: str, unit_key: str) -> int | None:
    patterns = [
        rf"toi da[^0-9]{{0,80}}([0-9][0-9.,\s]*)\s*(?:vnd|dong)[^.\n]{{0,80}}/\s*{unit_key}",
        rf"bang[^0-9]{{0,40}}([0-9][0-9.,\s]*)\s*(?:vnd|dong)[^.\n]{{0,80}}/\s*{unit_key}",
        rf"([0-9][0-9.,\s]*)\s*(?:vnd|dong)\s*/\s*{unit_key}",
    ]
    for pattern in patterns:
        match = re.search(pattern, note_norm, flags=re.IGNORECASE)
        if match:
            return parse_vnd_amount(match.group(1))
    return None


def extract_formula(note_text: str) -> dict[str, int] | None:
    match = FORMULA_RE.search(note_text)
    if not match:
        return None
    requested = parse_vnd_amount(match.group(1))
    pay_percent = parse_vnd_amount(match.group(2))
    paid = parse_vnd_amount(match.group(3))
    if requested is None or pay_percent is None or paid is None:
        return None
    return {
        "formula_requested_vnd": requested,
        "formula_pay_percent": pay_percent,
        "formula_paid_vnd": paid,
    }


def detect_note_signals(note_text: str, atomic_reason_norms: list[str]) -> dict[str, Any]:
    note = clean_display_text(note_text)
    note_norm = normalize_for_match(note)
    reason_blob = " ; ".join(atomic_reason_norms)

    formula = extract_formula(note)
    copay_phrase = re.search(r"dong chi tra\s*(\d{1,3})\s*%", note_norm, flags=re.IGNORECASE)
    copay_percent = parse_vnd_amount(copay_phrase.group(1)) if copay_phrase else None
    if copay_percent is None and formula and "dong chi tra" in note_norm:
        pay_percent = formula["formula_pay_percent"]
        if 0 <= pay_percent <= 100:
            copay_percent = 100 - pay_percent

    per_visit = "lan kham" in note_norm or "mot lan kham" in reason_blob
    per_day = "ngay nam vien" in note_norm or "mot ngay nam vien" in reason_blob
    per_year = "/ nam" in note_norm or "trong nam hop dong" in note_norm or "trong nam hop dong" in reason_blob

    screening = any(token in note_norm for token in ["tam soat", "kiem tra/tam soat", "kiem tra, tam soat"]) or any(
        token in reason_blob for token in ["tam soat", "kiem tra/tam soat"]
    )
    missing_docs = "chung tu" in note_norm or any(token in reason_blob for token in ["chung tu", "ho so khong day du"])
    not_covered = any(
        token in note_norm
        for token in [
            "khong thuoc pham vi bao hiem",
            "khong thuoc quyen loi",
            "diem loai tru",
        ]
    ) or any(token in reason_blob for token in ["khong thuoc quyen loi", "diem loai tru"])

    return {
        "copay_flag": copay_percent is not None or "dong chi tra" in note_norm or any("dong chi tra" in token for token in atomic_reason_norms),
        "copay_percent": copay_percent,
        "screening_flag": screening,
        "missing_documents_flag": missing_docs,
        "not_covered_flag": not_covered,
        "exclusion_scope_flag": "loai tru" in note_norm,
        "limit_per_visit_flag": per_visit,
        "limit_per_day_flag": per_day,
        "limit_per_year_flag": per_year,
        "generic_limit_flag": any("han muc" in token for token in atomic_reason_norms),
        "limit_per_visit_vnd": first_amount_for_unit(note_norm, "lan kham") if per_visit else None,
        "limit_per_day_vnd": first_amount_for_unit(note_norm, "ngay") if per_day else None,
        "limit_per_year_vnd": first_amount_for_unit(note_norm, "nam") if per_year else None,
        "clause_references": extract_clause_refs(note),
        "service_mentions": extract_service_mentions(note),
        "formula": formula,
    }


def decision_label(gap_amount_vnd: int, paid_amount_vnd: int) -> str:
    if gap_amount_vnd <= 0:
        return "full_pay"
    if paid_amount_vnd <= 0:
        return "full_deny"
    return "partial_pay"


def load_claim_frame(path: Path) -> pd.DataFrame:
    df = pd.read_excel(path, sheet_name="data")
    return pd.DataFrame(
        {
            "policy_no": df.iloc[:, 0].fillna("").astype(str),
            "contract": df.iloc[:, 1].fillna("").astype(str),
            "rule": df.iloc[:, 2].fillna("").astype(str),
            "claim_id": df.iloc[:, 3].fillna("").astype(str),
            "admission_date": df.iloc[:, 4],
            "discharge_date": df.iloc[:, 5],
            "diagnosis_text": df.iloc[:, 6].fillna("").astype(str),
            "facility_name": df.iloc[:, 7].fillna("").astype(str),
            "care_type": df.iloc[:, 8].fillna("").astype(str),
            "benefit_name": df.iloc[:, 9].fillna("").astype(str),
            "requested_amount_vnd": pd.to_numeric(df.iloc[:, 10], errors="coerce").fillna(0).astype(int),
            "paid_amount_vnd": pd.to_numeric(df.iloc[:, 11], errors="coerce").fillna(0).astype(int),
            "reason_raw": df.iloc[:, 12].fillna("").astype(str),
            "note_text": df.iloc[:, 13].fillna("").astype(str),
            "insured_name": df.iloc[:, 14].fillna("").astype(str),
            "approved_entitlement_hint": df.iloc[:, 15].fillna("").astype(str),
            "icd_text": df.iloc[:, 16].fillna("").astype(str),
            "status": df.iloc[:, 17].fillna("").astype(str),
        }
    )


def build_line_record(
    row_number: int,
    row: pd.Series,
    reason_index: dict[str, dict[str, Any]],
    ranked_reason_items: list[dict[str, Any]],
) -> dict[str, Any]:
    atomic_reason_entries: list[dict[str, Any]] = []
    atomic_reason_norms: list[str] = []
    for atomic_reason in split_atomic_reasons(row["reason_raw"]):
        matched = match_reason_dictionary(atomic_reason, reason_index, ranked_reason_items)
        entry = {
            "reason_text": atomic_reason,
            "reason_norm": normalize_for_match(atomic_reason),
            "dictionary_code": matched["code"] if matched else None,
            "dictionary_group": matched["group"] if matched else None,
            "dictionary_process_path": matched["process_path"] if matched else None,
        }
        atomic_reason_entries.append(entry)
        atomic_reason_norms.append(entry["reason_norm"])

    gap_amount_vnd = int(row["requested_amount_vnd"] - row["paid_amount_vnd"])
    note_signals = detect_note_signals(row["note_text"], atomic_reason_norms)
    return {
        "row_id": row_number,
        "claim_line_id": f"{clean_display_text(row['claim_id'])}:{row_number}",
        "claim_id": clean_display_text(row["claim_id"]),
        "policy_no": clean_display_text(row["policy_no"]),
        "contract_name": clean_display_text(row["contract"]),
        "insurer": infer_insurer(clean_display_text(row["contract"])),
        "rule_name": clean_display_text(row["rule"]),
        "admission_date": parse_date(row["admission_date"]),
        "discharge_date": parse_date(row["discharge_date"]),
        "diagnosis_text": clean_display_text(row["diagnosis_text"]),
        "diagnosis_norm": normalize_for_match(row["diagnosis_text"]),
        "facility_name": clean_display_text(row["facility_name"]),
        "care_type": clean_display_text(row["care_type"]),
        "benefit_name": clean_display_text(row["benefit_name"]),
        "requested_amount_vnd": int(row["requested_amount_vnd"]),
        "paid_amount_vnd": int(row["paid_amount_vnd"]),
        "gap_amount_vnd": gap_amount_vnd,
        "decision_label": decision_label(gap_amount_vnd, int(row["paid_amount_vnd"])),
        "reason_raw": clean_display_text(row["reason_raw"]),
        "atomic_reasons": atomic_reason_entries,
        "note_text": clean_display_text(row["note_text"]),
        "note_signals": note_signals,
        "insured_name": clean_display_text(row["insured_name"]),
        "approved_entitlement_hint": clean_display_text(row["approved_entitlement_hint"]),
        "icd_text": clean_display_text(row["icd_text"]),
        "status": clean_display_text(row["status"]),
    }


def summarize_records(
    records: list[dict[str, Any]],
    reason_items: list[dict[str, Any]],
    source_path: Path,
    reason_dict_path: Path,
) -> tuple[dict[str, Any], dict[str, Any]]:
    decision_counts = Counter(record["decision_label"] for record in records)
    signal_counts = Counter()
    atomic_reason_counts = Counter()
    atomic_reason_gap = Counter()
    reason_group_counts = Counter()
    contract_stats: dict[str, dict[str, Any]] = {}
    top_service_mentions = Counter()
    copay_percent_counts = Counter()
    limit_type_counts = Counter()
    contract_reason_profiles: dict[tuple[str, str], dict[str, Any]] = {}

    for record in records:
        note_signals = record["note_signals"]
        for key in [
            "copay_flag",
            "screening_flag",
            "missing_documents_flag",
            "not_covered_flag",
            "exclusion_scope_flag",
            "limit_per_visit_flag",
            "limit_per_day_flag",
            "limit_per_year_flag",
            "generic_limit_flag",
        ]:
            if note_signals.get(key):
                signal_counts[key] += 1
        if note_signals.get("formula"):
            signal_counts["formula_present"] += 1
        if note_signals.get("clause_references"):
            signal_counts["clause_reference_present"] += 1
        if note_signals.get("service_mentions"):
            signal_counts["service_mentions_present"] += 1
        if note_signals.get("copay_percent") is not None:
            copay_percent_counts[str(note_signals["copay_percent"])] += 1
        if note_signals.get("limit_per_visit_flag"):
            limit_type_counts["per_visit"] += 1
        if note_signals.get("limit_per_day_flag"):
            limit_type_counts["per_day"] += 1
        if note_signals.get("limit_per_year_flag"):
            limit_type_counts["per_year"] += 1
        if note_signals.get("generic_limit_flag"):
            limit_type_counts["generic_limit"] += 1

        contract_name = record["contract_name"] or "(trống)"
        contract_entry = contract_stats.setdefault(
            contract_name,
            {
                "contract_name": contract_name,
                "insurer": record["insurer"],
                "rows": 0,
                "requested_sum_vnd": 0,
                "paid_sum_vnd": 0,
                "gap_sum_vnd": 0,
                "decision_counts": Counter(),
                "signal_counts": Counter(),
            },
        )
        contract_entry["rows"] += 1
        contract_entry["requested_sum_vnd"] += record["requested_amount_vnd"]
        contract_entry["paid_sum_vnd"] += record["paid_amount_vnd"]
        contract_entry["gap_sum_vnd"] += record["gap_amount_vnd"]
        contract_entry["decision_counts"][record["decision_label"]] += 1
        for signal_key, signal_value in note_signals.items():
            if isinstance(signal_value, bool) and signal_value:
                contract_entry["signal_counts"][signal_key] += 1

        for mention in note_signals.get("service_mentions", []):
            top_service_mentions[mention["item_name"]] += 1

        for atomic_reason in record["atomic_reasons"]:
            atomic_reason_counts[atomic_reason["reason_text"]] += 1
            atomic_reason_gap[atomic_reason["reason_text"]] += record["gap_amount_vnd"]
            if atomic_reason["dictionary_group"]:
                reason_group_counts[atomic_reason["dictionary_group"]] += 1

            profile_key = (contract_name, atomic_reason["reason_text"])
            profile = contract_reason_profiles.setdefault(
                profile_key,
                {
                    "contract_name": contract_name,
                    "insurer": record["insurer"],
                    "atomic_reason": atomic_reason["reason_text"],
                    "atomic_reason_norm": atomic_reason["reason_norm"],
                    "dictionary_code": atomic_reason["dictionary_code"],
                    "dictionary_group": atomic_reason["dictionary_group"],
                    "rows": 0,
                    "gap_sum_vnd": 0,
                    "care_types": Counter(),
                    "benefits": Counter(),
                    "copay_percents": Counter(),
                    "limit_types": Counter(),
                    "clause_references": Counter(),
                    "service_mentions": Counter(),
                    "example_notes": [],
                },
            )
            profile["rows"] += 1
            profile["gap_sum_vnd"] += record["gap_amount_vnd"]
            profile["care_types"][record["care_type"] or "(trống)"] += 1
            profile["benefits"][record["benefit_name"] or "(trống)"] += 1
            if note_signals.get("copay_percent") is not None:
                profile["copay_percents"][str(note_signals["copay_percent"])] += 1
            if note_signals.get("limit_per_visit_flag"):
                profile["limit_types"]["per_visit"] += 1
            if note_signals.get("limit_per_day_flag"):
                profile["limit_types"]["per_day"] += 1
            if note_signals.get("limit_per_year_flag"):
                profile["limit_types"]["per_year"] += 1
            if note_signals.get("generic_limit_flag"):
                profile["limit_types"]["generic_limit"] += 1
            for ref in note_signals.get("clause_references", []):
                profile["clause_references"][ref] += 1
            for mention in note_signals.get("service_mentions", []):
                profile["service_mentions"][mention["item_name"]] += 1
            if record["note_text"] and len(profile["example_notes"]) < 3:
                profile["example_notes"].append(record["note_text"])

    total_atomic_reasons = sum(len(record["atomic_reasons"]) for record in records)
    matched_atomic_reasons = sum(
        1
        for record in records
        for atomic_reason in record["atomic_reasons"]
        if atomic_reason["dictionary_code"]
    )

    summary = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "sources": {
            "main_claims": str(source_path.relative_to(PROJECT_DIR)),
            "reason_dictionary": str(reason_dict_path.relative_to(PROJECT_DIR)),
        },
        "stats": {
            "rows": len(records),
            "unique_claim_ids": len({record["claim_id"] for record in records}),
            "unique_contracts": len({record["contract_name"] for record in records}),
            "requested_sum_vnd": sum(record["requested_amount_vnd"] for record in records),
            "paid_sum_vnd": sum(record["paid_amount_vnd"] for record in records),
            "gap_sum_vnd": sum(record["gap_amount_vnd"] for record in records),
            "decision_counts": dict(decision_counts),
            "atomic_reason_total": total_atomic_reasons,
            "atomic_reason_dictionary_match_pct": round(
                100.0 * matched_atomic_reasons / total_atomic_reasons, 2
            )
            if total_atomic_reasons
            else 0.0,
            "reason_dictionary_size": len(reason_items),
        },
        "signal_counts": dict(signal_counts),
        "copay_percent_distribution": dict(copay_percent_counts),
        "limit_type_counts": dict(limit_type_counts),
        "top_atomic_reasons_by_rows": [
            {"reason": reason, "rows": count}
            for reason, count in atomic_reason_counts.most_common(20)
        ],
        "top_atomic_reasons_by_gap_vnd": [
            {"reason": reason, "gap_vnd": gap}
            for reason, gap in atomic_reason_gap.most_common(20)
        ],
        "reason_group_distribution": [
            {"group": group, "rows": count}
            for group, count in reason_group_counts.most_common(20)
        ],
        "top_service_mentions_in_notes": [
            {"item_name": name, "rows": count}
            for name, count in top_service_mentions.most_common(20)
        ],
        "contracts": [
            {
                "contract_name": item["contract_name"],
                "insurer": item["insurer"],
                "rows": item["rows"],
                "requested_sum_vnd": item["requested_sum_vnd"],
                "paid_sum_vnd": item["paid_sum_vnd"],
                "gap_sum_vnd": item["gap_sum_vnd"],
                "paid_ratio_pct": round(
                    100.0 * item["paid_sum_vnd"] / item["requested_sum_vnd"], 2
                )
                if item["requested_sum_vnd"]
                else 0.0,
                "decision_counts": dict(item["decision_counts"]),
                "signal_counts": dict(item["signal_counts"]),
            }
            for item in sorted(contract_stats.values(), key=lambda entry: entry["rows"], reverse=True)
        ],
    }

    grouped_profiles: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for profile in contract_reason_profiles.values():
        grouped_profiles[profile["contract_name"]].append(
            {
                "atomic_reason": profile["atomic_reason"],
                "atomic_reason_norm": profile["atomic_reason_norm"],
                "dictionary_code": profile["dictionary_code"],
                "dictionary_group": profile["dictionary_group"],
                "rows": profile["rows"],
                "gap_sum_vnd": profile["gap_sum_vnd"],
                "top_care_types": [
                    {"care_type": value, "rows": count}
                    for value, count in profile["care_types"].most_common(5)
                ],
                "top_benefits": [
                    {"benefit": value, "rows": count}
                    for value, count in profile["benefits"].most_common(5)
                ],
                "copay_percent_distribution": dict(profile["copay_percents"]),
                "limit_type_counts": dict(profile["limit_types"]),
                "top_clause_references": [
                    {"reference": value, "rows": count}
                    for value, count in profile["clause_references"].most_common(5)
                ],
                "top_service_mentions": [
                    {"item_name": value, "rows": count}
                    for value, count in profile["service_mentions"].most_common(10)
                ],
                "example_notes": profile["example_notes"],
            }
        )

    catalog = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "sources": summary["sources"],
        "contracts": [
            {
                "contract_name": contract["contract_name"],
                "insurer": contract["insurer"],
                "rows": contract["rows"],
                "gap_sum_vnd": contract["gap_sum_vnd"],
                "reason_profiles": sorted(
                    grouped_profiles.get(contract["contract_name"], []),
                    key=lambda item: item["rows"],
                    reverse=True,
                ),
            }
            for contract in summary["contracts"]
        ],
    }
    return summary, catalog


def main() -> None:
    main_claims_path = find_required_file("Danh sách hồ sơ loại trừ.xlsx")
    reason_dict_path = find_required_file("Lý do loại trừ.xlsx")

    reason_index, ranked_reason_items = parse_reason_dictionary(reason_dict_path)
    frame = load_claim_frame(main_claims_path)
    records = [
        build_line_record(row_number + 1, row, reason_index, ranked_reason_items)
        for row_number, (_, row) in enumerate(frame.iterrows())
    ]

    OUTPUT_JSONL_PATH.write_text(
        "\n".join(json.dumps(record, ensure_ascii=False) for record in records) + "\n",
        encoding="utf-8",
    )
    summary, catalog = summarize_records(records, ranked_reason_items, main_claims_path, reason_dict_path)
    OUTPUT_SUMMARY_PATH.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    OUTPUT_CATALOG_PATH.write_text(json.dumps(catalog, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"Claim signal dataset saved: {OUTPUT_JSONL_PATH}")
    print(f"Summary saved: {OUTPUT_SUMMARY_PATH}")
    print(f"Catalog saved: {OUTPUT_CATALOG_PATH}")
    print(f"Rows: {summary['stats']['rows']}")
    print(f"Atomic reason match pct: {summary['stats']['atomic_reason_dictionary_match_pct']}")
    print(f"Signals: {summary['signal_counts']}")


if __name__ == "__main__":
    main()
