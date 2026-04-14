from __future__ import annotations

import json
import re
from collections import Counter
from datetime import datetime, timezone
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any

import pandas as pd

from extract_exclusion_claim_signals import clean_display_text, find_required_file, normalize_for_match
from link_exclusion_note_mentions import (
    build_observation_mapping,
    build_service_links_index,
    build_service_mapping,
    load_observation_helpers,
    load_service_mapper,
    summarize_clinical_links,
)


PROJECT_DIR = Path(__file__).parent.parent
INSURANCE_DIR = PROJECT_DIR / "06_insurance"

CONTRACT_CATALOG_PATH = INSURANCE_DIR / "contract_benefit_catalog.json"
INTERPRETATION_CATALOG_PATH = INSURANCE_DIR / "benefit_interpretation_catalog.json"
DETAIL_LINKS_JSONL_PATH = INSURANCE_DIR / "benefit_detail_service_links.jsonl"
PACK_JSON_PATH = INSURANCE_DIR / "benefit_contract_knowledge_pack.json"
PACK_MD_PATH = INSURANCE_DIR / "benefit_contract_knowledge_pack.md"

SERVICE_THRESHOLD = 78.0

CONTRACT_SPECS = [
    {
        "filename": "FPT NT 2024.xlsx",
        "contract_id": "FPT-NT-2024",
        "product_name": "FPT Ngoai tru 2024",
        "benefit_sheet": "1. Quyen loi bao hiem",
        "condition_sheets": ["2. Dieu kien mo rong"],
        "mode": "multi_plan",
    },
    {
        "filename": "FPT NT 2025.xlsx",
        "contract_id": "FPT-NT-2025",
        "product_name": "FPT Ngoai tru 2025",
        "benefit_sheet": "Quyen loi",
        "condition_sheets": ["Dieu kien"],
        "mode": "multi_plan",
    },
    {
        "filename": "FPT NV.xlsx",
        "contract_id": "FPT-NV",
        "product_name": "FPT Noi vien",
        "benefit_sheet": "QLBH",
        "condition_sheets": ["PL3 2025 - FINAL"],
        "mode": "multi_plan",
    },
    {
        "filename": "TINPNC.xlsx",
        "contract_id": "TIN-PNC",
        "product_name": "TINPNC",
        "benefit_sheet": "Quyen loi BHSK",
        "condition_sheets": ["Điều khoản", "PL3- So sanh"],
        "mode": "single_plan",
    },
]


def compact_text(value: object) -> str:
    text = clean_display_text(value)
    return "" if text.lower() == "nat" else text


def normalize_key(value: object) -> str:
    return normalize_for_match(compact_text(value))


def split_lines(value: object) -> list[str]:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return []
    text = str(value).replace("\r\n", "\n").replace("\r", "\n")
    lines = [clean_display_text(line).strip(" -*•\t") for line in text.split("\n")]
    return [line for line in lines if line and line.lower() != "nan"]


def shorten_cell(value: object) -> str:
    lines = split_lines(value)
    if not lines:
        return compact_text(value)
    return lines[0]


def clean_plan_label(value: object) -> str:
    label = shorten_cell(value)
    return re.sub(r"\s+", " ", label) if label else ""


def strip_leading_marker(value: str) -> str:
    text = re.sub(r"^\d+(?:\.\d+)*[\).]?\s*", "", value, flags=re.IGNORECASE).strip()
    text = re.sub(r"^[ivxlcdm]+[\).]\s*", "", text, flags=re.IGNORECASE).strip()
    text = re.sub(r"^[a-z][\).]\s*", "", text, flags=re.IGNORECASE).strip()
    return text


def benefit_name_candidates(value: str) -> list[str]:
    raw = compact_text(value)
    if not raw:
        return []
    candidates = [raw, strip_leading_marker(raw)]
    for sep in [":", ";", "("]:
        if sep in raw:
            candidates.append(raw.split(sep, 1)[0].strip())
    seen: set[str] = set()
    result: list[str] = []
    for candidate in candidates:
        key = normalize_key(candidate)
        if candidate and key and key not in seen:
            seen.add(key)
            result.append(candidate)
    return result


def is_major_section(value: str) -> bool:
    key = normalize_key(value)
    if not key:
        return False
    if re.match(r"^[ivxlcdm]+\b", key):
        return True
    return key in {"thong tin chung", "quyen loi bao hiem", "bao hiem suc khoe", "bao hiem tai nan"}


def is_subsection(value: str) -> bool:
    return bool(re.match(r"^\d+(?:\.\d+)?\b", normalize_key(value)))


def detect_header_row(frame: pd.DataFrame, patterns: list[str]) -> int:
    for idx, row in frame.iterrows():
        joined = " | ".join(normalize_key(cell) for cell in row.tolist() if normalize_key(cell))
        if any(pattern in joined for pattern in patterns):
            return int(idx)
    return 0


def detect_multiplan_header_row(frame: pd.DataFrame) -> int:
    for idx, row in frame.iterrows():
        row_values = [compact_text(cell) for cell in row.tolist()]
        joined = " | ".join(normalize_key(value) for value in row_values if normalize_key(value))
        nonempty_cells = [value for value in row_values if value]
        if len(nonempty_cells) >= 3 and ("goi bao hiem" in joined or "quyen loi bao hiem" in joined):
            return int(idx)
    return detect_header_row(frame, ["goi bao hiem", "quyen loi bao hiem"])


def inspect_workbook(path: Path) -> list[dict[str, Any]]:
    xls = pd.ExcelFile(path)
    sheets: list[dict[str, Any]] = []
    for sheet_name in xls.sheet_names:
        df = pd.read_excel(path, sheet_name=sheet_name, header=None)
        nonempty = df.dropna(how="all")
        sheets.append(
            {
                "sheet_name": sheet_name,
                "rows": int(df.shape[0]),
                "cols": int(df.shape[1]),
                "nonempty_rows": int(nonempty.shape[0]),
                "nonempty_cols": int(nonempty.dropna(axis=1, how="all").shape[1]) if not nonempty.empty else 0,
            }
        )
    return sheets


def build_interpretation_catalog() -> dict[str, Any]:
    path = find_required_file("Diễn giải chi tiết về các quyền lợi bảo hiểm 7.10.25.xlsx")
    df = pd.read_excel(path).fillna("")
    entries: list[dict[str, Any]] = []
    alias_items: list[dict[str, Any]] = []
    current_group = ""

    for row_index, (_, row) in enumerate(df.iterrows(), start=1):
        group_name = compact_text(row.iloc[0])
        if group_name:
            current_group = group_name

        definition = compact_text(row.iloc[2])
        interpretation = compact_text(row.iloc[3])
        aliases = [strip_leading_marker(line) for line in split_lines(row.iloc[1])]
        aliases = [alias for alias in aliases if alias]
        if not aliases:
            continue

        entry_id = f"BEN-{len(entries) + 1:03d}"
        entry = {
            "entry_id": entry_id,
            "row_index": row_index,
            "group_name": current_group,
            "canonical_name": aliases[0],
            "aliases": aliases,
            "definition_text": definition,
            "interpretation_text": interpretation,
            "evidence_hints": split_lines(row.iloc[4]),
            "source_file": str(path.relative_to(PROJECT_DIR)),
        }
        entries.append(entry)
        for alias in aliases:
            alias_items.append(
                {
                    "entry_id": entry_id,
                    "alias": alias,
                    "alias_norm": normalize_key(alias),
                    "canonical_name": entry["canonical_name"],
                    "group_name": current_group,
                }
            )

    catalog = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source_file": str(path.relative_to(PROJECT_DIR)),
        "stats": {
            "rows": int(len(df)),
            "entries": len(entries),
            "aliases": len(alias_items),
            "groups": len({entry["group_name"] for entry in entries if entry["group_name"]}),
        },
        "entries": entries,
    }
    catalog["_alias_items"] = alias_items
    return catalog


def match_interpretation(name: str, alias_items: list[dict[str, Any]]) -> dict[str, Any]:
    best: dict[str, Any] | None = None
    for candidate in benefit_name_candidates(name):
        candidate_norm = normalize_key(candidate)
        if not candidate_norm:
            continue
        for alias in alias_items:
            alias_norm = alias["alias_norm"]
            if not alias_norm:
                continue
            if candidate_norm == alias_norm:
                score = 100.0
                method = "exact"
            elif alias_norm in candidate_norm or candidate_norm in alias_norm:
                score = 92.0
                method = "contains"
            else:
                score = round(100.0 * SequenceMatcher(None, candidate_norm, alias_norm).ratio(), 2)
                method = "fuzzy"
            if score < 78.0:
                continue
            match = {
                "entry_id": alias["entry_id"],
                "canonical_name": alias["canonical_name"],
                "group_name": alias["group_name"],
                "matched_alias": alias["alias"],
                "candidate": candidate,
                "score": score,
                "method": method,
            }
            if not best or match["score"] > best["score"]:
                best = match
    return best or {
        "entry_id": "",
        "canonical_name": "",
        "group_name": "",
        "matched_alias": "",
        "candidate": "",
        "score": 0.0,
        "method": "unmatched",
    }


def extract_multiplan_benefits(path: Path, contract_id: str, sheet_name: str, alias_items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    df = pd.read_excel(path, sheet_name=sheet_name, header=None).fillna("")
    header_idx = detect_multiplan_header_row(df)
    header_row = df.iloc[header_idx]
    label_col_idx = next((col_idx for col_idx, value in enumerate(header_row) if clean_plan_label(value)), 0)
    plan_columns = [
        (col_idx, clean_plan_label(value))
        for col_idx, value in enumerate(header_row)
        if col_idx > label_col_idx and clean_plan_label(value)
    ]
    entries: list[dict[str, Any]] = []
    current_section = ""
    current_subsection = ""

    for idx in range(header_idx + 1, len(df)):
        row = df.iloc[idx]
        label = compact_text(row.iloc[label_col_idx]) if len(row) > label_col_idx else ""
        if not label:
            continue
        values = {plan_name: compact_text(row.iloc[col_idx]) for col_idx, plan_name in plan_columns}
        nonempty_values = [value for value in values.values() if value]
        if not nonempty_values:
            if is_major_section(label):
                current_section = label
                current_subsection = ""
            elif is_subsection(label):
                current_subsection = label
            continue

        if is_major_section(label):
            current_section = label
        elif is_subsection(label):
            current_subsection = label

        entries.append(
            {
                "contract_id": contract_id,
                "source_file": str(path.relative_to(PROJECT_DIR)),
                "sheet_name": sheet_name,
                "row_index": idx + 1,
                "entry_label": label,
                "major_section": current_section,
                "subsection": current_subsection,
                "mode": "multi_plan",
                "coverage_by_plan": values,
                "nonempty_plan_count": len(nonempty_values),
                "interpretation_match": match_interpretation(label, alias_items),
            }
        )
    return entries


def extract_singleplan_benefits(path: Path, contract_id: str, sheet_name: str, alias_items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    df = pd.read_excel(path, sheet_name=sheet_name, header=None).fillna("")
    header_idx = detect_header_row(df, ["stt", "quyen loi"])
    entries: list[dict[str, Any]] = []
    current_section = ""

    for idx in range(header_idx + 1, len(df)):
        row = df.iloc[idx]
        code = compact_text(row.iloc[0])
        label = compact_text(row.iloc[1]) if len(row) > 1 else ""
        coverage = compact_text(row.iloc[2]) if len(row) > 2 else ""
        if not code and not label and not coverage:
            continue
        if code and re.fullmatch(r"[IVXLC]+", code, flags=re.IGNORECASE) and label:
            current_section = label
            continue
        if not label:
            continue
        entries.append(
            {
                "contract_id": contract_id,
                "source_file": str(path.relative_to(PROJECT_DIR)),
                "sheet_name": sheet_name,
                "row_index": idx + 1,
                "entry_code": code,
                "entry_label": label,
                "major_section": current_section,
                "mode": "single_plan",
                "coverage_value": coverage,
                "interpretation_match": match_interpretation(label, alias_items),
            }
        )
    return entries


def extract_clause_rows(path: Path, contract_id: str, sheet_name: str) -> list[dict[str, Any]]:
    df = pd.read_excel(path, sheet_name=sheet_name, header=None).fillna("")
    header_idx = detect_header_row(df, ["stt", "muc", "quyen loi bao hiem"])
    clauses: list[dict[str, Any]] = []
    current_section = ""

    for idx in range(header_idx + 1, len(df)):
        row = [compact_text(value) for value in df.iloc[idx].tolist()[:5]]
        if not any(row):
            continue
        code = row[0]
        title = row[1] if len(row) > 1 else ""
        body = "\n".join(value for value in row[2:] if value)

        if code and not title and not body:
            current_section = code
            continue
        if is_major_section(code) and title and not body:
            current_section = f"{code} {title}".strip()
            continue

        clause_title = title or code
        if not clause_title and not body:
            continue
        clauses.append(
            {
                "contract_id": contract_id,
                "source_file": str(path.relative_to(PROJECT_DIR)),
                "sheet_name": sheet_name,
                "row_index": idx + 1,
                "section": current_section,
                "clause_code": code,
                "clause_title": clause_title,
                "clause_body": body,
            }
        )
    return clauses


def build_contract_catalog(alias_items: list[dict[str, Any]]) -> dict[str, Any]:
    contracts: list[dict[str, Any]] = []
    all_benefits: list[dict[str, Any]] = []
    all_clauses: list[dict[str, Any]] = []

    for spec in CONTRACT_SPECS:
        path = find_required_file(spec["filename"])
        sheets = inspect_workbook(path)
        if spec["mode"] == "multi_plan":
            benefit_entries = extract_multiplan_benefits(path, spec["contract_id"], spec["benefit_sheet"], alias_items)
        else:
            benefit_entries = extract_singleplan_benefits(path, spec["contract_id"], spec["benefit_sheet"], alias_items)

        clause_entries: list[dict[str, Any]] = []
        available_sheets = {sheet["sheet_name"] for sheet in sheets}
        for sheet_name in spec["condition_sheets"]:
            if sheet_name in available_sheets:
                clause_entries.extend(extract_clause_rows(path, spec["contract_id"], sheet_name))

        contracts.append(
            {
                "contract_id": spec["contract_id"],
                "product_name": spec["product_name"],
                "source_file": str(path.relative_to(PROJECT_DIR)),
                "mode": spec["mode"],
                "sheet_inventory": sheets,
                "benefit_entries": benefit_entries,
                "condition_clauses": clause_entries,
                "stats": {
                    "benefit_rows": len(benefit_entries),
                    "clause_rows": len(clause_entries),
                    "matched_benefit_rows": sum(1 for entry in benefit_entries if entry["interpretation_match"].get("entry_id")),
                },
            }
        )
        all_benefits.extend(benefit_entries)
        all_clauses.extend(clause_entries)

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "contracts": contracts,
        "stats": {
            "contracts": len(contracts),
            "benefit_rows": len(all_benefits),
            "clause_rows": len(all_clauses),
            "matched_benefit_rows": sum(1 for entry in all_benefits if entry["interpretation_match"].get("entry_id")),
            "unique_interpretation_hits": len({entry["interpretation_match"]["entry_id"] for entry in all_benefits if entry["interpretation_match"].get("entry_id")}),
        },
    }


def build_detail_links(alias_items: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    path = find_required_file("BaoCaoPhanBoChiPhi 14.10.25.xlsx")
    df = pd.read_excel(path, sheet_name="DataTemplate").dropna(how="all")
    detail_frame = pd.DataFrame(
        {
            "claim_id": df.iloc[:, 0].fillna("").astype(str),
            "benefit_count": pd.to_numeric(df.iloc[:, 1], errors="coerce").fillna(0).astype(int),
            "requested_amount_vnd": pd.to_numeric(df.iloc[:, 2], errors="coerce").fillna(0).astype(int),
            "claim_total_vnd": pd.to_numeric(df.iloc[:, 3], errors="coerce").fillna(0).astype(int),
            "benefit_code": df.iloc[:, 4].fillna("").astype(str),
            "benefit_name": df.iloc[:, 5].fillna("").astype(str),
            "detail_name": df.iloc[:, 6].fillna("").astype(str),
            "detail_amount_vnd": pd.to_numeric(df.iloc[:, 7], errors="coerce").fillna(0).astype(int),
            "amount_gap_vnd": pd.to_numeric(df.iloc[:, 10], errors="coerce").fillna(0).astype(int),
            "status": df.iloc[:, 11].fillna("").astype(str),
        }
    )
    benefit_per_claim = pd.read_excel(path, sheet_name="QuyenLoiHoSo").dropna(how="all")

    unique_detail_names = {compact_text(name) for name in detail_frame["detail_name"].tolist() if compact_text(name)}
    mapper, codebook_by_code = load_service_mapper()
    alias_map, _concept_by_code, candidate_builder = load_observation_helpers()
    links_by_service = build_service_links_index()
    service_mapping = build_service_mapping(unique_detail_names, mapper, codebook_by_code, SERVICE_THRESHOLD)
    observation_mapping = build_observation_mapping(unique_detail_names, alias_map, candidate_builder)

    detail_rows: list[dict[str, Any]] = []
    unmatched_details = Counter()
    benefit_profiles: dict[str, dict[str, Any]] = {}
    service_mapped_rows = 0
    observation_mapped_rows = 0
    clinical_linked_rows = 0

    for row_index, (_, row) in enumerate(detail_frame.iterrows(), start=1):
        detail_name = compact_text(row["detail_name"])
        benefit_name = compact_text(row["benefit_name"])
        service_info = service_mapping.get(detail_name, {})
        observation_info = observation_mapping.get(detail_name, {})
        service_code = service_info.get("service_code", "")
        clinical_links = summarize_clinical_links(links_by_service.get(service_code, []))
        interpretation = match_interpretation(benefit_name, alias_items)

        if service_code:
            service_mapped_rows += 1
        else:
            unmatched_details[detail_name] += 1
        if observation_info.get("concept_code"):
            observation_mapped_rows += 1
        if clinical_links.get("linked_disease_count", 0) > 0:
            clinical_linked_rows += 1

        record = {
            "detail_id": f"{compact_text(row['claim_id'])}:{row_index}",
            "claim_id": compact_text(row["claim_id"]),
            "benefit_code": compact_text(row["benefit_code"]),
            "benefit_name": benefit_name,
            "detail_name": detail_name,
            "detail_amount_vnd": int(row["detail_amount_vnd"]),
            "requested_amount_vnd": int(row["requested_amount_vnd"]),
            "claim_total_vnd": int(row["claim_total_vnd"]),
            "amount_gap_vnd": int(row["amount_gap_vnd"]),
            "status": compact_text(row["status"]),
            "benefit_interpretation_match": interpretation,
            "service_mapping": service_info,
            "observation_mapping": observation_info,
            "clinical_links": clinical_links,
        }
        detail_rows.append(record)

        profile = benefit_profiles.setdefault(
            benefit_name or "(trong)",
            {
                "benefit_name": benefit_name or "(trong)",
                "benefit_codes": Counter(),
                "rows": 0,
                "unique_claims": set(),
                "detail_amount_vnd": 0,
                "status": Counter(),
                "detail_names": Counter(),
                "service_codes": Counter(),
                "service_names": {},
                "unmapped_details": Counter(),
                "interpretation_match": interpretation,
            },
        )
        profile["rows"] += 1
        profile["unique_claims"].add(compact_text(row["claim_id"]))
        profile["detail_amount_vnd"] += int(row["detail_amount_vnd"])
        profile["benefit_codes"][compact_text(row["benefit_code"]) or "(trong)"] += 1
        profile["status"][compact_text(row["status"]) or "(trong)"] += 1
        profile["detail_names"][detail_name or "(trong)"] += 1
        if service_code:
            profile["service_codes"][service_code] += 1
            profile["service_names"][service_code] = service_info.get("canonical_name", "")
        else:
            profile["unmapped_details"][detail_name or "(trong)"] += 1

    benefit_profiles_list = []
    for profile in benefit_profiles.values():
        benefit_profiles_list.append(
            {
                "benefit_name": profile["benefit_name"],
                "benefit_code_distribution": dict(profile["benefit_codes"]),
                "rows": profile["rows"],
                "unique_claims": len(profile["unique_claims"]),
                "detail_amount_vnd": profile["detail_amount_vnd"],
                "interpretation_match": profile["interpretation_match"],
                "top_statuses": [{"status": name, "rows": count} for name, count in profile["status"].most_common(10)],
                "top_detail_names": [{"detail_name": name, "rows": count} for name, count in profile["detail_names"].most_common(10)],
                "top_mapped_services": [
                    {"service_code": code, "canonical_name": profile["service_names"].get(code, ""), "rows": count}
                    for code, count in profile["service_codes"].most_common(10)
                ],
                "top_unmapped_details": [{"detail_name": name, "rows": count} for name, count in profile["unmapped_details"].most_common(10)],
            }
        )

    summary = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source_file": str(path.relative_to(PROJECT_DIR)),
        "stats": {
            "rows": len(detail_rows),
            "unique_claim_ids": int(detail_frame["claim_id"].nunique()),
            "unique_benefit_codes": int(detail_frame["benefit_code"].nunique()),
            "unique_benefit_names": int(detail_frame["benefit_name"].nunique()),
            "unique_detail_names": len(unique_detail_names),
            "service_mapped_rows": service_mapped_rows,
            "service_mapped_pct": round(100.0 * service_mapped_rows / len(detail_rows), 2) if detail_rows else 0.0,
            "observation_mapped_rows": observation_mapped_rows,
            "observation_mapped_pct": round(100.0 * observation_mapped_rows / len(detail_rows), 2) if detail_rows else 0.0,
            "clinical_linked_rows": clinical_linked_rows,
            "clinical_linked_pct": round(100.0 * clinical_linked_rows / len(detail_rows), 2) if detail_rows else 0.0,
            "benefit_per_claim_rows": int(len(benefit_per_claim)),
        },
        "status_distribution": [{"status": name, "rows": int(count)} for name, count in detail_frame["status"].fillna("(trong)").astype(str).value_counts(dropna=False).items()],
        "top_benefits_by_rows": sorted(benefit_profiles_list, key=lambda item: item["rows"], reverse=True)[:15],
        "top_unmapped_details": [{"detail_name": name, "rows": count} for name, count in unmatched_details.most_common(20)],
    }
    return detail_rows, summary


def build_alignment_profiles(contract_catalog: dict[str, Any], interpretation_catalog: dict[str, Any], detail_summary: dict[str, Any]) -> list[dict[str, Any]]:
    entry_index = {entry["entry_id"]: entry for entry in interpretation_catalog["entries"]}
    profiles: dict[str, dict[str, Any]] = {}

    for contract in contract_catalog["contracts"]:
        for entry in contract["benefit_entries"]:
            match = entry["interpretation_match"]
            key = match["entry_id"] or f"RAW::{normalize_key(entry['entry_label'])}"
            profile = profiles.setdefault(
                key,
                {
                    "profile_key": key,
                    "canonical_benefit_name": match["canonical_name"] or entry["entry_label"],
                    "group_name": match["group_name"],
                    "interpretation_entry": entry_index.get(match["entry_id"]),
                    "contract_hits": [],
                    "allocation_hits": [],
                },
            )
            profile["contract_hits"].append(
                {
                    "contract_id": contract["contract_id"],
                    "entry_label": entry["entry_label"],
                    "sheet_name": entry["sheet_name"],
                    "coverage_by_plan": entry.get("coverage_by_plan", {}),
                    "coverage_value": entry.get("coverage_value", ""),
                    "match_score": match.get("score", 0.0),
                }
            )

    for benefit in detail_summary["top_benefits_by_rows"]:
        match = benefit["interpretation_match"]
        key = match["entry_id"] or f"RAW::{normalize_key(benefit['benefit_name'])}"
        profile = profiles.setdefault(
            key,
            {
                "profile_key": key,
                "canonical_benefit_name": match["canonical_name"] or benefit["benefit_name"],
                "group_name": match["group_name"],
                "interpretation_entry": entry_index.get(match["entry_id"]),
                "contract_hits": [],
                "allocation_hits": [],
            },
        )
        profile["allocation_hits"].append(benefit)

    result = list(profiles.values())
    result.sort(key=lambda item: (sum(hit.get("rows", 0) for hit in item["allocation_hits"]), len(item["contract_hits"])), reverse=True)
    return result


def render_markdown(pack: dict[str, Any]) -> str:
    contracts = pack["contract_catalog"]["stats"]
    interpretations = pack["interpretation_catalog"]["stats"]
    details = pack["detail_links"]["stats"]
    lines = [
        "# Benefit Contract Knowledge Pack",
        "",
        "## Assets",
        f"- Contract workbooks: {contracts['contracts']}",
        f"- Contract benefit rows: {contracts['benefit_rows']}",
        f"- Contract clause rows: {contracts['clause_rows']}",
        f"- Interpretation entries: {interpretations['entries']}",
        f"- Allocation detail rows: {details['rows']}",
        "",
        "## Allocation Linking",
        f"- Service mapped: {details['service_mapped_rows']} ({details['service_mapped_pct']}%)",
        f"- Observation mapped: {details['observation_mapped_rows']} ({details['observation_mapped_pct']}%)",
        f"- Clinical linked: {details['clinical_linked_rows']} ({details['clinical_linked_pct']}%)",
        "",
        "## Top Benefit Profiles",
    ]
    for profile in pack["alignment_profiles"][:10]:
        allocation_rows = sum(hit.get("rows", 0) for hit in profile["allocation_hits"])
        lines.append(f"- {profile['canonical_benefit_name']}: allocation_rows={allocation_rows}, contract_hits={len(profile['contract_hits'])}")
    lines.extend(["", "## Unmapped Detail Backlog"])
    for row in pack["detail_links"]["top_unmapped_details"][:10]:
        lines.append(f"- {row['detail_name']}: {row['rows']}")
    return "\n".join(lines) + "\n"


def build_pack() -> dict[str, Any]:
    interpretation_catalog = build_interpretation_catalog()
    alias_items = interpretation_catalog.pop("_alias_items")
    contract_catalog = build_contract_catalog(alias_items)
    detail_rows, detail_summary = build_detail_links(alias_items)
    alignment_profiles = build_alignment_profiles(contract_catalog, interpretation_catalog, detail_summary)

    CONTRACT_CATALOG_PATH.write_text(json.dumps(contract_catalog, ensure_ascii=False, indent=2), encoding="utf-8")
    INTERPRETATION_CATALOG_PATH.write_text(json.dumps(interpretation_catalog, ensure_ascii=False, indent=2), encoding="utf-8")
    DETAIL_LINKS_JSONL_PATH.write_text("\n".join(json.dumps(row, ensure_ascii=False) for row in detail_rows) + "\n", encoding="utf-8")

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "sources": {
            "contract_files": [spec["filename"] for spec in CONTRACT_SPECS],
            "interpretation_file": "Diễn giải chi tiết về các quyền lợi bảo hiểm 7.10.25.xlsx",
            "allocation_file": "BaoCaoPhanBoChiPhi 14.10.25.xlsx",
        },
        "outputs": {
            "contract_catalog": str(CONTRACT_CATALOG_PATH.relative_to(PROJECT_DIR)),
            "interpretation_catalog": str(INTERPRETATION_CATALOG_PATH.relative_to(PROJECT_DIR)),
            "detail_links_jsonl": str(DETAIL_LINKS_JSONL_PATH.relative_to(PROJECT_DIR)),
        },
        "contract_catalog": contract_catalog,
        "interpretation_catalog": interpretation_catalog,
        "detail_links": detail_summary,
        "alignment_profiles": alignment_profiles,
    }


def main() -> None:
    pack = build_pack()
    PACK_JSON_PATH.write_text(json.dumps(pack, ensure_ascii=False, indent=2), encoding="utf-8")
    PACK_MD_PATH.write_text(render_markdown(pack), encoding="utf-8")

    print(f"Contract catalog saved: {CONTRACT_CATALOG_PATH}")
    print(f"Interpretation catalog saved: {INTERPRETATION_CATALOG_PATH}")
    print(f"Detail links saved: {DETAIL_LINKS_JSONL_PATH}")
    print(f"Knowledge pack saved: {PACK_JSON_PATH}")
    print(f"Knowledge report saved: {PACK_MD_PATH}")
    print(f"Contract benefit rows: {pack['contract_catalog']['stats']['benefit_rows']}")
    print(f"Contract clause rows: {pack['contract_catalog']['stats']['clause_rows']}")
    print(f"Detail service mapped pct: {pack['detail_links']['stats']['service_mapped_pct']}")


if __name__ == "__main__":
    main()
