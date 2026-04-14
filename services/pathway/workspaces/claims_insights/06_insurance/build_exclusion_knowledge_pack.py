from __future__ import annotations

import json
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

try:
    from pypdf import PdfReader
except Exception:  # pragma: no cover
    from PyPDF2 import PdfReader

from extract_exclusion_claim_signals import (
    build_line_record,
    clean_display_text,
    find_required_file,
    load_claim_frame,
    parse_reason_dictionary,
    summarize_records,
)
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

OUTPATIENT_SIGNALS_JSONL_PATH = INSURANCE_DIR / "outpatient_exclusion_signals.jsonl"
COMBINED_LINKED_JSONL_PATH = INSURANCE_DIR / "combined_exclusion_note_mentions_linked.jsonl"
PACK_JSON_PATH = INSURANCE_DIR / "exclusion_knowledge_pack.json"
PACK_MD_PATH = INSURANCE_DIR / "exclusion_knowledge_pack.md"


def clean_cell(value: object) -> str:
    text = clean_display_text(value)
    return "" if text.lower() == "nan" else text


def canonical_clause_key(text: str) -> str:
    value = clean_display_text(text)
    value = value.replace(" - ", "-").replace(" :", ":")
    value = " ".join(value.split())
    return value.lower()


def load_outpatient_frame(path: Path) -> pd.DataFrame:
    xls = pd.ExcelFile(path)
    sheet_name = xls.sheet_names[0]
    df = pd.read_excel(path, sheet_name=sheet_name)
    return pd.DataFrame(
        {
            "policy_no": df.iloc[:, 0].fillna("").astype(str),
            "contract": df.iloc[:, 1].fillna("").astype(str),
            "rule": df.iloc[:, 2].fillna("").astype(str),
            "claim_id": df.iloc[:, 3].fillna("").astype(str),
            "admission_date": df.iloc[:, 6],
            "discharge_date": df.iloc[:, 7],
            "diagnosis_text": df.iloc[:, 8].fillna("").astype(str),
            "facility_name": df.iloc[:, 9].fillna("").astype(str),
            "care_type": df.iloc[:, 10].fillna("").astype(str),
            "benefit_name": df.iloc[:, 11].fillna("").astype(str),
            "requested_amount_vnd": pd.to_numeric(df.iloc[:, 12], errors="coerce").fillna(0).astype(int),
            "paid_amount_vnd": pd.to_numeric(df.iloc[:, 13], errors="coerce").fillna(0).astype(int),
            "reason_raw": df.iloc[:, 14].fillna("").astype(str),
            "note_text": df.iloc[:, 15].fillna("").astype(str),
            "insured_name": df.iloc[:, 16].fillna("").astype(str),
            "approved_entitlement_hint": df.iloc[:, 17].fillna("").astype(str),
            "icd_text": df.iloc[:, 18].fillna("").astype(str),
            "status": df.iloc[:, 19].fillna("").astype(str),
        }
    )


def build_records(
    dataset_name: str,
    frame: pd.DataFrame,
    reason_index: dict[str, dict[str, Any]],
    ranked_reason_items: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for row_number, (_, row) in enumerate(frame.iterrows(), start=1):
        record = build_line_record(row_number, row, reason_index, ranked_reason_items)
        record["source_dataset"] = dataset_name
        records.append(record)
    return records


def save_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.write_text(
        "\n".join(json.dumps(row, ensure_ascii=False) for row in rows) + "\n",
        encoding="utf-8",
    )


def extract_pdf_asset(path: Path) -> dict[str, Any]:
    reader = PdfReader(str(path))
    page_char_counts: list[int] = []
    for page in reader.pages:
        text = (page.extract_text() or "").strip()
        page_char_counts.append(len(text))
    text_extractable_pages = sum(1 for count in page_char_counts if count > 0)
    return {
        "path": str(path.relative_to(PROJECT_DIR)),
        "page_count": len(reader.pages),
        "text_extractable_pages": text_extractable_pages,
        "total_extracted_chars": sum(page_char_counts),
        "ocr_status": "ocr_required" if text_extractable_pages == 0 else "text_layer_present",
        "page_char_counts": page_char_counts,
    }


def extract_link_registry(path: Path) -> list[dict[str, Any]]:
    xls = pd.ExcelFile(path)
    if "link tài liệu" not in xls.sheet_names:
        return []
    df = pd.read_excel(path, sheet_name="link tài liệu", header=None).dropna(how="all")
    entries: list[dict[str, Any]] = []
    current_section = ""
    for _, row in df.iterrows():
        values = [clean_cell(value) for value in row.tolist()]
        if len(values) < 3:
            continue
        col0, col1, col2 = values[:3]
        if col0:
            current_section = col0
        label = col1 or col0
        url = col2
        if not url.startswith("http"):
            continue
        entries.append(
            {
                "section": current_section,
                "label": label,
                "url": url,
            }
        )
    return entries


def link_mentions(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    mention_names = {
        mention.get("item_name", "").strip()
        for record in records
        for mention in (record.get("note_signals") or {}).get("service_mentions", [])
        if mention.get("item_name")
    }
    mapper, codebook_by_code = load_service_mapper()
    alias_map, _concept_by_code, candidate_builder = load_observation_helpers()
    links_by_service = build_service_links_index()

    service_mapping = build_service_mapping(mention_names, mapper, codebook_by_code, 78.0)
    observation_mapping = build_observation_mapping(mention_names, alias_map, candidate_builder)

    linked_mentions: list[dict[str, Any]] = []
    for record in records:
        note_signals = record.get("note_signals") or {}
        mentions = note_signals.get("service_mentions") or []
        atomic_reasons = record.get("atomic_reasons") or []
        atomic_reason_texts = [item.get("reason_text", "") for item in atomic_reasons if item.get("reason_text")]
        atomic_reason_groups = [item.get("dictionary_group", "") for item in atomic_reasons if item.get("dictionary_group")]
        for idx, mention in enumerate(mentions, start=1):
            mention_name = str(mention.get("item_name") or "").strip()
            if not mention_name:
                continue
            service_info = service_mapping.get(mention_name, {})
            observation_info = observation_mapping.get(mention_name, {})
            service_code = service_info.get("service_code", "")
            linked_mentions.append(
                {
                    "mention_id": f"{record.get('source_dataset', '')}:{record.get('claim_line_id', '')}:M{idx}",
                    "source_dataset": record.get("source_dataset", ""),
                    "claim_line_id": record.get("claim_line_id", ""),
                    "claim_id": record.get("claim_id", ""),
                    "contract_name": record.get("contract_name", ""),
                    "insurer": record.get("insurer", ""),
                    "rule_name": record.get("rule_name", ""),
                    "benefit_name": record.get("benefit_name", ""),
                    "care_type": record.get("care_type", ""),
                    "reason_raw": record.get("reason_raw", ""),
                    "atomic_reasons": atomic_reason_texts,
                    "atomic_reason_groups": atomic_reason_groups,
                    "mention_name": mention_name,
                    "mention_amount_vnd": mention.get("amount_vnd"),
                    "service_mapping": service_info,
                    "observation_mapping": observation_info,
                    "clinical_links": summarize_clinical_links(links_by_service.get(service_code, [])),
                }
            )
    return linked_mentions


def summarize_linked_mentions(linked_mentions: list[dict[str, Any]]) -> dict[str, Any]:
    dataset_counts = Counter()
    mention_counts = Counter()
    service_counts = Counter()
    service_names: dict[str, str] = {}
    concept_counts = Counter()
    concept_names: dict[str, str] = {}
    disease_counts = Counter()
    disease_names: dict[str, str] = {}
    unmapped_mentions = Counter()

    service_mapped_rows = 0
    observation_mapped_rows = 0
    clinical_linked_rows = 0

    for row in linked_mentions:
        dataset_counts[row["source_dataset"]] += 1
        mention_counts[row["mention_name"]] += 1

        service_info = row["service_mapping"]
        service_code = service_info.get("service_code", "")
        if service_code:
            service_mapped_rows += 1
            service_counts[service_code] += 1
            service_names[service_code] = service_info.get("canonical_name", "")
        else:
            unmapped_mentions[row["mention_name"]] += 1

        observation_info = row["observation_mapping"]
        concept_code = observation_info.get("concept_code", "")
        if concept_code:
            observation_mapped_rows += 1
            concept_counts[concept_code] += 1
            concept_names[concept_code] = observation_info.get("canonical_name", "")

        links = row["clinical_links"].get("top_disease_links", [])
        if links:
            clinical_linked_rows += 1
            for link in links:
                key = link.get("icd10", "") or link.get("icd10_group", "")
                if key:
                    disease_counts[key] += 1
                    disease_names[key] = link.get("disease_name", "")

    total = len(linked_mentions)
    return {
        "mention_rows": total,
        "dataset_distribution": dict(dataset_counts),
        "service_mapped_rows": service_mapped_rows,
        "service_mapped_pct": round(100.0 * service_mapped_rows / total, 2) if total else 0.0,
        "observation_mapped_rows": observation_mapped_rows,
        "observation_mapped_pct": round(100.0 * observation_mapped_rows / total, 2) if total else 0.0,
        "clinical_linked_rows": clinical_linked_rows,
        "clinical_linked_pct": round(100.0 * clinical_linked_rows / total, 2) if total else 0.0,
        "top_mentions": [
            {"mention_name": name, "rows": count}
            for name, count in mention_counts.most_common(20)
        ],
        "top_services": [
            {"service_code": code, "canonical_name": service_names.get(code, ""), "rows": count}
            for code, count in service_counts.most_common(20)
        ],
        "top_observation_concepts": [
            {"concept_code": code, "canonical_name": concept_names.get(code, ""), "rows": count}
            for code, count in concept_counts.most_common(20)
        ],
        "top_diseases": [
            {"icd10_or_group": code, "disease_name": disease_names.get(code, ""), "rows": count}
            for code, count in disease_counts.most_common(20)
        ],
        "top_unmapped_mentions": [
            {"mention_name": name, "rows": count}
            for name, count in unmapped_mentions.most_common(20)
        ],
    }


def build_reason_usage(records: list[dict[str, Any]], ranked_reason_items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    index: dict[str, dict[str, Any]] = {
        item["code"]: {
            "code": item["code"],
            "reason": item["reason"],
            "group": item["group"],
            "process_path": item["process_path"],
            "source_note": item["source_note"],
            "main_rows": 0,
            "outpatient_rows": 0,
            "main_gap_vnd": 0,
            "outpatient_gap_vnd": 0,
            "contracts": Counter(),
            "rules": Counter(),
        }
        for item in ranked_reason_items
        if item.get("code")
    }
    for record in records:
        dataset_key = "main" if record["source_dataset"] == "main_exclusion" else "outpatient"
        for atomic in record.get("atomic_reasons", []):
            code = atomic.get("dictionary_code", "")
            if not code or code not in index:
                continue
            entry = index[code]
            entry[f"{dataset_key}_rows"] += 1
            entry[f"{dataset_key}_gap_vnd"] += record.get("gap_amount_vnd", 0)
            entry["contracts"][record.get("contract_name", "(trống)")] += 1
            entry["rules"][record.get("rule_name", "(trống)")] += 1
    rows = []
    for entry in index.values():
        rows.append(
            {
                "code": entry["code"],
                "reason": entry["reason"],
                "group": entry["group"],
                "process_path": entry["process_path"],
                "main_rows": entry["main_rows"],
                "outpatient_rows": entry["outpatient_rows"],
                "total_rows": entry["main_rows"] + entry["outpatient_rows"],
                "main_gap_vnd": entry["main_gap_vnd"],
                "outpatient_gap_vnd": entry["outpatient_gap_vnd"],
                "total_gap_vnd": entry["main_gap_vnd"] + entry["outpatient_gap_vnd"],
                "top_contracts": [
                    {"contract_name": name, "rows": count}
                    for name, count in entry["contracts"].most_common(5)
                ],
                "top_rules": [
                    {"rule_name": name, "rows": count}
                    for name, count in entry["rules"].most_common(5)
                ],
            }
        )
    rows.sort(key=lambda item: (item["total_rows"], item["total_gap_vnd"]), reverse=True)
    return rows


def build_clause_reference_index(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    index: dict[tuple[str, str], dict[str, Any]] = {}
    for record in records:
        for ref in (record.get("note_signals") or {}).get("clause_references", []):
            ref_key = canonical_clause_key(ref)
            key = (record.get("rule_name", "(trống)"), ref_key)
            entry = index.setdefault(
                key,
                {
                    "rule_name": record.get("rule_name", "(trống)"),
                    "clause_reference": clean_display_text(ref),
                    "datasets": Counter(),
                    "reasons": Counter(),
                    "benefits": Counter(),
                    "contracts": Counter(),
                    "service_mentions": Counter(),
                    "gap_sum_vnd": 0,
                    "sample_notes": [],
                },
            )
            entry["datasets"][record["source_dataset"]] += 1
            entry["benefits"][record.get("benefit_name", "(trống)")] += 1
            entry["contracts"][record.get("contract_name", "(trống)")] += 1
            entry["gap_sum_vnd"] += record.get("gap_amount_vnd", 0)
            if len(clean_display_text(ref)) > len(entry["clause_reference"]):
                entry["clause_reference"] = clean_display_text(ref)
            for atomic in record.get("atomic_reasons", []):
                if atomic.get("reason_text"):
                    entry["reasons"][atomic["reason_text"]] += 1
            for mention in (record.get("note_signals") or {}).get("service_mentions", []):
                if mention.get("item_name"):
                    entry["service_mentions"][mention["item_name"]] += 1
            if record.get("note_text") and len(entry["sample_notes"]) < 3:
                entry["sample_notes"].append(record["note_text"])
    rows = []
    for entry in index.values():
        rows.append(
            {
                "rule_name": entry["rule_name"],
                "clause_reference": entry["clause_reference"],
                "rows": sum(entry["datasets"].values()),
                "dataset_distribution": dict(entry["datasets"]),
                "gap_sum_vnd": entry["gap_sum_vnd"],
                "top_reasons": [
                    {"reason": name, "rows": count}
                    for name, count in entry["reasons"].most_common(10)
                ],
                "top_benefits": [
                    {"benefit": name, "rows": count}
                    for name, count in entry["benefits"].most_common(10)
                ],
                "top_contracts": [
                    {"contract_name": name, "rows": count}
                    for name, count in entry["contracts"].most_common(10)
                ],
                "top_service_mentions": [
                    {"mention_name": name, "rows": count}
                    for name, count in entry["service_mentions"].most_common(10)
                ],
                "sample_notes": entry["sample_notes"],
            }
        )
    rows.sort(key=lambda item: (item["rows"], item["gap_sum_vnd"]), reverse=True)
    return rows


def build_qt711_focus(records: list[dict[str, Any]], linked_mentions: list[dict[str, Any]]) -> dict[str, Any]:
    qt711_records = [
        record
        for record in records
        if "711" in record.get("rule_name", "")
        or any("711" in ref for ref in (record.get("note_signals") or {}).get("clause_references", []))
    ]
    reason_counts = Counter()
    benefit_counts = Counter()
    clause_counts = Counter()
    clause_labels: dict[str, str] = {}
    dataset_counts = Counter()
    for record in qt711_records:
        dataset_counts[record["source_dataset"]] += 1
        benefit_counts[record.get("benefit_name", "(trống)")] += 1
        for atomic in record.get("atomic_reasons", []):
            if atomic.get("reason_text"):
                reason_counts[atomic["reason_text"]] += 1
        for ref in (record.get("note_signals") or {}).get("clause_references", []):
            key = canonical_clause_key(ref)
            clause_counts[key] += 1
            label = clean_display_text(ref)
            if len(label) > len(clause_labels.get(key, "")):
                clause_labels[key] = label
    qt711_mentions = [
        row for row in linked_mentions if "711" in row.get("rule_name", "")
    ]
    service_counts = Counter()
    service_names: dict[str, str] = {}
    unmapped_counts = Counter()
    for row in qt711_mentions:
        service_code = (row.get("service_mapping") or {}).get("service_code", "")
        if service_code:
            service_counts[service_code] += 1
            service_names[service_code] = (row.get("service_mapping") or {}).get("canonical_name", "")
        else:
            unmapped_counts[row.get("mention_name", "")] += 1
    return {
        "rows": len(qt711_records),
        "dataset_distribution": dict(dataset_counts),
        "top_atomic_reasons": [
            {"reason": name, "rows": count}
            for name, count in reason_counts.most_common(15)
        ],
        "top_benefits": [
            {"benefit": name, "rows": count}
            for name, count in benefit_counts.most_common(15)
        ],
        "top_clause_references": [
            {"clause_reference": clause_labels.get(name, name), "rows": count}
            for name, count in clause_counts.most_common(15)
        ],
        "top_linked_services": [
            {"service_code": code, "canonical_name": service_names.get(code, ""), "rows": count}
            for code, count in service_counts.most_common(15)
        ],
        "top_unmapped_mentions": [
            {"mention_name": name, "rows": count}
            for name, count in unmapped_counts.most_common(15)
        ],
    }


def render_markdown(pack: dict[str, Any]) -> str:
    assets = pack["assets"]
    combined = pack["combined_mentions"]
    qt711 = pack["qt711_focus"]
    reason_usage = pack["reason_usage"][:10]
    clause_refs = pack["clause_reference_index"][:10]
    unresolved = combined["top_unmapped_mentions"][:10]

    lines = [
        "# Exclusion Knowledge Pack",
        "",
        "## Assets",
        f"- QT 711 PDF: {assets['qt711_pdf']['page_count']} pages, text-extractable pages = {assets['qt711_pdf']['text_extractable_pages']}, OCR status = {assets['qt711_pdf']['ocr_status']}",
        f"- Main exclusion rows: {pack['main_summary']['stats']['rows']}",
        f"- Outpatient exclusion rows: {pack['outpatient_summary']['stats']['rows']}",
        f"- Reason dictionary size: {pack['main_summary']['stats']['reason_dictionary_size']}",
        "",
        "## Combined Mention Linking",
        f"- Mention rows: {combined['mention_rows']}",
        f"- Service mapped: {combined['service_mapped_rows']} ({combined['service_mapped_pct']}%)",
        f"- Clinical linked: {combined['clinical_linked_rows']} ({combined['clinical_linked_pct']}%)",
        "",
        "## QT 711 Focus",
        f"- Rows touching QT 711: {qt711['rows']}",
        "- Top clause references:",
    ]
    for row in qt711["top_clause_references"][:8]:
        lines.append(f"  - {row['clause_reference']}: {row['rows']}")
    lines.extend(
        [
            "",
            "## Top Reason Usage",
        ]
    )
    for row in reason_usage:
        lines.append(
            f"- {row['code']} | {row['reason']}: total_rows={row['total_rows']}, total_gap_vnd={row['total_gap_vnd']}"
        )
    lines.extend(
        [
            "",
            "## Top Clause References",
        ]
    )
    for row in clause_refs:
        lines.append(f"- {row['rule_name']} | {row['clause_reference']}: rows={row['rows']}, gap_vnd={row['gap_sum_vnd']}")
    lines.extend(
        [
            "",
            "## Unmapped Mention Backlog",
        ]
    )
    for row in unresolved:
        lines.append(f"- {row['mention_name']}: {row['rows']}")
    return "\n".join(lines) + "\n"


def main() -> None:
    main_path = find_required_file("Danh sách hồ sơ loại trừ.xlsx")
    outpatient_path = find_required_file("DM Hồ sơ loại trừ 15.7.24- 30.9.25.xlsx")
    reason_path = find_required_file("Lý do loại trừ.xlsx")
    qt711_pdf_path = find_required_file("QT 711 - BH SUC KHOE.pdf")

    reason_index, ranked_reason_items = parse_reason_dictionary(reason_path)

    main_records = build_records("main_exclusion", load_claim_frame(main_path), reason_index, ranked_reason_items)
    outpatient_records = build_records("outpatient_exclusion", load_outpatient_frame(outpatient_path), reason_index, ranked_reason_items)
    save_jsonl(OUTPATIENT_SIGNALS_JSONL_PATH, outpatient_records)

    combined_records = [*main_records, *outpatient_records]
    linked_mentions = link_mentions(combined_records)
    save_jsonl(COMBINED_LINKED_JSONL_PATH, linked_mentions)

    main_summary, _ = summarize_records(main_records, ranked_reason_items, main_path, reason_path)
    outpatient_summary, _ = summarize_records(outpatient_records, ranked_reason_items, outpatient_path, reason_path)
    combined_mention_summary = summarize_linked_mentions(linked_mentions)
    clause_reference_index = build_clause_reference_index(combined_records)
    reason_usage = build_reason_usage(combined_records, ranked_reason_items)
    qt711_focus = build_qt711_focus(combined_records, linked_mentions)

    pack = {
        "version": "1.0",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "sources": {
            "main_claims": str(main_path.relative_to(PROJECT_DIR)),
            "outpatient_claims": str(outpatient_path.relative_to(PROJECT_DIR)),
            "reason_dictionary": str(reason_path.relative_to(PROJECT_DIR)),
            "qt711_pdf": str(qt711_pdf_path.relative_to(PROJECT_DIR)),
        },
        "assets": {
            "qt711_pdf": extract_pdf_asset(qt711_pdf_path),
            "link_registry": extract_link_registry(outpatient_path),
        },
        "main_summary": main_summary,
        "outpatient_summary": outpatient_summary,
        "combined_mentions": combined_mention_summary,
        "reason_usage": reason_usage,
        "clause_reference_index": clause_reference_index,
        "qt711_focus": qt711_focus,
        "outputs": {
            "outpatient_signals_jsonl": str(OUTPATIENT_SIGNALS_JSONL_PATH.relative_to(PROJECT_DIR)),
            "combined_linked_mentions_jsonl": str(COMBINED_LINKED_JSONL_PATH.relative_to(PROJECT_DIR)),
        },
    }

    PACK_JSON_PATH.write_text(json.dumps(pack, ensure_ascii=False, indent=2), encoding="utf-8")
    PACK_MD_PATH.write_text(render_markdown(pack), encoding="utf-8")

    print(f"Outpatient signals saved: {OUTPATIENT_SIGNALS_JSONL_PATH}")
    print(f"Combined linked mentions saved: {COMBINED_LINKED_JSONL_PATH}")
    print(f"Knowledge pack saved: {PACK_JSON_PATH}")
    print(f"Knowledge report saved: {PACK_MD_PATH}")
    print(f"Combined mention rows: {combined_mention_summary['mention_rows']}")
    print(f"QT711 rows: {qt711_focus['rows']}")


if __name__ == "__main__":
    main()
