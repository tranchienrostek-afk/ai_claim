from __future__ import annotations

import json
import re
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import fitz

from extract_exclusion_claim_signals import clean_display_text, normalize_for_match


PROJECT_DIR = Path(__file__).parent.parent
INSURANCE_DIR = PROJECT_DIR / "06_insurance"

MAIN_SIGNALS_PATH = INSURANCE_DIR / "exclusion_claim_signals.jsonl"
OUTPATIENT_SIGNALS_PATH = INSURANCE_DIR / "outpatient_exclusion_signals.jsonl"

RULEBOOK_TEXT_DIR = INSURANCE_DIR / "rulebook_texts"
RULEBOOK_CATALOG_PATH = INSURANCE_DIR / "rulebook_policy_catalog.json"
RULEBOOK_PACK_PATH = INSURANCE_DIR / "rulebook_policy_pack.json"
RULEBOOK_REPORT_PATH = INSURANCE_DIR / "rulebook_policy_pack.md"

RULEBOOK_SPECS = [
    {
        "rulebook_id": "BHV-151B",
        "insurer": "BHV",
        "rule_code": "151B",
        "display_name": "BHV - Quy tac SKN 151B",
        "filename": "QUY TAC SKN- 151B.pdf",
        "aliases": ["151b", "qt 151b", "skn 151b", "bhv 151b"],
    },
    {
        "rulebook_id": "PJICO-384",
        "insurer": "PJICO",
        "rule_code": "384",
        "display_name": "PJICO - QT 384 - BH suc khoe",
        "filename": "QT 384 - Bao hiem suc khoe.pdf",
        "aliases": ["384", "qt 384", "pjico 384"],
    },
    {
        "rulebook_id": "PJICO-710",
        "insurer": "PJICO",
        "rule_code": "710",
        "display_name": "PJICO - QT 710 - BH tai nan",
        "filename": "QT 710 - BH TAI NAN.pdf",
        "aliases": ["710", "qt 710", "pjico 710"],
    },
    {
        "rulebook_id": "PJICO-711",
        "insurer": "PJICO",
        "rule_code": "711",
        "display_name": "PJICO - QT 711 - BH suc khoe",
        "filename": "QT 711 - BH SUC KHOE.pdf",
        "aliases": ["711", "qt 711", "pjico 711"],
    },
    {
        "rulebook_id": "KYNGUYEN-380",
        "insurer": "Ky Nguyen",
        "rule_code": "380",
        "display_name": "Ky Nguyen - Wording CPH250080HBD",
        "filename": "380. Kỷ Nguyên_CPH250080HBD wording.pdf",
        "aliases": ["380", "qt 380", "ky nguyen 380", "cph250080hbd"],
    },
    {
        "rulebook_id": "TCGINS-106",
        "insurer": "TCGIns",
        "rule_code": "106",
        "display_name": "TCGIns - QD 106 - BH suc khoe ca nhan",
        "filename": "106 QĐ TCGIns ban hanh quy tac Bao hiem Suc khoe ca nhan (1) (1).pdf",
        "aliases": ["106", "quy tac 106", "tcgins 106"],
    },
]

CHAPTER_RE = re.compile(r"^CHƯƠNG\s+[IVXLC]+\s*[-–—]?\s*(.*)$", re.IGNORECASE)
CLAUSE_RE = re.compile(r"^(\d+)\.\s+(.*)$")


def load_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    with path.open(encoding="utf-8") as handle:
        for line in handle:
            raw = line.strip()
            if raw:
                rows.append(json.loads(raw))
    return rows


def find_rulebook_file(filename: str) -> Path:
    matches = list(INSURANCE_DIR.rglob(filename))
    if not matches:
        raise FileNotFoundError(f"Missing rulebook: {filename}")
    preferred = [path for path in matches if "Quy tắc + hợp đồng" in str(path)]
    return sorted(preferred or matches)[0]


def safe_slug(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", normalize_for_match(value)).strip("_")


def extract_text_asset(path: Path) -> dict[str, Any]:
    doc = fitz.open(path)
    page_items: list[dict[str, Any]] = []
    text_pages: list[dict[str, Any]] = []
    for page_number, page in enumerate(doc, start=1):
        text = page.get_text().strip()
        page_item = {
            "page_number": page_number,
            "char_count": len(text),
            "snippet": text[:240].replace("\n", " "),
        }
        page_items.append(page_item)
        if text:
            text_pages.append({"page_number": page_number, "text": text})
    return {
        "page_count": len(doc),
        "text_extractable_pages": len(text_pages),
        "total_extracted_chars": sum(item["char_count"] for item in page_items),
        "ocr_status": "ocr_required" if not text_pages else "text_layer_present",
        "pages": page_items,
        "text_pages": text_pages,
    }


def parse_text_structure(text_pages: list[dict[str, Any]]) -> dict[str, Any]:
    if not text_pages:
        return {
            "chapters": [],
            "chapter_count": 0,
            "clause_count": 0,
        }

    chapters: list[dict[str, Any]] = []
    current_chapter: dict[str, Any] | None = None
    current_clause: dict[str, Any] | None = None

    for page in text_pages:
        for raw_line in page["text"].splitlines():
            line = clean_display_text(raw_line)
            if not line:
                continue

            chapter_match = CHAPTER_RE.match(line)
            if chapter_match:
                if current_clause and current_chapter is not None:
                    current_chapter["clauses"].append(current_clause)
                    current_clause = None
                current_chapter = {
                    "chapter_title": line,
                    "chapter_label": chapter_match.group(0),
                    "clauses": [],
                }
                chapters.append(current_chapter)
                continue

            clause_match = CLAUSE_RE.match(line)
            if clause_match and current_chapter is not None:
                if current_clause:
                    current_chapter["clauses"].append(current_clause)
                current_clause = {
                    "clause_number": clause_match.group(1),
                    "clause_title": clause_match.group(2),
                    "body_lines": [],
                }
                continue

            if current_clause is not None:
                current_clause["body_lines"].append(line)

    if current_clause and current_chapter is not None:
        current_chapter["clauses"].append(current_clause)

    for chapter in chapters:
        for clause in chapter["clauses"]:
            clause["clause_body"] = "\n".join(clause.pop("body_lines", []))

    return {
        "chapters": chapters,
        "chapter_count": len(chapters),
        "clause_count": sum(len(chapter["clauses"]) for chapter in chapters),
    }


def match_rulebook_id(rule_name: str) -> str | None:
    norm = normalize_for_match(rule_name)
    if not norm:
        return None
    for spec in RULEBOOK_SPECS:
        if any(alias in norm for alias in spec["aliases"]):
            return spec["rulebook_id"]
    return None


def build_claim_evidence() -> dict[str, dict[str, Any]]:
    evidence: dict[str, dict[str, Any]] = {}
    records = [*load_jsonl(MAIN_SIGNALS_PATH), *load_jsonl(OUTPATIENT_SIGNALS_PATH)]

    for record in records:
        rulebook_id = match_rulebook_id(record.get("rule_name", ""))
        if not rulebook_id:
            continue
        entry = evidence.setdefault(
            rulebook_id,
            {
                "rows": 0,
                "datasets": Counter(),
                "contracts": Counter(),
                "benefits": Counter(),
                "atomic_reasons": Counter(),
                "clause_references": Counter(),
                "gap_sum_vnd": 0,
                "sample_notes": [],
            },
        )
        entry["rows"] += 1
        entry["datasets"][record.get("source_dataset", "main_exclusion")] += 1
        entry["contracts"][record.get("contract_name", "(trong)")] += 1
        entry["benefits"][record.get("benefit_name", "(trong)")] += 1
        entry["gap_sum_vnd"] += int(record.get("gap_amount_vnd", 0) or 0)
        for atomic in record.get("atomic_reasons", []):
            reason_text = atomic.get("reason_text", "")
            if reason_text:
                entry["atomic_reasons"][reason_text] += 1
        for clause_ref in (record.get("note_signals") or {}).get("clause_references", []):
            entry["clause_references"][clause_ref] += 1
        note_text = record.get("note_text", "")
        if note_text and len(entry["sample_notes"]) < 3:
            entry["sample_notes"].append(note_text)

    return evidence


def build_rulebook_catalog() -> dict[str, Any]:
    RULEBOOK_TEXT_DIR.mkdir(parents=True, exist_ok=True)
    claim_evidence = build_claim_evidence()
    items: list[dict[str, Any]] = []

    for spec in RULEBOOK_SPECS:
        path = find_rulebook_file(spec["filename"])
        asset = extract_text_asset(path)
        text_pages = asset.pop("text_pages")
        structure = parse_text_structure(text_pages)
        text_output = ""
        if text_pages:
            text_output = str((RULEBOOK_TEXT_DIR / f"{safe_slug(spec['rulebook_id'])}.txt").relative_to(PROJECT_DIR))
            (PROJECT_DIR / text_output).write_text(
                "\n\n".join(
                    f"=== PAGE {page['page_number']} ===\n{page['text']}" for page in text_pages
                ),
                encoding="utf-8",
            )

        evidence = claim_evidence.get(spec["rulebook_id"], {})
        items.append(
            {
                "rulebook_id": spec["rulebook_id"],
                "insurer": spec["insurer"],
                "rule_code": spec["rule_code"],
                "display_name": spec["display_name"],
                "source_file": str(path.relative_to(PROJECT_DIR)),
                "text_output": text_output,
                "asset": asset,
                "structure": structure,
                "claim_evidence": {
                    "rows": evidence.get("rows", 0),
                    "dataset_distribution": dict(evidence.get("datasets", Counter())),
                    "gap_sum_vnd": evidence.get("gap_sum_vnd", 0),
                    "top_contracts": [
                        {"contract_name": name, "rows": count}
                        for name, count in evidence.get("contracts", Counter()).most_common(10)
                    ],
                    "top_benefits": [
                        {"benefit_name": name, "rows": count}
                        for name, count in evidence.get("benefits", Counter()).most_common(10)
                    ],
                    "top_atomic_reasons": [
                        {"atomic_reason": name, "rows": count}
                        for name, count in evidence.get("atomic_reasons", Counter()).most_common(10)
                    ],
                    "top_clause_references": [
                        {"clause_reference": name, "rows": count}
                        for name, count in evidence.get("clause_references", Counter()).most_common(10)
                    ],
                    "sample_notes": evidence.get("sample_notes", []),
                },
            }
        )

    stats = {
        "rulebooks": len(items),
        "text_extractable_rulebooks": sum(1 for item in items if item["asset"]["text_extractable_pages"] > 0),
        "ocr_required_rulebooks": sum(1 for item in items if item["asset"]["text_extractable_pages"] == 0),
        "total_pages": sum(item["asset"]["page_count"] for item in items),
        "total_claim_evidence_rows": sum(item["claim_evidence"]["rows"] for item in items),
    }
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "items": items,
        "stats": stats,
    }


def render_markdown(pack: dict[str, Any]) -> str:
    lines = [
        "# Rulebook Policy Pack",
        "",
        "## Stats",
        f"- Rulebooks: {pack['stats']['rulebooks']}",
        f"- Text extractable: {pack['stats']['text_extractable_rulebooks']}",
        f"- OCR required: {pack['stats']['ocr_required_rulebooks']}",
        f"- Total pages: {pack['stats']['total_pages']}",
        f"- Total claim evidence rows: {pack['stats']['total_claim_evidence_rows']}",
        "",
        "## Rulebooks",
    ]
    for item in pack["items"]:
        lines.append(
            f"- {item['rulebook_id']}: pages={item['asset']['page_count']}, "
            f"text_pages={item['asset']['text_extractable_pages']}, "
            f"claim_rows={item['claim_evidence']['rows']}, "
            f"ocr_status={item['asset']['ocr_status']}"
        )
    return "\n".join(lines) + "\n"


def main() -> None:
    pack = build_rulebook_catalog()
    RULEBOOK_CATALOG_PATH.write_text(json.dumps(pack["items"], ensure_ascii=False, indent=2), encoding="utf-8")
    RULEBOOK_PACK_PATH.write_text(json.dumps(pack, ensure_ascii=False, indent=2), encoding="utf-8")
    RULEBOOK_REPORT_PATH.write_text(render_markdown(pack), encoding="utf-8")

    print(f"Rulebook catalog saved: {RULEBOOK_CATALOG_PATH}")
    print(f"Rulebook pack saved: {RULEBOOK_PACK_PATH}")
    print(f"Rulebook report saved: {RULEBOOK_REPORT_PATH}")
    print(f"Text extractable rulebooks: {pack['stats']['text_extractable_rulebooks']}")
    print(f"OCR required rulebooks: {pack['stats']['ocr_required_rulebooks']}")


if __name__ == "__main__":
    main()
