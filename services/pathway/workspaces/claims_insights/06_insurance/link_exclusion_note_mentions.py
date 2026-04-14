from __future__ import annotations

import json
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


PROJECT_DIR = Path(__file__).parent.parent
STANDARDIZE_DIR = PROJECT_DIR / "02_standardize"
ENRICH_DIR = PROJECT_DIR / "03_enrich"
OBSERVATION_DIR = PROJECT_DIR / "05_observations"
INSURANCE_DIR = PROJECT_DIR / "06_insurance"

INPUT_SIGNALS_PATH = INSURANCE_DIR / "exclusion_claim_signals.jsonl"
CODEBOOK_PATH = STANDARDIZE_DIR / "service_codebook.json"
MATRIX_PATH = ENRICH_DIR / "service_disease_matrix.json"
CONCEPT_CATALOG_PATH = OBSERVATION_DIR / "observation_concept_catalog_seed.json"

OUTPUT_JSONL_PATH = INSURANCE_DIR / "exclusion_note_mentions_linked.jsonl"
OUTPUT_SUMMARY_PATH = INSURANCE_DIR / "exclusion_note_mentions_summary.json"
OUTPUT_CATALOG_PATH = INSURANCE_DIR / "contract_clause_service_catalog.json"

DEFAULT_SERVICE_THRESHOLD = 78.0
TOP_LINKS_PER_SERVICE = 5


def load_jsonl(path: Path) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    with path.open(encoding="utf-8") as handle:
        for line in handle:
            raw = line.strip()
            if raw:
                records.append(json.loads(raw))
    return records


def load_service_mapper() -> tuple[Any, dict[str, dict[str, Any]]]:
    mapper_path = str(STANDARDIZE_DIR)
    if mapper_path not in sys.path:
        sys.path.insert(0, mapper_path)
    from service_text_mapper import ServiceTextMapper  # pylint: disable=import-error

    payload = json.loads(CODEBOOK_PATH.read_text(encoding="utf-8"))
    codebook_by_code = {
        entry["service_code"]: entry
        for entry in payload.get("codebook", [])
        if entry.get("service_code")
    }
    return ServiceTextMapper(codebook_path=CODEBOOK_PATH), codebook_by_code


def load_observation_helpers() -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]], Any]:
    obs_path = str(OBSERVATION_DIR)
    if obs_path not in sys.path:
        sys.path.insert(0, obs_path)
    from extract_lab_observations import (  # pylint: disable=import-error
        load_observation_concept_catalog,
        observation_concept_candidates,
    )

    alias_map, concept_by_code = load_observation_concept_catalog(CONCEPT_CATALOG_PATH)
    return alias_map, concept_by_code, observation_concept_candidates


def build_service_links_index() -> dict[str, list[dict[str, Any]]]:
    payload = json.loads(MATRIX_PATH.read_text(encoding="utf-8"))
    links_by_service: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for link in payload.get("links", []):
        service_code = link.get("service_code", "")
        if service_code:
            links_by_service[service_code].append(link)
    for service_code, links in links_by_service.items():
        links.sort(
            key=lambda item: (
                float(item.get("score", 0.0) or 0.0),
                int((item.get("support") or {}).get("co_occurrence") or 0),
            ),
            reverse=True,
        )
        links_by_service[service_code] = links
    return links_by_service


def build_service_mapping(
    mention_names: set[str],
    mapper: Any,
    codebook_by_code: dict[str, dict[str, Any]],
    threshold: float,
) -> dict[str, dict[str, Any]]:
    mapping: dict[str, dict[str, Any]] = {}
    for mention_name in sorted(mention_names):
        result = mapper.score_text(mention_name, top_k=3)
        suggestions = result.get("suggestions") or []
        top = suggestions[0] if suggestions else {}
        score = float(top.get("score", 0.0) or 0.0)
        service_code = top.get("service_code", "") if score >= threshold else ""
        codebook_entry = codebook_by_code.get(service_code, {})
        mapping[mention_name] = {
            "service_code": service_code,
            "canonical_name": codebook_entry.get("canonical_name", ""),
            "category_code": codebook_entry.get("category_code", "UNKNOWN") if service_code else "UNKNOWN",
            "category_name": codebook_entry.get("category_name", "") if service_code else "",
            "mapper_score": round(score, 2),
            "mapper_confidence": top.get("confidence", "REVIEW"),
            "matched_variant": top.get("matched_variant", ""),
            "mapping_status": "mapped" if service_code else "unmapped",
            "alternatives": suggestions[1:3],
        }
    return mapping


def build_observation_mapping(
    mention_names: set[str],
    alias_map: dict[str, dict[str, Any]],
    candidate_builder: Any,
) -> dict[str, dict[str, Any]]:
    mapping: dict[str, dict[str, Any]] = {}
    for mention_name in sorted(mention_names):
        concept = None
        matched_candidate = ""
        for candidate in candidate_builder(mention_name):
            concept = alias_map.get(candidate)
            if concept:
                matched_candidate = candidate
                break
        mapping[mention_name] = {
            "concept_code": (concept or {}).get("concept_code", ""),
            "canonical_name": (concept or {}).get("canonical_name", ""),
            "category_code": (concept or {}).get("category_code", "UNKNOWN") if concept else "UNKNOWN",
            "category_name": (concept or {}).get("category_name", "") if concept else "",
            "result_semantics": (concept or {}).get("result_semantics", "") if concept else "",
            "matched_candidate": matched_candidate,
            "mapping_status": "mapped" if concept else "unmapped",
        }
    return mapping


def summarize_clinical_links(links: list[dict[str, Any]]) -> dict[str, Any]:
    if not links:
        return {
            "linked_disease_count": 0,
            "top_disease_links": [],
        }
    top_links = []
    for link in links[:TOP_LINKS_PER_SERVICE]:
        top_links.append(
            {
                "icd10": link.get("icd10", ""),
                "icd10_group": link.get("icd10_group", ""),
                "disease_name": link.get("disease_name", ""),
                "role": link.get("role", ""),
                "evidence": link.get("evidence", ""),
                "score": link.get("score", 0.0),
            }
        )
    return {
        "linked_disease_count": len(links),
        "top_disease_links": top_links,
    }


def build_linked_mentions() -> list[dict[str, Any]]:
    source_records = load_jsonl(INPUT_SIGNALS_PATH)
    mention_names = {
        mention.get("item_name", "").strip()
        for record in source_records
        for mention in (record.get("note_signals") or {}).get("service_mentions", [])
        if mention.get("item_name")
    }

    mapper, codebook_by_code = load_service_mapper()
    alias_map, _concept_by_code, candidate_builder = load_observation_helpers()
    links_by_service = build_service_links_index()
    service_mapping = build_service_mapping(mention_names, mapper, codebook_by_code, DEFAULT_SERVICE_THRESHOLD)
    observation_mapping = build_observation_mapping(mention_names, alias_map, candidate_builder)

    linked_mentions: list[dict[str, Any]] = []
    for record in source_records:
        note_signals = record.get("note_signals") or {}
        mention_list = note_signals.get("service_mentions") or []
        atomic_reasons = record.get("atomic_reasons") or []
        atomic_reason_texts = [entry.get("reason_text", "") for entry in atomic_reasons if entry.get("reason_text")]
        atomic_reason_groups = [entry.get("dictionary_group", "") for entry in atomic_reasons if entry.get("dictionary_group")]
        for idx, mention in enumerate(mention_list, start=1):
            mention_name = str(mention.get("item_name") or "").strip()
            if not mention_name:
                continue
            service_info = service_mapping.get(mention_name, {})
            observation_info = observation_mapping.get(mention_name, {})
            service_code = service_info.get("service_code", "")
            clinical_links = summarize_clinical_links(links_by_service.get(service_code, []))
            linked_mentions.append(
                {
                    "mention_id": f"{record.get('claim_line_id', '')}:M{idx}",
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
                    "clinical_links": clinical_links,
                }
            )
    return linked_mentions


def build_summary(linked_mentions: list[dict[str, Any]]) -> tuple[dict[str, Any], dict[str, Any]]:
    mention_name_counts = Counter()
    service_counts = Counter()
    service_name_lookup: dict[str, str] = {}
    category_counts = Counter()
    concept_counts = Counter()
    concept_name_lookup: dict[str, str] = {}
    contract_counts = Counter()
    reason_counts = Counter()
    reason_group_counts = Counter()
    disease_counts = Counter()
    disease_name_lookup: dict[str, str] = {}
    unmapped_mentions = Counter()
    contract_reason_profiles: dict[tuple[str, str], dict[str, Any]] = {}

    service_mapped_rows = 0
    observation_mapped_rows = 0
    clinical_linked_rows = 0

    for record in linked_mentions:
        mention_name = record["mention_name"]
        mention_name_counts[mention_name] += 1
        contract_counts[record["contract_name"] or "(trống)"] += 1
        for reason in record["atomic_reasons"]:
            reason_counts[reason] += 1
        for group in record["atomic_reason_groups"]:
            reason_group_counts[group] += 1

        service_info = record["service_mapping"]
        observation_info = record["observation_mapping"]
        clinical_links = record["clinical_links"]

        service_code = service_info.get("service_code", "")
        if service_code:
            service_mapped_rows += 1
            service_counts[service_code] += 1
            service_name_lookup[service_code] = service_info.get("canonical_name", "")
            category_counts[service_info.get("category_code", "UNKNOWN")] += 1
        else:
            unmapped_mentions[mention_name] += 1

        concept_code = observation_info.get("concept_code", "")
        if concept_code:
            observation_mapped_rows += 1
            concept_counts[concept_code] += 1
            concept_name_lookup[concept_code] = observation_info.get("canonical_name", "")

        if clinical_links.get("linked_disease_count", 0) > 0:
            clinical_linked_rows += 1
            for link in clinical_links.get("top_disease_links", []):
                key = link.get("icd10", "") or link.get("icd10_group", "")
                if key:
                    disease_counts[key] += 1
                    disease_name_lookup[key] = link.get("disease_name", "")

        profile_reasons = record["atomic_reasons"] or ["(không có lý do atomic)"]
        for atomic_reason in profile_reasons:
            profile_key = (record["contract_name"] or "(trống)", atomic_reason)
            profile = contract_reason_profiles.setdefault(
                profile_key,
                {
                    "contract_name": record["contract_name"] or "(trống)",
                    "insurer": record["insurer"],
                    "atomic_reason": atomic_reason,
                    "rows": 0,
                    "mapped_service_rows": 0,
                    "top_services": Counter(),
                    "service_names": {},
                    "top_unmapped_mentions": Counter(),
                    "top_observation_concepts": Counter(),
                    "concept_names": {},
                },
            )
            profile["rows"] += 1
            if service_code:
                profile["mapped_service_rows"] += 1
                profile["top_services"][service_code] += 1
                profile["service_names"][service_code] = service_info.get("canonical_name", "")
            else:
                profile["top_unmapped_mentions"][mention_name] += 1
            if concept_code:
                profile["top_observation_concepts"][concept_code] += 1
                profile["concept_names"][concept_code] = observation_info.get("canonical_name", "")

    total_mentions = len(linked_mentions)
    summary = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "sources": {
            "input_mentions": str(INPUT_SIGNALS_PATH.relative_to(PROJECT_DIR)),
            "service_codebook": str(CODEBOOK_PATH.relative_to(PROJECT_DIR)),
            "service_disease_matrix": str(MATRIX_PATH.relative_to(PROJECT_DIR)),
            "observation_concepts": str(CONCEPT_CATALOG_PATH.relative_to(PROJECT_DIR)),
        },
        "stats": {
            "mention_rows": total_mentions,
            "unique_mention_names": len(mention_name_counts),
            "unique_claim_lines": len({record["claim_line_id"] for record in linked_mentions}),
            "service_mapped_rows": service_mapped_rows,
            "service_mapped_pct": round(100.0 * service_mapped_rows / total_mentions, 2) if total_mentions else 0.0,
            "observation_mapped_rows": observation_mapped_rows,
            "observation_mapped_pct": round(100.0 * observation_mapped_rows / total_mentions, 2) if total_mentions else 0.0,
            "clinical_linked_rows": clinical_linked_rows,
            "clinical_linked_pct": round(100.0 * clinical_linked_rows / total_mentions, 2) if total_mentions else 0.0,
            "unique_mapped_services": len(service_counts),
            "unique_mapped_observation_concepts": len(concept_counts),
        },
        "top_mention_names": [
            {"mention_name": name, "rows": count}
            for name, count in mention_name_counts.most_common(20)
        ],
        "top_mapped_services": [
            {
                "service_code": code,
                "canonical_name": service_name_lookup.get(code, ""),
                "rows": count,
            }
            for code, count in service_counts.most_common(20)
        ],
        "top_service_categories": [
            {"category_code": category_code, "rows": count}
            for category_code, count in category_counts.most_common(20)
        ],
        "top_observation_concepts": [
            {
                "concept_code": code,
                "canonical_name": concept_name_lookup.get(code, ""),
                "rows": count,
            }
            for code, count in concept_counts.most_common(20)
        ],
        "top_contracts": [
            {"contract_name": name, "rows": count}
            for name, count in contract_counts.most_common(20)
        ],
        "top_atomic_reasons": [
            {"atomic_reason": reason, "rows": count}
            for reason, count in reason_counts.most_common(20)
        ],
        "top_reason_groups": [
            {"group": group, "rows": count}
            for group, count in reason_group_counts.most_common(20)
        ],
        "top_clinically_linked_diseases": [
            {
                "icd10_or_group": code,
                "disease_name": disease_name_lookup.get(code, ""),
                "rows": count,
            }
            for code, count in disease_counts.most_common(20)
        ],
        "top_unmapped_mentions": [
            {"mention_name": name, "rows": count}
            for name, count in unmapped_mentions.most_common(20)
        ],
    }

    catalog = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "sources": summary["sources"],
        "contracts": [],
    }
    grouped_by_contract: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for profile in contract_reason_profiles.values():
        grouped_by_contract[profile["contract_name"]].append(
            {
                "atomic_reason": profile["atomic_reason"],
                "rows": profile["rows"],
                "mapped_service_rows": profile["mapped_service_rows"],
                "top_services": [
                    {
                        "service_code": code,
                        "canonical_name": profile["service_names"].get(code, ""),
                        "rows": count,
                    }
                    for code, count in profile["top_services"].most_common(10)
                ],
                "top_observation_concepts": [
                    {
                        "concept_code": code,
                        "canonical_name": profile["concept_names"].get(code, ""),
                        "rows": count,
                    }
                    for code, count in profile["top_observation_concepts"].most_common(10)
                ],
                "top_unmapped_mentions": [
                    {"mention_name": name, "rows": count}
                    for name, count in profile["top_unmapped_mentions"].most_common(10)
                ],
            }
        )
    for contract_name, profiles in grouped_by_contract.items():
        insurer = ""
        for profile in profiles:
            if profile["top_services"]:
                break
        for record in linked_mentions:
            if record["contract_name"] == contract_name:
                insurer = record["insurer"]
                break
        catalog["contracts"].append(
            {
                "contract_name": contract_name,
                "insurer": insurer,
                "mention_rows": sum(profile["rows"] for profile in profiles),
                "reason_profiles": sorted(profiles, key=lambda item: item["rows"], reverse=True),
            }
        )
    catalog["contracts"].sort(key=lambda item: item["mention_rows"], reverse=True)
    return summary, catalog


def main() -> None:
    if not INPUT_SIGNALS_PATH.exists():
        raise FileNotFoundError(f"Missing input: {INPUT_SIGNALS_PATH}")

    linked_mentions = build_linked_mentions()
    OUTPUT_JSONL_PATH.write_text(
        "\n".join(json.dumps(record, ensure_ascii=False) for record in linked_mentions) + "\n",
        encoding="utf-8",
    )
    summary, catalog = build_summary(linked_mentions)
    OUTPUT_SUMMARY_PATH.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    OUTPUT_CATALOG_PATH.write_text(json.dumps(catalog, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"Linked mention dataset saved: {OUTPUT_JSONL_PATH}")
    print(f"Summary saved: {OUTPUT_SUMMARY_PATH}")
    print(f"Catalog saved: {OUTPUT_CATALOG_PATH}")
    print(f"Mention rows: {summary['stats']['mention_rows']}")
    print(f"Service mapped pct: {summary['stats']['service_mapped_pct']}")
    print(f"Clinical linked pct: {summary['stats']['clinical_linked_pct']}")


if __name__ == "__main__":
    main()
