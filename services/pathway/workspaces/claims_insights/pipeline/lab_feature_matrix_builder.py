from __future__ import annotations

import csv
import json
import math
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

try:
    from lab_result_interpreter import classify_lab_result_signal
except ModuleNotFoundError:  # pragma: no cover
    sys.path.append(str(Path(__file__).resolve().parent))
    from lab_result_interpreter import classify_lab_result_signal


PROJECT_DIR = Path(__file__).resolve().parent.parent
DEFAULT_INPUT_JSONL = PROJECT_DIR / "05_observations" / "lab_observations.jsonl"
DEFAULT_OUTPUT_DIR = PROJECT_DIR / "05_observations" / "lab_feature_matrix"
DEFAULT_CORE_MIN_CASES = 25
GROUPING_STRATEGY = "message_id_hash_then_attachment_id_then_filename"

BLOOD_REPORT_STYLE_CONCEPTS = {
    "WBC": ["OBS-HEM-WBC"],
    "RBC": ["OBS-HEM-RBC"],
    "HGB": ["OBS-HEM-HGB"],
    "PLT": ["OBS-HEM-PLT"],
    "NEUT": ["OBS-HEM-NEU-PCT", "OBS-HEM-NEU-ABS"],
    "LYMPH": ["OBS-HEM-LYM-PCT", "OBS-HEM-LYM-ABS"],
    "MONO": ["OBS-HEM-MONO-PCT", "OBS-HEM-MONO-ABS"],
    "EOS": ["OBS-HEM-EOS-PCT", "OBS-HEM-EOS-ABS"],
    "BASO": ["OBS-HEM-BASO-PCT", "OBS-HEM-BASO-ABS"],
}


def as_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    return str(value).strip()


def as_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(number) or math.isinf(number):
        return None
    return number


def slugify(text: str) -> str:
    cleaned = re.sub(r"[^a-zA-Z0-9]+", "_", text.strip().lower())
    return cleaned.strip("_")


def feature_name(*parts: str) -> str:
    return "__".join(slugify(part) for part in parts if part)


def set_add(target: set[str], value: Any) -> None:
    text = as_text(value)
    if text:
        target.add(text)


def summarize_numeric(values: list[float]) -> dict[str, float | None]:
    if not values:
        return {"value_mean": None, "value_min": None, "value_max": None}
    return {
        "value_mean": round(sum(values) / len(values), 6),
        "value_min": round(min(values), 6),
        "value_max": round(max(values), 6),
    }


def init_case_bucket(case_id: str) -> dict[str, Any]:
    return {
        "case_id": case_id,
        "attachment_ids": set(),
        "report_dates": set(),
        "hospital_names": set(),
        "departments": set(),
        "patient_genders": set(),
        "result_flag_counts": Counter(),
        "polarity_counts": Counter(),
        "abnormality_counts": Counter(),
        "observation_kind_counts": Counter(),
        "category_buckets": {},
        "concept_buckets": {},
        "totals": {
            "observations": 0,
            "mapped_concepts": 0,
            "numeric_observations": 0,
            "qualitative_observations": 0,
            "narrative_observations": 0,
            "positive_count": 0,
            "negative_count": 0,
            "normal_count": 0,
            "abnormal_count": 0,
            "high_count": 0,
            "low_count": 0,
            "unknown_count": 0,
        },
    }


def init_category_bucket(code: str, name: str) -> dict[str, Any]:
    return {
        "category_code": code,
        "category_name": name,
        "observed_count": 0,
        "numeric_count": 0,
        "qualitative_count": 0,
        "narrative_count": 0,
        "positive_count": 0,
        "negative_count": 0,
        "normal_count": 0,
        "abnormal_count": 0,
        "high_count": 0,
        "low_count": 0,
    }


def init_concept_bucket(code: str, name: str, category_code: str, category_name: str) -> dict[str, Any]:
    return {
        "concept_code": code,
        "concept_name": name,
        "category_code": category_code,
        "category_name": category_name,
        "units": set(),
        "service_codes": set(),
        "service_names": set(),
        "observed_count": 0,
        "numeric_values": [],
        "positive_count": 0,
        "negative_count": 0,
        "normal_count": 0,
        "abnormal_count": 0,
        "high_count": 0,
        "low_count": 0,
        "narrative_count": 0,
    }


def load_cases(observation_path: Path) -> tuple[dict[str, dict[str, Any]], dict[str, set[str]], dict[str, dict[str, Any]], Counter]:
    cases: dict[str, dict[str, Any]] = {}
    concept_case_sets: dict[str, set[str]] = defaultdict(set)
    concept_meta: dict[str, dict[str, Any]] = {}
    category_case_counts: Counter = Counter()

    with observation_path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            row = json.loads(line)
            case_id = (
                as_text(row.get("message_id_hash"))
                or as_text(row.get("attachment_id"))
                or as_text(row.get("filename"))
            )
            if not case_id:
                continue
            case = cases.setdefault(case_id, init_case_bucket(case_id))
            set_add(case["attachment_ids"], row.get("attachment_id"))
            set_add(case["report_dates"], row.get("report_date"))
            set_add(case["hospital_names"], row.get("hospital_name"))
            set_add(case["departments"], row.get("department"))
            set_add(case["patient_genders"], row.get("patient_gender"))

            totals = case["totals"]
            totals["observations"] += 1

            result_flag = as_text(row.get("result_flag")).lower() or "missing"
            polarity = as_text(row.get("polarity")).lower() or "missing"
            abnormality = as_text(row.get("abnormality")).lower() or "missing"
            observation_kind = as_text(row.get("observation_kind")).lower() or "missing"
            case["result_flag_counts"][result_flag] += 1
            case["polarity_counts"][polarity] += 1
            case["abnormality_counts"][abnormality] += 1
            case["observation_kind_counts"][observation_kind] += 1

            signal = classify_lab_result_signal(row)
            totals["positive_count"] += int(signal["is_positive"])
            totals["negative_count"] += int(signal["is_negative"])
            totals["normal_count"] += int(signal["is_normal"])
            totals["abnormal_count"] += int(signal["is_abnormal"])
            totals["unknown_count"] += int(result_flag in {"unknown", "missing"})
            totals["high_count"] += int(abnormality == "high")
            totals["low_count"] += int(abnormality == "low")

            if observation_kind == "quantitative":
                totals["numeric_observations"] += 1
            elif observation_kind == "narrative":
                totals["narrative_observations"] += 1
            else:
                totals["qualitative_observations"] += 1

            category_code = (
                as_text(row.get("observation_concept_category_code"))
                or as_text(row.get("observation_node_category_code"))
                or as_text(row.get("category_code"))
                or "UNKNOWN"
            )
            category_name = (
                as_text(row.get("observation_concept_category_name"))
                or as_text(row.get("observation_node_category_name"))
                or as_text(row.get("category_name"))
            )
            category_bucket = case["category_buckets"].setdefault(
                category_code,
                init_category_bucket(category_code, category_name),
            )
            category_bucket["observed_count"] += 1
            category_bucket["numeric_count"] += int(observation_kind == "quantitative")
            category_bucket["qualitative_count"] += int(observation_kind == "qualitative")
            category_bucket["narrative_count"] += int(observation_kind == "narrative")
            category_bucket["positive_count"] += int(signal["is_positive"])
            category_bucket["negative_count"] += int(signal["is_negative"])
            category_bucket["normal_count"] += int(signal["is_normal"])
            category_bucket["abnormal_count"] += int(signal["is_abnormal"])
            category_bucket["high_count"] += int(abnormality == "high")
            category_bucket["low_count"] += int(abnormality == "low")
            category_case_counts[category_code] += 0  # keep key alive

            concept_code = (
                as_text(row.get("observation_concept_code"))
                or as_text(row.get("observation_node_code"))
            )
            concept_name = (
                as_text(row.get("observation_concept_name"))
                or as_text(row.get("observation_node_name"))
            )
            if concept_code:
                totals["mapped_concepts"] += 1
                concept_case_sets[concept_code].add(case_id)
                concept_meta.setdefault(
                    concept_code,
                    {
                        "concept_code": concept_code,
                        "concept_name": concept_name,
                        "category_code": category_code,
                        "category_name": category_name,
                    },
                )
                concept_bucket = case["concept_buckets"].setdefault(
                    concept_code,
                    init_concept_bucket(concept_code, concept_name, category_code, category_name),
                )
                concept_bucket["observed_count"] += 1
                concept_bucket["positive_count"] += int(signal["is_positive"])
                concept_bucket["negative_count"] += int(signal["is_negative"])
                concept_bucket["normal_count"] += int(signal["is_normal"])
                concept_bucket["abnormal_count"] += int(signal["is_abnormal"])
                concept_bucket["high_count"] += int(abnormality == "high")
                concept_bucket["low_count"] += int(abnormality == "low")
                concept_bucket["narrative_count"] += int(signal["is_narrative"])
                set_add(concept_bucket["units"], row.get("unit_raw"))
                set_add(concept_bucket["service_codes"], row.get("service_code"))
                set_add(concept_bucket["service_names"], row.get("service_canonical_name"))
                numeric_value = as_float(row.get("numeric_value"))
                if numeric_value is not None:
                    concept_bucket["numeric_values"].append(numeric_value)

    for category_code, count in Counter(
        category
        for case in cases.values()
        for category in case["category_buckets"].keys()
    ).items():
        category_case_counts[category_code] = count

    return cases, concept_case_sets, concept_meta, category_case_counts


def finalize_case_rows(cases: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for case_id, case in sorted(cases.items()):
        concept_features: dict[str, dict[str, Any]] = {}
        for concept_code, bucket in sorted(case["concept_buckets"].items()):
            numeric_summary = summarize_numeric(bucket["numeric_values"])
            concept_features[concept_code] = {
                "concept_name": bucket["concept_name"],
                "category_code": bucket["category_code"],
                "category_name": bucket["category_name"],
                "observed_count": bucket["observed_count"],
                "positive_count": bucket["positive_count"],
                "negative_count": bucket["negative_count"],
                "normal_count": bucket["normal_count"],
                "abnormal_count": bucket["abnormal_count"],
                "high_count": bucket["high_count"],
                "low_count": bucket["low_count"],
                "narrative_count": bucket["narrative_count"],
                "units": sorted(bucket["units"]),
                "service_codes": sorted(bucket["service_codes"]),
                "service_names": sorted(bucket["service_names"]),
                **numeric_summary,
            }

        category_features = {
            code: {
                "category_name": bucket["category_name"],
                "observed_count": bucket["observed_count"],
                "numeric_count": bucket["numeric_count"],
                "qualitative_count": bucket["qualitative_count"],
                "narrative_count": bucket["narrative_count"],
                "positive_count": bucket["positive_count"],
                "negative_count": bucket["negative_count"],
                "normal_count": bucket["normal_count"],
                "abnormal_count": bucket["abnormal_count"],
                "high_count": bucket["high_count"],
                "low_count": bucket["low_count"],
            }
            for code, bucket in sorted(case["category_buckets"].items())
        }

        rows.append(
            {
                "case_id": case_id,
                "attachment_ids": sorted(case["attachment_ids"]),
                "report_dates": sorted(case["report_dates"]),
                "hospital_names": sorted(case["hospital_names"]),
                "departments": sorted(case["departments"]),
                "patient_genders": sorted(case["patient_genders"]),
                "feature_summary": dict(case["totals"]),
                "result_flag_counts": dict(case["result_flag_counts"]),
                "polarity_counts": dict(case["polarity_counts"]),
                "abnormality_counts": dict(case["abnormality_counts"]),
                "observation_kind_counts": dict(case["observation_kind_counts"]),
                "category_features": category_features,
                "concept_features": concept_features,
            }
        )
    return rows


def build_feature_dictionary(
    concept_case_sets: dict[str, set[str]],
    concept_meta: dict[str, dict[str, Any]],
    category_case_counts: Counter,
) -> dict[str, Any]:
    concepts = []
    for concept_code, cases in sorted(concept_case_sets.items()):
        meta = concept_meta.get(concept_code) or {}
        concepts.append(
            {
                "concept_code": concept_code,
                "concept_name": meta.get("concept_name", ""),
                "category_code": meta.get("category_code", ""),
                "category_name": meta.get("category_name", ""),
                "case_count": len(cases),
            }
        )
    categories = [
        {"category_code": code, "case_count": count}
        for code, count in sorted(category_case_counts.items(), key=lambda item: (-item[1], item[0]))
    ]
    return {"concepts": concepts, "categories": categories}


def pick_core_concepts(concept_dictionary: dict[str, Any], min_cases: int) -> list[dict[str, Any]]:
    concepts = concept_dictionary.get("concepts") or []
    return [
        row
        for row in concepts
        if row.get("concept_code") and int(row.get("case_count", 0)) >= min_cases
    ]


def build_flat_core_matrix(case_rows: list[dict[str, Any]], core_concepts: list[dict[str, Any]]) -> tuple[list[str], list[dict[str, Any]]]:
    columns = [
        "case_id",
        "report_date_first",
        "hospital_name_first",
        "summary__observations",
        "summary__mapped_concepts",
        "summary__numeric_observations",
        "summary__positive_count",
        "summary__negative_count",
        "summary__normal_count",
        "summary__abnormal_count",
    ]
    concept_columns: list[str] = []
    for concept in core_concepts:
        concept_code = concept["concept_code"]
        prefix = feature_name("concept", concept_code)
        concept_columns.extend(
            [
                f"{prefix}__observed",
                f"{prefix}__positive",
                f"{prefix}__negative",
                f"{prefix}__normal",
                f"{prefix}__abnormal",
                f"{prefix}__high",
                f"{prefix}__low",
                f"{prefix}__value_mean",
            ]
        )
    rows: list[dict[str, Any]] = []
    for case in case_rows:
        row = {
            "case_id": case["case_id"],
            "report_date_first": (case.get("report_dates") or [""])[0] if case.get("report_dates") else "",
            "hospital_name_first": (case.get("hospital_names") or [""])[0] if case.get("hospital_names") else "",
            "summary__observations": case["feature_summary"]["observations"],
            "summary__mapped_concepts": case["feature_summary"]["mapped_concepts"],
            "summary__numeric_observations": case["feature_summary"]["numeric_observations"],
            "summary__positive_count": case["feature_summary"]["positive_count"],
            "summary__negative_count": case["feature_summary"]["negative_count"],
            "summary__normal_count": case["feature_summary"]["normal_count"],
            "summary__abnormal_count": case["feature_summary"]["abnormal_count"],
        }
        concept_features = case.get("concept_features") or {}
        for concept in core_concepts:
            concept_code = concept["concept_code"]
            prefix = feature_name("concept", concept_code)
            bucket = concept_features.get(concept_code) or {}
            row[f"{prefix}__observed"] = int(bool(bucket))
            row[f"{prefix}__positive"] = bucket.get("positive_count", 0)
            row[f"{prefix}__negative"] = bucket.get("negative_count", 0)
            row[f"{prefix}__normal"] = bucket.get("normal_count", 0)
            row[f"{prefix}__abnormal"] = bucket.get("abnormal_count", 0)
            row[f"{prefix}__high"] = bucket.get("high_count", 0)
            row[f"{prefix}__low"] = bucket.get("low_count", 0)
            row[f"{prefix}__value_mean"] = bucket.get("value_mean")
        rows.append(row)
    return columns + concept_columns, rows


def build_blood_style_matrix(case_rows: list[dict[str, Any]]) -> tuple[list[str], list[dict[str, Any]], dict[str, int]]:
    columns = ["case_id", "report_date_first", "hospital_name_first"]
    for name in BLOOD_REPORT_STYLE_CONCEPTS:
        columns.extend([name, f"{name}__observed", f"{name}__abnormal", f"{name}__high", f"{name}__low"])
    coverage = Counter()
    rows: list[dict[str, Any]] = []

    for case in case_rows:
        concept_features = case.get("concept_features") or {}
        row = {
            "case_id": case["case_id"],
            "report_date_first": (case.get("report_dates") or [""])[0] if case.get("report_dates") else "",
            "hospital_name_first": (case.get("hospital_names") or [""])[0] if case.get("hospital_names") else "",
        }
        for feature_name_key, candidates in BLOOD_REPORT_STYLE_CONCEPTS.items():
            chosen = None
            for concept_code in candidates:
                if concept_code in concept_features:
                    chosen = concept_features[concept_code]
                    coverage[feature_name_key] += 1
                    break
            row[feature_name_key] = chosen.get("value_mean") if chosen else None
            row[f"{feature_name_key}__observed"] = int(chosen is not None)
            row[f"{feature_name_key}__abnormal"] = chosen.get("abnormal_count", 0) if chosen else 0
            row[f"{feature_name_key}__high"] = chosen.get("high_count", 0) if chosen else 0
            row[f"{feature_name_key}__low"] = chosen.get("low_count", 0) if chosen else 0
        rows.append(row)
    return columns, rows, dict(coverage)


def write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")


def write_csv(path: Path, columns: list[str], rows: list[dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def write_summary_markdown(path: Path, summary: dict[str, Any]) -> None:
    lines = [
        "# Lab Feature Matrix Summary",
        "",
        f"- cases: `{summary['case_count']}`",
        f"- observations: `{summary['observation_count']}`",
        f"- mapped concept cases: `{summary['cases_with_mapped_concepts']}`",
        f"- unique concept codes: `{summary['unique_concept_count']}`",
        f"- core concepts (`>= {summary['core_min_cases']}` cases): `{summary['core_concept_count']}`",
        "",
        "## Blood-Style Coverage",
    ]
    for key, value in summary.get("blood_style_feature_coverage", {}).items():
        lines.append(f"- `{key}`: `{value}` cases")
    lines.extend(["", "## Top Concepts By Case Coverage"])
    for row in summary.get("top_concepts_by_case_count", []):
        lines.append(
            f"- `{row['concept_code']}` {row['concept_name']}: `{row['case_count']}` cases"
        )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def build_lab_feature_matrix(
    observation_path: Path = DEFAULT_INPUT_JSONL,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    core_min_cases: int = DEFAULT_CORE_MIN_CASES,
) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)

    cases, concept_case_sets, concept_meta, category_case_counts = load_cases(observation_path)
    case_rows = finalize_case_rows(cases)
    feature_dictionary = build_feature_dictionary(concept_case_sets, concept_meta, category_case_counts)
    core_concepts = pick_core_concepts(feature_dictionary, core_min_cases)

    case_feature_rows_jsonl = output_dir / "lab_case_feature_rows.jsonl"
    feature_dictionary_json = output_dir / "lab_feature_dictionary.json"
    core_matrix_csv = output_dir / "lab_feature_matrix_core.csv"
    blood_style_csv = output_dir / "blood_report_style_matrix.csv"
    summary_json = output_dir / "lab_feature_matrix_summary.json"
    summary_md = output_dir / "lab_feature_matrix_summary.md"

    write_jsonl(case_feature_rows_jsonl, case_rows)
    feature_dictionary_json.write_text(json.dumps(feature_dictionary, ensure_ascii=False, indent=2), encoding="utf-8")

    core_columns, core_rows = build_flat_core_matrix(case_rows, core_concepts)
    write_csv(core_matrix_csv, core_columns, core_rows)

    blood_columns, blood_rows, blood_coverage = build_blood_style_matrix(case_rows)
    write_csv(blood_style_csv, blood_columns, blood_rows)

    summary = {
        "generated_from": str(observation_path),
        "output_dir": str(output_dir),
        "grouping_strategy": GROUPING_STRATEGY,
        "case_count": len(case_rows),
        "unique_attachment_count": len(
            {
                attachment_id
                for row in case_rows
                for attachment_id in (row.get("attachment_ids") or [])
                if attachment_id
            }
        ),
        "observation_count": sum(row["feature_summary"]["observations"] for row in case_rows),
        "cases_with_mapped_concepts": sum(
            1 for row in case_rows if row["feature_summary"]["mapped_concepts"] > 0
        ),
        "unique_concept_count": len(feature_dictionary["concepts"]),
        "core_min_cases": core_min_cases,
        "core_concept_count": len(core_concepts),
        "core_matrix_column_count": len(core_columns),
        "blood_style_feature_coverage": blood_coverage,
        "top_concepts_by_case_count": sorted(
            feature_dictionary["concepts"],
            key=lambda row: (-row["case_count"], row["concept_code"]),
        )[:20],
        "top_categories_by_case_count": feature_dictionary["categories"][:20],
        "artifacts": {
            "case_feature_rows_jsonl": str(case_feature_rows_jsonl),
            "feature_dictionary_json": str(feature_dictionary_json),
            "core_matrix_csv": str(core_matrix_csv),
            "blood_style_csv": str(blood_style_csv),
            "summary_json": str(summary_json),
            "summary_md": str(summary_md),
        },
    }
    summary_json.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    write_summary_markdown(summary_md, summary)
    return summary


if __name__ == "__main__":
    result = build_lab_feature_matrix()
    print(json.dumps(result, ensure_ascii=False, indent=2))
