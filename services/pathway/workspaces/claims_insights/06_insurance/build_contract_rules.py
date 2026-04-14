from __future__ import annotations

import json
import unicodedata
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


PROJECT_DIR = Path(__file__).parent.parent
REPORTS_DIR = PROJECT_DIR / "04_reports"
INSURANCE_DIR = PROJECT_DIR / "06_insurance"
SUMMARY_JSON_PATH = REPORTS_DIR / "tong_hop_hdbh.json"
OUTPUT_PATH = Path(__file__).parent / "contract_rules.json"


def find_required_file(filename: str) -> Path:
    matches = sorted(INSURANCE_DIR.rglob(filename))
    if not matches:
        raise FileNotFoundError(f"Missing required file: {filename}")
    return matches[0]


def strip_diacritics(text: str) -> str:
    text = str(text or "").lower().replace("đ", "d").replace("Đ", "d")
    nfkd = unicodedata.normalize("NFKD", text)
    return "".join(c for c in nfkd if not unicodedata.combining(c))


def normalize_text(text: str) -> str:
    return " ".join(str(text or "").strip().split())


def infer_insurer(contract_name: str) -> str:
    key = strip_diacritics(contract_name)
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


def infer_rule_type(reasons: list[str]) -> str:
    text = " ".join(strip_diacritics(reason) for reason in reasons)
    if any(key in text for key in ["duong tinh", "am tinh", "ket qua xet nghiem"]):
        return "pay_if_positive"
    if any(key in text for key in ["preauth", "phe duyet truoc", "chap thuan truoc"]):
        return "pay_if_preauthorized"
    if any(key in text for key in ["khong phu hop chan doan", "icd", "chan doan cuoi"]):
        return "pay_if_final_icd_match"
    return "pay_if_medically_necessary"


def parse_reason_dictionary(path: Path) -> tuple[list[dict[str, Any]], dict[str, list[str]]]:
    xls = pd.ExcelFile(path)
    df = pd.read_excel(path, sheet_name=xls.sheet_names[0], skiprows=1).dropna(how="all")
    exclusion_items: list[dict[str, Any]] = []
    by_group: dict[str, list[str]] = defaultdict(list)
    for _, row in df.iterrows():
        code = normalize_text(row.iloc[0])
        reason = normalize_text(row.iloc[1])
        group = normalize_text(row.iloc[2])
        process = normalize_text(row.iloc[3])
        source = normalize_text(row.iloc[4])
        if not reason:
            continue
        exclusion_items.append(
            {
                "code": code,
                "reason": reason,
                "group": group,
                "process_path": process,
                "source_note": source,
            }
        )
        if group:
            by_group[group].append(reason)
    return exclusion_items, dict(by_group)


def load_main_claims(path: Path) -> pd.DataFrame:
    df = pd.read_excel(path, sheet_name="data")
    return pd.DataFrame(
        {
            "contract": df.iloc[:, 1].fillna("(trống)").astype(str),
            "rule": df.iloc[:, 2].fillna("(trống)").astype(str),
            "benefit": df.iloc[:, 9].fillna("(trống)").astype(str),
            "requested": pd.to_numeric(df.iloc[:, 10], errors="coerce").fillna(0.0),
            "paid": pd.to_numeric(df.iloc[:, 11], errors="coerce").fillna(0.0),
            "reason": df.iloc[:, 12].fillna("(trống)").astype(str),
            "status": df.iloc[:, 17].fillna("(trống)").astype(str),
        }
    )


def build_contract_rule_items(df: pd.DataFrame, exclusion_by_group: dict[str, list[str]]) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for contract, group_df in df.groupby("contract"):
        contract_name = normalize_text(contract)
        reasons = [normalize_text(reason) for reason in group_df["reason"].dropna().astype(str).tolist()]
        top_reasons = (
            group_df["reason"].fillna("(trống)").astype(str).value_counts().head(20).to_dict()
        )
        top_benefits = (
            group_df["benefit"].fillna("(trống)").astype(str).value_counts().head(20).to_dict()
        )
        top_rules = group_df["rule"].fillna("(trống)").astype(str).value_counts().head(10).to_dict()

        requested_sum = float(group_df["requested"].sum())
        paid_sum = float(group_df["paid"].sum())
        paid_ratio = round(100.0 * paid_sum / requested_sum, 2) if requested_sum > 0 else 0.0

        reason_text = " ".join(strip_diacritics(reason) for reason in reasons)
        requires_preauth = any(key in reason_text for key in ["preauth", "phe duyet truoc", "chap thuan truoc"])
        positive_result_required = any(key in reason_text for key in ["duong tinh", "am tinh", "ket qua xet nghiem"])

        items.append(
            {
                "contract_id": contract_name,
                "insurer": infer_insurer(contract_name),
                "product": contract_name,
                "rule_type": infer_rule_type(reasons),
                "covered_categories": ["LAB-*", "IMG-*", "END-*", "FUN-*", "PAT-*", "PRO-*"],
                "requires_preauth": requires_preauth,
                "positive_result_required": positive_result_required,
                "copay_percent": 0,
                "sublimit_per_visit": None,
                "exclusion_groups": sorted(exclusion_by_group.keys()),
                "top_denial_reasons": [
                    {"reason": normalize_text(reason), "count": int(count)}
                    for reason, count in top_reasons.items()
                ],
                "top_rules_in_data": [
                    {"rule": normalize_text(rule_name), "count": int(count)}
                    for rule_name, count in top_rules.items()
                ],
                "top_benefits": [
                    {"benefit": normalize_text(benefit), "count": int(count)}
                    for benefit, count in top_benefits.items()
                ],
                "stats": {
                    "rows": int(len(group_df)),
                    "requested_sum_vnd": round(requested_sum, 0),
                    "paid_sum_vnd": round(paid_sum, 0),
                    "paid_ratio_pct": paid_ratio,
                },
            }
        )
    items.sort(key=lambda item: item["contract_id"])
    return items


def load_summary(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def main() -> None:
    main_claims_path = find_required_file("Danh sách hồ sơ loại trừ.xlsx")
    reason_dictionary_path = find_required_file("Lý do loại trừ.xlsx")

    claims_df = load_main_claims(main_claims_path)
    exclusion_items, exclusion_by_group = parse_reason_dictionary(reason_dictionary_path)
    contract_rules = build_contract_rule_items(claims_df, exclusion_by_group)
    summary = load_summary(SUMMARY_JSON_PATH)

    output = {
        "version": "1.0",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "sources": {
            "main_claims": str(main_claims_path.relative_to(PROJECT_DIR)),
            "reason_dictionary": str(reason_dictionary_path.relative_to(PROJECT_DIR)),
            "summary_report": str(SUMMARY_JSON_PATH.relative_to(PROJECT_DIR)),
        },
        "taxonomy": {
            "exclusion_items": exclusion_items,
            "exclusion_groups": sorted(exclusion_by_group.keys()),
        },
        "contract_rules": contract_rules,
        "stats": {
            "contracts": len(contract_rules),
            "exclusion_items": len(exclusion_items),
            "exclusion_groups": len(exclusion_by_group),
            "summary_generated_at": summary.get("generated_at"),
        },
    }

    OUTPUT_PATH.write_text(json.dumps(output, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"Contract rules saved: {OUTPUT_PATH}")
    print(f"Contracts: {len(contract_rules)}")
    print(f"Exclusion groups: {len(exclusion_by_group)}")


if __name__ == "__main__":
    main()
