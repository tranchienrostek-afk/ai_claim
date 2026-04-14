from __future__ import annotations

import hashlib
import json
import os
import sys
import unicodedata
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import pandas as pd
from neo4j import GraphDatabase

from runtime_env import load_notebooklm_env


load_notebooklm_env()


STANDARDIZE_DIR = Path(__file__).resolve().parents[1] / "workspaces" / "claims_insights" / "02_standardize"
if str(STANDARDIZE_DIR) not in sys.path:
    sys.path.insert(0, str(STANDARDIZE_DIR))

from service_text_mapper import ServiceTextMapper  # noqa: E402


DEFAULT_NAMESPACE = "claims_insights_explorer_v1"
CANONICAL_NAMESPACE = "canonical_service_v1"

SERVICE_NAME_COLUMNS = (
    "service_name_raw",
    "service_name",
    "service_text",
    "ten_dich_vu",
    "ten_dv",
    "ten_dvkt",
    "ten",
    "noi_dung",
    "description",
    "item_name",
)
SERVICE_CODE_COLUMNS = (
    "hospital_service_code",
    "service_code_raw",
    "ma_his_dv",
    "ma_his",
    "ma_dich_vu",
    "ma_dv",
    "madv",
    "code",
    "ma",
)
DIRECT_CANONICAL_COLUMNS = (
    "maanhxa",
    "ma_anh_xa",
    "ma_tuong_duong",
    "bhyt_code",
    "ma_byt",
)
PRICE_COLUMNS = (
    "price_vnd",
    "don_gia",
    "gia",
    "gia_dv",
    "unit_price",
    "thanh_tien",
)
HOSPITAL_COLUMNS = (
    "hospital_name",
    "benh_vien",
    "ten_benh_vien",
    "co_so",
    "branch",
)


def _ascii_fold(text: Any) -> str:
    value = str(text or "").strip()
    value = value.replace("đ", "d").replace("Đ", "D")
    value = unicodedata.normalize("NFD", value)
    value = "".join(ch for ch in value if unicodedata.category(ch) != "Mn")
    lowered = value.lower()
    return "".join(ch if ch.isalnum() else "_" for ch in lowered).strip("_")


def _now_iso() -> str:
    return datetime.now(timezone.utc).astimezone().isoformat()


def _safe_float(value: Any) -> Optional[float]:
    if value in (None, "", "nan"):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip().replace(",", "")
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _json_dump(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False)


@dataclass
class SheetPayload:
    sheet_name: str
    frame: pd.DataFrame
    service_name_column: str
    service_code_column: str
    direct_canonical_column: str
    price_column: str
    hospital_column: str


class PathwayServiceTableIngestor:
    """Map hospital service lists to canonical services and persist them into Neo4j."""

    def __init__(self) -> None:
        self.mapper = ServiceTextMapper()
        self.codebook_lookup, self.maanhxa_lookup = self._load_codebook_lookup()

    def ingest_asset(
        self,
        *,
        asset: dict[str, Any],
        file_path: Path,
        namespace: str = DEFAULT_NAMESPACE,
        source_type: Optional[str] = None,
    ) -> dict[str, Any]:
        namespace = namespace or DEFAULT_NAMESPACE
        sheet_payloads = self._load_sheet_payloads(file_path)
        if not sheet_payloads:
            raise ValueError("Khong tim thay sheet/co cot dich vu hop le trong file service table.")

        mapped_sheets: list[dict[str, Any]] = []
        aggregate_rows: list[dict[str, Any]] = []
        skipped_sheets: list[str] = []
        for payload in sheet_payloads:
            if payload.service_name_column:
                mapped_frame = self._map_sheet(payload.frame, payload.service_name_column)
            else:
                mapped_frame = payload.frame.copy()
            enriched_frame = self._enrich_mapped_frame(
                mapped_frame,
                payload=payload,
                asset=asset,
                file_path=file_path,
                namespace=namespace,
                source_type=source_type or (asset.get("config") or {}).get("source_type") or "hospital",
            )
            if enriched_frame.empty:
                skipped_sheets.append(payload.sheet_name)
                continue
            sheet_stats = self._sheet_stats(enriched_frame)
            mapped_sheets.append(
                {
                    "sheet_name": payload.sheet_name,
                    "row_count": int(len(enriched_frame)),
                    "stats": sheet_stats,
                }
            )
            aggregate_rows.extend(enriched_frame.to_dict(orient="records"))

        if not aggregate_rows:
            raise ValueError("Khong co dong du lieu hop le nao de ingest tu service table.")

        graph_stats = self._write_graph_rows(
            asset=asset,
            file_path=file_path,
            rows=aggregate_rows,
            namespace=namespace,
            source_type=source_type or (asset.get("config") or {}).get("source_type") or "hospital",
        )
        summary = self._aggregate_stats(aggregate_rows)
        summary.update(graph_stats)

        return {
            "status": "completed",
            "mode": "service_table_excel",
            "namespace": namespace,
            "asset_id": asset.get("asset_id"),
            "document_count": 1,
            "diseases": [],
            "service_count": summary.get("mapped_service_codes", 0),
            "canonical_count": summary.get("mapped_canonical_services", 0),
            "row_count": summary.get("row_count", 0),
            "items": mapped_sheets,
            "summary": summary,
            "skipped_sheets": skipped_sheets,
        }

    def _load_codebook_lookup(self) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
        codebook_path = STANDARDIZE_DIR / "service_codebook.json"
        payload = json.loads(codebook_path.read_text(encoding="utf-8"))
        by_service_code: dict[str, dict[str, Any]] = {}
        by_maanhxa: dict[str, dict[str, Any]] = {}
        for item in payload.get("codebook", []):
            service_code = str(item.get("service_code") or "").strip()
            if service_code:
                by_service_code[service_code] = item
            bhyt = item.get("bhyt") or {}
            maanhxa = str(bhyt.get("ma_tuong_duong") or "").strip()
            if maanhxa and maanhxa not in by_maanhxa:
                by_maanhxa[maanhxa] = item
        return by_service_code, by_maanhxa

    def _load_sheet_payloads(self, file_path: Path) -> list[SheetPayload]:
        suffix = file_path.suffix.lower()
        payloads: list[SheetPayload] = []
        if suffix in {".xlsx", ".xls"}:
            workbook = pd.ExcelFile(file_path)
            for sheet_name in workbook.sheet_names:
                frame = workbook.parse(sheet_name=sheet_name)
                payload = self._build_sheet_payload(sheet_name, frame)
                if payload is not None:
                    payloads.append(payload)
            return payloads

        if suffix == ".csv":
            frame = pd.read_csv(file_path)
            payload = self._build_sheet_payload("csv", frame)
            return [payload] if payload is not None else []

        raise ValueError(f"Unsupported service table format: {file_path.suffix}")

    def _build_sheet_payload(self, sheet_name: str, frame: pd.DataFrame) -> Optional[SheetPayload]:
        if frame is None or frame.empty:
            return None
        normalized_columns = {_ascii_fold(column): str(column) for column in frame.columns}
        service_name_column = self._pick_column(normalized_columns, SERVICE_NAME_COLUMNS)
        direct_canonical_column = self._pick_column(normalized_columns, DIRECT_CANONICAL_COLUMNS)
        pre_mapped_column = self._pick_column(normalized_columns, ("mapped_service_code",))
        if not service_name_column and not direct_canonical_column and not pre_mapped_column:
            return None
        return SheetPayload(
            sheet_name=sheet_name,
            frame=frame.copy(),
            service_name_column=service_name_column,
            service_code_column=self._pick_column(normalized_columns, SERVICE_CODE_COLUMNS),
            direct_canonical_column=direct_canonical_column,
            price_column=self._pick_column(normalized_columns, PRICE_COLUMNS),
            hospital_column=self._pick_column(normalized_columns, HOSPITAL_COLUMNS),
        )

    def _pick_column(self, normalized_columns: dict[str, str], candidates: tuple[str, ...]) -> str:
        for candidate in candidates:
            match = normalized_columns.get(_ascii_fold(candidate))
            if match:
                return match
        return ""

    def _map_sheet(self, frame: pd.DataFrame, service_name_column: str) -> pd.DataFrame:
        mapped_frame = frame.copy()
        missing_mask = pd.Series(True, index=mapped_frame.index)
        if "mapped_service_code" in mapped_frame.columns:
            missing_mask = mapped_frame["mapped_service_code"].fillna("").astype(str).str.strip().eq("")

        if missing_mask.any():
            mapped_subset = self.mapper.map_dataframe(
                mapped_frame.loc[missing_mask].copy(),
                text_column=service_name_column,
                top_k=3,
            )
            for column in mapped_subset.columns:
                mapped_frame.loc[missing_mask, column] = mapped_subset[column].values
        if "normalized_text" not in mapped_frame.columns:
            mapped_frame["normalized_text"] = mapped_frame[service_name_column].fillna("").astype(str).str.strip()
        return mapped_frame

    def _enrich_mapped_frame(
        self,
        mapped_frame: pd.DataFrame,
        *,
        payload: SheetPayload,
        asset: dict[str, Any],
        file_path: Path,
        namespace: str,
        source_type: str,
    ) -> pd.DataFrame:
        rows: list[dict[str, Any]] = []
        asset_id = str(asset.get("asset_id") or "")
        version_id = str(asset.get("current_version_id") or "")
        default_hospital = str((asset.get("config") or {}).get("hospital_name") or asset.get("title") or file_path.stem)
        now = _now_iso()
        for row_number, (_, row) in enumerate(mapped_frame.iterrows(), start=2):
            service_name_raw = self._read_cell(row, payload.service_name_column)
            mapped_service_code = self._read_cell(row, "mapped_service_code")
            mapped_canonical_name = self._read_cell(row, "mapped_canonical_name")
            direct_maanhxa = self._read_cell(row, payload.direct_canonical_column)
            hospital_service_code = self._read_cell(row, payload.service_code_column)
            hospital_name = self._read_cell(row, payload.hospital_column) or default_hospital
            normalized_text = self._read_cell(row, "normalized_text") or service_name_raw
            mapping_resolution = self._read_cell(row, "mapping_resolution") or ("coded" if mapped_service_code else "unknown")
            mapped_confidence = self._read_cell(row, "mapped_confidence") or ("DIRECT" if direct_maanhxa else "")
            mapped_score = _safe_float(self._read_cell(row, "mapped_score"))
            matched_variant = self._read_cell(row, "matched_variant")
            matched_reasons = self._read_cell(row, "matched_reasons")
            matched_conflicts = self._read_cell(row, "matched_conflicts")
            alternative_candidates = self._read_cell(row, "alternative_candidates")
            price_vnd = _safe_float(self._read_cell(row, payload.price_column))

            if not any([service_name_raw, mapped_service_code, direct_maanhxa]):
                continue

            codebook_entry = self.codebook_lookup.get(mapped_service_code or "")
            canonical_entry = None
            if direct_maanhxa:
                canonical_entry = self.maanhxa_lookup.get(direct_maanhxa)
            if canonical_entry is None and codebook_entry is not None:
                bhyt = codebook_entry.get("bhyt") or {}
                maanhxa = str(bhyt.get("ma_tuong_duong") or "").strip()
                if maanhxa:
                    canonical_entry = self.maanhxa_lookup.get(maanhxa) or codebook_entry

            canonical_payload = self._canonical_payload(
                codebook_entry=codebook_entry,
                canonical_entry=canonical_entry,
                direct_maanhxa=direct_maanhxa,
                fallback_name=mapped_canonical_name or service_name_raw,
            )
            conflict_flags = self._conflict_flags(
                direct_maanhxa=direct_maanhxa,
                codebook_entry=codebook_entry,
                mapped_service_code=mapped_service_code,
            )
            row_uid = self._row_uid(
                asset_id=asset_id,
                sheet_name=payload.sheet_name,
                row_number=row_number,
                service_name_raw=service_name_raw,
                hospital_service_code=hospital_service_code,
            )
            rows.append(
                {
                    "row_uid": row_uid,
                    "asset_id": asset_id,
                    "namespace": namespace,
                    "version_id": version_id,
                    "sheet_name": payload.sheet_name,
                    "row_number": row_number,
                    "service_name_raw": service_name_raw,
                    "service_name_normalized": normalized_text,
                    "hospital_service_code": hospital_service_code,
                    "hospital_name": hospital_name,
                    "source_file": str(file_path),
                    "source_type": source_type,
                    "mapping_resolution": mapping_resolution,
                    "mapped_service_code": mapped_service_code,
                    "mapped_canonical_name": mapped_canonical_name,
                    "mapped_category_name": self._read_cell(row, "mapped_category_name"),
                    "mapped_confidence": mapped_confidence,
                    "mapped_score": mapped_score,
                    "matched_variant": matched_variant,
                    "matched_reasons": matched_reasons,
                    "matched_conflicts": matched_conflicts,
                    "alternative_candidates": alternative_candidates,
                    "input_price_vnd": price_vnd,
                    "direct_maanhxa": direct_maanhxa,
                    "canonical_maanhxa": canonical_payload.get("maanhxa") or "",
                    "canonical_name_primary": canonical_payload.get("canonical_name_primary") or "",
                    "canonical_price_vnd": canonical_payload.get("byt_price"),
                    "canonical_category_code": canonical_payload.get("category_code") or "",
                    "canonical_category_name": canonical_payload.get("category_name") or "",
                    "canonical_specialty_name": canonical_payload.get("specialty_name") or "",
                    "ma_tt43": canonical_payload.get("ma_tt43") or "",
                    "ma_lien_thong_bhyt": canonical_payload.get("ma_lien_thong_bhyt") or "",
                    "bridge_method": self._bridge_method(
                        direct_maanhxa=direct_maanhxa,
                        mapped_service_code=mapped_service_code,
                        conflict_flags=conflict_flags,
                    ),
                    "review_status": "needs_review" if conflict_flags or mapping_resolution in {"unknown", "family_only"} or mapped_confidence == "REVIEW" else "clear",
                    "conflict_flags_json": _json_dump(conflict_flags),
                    "raw_payload_json": _json_dump({key: self._coerce_jsonable(value) for key, value in row.to_dict().items()}),
                    "updated_at": now,
                }
            )
        return pd.DataFrame(rows)

    def _canonical_payload(
        self,
        *,
        codebook_entry: Optional[dict[str, Any]],
        canonical_entry: Optional[dict[str, Any]],
        direct_maanhxa: str,
        fallback_name: str,
    ) -> dict[str, Any]:
        source = canonical_entry or codebook_entry or {}
        bhyt = source.get("bhyt") or {}
        maanhxa = direct_maanhxa or str(bhyt.get("ma_tuong_duong") or "").strip()
        return {
            "maanhxa": maanhxa,
            "canonical_name_primary": str(bhyt.get("ten_tt43") or source.get("canonical_name") or fallback_name or "").strip(),
            "category_code": str(source.get("category_code") or "").strip(),
            "category_name": str(source.get("category_name") or "").strip(),
            "specialty_name": str(bhyt.get("chuyen_khoa_tt43") or "").strip(),
            "byt_price": _safe_float(bhyt.get("gia_tt39_vnd")),
            "ma_tt43": str(bhyt.get("ma_tt43") or "").strip(),
            "ma_lien_thong_bhyt": str(bhyt.get("ma_lien_thong_bhyt") or "").strip(),
        }

    def _conflict_flags(
        self,
        *,
        direct_maanhxa: str,
        codebook_entry: Optional[dict[str, Any]],
        mapped_service_code: str,
    ) -> list[str]:
        flags: list[str] = []
        if mapped_service_code and not codebook_entry:
            flags.append("mapped_service_code_missing_in_codebook")
        codebook_maanhxa = ""
        if codebook_entry:
            codebook_maanhxa = str((codebook_entry.get("bhyt") or {}).get("ma_tuong_duong") or "").strip()
        if direct_maanhxa and codebook_maanhxa and direct_maanhxa != codebook_maanhxa:
            flags.append("direct_maanhxa_conflicts_with_mapped_service_code")
        return flags

    def _bridge_method(
        self,
        *,
        direct_maanhxa: str,
        mapped_service_code: str,
        conflict_flags: list[str],
    ) -> str:
        if conflict_flags:
            return "manual_review"
        if direct_maanhxa and mapped_service_code:
            return "hospital_excel_maanhxa_plus_mapper"
        if direct_maanhxa:
            return "hospital_excel_maanhxa"
        if mapped_service_code:
            return "service_text_mapper"
        return "unmapped"

    def _sheet_stats(self, frame: pd.DataFrame) -> dict[str, Any]:
        review_count = int(frame["review_status"].eq("needs_review").sum()) if "review_status" in frame.columns else 0
        mapped_count = int(frame["mapped_service_code"].fillna("").astype(str).str.strip().ne("").sum()) if "mapped_service_code" in frame.columns else 0
        canonical_count = int(frame["canonical_maanhxa"].fillna("").astype(str).str.strip().ne("").sum()) if "canonical_maanhxa" in frame.columns else 0
        return {
            "mapped_rows": mapped_count,
            "canonical_rows": canonical_count,
            "review_rows": review_count,
        }

    def _aggregate_stats(self, rows: list[dict[str, Any]]) -> dict[str, Any]:
        mapped_service_codes = {str(row.get("mapped_service_code") or "").strip() for row in rows if str(row.get("mapped_service_code") or "").strip()}
        canonical_services = {str(row.get("canonical_maanhxa") or "").strip() for row in rows if str(row.get("canonical_maanhxa") or "").strip()}
        review_rows = [row for row in rows if row.get("review_status") == "needs_review"]
        return {
            "row_count": len(rows),
            "mapped_rows": sum(1 for row in rows if str(row.get("mapped_service_code") or "").strip()),
            "canonical_rows": sum(1 for row in rows if str(row.get("canonical_maanhxa") or "").strip()),
            "review_rows": len(review_rows),
            "mapped_service_codes": len(mapped_service_codes),
            "mapped_canonical_services": len(canonical_services),
            "hospital_count": len({str(row.get("hospital_name") or "").strip() for row in rows if str(row.get("hospital_name") or "").strip()}),
        }

    def _write_graph_rows(
        self,
        *,
        asset: dict[str, Any],
        file_path: Path,
        rows: list[dict[str, Any]],
        namespace: str,
        source_type: str,
    ) -> dict[str, Any]:
        uri = os.getenv("NEO4J_URI", os.getenv("neo4j_uri", "bolt://localhost:7688"))
        user = os.getenv("NEO4J_USER", os.getenv("neo4j_user", "neo4j"))
        password = os.getenv("NEO4J_PASSWORD", os.getenv("neo4j_password", "password123"))
        driver = GraphDatabase.driver(uri, auth=(user, password))
        try:
            driver.verify_connectivity()
            with driver.session() as session:
                self._ensure_graph_schema(session)
                session.run(
                    """
                    MATCH (row:HospitalServiceRow {asset_id:$asset_id})
                    DETACH DELETE row
                    """,
                    asset_id=asset.get("asset_id"),
                )
                session.run(
                    """
                    MATCH (asset:HospitalServiceAsset {asset_id:$asset_id})
                    DETACH DELETE asset
                    """,
                    asset_id=asset.get("asset_id"),
                )
                session.run(
                    """
                    MERGE (asset:HospitalServiceAsset {asset_id:$asset_id})
                    SET asset.namespace = $namespace,
                        asset.title = $title,
                        asset.source_file = $source_file,
                        asset.source_type = $source_type,
                        asset.current_version_id = $version_id,
                        asset.kind = 'service_table',
                        asset.updated_at = $updated_at
                    """,
                    asset_id=asset.get("asset_id"),
                    namespace=namespace,
                    title=asset.get("title") or file_path.stem,
                    source_file=str(file_path),
                    source_type=source_type,
                    version_id=asset.get("current_version_id") or "",
                    updated_at=_now_iso(),
                )
                for row in rows:
                    self._merge_row(session, row)
            return {
                "neo4j_status": "ok",
                "driver_uri": uri,
            }
        finally:
            driver.close()

    def _ensure_graph_schema(self, session) -> None:
        statements = [
            "CREATE INDEX hospital_service_asset_id_idx IF NOT EXISTS FOR (n:HospitalServiceAsset) ON (n.asset_id)",
            "CREATE INDEX hospital_service_row_uid_idx IF NOT EXISTS FOR (n:HospitalServiceRow) ON (n.row_uid)",
            "CREATE INDEX canonical_service_maanhxa_idx IF NOT EXISTS FOR (n:CanonicalService) ON (n.maanhxa)",
            "CREATE INDEX service_classification_idx IF NOT EXISTS FOR (n:ServiceClassification) ON (n.classification_id)",
            "CREATE INDEX price_variant_idx IF NOT EXISTS FOR (n:PriceVariant) ON (n.variant_id)",
            "CREATE INDEX ci_service_code_idx IF NOT EXISTS FOR (n:CIService) ON (n.service_code)",
        ]
        for statement in statements:
            session.run(statement)

    def _merge_row(self, session, row: dict[str, Any]) -> None:
        session.run(
            """
            MATCH (asset:HospitalServiceAsset {asset_id:$asset_id})
            MERGE (row:HospitalServiceRow {row_uid:$row_uid})
            SET row.asset_id = $asset_id,
                row.namespace = $namespace,
                row.version_id = $version_id,
                row.sheet_name = $sheet_name,
                row.row_number = $row_number,
                row.service_name_raw = $service_name_raw,
                row.service_name_normalized = $service_name_normalized,
                row.hospital_service_code = $hospital_service_code,
                row.hospital_name = $hospital_name,
                row.source_file = $source_file,
                row.source_type = $source_type,
                row.mapping_resolution = $mapping_resolution,
                row.mapped_service_code = $mapped_service_code,
                row.mapped_canonical_name = $mapped_canonical_name,
                row.mapped_category_name = $mapped_category_name,
                row.mapped_confidence = $mapped_confidence,
                row.mapped_score = $mapped_score,
                row.matched_variant = $matched_variant,
                row.matched_reasons = $matched_reasons,
                row.matched_conflicts = $matched_conflicts,
                row.alternative_candidates = $alternative_candidates,
                row.input_price_vnd = $input_price_vnd,
                row.direct_maanhxa = $direct_maanhxa,
                row.canonical_maanhxa = $canonical_maanhxa,
                row.canonical_name_primary = $canonical_name_primary,
                row.canonical_price_vnd = $canonical_price_vnd,
                row.canonical_category_code = $canonical_category_code,
                row.canonical_category_name = $canonical_category_name,
                row.canonical_specialty_name = $canonical_specialty_name,
                row.ma_tt43 = $ma_tt43,
                row.ma_lien_thong_bhyt = $ma_lien_thong_bhyt,
                row.bridge_method = $bridge_method,
                row.review_status = $review_status,
                row.conflict_flags_json = $conflict_flags_json,
                row.raw_payload_json = $raw_payload_json,
                row.updated_at = $updated_at
            MERGE (asset)-[:HAS_SERVICE_ROW]->(row)
            """,
            **row,
        )

        mapped_service_code = str(row.get("mapped_service_code") or "").strip()
        canonical_maanhxa = str(row.get("canonical_maanhxa") or "").strip()
        review_status = str(row.get("review_status") or "")
        bridge_method = str(row.get("bridge_method") or "")

        if mapped_service_code:
            session.run(
                """
                MERGE (svc:CIService {service_code:$service_code})
                ON CREATE SET
                    svc.namespace = $namespace,
                    svc.service_name = $service_name,
                    svc.category_name = $category_name,
                    svc.category_code = $category_code
                ON MATCH SET
                    svc.service_name = coalesce(svc.service_name, $service_name),
                    svc.category_name = coalesce(svc.category_name, $category_name),
                    svc.category_code = coalesce(svc.category_code, $category_code)
                """,
                service_code=mapped_service_code,
                namespace=row.get("namespace"),
                service_name=row.get("mapped_canonical_name") or row.get("service_name_raw"),
                category_name=row.get("mapped_category_name") or row.get("canonical_category_name"),
                category_code=row.get("canonical_category_code"),
            )
            session.run(
                """
                MATCH (row:HospitalServiceRow {row_uid:$row_uid})
                MATCH (svc:CIService {service_code:$service_code})
                MERGE (row)-[rel:MAPS_TO_CI_SERVICE]->(svc)
                SET rel.confidence = $confidence,
                    rel.score = $score,
                    rel.method = $method,
                    rel.updated_at = $updated_at
                """,
                row_uid=row.get("row_uid"),
                service_code=mapped_service_code,
                confidence=row.get("mapped_confidence"),
                score=row.get("mapped_score"),
                method=bridge_method,
                updated_at=row.get("updated_at"),
            )

        if canonical_maanhxa:
            session.run(
                """
                MERGE (cs:CanonicalService {maanhxa:$maanhxa})
                ON CREATE SET
                    cs.namespace = $canonical_namespace,
                    cs.canonical_name_primary = $canonical_name_primary,
                    cs.ma_tt43 = $ma_tt43,
                    cs.ma_lien_thong_bhyt = $ma_lien_thong_bhyt,
                    cs.source = 'service_codebook'
                ON MATCH SET
                    cs.canonical_name_primary = coalesce(cs.canonical_name_primary, $canonical_name_primary),
                    cs.ma_tt43 = coalesce(cs.ma_tt43, $ma_tt43),
                    cs.ma_lien_thong_bhyt = coalesce(cs.ma_lien_thong_bhyt, $ma_lien_thong_bhyt)
                """,
                maanhxa=canonical_maanhxa,
                canonical_namespace=CANONICAL_NAMESPACE,
                canonical_name_primary=row.get("canonical_name_primary"),
                ma_tt43=row.get("ma_tt43"),
                ma_lien_thong_bhyt=row.get("ma_lien_thong_bhyt"),
            )
            classification_id = self._classification_id(
                row.get("canonical_category_code"),
                row.get("canonical_category_name"),
                row.get("canonical_specialty_name"),
            )
            if classification_id:
                session.run(
                    """
                    MERGE (cls:ServiceClassification {classification_id:$classification_id})
                    ON CREATE SET
                        cls.category_code = $category_code,
                        cls.category_name = $category_name,
                        cls.name = $name
                    ON MATCH SET
                        cls.category_code = coalesce(cls.category_code, $category_code),
                        cls.category_name = coalesce(cls.category_name, $category_name),
                        cls.name = coalesce(cls.name, $name)
                    """,
                    classification_id=classification_id,
                    category_code=row.get("canonical_category_code"),
                    category_name=row.get("canonical_category_name"),
                    name=row.get("canonical_specialty_name") or row.get("canonical_category_name"),
                )
                session.run(
                    """
                    MATCH (cs:CanonicalService {maanhxa:$maanhxa})
                    MATCH (cls:ServiceClassification {classification_id:$classification_id})
                    MERGE (cs)-[:CLASSIFIED_AS]->(cls)
                    """,
                    maanhxa=canonical_maanhxa,
                    classification_id=classification_id,
                )
            if row.get("canonical_price_vnd") is not None:
                variant_id = f"pv:{canonical_maanhxa}:tt39"
                session.run(
                    """
                    MERGE (pv:PriceVariant {variant_id:$variant_id})
                    ON CREATE SET
                        pv.source = 'service_codebook',
                        pv.maanhxa = $maanhxa,
                        pv.gia = $gia,
                        pv.ma_tt43 = $ma_tt43
                    ON MATCH SET
                        pv.gia = coalesce(pv.gia, $gia),
                        pv.ma_tt43 = coalesce(pv.ma_tt43, $ma_tt43)
                    """,
                    variant_id=variant_id,
                    maanhxa=canonical_maanhxa,
                    gia=row.get("canonical_price_vnd"),
                    ma_tt43=row.get("ma_tt43"),
                )
                session.run(
                    """
                    MATCH (cs:CanonicalService {maanhxa:$maanhxa})
                    MATCH (pv:PriceVariant {variant_id:$variant_id})
                    MERGE (cs)-[:HAS_PRICE_VARIANT]->(pv)
                    """,
                    maanhxa=canonical_maanhxa,
                    variant_id=variant_id,
                )
            session.run(
                """
                MATCH (row:HospitalServiceRow {row_uid:$row_uid})
                MATCH (cs:CanonicalService {maanhxa:$maanhxa})
                MERGE (row)-[rel:MAPS_TO_CANONICAL]->(cs)
                SET rel.confidence = $confidence,
                    rel.method = $method,
                    rel.updated_at = $updated_at
                """,
                row_uid=row.get("row_uid"),
                maanhxa=canonical_maanhxa,
                confidence=row.get("mapped_confidence") or ("DIRECT" if row.get("direct_maanhxa") else ""),
                method=bridge_method,
                updated_at=row.get("updated_at"),
            )
            if mapped_service_code and review_status != "needs_review" and bridge_method != "manual_review":
                session.run(
                    """
                    MATCH (svc:CIService {service_code:$service_code})
                    MATCH (cs:CanonicalService {maanhxa:$maanhxa})
                    MERGE (svc)-[rel:MAPS_TO_CANONICAL]->(cs)
                    SET rel.confidence = $confidence,
                        rel.method = $method,
                        rel.updated_at = $updated_at
                    """,
                    service_code=mapped_service_code,
                    maanhxa=canonical_maanhxa,
                    confidence=row.get("mapped_confidence"),
                    method=bridge_method,
                    updated_at=row.get("updated_at"),
                )

    def _classification_id(self, category_code: Any, category_name: Any, specialty_name: Any) -> str:
        key = category_code or category_name or specialty_name
        safe = _ascii_fold(key)
        return f"classification:{safe}" if safe else ""

    def _row_uid(
        self,
        *,
        asset_id: str,
        sheet_name: str,
        row_number: int,
        service_name_raw: str,
        hospital_service_code: str,
    ) -> str:
        raw = "|".join(
            [
                asset_id,
                sheet_name,
                str(row_number),
                service_name_raw.strip(),
                hospital_service_code.strip(),
            ]
        )
        return "hsr:" + hashlib.sha1(raw.encode("utf-8")).hexdigest()[:18]

    def _read_cell(self, row: pd.Series, column_name: str) -> str:
        if not column_name or column_name not in row.index:
            return ""
        value = row.get(column_name)
        if value is None:
            return ""
        if isinstance(value, float) and pd.isna(value):
            return ""
        return str(value).strip()

    def _coerce_jsonable(self, value: Any) -> Any:
        if value is None:
            return None
        if isinstance(value, (str, int, float, bool)):
            if isinstance(value, float) and pd.isna(value):
                return None
            return value
        if pd.isna(value):
            return None
        return str(value)
