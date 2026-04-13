from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .knowledge_registry import KnowledgeRegistry
from .pathway_knowledge_bridge import PathwayKnowledgeBridge


DIRECT_PATHWAY_ROOTS = {
    "protocols",
    "service_tables",
    "insurance_rules",
    "benefit_tables",
    "symptom_tables",
}
CATALOG_ONLY_ROOTS = {"legal_documents"}


@dataclass(slots=True)
class IngestCompiler:
    registry: KnowledgeRegistry
    pathway: PathwayKnowledgeBridge

    def support_matrix(self) -> dict[str, Any]:
        rows: list[dict[str, Any]] = []
        for root in self.registry.config.get("roots", []):
            root_key = root["key"]
            pathway_supported = root_key in DIRECT_PATHWAY_ROOTS or root_key in CATALOG_ONLY_ROOTS
            direct_ingest = root_key in DIRECT_PATHWAY_ROOTS
            rows.append(
                {
                    "root_key": root_key,
                    "graph_target": root.get("graph_target"),
                    "accepted_types": root.get("accepted_types", []),
                    "local_registry": True,
                    "pathway_catalog": pathway_supported,
                    "pathway_direct_ingest": direct_ingest,
                    "recommended_engine": (
                        "pathway_direct_ingest"
                        if direct_ingest
                        else "pathway_catalog_only"
                        if pathway_supported
                        else "local_registry_only"
                    ),
                    "notes_vi": self._notes_for_root(root_key),
                }
            )
        return {"rows": rows}

    def upload_and_bridge(
        self,
        *,
        root_key: str,
        filename: str,
        content: bytes,
        auto_ingest: bool = False,
        namespace: str = "ontology_v2",
        source_type: str | None = None,
        title: str | None = None,
        wait_for_completion: bool = False,
    ) -> dict[str, Any]:
        self.registry.register_upload(root_key=root_key, filename=filename, content=content)
        local_asset = self.registry.find_asset(root_key=root_key, filename=filename)
        if local_asset is None:
            raise RuntimeError(f"Khong tim thay local asset sau upload: {filename}")

        pathway_result = None
        if root_key in DIRECT_PATHWAY_ROOTS or root_key in CATALOG_ONLY_ROOTS:
            file_path = self.registry.project_root / local_asset["path"]
            pathway_result = self.pathway.upload_asset(
                root_key=root_key,
                file_path=file_path,
                auto_ingest=auto_ingest,
                namespace=namespace,
                source_type=source_type,
                title=title or local_asset.get("filename") or file_path.stem,
                wait_for_completion=wait_for_completion,
            )

        return {
            "status": "ok",
            "root_key": root_key,
            "local_asset": local_asset,
            "pathway": pathway_result,
            "support": self._notes_for_root(root_key),
        }

    def bridge_existing_asset(
        self,
        *,
        asset_id: str,
        auto_ingest: bool = False,
        namespace: str = "ontology_v2",
        source_type: str | None = None,
        title: str | None = None,
        wait_for_completion: bool = False,
    ) -> dict[str, Any]:
        asset = self.registry.get_asset(asset_id)
        if asset is None:
            raise KeyError(f"Unknown asset_id: {asset_id}")
        root_key = str(asset.get("root_key") or "")
        if root_key not in DIRECT_PATHWAY_ROOTS and root_key not in CATALOG_ONLY_ROOTS:
            raise ValueError(f"Root {root_key} is not bridged to Pathway")
        file_path = self.registry.project_root / asset["path"]
        pathway_result = self.pathway.upload_asset(
            root_key=root_key,
            file_path=file_path,
            auto_ingest=auto_ingest,
            namespace=namespace,
            source_type=source_type,
            title=title or asset.get("filename") or file_path.stem,
            wait_for_completion=wait_for_completion,
        )
        return {
            "status": "ok",
            "local_asset": asset,
            "pathway": pathway_result,
        }

    def _notes_for_root(self, root_key: str) -> list[str]:
        if root_key == "protocols":
            return [
                "Upload vao local registry va day sang Pathway knowledge registry.",
                "Cho phep direct ingest vao ontology_v2 bang pipeline phac do co san.",
            ]
        if root_key == "service_tables":
            return [
                "Upload vao local registry va day sang Pathway.",
                "Cho phep direct ingest bang mapper/service_table scripts co san.",
            ]
        if root_key == "insurance_rules":
            return [
                "First-class ingest: neo4j_ingest_insurance.py tao Insurer, Contract, Benefit, Exclusion, Rulebook nodes.",
                "Bridge: bridge_insurance_service.py lien ket Plan, Category->Benefit, Exclusion patterns.",
                "Chay qua API POST /api/ingest/insurance hoac CLI script truc tiep.",
            ]
        if root_key == "benefit_tables":
            return [
                "First-class ingest: benefit_contract_knowledge_pack.json -> Benefit nodes trong Neo4j.",
                "Lien ket qua bridge_insurance_service.py de map CIService category -> Benefit entries.",
            ]
        if root_key == "symptom_tables":
            return [
                "First-class ingest: tao CISign nodes tu bang trieu chung, lien ket CIDisease->CI_HAS_SIGN.",
                "Du lieu hien co san trong claims_insights_explorer_v1 namespace.",
            ]
        if root_key == "legal_documents":
            return [
                "Hien catalog-only: catalog tai local registry, review va bridge qua Pathway.",
                "Can bo sung parser rieng cho van ban phap ly (PDF docx) de ingest thanh clause nodes.",
            ]
        return [
            "Dang catalog tai local registry.",
            "Can bo sung bridge neu muon dua thang sang Pathway.",
        ]
