"""Neo4j Ingestion Script for Insurance Knowledge Packs.

This script ingests insurance data (contracts, benefits, exclusions, rulebooks)
into Neo4j knowledge graph for use by adjudication agents.

Usage:
    python neo4j_ingest_insurance.py [--clear]

Options:
    --clear: Clear all insurance nodes before ingestion (for full rebuild)
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from neo4j import GraphDatabase

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Paths
PROJECT_DIR = Path(__file__).parent.parent.parent.parent.parent
INSURANCE_DIR = PROJECT_DIR / "notebooklm" / "workspaces" / "claims_insights" / "06_insurance"
CLAIMS_INSIGHTS_DIR = INSURANCE_DIR.parent
INSURANCE_NAMESPACE = "claims_insights_insurance_v1"

# Neo4j connection
NEO4J_URI = "bolt://localhost:7688"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "password123"


class InsuranceIngestionEngine:
    """Engine for ingesting insurance knowledge packs into Neo4j."""

    def __init__(self, uri: str, user: str, password: str):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        self.stats = {
            "contracts": 0,
            "benefits": 0,
            "exclusions": 0,
            "rulebooks": 0,
            "plans": 0,
            "insurers": 0,
            "relationships": 0,
            "errors": []
        }

    def close(self):
        """Close Neo4j driver."""
        if self.driver:
            self.driver.close()

    @staticmethod
    def _now_iso() -> str:
        return datetime.now(timezone.utc).isoformat()

    @staticmethod
    def _stable_id(prefix: str, *parts: object) -> str:
        payload = "||".join(str(part or "").strip() for part in parts)
        digest = hashlib.sha1(payload.encode("utf-8")).hexdigest()[:16]
        return f"{prefix}-{digest}"

    @staticmethod
    def _json_dumps(value: Any) -> str:
        return json.dumps(value, ensure_ascii=False)

    @staticmethod
    def _normalize_contract_key(value: str) -> str:
        lowered = (value or "").strip().lower()
        lowered = re.sub(r"[\s\-_]+", "", lowered)
        return lowered

    @classmethod
    def _canonical_contract_id(cls, value: str) -> str:
        normalized = cls._normalize_contract_key(value)
        canonical_map = {
            "bhv": "BHV",
            "fptnt2025": "FPT-NT-2025",
            "fptnt2024": "FPT-NT-2024",
            "fptnv": "FPT-NV",
            "tcgins": "TCGIns",
            "tinpnc": "TIN-PNC",
            "uic": "UIC",
        }
        return canonical_map.get(normalized, (value or "").strip())

    @staticmethod
    def _load_jsonl(path: Path) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        if not path.exists():
            return rows
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                raw = line.strip()
                if raw:
                    rows.append(json.loads(raw))
        return rows

    @staticmethod
    def _resolve_output_path(relative_path: str | None, fallback: Path | None = None) -> Path | None:
        candidates: list[Path] = []
        if relative_path:
            rel = Path(relative_path)
            candidates.extend(
                [
                    CLAIMS_INSIGHTS_DIR / rel,
                    PROJECT_DIR / rel,
                    INSURANCE_DIR / rel.name,
                ]
            )
        if fallback is not None:
            candidates.append(fallback)
        for candidate in candidates:
            if candidate.exists():
                return candidate
        return fallback if fallback is not None else (candidates[0] if candidates else None)

    def clear_insurance_data(self) -> None:
        """Clear all insurance-related nodes and relationships."""
        with self.driver.session() as session:
            logger.info("Clearing insurance data for namespace=%s ...", INSURANCE_NAMESPACE)

            for label in [
                "InsuranceContract", "Benefit", "Exclusion", "ExclusionReason",
                "Rulebook", "ContractPlan", "Insurer", "ServiceLineCoverage",
                "ServiceExclusion", "ClauseReference", "BenefitInterpretation",
                "ContractRule", "ContractClause", "BenefitDetailEvidence",
            ]:
                session.run(
                    f"""
                    MATCH (n:{label})
                    WHERE n.namespace = $ns
                    DETACH DELETE n
                    """,
                    ns=INSURANCE_NAMESPACE,
                )
                logger.info("  Cleared %s nodes in namespace", label)

            logger.info("Clearance complete")

    def create_indexes_and_constraints(self) -> None:
        """Create indexes and constraints for insurance namespace."""
        with self.driver.session() as session:
            logger.info("Creating indexes and constraints...")

            # Constraints for uniqueness
            constraints = [
                "CREATE CONSTRAINT contract_id_unique IF NOT EXISTS FOR (c:InsuranceContract) REQUIRE c.contract_id IS UNIQUE",
                "CREATE CONSTRAINT benefit_entry_id_unique IF NOT EXISTS FOR (b:Benefit) REQUIRE b.entry_id IS UNIQUE",
                "CREATE CONSTRAINT exclusion_code_unique IF NOT EXISTS FOR (e:Exclusion) REQUIRE e.code IS UNIQUE",
                "CREATE CONSTRAINT rulebook_id_unique IF NOT EXISTS FOR (r:Rulebook) REQUIRE r.rulebook_id IS UNIQUE",
                "CREATE CONSTRAINT plan_name_unique IF NOT EXISTS FOR (p:ContractPlan) REQUIRE p.plan_id IS UNIQUE",
                "CREATE CONSTRAINT insurer_name_unique IF NOT EXISTS FOR (i:Insurer) REQUIRE i.name IS UNIQUE",
                "CREATE CONSTRAINT benefit_interp_entry_id_unique IF NOT EXISTS FOR (bi:BenefitInterpretation) REQUIRE bi.entry_id IS UNIQUE",
                "CREATE CONSTRAINT contract_rule_id_unique IF NOT EXISTS FOR (cr:ContractRule) REQUIRE cr.rule_id IS UNIQUE",
                "CREATE CONSTRAINT contract_clause_id_unique IF NOT EXISTS FOR (cc:ContractClause) REQUIRE cc.clause_id IS UNIQUE",
                "CREATE CONSTRAINT benefit_detail_id_unique IF NOT EXISTS FOR (bd:BenefitDetailEvidence) REQUIRE bd.detail_id IS UNIQUE",
                "CREATE CONSTRAINT clause_reference_id_unique IF NOT EXISTS FOR (cr:ClauseReference) REQUIRE cr.clause_id IS UNIQUE",
                "CREATE CONSTRAINT service_exclusion_id_unique IF NOT EXISTS FOR (se:ServiceExclusion) REQUIRE se.exc_id IS UNIQUE",
                "CREATE CONSTRAINT service_line_coverage_id_unique IF NOT EXISTS FOR (sc:ServiceLineCoverage) REQUIRE sc.cov_id IS UNIQUE",
            ]

            for constraint in constraints:
                try:
                    session.run(constraint)
                    logger.info(f"  Created: {constraint.split('FOR')[1].split('REQUIRE')[0].strip()}")
                except Exception as e:
                    if "already exists" not in str(e).lower():
                        logger.warning(f"  Constraint warning: {e}")

            # Indexes for search performance
            indexes = [
                "CREATE INDEX service_code_idx IF NOT EXISTS FOR (s:Service) ON (s.code)",
                "CREATE INDEX service_canonical_name_idx IF NOT EXISTS FOR (s:Service) ON (s.canonical_name)",
                "CREATE INDEX disease_icd_idx IF NOT EXISTS FOR (d:Disease) ON (d.icd_code)",
                "CREATE INDEX contract_product_name_idx IF NOT EXISTS FOR (c:InsuranceContract) ON (c.product_name)",
                "CREATE INDEX ci_service_code_idx IF NOT EXISTS FOR (s:CIService) ON (s.service_code)",
                "CREATE INDEX benefit_namespace_idx IF NOT EXISTS FOR (b:Benefit) ON (b.namespace)",
                "CREATE INDEX exclusion_namespace_idx IF NOT EXISTS FOR (e:Exclusion) ON (e.namespace)",
                "CREATE INDEX claim_ho_so_idx IF NOT EXISTS FOR (c:Claim) ON (c.ho_so_id)",
            ]

            for index in indexes:
                try:
                    session.run(index)
                    logger.info(f"  Created: {index.split('FOR')[1].split('ON')[0].strip()}")
                except Exception as e:
                    if "already exists" not in str(e).lower():
                        logger.warning(f"  Index warning: {e}")

            logger.info("Indexes and constraints ready")

    def ingest_contract_rules(self, data: dict[str, Any]) -> None:
        """Ingest contract rules from contract_rules.json."""
        logger.info("Ingesting contract rules...")
        ingested_at = self._now_iso()
        source_summary = self._json_dumps(data.get("sources", {}))

        with self.driver.session() as session:
            # Ingest insurers first
            unique_insurers = set()
            for contract in data.get("contract_rules", []):
                insurer = contract.get("insurer", "UNKNOWN")
                if insurer and insurer != "UNKNOWN":
                    unique_insurers.add(insurer)

            for insurer in unique_insurers:
                session.run("""
                    MERGE (i:Insurer {name: $name})
                    SET i.namespace = $namespace,
                        i.ingested_at = $ingested_at
                """, name=insurer, namespace=INSURANCE_NAMESPACE, ingested_at=ingested_at)
                self.stats["insurers"] += 1

            # Ingest contracts
            for contract in data.get("contract_rules", []):
                contract_id = self._canonical_contract_id(contract.get("contract_id", ""))
                product_name = contract.get("product_name") or contract.get("product", "")
                insurer = contract.get("insurer", "UNKNOWN")
                rule_id = f"{contract_id}:core_rule"

                session.run("""
                    MERGE (c:InsuranceContract {contract_id: $contract_id})
                    SET c.namespace = $namespace,
                        c.contract_key = $contract_key,
                        c.product_name = $product_name,
                        c.insurer = $insurer,
                        c.mode = $mode,
                        c.requires_preauth = $requires_preauth,
                        c.positive_result_required = $positive_result_required,
                        c.paid_ratio_pct = $paid_ratio_pct,
                        c.copay_percent = $copay_percent,
                        c.sublimit_per_visit = $sublimit_per_visit,
                        c.source_dataset = 'contract_rules',
                        c.source_summary = $source_summary,
                        c.ingested_at = $ingested_at
                """, contract_id=contract_id, product_name=product_name, insurer=insurer,
                     namespace=INSURANCE_NAMESPACE,
                     contract_key=self._normalize_contract_key(contract_id),
                     mode=contract.get("rule_type", ""),
                     requires_preauth=contract.get("requires_preauth", False),
                     positive_result_required=contract.get("positive_result_required", False),
                     paid_ratio_pct=contract.get("stats", {}).get("paid_ratio_pct", 0.0),
                     copay_percent=contract.get("copay_percent", 0),
                     sublimit_per_visit=contract.get("sublimit_per_visit", 0),
                     source_summary=source_summary,
                     ingested_at=ingested_at)

                session.run("""
                    MERGE (cr:ContractRule {rule_id: $rule_id})
                    SET cr.namespace = $namespace,
                        cr.contract_id = $contract_id,
                        cr.insurer = $insurer,
                        cr.rule_type = $rule_type,
                        cr.requires_preauth = $requires_preauth,
                        cr.positive_result_required = $positive_result_required,
                        cr.copay_percent = $copay_percent,
                        cr.sublimit_per_visit = $sublimit_per_visit,
                        cr.covered_categories = $covered_categories,
                        cr.exclusion_groups = $exclusion_groups,
                        cr.top_denial_reasons_json = $top_denial_reasons_json,
                        cr.top_rules_in_data_json = $top_rules_in_data_json,
                        cr.top_benefits_json = $top_benefits_json,
                        cr.stats_json = $stats_json,
                        cr.source_summary = $source_summary,
                        cr.ingested_at = $ingested_at
                """,
                    rule_id=rule_id,
                    namespace=INSURANCE_NAMESPACE,
                    contract_id=contract_id,
                    insurer=insurer,
                    rule_type=contract.get("rule_type", ""),
                    requires_preauth=contract.get("requires_preauth", False),
                    positive_result_required=contract.get("positive_result_required", False),
                    copay_percent=contract.get("copay_percent", 0),
                    sublimit_per_visit=contract.get("sublimit_per_visit", 0),
                    covered_categories=contract.get("covered_categories", []),
                    exclusion_groups=contract.get("exclusion_groups", []),
                    top_denial_reasons_json=self._json_dumps(contract.get("top_denial_reasons", [])),
                    top_rules_in_data_json=self._json_dumps(contract.get("top_rules_in_data", [])),
                    top_benefits_json=self._json_dumps(contract.get("top_benefits", [])),
                    stats_json=self._json_dumps(contract.get("stats", {})),
                    source_summary=source_summary,
                    ingested_at=ingested_at,
                )

                session.run("""
                    MATCH (c:InsuranceContract {contract_id: $contract_id})
                    MATCH (cr:ContractRule {rule_id: $rule_id})
                    MERGE (c)-[:HAS_CONTRACT_RULE]->(cr)
                """, contract_id=contract_id, rule_id=rule_id)

                # Link to insurer
                session.run("""
                    MATCH (c:InsuranceContract {contract_id: $contract_id})
                    MATCH (i:Insurer {name: $insurer})
                    MERGE (i)-[:ISSUES]->(c)
                """, contract_id=contract_id, insurer=insurer)

                # Ingest exclusion groups as Exclusion nodes
                exclusion_groups = contract.get("exclusion_groups", [])
                for group_name in exclusion_groups:
                    session.run("""
                        MERGE (e:Exclusion {code: $code})
                        SET e.namespace = $namespace,
                            e.name = $name,
                            e.group = $group,
                            e.source_dataset = 'contract_rules_group',
                            e.ingested_at = $ingested_at
                    """, code=f"GRP-{contract_id}-{group_name}", name=group_name, group=group_name,
                         namespace=INSURANCE_NAMESPACE, ingested_at=ingested_at)

                    # Link contract to exclusion
                    session.run("""
                        MATCH (c:InsuranceContract {contract_id: $contract_id})
                        MATCH (e:Exclusion {code: $code})
                        MERGE (c)-[:HAS_EXCLUSION]->(e)
                    """, contract_id=contract_id, code=f"GRP-{contract_id}-{group_name}")
                    self.stats["exclusions"] += 1

                self.stats["contracts"] += 1

        logger.info(f"  Contract rules: {self.stats['contracts']} contracts, {self.stats['insurers']} insurers")

    def ingest_benefit_contract_pack(self, data: dict[str, Any]) -> None:
        """Ingest benefit entries from benefit_contract_knowledge_pack.json."""
        logger.info("Ingesting benefit contract pack...")
        ingested_at = self._now_iso()
        detail_links_path = self._resolve_output_path(
            (data.get("outputs") or {}).get("detail_links_jsonl"),
            INSURANCE_DIR / "benefit_detail_service_links.jsonl",
        )
        detail_rows = self._load_jsonl(detail_links_path) if detail_links_path else []

        with self.driver.session() as session:
            for entry in (data.get("interpretation_catalog") or {}).get("entries", []):
                session.run("""
                    MERGE (bi:BenefitInterpretation {entry_id: $entry_id})
                    SET bi.namespace = $namespace,
                        bi.row_index = $row_index,
                        bi.group_name = $group_name,
                        bi.canonical_name = $canonical_name,
                        bi.aliases = $aliases,
                        bi.definition_text = $definition_text,
                        bi.interpretation_text = $interpretation_text,
                        bi.evidence_hints = $evidence_hints,
                        bi.source_file = $source_file,
                        bi.ingested_at = $ingested_at
                """,
                    entry_id=entry.get("entry_id"),
                    namespace=INSURANCE_NAMESPACE,
                    row_index=entry.get("row_index", 0),
                    group_name=entry.get("group_name", ""),
                    canonical_name=entry.get("canonical_name", ""),
                    aliases=entry.get("aliases", []),
                    definition_text=entry.get("definition_text", ""),
                    interpretation_text=entry.get("interpretation_text", ""),
                    evidence_hints=entry.get("evidence_hints", []),
                    source_file=entry.get("source_file", ""),
                    ingested_at=ingested_at,
                )

            catalog = data.get("contract_catalog", {})
            contracts = catalog.get("contracts", [])

            for contract in contracts:
                contract_id = self._canonical_contract_id(contract.get("contract_id", ""))

                # Ensure contract exists
                session.run("""
                    MERGE (c:InsuranceContract {contract_id: $contract_id})
                    SET c.namespace = $namespace,
                        c.contract_key = $contract_key,
                        c.mode = $mode,
                        c.product_name = coalesce(c.product_name, $product_name),
                        c.source_file = $source_file,
                        c.source_dataset = 'benefit_contract_knowledge_pack',
                        c.ingested_at = $ingested_at
                """,
                    contract_id=contract_id,
                    namespace=INSURANCE_NAMESPACE,
                    contract_key=self._normalize_contract_key(contract_id),
                    mode=contract.get("mode", ""),
                    product_name=contract.get("product_name", ""),
                    source_file=contract.get("source_file", ""),
                    ingested_at=ingested_at,
                )

                # Ingest contract plans
                mode = contract.get("mode", "")
                benefit_entries = contract.get("benefit_entries", [])
                clause_entries = contract.get("condition_clauses", [])

                # Extract unique plans from benefit entries
                plans = set()
                for entry in benefit_entries:
                    coverage_by_plan = entry.get("coverage_by_plan", {})
                    for plan_name in coverage_by_plan.keys():
                        if coverage_by_plan[plan_name]:  # Only if has coverage value
                            plans.add(plan_name)

                for plan_name in plans:
                    plan_id = f"{contract_id}:{plan_name}"
                    session.run("""
                        MERGE (p:ContractPlan {plan_id: $plan_id})
                        SET p.namespace = $namespace,
                            p.name = $plan_name,
                            p.contract_id = $contract_id,
                            p.source_file = $source_file,
                            p.ingested_at = $ingested_at
                    """,
                        plan_id=plan_id,
                        namespace=INSURANCE_NAMESPACE,
                        plan_name=plan_name,
                        contract_id=contract_id,
                        source_file=contract.get("source_file", ""),
                        ingested_at=ingested_at,
                    )

                    # Link contract to plan
                    session.run("""
                        MATCH (c:InsuranceContract {contract_id: $contract_id})
                        MATCH (p:ContractPlan {plan_id: $plan_id})
                        MERGE (c)-[:COVERS_PLAN]->(p)
                    """, contract_id=contract_id, plan_id=plan_id)
                    self.stats["plans"] += 1

                # Ingest benefits
                for entry in benefit_entries:
                    entry_label = entry.get("entry_label", "")
                    major_section = entry.get("major_section", "")
                    subsection = entry.get("subsection", "")
                    coverage_by_plan = entry.get("coverage_by_plan", {})
                    interpretation_match = entry.get("interpretation_match", {})
                    canonical_name = interpretation_match.get("canonical_name", entry_label)
                    interpretation_entry_id = interpretation_match.get("entry_id", "")

                    # Generate entry_id
                    row_index = entry.get("row_index", 0)
                    entry_id = f"BEN-{contract_id}-{row_index}"

                    session.run("""
                        MERGE (b:Benefit {entry_id: $entry_id})
                        SET b.namespace = $namespace,
                            b.entry_label = $entry_label,
                            b.entry_code = $entry_code,
                            b.major_section = $major_section,
                            b.subsection = $subsection,
                            b.canonical_name = $canonical_name,
                            b.contract_id = $contract_id,
                            b.mode = $mode,
                            b.source_file = $source_file,
                            b.sheet_name = $sheet_name,
                            b.row_index = $row_index,
                            b.nonempty_plan_count = $nonempty_plan_count,
                            b.interpretation_entry_id = $interpretation_entry_id,
                            b.interpretation_match_score = $match_score,
                            b.interpretation_match_method = $match_method,
                            b.interpretation_matched_alias = $matched_alias,
                            b.ingested_at = $ingested_at
                    """,
                        entry_id=entry_id,
                        namespace=INSURANCE_NAMESPACE,
                        entry_label=entry_label,
                        entry_code=entry.get("entry_code", ""),
                        major_section=major_section,
                        subsection=subsection,
                        canonical_name=canonical_name,
                        contract_id=contract_id,
                        mode=mode,
                        source_file=entry.get("source_file", contract.get("source_file", "")),
                        sheet_name=entry.get("sheet_name", ""),
                        row_index=row_index,
                        nonempty_plan_count=entry.get("nonempty_plan_count", 0),
                        interpretation_entry_id=interpretation_entry_id,
                        match_score=float(interpretation_match.get("score", 0.0) or 0.0),
                        match_method=interpretation_match.get("method", ""),
                        matched_alias=interpretation_match.get("matched_alias", ""),
                        ingested_at=ingested_at,
                    )

                    # Link contract to benefit
                    session.run("""
                        MATCH (c:InsuranceContract {contract_id: $contract_id})
                        MATCH (b:Benefit {entry_id: $entry_id})
                        MERGE (c)-[:HAS_BENEFIT]->(b)
                    """, contract_id=contract_id, entry_id=entry_id)

                    # Link benefits to plans with coverage
                    for plan_name, coverage in coverage_by_plan.items():
                        if coverage:  # Only if has coverage
                            plan_id = f"{contract_id}:{plan_name}"
                            session.run("""
                                MATCH (b:Benefit {entry_id: $entry_id})
                                MATCH (p:ContractPlan {plan_id: $plan_id})
                                MERGE (p)-[r:COVERS_BENEFIT]->(b)
                                SET r.coverage = $coverage,
                                    r.namespace = $namespace
                            """, entry_id=entry_id, plan_id=plan_id, coverage=coverage, namespace=INSURANCE_NAMESPACE)
                            self.stats["relationships"] += 1

                    if interpretation_entry_id:
                        session.run("""
                            MATCH (b:Benefit {entry_id: $benefit_entry_id})
                            MATCH (bi:BenefitInterpretation {entry_id: $interp_entry_id})
                            MERGE (b)-[r:INTERPRETED_AS]->(bi)
                            SET r.match_score = $match_score,
                                r.match_method = $match_method,
                                r.matched_alias = $matched_alias,
                                r.namespace = $namespace
                        """,
                            benefit_entry_id=entry_id,
                            interp_entry_id=interpretation_entry_id,
                            match_score=float(interpretation_match.get("score", 0.0) or 0.0),
                            match_method=interpretation_match.get("method", ""),
                            matched_alias=interpretation_match.get("matched_alias", ""),
                            namespace=INSURANCE_NAMESPACE,
                        )

                    self.stats["benefits"] += 1

                for clause in clause_entries:
                    clause_id = self._stable_id(
                        "CCLAUSE",
                        contract_id,
                        clause.get("sheet_name", ""),
                        clause.get("row_index", 0),
                        clause.get("clause_code", ""),
                        clause.get("clause_title", ""),
                    )
                    session.run("""
                        MERGE (cc:ContractClause {clause_id: $clause_id})
                        SET cc.namespace = $namespace,
                            cc.contract_id = $contract_id,
                            cc.source_file = $source_file,
                            cc.sheet_name = $sheet_name,
                            cc.row_index = $row_index,
                            cc.section = $section,
                            cc.clause_code = $clause_code,
                            cc.clause_title = $clause_title,
                            cc.clause_body = $clause_body,
                            cc.ingested_at = $ingested_at
                    """,
                        clause_id=clause_id,
                        namespace=INSURANCE_NAMESPACE,
                        contract_id=contract_id,
                        source_file=clause.get("source_file", contract.get("source_file", "")),
                        sheet_name=clause.get("sheet_name", ""),
                        row_index=clause.get("row_index", 0),
                        section=clause.get("section", ""),
                        clause_code=clause.get("clause_code", ""),
                        clause_title=clause.get("clause_title", ""),
                        clause_body=clause.get("clause_body", ""),
                        ingested_at=ingested_at,
                    )
                    session.run("""
                        MATCH (c:InsuranceContract {contract_id: $contract_id})
                        MATCH (cc:ContractClause {clause_id: $clause_id})
                        MERGE (c)-[:HAS_CLAUSE]->(cc)
                    """, contract_id=contract_id, clause_id=clause_id)

            support_aggregate: dict[tuple[str, str], dict[str, Any]] = {}
            for row in detail_rows:
                detail_id = row.get("detail_id", "")
                claim_id = row.get("claim_id", "")
                interpretation = row.get("benefit_interpretation_match", {}) or {}
                service_mapping = row.get("service_mapping", {}) or {}
                service_code = service_mapping.get("service_code", "")
                interpretation_entry_id = interpretation.get("entry_id", "")
                status = row.get("status", "")

                session.run("""
                    MERGE (bd:BenefitDetailEvidence {detail_id: $detail_id})
                    SET bd.namespace = $namespace,
                        bd.claim_id = $claim_id,
                        bd.benefit_code = $benefit_code,
                        bd.benefit_name = $benefit_name,
                        bd.detail_name = $detail_name,
                        bd.detail_amount_vnd = $detail_amount_vnd,
                        bd.requested_amount_vnd = $requested_amount_vnd,
                        bd.claim_total_vnd = $claim_total_vnd,
                        bd.amount_gap_vnd = $amount_gap_vnd,
                        bd.status = $status,
                        bd.source_file = $source_file,
                        bd.interpretation_entry_id = $interpretation_entry_id,
                        bd.interpretation_canonical_name = $interpretation_canonical_name,
                        bd.service_code = $service_code,
                        bd.service_name = $service_name,
                        bd.category_code = $category_code,
                        bd.mapping_status = $mapping_status,
                        bd.mapping_confidence = $mapping_confidence,
                        bd.matched_variant = $matched_variant,
                        bd.clinical_links_json = $clinical_links_json,
                        bd.ingested_at = $ingested_at
                """,
                    detail_id=detail_id,
                    namespace=INSURANCE_NAMESPACE,
                    claim_id=claim_id,
                    benefit_code=row.get("benefit_code", ""),
                    benefit_name=row.get("benefit_name", ""),
                    detail_name=row.get("detail_name", ""),
                    detail_amount_vnd=int(row.get("detail_amount_vnd", 0) or 0),
                    requested_amount_vnd=int(row.get("requested_amount_vnd", 0) or 0),
                    claim_total_vnd=int(row.get("claim_total_vnd", 0) or 0),
                    amount_gap_vnd=int(row.get("amount_gap_vnd", 0) or 0),
                    status=status,
                    source_file=str(detail_links_path.relative_to(PROJECT_DIR)) if detail_links_path else "",
                    interpretation_entry_id=interpretation_entry_id,
                    interpretation_canonical_name=interpretation.get("canonical_name", ""),
                    service_code=service_code,
                    service_name=service_mapping.get("canonical_name", ""),
                    category_code=service_mapping.get("category_code", ""),
                    mapping_status=service_mapping.get("mapping_status", ""),
                    mapping_confidence=service_mapping.get("mapper_confidence", ""),
                    matched_variant=service_mapping.get("matched_variant", ""),
                    clinical_links_json=self._json_dumps(row.get("clinical_links", {})),
                    ingested_at=ingested_at,
                )

                if claim_id:
                    session.run("""
                        MERGE (cl:Claim {ho_so_id: $claim_id})
                        SET cl.namespace = coalesce(cl.namespace, $namespace),
                            cl.claim_id = coalesce(cl.claim_id, $claim_id)
                    """, claim_id=claim_id, namespace=INSURANCE_NAMESPACE)
                    session.run("""
                        MATCH (cl:Claim {ho_so_id: $claim_id})
                        MATCH (bd:BenefitDetailEvidence {detail_id: $detail_id})
                        MERGE (cl)-[:HAS_BENEFIT_DETAIL]->(bd)
                    """, claim_id=claim_id, detail_id=detail_id)

                if interpretation_entry_id:
                    session.run("""
                        MATCH (bd:BenefitDetailEvidence {detail_id: $detail_id})
                        MATCH (bi:BenefitInterpretation {entry_id: $entry_id})
                        MERGE (bd)-[:ALLOCATED_TO_INTERPRETATION]->(bi)
                    """, detail_id=detail_id, entry_id=interpretation_entry_id)

                if service_code:
                    session.run("""
                        MATCH (bd:BenefitDetailEvidence {detail_id: $detail_id})
                        MATCH (svc:CIService {service_code: $service_code})
                        MERGE (bd)-[r:DETAIL_MATCHED_SERVICE]->(svc)
                        SET r.namespace = $namespace,
                            r.mapping_status = $mapping_status,
                            r.mapping_confidence = $mapping_confidence
                    """,
                        detail_id=detail_id,
                        service_code=service_code,
                        namespace=INSURANCE_NAMESPACE,
                        mapping_status=service_mapping.get("mapping_status", ""),
                        mapping_confidence=service_mapping.get("mapper_confidence", ""),
                    )

                if service_code and interpretation_entry_id:
                    key = (service_code, interpretation_entry_id)
                    bucket = support_aggregate.setdefault(
                        key,
                        {
                            "rows": 0,
                            "claim_ids": set(),
                            "detail_amount_vnd": 0,
                            "statuses": {},
                        },
                    )
                    bucket["rows"] += 1
                    if claim_id:
                        bucket["claim_ids"].add(claim_id)
                    bucket["detail_amount_vnd"] += int(row.get("detail_amount_vnd", 0) or 0)
                    bucket["statuses"][status or "(trong)"] = bucket["statuses"].get(status or "(trong)", 0) + 1

            for (service_code, interpretation_entry_id), bucket in support_aggregate.items():
                session.run("""
                    MATCH (svc:CIService {service_code: $service_code})
                    MATCH (bi:BenefitInterpretation {entry_id: $entry_id})
                    MERGE (svc)-[r:SUPPORTED_BY_BENEFIT]->(bi)
                    SET r.namespace = $namespace,
                        r.support_rows = $support_rows,
                        r.claim_count = $claim_count,
                        r.total_detail_amount_vnd = $total_detail_amount_vnd,
                        r.status_distribution_json = $status_distribution_json,
                        r.source = 'benefit_detail_service_links'
                """,
                    service_code=service_code,
                    entry_id=interpretation_entry_id,
                    namespace=INSURANCE_NAMESPACE,
                    support_rows=bucket["rows"],
                    claim_count=len(bucket["claim_ids"]),
                    total_detail_amount_vnd=bucket["detail_amount_vnd"],
                    status_distribution_json=self._json_dumps(bucket["statuses"]),
                )

        logger.info(
            "  Benefit pack: %s benefits, %s plans, %s detail evidence rows",
            self.stats["benefits"],
            self.stats["plans"],
            len(detail_rows),
        )

    def ingest_exclusion_knowledge_pack(self, data: dict[str, Any]) -> None:
        """Ingest exclusion knowledge pack into Neo4j."""
        logger.info("Ingesting exclusion knowledge pack...")
        ingested_at = self._now_iso()
        reason_usage = data.get("reason_usage", [])
        clause_refs = data.get("clause_reference_index", [])
        linked_mentions_path = self._resolve_output_path(
            (data.get("outputs") or {}).get("combined_linked_mentions_jsonl"),
            INSURANCE_DIR / "combined_exclusion_note_mentions_linked.jsonl",
        )
        linked_mentions = self._load_jsonl(linked_mentions_path) if linked_mentions_path else []
        reason_to_code = {
            item.get("reason", ""): item.get("code", "")
            for item in reason_usage
            if item.get("reason") and item.get("code")
        }

        with self.driver.session() as session:
            for item in reason_usage:
                code = item.get("code")
                reason = item.get("reason", "")
                group = item.get("group", "")
                process_path = item.get("process_path", "")
                if not code:
                    continue

                session.run("""
                    MERGE (e:Exclusion {code: $code})
                    SET e.namespace = $namespace,
                        e.reason = $reason,
                        e.group = $group,
                        e.process_path = $process_path,
                        e.usage_main_rows = $main_rows,
                        e.usage_outpatient_rows = $outpatient_rows,
                        e.usage_total_rows = $total_rows,
                        e.main_gap_vnd = $main_gap_vnd,
                        e.outpatient_gap_vnd = $outpatient_gap_vnd,
                        e.usage_gap_vnd = $gap_vnd,
                        e.top_contracts_json = $top_contracts_json,
                        e.top_rules_json = $top_rules_json,
                        e.source_dataset = 'exclusion_knowledge_pack',
                        e.source_summary = $source_summary,
                        e.ingested_at = $ingested_at
                """,
                    code=code,
                    namespace=INSURANCE_NAMESPACE,
                    reason=reason,
                    group=group,
                    process_path=process_path,
                    main_rows=int(item.get("main_rows", 0) or 0),
                    outpatient_rows=int(item.get("outpatient_rows", 0) or 0),
                    total_rows=int(item.get("total_rows", 0) or 0),
                    main_gap_vnd=int(item.get("main_gap_vnd", 0) or 0),
                    outpatient_gap_vnd=int(item.get("outpatient_gap_vnd", 0) or 0),
                    gap_vnd=int(item.get("total_gap_vnd", 0) or 0),
                    top_contracts_json=self._json_dumps(item.get("top_contracts", [])),
                    top_rules_json=self._json_dumps(item.get("top_rules", [])),
                    source_summary=self._json_dumps(data.get("sources", {})),
                    ingested_at=ingested_at,
                )

                reason_id = f"EXC-REASON-{code}"
                session.run("""
                    MERGE (er:ExclusionReason {reason_id: $reason_id})
                    SET er.namespace = $namespace,
                        er.text = $reason,
                        er.code = $code,
                        er.group = $group,
                        er.ingested_at = $ingested_at
                """,
                    reason_id=reason_id,
                    namespace=INSURANCE_NAMESPACE,
                    reason=reason,
                    code=code,
                    group=group,
                    ingested_at=ingested_at,
                )

                session.run("""
                    MATCH (e:Exclusion {code: $code})
                    MATCH (er:ExclusionReason {reason_id: $reason_id})
                    MERGE (e)-[:HAS_REASON]->(er)
                """, code=code, reason_id=reason_id)

                for contract_ref in item.get("top_contracts", [])[:10]:
                    contract_name = contract_ref.get("contract_name", "")
                    contract_rows = int(contract_ref.get("rows", 0) or 0)
                    if not contract_name:
                        continue
                    session.run("""
                        MATCH (e:Exclusion {code: $code})
                        MATCH (c:InsuranceContract)
                        WHERE coalesce(c.namespace, '') = $namespace
                          AND c.contract_key = $contract_key
                        MERGE (c)-[r:HAS_EXCLUSION]->(e)
                        SET r.namespace = $namespace,
                            r.source = 'exclusion_top_contracts',
                            r.rows = coalesce(r.rows, 0) + $rows
                    """,
                        code=code,
                        namespace=INSURANCE_NAMESPACE,
                        contract_key=self._normalize_contract_key(contract_name),
                        rows=contract_rows,
                    )
                self.stats["exclusions"] += 1

            for ref_item in clause_refs[:250]:
                rule_name = ref_item.get("rule_name", "")
                clause_ref = ref_item.get("clause_reference", "")
                if not clause_ref:
                    continue
                clause_id = self._stable_id("CLAUSE", rule_name, clause_ref)
                session.run("""
                    MERGE (cr:ClauseReference {clause_id: $clause_id})
                    SET cr.namespace = $namespace,
                        cr.rule_name = $rule_name,
                        cr.clause_reference = $clause_reference,
                        cr.rows = $rows,
                        cr.gap_vnd = $gap_vnd,
                        cr.top_reasons_json = $top_reasons_json,
                        cr.ingested_at = $ingested_at
                """,
                    clause_id=clause_id,
                    namespace=INSURANCE_NAMESPACE,
                    rule_name=rule_name,
                    clause_reference=clause_ref,
                    rows=int(ref_item.get("rows", 0) or 0),
                    gap_vnd=int(ref_item.get("gap_sum_vnd", 0) or 0),
                    top_reasons_json=self._json_dumps(ref_item.get("top_reasons", [])),
                    ingested_at=ingested_at,
                )

                for reason_item in ref_item.get("top_reasons", [])[:5]:
                    reason_text = reason_item.get("reason", "")
                    if not reason_text:
                        continue
                    session.run("""
                        MATCH (cr:ClauseReference {clause_id: $clause_id})
                        MATCH (er:ExclusionReason)
                        WHERE er.text = $reason_text
                        MERGE (cr)-[:LINKS_TO]->(er)
                    """, clause_id=clause_id, reason_text=reason_text)

            service_exclusion_agg: dict[tuple[str, str], dict[str, Any]] = {}
            service_contract_exclusion_agg: dict[tuple[str, str, str], dict[str, Any]] = {}
            for row in linked_mentions:
                service_mapping = row.get("service_mapping", {}) or {}
                service_code = service_mapping.get("service_code", "")
                if not service_code:
                    continue
                atomic_reasons = row.get("atomic_reasons", []) or []
                for reason_text in atomic_reasons:
                    code = reason_to_code.get(reason_text, "")
                    if not code:
                        continue
                    key = (service_code, code)
                    bucket = service_exclusion_agg.setdefault(
                        key,
                        {
                            "rows": 0,
                            "contracts": {},
                            "rules": {},
                            "datasets": {},
                            "reasons": {},
                        },
                    )
                    bucket["rows"] += 1
                    contract_name = row.get("contract_name", "") or "(trong)"
                    rule_name = row.get("rule_name", "") or "(trong)"
                    dataset = row.get("source_dataset", "") or "(trong)"
                    bucket["contracts"][contract_name] = bucket["contracts"].get(contract_name, 0) + 1
                    bucket["rules"][rule_name] = bucket["rules"].get(rule_name, 0) + 1
                    bucket["datasets"][dataset] = bucket["datasets"].get(dataset, 0) + 1
                    bucket["reasons"][reason_text] = bucket["reasons"].get(reason_text, 0) + 1

                    contract_id = self._canonical_contract_id(contract_name)
                    if contract_id:
                        contract_key = (service_code, code, contract_id)
                        contract_bucket = service_contract_exclusion_agg.setdefault(
                            contract_key,
                            {
                                "rows": 0,
                                "raw_contract_names": {},
                                "rules": {},
                                "datasets": {},
                                "reasons": {},
                            },
                        )
                        contract_bucket["rows"] += 1
                        contract_bucket["raw_contract_names"][contract_name] = (
                            contract_bucket["raw_contract_names"].get(contract_name, 0) + 1
                        )
                        contract_bucket["rules"][rule_name] = contract_bucket["rules"].get(rule_name, 0) + 1
                        contract_bucket["datasets"][dataset] = contract_bucket["datasets"].get(dataset, 0) + 1
                        contract_bucket["reasons"][reason_text] = contract_bucket["reasons"].get(reason_text, 0) + 1

            for (service_code, exclusion_code), bucket in service_exclusion_agg.items():
                service_exc_id = self._stable_id("SEXC", service_code, exclusion_code)
                session.run("""
                    MERGE (se:ServiceExclusion {exc_id: $exc_id})
                    SET se.namespace = $namespace,
                        se.service_code = $service_code,
                        se.exclusion_code = $exclusion_code,
                        se.evidence_rows = $rows,
                        se.reason_texts = $reason_texts,
                        se.contract_distribution_json = $contract_distribution_json,
                        se.rule_distribution_json = $rule_distribution_json,
                        se.dataset_distribution_json = $dataset_distribution_json,
                        se.source_file = $source_file,
                        se.ingested_at = $ingested_at
                """,
                    exc_id=service_exc_id,
                    namespace=INSURANCE_NAMESPACE,
                    service_code=service_code,
                    exclusion_code=exclusion_code,
                    rows=bucket["rows"],
                    reason_texts=sorted(bucket["reasons"].keys()),
                    contract_distribution_json=self._json_dumps(bucket["contracts"]),
                    rule_distribution_json=self._json_dumps(bucket["rules"]),
                    dataset_distribution_json=self._json_dumps(bucket["datasets"]),
                    source_file=str(linked_mentions_path.relative_to(PROJECT_DIR)) if linked_mentions_path else "",
                    ingested_at=ingested_at,
                )

                session.run("""
                    MATCH (svc:CIService {service_code: $service_code})
                    MATCH (se:ServiceExclusion {exc_id: $exc_id})
                    MERGE (svc)-[:HAS_SERVICE_EXCLUSION]->(se)
                """, service_code=service_code, exc_id=service_exc_id)

                session.run("""
                    MATCH (se:ServiceExclusion {exc_id: $exc_id})
                    MATCH (e:Exclusion {code: $exclusion_code})
                    MERGE (se)-[:MATCHES_EXCLUSION]->(e)
                """, exc_id=service_exc_id, exclusion_code=exclusion_code)

                session.run("""
                    MATCH (svc:CIService {service_code: $service_code})
                    MATCH (e:Exclusion {code: $exclusion_code})
                    MERGE (svc)-[r:EXCLUDED_BY]->(e)
                    SET r.namespace = $namespace,
                        r.rows = $rows,
                        r.reason_texts = $reason_texts,
                        r.contract_distribution_json = $contract_distribution_json,
                        r.rule_distribution_json = $rule_distribution_json,
                        r.source = 'combined_exclusion_note_mentions_linked'
                """,
                    service_code=service_code,
                    exclusion_code=exclusion_code,
                    namespace=INSURANCE_NAMESPACE,
                    rows=bucket["rows"],
                    reason_texts=sorted(bucket["reasons"].keys()),
                    contract_distribution_json=self._json_dumps(bucket["contracts"]),
                    rule_distribution_json=self._json_dumps(bucket["rules"]),
                )
                self.stats["relationships"] += 1

            for (service_code, exclusion_code, contract_id), bucket in service_contract_exclusion_agg.items():
                session.run("""
                    MATCH (svc:CIService {service_code: $service_code})
                    MATCH (e:Exclusion {code: $exclusion_code})
                    MATCH (c:InsuranceContract {contract_id: $contract_id})
                    WHERE coalesce(c.namespace, '') = $namespace
                    MERGE (svc)-[r:EXCLUDED_BY_CONTRACT {contract_id: $contract_id}]->(e)
                    SET r.namespace = $namespace,
                        r.rows = $rows,
                        r.reason_texts = $reason_texts,
                        r.rule_distribution_json = $rule_distribution_json,
                        r.dataset_distribution_json = $dataset_distribution_json,
                        r.contract_name_distribution_json = $contract_name_distribution_json,
                        r.source = 'combined_exclusion_note_mentions_linked'
                    MERGE (c)-[rc:EXCLUDES_SERVICE {exclusion_code: $exclusion_code}]->(svc)
                    SET rc.namespace = $namespace,
                        rc.rows = $rows,
                        rc.reason_texts = $reason_texts,
                        rc.rule_distribution_json = $rule_distribution_json,
                        rc.dataset_distribution_json = $dataset_distribution_json,
                        rc.source = 'combined_exclusion_note_mentions_linked'
                """,
                    service_code=service_code,
                    exclusion_code=exclusion_code,
                    contract_id=contract_id,
                    namespace=INSURANCE_NAMESPACE,
                    rows=bucket["rows"],
                    reason_texts=sorted(bucket["reasons"].keys()),
                    rule_distribution_json=self._json_dumps(bucket["rules"]),
                    dataset_distribution_json=self._json_dumps(bucket["datasets"]),
                    contract_name_distribution_json=self._json_dumps(bucket["raw_contract_names"]),
                )
                self.stats["relationships"] += 2

        logger.info(
            "  Exclusion pack: %s exclusions, %s clause refs, %s service exclusion links, %s contract-aware service exclusions",
            self.stats["exclusions"],
            len(clause_refs),
            len(service_exclusion_agg),
            len(service_contract_exclusion_agg),
        )

    def ingest_rulebook_policy_pack(self, data: dict[str, Any]) -> None:
        """Ingest rulebook policy pack into Neo4j."""
        logger.info("Ingesting rulebook policy pack...")
        ingested_at = self._now_iso()

        with self.driver.session() as session:
            items = data.get("items", [])

            for item in items:
                rulebook_id = item.get("rulebook_id")
                insurer = item.get("insurer", "")
                rule_code = item.get("rule_code", "")
                display_name = item.get("display_name", "")
                source_file = item.get("source_file", "")
                text_output = item.get("text_output", "")
                asset = item.get("asset", {})
                structure = item.get("structure", {})
                claim_evidence = item.get("claim_evidence", {})

                # Create rulebook node
                session.run("""
                    MERGE (r:Rulebook {rulebook_id: $rulebook_id})
                    SET r.namespace = $namespace,
                        r.insurer = $insurer,
                        r.rule_code = $rule_code,
                        r.display_name = $display_name,
                        r.source_file = $source_file,
                        r.text_output = $text_output,
                        r.page_count = $page_count,
                        r.text_extractable_pages = $text_extractable_pages,
                        r.ocr_status = $ocr_status,
                        r.total_claim_evidence_rows = $claim_rows,
                        r.claim_evidence_json = $claim_evidence_json,
                        r.ingested_at = $ingested_at
                """, rulebook_id=rulebook_id, insurer=insurer, rule_code=rule_code,
                     namespace=INSURANCE_NAMESPACE,
                     display_name=display_name, source_file=source_file, text_output=text_output,
                     page_count=asset.get("page_count", 0),
                     text_extractable_pages=asset.get("text_extractable_pages", 0),
                     ocr_status=asset.get("ocr_status", ""),
                     claim_rows=claim_evidence.get("rows", 0),
                     claim_evidence_json=self._json_dumps(claim_evidence),
                     ingested_at=ingested_at)

                # Link to insurer
                session.run("""
                    MATCH (r:Rulebook {rulebook_id: $rulebook_id})
                    MERGE (i:Insurer {name: $insurer})
                    SET i.namespace = coalesce(i.namespace, $namespace)
                    MERGE (i)-[:HAS_RULEBOOK]->(r)
                """, rulebook_id=rulebook_id, insurer=insurer, namespace=INSURANCE_NAMESPACE)

                top_contracts = (claim_evidence or {}).get("top_contracts", []) or []
                linked_contract = False
                for contract_ref in top_contracts[:10]:
                    contract_name = contract_ref.get("contract_name", "")
                    if not contract_name:
                        continue
                    session.run("""
                        MATCH (r:Rulebook {rulebook_id: $rulebook_id})
                        MATCH (c:InsuranceContract)
                        WHERE coalesce(c.namespace, '') = $namespace
                          AND c.contract_key = $contract_key
                        MERGE (c)-[rel:HAS_RULEBOOK]->(r)
                        SET rel.namespace = $namespace,
                            rel.source = 'rulebook_claim_evidence',
                            rel.rows = coalesce(rel.rows, 0) + $rows
                    """,
                        rulebook_id=rulebook_id,
                        namespace=INSURANCE_NAMESPACE,
                        contract_key=self._normalize_contract_key(contract_name),
                        rows=int(contract_ref.get("rows", 0) or 0),
                    )
                    linked_contract = True

                if not linked_contract:
                    session.run("""
                        MATCH (r:Rulebook {rulebook_id: $rulebook_id})
                        MATCH (c:InsuranceContract {insurer: $insurer})
                        WHERE coalesce(c.namespace, '') = $namespace
                        MERGE (c)-[rel:HAS_RULEBOOK]->(r)
                        SET rel.namespace = $namespace,
                            rel.source = 'rulebook_insurer_fallback'
                    """, rulebook_id=rulebook_id, insurer=insurer, namespace=INSURANCE_NAMESPACE)

                # Store chapter structure
                chapters = structure.get("chapters", [])
                for chapter in chapters:
                    chapter_title = chapter.get("chapter_title", "")
                    chapter_num = chapter.get("chapter_count", 0) or chapter.get("chapter_number", 0)
                    clauses = chapter.get("clauses", [])

                    chapter_id = self._stable_id("RCH", rulebook_id, chapter_num, chapter_title)

                    session.run("""
                        MATCH (r:Rulebook {rulebook_id: $rulebook_id})
                        MERGE (ch:RulebookChapter {chapter_id: $chapter_id})
                        SET ch.namespace = $namespace,
                            ch.title = $title,
                            ch.chapter_number = $chapter_num
                        MERGE (r)-[:HAS_CHAPTER]->(ch)
                    """, rulebook_id=rulebook_id, chapter_id=chapter_id, title=chapter_title,
                         namespace=INSURANCE_NAMESPACE, chapter_num=chapter_num)

                    for clause in clauses:
                        clause_num = clause.get("clause_number", "")
                        clause_title = clause.get("clause_title", "")
                        clause_body = clause.get("clause_body", "")

                        clause_id = self._stable_id("RCL", chapter_id, clause_num, clause_title)

                        session.run("""
                            MATCH (ch:RulebookChapter {chapter_id: $chapter_id})
                            MERGE (cl:RulebookClause {clause_id: $clause_id})
                            SET cl.namespace = $namespace,
                                cl.number = $number,
                                cl.title = $title,
                                cl.body = $body
                            MERGE (ch)-[:HAS_CLAUSE]->(cl)
                        """, chapter_id=chapter_id, clause_id=clause_id, number=clause_num,
                             namespace=INSURANCE_NAMESPACE, title=clause_title, body=clause_body[:2000])

                self.stats["rulebooks"] += 1

        logger.info(f"  Rulebook pack: {self.stats['rulebooks']} rulebooks")

    def ingest_service_links(self, benefit_pack: dict[str, Any], exclusion_pack: dict[str, Any]) -> None:
        """Derive compact service coverage markers from already ingested evidence."""
        logger.info("Refreshing compact service coverage markers...")
        with self.driver.session() as session:
            result = session.run("""
                MATCH (svc:CIService)-[r:SUPPORTED_BY_BENEFIT]->(bi:BenefitInterpretation)
                RETURN svc.service_code AS service_code,
                       svc.service_name AS service_name,
                       sum(coalesce(r.support_rows, 0)) AS support_rows
            """).data()

            for row in result:
                service_code = row.get("service_code", "")
                if not service_code:
                    continue
                cov_id = self._stable_id("SCOV", service_code)
                session.run("""
                    MERGE (sc:ServiceLineCoverage {cov_id: $cov_id})
                    SET sc.namespace = $namespace,
                        sc.service_code = $service_code,
                        sc.canonical_name = $canonical_name,
                        sc.claim_rows = $rows,
                        sc.ingested_at = $ingested_at
                """,
                    cov_id=cov_id,
                    namespace=INSURANCE_NAMESPACE,
                    service_code=service_code,
                    canonical_name=row.get("service_name", ""),
                    rows=int(row.get("support_rows", 0) or 0),
                    ingested_at=self._now_iso(),
                )
                self.stats["relationships"] += 1

        logger.info("  Service coverage markers refreshed: %s", len(result))

    def print_summary(self) -> None:
        """Print ingestion summary."""
        logger.info("=" * 50)
        logger.info("INGESTION SUMMARY")
        logger.info("=" * 50)
        logger.info(f"Contracts:     {self.stats['contracts']}")
        logger.info(f"Benefits:      {self.stats['benefits']}")
        logger.info(f"Exclusions:    {self.stats['exclusions']}")
        logger.info(f"Rulebooks:     {self.stats['rulebooks']}")
        logger.info(f"Plans:         {self.stats['plans']}")
        logger.info(f"Insurers:      {self.stats['insurers']}")
        logger.info(f"Relationships: {self.stats['relationships']}")

        if self.stats["errors"]:
            logger.warning(f"Errors: {len(self.stats['errors'])}")
            for error in self.stats["errors"][:10]:
                logger.warning(f"  - {error}")

        logger.info("=" * 50)


def main():
    parser = argparse.ArgumentParser(description="Ingest insurance knowledge packs into Neo4j")
    parser.add_argument("--clear", action="store_true", help="Clear existing insurance data before ingestion")
    args = parser.parse_args()

    engine = None
    try:
        engine = InsuranceIngestionEngine(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD)

        if args.clear:
            engine.clear_insurance_data()

        # Create indexes and constraints
        engine.create_indexes_and_constraints()

        # Load knowledge packs
        contract_rules_path = INSURANCE_DIR / "contract_rules.json"
        benefit_pack_path = INSURANCE_DIR / "benefit_contract_knowledge_pack.json"
        exclusion_pack_path = INSURANCE_DIR / "exclusion_knowledge_pack.json"
        rulebook_pack_path = INSURANCE_DIR / "rulebook_policy_pack.json"

        # Check which files exist
        packs_to_ingest = []

        if contract_rules_path.exists():
            packs_to_ingest.append(("contract_rules", contract_rules_path))
        else:
            logger.warning(f"File not found: {contract_rules_path}")

        if benefit_pack_path.exists():
            packs_to_ingest.append(("benefit_contract", benefit_pack_path))
        else:
            logger.warning(f"File not found: {benefit_pack_path}")

        if exclusion_pack_path.exists():
            packs_to_ingest.append(("exclusion_knowledge", exclusion_pack_path))
        else:
            logger.warning(f"File not found: {exclusion_pack_path}")

        if rulebook_pack_path.exists():
            packs_to_ingest.append(("rulebook_policy", rulebook_pack_path))
        else:
            logger.warning(f"File not found: {rulebook_pack_path}")

        # Ingest each pack
        benefit_data = None
        exclusion_data = None

        for pack_name, pack_path in packs_to_ingest:
            logger.info(f"Loading {pack_name} from {pack_path}")
            with pack_path.open("r", encoding="utf-8") as f:
                data = json.load(f)

            if pack_name == "contract_rules":
                engine.ingest_contract_rules(data)
            elif pack_name == "benefit_contract":
                benefit_data = data
                engine.ingest_benefit_contract_pack(data)
            elif pack_name == "exclusion_knowledge":
                exclusion_data = data
                engine.ingest_exclusion_knowledge_pack(data)
            elif pack_name == "rulebook_policy":
                engine.ingest_rulebook_policy_pack(data)

        # Create service links
        if benefit_data and exclusion_data:
            engine.ingest_service_links(benefit_data, exclusion_data)

        engine.print_summary()

    except Exception as e:
        logger.error(f"Error during ingestion: {e}", exc_info=True)
    finally:
        if engine:
            engine.close()


if __name__ == "__main__":
    main()
