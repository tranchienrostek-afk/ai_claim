"""Neo4j MCP Server for Pathway Adjudication Agents.

This MCP server provides tools for agents to query insurance knowledge,
clinical data, and contract rules from Neo4j.

Usage:
    python -m mcp_server.pathway_neo4j_server.server

The server will listen on stdio for MCP protocol messages.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from typing import Any

from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import Tool, TextContent
from neo4j import GraphDatabase
from server_support.pathway_graph_operating import PathwayGraphOperatingStore

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Neo4j connection — env-overridable with multi-URI fallback so the same MCP
# server image works whether launched on the host (7688) or inside docker
# (neo4j:7687 / host.docker.internal:7688).
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "password123")
INSURANCE_NAMESPACE = "claims_insights_insurance_v1"


def _candidate_neo4j_uris() -> list[str]:
    env_uri = (os.getenv("NEO4J_URI") or "").strip()
    candidates = [
        env_uri,
        "bolt://localhost:7688",
        "bolt://host.docker.internal:7688",
        "bolt://neo4j:7687",
    ]
    seen: set[str] = set()
    result: list[str] = []
    for uri in candidates:
        if not uri or uri in seen:
            continue
        seen.add(uri)
        result.append(uri)
    return result


def _connect_with_fallback() -> tuple[Any, str]:
    errors: list[str] = []
    for uri in _candidate_neo4j_uris():
        try:
            driver = GraphDatabase.driver(
                uri,
                auth=(NEO4J_USER, NEO4J_PASSWORD),
                connection_timeout=3,
            )
            driver.verify_connectivity()
            logger.info(f"Connected to Neo4j at {uri}")
            return driver, uri
        except Exception as exc:  # pragma: no cover - runtime fallback
            errors.append(f"{uri}: {exc}")
            logger.warning(f"Neo4j connect failed for {uri}: {exc}")
    raise RuntimeError(
        "Could not connect to Neo4j with any candidate URI. Tried:\n  - "
        + "\n  - ".join(errors)
    )


# Resolved at first successful connect (see _ensure_store()). Kept None at
# module import time so that importing the server never fails when the DB
# is temporarily down — the MCP handshake can still complete and return an
# informative error on the first query instead.
NEO4J_URI = _candidate_neo4j_uris()[0]

# Create MCP server
server = Server("pathway-neo4j")

# Create MCP server
server = Server("pathway-neo4j")


class Neo4jKnowledgeStore:
    """Neo4j knowledge store for adjudication queries."""

    def __init__(self, uri: str, user: str, password: str):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        """Close Neo4j driver."""
        if self.driver:
            self.driver.close()

    def query_contracts_by_insurer(self, insurer: str) -> list[dict]:
        """Query all contracts for a given insurer."""
        with self.driver.session() as session:
            result = session.run("""
                MATCH (i:Insurer {name: $insurer})-[:ISSUES]->(c:InsuranceContract)
                WHERE coalesce(i.namespace, '') = $namespace
                  AND coalesce(c.namespace, '') = $namespace
                RETURN c.contract_id AS contract_id,
                       c.product_name AS product_name,
                       c.mode AS mode,
                       c.requires_preauth AS requires_preauth,
                       c.positive_result_required AS positive_result_required,
                       c.paid_ratio_pct AS paid_ratio_pct
                ORDER BY c.contract_id
            """, insurer=insurer, namespace=INSURANCE_NAMESPACE)
            return [dict(record) for record in result]

    def query_benefits_for_contract(self, contract_id: str, benefit_name: str = "") -> list[dict]:
        """Query benefits for a specific contract.

        When *benefit_name* is supplied the results are ranked by
        relevance (exact > starts-with > substring) so the most
        specific matches appear first and the limit is tighter (10
        instead of 50) to reduce noise for downstream reasoning.
        """
        with self.driver.session() as session:
            if benefit_name:
                # Relevance-ranked query: exact=0, starts-with=1, contains=2
                result = session.run("""
                    MATCH (c:InsuranceContract {contract_id: $contract_id})-[:HAS_BENEFIT]->(b:Benefit)
                    WHERE coalesce(c.namespace, '') = $namespace
                      AND coalesce(b.namespace, '') = $namespace
                    WITH b,
                         toLower(coalesce(b.canonical_name, '')) AS cn,
                         toLower(coalesce(b.entry_label, ''))    AS el,
                         toLower($benefit_name)                  AS q
                    WHERE cn CONTAINS q OR el CONTAINS q
                    WITH b,
                         CASE
                           WHEN cn = q OR el = q             THEN 0
                           WHEN cn STARTS WITH q OR el STARTS WITH q THEN 1
                           ELSE 2
                         END AS relevance
                    OPTIONAL MATCH (b)-[:INTERPRETED_AS]->(bi:BenefitInterpretation)
                    RETURN b.entry_id AS entry_id,
                           b.entry_label AS entry_label,
                           b.major_section AS major_section,
                           b.subsection AS subsection,
                           b.canonical_name AS canonical_name,
                           bi.entry_id AS interpretation_entry_id,
                           bi.canonical_name AS interpretation_name,
                           b.source_file AS source_file,
                           b.row_index AS row_index,
                           relevance
                    ORDER BY relevance, b.entry_label
                    LIMIT 10
                """, contract_id=contract_id, benefit_name=benefit_name, namespace=INSURANCE_NAMESPACE)
            else:
                result = session.run("""
                    MATCH (c:InsuranceContract {contract_id: $contract_id})-[:HAS_BENEFIT]->(b:Benefit)
                    WHERE coalesce(c.namespace, '') = $namespace
                      AND coalesce(b.namespace, '') = $namespace
                    OPTIONAL MATCH (b)-[:INTERPRETED_AS]->(bi:BenefitInterpretation)
                    RETURN b.entry_id AS entry_id,
                           b.entry_label AS entry_label,
                           b.major_section AS major_section,
                           b.subsection AS subsection,
                           b.canonical_name AS canonical_name,
                           bi.entry_id AS interpretation_entry_id,
                           bi.canonical_name AS interpretation_name,
                           b.source_file AS source_file,
                           b.row_index AS row_index
                    ORDER BY b.entry_label
                    LIMIT 30
                """, contract_id=contract_id, namespace=INSURANCE_NAMESPACE)
            return [dict(record) for record in result]

    def query_exclusions_by_contract(self, contract_id: str) -> list[dict]:
        """Query exclusions associated with a contract."""
        with self.driver.session() as session:
            result = session.run("""
                MATCH (c:InsuranceContract {contract_id: $contract_id})-[:HAS_EXCLUSION]->(e:Exclusion)
                OPTIONAL MATCH (e)-[:HAS_REASON]->(er:ExclusionReason)
                WHERE coalesce(c.namespace, '') = $namespace
                  AND coalesce(e.namespace, '') = $namespace
                RETURN e.code AS code,
                       e.group AS group,
                       e.reason AS reason,
                       e.process_path AS process_path,
                       e.source_note AS source_note,
                       er.text AS exclusion_reason_text,
                       e.usage_total_rows AS usage_total_rows,
                       e.usage_gap_vnd AS usage_gap_vnd
                ORDER BY e.usage_total_rows DESC
                LIMIT 50
            """, contract_id=contract_id, namespace=INSURANCE_NAMESPACE)
            return [dict(record) for record in result]

    def query_exclusion_by_reason_text(self, reason_text: str) -> list[dict]:
        """Search exclusions by reason text."""
        with self.driver.session() as session:
            result = session.run("""
                MATCH (e:Exclusion)-[:HAS_REASON]->(er:ExclusionReason)
                WHERE coalesce(e.namespace, '') = $namespace
                  AND coalesce(er.namespace, '') = $namespace
                  AND (
                       toLower(er.text) CONTAINS toLower($reason_text)
                    OR toLower(e.reason) CONTAINS toLower($reason_text)
                  )
                RETURN DISTINCT e.code AS code,
                       e.group AS group,
                       e.reason AS reason,
                       er.text AS exclusion_reason_text,
                       e.usage_total_rows AS usage_total_rows,
                       e.usage_gap_vnd AS usage_gap_vnd
                ORDER BY e.usage_total_rows DESC
                LIMIT 20
            """, reason_text=reason_text, namespace=INSURANCE_NAMESPACE)
            return [dict(record) for record in result]

    def query_rulebook_by_insurer(self, insurer: str) -> list[dict]:
        """Query rulebooks for an insurer."""
        with self.driver.session() as session:
            result = session.run("""
                MATCH (i:Insurer {name: $insurer})-[:HAS_RULEBOOK]->(r:Rulebook)
                WHERE coalesce(i.namespace, '') = $namespace
                  AND coalesce(r.namespace, '') = $namespace
                RETURN r.rulebook_id AS rulebook_id,
                       r.rule_code AS rule_code,
                       r.display_name AS display_name,
                       r.page_count AS page_count,
                       r.ocr_status AS ocr_status,
                       r.total_claim_evidence_rows AS claim_evidence_rows
                ORDER BY r.rule_code
            """, insurer=insurer, namespace=INSURANCE_NAMESPACE)
            return [dict(record) for record in result]

    def query_plans_for_contract(self, contract_id: str) -> list[dict]:
        """Query available plans for a contract."""
        with self.driver.session() as session:
            result = session.run("""
                MATCH (c:InsuranceContract {contract_id: $contract_id})-[:COVERS_PLAN]->(p:ContractPlan)
                WHERE coalesce(c.namespace, '') = $namespace
                  AND coalesce(p.namespace, '') = $namespace
                RETURN p.plan_id AS plan_id,
                       p.name AS name,
                       p.contract_id AS contract_id
                ORDER BY p.name
            """, contract_id=contract_id, namespace=INSURANCE_NAMESPACE)
            return [dict(record) for record in result]

    def query_plan_coverage_for_benefit(self, contract_id: str, plan_name: str, benefit_label: str) -> list[dict]:
        """Check if a benefit is covered under a specific plan."""
        with self.driver.session() as session:
            result = session.run("""
                MATCH (c:InsuranceContract {contract_id: $contract_id})
                MATCH (c)-[:COVERS_PLAN]->(p:ContractPlan {name: $plan_name})-[r:COVERS_BENEFIT]->(b:Benefit)
                WHERE coalesce(c.namespace, '') = $namespace
                  AND coalesce(p.namespace, '') = $namespace
                  AND coalesce(b.namespace, '') = $namespace
                  AND b.contract_id = $contract_id
                  AND toLower(b.entry_label) = toLower($benefit_label)
                RETURN b.entry_label AS benefit_label,
                       p.name AS plan_name,
                       r.coverage AS coverage
                LIMIT 1
            """, contract_id=contract_id, plan_name=plan_name, benefit_label=benefit_label, namespace=INSURANCE_NAMESPACE)
            return [dict(record) for record in result]

    def query_service_exclusions(
        self,
        service_code: str = "",
        service_name: str = "",
        contract_id: str = "",
    ) -> list[dict]:
        """Query exclusions related to a service (code or name)."""
        with self.driver.session() as session:
            if service_code:
                if contract_id:
                    result = session.run("""
                        MATCH (s:CIService {service_code: $code})-[direct:EXCLUDED_BY_CONTRACT {contract_id: $contract_id}]->(e:Exclusion)
                        WHERE direct.namespace = $namespace AND e.namespace = $namespace
                        OPTIONAL MATCH (e)-[:HAS_REASON]->(er:ExclusionReason)
                        RETURN s.service_code AS service_code,
                               s.service_name AS service_name,
                               e.code AS exclusion_code,
                               e.group AS exclusion_group,
                               e.reason AS exclusion_reason,
                               er.text AS exclusion_reason_text,
                               direct.rows AS evidence_rows,
                               '' AS contract_distribution_json,
                               'direct_contract_service_exclusion' AS evidence_source
                        UNION
                        MATCH (s:CIService {service_code: $code})-[hist:HISTORICALLY_EXCLUDED]->(ep:ExclusionPattern)
                        WHERE hist.namespace = $namespace AND ep.namespace = $namespace
                        OPTIONAL MATCH (ep)-[:MATCHES_EXCLUSION]->(e:Exclusion)
                        RETURN s.service_code AS service_code,
                               s.service_name AS service_name,
                               coalesce(e.code, ep.exclusion_code) AS exclusion_code,
                               e.group AS exclusion_group,
                               coalesce(e.reason, ep.reason) AS exclusion_reason,
                               '' AS exclusion_reason_text,
                               hist.rows AS evidence_rows,
                               '' AS contract_distribution_json,
                               'historical_service_exclusion' AS evidence_source
                        LIMIT 20
                    """, code=service_code, contract_id=contract_id, namespace=INSURANCE_NAMESPACE)
                else:
                    result = session.run("""
                        MATCH (s:CIService {service_code: $code})-[direct:EXCLUDED_BY]->(e:Exclusion)
                        WHERE direct.namespace = $namespace AND e.namespace = $namespace
                        OPTIONAL MATCH (e)-[:HAS_REASON]->(er:ExclusionReason)
                        RETURN s.service_code AS service_code,
                               s.service_name AS service_name,
                               e.code AS exclusion_code,
                               e.group AS exclusion_group,
                               e.reason AS exclusion_reason,
                               er.text AS exclusion_reason_text,
                               direct.rows AS evidence_rows,
                               direct.contract_distribution_json AS contract_distribution_json,
                               'direct_service_exclusion' AS evidence_source
                        UNION
                        MATCH (s:CIService {service_code: $code})-[hist:HISTORICALLY_EXCLUDED]->(ep:ExclusionPattern)
                        WHERE hist.namespace = $namespace AND ep.namespace = $namespace
                        OPTIONAL MATCH (ep)-[:MATCHES_EXCLUSION]->(e:Exclusion)
                        RETURN s.service_code AS service_code,
                               s.service_name AS service_name,
                               coalesce(e.code, ep.exclusion_code) AS exclusion_code,
                               e.group AS exclusion_group,
                               coalesce(e.reason, ep.reason) AS exclusion_reason,
                               '' AS exclusion_reason_text,
                               hist.rows AS evidence_rows,
                               '' AS contract_distribution_json,
                               'historical_service_exclusion' AS evidence_source
                        LIMIT 20
                    """, code=service_code, namespace=INSURANCE_NAMESPACE)
            elif service_name:
                result = session.run("""
                    MATCH (s:CIService)
                    WHERE toLower(coalesce(s.service_name, '')) CONTAINS toLower($name)
                    MATCH (s)-[direct:EXCLUDED_BY]->(e:Exclusion)
                    WHERE direct.namespace = $namespace AND e.namespace = $namespace
                    OPTIONAL MATCH (e)-[:HAS_REASON]->(er:ExclusionReason)
                    RETURN s.service_code AS service_code,
                           s.service_name AS service_name,
                           e.code AS exclusion_code,
                           e.group AS exclusion_group,
                           e.reason AS exclusion_reason,
                           er.text AS exclusion_reason_text,
                           direct.rows AS evidence_rows,
                           direct.contract_distribution_json AS contract_distribution_json,
                           'direct_service_exclusion' AS evidence_source
                    LIMIT 20
                """, name=service_name, namespace=INSURANCE_NAMESPACE)
            else:
                return []
            return [dict(record) for record in result]

    def query_contract_stats(self, contract_id: str) -> dict:
        """Get statistical summary for a contract."""
        with self.driver.session() as session:
            result = session.run("""
                MATCH (c:InsuranceContract {contract_id: $contract_id})
                OPTIONAL MATCH (c)-[:HAS_BENEFIT]->(b:Benefit)
                OPTIONAL MATCH (c)-[:HAS_EXCLUSION]->(e:Exclusion)
                OPTIONAL MATCH (c)-[:COVERS_PLAN]->(p:ContractPlan)
                WHERE coalesce(c.namespace, '') = $namespace
                RETURN c.contract_id AS contract_id,
                       c.product_name AS product_name,
                       c.mode AS mode,
                       c.paid_ratio_pct AS paid_ratio_pct,
                       count(DISTINCT b) AS benefit_count,
                       count(DISTINCT e) AS exclusion_count,
                       count(DISTINCT p) AS plan_count
                LIMIT 1
            """, contract_id=contract_id, namespace=INSURANCE_NAMESPACE)
            records = [dict(record) for record in result]
            return records[0] if records else {}

    def query_clinical_service_info(self, service_code: str) -> dict:
        """Query clinical information about a service from Neo4j."""
        with self.driver.session() as session:
            result = session.run("""
                MATCH (s:CIService {service_code: $code})
                OPTIONAL MATCH (d:CIDisease)-[:CI_INDICATES_SERVICE]->(s)
                RETURN s.service_code AS code,
                       s.service_name AS canonical_name,
                       s.category_code AS category_code,
                       s.category_name AS category_name,
                       collect(DISTINCT d.disease_id) AS related_icds,
                       collect(DISTINCT d.disease_name) AS related_diseases,
                       [] AS related_lab_tests,
                       [] AS related_procedures
                LIMIT 1
            """, code=service_code)
            records = [dict(record) for record in result]
            return records[0] if records else {}

    def query_disease_services(self, icd_code: str = "", disease_name: str = "") -> list[dict]:
        """Query services commonly used for a disease.

        Tries the clinical Disease→Chunk→Service path first.  When that
        yields no service codes, falls back to the claims-insights
        CIDisease→CI_INDICATES_SERVICE→CIService domain so that callers
        still get actionable results even when the clinical graph lacks
        coverage for a particular disease.
        """
        with self.driver.session() as session:
            # --- primary path: clinical graph ---
            if icd_code:
                result = session.run("""
                    MATCH (d:Disease {icd_code: $code})<-[:ABOUT_DISEASE]-(ch:Chunk)
                    OPTIONAL MATCH (ch)-[:MENTIONS]->(s:Service)
                    RETURN d.icd_code AS icd_code,
                           d.name AS disease_name,
                           collect(DISTINCT s.code) AS service_codes,
                           collect(DISTINCT s.canonical_name) AS service_names
                    LIMIT 1
                """, code=icd_code)
            elif disease_name:
                result = session.run("""
                    MATCH (d:Disease)
                    WHERE toLower(d.name) CONTAINS toLower($name)
                    MATCH (d)<-[:ABOUT_DISEASE]-(ch:Chunk)
                    OPTIONAL MATCH (ch)-[:MENTIONS]->(s:Service)
                    RETURN d.icd_code AS icd_code,
                           d.name AS disease_name,
                           collect(DISTINCT s.code) AS service_codes,
                           collect(DISTINCT s.canonical_name) AS service_names
                    LIMIT 5
                """, name=disease_name)
            else:
                return []

            records = [dict(record) for record in result]

            # If primary path returned results with actual service codes, use them
            has_services = any(
                r.get("service_codes") for r in records
            )
            if has_services:
                return records

            # --- fallback: CIDisease → CI_INDICATES_SERVICE → CIService ---
            if icd_code:
                ci_result = session.run("""
                    MATCH (d:CIDisease)
                    WHERE d.icd_code = $code
                       OR d.disease_id STARTS WITH $code
                    OPTIONAL MATCH (d)-[:CI_INDICATES_SERVICE]->(svc:CIService)
                    RETURN d.disease_id AS icd_code,
                           d.disease_name AS disease_name,
                           collect(DISTINCT svc.service_code) AS service_codes,
                           collect(DISTINCT svc.canonical_name) AS service_names
                    LIMIT 1
                """, code=icd_code)
            elif disease_name:
                ci_result = session.run("""
                    MATCH (d:CIDisease)
                    WHERE toLower(coalesce(d.disease_name, '')) CONTAINS toLower($name)
                    OPTIONAL MATCH (d)-[:CI_INDICATES_SERVICE]->(svc:CIService)
                    RETURN d.disease_id AS icd_code,
                           d.disease_name AS disease_name,
                           collect(DISTINCT svc.service_code) AS service_codes,
                           collect(DISTINCT svc.canonical_name) AS service_names
                    LIMIT 5
                """, name=disease_name)
            else:
                return records  # return whatever we got from primary

            ci_records = [dict(r) for r in ci_result]
            ci_has_services = any(r.get("service_codes") for r in ci_records)
            if ci_has_services:
                for r in ci_records:
                    r["_source"] = "ci_disease_fallback"
                return ci_records

            # --- fallback 2: DiseaseHypothesis → DISEASE_EXPECTS_SERVICE → ProtocolService ---
            if icd_code:
                hyp_result = session.run("""
                    MATCH (h:DiseaseHypothesis)
                    WHERE h.icd10 = $code
                    MATCH (h)-[r:DISEASE_EXPECTS_SERVICE]->(s)
                    WHERE s:ProtocolService OR s:CIService
                    RETURN h.icd10 AS icd_code,
                           h.disease_name AS disease_name,
                           collect(DISTINCT s.service_code) AS service_codes,
                           collect(DISTINCT coalesce(s.service_name, s.canonical_name)) AS service_names,
                           collect(DISTINCT {code: s.service_code, name: coalesce(s.service_name, s.canonical_name), role: r.role}) AS service_details
                    LIMIT 1
                """, code=icd_code)
            elif disease_name:
                hyp_result = session.run("""
                    MATCH (h:DiseaseHypothesis)
                    WHERE toLower(coalesce(h.disease_name, '')) CONTAINS toLower($name)
                    MATCH (h)-[r:DISEASE_EXPECTS_SERVICE]->(s)
                    WHERE s:ProtocolService OR s:CIService
                    RETURN h.icd10 AS icd_code,
                           h.disease_name AS disease_name,
                           collect(DISTINCT s.service_code) AS service_codes,
                           collect(DISTINCT coalesce(s.service_name, s.canonical_name)) AS service_names,
                           collect(DISTINCT {code: s.service_code, name: coalesce(s.service_name, s.canonical_name), role: r.role}) AS service_details
                    LIMIT 5
                """, name=disease_name)
            else:
                return records or ci_records

            hyp_records = [dict(r) for r in hyp_result]
            if hyp_records:
                for r in hyp_records:
                    r["_source"] = "disease_hypothesis_seed"
                return hyp_records

            return records  # return primary even if empty (disease exists, no services)

    def list_recent_ci_diseases(self, limit: int = 5, namespace: str = "claims_insights_explorer_v1") -> list[dict]:
        """List recently inserted CIDisease nodes using internal node id as a recency proxy."""
        with self.driver.session() as session:
            result = session.run(
                """
                MATCH (d:CIDisease)
                WHERE $ns = '' OR d.namespace = $ns
                WITH d, id(d) AS node_id
                OPTIONAL MATCH (d)-[:CI_HAS_SIGN]->(sign:CISign)
                OPTIONAL MATCH (d)-[:CI_INDICATES_SERVICE]->(svc:CIService)
                RETURN d.disease_id AS disease_id,
                       d.disease_name AS disease_name,
                       d.namespace AS namespace,
                       node_id AS neo4j_node_id,
                       count(DISTINCT sign) AS sign_count,
                       count(DISTINCT svc) AS service_count
                ORDER BY node_id DESC
                LIMIT $limit
                """,
                ns=namespace,
                limit=max(1, min(int(limit or 5), 20)),
            )
            return [dict(record) for record in result]

    def query_ci_disease_snapshot(
        self,
        disease_id: str = "",
        disease_name: str = "",
        limit: int = 12,
        namespace: str = "claims_insights_explorer_v1",
    ) -> dict:
        """Return a compact CIDisease snapshot with signs and service edges for tool-based reasoning."""
        with self.driver.session() as session:
            if disease_id:
                disease = session.run(
                    """
                    MATCH (d:CIDisease {disease_id:$disease_id})
                    WHERE $ns = '' OR d.namespace = $ns
                    RETURN d.disease_id AS disease_id,
                           d.disease_name AS disease_name,
                           d.namespace AS namespace,
                           properties(d) AS raw_properties
                    LIMIT 1
                    """,
                    disease_id=disease_id,
                    ns=namespace,
                ).single()
            elif disease_name:
                disease = session.run(
                    """
                    MATCH (d:CIDisease)
                    WHERE ($ns = '' OR d.namespace = $ns)
                      AND toLower(coalesce(d.disease_name, '')) CONTAINS toLower($disease_name)
                    RETURN d.disease_id AS disease_id,
                           d.disease_name AS disease_name,
                           d.namespace AS namespace,
                           properties(d) AS raw_properties
                    ORDER BY id(d) DESC
                    LIMIT 1
                    """,
                    disease_name=disease_name,
                    ns=namespace,
                ).single()
            else:
                return {}

            if disease is None:
                return {}

            resolved_id = disease["disease_id"]
            sign_rows = [
                dict(record)
                for record in session.run(
                    """
                    MATCH (:CIDisease {disease_id:$disease_id})-[:CI_HAS_SIGN]->(sign:CISign)
                    RETURN sign.sign_id AS sign_id,
                           sign.text AS text,
                           sign.normalized_key AS normalized_key
                    ORDER BY sign.text
                    LIMIT $limit
                    """,
                    disease_id=resolved_id,
                    limit=max(1, min(int(limit or 12), 30)),
                )
            ]
            service_rows = [
                dict(record)
                for record in session.run(
                    """
                    MATCH (:CIDisease {disease_id:$disease_id})-[r:CI_INDICATES_SERVICE]->(svc:CIService)
                    RETURN svc.service_code AS service_code,
                           svc.service_name AS service_name,
                           svc.category_name AS category_name,
                           properties(r) AS relation_props
                    ORDER BY coalesce(r.max_score, 0.0) DESC, coalesce(r.guideline_hits, 0) DESC, svc.service_name
                    LIMIT $limit
                    """,
                    disease_id=resolved_id,
                    limit=max(1, min(int(limit or 12), 30)),
                )
            ]
            return {
                "disease": dict(disease),
                "signs": sign_rows,
                "services": service_rows,
                "sign_count": len(sign_rows),
                "service_count": len(service_rows),
            }


# Lazy singletons — initialized on first use, with retry on Neo4j outage.
# Module import must not depend on Neo4j being up, otherwise the MCP handshake
# with Claude Code fails and the server stays "pending" forever.
store: "Neo4jKnowledgeStore | None" = None
graph_ops_store: "PathwayGraphOperatingStore | None" = None


def _ensure_store() -> "Neo4jKnowledgeStore":
    global store, graph_ops_store, NEO4J_URI
    if store is not None:
        try:
            store.driver.verify_connectivity()
            return store
        except Exception as exc:
            logger.warning(f"Cached Neo4j driver is stale ({exc}); reconnecting")
            try:
                store.close()
            except Exception:
                pass
            store = None
    driver, uri = _connect_with_fallback()
    NEO4J_URI = uri
    new_store = Neo4jKnowledgeStore.__new__(Neo4jKnowledgeStore)
    new_store.driver = driver
    store = new_store
    if graph_ops_store is None:
        graph_ops_store = PathwayGraphOperatingStore()
    return store


# ---------------------------------------------------------------------------
# MCP Tool Definitions
# ---------------------------------------------------------------------------

@server.list_tools()
async def list_tools() -> list[Tool]:
    """List available MCP tools."""
    return [
        Tool(
            name="query_contracts_by_insurer",
            description="Query all contracts for a given insurer name (e.g., 'FPT', 'PJICO', 'BHV')",
            inputSchema={
                "type": "object",
                "properties": {
                    "insurer": {
                        "type": "string",
                        "description": "Insurer name (e.g., FPT, PJICO, BHV, TCGIns, UIC)"
                    }
                },
                "required": ["insurer"]
            }
        ),
        Tool(
            name="query_benefits_for_contract",
            description="Query benefits for a specific contract. Optionally filter by benefit name.",
            inputSchema={
                "type": "object",
                "properties": {
                    "contract_id": {
                        "type": "string",
                        "description": "Contract ID (e.g., FPT-NT-2024)"
                    },
                    "benefit_name": {
                        "type": "string",
                        "description": "Optional benefit name to filter (e.g., 'Điều trị ngoại trú')"
                    }
                },
                "required": ["contract_id"]
            }
        ),
        Tool(
            name="query_exclusions_by_contract",
            description="Query exclusions associated with a contract, sorted by usage frequency.",
            inputSchema={
                "type": "object",
                "properties": {
                    "contract_id": {
                        "type": "string",
                        "description": "Contract ID"
                    }
                },
                "required": ["contract_id"]
            }
        ),
        Tool(
            name="query_exclusion_by_reason_text",
            description="Search exclusions by reason text (e.g., 'thuốc', 'cận lâm sàng').",
            inputSchema={
                "type": "object",
                "properties": {
                    "reason_text": {
                        "type": "string",
                        "description": "Reason text to search for in exclusions"
                    }
                },
                "required": ["reason_text"]
            }
        ),
        Tool(
            name="query_rulebook_by_insurer",
            description="Query rulebooks for an insurer (QT 384, QT 711, etc.).",
            inputSchema={
                "type": "object",
                "properties": {
                    "insurer": {
                        "type": "string",
                        "description": "Insurer name"
                    }
                },
                "required": ["insurer"]
            }
        ),
        Tool(
            name="query_plans_for_contract",
            description="Query available plans for a contract.",
            inputSchema={
                "type": "object",
                "properties": {
                    "contract_id": {
                        "type": "string",
                        "description": "Contract ID"
                    }
                },
                "required": ["contract_id"]
            }
        ),
        Tool(
            name="query_plan_coverage_for_benefit",
            description="Check if a specific benefit is covered under a plan.",
            inputSchema={
                "type": "object",
                "properties": {
                    "contract_id": {
                        "type": "string",
                        "description": "Contract ID"
                    },
                    "plan_name": {
                        "type": "string",
                        "description": "Plan name (e.g., Gói A, Gói B)"
                    },
                    "benefit_label": {
                        "type": "string",
                        "description": "Benefit label to check"
                    }
                },
                "required": ["contract_id", "plan_name", "benefit_label"]
            }
        ),
        Tool(
            name="query_service_exclusions",
            description="Query exclusions related to a service by code or name.",
            inputSchema={
                "type": "object",
                "properties": {
                    "service_code": {
                        "type": "string",
                        "description": "Service code (e.g., LAB-BIO-002)"
                    },
                    "service_name": {
                        "type": "string",
                        "description": "Service name (used if code not provided)"
                    },
                    "contract_id": {
                        "type": "string",
                        "description": "Optional contract ID to prefer direct contract-specific exclusion evidence"
                    }
                },
                "required": []
            }
        ),
        Tool(
            name="query_contract_stats",
            description="Get statistical summary for a contract (benefits, exclusions, plans counts).",
            inputSchema={
                "type": "object",
                "properties": {
                    "contract_id": {
                        "type": "string",
                        "description": "Contract ID"
                    }
                },
                "required": ["contract_id"]
            }
        ),
        Tool(
            name="query_clinical_service_info",
            description="Query clinical information about a service from Neo4j (related diseases, lab tests, procedures).",
            inputSchema={
                "type": "object",
                "properties": {
                    "service_code": {
                        "type": "string",
                        "description": "Service code"
                    }
                },
                "required": ["service_code"]
            }
        ),
        Tool(
            name="query_disease_services",
            description="Query services commonly used for a disease (by ICD code or name).",
            inputSchema={
                "type": "object",
                "properties": {
                    "icd_code": {
                        "type": "string",
                        "description": "ICD-10 code (e.g., J18.9)"
                    },
                    "disease_name": {
                        "type": "string",
                        "description": "Disease name (used if ICD code not provided)"
                    }
                },
                "required": []
            }
        ),
        Tool(
            name="list_recent_ci_diseases",
            description="List recent CIDisease nodes from claims_insights_explorer_v1 using Neo4j node id as a recency proxy.",
            inputSchema={
                "type": "object",
                "properties": {
                    "limit": {
                        "type": "integer",
                        "description": "How many diseases to return (default 5, max 20)"
                    },
                    "namespace": {
                        "type": "string",
                        "description": "Optional namespace filter (default claims_insights_explorer_v1)"
                    }
                },
                "required": []
            }
        ),
        Tool(
            name="query_ci_disease_snapshot",
            description="Return a CIDisease snapshot with signs and service edges for tool-based clinical reasoning.",
            inputSchema={
                "type": "object",
                "properties": {
                    "disease_id": {
                        "type": "string",
                        "description": "CIDisease id such as disease:J03"
                    },
                    "disease_name": {
                        "type": "string",
                        "description": "Disease name fallback if disease_id is not provided"
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Max signs/services to return per section (default 12, max 30)"
                    },
                    "namespace": {
                        "type": "string",
                        "description": "Optional namespace filter (default claims_insights_explorer_v1)"
                    }
                },
                "required": []
            }
        ),
        Tool(
            name="graph_operating_search",
            description="Unified graph search across ontology, claims, and insurance domains.",
            inputSchema={
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "Free-text query"
                    },
                    "domains": {
                        "type": "string",
                        "description": "Comma-separated domains: ontology,claims,insurance"
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Max hits to return"
                    }
                },
                "required": ["query"]
            }
        ),
        Tool(
            name="trace_service_evidence",
            description="Trace a service across ontology, claims, and insurance graph surfaces.",
            inputSchema={
                "type": "object",
                "properties": {
                    "service_code": {
                        "type": "string",
                        "description": "Optional canonical service code"
                    },
                    "service_name": {
                        "type": "string",
                        "description": "Optional service name"
                    },
                    "disease_id": {
                        "type": "string",
                        "description": "Optional disease id"
                    },
                    "contract_id": {
                        "type": "string",
                        "description": "Optional contract id"
                    },
                    "ontology_namespace": {
                        "type": "string",
                        "description": "Optional ontology namespace"
                    }
                },
                "required": []
            }
        ),
        Tool(
            name="graph_operating_health",
            description="Return graph operating health across ontology, claims, and insurance.",
            inputSchema={
                "type": "object",
                "properties": {}
            }
        ),
        Tool(
            name="graph_operating_report",
            description="Return a compact cross-domain graph report for hotspot navigation.",
            inputSchema={
                "type": "object",
                "properties": {}
            }
        ),
    ]


# ---------------------------------------------------------------------------
# MCP Tool Handlers
# ---------------------------------------------------------------------------

@server.call_tool()
async def call_tool(name: str, arguments: dict[str, Any]) -> list[TextContent]:
    """Handle MCP tool calls."""
    logger.info(f"Tool call: {name} with args: {arguments}")

    try:
        # Ensure a live Neo4j connection for every tool invocation. This
        # reconnects transparently if Neo4j was down when the MCP server
        # started or has since restarted.
        _ensure_store()
        result = []

        if name == "query_contracts_by_insurer":
            insurer = arguments.get("insurer")
            records = store.query_contracts_by_insurer(insurer)
            result = [TextContent(type="text", text=json.dumps(records, ensure_ascii=False, indent=2))]

        elif name == "query_benefits_for_contract":
            contract_id = arguments.get("contract_id")
            benefit_name = arguments.get("benefit_name", "")
            records = store.query_benefits_for_contract(contract_id, benefit_name)
            result = [TextContent(type="text", text=json.dumps(records, ensure_ascii=False, indent=2))]

        elif name == "query_exclusions_by_contract":
            contract_id = arguments.get("contract_id")
            records = store.query_exclusions_by_contract(contract_id)
            result = [TextContent(type="text", text=json.dumps(records, ensure_ascii=False, indent=2))]

        elif name == "query_exclusion_by_reason_text":
            reason_text = arguments.get("reason_text")
            records = store.query_exclusion_by_reason_text(reason_text)
            result = [TextContent(type="text", text=json.dumps(records, ensure_ascii=False, indent=2))]

        elif name == "query_rulebook_by_insurer":
            insurer = arguments.get("insurer")
            records = store.query_rulebook_by_insurer(insurer)
            result = [TextContent(type="text", text=json.dumps(records, ensure_ascii=False, indent=2))]

        elif name == "query_plans_for_contract":
            contract_id = arguments.get("contract_id")
            records = store.query_plans_for_contract(contract_id)
            result = [TextContent(type="text", text=json.dumps(records, ensure_ascii=False, indent=2))]

        elif name == "query_plan_coverage_for_benefit":
            contract_id = arguments.get("contract_id")
            plan_name = arguments.get("plan_name")
            benefit_label = arguments.get("benefit_label")
            records = store.query_plan_coverage_for_benefit(contract_id, plan_name, benefit_label)
            result = [TextContent(type="text", text=json.dumps(records, ensure_ascii=False, indent=2))]

        elif name == "query_service_exclusions":
            service_code = arguments.get("service_code", "")
            service_name = arguments.get("service_name", "")
            contract_id = arguments.get("contract_id", "")
            records = store.query_service_exclusions(service_code, service_name, contract_id)
            result = [TextContent(type="text", text=json.dumps(records, ensure_ascii=False, indent=2))]

        elif name == "query_contract_stats":
            contract_id = arguments.get("contract_id")
            record = store.query_contract_stats(contract_id)
            result = [TextContent(type="text", text=json.dumps(record, ensure_ascii=False, indent=2))]

        elif name == "query_clinical_service_info":
            service_code = arguments.get("service_code")
            record = store.query_clinical_service_info(service_code)
            result = [TextContent(type="text", text=json.dumps(record, ensure_ascii=False, indent=2))]

        elif name == "query_disease_services":
            icd_code = arguments.get("icd_code", "")
            disease_name = arguments.get("disease_name", "")
            records = store.query_disease_services(icd_code, disease_name)
            result = [TextContent(type="text", text=json.dumps(records, ensure_ascii=False, indent=2))]

        elif name == "list_recent_ci_diseases":
            limit = arguments.get("limit", 5)
            namespace = arguments.get("namespace", "claims_insights_explorer_v1")
            records = store.list_recent_ci_diseases(limit, namespace)
            result = [TextContent(type="text", text=json.dumps(records, ensure_ascii=False, indent=2))]

        elif name == "query_ci_disease_snapshot":
            disease_id = arguments.get("disease_id", "")
            disease_name = arguments.get("disease_name", "")
            limit = arguments.get("limit", 12)
            namespace = arguments.get("namespace", "claims_insights_explorer_v1")
            record = store.query_ci_disease_snapshot(disease_id, disease_name, limit, namespace)
            result = [TextContent(type="text", text=json.dumps(record, ensure_ascii=False, indent=2))]

        elif name == "graph_operating_search":
            query = arguments.get("query", "")
            domains = arguments.get("domains", "")
            limit = arguments.get("limit", 12)
            payload = graph_ops_store.search(query, domains=domains, limit=limit)
            result = [TextContent(type="text", text=json.dumps(payload, ensure_ascii=False, indent=2))]

        elif name == "trace_service_evidence":
            payload = graph_ops_store.trace_service(
                service_code=arguments.get("service_code", ""),
                service_name=arguments.get("service_name", ""),
                disease_id=arguments.get("disease_id", ""),
                contract_id=arguments.get("contract_id", ""),
                ontology_namespace=arguments.get("ontology_namespace") or None,
            )
            result = [TextContent(type="text", text=json.dumps(payload, ensure_ascii=False, indent=2))]

        elif name == "graph_operating_health":
            payload = graph_ops_store.health()
            result = [TextContent(type="text", text=json.dumps(payload, ensure_ascii=False, indent=2))]

        elif name == "graph_operating_report":
            payload = graph_ops_store.report()
            result = [TextContent(type="text", text=json.dumps(payload, ensure_ascii=False, indent=2))]

        else:
            result = [TextContent(type="text", text=f"Unknown tool: {name}")]

        return result

    except Exception as e:
        logger.error(f"Error in tool call: {e}", exc_info=True)
        return [TextContent(type="text", text=json.dumps({"error": str(e)}))]


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

async def main():
    """Run the MCP server."""
    logger.info("Starting Pathway Neo4j MCP Server...")
    logger.info(f"Connecting to Neo4j at {NEO4J_URI}")

    async with stdio_server() as (read_stream, write_stream):
        await server.run(
            read_stream,
            write_stream,
            server.create_initialization_options()
        )


if __name__ == "__main__":
    asyncio.run(main())
