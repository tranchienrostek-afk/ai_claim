from __future__ import annotations

import unicodedata
from collections import Counter
from typing import Any, Iterable

from server_support.adjudication.contract_agent_neo4j import INSURANCE_NAMESPACE
from server_support.claims_insights_graph_store import (
    NAMESPACE as CLAIMS_INSIGHTS_NAMESPACE,
    ClaimsInsightsGraphStore,
)
from server_support.ontology_v2_inspector_store import DEFAULT_NAMESPACE, OntologyV2InspectorStore
from server_support.pathway_data_architecture import PathwayDataArchitectureStore


def _text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _ascii_fold(value: Any) -> str:
    text = _text(value).lower()
    normalized = unicodedata.normalize("NFKD", text)
    return "".join(ch for ch in normalized if not unicodedata.combining(ch))


def _score_match(query: str, *values: Any) -> float:
    needle = _ascii_fold(query)
    if not needle:
        return 0.0
    best = 0.0
    for value in values:
        hay = _ascii_fold(value)
        if not hay:
            continue
        if hay == needle:
            best = max(best, 1.0)
        elif hay.startswith(needle):
            best = max(best, 0.96)
        elif needle in hay:
            best = max(best, 0.9)
    return best


def _parse_domains(domains: Iterable[str] | str | None) -> list[str]:
    if domains is None:
        return ["ontology", "claims", "insurance"]
    if isinstance(domains, str):
        values = [item.strip().lower() for item in domains.split(",")]
    else:
        values = [_text(item).lower() for item in domains]
    allowed = {"ontology", "claims", "insurance"}
    result: list[str] = []
    for value in values:
        if value in allowed and value not in result:
            result.append(value)
    return result or ["ontology", "claims", "insurance"]


class PathwayGraphOperatingStore:
    """Unified graph operating layer over ontology, claims, and insurance."""

    def __init__(
        self,
        ontology_store: OntologyV2InspectorStore | None = None,
        claims_store: ClaimsInsightsGraphStore | None = None,
        data_architecture_store: PathwayDataArchitectureStore | None = None,
        *,
        ontology_namespace: str = DEFAULT_NAMESPACE,
        claims_namespace: str = CLAIMS_INSIGHTS_NAMESPACE,
        insurance_namespace: str = INSURANCE_NAMESPACE,
    ) -> None:
        self.ontology_store = ontology_store or OntologyV2InspectorStore()
        self.claims_store = claims_store or ClaimsInsightsGraphStore()
        self.data_architecture_store = data_architecture_store or PathwayDataArchitectureStore(self.ontology_store)
        self.ontology_namespace = ontology_namespace
        self.claims_namespace = claims_namespace
        self.insurance_namespace = insurance_namespace
        self._schema_labels_cache: set[str] | None = None

    def _run_query(self, query: str, **params: Any) -> list[dict[str, Any]]:
        with self.ontology_store.driver.session() as session:
            return [dict(record) for record in session.run(query, params)]

    def _schema_labels(self) -> set[str]:
        if self._schema_labels_cache is not None:
            return self._schema_labels_cache
        rows = self._run_query("CALL db.labels() YIELD label RETURN label")
        self._schema_labels_cache = {str(row.get("label") or "") for row in rows}
        return self._schema_labels_cache

    def _insurance_summary(self) -> dict[str, Any]:
        counts = {
            "contracts": 0,
            "benefits": 0,
            "benefit_interpretations": 0,
            "exclusions": 0,
            "rulebooks": 0,
            "supported_by_contract_benefit": 0,
            "excluded_by_contract": 0,
            "excludes_service": 0,
            "disease_contract_benefit": 0,
            "disease_contract_exclusion": 0,
            "sign_contract_benefit": 0,
            "sign_contract_exclusion": 0,
            "benefit_clause_grounding": 0,
            "clause_reference_rulebook": 0,
            "exclusion_rulebook_grounding": 0,
        }
        queries = {
            "contracts": "MATCH (n:InsuranceContract {namespace:$ns}) RETURN count(n) AS c",
            "benefits": "MATCH (n:Benefit {namespace:$ns}) RETURN count(n) AS c",
            "benefit_interpretations": "MATCH (n:BenefitInterpretation {namespace:$ns}) RETURN count(n) AS c",
            "exclusions": "MATCH (n:Exclusion {namespace:$ns}) RETURN count(n) AS c",
            "rulebooks": "MATCH (n:Rulebook {namespace:$ns}) RETURN count(n) AS c",
            "supported_by_contract_benefit": """
                MATCH (:CIService)-[r:SUPPORTED_BY_CONTRACT_BENEFIT]->(:Benefit {namespace:$ns})
                RETURN count(r) AS c
            """,
            "excluded_by_contract": """
                MATCH (:CIService)-[r:EXCLUDED_BY_CONTRACT]->(:Exclusion {namespace:$ns})
                WHERE coalesce(r.namespace, '') = $ns
                RETURN count(r) AS c
            """,
            "excludes_service": """
                MATCH (:InsuranceContract {namespace:$ns})-[r:EXCLUDES_SERVICE]->(:CIService)
                RETURN count(r) AS c
            """,
            "disease_contract_benefit": """
                MATCH (:CIDisease)-[r:CI_SUPPORTS_CONTRACT_BENEFIT]->(:Benefit {namespace:$ns})
                WHERE coalesce(r.namespace, '') = $ns
                RETURN count(r) AS c
            """,
            "disease_contract_exclusion": """
                MATCH (:CIDisease)-[r:CI_FLAGGED_BY_CONTRACT_EXCLUSION]->(:Exclusion {namespace:$ns})
                WHERE coalesce(r.namespace, '') = $ns
                RETURN count(r) AS c
            """,
            "sign_contract_benefit": """
                MATCH (:CISign)-[r:CI_SUPPORTS_CONTRACT_BENEFIT]->(:Benefit {namespace:$ns})
                WHERE coalesce(r.namespace, '') = $ns
                RETURN count(r) AS c
            """,
            "sign_contract_exclusion": """
                MATCH (:CISign)-[r:CI_FLAGGED_BY_CONTRACT_EXCLUSION]->(:Exclusion {namespace:$ns})
                WHERE coalesce(r.namespace, '') = $ns
                RETURN count(r) AS c
            """,
            "benefit_clause_grounding": """
                MATCH (:Benefit {namespace:$ns})-[r:GROUNDED_IN_CONTRACT_CLAUSE]->(:ContractClause {namespace:$ns})
                RETURN count(r) AS c
            """,
            "clause_reference_rulebook": """
                MATCH (:ClauseReference {namespace:$ns})-[r:REFERS_TO_RULEBOOK]->(:Rulebook {namespace:$ns})
                RETURN count(r) AS c
            """,
            "exclusion_rulebook_grounding": """
                MATCH (:Exclusion {namespace:$ns})-[r:SUPPORTED_BY_RULEBOOK]->(:Rulebook {namespace:$ns})
                RETURN count(r) AS c
            """,
        }
        for key, query in queries.items():
            record = self._run_query(query, ns=self.insurance_namespace)
            counts[key] = int(record[0]["c"]) if record else 0
        return counts

    def _ontology_health(self) -> dict[str, Any]:
        summary = self.ontology_store.namespace_summary(self.ontology_namespace)
        edge_counts = {
            "assertion_indicates_service": 0,
            "assertion_contraindicates_service": 0,
            "disease_expects_service": 0,
        }
        labels = self._schema_labels()
        queries = {
            "assertion_indicates_service": """
                MATCH (:ProtocolAssertion {namespace:$ns})-[r:ASSERTION_INDICATES_SERVICE]->(svc)
                WHERE svc:ProtocolService OR svc:CIService
                RETURN count(r) AS c
            """,
            "assertion_contraindicates_service": """
                MATCH (:ProtocolAssertion {namespace:$ns})-[r:ASSERTION_CONTRAINDICATES]->(svc)
                WHERE svc:ProtocolService OR svc:CIService
                RETURN count(r) AS c
            """,
            "disease_expects_service": """
                MATCH (h:DiseaseHypothesis)-[r:DISEASE_EXPECTS_SERVICE]->(svc)
                WHERE (svc:ProtocolService OR svc:CIService)
                  AND coalesce(h.namespace, $ns) = $ns
                RETURN count(r) AS c
            """,
        }
        for key, query in queries.items():
            if key == "disease_expects_service" and "DiseaseHypothesis" not in labels:
                continue
            rows = self._run_query(query, ns=self.ontology_namespace)
            edge_counts[key] = int(rows[0]["c"]) if rows else 0
        return {
            "namespace": self.ontology_namespace,
            "summary": summary,
            "edge_counts": edge_counts,
        }

    def _claims_health(self) -> dict[str, Any]:
        bootstrap = self.claims_store.bootstrap()
        return {
            "namespace": self.claims_namespace,
            "stats": bootstrap.get("stats") or {},
            "disease_index_count": len(bootstrap.get("disease_index") or []),
        }

    def bootstrap(self) -> dict[str, Any]:
        ontology_bootstrap = self.ontology_store.bootstrap(namespace=self.ontology_namespace)
        claims_bootstrap = self.claims_store.bootstrap()
        data_architecture = self.data_architecture_store.bootstrap()
        insurance_summary = self._insurance_summary()

        return {
            "source": "neo4j",
            "mission": "Van hanh graph thong nhat cho ontology, claims, va insurance de search/trace/health/report phuc vu suy luan.",
            "capabilities": [
                "search",
                "trace_service",
                "health",
                "report",
                "cross_domain_graph_navigation",
            ],
            "domains": {
                "ontology": {
                    "namespace": self.ontology_namespace,
                    "summary": ontology_bootstrap.get("summary") or {},
                    "disease_count": len(ontology_bootstrap.get("diseases") or []),
                    "active_namespace": ontology_bootstrap.get("active_namespace"),
                },
                "claims": {
                    "namespace": self.claims_namespace,
                    "stats": claims_bootstrap.get("stats") or {},
                    "disease_count": len(claims_bootstrap.get("disease_index") or []),
                },
                "insurance": {
                    "namespace": self.insurance_namespace,
                    "summary": insurance_summary,
                },
            },
            "operating_contract": {
                "search_entrypoint": "/api/graph-operating/search",
                "trace_entrypoint": "/api/graph-operating/trace-service",
                "health_entrypoint": "/api/graph-operating/health",
                "report_entrypoint": "/api/graph-operating/report",
            },
            "data_architecture": {
                "schema_version": data_architecture.get("schema_version"),
                "warning_count": (data_architecture.get("summary") or {}).get("warning_count", 0),
                "domain_count": (data_architecture.get("summary") or {}).get("domain_count", 0),
            },
        }

    def _search_ontology(self, query: str, limit: int) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []

        disease_rows = self._run_query(
            """
            MATCH (d:DiseaseEntity {namespace:$ns})
            WHERE toLower(coalesce(d.disease_name, '')) CONTAINS toLower($q)
               OR toLower(coalesce(d.disease_id, '')) CONTAINS toLower($q)
            OPTIONAL MATCH (a:ProtocolAssertion {namespace:$ns})-[:ASSERTION_ABOUT_DISEASE]->(d)
            RETURN d.disease_id AS id,
                   d.disease_name AS label,
                   count(DISTINCT a) AS assertion_count
            ORDER BY assertion_count DESC, label
            LIMIT $limit
            """,
            ns=self.ontology_namespace,
            q=query,
            limit=max(limit, 8),
        )
        for row in disease_rows:
            rows.append(
                {
                    "domain": "ontology",
                    "entity_type": "disease",
                    "id": row["id"],
                    "label": row["label"],
                    "subtitle": f"assertions={int(row.get('assertion_count') or 0)}",
                    "score": _score_match(query, row.get("label"), row.get("id")),
                    "evidence": {"assertion_count": int(row.get("assertion_count") or 0)},
                }
            )

        service_rows = self._run_query(
            """
            MATCH (svc)
            WHERE (svc:ProtocolService OR svc:CIService)
              AND (
                    toLower(coalesce(svc.service_name, svc.name, '')) CONTAINS toLower($q)
                 OR toLower(coalesce(svc.service_code, '')) CONTAINS toLower($q)
              )
            OPTIONAL MATCH (a:ProtocolAssertion {namespace:$ns})-[:ASSERTION_INDICATES_SERVICE|ASSERTION_CONTRAINDICATES]->(svc)
            OPTIONAL MATCH (a)-[:ASSERTION_ABOUT_DISEASE]->(d:DiseaseEntity {namespace:$ns})
            RETURN svc.service_code AS id,
                   coalesce(svc.service_name, svc.name) AS label,
                   collect(DISTINCT d.disease_name)[0..3] AS diseases,
                   count(DISTINCT a) AS assertion_count
            ORDER BY assertion_count DESC, label
            LIMIT $limit
            """,
            ns=self.ontology_namespace,
            q=query,
            limit=max(limit, 8),
        )
        for row in service_rows:
            rows.append(
                {
                    "domain": "ontology",
                    "entity_type": "service",
                    "id": row["id"],
                    "label": row["label"],
                    "subtitle": ", ".join([item for item in (row.get("diseases") or []) if _text(item)]),
                    "score": _score_match(query, row.get("label"), row.get("id")),
                    "evidence": {
                        "related_diseases": row.get("diseases") or [],
                        "assertion_count": int(row.get("assertion_count") or 0),
                    },
                }
            )

        assertion_rows = self._run_query(
            """
            MATCH (a:ProtocolAssertion {namespace:$ns})-[:ASSERTION_ABOUT_DISEASE]->(d:DiseaseEntity {namespace:$ns})
            WHERE toLower(coalesce(a.assertion_text, '')) CONTAINS toLower($q)
               OR toLower(coalesce(a.action_text, '')) CONTAINS toLower($q)
               OR toLower(coalesce(d.disease_name, '')) CONTAINS toLower($q)
            RETURN a.assertion_id AS id,
                   a.assertion_type AS label,
                   d.disease_name AS disease_name,
                   a.assertion_text AS assertion_text
            ORDER BY label, disease_name
            LIMIT $limit
            """,
            ns=self.ontology_namespace,
            q=query,
            limit=max(limit, 8),
        )
        for row in assertion_rows:
            rows.append(
                {
                    "domain": "ontology",
                    "entity_type": "assertion",
                    "id": row["id"],
                    "label": row.get("label") or row["id"],
                    "subtitle": row.get("disease_name") or "",
                    "score": _score_match(query, row.get("assertion_text"), row.get("disease_name"), row.get("id")),
                    "evidence": {"assertion_text": row.get("assertion_text") or ""},
                }
            )
        return rows

    def _search_claims(self, query: str, limit: int) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        disease_rows = self._run_query(
            """
            MATCH (d:CIDisease {namespace:$ns})
            WHERE toLower(coalesce(d.disease_name, '')) CONTAINS toLower($q)
               OR toLower(coalesce(d.disease_id, '')) CONTAINS toLower($q)
               OR toLower(coalesce(d.icd10, '')) CONTAINS toLower($q)
            RETURN d.disease_id AS id,
                   d.disease_name AS label,
                   d.icd10 AS icd10,
                   d.case_count AS case_count,
                   d.linked_service_count AS linked_service_count
            ORDER BY coalesce(d.case_count, 0) DESC, coalesce(d.linked_service_count, 0) DESC
            LIMIT $limit
            """,
            ns=self.claims_namespace,
            q=query,
            limit=max(limit, 8),
        )
        for row in disease_rows:
            rows.append(
                {
                    "domain": "claims",
                    "entity_type": "disease",
                    "id": row["id"],
                    "label": row["label"],
                    "subtitle": _text(row.get("icd10")),
                    "score": _score_match(query, row.get("label"), row.get("id"), row.get("icd10")),
                    "evidence": {
                        "case_count": int(row.get("case_count") or 0),
                        "linked_service_count": int(row.get("linked_service_count") or 0),
                    },
                }
            )

        service_rows = self._run_query(
            """
            MATCH (s:CIService)
            WHERE toLower(coalesce(s.service_name, '')) CONTAINS toLower($q)
               OR toLower(coalesce(s.service_code, '')) CONTAINS toLower($q)
            OPTIONAL MATCH (d:CIDisease {namespace:$ns})-[r:CI_INDICATES_SERVICE]->(s)
            RETURN s.service_code AS id,
                   s.service_name AS label,
                   s.category_code AS category_code,
                   s.category_name AS category_name,
                   count(DISTINCT d) AS disease_count
            ORDER BY disease_count DESC, label
            LIMIT $limit
            """,
            ns=self.claims_namespace,
            q=query,
            limit=max(limit, 8),
        )
        for row in service_rows:
            rows.append(
                {
                    "domain": "claims",
                    "entity_type": "service",
                    "id": row["id"],
                    "label": row["label"],
                    "subtitle": _text(row.get("category_name") or row.get("category_code")),
                    "score": _score_match(query, row.get("label"), row.get("id")),
                    "evidence": {"disease_count": int(row.get("disease_count") or 0)},
                }
            )
        return rows

    def _search_insurance(self, query: str, limit: int) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []

        contract_rows = self._run_query(
            """
            MATCH (c:InsuranceContract {namespace:$ns})
            WHERE toLower(coalesce(c.contract_id, '')) CONTAINS toLower($q)
               OR toLower(coalesce(c.product_name, '')) CONTAINS toLower($q)
               OR toLower(coalesce(c.insurer, '')) CONTAINS toLower($q)
            RETURN c.contract_id AS id,
                   c.product_name AS label,
                   c.insurer AS insurer,
                   c.mode AS mode
            ORDER BY label, id
            LIMIT $limit
            """,
            ns=self.insurance_namespace,
            q=query,
            limit=max(limit, 8),
        )
        for row in contract_rows:
            rows.append(
                {
                    "domain": "insurance",
                    "entity_type": "contract",
                    "id": row["id"],
                    "label": row.get("label") or row["id"],
                    "subtitle": " | ".join([item for item in [_text(row.get("insurer")), _text(row.get("mode"))] if item]),
                    "score": _score_match(query, row.get("label"), row.get("id"), row.get("insurer")),
                    "evidence": {},
                }
            )

        benefit_rows = self._run_query(
            """
            MATCH (b:Benefit {namespace:$ns})
            WHERE toLower(coalesce(b.entry_label, '')) CONTAINS toLower($q)
               OR toLower(coalesce(b.canonical_name, '')) CONTAINS toLower($q)
               OR toLower(coalesce(b.entry_id, '')) CONTAINS toLower($q)
            RETURN b.entry_id AS id,
                   b.entry_label AS label,
                   b.contract_id AS contract_id,
                   b.canonical_name AS canonical_name,
                   b.major_section AS major_section
            ORDER BY label
            LIMIT $limit
            """,
            ns=self.insurance_namespace,
            q=query,
            limit=max(limit, 8),
        )
        for row in benefit_rows:
            rows.append(
                {
                    "domain": "insurance",
                    "entity_type": "benefit",
                    "id": row["id"],
                    "label": row["label"],
                    "subtitle": " | ".join([item for item in [_text(row.get("contract_id")), _text(row.get("major_section"))] if item]),
                    "score": _score_match(query, row.get("label"), row.get("canonical_name"), row.get("id")),
                    "evidence": {"canonical_name": row.get("canonical_name") or ""},
                }
            )

        exclusion_rows = self._run_query(
            """
            MATCH (e:Exclusion {namespace:$ns})
            WHERE toLower(coalesce(e.reason, '')) CONTAINS toLower($q)
               OR toLower(coalesce(e.group, '')) CONTAINS toLower($q)
               OR toLower(coalesce(e.code, '')) CONTAINS toLower($q)
            RETURN e.code AS id,
                   e.group AS label,
                   e.reason AS reason,
                   coalesce(e.usage_total_rows, 0) AS usage_total_rows
            ORDER BY usage_total_rows DESC, label
            LIMIT $limit
            """,
            ns=self.insurance_namespace,
            q=query,
            limit=max(limit, 8),
        )
        for row in exclusion_rows:
            rows.append(
                {
                    "domain": "insurance",
                    "entity_type": "exclusion",
                    "id": row["id"],
                    "label": row.get("label") or row["id"],
                    "subtitle": _text(row.get("reason")),
                    "score": _score_match(query, row.get("label"), row.get("reason"), row.get("id")),
                    "evidence": {"usage_total_rows": int(row.get("usage_total_rows") or 0)},
                }
            )
        return rows

    def search(self, query: str, domains: Iterable[str] | str | None = None, limit: int = 12) -> dict[str, Any]:
        normalized_domains = _parse_domains(domains)
        if not _text(query):
            return {
                "query": "",
                "domains": normalized_domains,
                "limit": limit,
                "total_hits": 0,
                "hits": [],
                "domain_breakdown": {},
            }

        results: list[dict[str, Any]] = []
        if "ontology" in normalized_domains:
            results.extend(self._search_ontology(query, limit))
        if "claims" in normalized_domains:
            results.extend(self._search_claims(query, limit))
        if "insurance" in normalized_domains:
            results.extend(self._search_insurance(query, limit))

        unique: dict[tuple[str, str, str], dict[str, Any]] = {}
        for item in results:
            key = (item["domain"], item["entity_type"], item["id"])
            current = unique.get(key)
            if current is None or float(item.get("score") or 0.0) > float(current.get("score") or 0.0):
                unique[key] = item

        ordered = sorted(
            unique.values(),
            key=lambda item: (
                -float(item.get("score") or 0.0),
                item.get("domain") or "",
                item.get("entity_type") or "",
                item.get("label") or "",
            ),
        )
        breakdown = Counter(item["domain"] for item in ordered)
        return {
            "query": query,
            "domains": normalized_domains,
            "limit": limit,
            "total_hits": len(ordered),
            "domain_breakdown": dict(breakdown),
            "hits": ordered[: max(1, min(limit, 50))],
        }

    def _trace_ontology_service(
        self,
        service_code: str,
        service_name: str,
        disease_id: str,
        ontology_namespace: str,
    ) -> dict[str, Any]:
        params = {
            "service_code": service_code,
            "service_query": service_name,
            "disease_id": disease_id,
            "ns": ontology_namespace,
        }
        labels = self._schema_labels()
        expected: list[dict[str, Any]] = []
        if "DiseaseHypothesis" in labels:
            expected = self._run_query(
                """
                MATCH (h:DiseaseHypothesis)-[r:DISEASE_EXPECTS_SERVICE]->(svc)
                OPTIONAL MATCH (h)-[:HYPOTHESIS_FOR_DISEASE]->(d:DiseaseEntity)
                WITH h, d, svc, r, coalesce(d.namespace, h.namespace, $ns) AS resolved_namespace
                WHERE resolved_namespace = $ns
                  AND (svc:ProtocolService OR svc:CIService)
                  AND ($disease_id = '' OR coalesce(d.disease_id, h.disease_id) = $disease_id)
                  AND (
                        ($service_code <> '' AND coalesce(svc.service_code, '') = $service_code)
                     OR ($service_code = '' AND $service_query <> '' AND toLower(coalesce(svc.service_name, svc.name, '')) CONTAINS toLower($service_query))
                  )
                RETURN coalesce(d.disease_id, h.disease_id) AS disease_id,
                       coalesce(d.disease_name, h.disease_name) AS disease_name,
                       svc.service_code AS service_code,
                       coalesce(svc.service_name, svc.name) AS service_name,
                       r.role AS role,
                       coalesce(r.category_code, svc.category_code, '') AS category_code,
                       'DISEASE_EXPECTS_SERVICE' AS evidence_type
                ORDER BY disease_name, service_name
                LIMIT 20
                """,
                **params,
            )
        assertion_support = self._run_query(
            """
            MATCH (a:ProtocolAssertion {namespace:$ns})-[:ASSERTION_ABOUT_DISEASE]->(d:DiseaseEntity {namespace:$ns})
            MATCH (a)-[:ASSERTION_INDICATES_SERVICE]->(svc)
            WHERE (svc:ProtocolService OR svc:CIService)
              AND ($disease_id = '' OR d.disease_id = $disease_id)
              AND (
                    ($service_code <> '' AND coalesce(svc.service_code, '') = $service_code)
                 OR ($service_code = '' AND $service_query <> '' AND toLower(coalesce(svc.service_name, svc.name, '')) CONTAINS toLower($service_query))
              )
            OPTIONAL MATCH (sec:ProtocolSection)-[:CONTAINS_ASSERTION]->(a)
            OPTIONAL MATCH (book:ProtocolBook)-[:BOOK_HAS_SECTION]->(sec)
                   RETURN d.disease_id AS disease_id,
                   d.disease_name AS disease_name,
                   svc.service_code AS service_code,
                   coalesce(svc.service_name, svc.name) AS service_name,
                   a.assertion_id AS assertion_id,
                   a.assertion_type AS assertion_type,
                   a.assertion_text AS assertion_text,
                   a.source_page AS source_page,
                   sec.section_title AS section_title,
                   book.book_name AS book_name,
                   'ASSERTION_INDICATES_SERVICE' AS evidence_type
            ORDER BY disease_name, service_name, assertion_id
            LIMIT 30
            """,
            **params,
        )
        contraindications = self._run_query(
            """
            MATCH (a:ProtocolAssertion {namespace:$ns})-[:ASSERTION_ABOUT_DISEASE]->(d:DiseaseEntity {namespace:$ns})
            MATCH (a)-[:ASSERTION_CONTRAINDICATES]->(svc)
            WHERE (svc:ProtocolService OR svc:CIService)
              AND ($disease_id = '' OR d.disease_id = $disease_id)
              AND (
                    ($service_code <> '' AND coalesce(svc.service_code, '') = $service_code)
                 OR ($service_code = '' AND $service_query <> '' AND toLower(coalesce(svc.service_name, svc.name, '')) CONTAINS toLower($service_query))
              )
            OPTIONAL MATCH (sec:ProtocolSection)-[:CONTAINS_ASSERTION]->(a)
            OPTIONAL MATCH (book:ProtocolBook)-[:BOOK_HAS_SECTION]->(sec)
                   RETURN d.disease_id AS disease_id,
                   d.disease_name AS disease_name,
                   svc.service_code AS service_code,
                   coalesce(svc.service_name, svc.name) AS service_name,
                   a.assertion_id AS assertion_id,
                   a.assertion_type AS assertion_type,
                   a.assertion_text AS assertion_text,
                   a.source_page AS source_page,
                   sec.section_title AS section_title,
                   book.book_name AS book_name,
                   'ASSERTION_CONTRAINDICATES' AS evidence_type
            ORDER BY disease_name, service_name, assertion_id
            LIMIT 30
            """,
            **params,
        )
        return {
            "namespace": ontology_namespace,
            "expected_service_edges": expected,
            "assertion_support": assertion_support,
            "assertion_contraindications": contraindications,
            "summary": {
                "expected_service_count": len(expected),
                "assertion_support_count": len(assertion_support),
                "assertion_contraindication_count": len(contraindications),
            },
        }

    def _trace_claims_service(self, service_code: str, service_name: str, disease_id: str) -> dict[str, Any]:
        params = {
            "service_code": service_code,
            "service_query": service_name,
            "disease_id": disease_id,
            "ns": self.claims_namespace,
        }
        rows = self._run_query(
            """
            MATCH (d:CIDisease {namespace:$ns})-[r:CI_INDICATES_SERVICE]->(svc:CIService)
            WHERE ($disease_id = '' OR d.disease_id = $disease_id)
              AND (
                    ($service_code <> '' AND coalesce(svc.service_code, '') = $service_code)
                 OR ($service_code = '' AND $service_query <> '' AND toLower(coalesce(svc.service_name, '')) CONTAINS toLower($service_query))
              )
            RETURN d.disease_id AS disease_id,
                   d.disease_name AS disease_name,
                   svc.service_code AS service_code,
                   svc.service_name AS service_name,
                   svc.category_code AS category_code,
                   svc.category_name AS category_name,
                   r.roles_json AS roles_json,
                   r.evidences_json AS evidences_json,
                   r.max_score AS max_score,
                   r.case_support AS case_support
            ORDER BY coalesce(r.case_support, 0) DESC, coalesce(r.max_score, 0.0) DESC
            LIMIT 20
            """,
            **params,
        )
        return {
            "namespace": self.claims_namespace,
            "service_support": rows,
            "summary": {"service_support_count": len(rows)},
        }

    def _trace_insurance_service(self, service_code: str, service_name: str, contract_id: str, disease_id: str = "") -> dict[str, Any]:
        params = {
            "service_code": service_code,
            "service_query": service_name,
            "contract_id": contract_id,
            "disease_id": disease_id,
            "ns": self.insurance_namespace,
        }
        benefit_support = self._run_query(
            """
            MATCH (svc:CIService)-[r:SUPPORTED_BY_CONTRACT_BENEFIT]->(b:Benefit {namespace:$ns})
            WHERE (
                    ($service_code <> '' AND coalesce(svc.service_code, '') = $service_code)
                 OR ($service_code = '' AND $service_query <> '' AND toLower(coalesce(svc.service_name, '')) CONTAINS toLower($service_query))
              )
              AND ($contract_id = '' OR coalesce(r.contract_id, b.contract_id, '') = $contract_id)
            OPTIONAL MATCH (b)-[:INTERPRETED_AS]->(bi:BenefitInterpretation)
            RETURN svc.service_code AS service_code,
                   svc.service_name AS service_name,
                   coalesce(r.contract_id, b.contract_id) AS contract_id,
                   b.entry_id AS benefit_entry_id,
                   b.entry_label AS benefit_label,
                   b.major_section AS major_section,
                   bi.canonical_name AS interpretation_name,
                   r.support_rows AS support_rows,
                   r.claim_count AS claim_count
            ORDER BY coalesce(r.support_rows, 0) DESC, coalesce(r.claim_count, 0) DESC
            LIMIT 25
            """,
            **params,
        )
        exclusions = self._run_query(
            """
            MATCH (svc:CIService)-[r:EXCLUDED_BY_CONTRACT]->(e:Exclusion {namespace:$ns})
            WHERE coalesce(r.namespace, '') = $ns
              AND (
                    ($service_code <> '' AND coalesce(svc.service_code, '') = $service_code)
                 OR ($service_code = '' AND $service_query <> '' AND toLower(coalesce(svc.service_name, '')) CONTAINS toLower($service_query))
              )
              AND ($contract_id = '' OR coalesce(r.contract_id, '') = $contract_id)
            OPTIONAL MATCH (e)-[:HAS_REASON]->(er:ExclusionReason)
            RETURN svc.service_code AS service_code,
                   svc.service_name AS service_name,
                   r.contract_id AS contract_id,
                   e.code AS exclusion_code,
                   e.group AS exclusion_group,
                   e.reason AS exclusion_reason,
                   er.text AS exclusion_reason_text,
                   r.rows AS matched_rows,
                   r.rule_distribution_json AS rule_distribution_json
            ORDER BY coalesce(r.rows, 0) DESC, exclusion_code
            LIMIT 25
            """,
            **params,
        )
        historical = self._run_query(
            """
            MATCH (svc:CIService)-[r:HISTORICALLY_EXCLUDED]->(ep:ExclusionPattern {namespace:$ns})
            WHERE coalesce(r.namespace, '') = $ns
              AND (
                    ($service_code <> '' AND coalesce(svc.service_code, '') = $service_code)
                 OR ($service_code = '' AND $service_query <> '' AND toLower(coalesce(svc.service_name, '')) CONTAINS toLower($service_query))
              )
            OPTIONAL MATCH (ep)-[:MATCHES_EXCLUSION]->(e:Exclusion)
            RETURN svc.service_code AS service_code,
                   svc.service_name AS service_name,
                   coalesce(e.code, ep.exclusion_code) AS exclusion_code,
                   coalesce(e.reason, ep.reason) AS exclusion_reason,
                   r.rows AS matched_rows
            ORDER BY coalesce(r.rows, 0) DESC, exclusion_code
            LIMIT 20
            """,
            **params,
        )
        disease_context_support = self._run_query(
            """
            MATCH (d:CIDisease)-[m:CI_INDICATES_SERVICE]->(svc:CIService)
            MATCH (d)-[r:CI_SUPPORTS_CONTRACT_BENEFIT]->(b:Benefit {namespace:$ns})
            WHERE (
                    ($service_code <> '' AND coalesce(svc.service_code, '') = $service_code)
                 OR ($service_code = '' AND $service_query <> '' AND toLower(coalesce(svc.service_name, '')) CONTAINS toLower($service_query))
              )
              AND ($disease_id = '' OR d.disease_id = $disease_id)
              AND ($contract_id = '' OR coalesce(r.contract_id, '') = $contract_id)
              AND (
                    $service_code = ''
                 OR coalesce(r.service_codes_json, '') CONTAINS $service_code
              )
            RETURN d.disease_id AS disease_id,
                   d.disease_name AS disease_name,
                   coalesce(r.contract_id, b.contract_id) AS contract_id,
                   b.entry_id AS benefit_entry_id,
                   b.entry_label AS benefit_label,
                   r.supporting_service_count AS supporting_service_count,
                   r.support_rows AS support_rows,
                   r.medical_role_distribution_json AS medical_role_distribution_json
            ORDER BY coalesce(r.support_rows, 0) DESC, disease_name
            LIMIT 20
            """,
            **params,
        )
        disease_context_exclusions = self._run_query(
            """
            MATCH (d:CIDisease)-[m:CI_INDICATES_SERVICE]->(svc:CIService)
            MATCH (d)-[r:CI_FLAGGED_BY_CONTRACT_EXCLUSION]->(e:Exclusion {namespace:$ns})
            WHERE (
                    ($service_code <> '' AND coalesce(svc.service_code, '') = $service_code)
                 OR ($service_code = '' AND $service_query <> '' AND toLower(coalesce(svc.service_name, '')) CONTAINS toLower($service_query))
              )
              AND ($disease_id = '' OR d.disease_id = $disease_id)
              AND ($contract_id = '' OR coalesce(r.contract_id, '') = $contract_id)
              AND (
                    $service_code = ''
                 OR coalesce(r.service_codes_json, '') CONTAINS $service_code
              )
            RETURN d.disease_id AS disease_id,
                   d.disease_name AS disease_name,
                   r.contract_id AS contract_id,
                   e.code AS exclusion_code,
                   e.group AS exclusion_group,
                   e.reason AS exclusion_reason,
                   r.resolution_type AS resolution_type,
                   r.matched_rows AS matched_rows
            ORDER BY coalesce(r.matched_rows, 0) DESC, disease_name
            LIMIT 20
            """,
            **params,
        )
        sign_context_support = self._run_query(
            """
            MATCH (sg:CISign)-[r:CI_SUPPORTS_CONTRACT_BENEFIT]->(b:Benefit {namespace:$ns})
            WHERE ($contract_id = '' OR coalesce(r.contract_id, '') = $contract_id)
              AND ($disease_id = '' OR coalesce(r.disease_ids_json, '') CONTAINS $disease_id)
              AND (
                    $service_code = ''
                 OR coalesce(r.service_codes_json, '') CONTAINS $service_code
              )
            RETURN sg.sign_id AS sign_id,
                   coalesce(sg.canonical_label, sg.text) AS sign_label,
                   r.contract_id AS contract_id,
                   b.entry_id AS benefit_entry_id,
                   b.entry_label AS benefit_label,
                   r.supporting_disease_count AS supporting_disease_count,
                   r.support_rows AS support_rows
            ORDER BY coalesce(r.support_rows, 0) DESC, sign_label
            LIMIT 20
            """,
            **params,
        )
        sign_context_exclusions = self._run_query(
            """
            MATCH (sg:CISign)-[r:CI_FLAGGED_BY_CONTRACT_EXCLUSION]->(e:Exclusion {namespace:$ns})
            WHERE ($contract_id = '' OR coalesce(r.contract_id, '') = $contract_id)
              AND ($disease_id = '' OR coalesce(r.disease_ids_json, '') CONTAINS $disease_id)
              AND (
                    $service_code = ''
                 OR coalesce(r.service_codes_json, '') CONTAINS $service_code
              )
            RETURN sg.sign_id AS sign_id,
                   coalesce(sg.canonical_label, sg.text) AS sign_label,
                   r.contract_id AS contract_id,
                   e.code AS exclusion_code,
                   e.group AS exclusion_group,
                   e.reason AS exclusion_reason,
                   r.resolution_type AS resolution_type,
                   r.matched_rows AS matched_rows
            ORDER BY coalesce(r.matched_rows, 0) DESC, sign_label
            LIMIT 20
            """,
            **params,
        )
        contextual_support_strength: dict[str, int] = {}
        for row in disease_context_support + sign_context_support:
            benefit_id = str(row.get("benefit_entry_id") or "").strip()
            if not benefit_id:
                continue
            support_strength = int(row.get("support_rows") or 0)
            contextual_support_strength[benefit_id] = max(
                support_strength,
                contextual_support_strength.get(benefit_id, 0),
            )
        preferred_benefit_ids: set[str] = set()
        if contextual_support_strength:
            max_strength = max(contextual_support_strength.values())
            min_strength = max(5, int(max_strength * 0.25))
            preferred_benefit_ids = {
                benefit_id
                for benefit_id, strength in contextual_support_strength.items()
                if strength >= min_strength
            }
            disease_context_support = [
                row for row in disease_context_support
                if str(row.get("benefit_entry_id") or "") in preferred_benefit_ids
            ] or disease_context_support
            sign_context_support = [
                row for row in sign_context_support
                if str(row.get("benefit_entry_id") or "") in preferred_benefit_ids
            ] or sign_context_support
        benefit_clause_grounding = self._run_query(
            """
            MATCH (svc:CIService)-[r:SUPPORTED_BY_CONTRACT_BENEFIT]->(b:Benefit {namespace:$ns})
            MATCH (b)-[g:GROUNDED_IN_CONTRACT_CLAUSE]->(cc:ContractClause {namespace:$ns})
            WHERE (
                    ($service_code <> '' AND coalesce(svc.service_code, '') = $service_code)
                 OR ($service_code = '' AND $service_query <> '' AND toLower(coalesce(svc.service_name, '')) CONTAINS toLower($service_query))
              )
              AND ($contract_id = '' OR coalesce(r.contract_id, b.contract_id, '') = $contract_id)
            RETURN svc.service_code AS service_code,
                   coalesce(r.contract_id, b.contract_id) AS contract_id,
                   b.entry_id AS benefit_entry_id,
                   b.entry_label AS benefit_label,
                   cc.clause_id AS clause_id,
                   cc.clause_code AS clause_code,
                   cc.clause_title AS clause_title,
                   cc.sheet_name AS sheet_name,
                   g.match_score AS match_score,
                   g.matched_terms AS matched_terms
            ORDER BY coalesce(g.match_score, 0.0) DESC, clause_code
            LIMIT 25
            """,
            **params,
        )
        if preferred_benefit_ids:
            filtered_grounding = [
                row for row in benefit_clause_grounding
                if str(row.get("benefit_entry_id") or "") in preferred_benefit_ids
            ]
            if filtered_grounding:
                benefit_clause_grounding = filtered_grounding
        exclusion_rulebook_grounding = self._run_query(
            """
            MATCH (svc:CIService)-[r:EXCLUDED_BY_CONTRACT]->(e:Exclusion {namespace:$ns})
            OPTIONAL MATCH (e)-[g:SUPPORTED_BY_RULEBOOK]->(rb:Rulebook {namespace:$ns})
            WHERE (
                    ($service_code <> '' AND coalesce(svc.service_code, '') = $service_code)
                 OR ($service_code = '' AND $service_query <> '' AND toLower(coalesce(svc.service_name, '')) CONTAINS toLower($service_query))
              )
              AND ($contract_id = '' OR coalesce(r.contract_id, '') = $contract_id)
            RETURN svc.service_code AS service_code,
                   r.contract_id AS contract_id,
                   e.code AS exclusion_code,
                   e.reason AS exclusion_reason,
                   rb.rulebook_id AS rulebook_id,
                   rb.rule_code AS rule_code,
                   rb.display_name AS rulebook_name,
                   g.clause_reference_count AS clause_reference_count,
                   g.match_score AS match_score
            ORDER BY coalesce(g.match_score, 0.0) DESC, exclusion_code
            LIMIT 25
            """,
            **params,
        )
        contract_info: dict[str, Any] = {}
        if contract_id:
            rows = self._run_query(
                """
                MATCH (c:InsuranceContract {contract_id:$contract_id, namespace:$ns})
                RETURN c.contract_id AS contract_id,
                       c.product_name AS product_name,
                       c.insurer AS insurer,
                       c.mode AS mode,
                       c.paid_ratio_pct AS paid_ratio_pct,
                       c.requires_preauth AS requires_preauth,
                       c.positive_result_required AS positive_result_required
                LIMIT 1
                """,
                **params,
            )
            contract_info = rows[0] if rows else {}
        return {
            "namespace": self.insurance_namespace,
            "contract_info": contract_info,
            "benefit_support": benefit_support,
            "contract_exclusions": exclusions,
            "historical_exclusions": historical,
            "disease_context_support": disease_context_support,
            "disease_context_exclusions": disease_context_exclusions,
            "sign_context_support": sign_context_support,
            "sign_context_exclusions": sign_context_exclusions,
            "benefit_clause_grounding": benefit_clause_grounding,
            "exclusion_rulebook_grounding": exclusion_rulebook_grounding,
            "summary": {
                "benefit_support_count": len(benefit_support),
                "contract_exclusion_count": len(exclusions),
                "historical_exclusion_count": len(historical),
                "disease_context_support_count": len(disease_context_support),
                "disease_context_exclusion_count": len(disease_context_exclusions),
                "sign_context_support_count": len(sign_context_support),
                "sign_context_exclusion_count": len(sign_context_exclusions),
                "benefit_clause_grounding_count": len(benefit_clause_grounding),
                "exclusion_rulebook_grounding_count": len(exclusion_rulebook_grounding),
            },
        }

    def trace_service(
        self,
        *,
        service_code: str = "",
        service_name: str = "",
        disease_id: str = "",
        contract_id: str = "",
        ontology_namespace: str | None = None,
    ) -> dict[str, Any]:
        if not _text(service_code) and not _text(service_name):
            return {
                "service_query": {
                    "service_code": "",
                    "service_name": "",
                    "disease_id": disease_id,
                    "contract_id": contract_id,
                },
                "error": "Provide service_code or service_name.",
            }

        ontology_ns = ontology_namespace or self.ontology_namespace
        ontology = self._trace_ontology_service(service_code, service_name, disease_id, ontology_ns)
        claims = self._trace_claims_service(service_code, service_name, disease_id)
        insurance = self._trace_insurance_service(service_code, service_name, contract_id, disease_id)

        return {
            "service_query": {
                "service_code": service_code,
                "service_name": service_name,
                "disease_id": disease_id,
                "contract_id": contract_id,
                "ontology_namespace": ontology_ns,
            },
            "ontology": ontology,
            "claims": claims,
            "insurance": insurance,
            "summary": {
                "medical_support_count": (
                    ontology["summary"]["expected_service_count"]
                    + ontology["summary"]["assertion_support_count"]
                    + claims["summary"]["service_support_count"]
                ),
                "medical_contraindication_count": ontology["summary"]["assertion_contraindication_count"],
                "insurance_support_count": insurance["summary"]["benefit_support_count"],
                "insurance_exclusion_count": (
                    insurance["summary"]["contract_exclusion_count"]
                    + insurance["summary"]["historical_exclusion_count"]
                ),
                "disease_contract_support_count": insurance["summary"]["disease_context_support_count"],
                "disease_contract_exclusion_count": insurance["summary"]["disease_context_exclusion_count"],
                "sign_contract_support_count": insurance["summary"]["sign_context_support_count"],
                "sign_contract_exclusion_count": insurance["summary"]["sign_context_exclusion_count"],
                "policy_grounding_count": (
                    insurance["summary"]["benefit_clause_grounding_count"]
                    + insurance["summary"]["exclusion_rulebook_grounding_count"]
                ),
            },
        }

    def health(self) -> dict[str, Any]:
        data_arch = self.data_architecture_store.bootstrap()
        ontology = self._ontology_health()
        claims = self._claims_health()
        insurance = self._insurance_summary()

        warnings: list[str] = list(data_arch.get("warnings") or [])
        if int((ontology.get("summary") or {}).get("diseases", 0)) == 0:
            warnings.append("Ontology namespace currently has 0 diseases.")
        if int((ontology.get("edge_counts") or {}).get("assertion_indicates_service", 0)) == 0:
            warnings.append("Ontology graph has no ASSERTION_INDICATES_SERVICE edges.")
        if int((claims.get("stats") or {}).get("disease_count", 0)) == 0:
            warnings.append("Claims explorer graph currently has 0 diseases.")
        if int(insurance.get("contracts", 0)) == 0:
            warnings.append("Insurance graph currently has 0 contracts.")
        if int(insurance.get("supported_by_contract_benefit", 0)) == 0:
            warnings.append("Insurance graph has no SUPPORTED_BY_CONTRACT_BENEFIT edges.")
        if int(insurance.get("disease_contract_benefit", 0)) == 0:
            warnings.append("Insurance graph has no disease -> contract benefit bridge edges.")
        if int(insurance.get("benefit_clause_grounding", 0)) == 0:
            warnings.append("Insurance graph has no benefit -> contract clause grounding edges.")
        if int(insurance.get("exclusion_rulebook_grounding", 0)) == 0:
            warnings.append("Insurance graph has no exclusion -> rulebook grounding edges.")

        statuses = {
            "ontology": "ready"
            if int((ontology.get("summary") or {}).get("diseases", 0)) > 0
            and int((ontology.get("edge_counts") or {}).get("assertion_indicates_service", 0)) > 0
            else "warning",
            "claims": "ready" if int((claims.get("stats") or {}).get("disease_count", 0)) > 0 else "warning",
            "insurance": "ready"
            if int(insurance.get("contracts", 0)) > 0
            and int(insurance.get("supported_by_contract_benefit", 0)) > 0
            and int(insurance.get("disease_contract_benefit", 0)) > 0
            else "warning",
        }

        return {
            "source": "neo4j",
            "statuses": statuses,
            "summary": {
                "warning_count": len(warnings),
                "ontology_disease_count": int((ontology.get("summary") or {}).get("diseases", 0)),
                "claims_disease_count": int((claims.get("stats") or {}).get("disease_count", 0)),
                "insurance_contract_count": int(insurance.get("contracts", 0)),
            },
            "ontology": ontology,
            "claims": claims,
            "insurance": insurance,
            "warnings": warnings[:32],
        }

    def report(self) -> dict[str, Any]:
        ontology_diseases = list(self.ontology_store.list_diseases(self.ontology_namespace))[:40]
        ontology_hotspots = sorted(
            ontology_diseases,
            key=lambda item: (
                -int(item.get("assertion_count") or 0),
                -int(item.get("chunk_count") or 0),
                _text(item.get("disease_name")),
            ),
        )[:10]
        claims_bootstrap = self.claims_store.bootstrap()
        claims_hotspots = list(claims_bootstrap.get("disease_index") or [])[:10]
        exclusion_rows = self._run_query(
            """
            MATCH (e:Exclusion {namespace:$ns})
            RETURN e.code AS code,
                   e.group AS exclusion_group,
                   e.reason AS reason,
                   coalesce(e.usage_total_rows, 0) AS usage_total_rows,
                   e.usage_gap_vnd AS usage_gap_vnd
            ORDER BY usage_total_rows DESC, code
            LIMIT 10
            """,
            ns=self.insurance_namespace,
        )
        contract_rows = self._run_query(
            """
            MATCH (c:InsuranceContract {namespace:$ns})
            OPTIONAL MATCH (:CIService)-[r:SUPPORTED_BY_CONTRACT_BENEFIT {contract_id:c.contract_id}]->(:Benefit {namespace:$ns})
            RETURN c.contract_id AS contract_id,
                   c.product_name AS product_name,
                   c.insurer AS insurer,
                   count(r) AS supported_edge_count
            ORDER BY supported_edge_count DESC, contract_id
            LIMIT 10
            """,
            ns=self.insurance_namespace,
        )
        medical_contract_rows = self._run_query(
            """
            MATCH (d:CIDisease)-[r:CI_SUPPORTS_CONTRACT_BENEFIT]->(b:Benefit {namespace:$ns})
            RETURN d.disease_id AS disease_id,
                   d.disease_name AS disease_name,
                   r.contract_id AS contract_id,
                   count(r) AS benefit_edge_count,
                   sum(coalesce(r.support_rows, 0)) AS support_rows
            ORDER BY support_rows DESC, benefit_edge_count DESC
            LIMIT 10
            """,
            ns=self.insurance_namespace,
        )
        grounding_rows = self._run_query(
            """
            MATCH (b:Benefit {namespace:$ns})-[r:GROUNDED_IN_CONTRACT_CLAUSE]->(:ContractClause {namespace:$ns})
            RETURN b.contract_id AS contract_id,
                   count(r) AS clause_grounding_count
            ORDER BY clause_grounding_count DESC, contract_id
            LIMIT 10
            """,
            ns=self.insurance_namespace,
        )
        return {
            "mission": "Tom tat nhanh cac diem nong trong graph y khoa, claims, va insurance de agent dieu huong search va tracing.",
            "ontology_hotspots": ontology_hotspots,
            "claims_hotspots": claims_hotspots,
            "insurance_hotspots": {
                "contracts": contract_rows,
                "exclusions": exclusion_rows,
                "medical_contract_context": medical_contract_rows,
                "policy_grounding": grounding_rows,
            },
        }
