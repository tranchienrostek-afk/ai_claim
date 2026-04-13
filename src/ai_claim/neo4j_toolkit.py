from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any

from neo4j import GraphDatabase


@dataclass(slots=True)
class Neo4jConfig:
    uri: str
    user: str
    password: str

    @classmethod
    def from_env(cls) -> "Neo4jConfig":
        return cls(
            uri=os.getenv("NEO4J_URI", os.getenv("neo4j_uri", "bolt://localhost:7688")),
            user=os.getenv("NEO4J_USER", os.getenv("neo4j_user", "neo4j")),
            password=os.getenv("NEO4J_PASSWORD", os.getenv("neo4j_password", "password123")),
        )


class Neo4jToolkit:
    def __init__(self, config: Neo4jConfig | None = None) -> None:
        self.config = config or Neo4jConfig.from_env()
        self.driver = GraphDatabase.driver(
            self.config.uri,
            auth=(self.config.user, self.config.password),
        )

    def close(self) -> None:
        self.driver.close()

    def _run(self, query: str, **params: Any) -> list[dict[str, Any]]:
        with self.driver.session() as session:
            return [dict(record) for record in session.run(query, **params)]

    def _labels(self) -> set[str]:
        rows = self._run("CALL db.labels() YIELD label RETURN collect(label) AS labels")
        return set(rows[0]["labels"] if rows else [])

    def _relationship_types(self) -> set[str]:
        rows = self._run("CALL db.relationshipTypes() YIELD relationshipType RETURN collect(relationshipType) AS rels")
        return set(rows[0]["rels"] if rows else [])

    def graph_health(self) -> dict[str, Any]:
        labels = self._labels()
        rel_types = self._relationship_types()
        ontology_label = "RawDiseaseProfile" if "RawDiseaseProfile" in labels else ""
        ontology = []
        if ontology_label:
            ontology = self._run(
                f"""
                MATCH (d:{ontology_label} {{namespace:'ontology_v2'}})
                RETURN count(d) AS diseases
                """
            )
        claims = self._run(
            """
            MATCH (d:CIDisease {namespace:'claims_insights_explorer_v1'})
            RETURN count(d) AS diseases
            """
        )
        insurance = self._run(
            """
            MATCH (c:InsuranceContract {namespace:'claims_insights_insurance_v1'})
            RETURN count(c) AS contracts
            """
        )
        edges = []
        if "ASSERTION_INDICATES_SERVICE" in rel_types:
            edges = self._run(
                """
                MATCH ()-[r:ASSERTION_INDICATES_SERVICE {namespace:'ontology_v2'}]->()
                RETURN count(r) AS assertion_indicates_service
                """
            )
        return {
            "ontology_disease_count": ontology[0]["diseases"] if ontology else 0,
            "claims_disease_count": claims[0]["diseases"] if claims else 0,
            "insurance_contract_count": insurance[0]["contracts"] if insurance else 0,
            "assertion_indicates_service_count": edges[0]["assertion_indicates_service"] if edges else 0,
            "ontology_label_used": ontology_label or "missing",
        }

    def mapping_key_audit(self) -> dict[str, Any]:
        checks = [
            {
                "name": "ci_service_code",
                "label": "CIService",
                "namespace": "",
                "property": "service_code",
            },
            {
                "name": "canonical_service_maanhxa",
                "label": "CanonicalService",
                "namespace": "",
                "property": "maanhxa",
            },
            {
                "name": "ci_sign_id",
                "label": "CISign",
                "namespace": "",
                "property": "sign_id",
            },
            {
                "name": "ci_disease_id",
                "label": "CIDisease",
                "namespace": "claims_insights_explorer_v1",
                "property": "disease_id",
            },
            {
                "name": "insurance_contract_id",
                "label": "InsuranceContract",
                "namespace": "claims_insights_insurance_v1",
                "property": "contract_id",
            },
            {
                "name": "benefit_entry_id",
                "label": "Benefit",
                "namespace": "claims_insights_insurance_v1",
                "property": "entry_id",
            },
        ]
        labels = self._labels()
        results: list[dict[str, Any]] = []
        for check in checks:
            if check["label"] not in labels:
                results.append(
                    {
                        "name": check["name"],
                        "label": check["label"],
                        "property": check["property"],
                        "status": "missing_label",
                        "duplicate_count": 0,
                        "samples": [],
                    }
                )
                continue
            namespace_clause = "AND n.namespace = $namespace" if check["namespace"] else ""
            rows = self._run(
                f"""
                MATCH (n:{check['label']})
                WHERE coalesce(toString(n.{check['property']}), '') <> ''
                  {namespace_clause}
                WITH n.{check['property']} AS key_value, count(*) AS c
                WHERE c > 1
                RETURN key_value, c
                ORDER BY c DESC, key_value
                LIMIT 20
                """,
                namespace=check["namespace"],
            )
            results.append(
                {
                    "name": check["name"],
                    "label": check["label"],
                    "property": check["property"],
                    "status": "ok" if not rows else "duplicates_detected",
                    "duplicate_count": len(rows),
                    "samples": rows,
                }
            )
        return {
            "checks": results,
            "has_duplicates": any(item["duplicate_count"] > 0 for item in results),
        }

    def list_recent_ci_diseases(self, limit: int = 10) -> list[dict[str, Any]]:
        return self._run(
            """
            MATCH (d:CIDisease)
            WHERE d.namespace = 'claims_insights_explorer_v1'
            WITH d, id(d) AS node_id
            OPTIONAL MATCH (d)-[:CI_HAS_SIGN]->(sign:CISign)
            OPTIONAL MATCH (d)-[:CI_INDICATES_SERVICE]->(svc:CIService)
            RETURN d.disease_id AS disease_id,
                   d.disease_name AS disease_name,
                   node_id AS neo4j_node_id,
                   count(DISTINCT sign) AS sign_count,
                   count(DISTINCT svc) AS service_count
            ORDER BY node_id DESC
            LIMIT $limit
            """,
            limit=max(1, min(int(limit), 30)),
        )

    def query_contract_stats(self, contract_id: str) -> dict[str, Any]:
        rows = self._run(
            """
            MATCH (c:InsuranceContract {contract_id:$contract_id, namespace:'claims_insights_insurance_v1'})
            OPTIONAL MATCH (c)-[:HAS_BENEFIT]->(b:Benefit {namespace:'claims_insights_insurance_v1'})
            OPTIONAL MATCH (c)-[:HAS_EXCLUSION]->(e:Exclusion {namespace:'claims_insights_insurance_v1'})
            RETURN c.contract_id AS contract_id,
                   c.product_name AS product_name,
                   c.mode AS mode,
                   c.paid_ratio_pct AS paid_ratio_pct,
                   count(DISTINCT b) AS benefit_count,
                   count(DISTINCT e) AS exclusion_count
            LIMIT 1
            """,
            contract_id=contract_id,
        )
        return rows[0] if rows else {}

    def query_benefits_for_contract(self, contract_id: str, benefit_name: str = "") -> list[dict[str, Any]]:
        if benefit_name:
            return self._run(
                """
                MATCH (c:InsuranceContract {contract_id:$contract_id, namespace:'claims_insights_insurance_v1'})-[:HAS_BENEFIT]->(b:Benefit {namespace:'claims_insights_insurance_v1'})
                WHERE toLower(coalesce(b.canonical_name,'')) CONTAINS toLower($benefit_name)
                   OR toLower(coalesce(b.entry_label,'')) CONTAINS toLower($benefit_name)
                RETURN b.entry_id AS entry_id,
                       b.entry_label AS entry_label,
                       b.canonical_name AS canonical_name,
                       b.major_section AS major_section,
                       b.subsection AS subsection
                ORDER BY b.entry_label
                LIMIT 30
                """,
                contract_id=contract_id,
                benefit_name=benefit_name,
            )
        return self._run(
            """
            MATCH (c:InsuranceContract {contract_id:$contract_id, namespace:'claims_insights_insurance_v1'})-[:HAS_BENEFIT]->(b:Benefit {namespace:'claims_insights_insurance_v1'})
            RETURN b.entry_id AS entry_id,
                   b.entry_label AS entry_label,
                   b.canonical_name AS canonical_name,
                   b.major_section AS major_section,
                   b.subsection AS subsection
            ORDER BY b.entry_label
            LIMIT 50
            """,
            contract_id=contract_id,
        )

    def query_exclusions_by_contract(self, contract_id: str) -> list[dict[str, Any]]:
        return self._run(
            """
            MATCH (c:InsuranceContract {contract_id:$contract_id, namespace:'claims_insights_insurance_v1'})-[:HAS_EXCLUSION]->(e:Exclusion {namespace:'claims_insights_insurance_v1'})
            OPTIONAL MATCH (e)-[:HAS_REASON]->(er:ExclusionReason)
            RETURN e.code AS code,
                   e.group AS group,
                   e.reason AS reason,
                   er.text AS exclusion_reason_text,
                   e.usage_total_rows AS usage_total_rows,
                   e.usage_gap_vnd AS usage_gap_vnd
            ORDER BY e.usage_total_rows DESC
            LIMIT 50
            """,
            contract_id=contract_id,
        )

    def query_disease_services(self, disease_name: str = "", icd_code: str = "") -> list[dict[str, Any]]:
        """Query services for a disease.  Tries CIDisease first, then
        DiseaseHypothesis→DISEASE_EXPECTS_SERVICE→ProtocolService as fallback."""
        if icd_code:
            rows = self._run(
                """
                MATCH (d:CIDisease {disease_id:$disease_id, namespace:'claims_insights_explorer_v1'})-[:CI_INDICATES_SERVICE]->(s:CIService)
                RETURN d.disease_id AS disease_id,
                       d.disease_name AS disease_name,
                       collect(DISTINCT s.service_code) AS service_codes,
                       collect(DISTINCT s.service_name) AS service_names
                LIMIT 1
                """,
                disease_id=f"disease:{icd_code}",
            )
        else:
            rows = self._run(
                """
                MATCH (d:CIDisease {namespace:'claims_insights_explorer_v1'})
                WHERE toLower(coalesce(d.disease_name,'')) CONTAINS toLower($disease_name)
                OPTIONAL MATCH (d)-[:CI_INDICATES_SERVICE]->(s:CIService)
                RETURN d.disease_id AS disease_id,
                       d.disease_name AS disease_name,
                       collect(DISTINCT s.service_code) AS service_codes,
                       collect(DISTINCT s.service_name) AS service_names
                LIMIT 5
                """,
                disease_name=disease_name,
            )

        if any(r.get("service_codes") for r in rows):
            return rows

        # Fallback: DiseaseHypothesis → DISEASE_EXPECTS_SERVICE → ProtocolService
        if icd_code:
            hyp = self._run(
                """
                MATCH (h:DiseaseHypothesis {icd10:$icd})-[r:DISEASE_EXPECTS_SERVICE]->(s)
                WHERE s:ProtocolService OR s:CIService
                RETURN h.icd10 AS disease_id,
                       h.disease_name AS disease_name,
                       collect(DISTINCT s.service_code) AS service_codes,
                       collect(DISTINCT coalesce(s.service_name, s.canonical_name)) AS service_names,
                       collect(DISTINCT {code: s.service_code, name: coalesce(s.service_name, s.canonical_name), role: r.role}) AS service_details
                LIMIT 1
                """,
                icd=icd_code,
            )
        else:
            hyp = self._run(
                """
                MATCH (h:DiseaseHypothesis)
                WHERE toLower(coalesce(h.disease_name,'')) CONTAINS toLower($disease_name)
                MATCH (h)-[r:DISEASE_EXPECTS_SERVICE]->(s)
                WHERE s:ProtocolService OR s:CIService
                RETURN h.icd10 AS disease_id,
                       h.disease_name AS disease_name,
                       collect(DISTINCT s.service_code) AS service_codes,
                       collect(DISTINCT coalesce(s.service_name, s.canonical_name)) AS service_names,
                       collect(DISTINCT {code: s.service_code, name: coalesce(s.service_name, s.canonical_name), role: r.role}) AS service_details
                LIMIT 5
                """,
                disease_name=disease_name,
            )
        if hyp:
            for r in hyp:
                r["_source"] = "disease_hypothesis_seed"
            return hyp
        return rows

    def query_ci_disease_snapshot(self, disease_id: str = "", disease_name: str = "", limit: int = 10) -> dict[str, Any]:
        if disease_id:
            disease_rows = self._run(
                """
                MATCH (d:CIDisease {disease_id:$disease_id, namespace:'claims_insights_explorer_v1'})
                RETURN d.disease_id AS disease_id, d.disease_name AS disease_name
                LIMIT 1
                """,
                disease_id=disease_id,
            )
        else:
            disease_rows = self._run(
                """
                MATCH (d:CIDisease {namespace:'claims_insights_explorer_v1'})
                WHERE toLower(coalesce(d.disease_name,'')) CONTAINS toLower($disease_name)
                RETURN d.disease_id AS disease_id, d.disease_name AS disease_name
                LIMIT 1
                """,
                disease_name=disease_name,
            )
        if not disease_rows:
            return {}
        disease = disease_rows[0]
        signs = self._run(
            """
            MATCH (d:CIDisease {disease_id:$disease_id, namespace:'claims_insights_explorer_v1'})-[:CI_HAS_SIGN]->(s:CISign)
            RETURN s.sign_id AS sign_id, s.sign_name AS sign_name
            LIMIT $limit
            """,
            disease_id=disease["disease_id"],
            limit=limit,
        )
        services = self._run(
            """
            MATCH (d:CIDisease {disease_id:$disease_id, namespace:'claims_insights_explorer_v1'})-[:CI_INDICATES_SERVICE]->(s:CIService)
            RETURN s.service_code AS service_code, s.service_name AS service_name, s.category_name AS category_name
            LIMIT $limit
            """,
            disease_id=disease["disease_id"],
            limit=limit,
        )
        return {**disease, "signs": signs, "services": services}

    def query_service_exclusions(self, service_code: str = "", service_name: str = "", contract_id: str = "") -> list[dict[str, Any]]:
        if service_code and contract_id:
            return self._run(
                """
                MATCH (s:CIService {service_code:$service_code})-[r:EXCLUDED_BY_CONTRACT {contract_id:$contract_id}]->(e:Exclusion {namespace:'claims_insights_insurance_v1'})
                RETURN s.service_code AS service_code,
                       s.service_name AS service_name,
                       e.code AS exclusion_code,
                       e.group AS exclusion_group,
                       e.reason AS exclusion_reason,
                       r.rows AS evidence_rows
                LIMIT 20
                """,
                service_code=service_code,
                contract_id=contract_id,
            )
        if service_code:
            return self._run(
                """
                MATCH (s:CIService {service_code:$service_code})-[r:EXCLUDED_BY]->(e:Exclusion {namespace:'claims_insights_insurance_v1'})
                RETURN s.service_code AS service_code,
                       s.service_name AS service_name,
                       e.code AS exclusion_code,
                       e.group AS exclusion_group,
                       e.reason AS exclusion_reason,
                       r.rows AS evidence_rows
                LIMIT 20
                """,
                service_code=service_code,
            )
        if service_name:
            return self._run(
                """
                MATCH (s:CIService)-[r:EXCLUDED_BY]->(e:Exclusion {namespace:'claims_insights_insurance_v1'})
                WHERE toLower(coalesce(s.service_name,'')) CONTAINS toLower($service_name)
                RETURN s.service_code AS service_code,
                       s.service_name AS service_name,
                       e.code AS exclusion_code,
                       e.group AS exclusion_group,
                       e.reason AS exclusion_reason,
                       r.rows AS evidence_rows
                LIMIT 20
                """,
                service_name=service_name,
            )
        return []

    def query_clinical_service_info(self, service_code: str) -> dict[str, Any]:
        rows = self._run(
            """
            MATCH (s:CIService {service_code:$service_code})
            OPTIONAL MATCH (d:CIDisease)-[:CI_INDICATES_SERVICE]->(s)
            RETURN s.service_code AS code,
                   s.service_name AS canonical_name,
                   s.category_code AS category_code,
                   s.category_name AS category_name,
                   collect(DISTINCT d.disease_id) AS related_icds,
                   collect(DISTINCT d.disease_name) AS related_diseases
            LIMIT 1
            """,
            service_code=service_code,
        )
        return rows[0] if rows else {}

    def trace_service_evidence(self, service_name: str, disease_id: str = "", contract_id: str = "") -> dict[str, Any]:
        medical_hits = self._run(
            """
            MATCH (d:CIDisease)-[:CI_INDICATES_SERVICE]->(s:CIService)
            WHERE ($disease_id = '' OR d.disease_id = $disease_id)
              AND toLower(coalesce(s.service_name,'')) CONTAINS toLower($service_name)
            RETURN d.disease_id AS disease_id,
                   d.disease_name AS disease_name,
                   s.service_code AS service_code,
                   s.service_name AS service_name
            LIMIT 10
            """,
            disease_id=disease_id,
            service_name=service_name,
        )
        insurance_hits = []
        if contract_id:
            insurance_hits = self._run(
                """
                MATCH (s:CIService)-[:SUPPORTED_BY_CONTRACT_BENEFIT {contract_id:$contract_id}]->(b:Benefit)
                WHERE toLower(coalesce(s.service_name,'')) CONTAINS toLower($service_name)
                RETURN s.service_code AS service_code,
                       s.service_name AS service_name,
                       b.entry_id AS benefit_entry_id,
                       b.entry_label AS benefit_label
                LIMIT 10
                """,
                contract_id=contract_id,
                service_name=service_name,
            )
        return {
            "medical_support": medical_hits,
            "insurance_support": insurance_hits,
            "summary": {
                "medical_support_count": len(medical_hits),
                "insurance_support_count": len(insurance_hits),
            },
        }
