from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from neo4j import GraphDatabase


NAMESPACE = "claims_insights_explorer_v1"

# Node caps to keep SVG rendering performant (~70 nodes max per disease)
MAX_SIGNS = 25
MAX_SERVICES = 30
MAX_OBSERVATIONS = 15


class ClaimsInsightsGraphStore:
    def __init__(self, bundle_path: Path | None = None):
        self.bundle_path = bundle_path
        self.user = os.getenv("NEO4J_USER", os.getenv("neo4j_user", "neo4j"))
        self.password = os.getenv("NEO4J_PASSWORD", os.getenv("neo4j_password", "password123"))
        self._driver = None
        self._uri = None

    def _candidate_uris(self) -> list[str]:
        env_uri = os.getenv("NEO4J_URI", os.getenv("neo4j_uri", "")).strip()
        candidates = [
            env_uri,
            "bolt://localhost:7688",
            "bolt://host.docker.internal:7688",
            "bolt://neo4j:7687",
        ]
        result = []
        seen = set()
        for candidate in candidates:
            if not candidate or candidate in seen:
                continue
            seen.add(candidate)
            result.append(candidate)
        return result

    @property
    def driver(self):
        if self._driver is not None:
            return self._driver

        last_error = None
        for uri in self._candidate_uris():
            try:
                driver = GraphDatabase.driver(uri, auth=(self.user, self.password), connection_timeout=10)
                driver.verify_connectivity()
                self._driver = driver
                self._uri = uri
                return self._driver
            except Exception as exc:  # pragma: no cover - runtime fallback
                last_error = exc
        raise RuntimeError(f"Could not connect to Neo4j with any candidate URI: {last_error}")

    @property
    def uri(self) -> str:
        if self._uri:
            return self._uri
        _ = self.driver
        return self._uri or ""

    def close(self) -> None:
        if self._driver is not None:
            self._driver.close()
            self._driver = None

    def bundle_exists(self) -> bool:
        return bool(self.bundle_path and self.bundle_path.exists())

    def load_bundle(self) -> dict[str, Any]:
        if not self.bundle_path:
            raise FileNotFoundError("Bundle path not configured")
        with self.bundle_path.open("r", encoding="utf-8") as handle:
            return json.load(handle)

    def ensure_constraints(self) -> None:
        statements = [
            "CREATE CONSTRAINT ci_disease_id IF NOT EXISTS FOR (n:CIDisease) REQUIRE n.disease_id IS UNIQUE",
            "CREATE CONSTRAINT ci_service_code IF NOT EXISTS FOR (n:CIService) REQUIRE n.service_code IS UNIQUE",
            "CREATE CONSTRAINT ci_sign_id IF NOT EXISTS FOR (n:CISign) REQUIRE n.sign_id IS UNIQUE",
            "CREATE CONSTRAINT ci_observation_code IF NOT EXISTS FOR (n:CIObservation) REQUIRE n.observation_node_code IS UNIQUE",
        ]
        with self.driver.session() as session:
            for statement in statements:
                session.run(statement)

    def clear_namespace(self) -> None:
        with self.driver.session() as session:
            session.run(
                """
                MATCH (n)
                WHERE n.namespace = $namespace
                DETACH DELETE n
                """,
                namespace=NAMESPACE,
            )

    def import_bundle(self, bundle: dict[str, Any], clear_existing: bool = True) -> dict[str, Any]:
        self.ensure_constraints()
        if clear_existing:
            self.clear_namespace()

        disease_index = bundle.get("disease_index", [])
        graphs = bundle.get("graphs", {})
        service_registry: dict[str, dict[str, Any]] = {}
        sign_registry: dict[str, dict[str, Any]] = {}
        observation_registry: dict[str, dict[str, Any]] = {}
        service_observation_rollup: dict[tuple[str, str], dict[str, Any]] = {}

        with self.driver.session() as session:
            for disease_stub in disease_index:
                disease_id = disease_stub["disease_id"]
                graph = graphs.get(disease_id) or {}
                summary = graph.get("summary") or {}

                session.run(
                    """
                    MERGE (d:CIDisease {disease_id:$disease_id})
                    SET d.namespace = $namespace,
                        d.icd10 = $icd10,
                        d.icd_group = $icd_group,
                        d.disease_name = $disease_name,
                        d.case_count = $case_count,
                        d.message_count = $message_count,
                        d.linked_service_count = $linked_service_count,
                        d.sign_count = $sign_count,
                        d.disease_observation_count = $disease_observation_count,
                        d.top_hospitals_json = $top_hospitals_json,
                        d.top_departments_json = $top_departments_json,
                        d.diagnosis_examples_json = $diagnosis_examples_json,
                        d.sample_case_ids_json = $sample_case_ids_json
                    """,
                    namespace=NAMESPACE,
                    disease_id=disease_id,
                    icd10=summary.get("icd10") or disease_stub.get("icd10"),
                    icd_group=summary.get("icd_group") or disease_stub.get("icd_group"),
                    disease_name=summary.get("disease_name") or disease_stub.get("disease_name"),
                    case_count=int(summary.get("case_count") or disease_stub.get("case_count") or 0),
                    message_count=int(summary.get("message_count") or disease_stub.get("message_count") or 0),
                    linked_service_count=int(summary.get("linked_service_count") or disease_stub.get("linked_service_count") or 0),
                    sign_count=int(summary.get("sign_count") or disease_stub.get("sign_count") or 0),
                    disease_observation_count=int(summary.get("disease_observation_count") or disease_stub.get("disease_observation_count") or 0),
                    top_hospitals_json=json.dumps(summary.get("top_hospitals", []), ensure_ascii=False),
                    top_departments_json=json.dumps(summary.get("top_departments", []), ensure_ascii=False),
                    diagnosis_examples_json=json.dumps(summary.get("diagnosis_examples", []), ensure_ascii=False),
                    sample_case_ids_json=json.dumps(summary.get("sample_case_ids", []), ensure_ascii=False),
                )

                for sign in graph.get("signs", []):
                    sign_registry[sign["id"]] = sign
                    session.run(
                        """
                        MERGE (s:CISign {sign_id:$sign_id})
                        SET s.namespace = $namespace,
                            s.text = $text,
                            s.normalized_key = $normalized_key
                        """,
                        namespace=NAMESPACE,
                        sign_id=sign["id"],
                        text=sign.get("label"),
                        normalized_key=sign.get("normalized_key"),
                    )
                    session.run(
                        """
                        MATCH (d:CIDisease {disease_id:$disease_id})
                        MATCH (s:CISign {sign_id:$sign_id})
                        MERGE (d)-[r:CI_HAS_SIGN]->(s)
                        SET r.namespace = $namespace,
                            r.support_cases = $support_cases
                        """,
                        namespace=NAMESPACE,
                        disease_id=disease_id,
                        sign_id=sign["id"],
                        support_cases=int(sign.get("support_cases") or 0),
                    )

                for service in graph.get("services", []):
                    service_registry[service["service_code"]] = service
                    session.run(
                        """
                        MERGE (s:CIService {service_code:$service_code})
                        SET s.namespace = $namespace,
                            s.service_name = $service_name,
                            s.category_code = $category_code,
                            s.category_name = $category_name,
                            s.avg_cost_vnd = $avg_cost_vnd,
                            s.total_occurrences = $total_occurrences,
                            s.variants_preview_json = $variants_preview_json
                        """,
                        namespace=NAMESPACE,
                        service_code=service["service_code"],
                        service_name=service.get("label"),
                        category_code=service.get("category_code"),
                        category_name=service.get("category_name"),
                        avg_cost_vnd=service.get("avg_cost_vnd"),
                        total_occurrences=int(service.get("total_occurrences") or 0),
                        variants_preview_json=json.dumps(service.get("variants_preview", []), ensure_ascii=False),
                    )
                    session.run(
                        """
                        MATCH (d:CIDisease {disease_id:$disease_id})
                        MATCH (s:CIService {service_code:$service_code})
                        MERGE (d)-[r:CI_INDICATES_SERVICE]->(s)
                        SET r.namespace = $namespace,
                            r.roles_json = $roles_json,
                            r.evidences_json = $evidences_json,
                            r.max_score = $max_score,
                            r.case_support = $case_support,
                            r.link_count = $link_count,
                            r.guideline_hits = $guideline_hits,
                            r.protocol_excel_hits = $protocol_excel_hits,
                            r.statistical_hits = $statistical_hits,
                            r.max_pmi = $max_pmi,
                            r.max_co_occurrence = $max_co_occurrence,
                            r.observations_json = $observations_json
                        """,
                        namespace=NAMESPACE,
                        disease_id=disease_id,
                        service_code=service["service_code"],
                        roles_json=json.dumps(service.get("roles", []), ensure_ascii=False),
                        evidences_json=json.dumps(service.get("evidences", []), ensure_ascii=False),
                        max_score=float(service.get("max_score") or 0.0),
                        case_support=int(service.get("case_support") or 0),
                        link_count=int(service.get("link_count") or 0),
                        guideline_hits=int(service.get("guideline_hits") or 0),
                        protocol_excel_hits=int(service.get("protocol_excel_hits") or 0),
                        statistical_hits=int(service.get("statistical_hits") or 0),
                        max_pmi=float(service.get("max_pmi") or 0.0),
                        max_co_occurrence=int(service.get("max_co_occurrence") or 0),
                        observations_json=json.dumps(service.get("observations", []), ensure_ascii=False),
                    )

                    for observation in service.get("observations", []):
                        observation_registry[observation["observation_node_code"]] = observation
                        key = (service["service_code"], observation["observation_node_code"])
                        bucket = service_observation_rollup.setdefault(
                            key,
                            {
                                "count": 0,
                                "result_flag_counter": {},
                                "abnormality_counter": {},
                            },
                        )
                        bucket["count"] += int(observation.get("count") or 0)

                for observation in graph.get("disease_observations", []):
                    observation_registry[observation["observation_node_code"]] = observation
                    session.run(
                        """
                        MERGE (o:CIObservation {observation_node_code:$observation_node_code})
                        SET o.namespace = $namespace,
                            o.name = $name,
                            o.category_code = $category_code,
                            o.category_name = $category_name
                        """,
                        namespace=NAMESPACE,
                        observation_node_code=observation["observation_node_code"],
                        name=observation.get("name"),
                        category_code=observation.get("category_code"),
                        category_name=observation.get("category_name"),
                    )
                    session.run(
                        """
                        MATCH (d:CIDisease {disease_id:$disease_id})
                        MATCH (o:CIObservation {observation_node_code:$observation_node_code})
                        MERGE (d)-[r:CI_HAS_OBSERVATION]->(o)
                        SET r.namespace = $namespace,
                            r.count = $count,
                            r.result_flag_counter_json = $result_flag_counter_json,
                            r.polarity_counter_json = $polarity_counter_json,
                            r.abnormality_counter_json = $abnormality_counter_json,
                            r.sample_results_json = $sample_results_json
                        """,
                        namespace=NAMESPACE,
                        disease_id=disease_id,
                        observation_node_code=observation["observation_node_code"],
                        count=int(observation.get("count") or 0),
                        result_flag_counter_json=json.dumps(observation.get("result_flag_counter", {}), ensure_ascii=False),
                        polarity_counter_json=json.dumps(observation.get("polarity_counter", {}), ensure_ascii=False),
                        abnormality_counter_json=json.dumps(observation.get("abnormality_counter", {}), ensure_ascii=False),
                        sample_results_json=json.dumps(observation.get("sample_results", []), ensure_ascii=False),
                    )

            for observation_code, observation in observation_registry.items():
                session.run(
                    """
                    MERGE (o:CIObservation {observation_node_code:$observation_node_code})
                    SET o.namespace = $namespace,
                        o.name = $name,
                        o.category_code = $category_code,
                        o.category_name = $category_name
                    """,
                    namespace=NAMESPACE,
                    observation_node_code=observation_code,
                    name=observation.get("name"),
                    category_code=observation.get("category_code"),
                    category_name=observation.get("category_name"),
                )

            for (service_code, observation_code), bucket in service_observation_rollup.items():
                session.run(
                    """
                    MATCH (s:CIService {service_code:$service_code})
                    MATCH (o:CIObservation {observation_node_code:$observation_node_code})
                    MERGE (s)-[r:CI_SUPPORTS_OBSERVATION]->(o)
                    SET r.namespace = $namespace,
                        r.count = $count
                    """,
                    namespace=NAMESPACE,
                    service_code=service_code,
                    observation_node_code=observation_code,
                    count=int(bucket.get("count") or 0),
                )

        return {
            "namespace": NAMESPACE,
            "neo4j_uri": self.uri,
            "diseases": len(disease_index),
            "services": len(service_registry),
            "signs": len(sign_registry),
            "observations": len(observation_registry),
        }

    def bootstrap(self) -> dict[str, Any]:
        with self.driver.session() as session:
            stats_record = session.run(
                """
                MATCH (d:CIDisease {namespace:$namespace})
                RETURN count(d) AS disease_count,
                       count(CASE WHEN d.case_count > 0 THEN 1 END) AS diseases_with_case_evidence
                """,
                namespace=NAMESPACE,
            ).single()
            service_edges = session.run(
                """
                MATCH (:CIDisease {namespace:$namespace})-[r:CI_INDICATES_SERVICE]->(:CIService)
                RETURN count(r) AS c
                """,
                namespace=NAMESPACE,
            ).single()["c"]
            sign_edges = session.run(
                """
                MATCH (:CIDisease {namespace:$namespace})-[r:CI_HAS_SIGN]->(:CISign)
                RETURN count(r) AS c
                """,
                namespace=NAMESPACE,
            ).single()["c"]
            observation_edges = session.run(
                """
                MATCH (:CIDisease {namespace:$namespace})-[r:CI_HAS_OBSERVATION]->(:CIObservation)
                RETURN count(r) AS c
                """,
                namespace=NAMESPACE,
            ).single()["c"]
            result = session.run(
                """
                MATCH (d:CIDisease {namespace:$namespace})
                RETURN d.disease_id AS disease_id,
                       d.icd10 AS icd10,
                       d.icd_group AS icd_group,
                       d.disease_name AS disease_name,
                       d.case_count AS case_count,
                       d.message_count AS message_count,
                       d.linked_service_count AS linked_service_count,
                       d.sign_count AS sign_count,
                       d.disease_observation_count AS disease_observation_count,
                       d.top_hospitals_json AS top_hospitals_json
                ORDER BY d.case_count DESC, d.linked_service_count DESC, d.disease_name
                """,
                namespace=NAMESPACE,
            )
            disease_index = []
            for record in result:
                item = dict(record)
                top_hospitals = json.loads(item.pop("top_hospitals_json") or "[]")
                item["top_hospital"] = top_hospitals[0]["name"] if top_hospitals else ""
                disease_index.append(item)

        return {
            "source": "neo4j",
            "namespace": NAMESPACE,
            "neo4j_uri": self.uri,
            "stats": {
                "disease_count": stats_record["disease_count"],
                "diseases_with_case_evidence": stats_record["diseases_with_case_evidence"],
                "service_link_count": service_edges,
                "sign_link_count": sign_edges,
                "disease_observation_count": observation_edges,
            },
            "disease_index": disease_index,
        }

    def disease_graph(self, disease_id: str) -> dict[str, Any] | None:
        with self.driver.session() as session:
            summary_record = session.run(
                """
                MATCH (d:CIDisease {namespace:$namespace, disease_id:$disease_id})
                RETURN d
                """,
                namespace=NAMESPACE,
                disease_id=disease_id,
            ).single()
            if summary_record is None:
                return None
            disease = dict(summary_record["d"])

            sign_records = session.run(
                """
                MATCH (d:CIDisease {namespace:$namespace, disease_id:$disease_id})-[r:CI_HAS_SIGN]->(s:CISign)
                RETURN s.sign_id AS id,
                       'sign' AS type,
                       s.text AS label,
                       r.support_cases AS support_cases,
                       s.normalized_key AS normalized_key
                ORDER BY r.support_cases DESC, s.text
                """,
                namespace=NAMESPACE,
                disease_id=disease_id,
            )
            signs = [dict(record) for record in sign_records][:MAX_SIGNS]

            service_records = session.run(
                """
                MATCH (d:CIDisease {namespace:$namespace, disease_id:$disease_id})-[r:CI_INDICATES_SERVICE]->(s:CIService)
                RETURN s.service_code AS service_code,
                       s.service_name AS label,
                       s.category_code AS category_code,
                       s.category_name AS category_name,
                       s.avg_cost_vnd AS avg_cost_vnd,
                       s.total_occurrences AS total_occurrences,
                       s.variants_preview_json AS variants_preview_json,
                       r.roles_json AS roles_json,
                       r.evidences_json AS evidences_json,
                       r.max_score AS max_score,
                       r.case_support AS case_support,
                       r.link_count AS link_count,
                       r.guideline_hits AS guideline_hits,
                       r.protocol_excel_hits AS protocol_excel_hits,
                       r.statistical_hits AS statistical_hits,
                       r.max_pmi AS max_pmi,
                       r.max_co_occurrence AS max_co_occurrence,
                       r.observations_json AS observations_json
                ORDER BY r.case_support DESC, r.max_score DESC, s.service_name
                """,
                namespace=NAMESPACE,
                disease_id=disease_id,
            )
            services = []
            for record in service_records:
                item = dict(record)
                services.append(
                    {
                        "id": f"service:{disease.get('icd10')}:{item['service_code']}",
                        "type": "service",
                        "service_code": item["service_code"],
                        "label": item["label"],
                        "category_code": item["category_code"],
                        "category_name": item["category_name"],
                        "avg_cost_vnd": item["avg_cost_vnd"],
                        "total_occurrences": item["total_occurrences"],
                        "variants_preview": json.loads(item["variants_preview_json"] or "[]"),
                        "roles": json.loads(item["roles_json"] or "[]"),
                        "evidences": json.loads(item["evidences_json"] or "[]"),
                        "max_score": item["max_score"],
                        "case_support": item["case_support"],
                        "link_count": item["link_count"],
                        "guideline_hits": item["guideline_hits"],
                        "protocol_excel_hits": item["protocol_excel_hits"],
                        "statistical_hits": item["statistical_hits"],
                        "max_pmi": item["max_pmi"],
                        "max_co_occurrence": item["max_co_occurrence"],
                        "observations": json.loads(item["observations_json"] or "[]"),
                    }
                )
            # Cap services to prevent SVG rendering issues
            services = services[:MAX_SERVICES]

            observation_records = session.run(
                """
                MATCH (d:CIDisease {namespace:$namespace, disease_id:$disease_id})-[r:CI_HAS_OBSERVATION]->(o:CIObservation)
                RETURN o.observation_node_code AS observation_node_code,
                       o.name AS name,
                       o.category_code AS category_code,
                       o.category_name AS category_name,
                       r.count AS count,
                       r.result_flag_counter_json AS result_flag_counter_json,
                       r.polarity_counter_json AS polarity_counter_json,
                       r.abnormality_counter_json AS abnormality_counter_json,
                       r.sample_results_json AS sample_results_json
                ORDER BY r.count DESC, o.name
                """,
                namespace=NAMESPACE,
                disease_id=disease_id,
            )
            disease_observations = []
            for record in observation_records:
                item = dict(record)
                disease_observations.append(
                    {
                        "id": f"disease-observation:{disease.get('icd10')}:{item['observation_node_code']}",
                        "observation_node_code": item["observation_node_code"],
                        "name": item["name"],
                        "category_code": item["category_code"],
                        "category_name": item["category_name"],
                        "count": item["count"],
                        "result_flag_counter": json.loads(item["result_flag_counter_json"] or "{}"),
                        "polarity_counter": json.loads(item["polarity_counter_json"] or "{}"),
                        "abnormality_counter": json.loads(item["abnormality_counter_json"] or "{}"),
                        "sample_results": json.loads(item["sample_results_json"] or "[]"),
                    }
                )
            # Cap disease_observations to prevent SVG rendering issues
            disease_observations = disease_observations[:MAX_OBSERVATIONS]

        summary = {
            "icd10": disease.get("icd10"),
            "disease_name": disease.get("disease_name"),
            "icd_group": disease.get("icd_group"),
            "case_count": disease.get("case_count", 0),
            "message_count": disease.get("message_count", 0),
            "linked_service_count": disease.get("linked_service_count", 0),
            "sign_count": disease.get("sign_count", 0),
            "disease_observation_count": disease.get("disease_observation_count", 0),
            "top_hospitals": json.loads(disease.get("top_hospitals_json") or "[]"),
            "top_departments": json.loads(disease.get("top_departments_json") or "[]"),
            "diagnosis_examples": json.loads(disease.get("diagnosis_examples_json") or "[]"),
            "sample_case_ids": json.loads(disease.get("sample_case_ids_json") or "[]"),
        }
        # Update summary counts to reflect capped values
        summary["linked_service_count"] = len(services)
        summary["sign_count"] = len(signs)
        summary["disease_observation_count"] = len(disease_observations)

        nodes = [
            {
                "id": disease_id,
                "type": "disease",
                "label": summary["disease_name"],
                "subtitle": summary["icd10"],
                "metrics": {
                    "linked_services": summary["linked_service_count"],
                    "matched_cases": summary["case_count"],
                    "signs": summary["sign_count"],
                    "observation_nodes": summary["disease_observation_count"],
                },
            }
        ]
        nodes.extend(signs)
        nodes.extend(services)

        edges = []
        for sign in signs:
            edges.append(
                {
                    "id": f"edge:{disease_id}:{sign['id']}",
                    "source": disease_id,
                    "target": sign["id"],
                    "type": "disease_sign",
                    "label": f"dau hieu • {sign['support_cases']} ca",
                    "details": {"relationship": "Disease -> Sign", "support_cases": sign["support_cases"]},
                }
            )

        # Cap service-level observations to prevent SVG rendering issues
        service_observation_count = 0
        for service in services:
            edges.append(
                {
                    "id": f"edge:{disease_id}:{service['id']}",
                    "source": disease_id,
                    "target": service["id"],
                    "type": "disease_service",
                    "label": f"{', '.join(service['roles']) or 'service'} • score {service['max_score']}",
                    "details": {
                        "relationship": "Disease -> Service",
                        "roles": service["roles"],
                        "evidence_modes": service["evidences"],
                        "case_support": service["case_support"],
                        "guideline_hits": service["guideline_hits"],
                        "protocol_excel_hits": service["protocol_excel_hits"],
                        "statistical_hits": service["statistical_hits"],
                        "max_pmi": service["max_pmi"],
                        "max_co_occurrence": service["max_co_occurrence"],
                    },
                }
            )
            for observation in service.get("observations", []):
                if service_observation_count >= MAX_OBSERVATIONS:
                    break
                observation_node_id = f"observation:{summary['icd10']}:{service['service_code']}:{observation['observation_node_code']}"
                nodes.append(
                    {
                        "id": observation_node_id,
                        "type": "observation",
                        "label": observation["name"],
                        "observation_node_code": observation["observation_node_code"],
                        "category_code": observation["category_code"],
                        "category_name": observation["category_name"],
                        "count": observation["count"],
                        "result_flag_counter": observation.get("result_flag_counter", {}),
                        "abnormality_counter": observation.get("abnormality_counter", {}),
                        "sample_results": observation.get("sample_results", []),
                        "hidden": True,
                        "parent_service_id": service["id"],
                    }
                )
                edges.append(
                    {
                        "id": f"edge:{service['id']}:{observation_node_id}",
                        "source": service["id"],
                        "target": observation_node_id,
                        "type": "service_observation",
                        "label": f"{observation['count']} ket qua",
                        "details": {
                            "relationship": "Service -> Observation",
                            "count": observation["count"],
                            "result_flag_counter": observation.get("result_flag_counter", {}),
                            "abnormality_counter": observation.get("abnormality_counter", {}),
                        },
                        "hidden": True,
                    }
                )
                service_observation_count += 1

        return {
            "summary": summary,
            "nodes": nodes,
            "edges": edges,
            "signs": signs,
            "services": services,
            "disease_observations": disease_observations,
        }
