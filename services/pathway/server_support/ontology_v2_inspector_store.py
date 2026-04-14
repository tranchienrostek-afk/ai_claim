from __future__ import annotations

import os
from collections import Counter
from typing import Any

from neo4j import GraphDatabase


DEFAULT_NAMESPACE = "ontology_v2"


class OntologyV2InspectorStore:
    def __init__(self):
        self.user = os.getenv("NEO4J_USER", os.getenv("neo4j_user", "neo4j"))
        self.password = os.getenv("NEO4J_PASSWORD", os.getenv("neo4j_password", "password123"))
        self._driver = None
        self._uri = None

    def _candidate_uris(self) -> list[str]:
        env_uri = os.getenv("NEO4J_URI", os.getenv("neo4j_uri", "")).strip()
        candidates = [
            env_uri,
            "bolt://host.docker.internal:7688",
            "bolt://localhost:7688",
            "bolt://neo4j:7687",
        ]
        result: list[str] = []
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

        errors: list[str] = []
        for uri in self._candidate_uris():
            try:
                driver = GraphDatabase.driver(uri, auth=(self.user, self.password), connection_timeout=3)
                driver.verify_connectivity()
                self._driver = driver
                self._uri = uri
                return self._driver
            except Exception as exc:  # pragma: no cover - runtime fallback
                errors.append(f"{uri}: {exc}")
        raise RuntimeError(
            "Could not connect to Neo4j with any candidate URI:\n  - "
            + "\n  - ".join(errors)
        )

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

    def _default_namespace(self, session) -> str:
        records = session.run(
            """
            MATCH (d:DiseaseEntity)
            WHERE d.namespace IS NOT NULL AND d.namespace <> ''
            RETURN d.namespace AS namespace, count(*) AS c
            ORDER BY c DESC, namespace
            """
        )
        namespaces = [r["namespace"] for r in records]
        if DEFAULT_NAMESPACE in namespaces:
            return DEFAULT_NAMESPACE
        return namespaces[0] if namespaces else DEFAULT_NAMESPACE

    def list_namespaces(self) -> list[dict[str, Any]]:
        with self.driver.session() as session:
            records = session.run(
                """
                MATCH (d:DiseaseEntity)
                WHERE d.namespace IS NOT NULL AND d.namespace <> ''
                OPTIONAL MATCH (c:RawChunk {namespace: d.namespace})
                WITH d.namespace AS namespace,
                     count(DISTINCT d) AS disease_count,
                     count(DISTINCT c) AS chunk_count
                RETURN namespace, disease_count, chunk_count
                ORDER BY disease_count DESC, namespace
                """
            )
            return [dict(record) for record in records]

    def namespace_summary(self, namespace: str) -> dict[str, Any]:
        with self.driver.session() as session:
            counts = {
                "diseases": session.run(
                    "MATCH (n:DiseaseEntity {namespace:$ns}) RETURN count(n) AS c", ns=namespace
                ).single()["c"],
                "chunks": session.run(
                    "MATCH (n:RawChunk {namespace:$ns}) RETURN count(n) AS c", ns=namespace
                ).single()["c"],
                "sign_mentions": session.run(
                    "MATCH (n:RawSignMention {namespace:$ns}) RETURN count(n) AS c", ns=namespace
                ).single()["c"],
                "service_mentions": session.run(
                    "MATCH (n:RawServiceMention {namespace:$ns}) RETURN count(n) AS c", ns=namespace
                ).single()["c"],
                "observation_mentions": session.run(
                    "MATCH (n:RawObservationMention {namespace:$ns}) RETURN count(n) AS c", ns=namespace
                ).single()["c"],
                "assertions": session.run(
                    "MATCH (n:ProtocolAssertion {namespace:$ns}) RETURN count(n) AS c", ns=namespace
                ).single()["c"],
                "summaries": session.run(
                    "MATCH (n:ProtocolDiseaseSummary {namespace:$ns}) RETURN count(n) AS c", ns=namespace
                ).single()["c"],
            }
        return counts

    def bootstrap(self, pdfs: list[dict[str, Any]] | None = None, namespace: str | None = None) -> dict[str, Any]:
        with self.driver.session() as session:
            default_namespace = self._default_namespace(session)
        active_namespace = namespace or default_namespace
        return {
            "source": "neo4j",
            "neo4j_uri": self.uri,
            "namespaces": self.list_namespaces(),
            "default_namespace": default_namespace,
            "active_namespace": active_namespace,
            "summary": self.namespace_summary(active_namespace),
            "diseases": self.list_diseases(active_namespace),
            "pdfs": pdfs or [],
        }

    def list_diseases(self, namespace: str) -> list[dict[str, Any]]:
        with self.driver.session() as session:
            records = session.run(
                """
                MATCH (d:DiseaseEntity {namespace:$ns})
                OPTIONAL MATCH (c:RawChunk {namespace:$ns})-[:CHUNK_ABOUT_DISEASE]->(d)
                OPTIONAL MATCH (s:ProtocolDiseaseSummary {namespace:$ns})-[:SUMMARIZES]->(d)
                OPTIONAL MATCH (a:ProtocolAssertion {namespace:$ns})-[:ASSERTION_ABOUT_DISEASE]->(d)
                RETURN d.disease_id AS disease_id,
                       d.disease_name AS disease_name,
                       count(DISTINCT c) AS chunk_count,
                       count(DISTINCT a) AS assertion_count,
                       count(DISTINCT s) AS summary_count
                ORDER BY disease_name
                """,
                ns=namespace,
            )
            return [dict(record) for record in records]

    def disease_graph(self, namespace: str, disease_id: str) -> dict[str, Any]:
        with self.driver.session() as session:
            disease = session.run(
                """
                MATCH (d:DiseaseEntity {namespace:$ns, disease_id:$disease_id})
                RETURN d.disease_id AS disease_id, d.disease_name AS disease_name, d.namespace AS namespace
                """,
                ns=namespace,
                disease_id=disease_id,
            ).single()
            if disease is None:
                raise KeyError(f"Disease not found: {disease_id}")

            summary_row = session.run(
                """
                MATCH (s:ProtocolDiseaseSummary {namespace:$ns})-[:SUMMARIZES]->(d:DiseaseEntity {disease_id:$disease_id})
                RETURN s.summary_id AS summary_id,
                       s.summary_text AS summary_text,
                       coalesce(s.key_signs, []) AS key_signs,
                       coalesce(s.key_services, []) AS key_services,
                       coalesce(s.key_drugs, []) AS key_drugs,
                       coalesce(s.differential_diseases, []) AS differential_diseases
                LIMIT 1
                """,
                ns=namespace,
                disease_id=disease_id,
            ).single()

            chunk_rows = [
                dict(record)
                for record in session.run(
                    """
                    MATCH (c:RawChunk {namespace:$ns})-[:CHUNK_ABOUT_DISEASE]->(:DiseaseEntity {disease_id:$disease_id})
                    RETURN c.chunk_id AS chunk_id,
                           c.section_type AS section_type,
                           c.section_title AS section_title,
                           c.body_preview AS body_preview,
                           coalesce(c.page_numbers, []) AS page_numbers,
                           c.parent_section_path AS parent_section_path
                    ORDER BY c.section_title, c.chunk_id
                    """,
                    ns=namespace,
                    disease_id=disease_id,
                )
            ]

            sign_rows = [
                dict(record)
                for record in session.run(
                    """
                    MATCH (c:RawChunk {namespace:$ns})-[:CHUNK_ABOUT_DISEASE]->(:DiseaseEntity {disease_id:$disease_id})
                    MATCH (c)-[:MENTIONS_SIGN]->(m:RawSignMention {namespace:$ns})
                    OPTIONAL MATCH (m)-[r]->(sc)
                    WHERE r IS NULL OR (type(r) = 'MAPS_TO_SIGN' AND (sc:SignConcept OR sc:ClaimSignConcept OR sc:CISign))
                    RETURN m.mention_id AS mention_id,
                           m.mention_text AS mention_text,
                           m.context_text AS context_text,
                           m.modifier_raw AS modifier_raw,
                           m.extraction_confidence AS extraction_confidence,
                           m.mapping_status AS mapping_status,
                           m.source_chunk_id AS source_chunk_id,
                           m.source_page AS source_page,
                           labels(sc) AS mapped_labels,
                           coalesce(sc.sign_id, sc.claim_sign_id, sc.canonical_label, sc.text) AS concept_id,
                           coalesce(sc.canonical_label, sc.text, sc.sign_id, sc.claim_sign_id) AS concept_label,
                           r.confidence AS map_confidence,
                           r.method AS map_method,
                           r.status AS map_status
                    ORDER BY m.source_chunk_id, m.mention_id
                    """,
                    ns=namespace,
                    disease_id=disease_id,
                )
            ]

            service_rows = [
                dict(record)
                for record in session.run(
                    """
                    MATCH (c:RawChunk {namespace:$ns})-[:CHUNK_ABOUT_DISEASE]->(:DiseaseEntity {disease_id:$disease_id})
                    MATCH (c)-[:MENTIONS_SERVICE]->(m:RawServiceMention {namespace:$ns})
                    OPTIONAL MATCH (m)-[r]->(svc)
                    WHERE r IS NULL OR (type(r) = 'MAPS_TO_SERVICE' AND (svc:ProtocolService OR svc:CIService))
                    OPTIONAL MATCH (svc)-[:BELONGS_TO_FAMILY]->(f:ServiceFamily)
                    RETURN m.mention_id AS mention_id,
                           m.mention_text AS mention_text,
                           m.context_text AS context_text,
                           m.medical_role AS medical_role,
                           m.condition_to_apply AS condition_to_apply,
                           m.extraction_confidence AS extraction_confidence,
                           m.mapping_status AS mapping_status,
                           m.source_chunk_id AS source_chunk_id,
                           m.source_page AS source_page,
                           svc.service_code AS service_code,
                           coalesce(svc.service_name, svc.name) AS service_name,
                           f.family_id AS family_id,
                           f.family_name AS family_name,
                           r.confidence AS map_confidence,
                           r.method AS map_method,
                           r.status AS map_status
                    ORDER BY m.source_chunk_id, m.mention_id
                    """,
                    ns=namespace,
                    disease_id=disease_id,
                )
            ]

            observation_rows = [
                dict(record)
                for record in session.run(
                    """
                    MATCH (c:RawChunk {namespace:$ns})-[:CHUNK_ABOUT_DISEASE]->(:DiseaseEntity {disease_id:$disease_id})
                    MATCH (c)-[:MENTIONS_OBSERVATION]->(m:RawObservationMention {namespace:$ns})
                    RETURN m.mention_id AS mention_id,
                           m.mention_text AS mention_text,
                           m.context_text AS context_text,
                           m.result_semantics AS result_semantics,
                           m.extraction_confidence AS extraction_confidence,
                           m.mapping_status AS mapping_status,
                           m.source_chunk_id AS source_chunk_id,
                           m.source_page AS source_page
                    ORDER BY m.source_chunk_id, m.mention_id
                    """,
                    ns=namespace,
                    disease_id=disease_id,
                )
            ]

            assertion_rows = [
                dict(record)
                for record in session.run(
                    """
                    MATCH (a:ProtocolAssertion {namespace:$ns})-[:ASSERTION_ABOUT_DISEASE]->(:DiseaseEntity {disease_id:$disease_id})
                    OPTIONAL MATCH (sec:ProtocolSection)-[:CONTAINS_ASSERTION]->(a)
                    OPTIONAL MATCH (book:ProtocolBook)-[:BOOK_HAS_SECTION]->(sec)
                    OPTIONAL MATCH (chunk:RawChunk {chunk_id:a.source_chunk_id})
                    OPTIONAL MATCH (chunk)-[:FROM_DOCUMENT]->(doc:RawDocument)
                    OPTIONAL MATCH (a)-[rs]->(sc)
                    WHERE rs IS NULL OR (type(rs) = 'ASSERTION_REQUIRES_SIGN' AND (sc:SignConcept OR sc:ClaimSignConcept OR sc:CISign))
                    WITH a, sec, book, chunk, doc, collect(DISTINCT coalesce(sc.canonical_label, sc.text, sc.sign_id, sc.claim_sign_id)) AS related_signs
                    OPTIONAL MATCH (a)-[:ASSERTION_INDICATES_SERVICE]->(svc)
                    WITH a, sec, book, chunk, doc, related_signs, collect(DISTINCT coalesce(svc.service_name, svc.name, svc.service_code)) AS related_services
                    OPTIONAL MATCH (a)-[:ASSERTION_CONTRAINDICATES]->(csvc)
                    WITH a, sec, book, chunk, doc, related_signs, related_services,
                         collect(DISTINCT coalesce(csvc.service_name, csvc.name, csvc.service_code)) AS contraindicated_services
                    RETURN a.assertion_id AS assertion_id,
                           a.assertion_type AS assertion_type,
                           a.assertion_text AS assertion_text,
                           a.condition_text AS condition_text,
                           a.action_text AS action_text,
                           a.status AS status,
                           a.evidence_level AS evidence_level,
                           a.source_chunk_id AS source_chunk_id,
                           a.source_page AS source_page,
                           sec.section_id AS section_id,
                           chunk.section_title AS section_title,
                           book.book_name AS book_name,
                           doc.title AS doc_title,
                           doc.file_path AS doc_file_path,
                           related_signs,
                           related_services,
                           contraindicated_services
                    ORDER BY a.source_chunk_id, a.assertion_id
                    """,
                    ns=namespace,
                    disease_id=disease_id,
                )
            ]

        mapping_stats = self._mapping_stats(sign_rows, service_rows, observation_rows)
        graph = self._build_graph(
            dict(disease),
            dict(summary_row) if summary_row else None,
            chunk_rows,
            sign_rows,
            service_rows,
            observation_rows,
            assertion_rows,
        )

        return {
            "source": "neo4j",
            "namespace": namespace,
            "neo4j_uri": self.uri,
            "disease": dict(disease),
            "summary": dict(summary_row) if summary_row else None,
            "mapping_stats": mapping_stats,
            "chunks": chunk_rows,
            "sign_mentions": sign_rows,
            "service_mentions": service_rows,
            "observation_mentions": observation_rows,
            "assertions": assertion_rows,
            "graph": graph,
        }

    def _mapping_stats(
        self,
        sign_rows: list[dict[str, Any]],
        service_rows: list[dict[str, Any]],
        observation_rows: list[dict[str, Any]],
    ) -> dict[str, Any]:
        sign_status = Counter((row.get("map_status") or row.get("mapping_status") or "unknown") for row in sign_rows)
        service_status = Counter((row.get("map_status") or row.get("mapping_status") or "unknown") for row in service_rows)
        observation_status = Counter((row.get("mapping_status") or "pending") for row in observation_rows)
        return {
            "signs": {"total": len(sign_rows), "by_status": dict(sign_status)},
            "services": {"total": len(service_rows), "by_status": dict(service_status)},
            "observations": {"total": len(observation_rows), "by_status": dict(observation_status)},
        }

    def _build_graph(
        self,
        disease: dict[str, Any],
        summary: dict[str, Any] | None,
        chunks: list[dict[str, Any]],
        sign_rows: list[dict[str, Any]],
        service_rows: list[dict[str, Any]],
        observation_rows: list[dict[str, Any]],
        assertion_rows: list[dict[str, Any]],
    ) -> dict[str, Any]:
        nodes: dict[str, dict[str, Any]] = {}
        edges: list[dict[str, Any]] = []

        def upsert_node(node_id: str, node_type: str, label: str, **props):
            nodes[node_id] = {
                "id": node_id,
                "type": node_type,
                "label": label,
                **props,
            }

        def add_edge(source: str, target: str, edge_type: str, label: str = "", **props):
            edges.append(
                {
                    "source": source,
                    "target": target,
                    "type": edge_type,
                    "label": label,
                    **props,
                }
            )

        disease_node_id = f"disease:{disease['disease_id']}"
        upsert_node(
            disease_node_id,
            "disease",
            disease.get("disease_name") or disease.get("disease_id") or "Unknown disease",
            subtitle=disease.get("disease_id"),
        )

        if summary:
            summary_node_id = f"summary:{summary['summary_id']}"
            upsert_node(
                summary_node_id,
                "summary",
                "Disease Summary",
                subtitle=summary["summary_id"],
                text_preview=(summary.get("summary_text") or "")[:180],
            )
            add_edge(summary_node_id, disease_node_id, "summarizes", "SUMMARIZES")

        for chunk in chunks:
            chunk_node_id = f"chunk:{chunk['chunk_id']}"
            upsert_node(
                chunk_node_id,
                "chunk",
                chunk.get("section_title") or chunk.get("chunk_id"),
                subtitle=chunk.get("section_type"),
                page_numbers=chunk.get("page_numbers", []),
            )
            add_edge(chunk_node_id, disease_node_id, "chunk_about_disease", "CHUNK_ABOUT_DISEASE")

        for row in sign_rows:
            mention_id = f"signmention:{row['mention_id']}"
            upsert_node(
                mention_id,
                "sign_mention",
                row.get("mention_text") or row["mention_id"],
                subtitle=row.get("map_status") or row.get("mapping_status"),
                source_chunk_id=row.get("source_chunk_id"),
            )
            add_edge(f"chunk:{row['source_chunk_id']}", mention_id, "mentions_sign", "MENTIONS_SIGN")
            concept_id = row.get("concept_id")
            if concept_id:
                concept_node_id = f"signconcept:{concept_id}"
                upsert_node(
                    concept_node_id,
                    "sign_concept",
                    row.get("concept_label") or concept_id,
                    subtitle=" / ".join(row.get("mapped_labels") or []),
                )
                add_edge(
                    mention_id,
                    concept_node_id,
                    "maps_to_sign",
                    row.get("map_status") or row.get("mapping_status") or "MAPS_TO_SIGN",
                    confidence=row.get("map_confidence"),
                    method=row.get("map_method"),
                )

        for row in service_rows:
            mention_id = f"servicemention:{row['mention_id']}"
            upsert_node(
                mention_id,
                "service_mention",
                row.get("mention_text") or row["mention_id"],
                subtitle=row.get("medical_role") or row.get("mapping_status"),
                source_chunk_id=row.get("source_chunk_id"),
            )
            add_edge(f"chunk:{row['source_chunk_id']}", mention_id, "mentions_service", "MENTIONS_SERVICE")
            service_code = row.get("service_code")
            if service_code:
                service_node_id = f"service:{service_code}"
                upsert_node(
                    service_node_id,
                    "service",
                    row.get("service_name") or service_code,
                    subtitle=service_code,
                )
                add_edge(
                    mention_id,
                    service_node_id,
                    "maps_to_service",
                    row.get("map_status") or row.get("mapping_status") or "MAPS_TO_SERVICE",
                    confidence=row.get("map_confidence"),
                    method=row.get("map_method"),
                )
                family_id = row.get("family_id")
                if family_id:
                    family_node_id = f"family:{family_id}"
                    upsert_node(
                        family_node_id,
                        "service_family",
                        row.get("family_name") or family_id,
                        subtitle=family_id,
                    )
                    add_edge(service_node_id, family_node_id, "belongs_to_family", "BELONGS_TO_FAMILY")

        for row in observation_rows:
            mention_id = f"observationmention:{row['mention_id']}"
            upsert_node(
                mention_id,
                "observation_mention",
                row.get("mention_text") or row["mention_id"],
                subtitle=row.get("result_semantics") or row.get("mapping_status"),
                source_chunk_id=row.get("source_chunk_id"),
            )
            add_edge(
                f"chunk:{row['source_chunk_id']}",
                mention_id,
                "mentions_observation",
                "MENTIONS_OBSERVATION",
            )

        for row in assertion_rows:
            assertion_id = f"assertion:{row['assertion_id']}"
            upsert_node(
                assertion_id,
                "assertion",
                row.get("assertion_type") or row["assertion_id"],
                subtitle=row.get("status") or row.get("evidence_level"),
                text_preview=(row.get("assertion_text") or "")[:160],
            )
            add_edge(f"chunk:{row['source_chunk_id']}", assertion_id, "contains_assertion", "CONTAINS_ASSERTION")
            add_edge(assertion_id, disease_node_id, "assertion_about_disease", "ASSERTION_ABOUT_DISEASE")

        type_counts = Counter(node["type"] for node in nodes.values())
        return {
            "nodes": list(nodes.values()),
            "edges": edges,
            "stats": {
                "node_count": len(nodes),
                "edge_count": len(edges),
                "node_types": dict(type_counts),
            },
        }
