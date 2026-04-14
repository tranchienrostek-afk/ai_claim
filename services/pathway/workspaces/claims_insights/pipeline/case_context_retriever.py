from __future__ import annotations

import os
import re
import unicodedata
from typing import Any

from neo4j import GraphDatabase
from openai import AzureOpenAI


def as_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    return str(value).strip()


def normalize_text(value: Any) -> str:
    text = as_text(value).lower().replace("đ", "d").replace("Đ", "d")
    normalized = unicodedata.normalize("NFD", text)
    stripped = "".join(ch for ch in normalized if unicodedata.category(ch) != "Mn")
    stripped = re.sub(r"[^a-z0-9 ]+", " ", stripped)
    return re.sub(r"\s+", " ", stripped).strip()


def tokenize(value: Any) -> list[str]:
    generic = {
        "",
        "benh",
        "hoi",
        "chung",
        "cap",
        "man",
        "tinh",
        "do",
        "khong",
        "co",
        "va",
    }
    return [token for token in normalize_text(value).split() if len(token) > 2 and token not in generic]


def unique_texts(values: list[Any]) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for value in values:
        text = as_text(value)
        if not text:
            continue
        key = normalize_text(text)
        if not key or key in seen:
            continue
        seen.add(key)
        ordered.append(text)
    return ordered


def reciprocal_rank_fusion(result_lists: list[list[dict[str, Any]]], k: int = 60) -> list[dict[str, Any]]:
    merged: dict[str, dict[str, Any]] = {}
    for rows in result_lists:
        for rank, item in enumerate(rows, start=1):
            key = as_text(item.get("_merge_key"))
            if not key:
                continue
            if key not in merged:
                merged[key] = dict(item)
                merged[key]["rrf_score"] = 0.0
            merged[key]["rrf_score"] += 1.0 / (k + rank)
            merged[key]["max_source_score"] = max(
                float(merged[key].get("max_source_score") or 0.0),
                float(item.get("score") or 0.0),
            )
    ranked = sorted(
        merged.values(),
        key=lambda row: (float(row.get("rrf_score") or 0.0), float(row.get("max_source_score") or 0.0)),
        reverse=True,
    )
    return ranked


class CaseContextRetriever:
    def __init__(self, namespace: str = "ontology_v2") -> None:
        self.namespace = namespace
        uri = os.getenv("NEO4J_URI", os.getenv("neo4j_uri", "bolt://localhost:7688"))
        user = os.getenv("NEO4J_USER", os.getenv("neo4j_user", "neo4j"))
        password = os.getenv("NEO4J_PASSWORD", os.getenv("neo4j_password", "password123"))
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

        endpoint = os.getenv("AZURE_EMBEDDINGS_ENDPOINT", "").strip()
        api_key = os.getenv("AZURE_EMBEDDINGS_API_KEY", "").strip()
        api_version = os.getenv("AZURE_OPENAI_API_VERSION", "").strip()
        self.embedding_model = "text-embedding-ada-002"
        self.embedding_client = None
        if endpoint and api_key and api_version:
            self.embedding_client = AzureOpenAI(
                azure_endpoint=endpoint,
                api_key=api_key,
                api_version=api_version,
            )

    def close(self) -> None:
        self.driver.close()

    def _get_embedding(self, text: str) -> list[float] | None:
        if self.embedding_client is None:
            return None
        try:
            return self.embedding_client.embeddings.create(
                input=[text.replace("\n", " ")[:8000]],
                model=self.embedding_model,
            ).data[0].embedding
        except Exception:
            return None

    def _escape_lucene(self, text: str) -> str:
        escaped = as_text(text)
        for ch in r'+-&|!(){}[]^"~*?:\/':
            escaped = escaped.replace(ch, f"\\{ch}")
        return escaped.strip()[:220]

    def _build_fulltext_query(self, query_text: str, extra_terms: list[str] | None = None) -> str:
        extra_terms = extra_terms or []
        parts: list[str] = []
        phrase = self._escape_lucene(query_text)
        if phrase:
            parts.append(f'"{phrase}"')
        for item in unique_texts([query_text, *extra_terms]):
            for token in tokenize(item):
                escaped = self._escape_lucene(token)
                if escaped:
                    parts.append(escaped)
        return " OR ".join(unique_texts(parts[:16])) or self._escape_lucene(query_text)

    def _resolve_disease_hint_ids(self, namespace: str, disease_hint: str) -> list[str]:
        hint = as_text(disease_hint)
        if not hint:
            return []
        with self.driver.session() as session:
            rows = session.run(
                """
                MATCH (d:DiseaseEntity {namespace:$ns})
                WHERE toLower(d.disease_name) = toLower($hint)
                   OR d.disease_id = $hint
                   OR toLower(d.disease_name) CONTAINS toLower($hint)
                RETURN d.disease_id AS disease_id
                ORDER BY CASE WHEN toLower(d.disease_name) = toLower($hint) THEN 0 ELSE 1 END, d.disease_name
                LIMIT 5
                """,
                ns=namespace,
                hint=hint,
            )
            return [as_text(row["disease_id"]) for row in rows if as_text(row["disease_id"])]

    def _summary_vector_search(self, namespace: str, query_vector: list[float] | None, top_k: int) -> list[dict[str, Any]]:
        if not query_vector:
            return []
        cypher = """
        CALL db.index.vector.queryNodes('protocoldiseasesummary_vector_idx', 40, $qv)
        YIELD node, score
        WHERE node.namespace = $ns
        MATCH (node)-[:SUMMARIZES]->(d:DiseaseEntity {namespace:$ns})
        RETURN
          node.summary_id AS summary_id,
          d.disease_id AS disease_id,
          d.disease_name AS disease_name,
          node.summary_text AS summary_text,
          coalesce(node.key_signs, []) AS key_signs,
          coalesce(node.key_services, []) AS key_services,
          coalesce(node.key_drugs, []) AS key_drugs,
          score
        ORDER BY score DESC
        LIMIT $top_k
        """
        with self.driver.session() as session:
            try:
                rows = [dict(row) for row in session.run(cypher, qv=query_vector, ns=namespace, top_k=top_k)]
            except Exception:
                return []
        for row in rows:
            row["source"] = "summary_vector"
            row["_merge_key"] = f"summary::{as_text(row.get('summary_id'))}"
        return rows

    def _assertion_vector_search(
        self,
        namespace: str,
        query_vector: list[float] | None,
        top_k: int,
        candidate_disease_ids: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        if not query_vector:
            return []
        cypher = """
        CALL db.index.vector.queryNodes('protocolassertion_vector_idx', 64, $qv)
        YIELD node, score
        WHERE node.namespace = $ns
        MATCH (node)-[:ASSERTION_ABOUT_DISEASE]->(d:DiseaseEntity {namespace:$ns})
        WHERE $candidate_ids = [] OR d.disease_id IN $candidate_ids
        RETURN
          node.assertion_id AS assertion_id,
          d.disease_id AS disease_id,
          d.disease_name AS disease_name,
          node.assertion_type AS assertion_type,
          node.assertion_text AS assertion_text,
          node.condition_text AS condition_text,
          node.action_text AS action_text,
          score
        ORDER BY score DESC
        LIMIT $top_k
        """
        with self.driver.session() as session:
            try:
                rows = [
                    dict(row)
                    for row in session.run(
                        cypher,
                        qv=query_vector,
                        ns=namespace,
                        top_k=top_k,
                        candidate_ids=candidate_disease_ids or [],
                    )
                ]
            except Exception:
                return []
        for row in rows:
            row["source"] = "assertion_vector"
            row["_merge_key"] = f"assertion::{as_text(row.get('assertion_id'))}"
        return rows

    def _assertion_fulltext_search(
        self,
        namespace: str,
        search_query: str,
        top_k: int,
        candidate_disease_ids: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        if not as_text(search_query):
            return []
        cypher = """
        CALL db.index.fulltext.queryNodes('assertion_fulltext', $search_query)
        YIELD node, score
        WHERE node.namespace = $ns
        MATCH (node)-[:ASSERTION_ABOUT_DISEASE]->(d:DiseaseEntity {namespace:$ns})
        WHERE $candidate_ids = [] OR d.disease_id IN $candidate_ids
        RETURN
          node.assertion_id AS assertion_id,
          d.disease_id AS disease_id,
          d.disease_name AS disease_name,
          node.assertion_type AS assertion_type,
          node.assertion_text AS assertion_text,
          node.condition_text AS condition_text,
          node.action_text AS action_text,
          score
        ORDER BY score DESC
        LIMIT $top_k
        """
        with self.driver.session() as session:
            try:
                rows = [
                    dict(row)
                    for row in session.run(
                        cypher,
                        search_query=search_query,
                        ns=namespace,
                        top_k=top_k,
                        candidate_ids=candidate_disease_ids or [],
                    )
                ]
            except Exception:
                return []
        for row in rows:
            row["source"] = "assertion_fulltext"
            row["_merge_key"] = f"assertion::{as_text(row.get('assertion_id'))}"
        return rows

    def _chunk_vector_search(
        self,
        namespace: str,
        query_vector: list[float] | None,
        top_k: int,
        candidate_disease_ids: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        if not query_vector:
            return []
        cypher = """
        CALL db.index.vector.queryNodes('rawchunk_vector_idx', 64, $qv)
        YIELD node, score
        WHERE node.namespace = $ns
        MATCH (node)-[:CHUNK_ABOUT_DISEASE]->(d:DiseaseEntity {namespace:$ns})
        WHERE $candidate_ids = [] OR d.disease_id IN $candidate_ids
        RETURN
          node.chunk_id AS chunk_id,
          d.disease_id AS disease_id,
          d.disease_name AS disease_name,
          node.section_type AS section_type,
          node.section_title AS section_title,
          node.body_preview AS body_preview,
          score
        ORDER BY score DESC
        LIMIT $top_k
        """
        with self.driver.session() as session:
            try:
                rows = [
                    dict(row)
                    for row in session.run(
                        cypher,
                        qv=query_vector,
                        ns=namespace,
                        top_k=top_k,
                        candidate_ids=candidate_disease_ids or [],
                    )
                ]
            except Exception:
                return []
        for row in rows:
            row["source"] = "chunk_vector"
            row["_merge_key"] = f"chunk::{as_text(row.get('chunk_id'))}"
        return rows

    def _chunk_fulltext_search(
        self,
        namespace: str,
        search_query: str,
        top_k: int,
        candidate_disease_ids: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        if not as_text(search_query):
            return []
        cypher = """
        CALL db.index.fulltext.queryNodes('raw_chunk_fulltext', $search_query)
        YIELD node, score
        WHERE node.namespace = $ns
        MATCH (node)-[:CHUNK_ABOUT_DISEASE]->(d:DiseaseEntity {namespace:$ns})
        WHERE $candidate_ids = [] OR d.disease_id IN $candidate_ids
        RETURN
          node.chunk_id AS chunk_id,
          d.disease_id AS disease_id,
          d.disease_name AS disease_name,
          node.section_type AS section_type,
          node.section_title AS section_title,
          node.body_preview AS body_preview,
          score
        ORDER BY score DESC
        LIMIT $top_k
        """
        with self.driver.session() as session:
            try:
                rows = [
                    dict(row)
                    for row in session.run(
                        cypher,
                        search_query=search_query,
                        ns=namespace,
                        top_k=top_k,
                        candidate_ids=candidate_disease_ids or [],
                    )
                ]
            except Exception:
                return []
        for row in rows:
            row["source"] = "chunk_fulltext"
            row["_merge_key"] = f"chunk::{as_text(row.get('chunk_id'))}"
        return rows

    def _aggregate_candidate_diseases(
        self,
        summary_hits: list[dict[str, Any]],
        assertion_hits: list[dict[str, Any]],
        chunk_hits: list[dict[str, Any]],
        hinted_ids: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        disease_rows: dict[str, dict[str, Any]] = {}

        def absorb(rows: list[dict[str, Any]], weight: float, source_key: str) -> None:
            max_score = max((float(item.get("score") or item.get("rrf_score") or 0.0) for item in rows), default=0.0) or 1.0
            for rank, item in enumerate(rows, start=1):
                disease_id = as_text(item.get("disease_id"))
                disease_name = as_text(item.get("disease_name"))
                if not disease_id:
                    continue
                bucket = disease_rows.setdefault(
                    disease_id,
                    {
                        "disease_id": disease_id,
                        "disease_name": disease_name,
                        "retrieval_score": 0.0,
                        "summary_hit_count": 0,
                        "assertion_hit_count": 0,
                        "chunk_hit_count": 0,
                        "hit_sources": [],
                    },
                )
                raw_score = float(item.get("score") or item.get("rrf_score") or 0.0)
                normalized = raw_score / max_score
                rank_component = 1.0 / rank
                bucket["retrieval_score"] += weight * (0.7 * normalized + 0.3 * rank_component)
                bucket[f"{source_key}_hit_count"] += 1
                if item.get("source") and item["source"] not in bucket["hit_sources"]:
                    bucket["hit_sources"].append(item["source"])

        absorb(summary_hits, 1.0, "summary")
        absorb(assertion_hits, 0.78, "assertion")
        absorb(chunk_hits, 0.46, "chunk")

        hinted_ids = hinted_ids or []
        for disease_id in hinted_ids:
            if disease_id in disease_rows:
                disease_rows[disease_id]["retrieval_score"] += 0.18

        ranked = sorted(
            disease_rows.values(),
            key=lambda row: (float(row["retrieval_score"]), row["summary_hit_count"], row["assertion_hit_count"]),
            reverse=True,
        )
        for row in ranked:
            row["retrieval_score"] = round(float(row["retrieval_score"]), 4)
        return ranked

    def retrieve(
        self,
        *,
        query_text: str,
        namespace: str | None = None,
        disease_hint: str = "",
        top_k_summary: int = 5,
        top_k_assertion: int = 8,
        top_k_chunk: int = 8,
    ) -> dict[str, Any]:
        namespace = namespace or self.namespace
        hinted_ids = self._resolve_disease_hint_ids(namespace, disease_hint)
        query_embedding = self._get_embedding(query_text)
        search_query = self._build_fulltext_query(query_text, extra_terms=[disease_hint])

        summary_hits = reciprocal_rank_fusion(
            [self._summary_vector_search(namespace, query_embedding, top_k_summary)],
        )[:top_k_summary]

        candidate_ids = unique_texts(hinted_ids + [row.get("disease_id") for row in summary_hits if as_text(row.get("disease_id"))])

        assertion_hits = reciprocal_rank_fusion(
            [
                self._assertion_vector_search(namespace, query_embedding, top_k_assertion, candidate_ids),
                self._assertion_fulltext_search(namespace, search_query, top_k_assertion, candidate_ids),
            ],
        )[:top_k_assertion]

        chunk_hits = reciprocal_rank_fusion(
            [
                self._chunk_vector_search(namespace, query_embedding, top_k_chunk, candidate_ids),
                self._chunk_fulltext_search(namespace, search_query, top_k_chunk, candidate_ids),
            ],
        )[:top_k_chunk]

        candidate_diseases = self._aggregate_candidate_diseases(summary_hits, assertion_hits, chunk_hits, hinted_ids)

        return {
            "namespace": namespace,
            "query_text": query_text,
            "disease_hint": disease_hint,
            "summary_hits": summary_hits,
            "assertion_hits": assertion_hits,
            "chunk_hits": chunk_hits,
            "candidate_diseases": candidate_diseases,
        }
