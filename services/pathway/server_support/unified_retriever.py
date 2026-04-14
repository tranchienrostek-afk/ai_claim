"""
Unified Knowledge Retriever — Queries ALL Neo4j data layers.

Bridges the gap between what's ingested and what's searchable:
- RawChunk (vector + fulltext) ← existing, kept
- ProtocolAssertion (vector + fulltext) ← NEW: enables rule/contraindication search
- ProtocolDiseaseSummary (vector) ← NEW: enables disease overview search
- RawSignMention (fulltext) ← NEW: enables symptom→disease reverse lookup
- RawServiceMention (fulltext) ← NEW: enables service/drug entity search
- RawObservationMention (fulltext) ← NEW: enables lab test search
- Experience memory ← NEW: integrates pipeline learnings into Q&A
- Claims Insights (CI*) ← NEW: integrates real-world claims frequency data

Architecture:
    UnifiedRetriever wraps MedicalAgent's Neo4j driver.
    Each search_* method queries one data layer.
    retrieve() fans out to all relevant layers based on query intent,
    then merges via Reciprocal Rank Fusion.
"""

from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass, field
from typing import Any

from neo4j import GraphDatabase


# ---------------------------------------------------------------------------
# Result container
# ---------------------------------------------------------------------------

@dataclass
class RetrievalResult:
    """Unified result from any data layer."""
    id: str
    title: str
    content: str
    source_layer: str  # chunk | assertion | summary | sign | service | observation | experience | claims
    score: float = 0.0
    disease_name: str = ""
    metadata: dict = field(default_factory=dict)

    def to_context_dict(self) -> dict:
        """Convert to format compatible with medical_agent.py context pipeline."""
        return {
            "title": self.title,
            "description": self.content,
            "block_id": self.id,
            "score": self.score,
            "source": self.source_layer,
            "disease_name": self.disease_name,
            "page_number": self.metadata.get("page_numbers"),
            "related_context": [],
            "prev_block_content": None,
            "next_block_content": None,
            "metadata": self.metadata,
        }


# ---------------------------------------------------------------------------
# RRF merge (same algorithm as medical_agent.py)
# ---------------------------------------------------------------------------

def _rrf_merge(result_lists: list[list[RetrievalResult]], k: int = 60) -> list[RetrievalResult]:
    """Reciprocal Rank Fusion across multiple result lists."""
    fused: dict[str, float] = {}
    items: dict[str, RetrievalResult] = {}

    for rlist in result_lists:
        for rank, item in enumerate(rlist):
            score = 1.0 / (k + rank)
            fused[item.id] = fused.get(item.id, 0.0) + score
            if item.id not in items or item.score > items[item.id].score:
                items[item.id] = item

    ranked = sorted(fused.items(), key=lambda x: x[1], reverse=True)
    results = []
    for rid, fscore in ranked:
        if rid in items:
            r = items[rid]
            r.score = fscore
            results.append(r)
    return results


# ---------------------------------------------------------------------------
# Index bootstrapper — creates missing indexes for entity mentions
# ---------------------------------------------------------------------------

INDEX_DEFINITIONS = [
    # Fulltext indexes for entity mentions (these are the NEW ones)
    ("sign_mention_fulltext", "RawSignMention", ["mention_text", "context_text"], "fulltext"),
    ("service_mention_fulltext", "RawServiceMention", ["mention_text", "context_text"], "fulltext"),
    ("observation_mention_fulltext", "RawObservationMention", ["mention_text", "context_text"], "fulltext"),
    # These should already exist but ensure them
    ("assertion_fulltext", "ProtocolAssertion", ["assertion_text", "condition_text", "action_text"], "fulltext"),
]


def ensure_indexes(driver) -> list[str]:
    """Create any missing fulltext indexes. Returns list of created index names."""
    created = []
    with driver.session() as session:
        for idx_name, label, props, idx_type in INDEX_DEFINITIONS:
            if idx_type == "fulltext":
                prop_list = ", ".join(f"n.{p}" for p in props)
                try:
                    session.run(f"""
                        CREATE FULLTEXT INDEX `{idx_name}` IF NOT EXISTS
                        FOR (n:{label}) ON EACH [{prop_list}]
                    """)
                    created.append(idx_name)
                except Exception:
                    pass  # Already exists
    return created


# ---------------------------------------------------------------------------
# UnifiedRetriever
# ---------------------------------------------------------------------------

class UnifiedRetriever:
    """Queries all Neo4j data layers and merges results."""

    def __init__(self, driver, embedding_fn=None):
        """
        Args:
            driver: Neo4j driver instance (reuse from MedicalAgent)
            embedding_fn: callable(text) -> list[float], for vector searches
        """
        self.driver = driver
        self.embedding_fn = embedding_fn
        self._indexes_ensured = False

    def _ensure_indexes(self):
        if not self._indexes_ensured:
            ensure_indexes(self.driver)
            self._indexes_ensured = True

    def _escape_lucene(self, text: str) -> str:
        special = r'+-&|!(){}[]^"~*?:\/'
        escaped = text
        for ch in special:
            escaped = escaped.replace(ch, f'\\{ch}')
        return escaped[:200]

    # ------------------------------------------------------------------
    # Layer 1: ProtocolAssertion search (clinical rules, contraindications)
    # ------------------------------------------------------------------

    def search_assertions(self, query: str, disease_name: str = None,
                          assertion_type: str = None, top_k: int = 10) -> list[RetrievalResult]:
        """Search ProtocolAssertion nodes via vector + fulltext.

        assertion_type: treatment_rule | diagnostic_rule | contraindication |
                        indication | dosage_rule | monitoring_rule
        """
        results = []

        # --- Vector search on assertion embeddings ---
        if self.embedding_fn:
            qv = self.embedding_fn(query)
            type_filter = "AND a.assertion_type = $atype" if assertion_type else ""
            disease_filter = """
                AND EXISTS {
                    MATCH (a)-[:ASSERTION_ABOUT_DISEASE]->(d:DiseaseEntity)
                    WHERE toLower(d.disease_name) CONTAINS toLower($disease)
                       OR toLower(d.disease_id) CONTAINS toLower($disease)
                }
            """ if disease_name else ""

            cypher = f"""
            CALL db.index.vector.queryNodes('protocolassertion_vector_idx', $top_k, $qv)
            YIELD node AS a, score
            WHERE true {type_filter} {disease_filter}
            OPTIONAL MATCH (a)-[:ASSERTION_ABOUT_DISEASE]->(d:DiseaseEntity)
            OPTIONAL MATCH (src:RawChunk {{chunk_id: a.source_chunk_id}})
            RETURN a.assertion_id AS id,
                   a.assertion_type AS atype,
                   a.assertion_text AS text,
                   a.condition_text AS condition,
                   a.action_text AS action,
                   a.evidence_level AS evidence_level,
                   a.status AS status,
                   a.related_signs AS related_signs,
                   a.related_services AS related_services,
                   d.disease_name AS disease,
                   src.section_title AS section,
                   score
            ORDER BY score DESC
            LIMIT $top_k
            """
            params = {"qv": qv, "top_k": top_k * 2}
            if assertion_type:
                params["atype"] = assertion_type
            if disease_name:
                params["disease"] = disease_name

            with self.driver.session() as session:
                try:
                    for rec in session.run(cypher, **params):
                        content = rec["text"] or ""
                        if rec["condition"]:
                            content = f"[Điều kiện] {rec['condition']}\n[Hành động] {rec['action']}\n{content}"
                        results.append(RetrievalResult(
                            id=f"assertion:{rec['id']}",
                            title=f"[{rec['atype'] or 'rule'}] {(rec['section'] or '')[:80]}",
                            content=content,
                            source_layer="assertion",
                            score=rec["score"],
                            disease_name=rec["disease"] or "",
                            metadata={
                                "assertion_type": rec["atype"],
                                "evidence_level": rec["evidence_level"],
                                "status": rec["status"],
                                "related_signs": rec["related_signs"] or [],
                                "related_services": rec["related_services"] or [],
                            }
                        ))
                except Exception as e:
                    print(f"[unified:assertion_vector] {e}")

        # --- Fulltext search on assertion text ---
        escaped = self._escape_lucene(query)
        ft_query = f"assertion_text:{escaped}^3 OR condition_text:{escaped}^2 OR action_text:{escaped}"
        type_filter_ft = "AND a.assertion_type = $atype" if assertion_type else ""
        disease_filter_ft = ""
        if disease_name:
            disease_filter_ft = """
                AND EXISTS {
                    MATCH (a)-[:ASSERTION_ABOUT_DISEASE]->(d2:DiseaseEntity)
                    WHERE toLower(d2.disease_name) CONTAINS toLower($disease)
                }
            """

        cypher_ft = f"""
        CALL db.index.fulltext.queryNodes('assertion_fulltext', $ft_query)
        YIELD node AS a, score
        WHERE true {type_filter_ft} {disease_filter_ft}
        OPTIONAL MATCH (a)-[:ASSERTION_ABOUT_DISEASE]->(d:DiseaseEntity)
        RETURN a.assertion_id AS id,
               a.assertion_type AS atype,
               a.assertion_text AS text,
               a.condition_text AS condition,
               a.action_text AS action,
               d.disease_name AS disease,
               score
        ORDER BY score DESC
        LIMIT $top_k
        """
        params_ft = {"ft_query": ft_query, "top_k": top_k}
        if assertion_type:
            params_ft["atype"] = assertion_type
        if disease_name:
            params_ft["disease"] = disease_name

        with self.driver.session() as session:
            try:
                for rec in session.run(cypher_ft, **params_ft):
                    rid = f"assertion:{rec['id']}"
                    if any(r.id == rid for r in results):
                        continue
                    content = rec["text"] or ""
                    if rec["condition"]:
                        content = f"[Điều kiện] {rec['condition']}\n[Hành động] {rec['action']}\n{content}"
                    results.append(RetrievalResult(
                        id=rid,
                        title=f"[{rec['atype'] or 'rule'}] assertion",
                        content=content,
                        source_layer="assertion",
                        score=rec["score"] * 0.8,  # Discount fulltext vs vector
                        disease_name=rec["disease"] or "",
                        metadata={"assertion_type": rec["atype"]}
                    ))
            except Exception as e:
                print(f"[unified:assertion_fulltext] {e}")

        return results[:top_k]

    # ------------------------------------------------------------------
    # Layer 2: ProtocolDiseaseSummary search
    # ------------------------------------------------------------------

    def search_summaries(self, query: str, disease_name: str = None,
                         top_k: int = 5) -> list[RetrievalResult]:
        """Search ProtocolDiseaseSummary nodes via vector similarity."""
        if not self.embedding_fn:
            return []

        qv = self.embedding_fn(query)
        disease_filter = ""
        if disease_name:
            disease_filter = """
                AND EXISTS {
                    MATCH (s)-[:SUMMARIZES]->(d:DiseaseEntity)
                    WHERE toLower(d.disease_name) CONTAINS toLower($disease)
                }
            """

        cypher = f"""
        CALL db.index.vector.queryNodes('protocoldiseasesummary_vector_idx', $top_k, $qv)
        YIELD node AS s, score
        WHERE true {disease_filter}
        OPTIONAL MATCH (s)-[:SUMMARIZES]->(d:DiseaseEntity)
        RETURN s.summary_id AS id,
               s.summary_text AS text,
               s.key_signs AS signs,
               s.key_services AS services,
               s.key_drugs AS drugs,
               d.disease_name AS disease,
               score
        ORDER BY score DESC
        LIMIT $top_k
        """
        params = {"qv": qv, "top_k": top_k}
        if disease_name:
            params["disease"] = disease_name

        results = []
        with self.driver.session() as session:
            try:
                for rec in session.run(cypher, **params):
                    results.append(RetrievalResult(
                        id=f"summary:{rec['id']}",
                        title=f"[Tóm tắt] {rec['disease'] or 'unknown'}",
                        content=rec["text"] or "",
                        source_layer="summary",
                        score=rec["score"],
                        disease_name=rec["disease"] or "",
                        metadata={
                            "key_signs": rec["signs"] or [],
                            "key_services": rec["services"] or [],
                            "key_drugs": rec["drugs"] or [],
                        }
                    ))
            except Exception as e:
                print(f"[unified:summary_vector] {e}")

        return results

    # ------------------------------------------------------------------
    # Layer 3: Entity mention search (signs, services, observations)
    # ------------------------------------------------------------------

    def search_sign_mentions(self, query: str, top_k: int = 10) -> list[RetrievalResult]:
        """Search RawSignMention nodes and traverse back to chunks + diseases."""
        self._ensure_indexes()
        escaped = self._escape_lucene(query)
        ft_query = f"mention_text:{escaped}^3 OR context_text:{escaped}"

        cypher = """
        CALL db.index.fulltext.queryNodes('sign_mention_fulltext', $ft_query)
        YIELD node AS sign, score
        OPTIONAL MATCH (chunk:RawChunk)-[:MENTIONS_SIGN]->(sign)
        OPTIONAL MATCH (chunk)-[:CHUNK_ABOUT_DISEASE]->(d:DiseaseEntity)
        RETURN sign.mention_id AS id,
               sign.mention_text AS text,
               sign.context_text AS context,
               sign.modifier_raw AS modifier,
               sign.extraction_confidence AS confidence,
               chunk.chunk_id AS chunk_id,
               chunk.section_title AS section,
               chunk.body_preview AS chunk_preview,
               d.disease_name AS disease,
               score
        ORDER BY score DESC
        LIMIT $top_k
        """

        results = []
        with self.driver.session() as session:
            try:
                for rec in session.run(cypher, ft_query=ft_query, top_k=top_k):
                    title = f"[Triệu chứng] {rec['text']}"
                    if rec["modifier"]:
                        title += f" ({rec['modifier']})"
                    content = rec["context"] or rec["chunk_preview"] or rec["text"]
                    results.append(RetrievalResult(
                        id=f"sign:{rec['id']}",
                        title=title,
                        content=content,
                        source_layer="sign",
                        score=rec["score"],
                        disease_name=rec["disease"] or "",
                        metadata={
                            "extraction_confidence": rec["confidence"],
                            "source_chunk": rec["chunk_id"],
                            "section": rec["section"],
                        }
                    ))
            except Exception as e:
                print(f"[unified:sign_fulltext] {e}")

        return results

    def search_service_mentions(self, query: str, top_k: int = 10) -> list[RetrievalResult]:
        """Search RawServiceMention nodes (drugs, procedures, tests)."""
        self._ensure_indexes()
        escaped = self._escape_lucene(query)
        ft_query = f"mention_text:{escaped}^3 OR context_text:{escaped}"

        cypher = """
        CALL db.index.fulltext.queryNodes('service_mention_fulltext', $ft_query)
        YIELD node AS svc, score
        OPTIONAL MATCH (chunk:RawChunk)-[:MENTIONS_SERVICE]->(svc)
        OPTIONAL MATCH (chunk)-[:CHUNK_ABOUT_DISEASE]->(d:DiseaseEntity)
        RETURN svc.mention_id AS id,
               svc.mention_text AS text,
               svc.context_text AS context,
               svc.medical_role AS role,
               svc.condition_to_apply AS condition,
               svc.extraction_confidence AS confidence,
               chunk.chunk_id AS chunk_id,
               chunk.section_title AS section,
               chunk.body_preview AS chunk_preview,
               d.disease_name AS disease,
               score
        ORDER BY score DESC
        LIMIT $top_k
        """

        results = []
        with self.driver.session() as session:
            try:
                for rec in session.run(cypher, ft_query=ft_query, top_k=top_k):
                    role_label = rec["role"] or "unknown"
                    title = f"[Dịch vụ/{role_label}] {rec['text']}"
                    content = rec["context"] or rec["chunk_preview"] or rec["text"]
                    if rec["condition"]:
                        content = f"Điều kiện áp dụng: {rec['condition']}\n{content}"
                    results.append(RetrievalResult(
                        id=f"service:{rec['id']}",
                        title=title,
                        content=content,
                        source_layer="service",
                        score=rec["score"],
                        disease_name=rec["disease"] or "",
                        metadata={
                            "medical_role": role_label,
                            "condition_to_apply": rec["condition"],
                            "extraction_confidence": rec["confidence"],
                            "source_chunk": rec["chunk_id"],
                        }
                    ))
            except Exception as e:
                print(f"[unified:service_fulltext] {e}")

        return results

    def search_observation_mentions(self, query: str, top_k: int = 10) -> list[RetrievalResult]:
        """Search RawObservationMention nodes (lab results, imaging findings)."""
        self._ensure_indexes()
        escaped = self._escape_lucene(query)
        ft_query = f"mention_text:{escaped}^3 OR context_text:{escaped}"

        cypher = """
        CALL db.index.fulltext.queryNodes('observation_mention_fulltext', $ft_query)
        YIELD node AS obs, score
        OPTIONAL MATCH (chunk:RawChunk)-[:MENTIONS_OBSERVATION]->(obs)
        OPTIONAL MATCH (chunk)-[:CHUNK_ABOUT_DISEASE]->(d:DiseaseEntity)
        RETURN obs.mention_id AS id,
               obs.mention_text AS text,
               obs.context_text AS context,
               obs.result_semantics AS semantics,
               obs.extraction_confidence AS confidence,
               chunk.chunk_id AS chunk_id,
               chunk.section_title AS section,
               d.disease_name AS disease,
               score
        ORDER BY score DESC
        LIMIT $top_k
        """

        results = []
        with self.driver.session() as session:
            try:
                for rec in session.run(cypher, ft_query=ft_query, top_k=top_k):
                    results.append(RetrievalResult(
                        id=f"observation:{rec['id']}",
                        title=f"[Xét nghiệm] {rec['text']}",
                        content=rec["context"] or rec["text"],
                        source_layer="observation",
                        score=rec["score"],
                        disease_name=rec["disease"] or "",
                        metadata={
                            "result_semantics": rec["semantics"],
                            "extraction_confidence": rec["confidence"],
                            "source_chunk": rec["chunk_id"],
                        }
                    ))
            except Exception as e:
                print(f"[unified:observation_fulltext] {e}")

        return results

    # ------------------------------------------------------------------
    # Layer 4: Graph relationship traversal (multi-hop reasoning)
    # ------------------------------------------------------------------

    def traverse_disease_graph(self, disease_name: str, top_k: int = 20) -> list[RetrievalResult]:
        """Full graph traversal from DiseaseEntity outward:
        Disease → Chunks → Signs, Services, Observations, Assertions
        Returns structured entity-level results, not just chunks.
        """
        cypher = """
        MATCH (d:DiseaseEntity)
        WHERE toLower(d.disease_name) CONTAINS toLower($disease)
           OR toLower(d.disease_id) CONTAINS toLower($disease)
        WITH d LIMIT 1

        // Get assertions about this disease
        OPTIONAL MATCH (a:ProtocolAssertion)-[:ASSERTION_ABOUT_DISEASE]->(d)
        WITH d, collect(DISTINCT {
            id: a.assertion_id,
            type: a.assertion_type,
            text: a.assertion_text,
            condition: a.condition_text,
            action: a.action_text,
            evidence: a.evidence_level
        })[..10] AS assertions

        // Get signs via chunks
        OPTIONAL MATCH (c:RawChunk)-[:CHUNK_ABOUT_DISEASE]->(d)
        OPTIONAL MATCH (c)-[:MENTIONS_SIGN]->(sign:RawSignMention)
        WITH d, assertions,
             collect(DISTINCT {
                 text: sign.mention_text,
                 modifier: sign.modifier_raw,
                 section: c.section_title
             })[..15] AS signs

        // Get services via chunks
        OPTIONAL MATCH (c2:RawChunk)-[:CHUNK_ABOUT_DISEASE]->(d)
        OPTIONAL MATCH (c2)-[:MENTIONS_SERVICE]->(svc:RawServiceMention)
        WITH d, assertions, signs,
             collect(DISTINCT {
                 text: svc.mention_text,
                 role: svc.medical_role,
                 condition: svc.condition_to_apply,
                 section: c2.section_title
             })[..15] AS services

        // Get summary
        OPTIONAL MATCH (s:ProtocolDiseaseSummary)-[:SUMMARIZES]->(d)

        RETURN d.disease_name AS disease,
               d.disease_id AS disease_id,
               assertions, signs, services,
               s.summary_text AS summary,
               s.key_drugs AS key_drugs,
               s.differential_diseases AS differential_diseases
        """

        results = []
        with self.driver.session() as session:
            try:
                rec = session.run(cypher, disease=disease_name).single()
                if not rec:
                    return []

                disease = rec["disease"] or disease_name

                # Summary result
                if rec["summary"]:
                    results.append(RetrievalResult(
                        id=f"graph:summary:{rec['disease_id']}",
                        title=f"[Tổng quan] {disease}",
                        content=rec["summary"],
                        source_layer="graph_traversal",
                        score=0.95,
                        disease_name=disease,
                        metadata={
                            "key_drugs": rec["key_drugs"] or [],
                            "differential_diseases": rec["differential_diseases"] or [],
                        }
                    ))

                # Assertions as individual results
                for a in (rec["assertions"] or []):
                    if not a.get("text"):
                        continue
                    content = a["text"]
                    if a.get("condition"):
                        content = f"[Nếu] {a['condition']} → [Thì] {a['action']}\n{a['text']}"
                    results.append(RetrievalResult(
                        id=f"graph:assertion:{a['id']}",
                        title=f"[{a['type'] or 'rule'}] {disease}",
                        content=content,
                        source_layer="graph_traversal",
                        score=0.90,
                        disease_name=disease,
                        metadata={"assertion_type": a["type"], "evidence_level": a.get("evidence")}
                    ))

                # Signs grouped
                sign_texts = [s for s in (rec["signs"] or []) if s.get("text")]
                if sign_texts:
                    sign_content = "\n".join(
                        f"• {s['text']}" + (f" ({s['modifier']})" if s.get("modifier") else "")
                        for s in sign_texts
                    )
                    results.append(RetrievalResult(
                        id=f"graph:signs:{rec['disease_id']}",
                        title=f"[Triệu chứng] {disease} ({len(sign_texts)} triệu chứng)",
                        content=sign_content,
                        source_layer="graph_traversal",
                        score=0.85,
                        disease_name=disease,
                    ))

                # Services grouped by role
                svc_items = [s for s in (rec["services"] or []) if s.get("text")]
                if svc_items:
                    svc_content = "\n".join(
                        f"• [{s.get('role', '?')}] {s['text']}"
                        + (f" — khi {s['condition']}" if s.get("condition") else "")
                        for s in svc_items
                    )
                    results.append(RetrievalResult(
                        id=f"graph:services:{rec['disease_id']}",
                        title=f"[Dịch vụ y tế] {disease} ({len(svc_items)} dịch vụ)",
                        content=svc_content,
                        source_layer="graph_traversal",
                        score=0.85,
                        disease_name=disease,
                    ))

            except Exception as e:
                print(f"[unified:traverse_disease] {e}")

        return results[:top_k]

    def reverse_lookup_by_signs(self, sign_names: list[str], top_k: int = 10) -> list[RetrievalResult]:
        """Reverse lookup: given symptoms → find diseases that mention them.
        Enables differential diagnosis reasoning.
        """
        if not sign_names:
            return []

        cypher = """
        UNWIND $signs AS sign_query
        MATCH (sign:RawSignMention)
        WHERE toLower(sign.mention_text) CONTAINS toLower(sign_query)
        WITH sign, sign_query
        MATCH (chunk:RawChunk)-[:MENTIONS_SIGN]->(sign)
        MATCH (chunk)-[:CHUNK_ABOUT_DISEASE]->(d:DiseaseEntity)
        WITH d.disease_name AS disease, d.disease_id AS disease_id,
             collect(DISTINCT sign.mention_text) AS matched_signs,
             count(DISTINCT sign) AS match_count
        RETURN disease, disease_id, matched_signs, match_count
        ORDER BY match_count DESC
        LIMIT $top_k
        """

        results = []
        with self.driver.session() as session:
            try:
                for rec in session.run(cypher, signs=sign_names, top_k=top_k):
                    matched = rec["matched_signs"] or []
                    results.append(RetrievalResult(
                        id=f"reverse:disease:{rec['disease_id']}",
                        title=f"[Chẩn đoán phân biệt] {rec['disease']}",
                        content=f"Bệnh {rec['disease']} có {rec['match_count']} triệu chứng phù hợp: {', '.join(matched)}",
                        source_layer="graph_reasoning",
                        score=rec["match_count"] / max(len(sign_names), 1),
                        disease_name=rec["disease"],
                        metadata={"matched_signs": matched, "match_count": rec["match_count"]}
                    ))
            except Exception as e:
                print(f"[unified:reverse_signs] {e}")

        return results

    def cross_disease_compare(self, disease_a: str, disease_b: str) -> list[RetrievalResult]:
        """Compare two diseases: shared signs, different treatments, unique features."""
        cypher = """
        MATCH (da:DiseaseEntity) WHERE toLower(da.disease_name) CONTAINS toLower($a)
        MATCH (db:DiseaseEntity) WHERE toLower(db.disease_name) CONTAINS toLower($b)
        WITH da, db

        // Signs for disease A
        OPTIONAL MATCH (ca:RawChunk)-[:CHUNK_ABOUT_DISEASE]->(da)
        OPTIONAL MATCH (ca)-[:MENTIONS_SIGN]->(sa:RawSignMention)
        WITH da, db, collect(DISTINCT sa.mention_text) AS signs_a

        // Signs for disease B
        OPTIONAL MATCH (cb:RawChunk)-[:CHUNK_ABOUT_DISEASE]->(db)
        OPTIONAL MATCH (cb)-[:MENTIONS_SIGN]->(sb:RawSignMention)
        WITH da, db, signs_a, collect(DISTINCT sb.mention_text) AS signs_b

        // Services for disease A
        OPTIONAL MATCH (ca2:RawChunk)-[:CHUNK_ABOUT_DISEASE]->(da)
        OPTIONAL MATCH (ca2)-[:MENTIONS_SERVICE]->(sva:RawServiceMention)
        WITH da, db, signs_a, signs_b, collect(DISTINCT sva.mention_text) AS services_a

        // Services for disease B
        OPTIONAL MATCH (cb2:RawChunk)-[:CHUNK_ABOUT_DISEASE]->(db)
        OPTIONAL MATCH (cb2)-[:MENTIONS_SERVICE]->(svb:RawServiceMention)

        RETURN da.disease_name AS disease_a, db.disease_name AS disease_b,
               signs_a, signs_b,
               services_a,
               collect(DISTINCT svb.mention_text) AS services_b
        """

        results = []
        with self.driver.session() as session:
            try:
                rec = session.run(cypher, a=disease_a, b=disease_b).single()
                if not rec:
                    return []

                sa = set(s for s in (rec["signs_a"] or []) if s)
                sb = set(s for s in (rec["signs_b"] or []) if s)
                sva = set(s for s in (rec["services_a"] or []) if s)
                svb = set(s for s in (rec["services_b"] or []) if s)

                shared_signs = sa & sb
                unique_a_signs = sa - sb
                unique_b_signs = sb - sa
                shared_services = sva & svb
                unique_a_services = sva - svb
                unique_b_services = svb - sva

                content_parts = []
                if shared_signs:
                    content_parts.append(f"**Triệu chứng chung ({len(shared_signs)}):** {', '.join(list(shared_signs)[:10])}")
                if unique_a_signs:
                    content_parts.append(f"**Chỉ {rec['disease_a']} ({len(unique_a_signs)}):** {', '.join(list(unique_a_signs)[:10])}")
                if unique_b_signs:
                    content_parts.append(f"**Chỉ {rec['disease_b']} ({len(unique_b_signs)}):** {', '.join(list(unique_b_signs)[:10])}")
                if shared_services:
                    content_parts.append(f"**Dịch vụ chung ({len(shared_services)}):** {', '.join(list(shared_services)[:10])}")
                if unique_a_services:
                    content_parts.append(f"**Điều trị chỉ {rec['disease_a']} ({len(unique_a_services)}):** {', '.join(list(unique_a_services)[:10])}")
                if unique_b_services:
                    content_parts.append(f"**Điều trị chỉ {rec['disease_b']} ({len(unique_b_services)}):** {', '.join(list(unique_b_services)[:10])}")

                results.append(RetrievalResult(
                    id=f"compare:{disease_a}:{disease_b}",
                    title=f"[So sánh] {rec['disease_a']} vs {rec['disease_b']}",
                    content="\n".join(content_parts) if content_parts else "Không đủ dữ liệu để so sánh.",
                    source_layer="graph_reasoning",
                    score=0.90,
                    metadata={
                        "shared_signs": len(shared_signs),
                        "unique_a_signs": len(unique_a_signs),
                        "unique_b_signs": len(unique_b_signs),
                    }
                ))
            except Exception as e:
                print(f"[unified:cross_disease] {e}")

        return results

    # ------------------------------------------------------------------
    # Layer 5: Claims Insights (real-world frequency data)
    # ------------------------------------------------------------------

    def search_claims_insights(self, disease_name: str = None,
                               service_name: str = None,
                               top_k: int = 10) -> list[RetrievalResult]:
        """Search Claims Insights namespace for real-world claims data."""
        results = []

        if disease_name:
            cypher = """
            MATCH (d:CIDisease)
            WHERE toLower(d.disease_name) CONTAINS toLower($disease)
               OR toLower(d.icd10) CONTAINS toLower($disease)
            OPTIONAL MATCH (d)-[r:CI_INDICATES_SERVICE]->(s:CIService)
            WITH d, s, r
            ORDER BY r.max_score DESC
            WITH d, collect({
                service: s.service_name,
                code: s.service_code,
                category: s.category_name,
                avg_cost: s.avg_cost_vnd,
                max_score: r.max_score,
                case_support: r.case_support,
                roles: r.roles_json
            })[..$top_k] AS services

            OPTIONAL MATCH (d)-[rs:CI_HAS_SIGN]->(sign:CISign)
            WITH d, services, collect({
                text: sign.text,
                support: rs.support_cases
            })[..10] AS signs

            RETURN d.disease_name AS disease,
                   d.icd10 AS icd10,
                   d.case_count AS case_count,
                   services, signs
            LIMIT 3
            """
            with self.driver.session() as session:
                try:
                    for rec in session.run(cypher, disease=disease_name, top_k=top_k):
                        services = rec["services"] or []
                        signs = rec["signs"] or []
                        svc_lines = [
                            f"• {s['service']} (code: {s['code']}, score: {s['max_score']}, {s['case_support']} ca)"
                            for s in services if s.get("service")
                        ]
                        sign_lines = [
                            f"• {s['text']} ({s['support']} ca)"
                            for s in signs if s.get("text")
                        ]
                        content = f"ICD-10: {rec['icd10']} | Số ca: {rec['case_count']}\n"
                        if sign_lines:
                            content += f"\nTriệu chứng thường gặp:\n" + "\n".join(sign_lines[:8])
                        if svc_lines:
                            content += f"\n\nDịch vụ liên quan:\n" + "\n".join(svc_lines[:10])

                        results.append(RetrievalResult(
                            id=f"claims:{rec['icd10'] or disease_name}",
                            title=f"[Claims Data] {rec['disease']}",
                            content=content,
                            source_layer="claims_insights",
                            score=0.80,
                            disease_name=rec["disease"] or "",
                            metadata={
                                "icd10": rec["icd10"],
                                "case_count": rec["case_count"],
                                "service_count": len(services),
                            }
                        ))
                except Exception as e:
                    print(f"[unified:claims_disease] {e}")

        if service_name:
            cypher_svc = """
            MATCH (s:CIService)
            WHERE toLower(s.service_name) CONTAINS toLower($service)
               OR toLower(s.service_code) CONTAINS toLower($service)
            OPTIONAL MATCH (d:CIDisease)-[r:CI_INDICATES_SERVICE]->(s)
            WITH s, collect({
                disease: d.disease_name,
                icd10: d.icd10,
                max_score: r.max_score,
                case_support: r.case_support
            }) AS diseases
            RETURN s.service_name AS service,
                   s.service_code AS code,
                   s.category_name AS category,
                   s.avg_cost_vnd AS avg_cost,
                   s.total_occurrences AS occurrences,
                   diseases
            LIMIT $top_k
            """
            with self.driver.session() as session:
                try:
                    for rec in session.run(cypher_svc, service=service_name, top_k=top_k):
                        diseases = rec["diseases"] or []
                        disease_lines = [
                            f"• {d['disease']} (ICD: {d['icd10']}, score: {d['max_score']}, {d['case_support']} ca)"
                            for d in diseases if d.get("disease")
                        ]
                        content = f"Dịch vụ: {rec['service']} ({rec['code']})\n"
                        content += f"Phân loại: {rec['category']} | Chi phí TB: {rec['avg_cost']} VND | Tần suất: {rec['occurrences']}\n"
                        if disease_lines:
                            content += f"\nBệnh liên quan:\n" + "\n".join(disease_lines[:10])

                        results.append(RetrievalResult(
                            id=f"claims:svc:{rec['code']}",
                            title=f"[Claims Service] {rec['service']}",
                            content=content,
                            source_layer="claims_insights",
                            score=0.75,
                            metadata={
                                "service_code": rec["code"],
                                "avg_cost": rec["avg_cost"],
                                "disease_count": len(diseases),
                            }
                        ))
                except Exception as e:
                    print(f"[unified:claims_service] {e}")

        return results[:top_k]

    # ------------------------------------------------------------------
    # Layer 6: Experience memory (pipeline learnings)
    # ------------------------------------------------------------------

    def search_experience(self, query: str, top_k: int = 5) -> list[RetrievalResult]:
        """Search Experience nodes for relevant past learnings."""
        # Try vector search first
        results = []

        if self.embedding_fn:
            qv = self.embedding_fn(query)
            cypher = """
            CALL db.index.vector.queryNodes('experience_vector_index', $top_k, $qv)
            YIELD node AS exp, score
            RETURN exp.experience_id AS id,
                   exp.type AS type,
                   exp.search_text AS text,
                   exp.title AS title,
                   score
            ORDER BY score DESC
            LIMIT $top_k
            """
            with self.driver.session() as session:
                try:
                    for rec in session.run(cypher, qv=qv, top_k=top_k):
                        results.append(RetrievalResult(
                            id=f"experience:{rec['id']}",
                            title=f"[Kinh nghiệm] {rec['title'] or rec['type'] or 'experience'}",
                            content=rec["text"] or "",
                            source_layer="experience",
                            score=rec["score"],
                            metadata={"experience_type": rec["type"]}
                        ))
                except Exception as e:
                    # Index may not exist if no experience data
                    pass

        # Also try fulltext
        if len(results) < top_k:
            escaped = self._escape_lucene(query)
            cypher_ft = """
            CALL db.index.fulltext.queryNodes('experience_fulltext', $ft_query)
            YIELD node AS exp, score
            RETURN exp.experience_id AS id,
                   exp.type AS type,
                   exp.search_text AS text,
                   exp.title AS title,
                   score
            ORDER BY score DESC
            LIMIT $top_k
            """
            with self.driver.session() as session:
                try:
                    seen = {r.id for r in results}
                    for rec in session.run(cypher_ft, ft_query=escaped, top_k=top_k):
                        rid = f"experience:{rec['id']}"
                        if rid in seen:
                            continue
                        results.append(RetrievalResult(
                            id=rid,
                            title=f"[Kinh nghiệm] {rec['title'] or rec['type']}",
                            content=rec["text"] or "",
                            source_layer="experience",
                            score=rec["score"] * 0.7,
                        ))
                except Exception:
                    pass

        return results[:top_k]

    # ==================================================================
    # UNIFIED RETRIEVE — fans out to all layers based on intent
    # ==================================================================

    def retrieve(self, query: str, intent: str = "general",
                 disease_name: str = None, entities: list[dict] = None,
                 top_k: int = 12) -> tuple[list[RetrievalResult], dict]:
        """
        Main entry point. Fans out to relevant layers based on intent,
        merges via RRF, returns (results, trace).

        intent: lookup | general | dosage | procedure | diagnosis |
                contraindication | compare
        entities: [{"name": "...", "type": "Drug|Disease|Symptom|..."}]
        """
        t0 = time.time()
        entities = entities or []
        trace = {"intent": intent, "disease": disease_name, "layers": []}
        result_lists = []

        # ------ Always search assertions (clinical rules) ------
        t1 = time.time()
        assertion_type = None
        if intent == "contraindication":
            assertion_type = "contraindication"
        elif intent == "dosage":
            assertion_type = "dosage_rule"

        assertions = self.search_assertions(
            query, disease_name=disease_name,
            assertion_type=assertion_type, top_k=8)
        if assertions:
            result_lists.append(assertions)
        trace["layers"].append({
            "layer": "assertion", "count": len(assertions),
            "ms": int((time.time() - t1) * 1000)
        })

        # ------ Disease summary (for overview / general queries) ------
        if intent in ("general", "lookup", "diagnosis"):
            t1 = time.time()
            summaries = self.search_summaries(query, disease_name=disease_name, top_k=3)
            if summaries:
                result_lists.append(summaries)
            trace["layers"].append({
                "layer": "summary", "count": len(summaries),
                "ms": int((time.time() - t1) * 1000)
            })

        # ------ Entity mention search ------
        # Signs: useful for diagnosis, general, lookup
        if intent in ("diagnosis", "general", "lookup"):
            t1 = time.time()
            sign_query = query
            # If entities have symptoms, search those specifically
            symptom_entities = [e["name"] for e in entities if e.get("type") in ("Symptom", "Sign")]
            if symptom_entities:
                sign_query = " ".join(symptom_entities)
            signs = self.search_sign_mentions(sign_query, top_k=8)
            if signs:
                result_lists.append(signs)
            trace["layers"].append({
                "layer": "sign_mentions", "count": len(signs),
                "ms": int((time.time() - t1) * 1000)
            })

        # Services/drugs: useful for dosage, procedure, contraindication
        if intent in ("dosage", "procedure", "contraindication", "general"):
            t1 = time.time()
            svc_query = query
            drug_entities = [e["name"] for e in entities if e.get("type") in ("Drug", "Procedure")]
            if drug_entities:
                svc_query = " ".join(drug_entities)
            services = self.search_service_mentions(svc_query, top_k=8)
            if services:
                result_lists.append(services)
            trace["layers"].append({
                "layer": "service_mentions", "count": len(services),
                "ms": int((time.time() - t1) * 1000)
            })

        # Observations: useful for diagnosis
        if intent in ("diagnosis", "general"):
            t1 = time.time()
            observations = self.search_observation_mentions(query, top_k=5)
            if observations:
                result_lists.append(observations)
            trace["layers"].append({
                "layer": "observation_mentions", "count": len(observations),
                "ms": int((time.time() - t1) * 1000)
            })

        # ------ Graph traversal ------
        if disease_name and intent in ("general", "lookup", "diagnosis", "compare"):
            t1 = time.time()
            graph_results = self.traverse_disease_graph(disease_name, top_k=10)
            if graph_results:
                result_lists.append(graph_results)
            trace["layers"].append({
                "layer": "graph_traversal", "count": len(graph_results),
                "ms": int((time.time() - t1) * 1000)
            })

        # ------ Reverse sign lookup (differential diagnosis) ------
        if intent == "diagnosis":
            symptom_names = [e["name"] for e in entities if e.get("type") in ("Symptom", "Sign")]
            if symptom_names:
                t1 = time.time()
                reverse = self.reverse_lookup_by_signs(symptom_names, top_k=5)
                if reverse:
                    result_lists.append(reverse)
                trace["layers"].append({
                    "layer": "reverse_sign_lookup", "count": len(reverse),
                    "ms": int((time.time() - t1) * 1000)
                })

        # ------ Cross-disease comparison ------
        if intent == "compare":
            disease_entities = [e["name"] for e in entities if e.get("type") == "Disease"]
            if len(disease_entities) >= 2:
                t1 = time.time()
                comparison = self.cross_disease_compare(disease_entities[0], disease_entities[1])
                if comparison:
                    result_lists.append(comparison)
                trace["layers"].append({
                    "layer": "cross_disease_compare", "count": len(comparison),
                    "ms": int((time.time() - t1) * 1000)
                })

        # ------ Claims Insights (real-world data) ------
        if disease_name or any(e.get("type") in ("Drug", "Procedure") for e in entities):
            t1 = time.time()
            service_entity = next((e["name"] for e in entities if e.get("type") in ("Drug", "Procedure")), None)
            claims = self.search_claims_insights(
                disease_name=disease_name,
                service_name=service_entity,
                top_k=3)
            if claims:
                result_lists.append(claims)
            trace["layers"].append({
                "layer": "claims_insights", "count": len(claims),
                "ms": int((time.time() - t1) * 1000)
            })

        # ------ Experience memory ------
        t1 = time.time()
        experience = self.search_experience(query, top_k=3)
        if experience:
            result_lists.append(experience)
        trace["layers"].append({
            "layer": "experience", "count": len(experience),
            "ms": int((time.time() - t1) * 1000)
        })

        # ------ Merge via RRF ------
        merged = _rrf_merge(result_lists)[:top_k]

        trace["total_results"] = len(merged)
        trace["total_ms"] = int((time.time() - t0) * 1000)
        trace["layers_searched"] = len(trace["layers"])

        return merged, trace
