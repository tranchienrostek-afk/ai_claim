"""
QueryMemory — Learn from past queries.

Inspired by Claude Code's memory system:
  - findRelevantMemories() auto-surfaces 5 most relevant memories per query
  - Lessons from past interactions inform future search strategy
  - Memory aging: recent discoveries surface faster

Pathway equivalent:
  After each query:
    - Low confidence → save: "query X about Y failed because Z"
    - High confidence → save: "query type X works best with layers [A, B]"
  Before each query:
    - Check: "have I seen a similar query? What worked?"
    - Skip layers known to be empty for this disease
    - Boost layers known to be effective

Storage: Neo4j :QueryMemory nodes with vector embeddings for similarity search.
"""

from __future__ import annotations

import hashlib
import json
import time
from dataclasses import dataclass, field
from typing import Any


@dataclass
class QueryLesson:
    """A lesson learned from a past query."""
    query_hash: str
    query_text: str
    disease_name: str = ""
    intent: str = "general"
    confidence: float = 0.0
    was_sufficient: bool = False
    effective_layers: list[str] = field(default_factory=list)  # layers that returned useful data
    empty_layers: list[str] = field(default_factory=list)      # layers that returned nothing
    refinements_used: list[str] = field(default_factory=list)  # what refinement strategies helped
    coverage_gaps: list[str] = field(default_factory=list)
    timestamp: float = 0.0
    access_count: int = 0

    def to_hint_text(self) -> str:
        """Generate a short hint for LLM context injection."""
        parts = []
        if self.effective_layers:
            parts.append(f"Layers hiệu quả: {', '.join(self.effective_layers)}")
        if self.empty_layers:
            parts.append(f"Layers trống: {', '.join(self.empty_layers)}")
        if self.refinements_used:
            parts.append(f"Refinements đã dùng: {', '.join(self.refinements_used)}")
        if self.coverage_gaps:
            parts.append(f"Gaps: {', '.join(self.coverage_gaps[:3])}")
        if self.confidence > 0:
            parts.append(f"Confidence lần trước: {self.confidence:.0%}")
        return " | ".join(parts) if parts else ""


def _query_hash(query: str, disease: str = "") -> str:
    """Deterministic hash for deduplication."""
    key = f"{query.lower().strip()}|{disease.lower().strip()}"
    return hashlib.sha256(key.encode()).hexdigest()[:16]


class QueryMemory:
    """Learns from past queries, surfaces relevant lessons for future queries."""

    # Neo4j node label for query memories
    LABEL = "QueryMemory"

    def __init__(self, driver, embedding_fn=None, max_memories: int = 500):
        """
        Args:
            driver: Neo4j driver
            embedding_fn: text → embedding vector (for similarity search)
            max_memories: max memories to keep (LRU eviction)
        """
        self.driver = driver
        self.embedding_fn = embedding_fn
        self.max_memories = max_memories
        self._ensure_schema()

    def _ensure_schema(self):
        """Create indexes for QueryMemory nodes."""
        with self.driver.session() as session:
            try:
                session.run(f"""
                    CREATE INDEX query_memory_hash_idx IF NOT EXISTS
                    FOR (n:{self.LABEL}) ON (n.query_hash)
                """)
            except Exception:
                pass
            try:
                session.run(f"""
                    CREATE FULLTEXT INDEX query_memory_fulltext IF NOT EXISTS
                    FOR (n:{self.LABEL}) ON EACH [n.query_text, n.disease_name]
                """)
            except Exception:
                pass
            if self.embedding_fn:
                try:
                    session.run(f"""
                        CREATE VECTOR INDEX query_memory_vector_idx IF NOT EXISTS
                        FOR (n:{self.LABEL}) ON (n.embedding)
                        OPTIONS {{indexConfig: {{
                            `vector.dimensions`: 1536,
                            `vector.similarity_function`: 'cosine'
                        }}}}
                    """)
                except Exception:
                    pass

    def save_lesson(self, query: str, disease_name: str = "",
                    intent: str = "general", confidence: float = 0.0,
                    was_sufficient: bool = False,
                    effective_layers: list[str] = None,
                    empty_layers: list[str] = None,
                    refinements_used: list[str] = None,
                    coverage_gaps: list[str] = None):
        """Save a lesson from a completed query."""
        qhash = _query_hash(query, disease_name)

        params: dict[str, Any] = {
            "query_hash": qhash,
            "query_text": query[:500],
            "disease_name": disease_name or "",
            "intent": intent,
            "confidence": confidence,
            "was_sufficient": was_sufficient,
            "effective_layers": json.dumps(effective_layers or []),
            "empty_layers": json.dumps(empty_layers or []),
            "refinements_used": json.dumps(refinements_used or []),
            "coverage_gaps": json.dumps(coverage_gaps or []),
            "timestamp": time.time(),
        }

        # Generate embedding for similarity search
        if self.embedding_fn:
            try:
                embed_text = f"{query} {disease_name} {intent}"
                params["embedding"] = self.embedding_fn(embed_text)
            except Exception:
                params["embedding"] = None

        cypher = f"""
        MERGE (m:{self.LABEL} {{query_hash: $query_hash}})
        SET m.query_text = $query_text,
            m.disease_name = $disease_name,
            m.intent = $intent,
            m.confidence = $confidence,
            m.was_sufficient = $was_sufficient,
            m.effective_layers = $effective_layers,
            m.empty_layers = $empty_layers,
            m.refinements_used = $refinements_used,
            m.coverage_gaps = $coverage_gaps,
            m.timestamp = $timestamp,
            m.access_count = coalesce(m.access_count, 0) + 1
        """
        if params.get("embedding"):
            cypher += ", m.embedding = $embedding"

        with self.driver.session() as session:
            try:
                session.run(cypher, **params)
            except Exception as e:
                print(f"[QueryMemory] Save error: {e}")

        # LRU eviction
        self._evict_old()

    def recall(self, query: str, disease_name: str = "",
               intent: str = "", top_k: int = 3) -> list[QueryLesson]:
        """Find relevant lessons from past queries.

        Uses vector similarity if embeddings available, falls back to fulltext.
        """
        lessons = []

        # Strategy 1: Exact hash match (same query seen before)
        qhash = _query_hash(query, disease_name)
        with self.driver.session() as session:
            try:
                rec = session.run(f"""
                    MATCH (m:{self.LABEL} {{query_hash: $hash}})
                    SET m.access_count = coalesce(m.access_count, 0) + 1
                    RETURN m
                """, hash=qhash).single()
                if rec:
                    lessons.append(self._record_to_lesson(rec["m"]))
            except Exception:
                pass

        if len(lessons) >= top_k:
            return lessons[:top_k]

        # Strategy 2: Vector similarity
        if self.embedding_fn and len(lessons) < top_k:
            try:
                embed_text = f"{query} {disease_name} {intent}"
                qv = self.embedding_fn(embed_text)
                with self.driver.session() as session:
                    for rec in session.run(f"""
                        CALL db.index.vector.queryNodes('query_memory_vector_idx', $top_k, $qv)
                        YIELD node AS m, score
                        WHERE m.query_hash <> $exclude_hash
                        RETURN m, score
                        ORDER BY score DESC
                        LIMIT $top_k
                    """, qv=qv, top_k=top_k, exclude_hash=qhash):
                        lesson = self._record_to_lesson(rec["m"])
                        lessons.append(lesson)
            except Exception:
                pass

        # Strategy 3: Fulltext fallback
        if len(lessons) < top_k:
            try:
                from server_support.unified_retriever import UnifiedRetriever
                escaped = query[:100].replace('"', '\\"')
                with self.driver.session() as session:
                    seen_hashes = {l.query_hash for l in lessons}
                    for rec in session.run(f"""
                        CALL db.index.fulltext.queryNodes('query_memory_fulltext', $ft)
                        YIELD node AS m, score
                        RETURN m, score
                        ORDER BY score DESC
                        LIMIT $top_k
                    """, ft=escaped, top_k=top_k + len(seen_hashes)):
                        m = rec["m"]
                        if m.get("query_hash") not in seen_hashes:
                            lessons.append(self._record_to_lesson(m))
                            seen_hashes.add(m.get("query_hash"))
            except Exception:
                pass

        return lessons[:top_k]

    def get_layer_effectiveness(self, disease_name: str = "",
                                intent: str = "") -> dict[str, float]:
        """Aggregate: which layers work best for this disease/intent combo?

        Returns {layer_name: effectiveness_score} based on past queries.
        """
        filters = []
        params: dict[str, Any] = {}

        if disease_name:
            filters.append("toLower(m.disease_name) CONTAINS toLower($disease)")
            params["disease"] = disease_name
        if intent:
            filters.append("m.intent = $intent")
            params["intent"] = intent

        where = "WHERE " + " AND ".join(filters) if filters else ""

        cypher = f"""
        MATCH (m:{self.LABEL})
        {where}
        RETURN m.effective_layers AS effective, m.empty_layers AS empty,
               m.confidence AS confidence, m.was_sufficient AS sufficient
        ORDER BY m.timestamp DESC
        LIMIT 50
        """

        layer_scores: dict[str, list[float]] = {}
        with self.driver.session() as session:
            try:
                for rec in session.run(cypher, **params):
                    effective = json.loads(rec["effective"] or "[]")
                    empty = json.loads(rec["empty"] or "[]")
                    conf = rec["confidence"] or 0.5

                    for layer in effective:
                        layer_scores.setdefault(layer, []).append(conf)
                    for layer in empty:
                        layer_scores.setdefault(layer, []).append(0.0)
            except Exception:
                pass

        # Average scores per layer
        return {
            layer: sum(scores) / len(scores) if scores else 0.0
            for layer, scores in layer_scores.items()
        }

    def _record_to_lesson(self, node_props) -> QueryLesson:
        """Convert Neo4j node properties to QueryLesson."""
        return QueryLesson(
            query_hash=node_props.get("query_hash", ""),
            query_text=node_props.get("query_text", ""),
            disease_name=node_props.get("disease_name", ""),
            intent=node_props.get("intent", "general"),
            confidence=node_props.get("confidence", 0.0),
            was_sufficient=node_props.get("was_sufficient", False),
            effective_layers=json.loads(node_props.get("effective_layers", "[]")),
            empty_layers=json.loads(node_props.get("empty_layers", "[]")),
            refinements_used=json.loads(node_props.get("refinements_used", "[]")),
            coverage_gaps=json.loads(node_props.get("coverage_gaps", "[]")),
            timestamp=node_props.get("timestamp", 0.0),
            access_count=node_props.get("access_count", 0),
        )

    def _evict_old(self):
        """Remove oldest memories if over limit."""
        with self.driver.session() as session:
            try:
                session.run(f"""
                    MATCH (m:{self.LABEL})
                    WITH m ORDER BY m.timestamp DESC
                    SKIP $limit
                    DETACH DELETE m
                """, limit=self.max_memories)
            except Exception:
                pass

    def get_stats(self) -> dict:
        """Stats for API/dashboard."""
        with self.driver.session() as session:
            try:
                rec = session.run(f"""
                    MATCH (m:{self.LABEL})
                    RETURN count(m) AS total,
                           avg(m.confidence) AS avg_confidence,
                           sum(CASE WHEN m.was_sufficient THEN 1 ELSE 0 END) AS sufficient_count
                """).single()
                return {
                    "total_memories": rec["total"] or 0,
                    "avg_confidence": round(rec["avg_confidence"] or 0, 3),
                    "sufficient_rate": round((rec["sufficient_count"] or 0) / max(rec["total"] or 1, 1), 3),
                }
            except Exception:
                return {"total_memories": 0, "avg_confidence": 0, "sufficient_rate": 0}
