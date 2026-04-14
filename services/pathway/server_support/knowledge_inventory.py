"""
KnowledgeInventory — Pathway's self-awareness layer.

Inspired by Claude Code's pre-context pattern: before answering any query,
Claude Code runs git status, reads CLAUDE.md, scans memory to know WHERE it is
and WHAT it knows. Pathway needs the same for its Neo4j knowledge graph.

This module:
1. On startup: scans Neo4j to build a cached inventory of all knowledge
2. Per-query: instantly answers "do I have data about X?" without hitting search
3. Periodically refreshes (configurable TTL)

Usage:
    inventory = KnowledgeInventory(driver)
    inventory.refresh()  # initial scan

    # Before searching:
    avail = inventory.check_availability("sốt xuất huyết dengue")
    # Returns: {has_chunks: True, chunk_count: 489, has_assertions: True, ...}
"""

from __future__ import annotations

import time
import threading
from dataclasses import dataclass, field
from typing import Any


@dataclass
class DiseaseProfile:
    """What Pathway knows about a specific disease."""
    disease_name: str
    disease_id: str
    chunk_count: int = 0
    assertion_count: int = 0
    sign_count: int = 0
    service_count: int = 0
    observation_count: int = 0
    has_summary: bool = False
    section_types: list[str] = field(default_factory=list)
    source_types: list[str] = field(default_factory=list)  # BYT, hospital, etc.
    hospitals: list[str] = field(default_factory=list)

    @property
    def coverage_score(self) -> float:
        """0.0-1.0 score of how well this disease is covered."""
        factors = [
            min(self.chunk_count / 50, 1.0) * 0.30,        # chunks (50+ = full)
            min(self.assertion_count / 10, 1.0) * 0.25,     # assertions (10+ = full)
            min(self.sign_count / 5, 1.0) * 0.15,           # signs (5+ = full)
            min(self.service_count / 5, 1.0) * 0.15,        # services (5+ = full)
            (1.0 if self.has_summary else 0.0) * 0.10,      # summary
            min(len(self.section_types) / 5, 1.0) * 0.05,   # section variety
        ]
        return sum(factors)

    def to_text(self) -> str:
        """Human-readable summary for LLM context injection."""
        parts = [f"Bệnh: {self.disease_name} (coverage: {self.coverage_score:.0%})"]
        parts.append(f"  Chunks: {self.chunk_count}, Assertions: {self.assertion_count}, "
                     f"Signs: {self.sign_count}, Services: {self.service_count}, "
                     f"Observations: {self.observation_count}")
        if self.has_summary:
            parts.append("  Có tóm tắt bệnh (ProtocolDiseaseSummary)")
        if self.section_types:
            parts.append(f"  Sections: {', '.join(self.section_types)}")
        if self.source_types:
            parts.append(f"  Sources: {', '.join(self.source_types)}")
        if self.hospitals:
            parts.append(f"  Hospitals: {', '.join(self.hospitals)}")
        return "\n".join(parts)


@dataclass
class InventorySnapshot:
    """Full inventory of everything Pathway knows."""
    # Global counts
    total_chunks: int = 0
    total_assertions: int = 0
    total_signs: int = 0
    total_services: int = 0
    total_observations: int = 0
    total_summaries: int = 0
    total_experiences: int = 0
    total_ci_diseases: int = 0
    total_ci_services: int = 0

    # Per-disease profiles
    diseases: dict[str, DiseaseProfile] = field(default_factory=dict)

    # Index health
    indexes: list[dict] = field(default_factory=list)
    missing_indexes: list[str] = field(default_factory=list)

    # Metadata
    scan_time_ms: int = 0
    scanned_at: float = 0.0  # time.time()

    @property
    def disease_count(self) -> int:
        return len(self.diseases)

    @property
    def age_seconds(self) -> float:
        return time.time() - self.scanned_at if self.scanned_at else float('inf')

    def get_disease(self, name: str) -> DiseaseProfile | None:
        """Fuzzy match disease name."""
        name_lower = name.lower()
        # Exact match
        for dname, profile in self.diseases.items():
            if dname.lower() == name_lower:
                return profile
        # Partial match
        for dname, profile in self.diseases.items():
            if name_lower in dname.lower() or dname.lower() in name_lower:
                return profile
        return None

    def to_context_text(self, disease_name: str = None) -> str:
        """Generate context text for LLM injection — what Pathway knows."""
        lines = []
        lines.append(f"=== Pathway Knowledge Inventory ===")
        lines.append(f"Tổng: {self.total_chunks} chunks, {self.total_assertions} assertions, "
                     f"{self.total_signs} signs, {self.total_services} services, "
                     f"{self.total_observations} observations")
        lines.append(f"Diseases: {self.disease_count} | Summaries: {self.total_summaries} | "
                     f"Experience: {self.total_experiences}")
        lines.append(f"Claims Insights: {self.total_ci_diseases} diseases, {self.total_ci_services} services")

        if disease_name:
            profile = self.get_disease(disease_name)
            if profile:
                lines.append(f"\n--- Data cho '{disease_name}' ---")
                lines.append(profile.to_text())
            else:
                lines.append(f"\n⚠️ KHÔNG CÓ DATA cho '{disease_name}' trong knowledge graph.")
                # Suggest similar diseases
                suggestions = self._suggest_similar(disease_name)
                if suggestions:
                    lines.append(f"Bệnh tương tự có data: {', '.join(suggestions[:5])}")

        return "\n".join(lines)

    def _suggest_similar(self, name: str) -> list[str]:
        """Find diseases with similar names."""
        import unicodedata
        def strip(t):
            t = t.lower().replace('đ', 'd').replace('Đ', 'd')
            return ''.join(c for c in unicodedata.normalize('NFKD', t) if not unicodedata.combining(c))

        target = strip(name)
        scored = []
        for dname in self.diseases:
            dstrip = strip(dname)
            # Simple overlap
            common = sum(1 for w in target.split() if w in dstrip)
            if common > 0:
                scored.append((common, dname))
        scored.sort(key=lambda x: x[0], reverse=True)
        return [s[1] for s in scored]


# Expected indexes that should exist
EXPECTED_INDEXES = [
    "rawchunk_vector_idx",
    "raw_chunk_fulltext",
    "protocolassertion_vector_idx",
    "assertion_fulltext",
    "protocoldiseasesummary_vector_idx",
    "sign_mention_fulltext",
    "service_mention_fulltext",
    "observation_mention_fulltext",
    "experience_vector_index",
    "experience_fulltext",
]


class KnowledgeInventory:
    """Pathway's self-awareness: knows what data exists before searching."""

    def __init__(self, driver, ttl_seconds: int = 300):
        """
        Args:
            driver: Neo4j driver
            ttl_seconds: how long before inventory is considered stale (default 5 min)
        """
        self.driver = driver
        self.ttl = ttl_seconds
        self._snapshot: InventorySnapshot | None = None
        self._lock = threading.Lock()

    @property
    def snapshot(self) -> InventorySnapshot:
        """Get current inventory, auto-refresh if stale."""
        if self._snapshot is None or self._snapshot.age_seconds > self.ttl:
            self.refresh()
        return self._snapshot

    def refresh(self) -> InventorySnapshot:
        """Full scan of Neo4j to build inventory."""
        t0 = time.time()
        snap = InventorySnapshot()

        with self.driver.session() as session:
            # --- Global counts ---
            counts_query = """
            OPTIONAL MATCH (c:RawChunk) WITH count(c) AS chunks
            OPTIONAL MATCH (a:ProtocolAssertion) WITH chunks, count(a) AS assertions
            OPTIONAL MATCH (s:RawSignMention) WITH chunks, assertions, count(s) AS signs
            OPTIONAL MATCH (svc:RawServiceMention) WITH chunks, assertions, signs, count(svc) AS services
            OPTIONAL MATCH (o:RawObservationMention) WITH chunks, assertions, signs, services, count(o) AS obs
            OPTIONAL MATCH (sum:ProtocolDiseaseSummary) WITH chunks, assertions, signs, services, obs, count(sum) AS summaries
            OPTIONAL MATCH (exp:Experience) WITH chunks, assertions, signs, services, obs, summaries, count(exp) AS experiences
            OPTIONAL MATCH (cid:CIDisease) WITH chunks, assertions, signs, services, obs, summaries, experiences, count(cid) AS ci_diseases
            OPTIONAL MATCH (cis:CIService)
            RETURN chunks, assertions, signs, services, obs, summaries, experiences, ci_diseases, count(cis) AS ci_services
            """
            try:
                rec = session.run(counts_query).single()
                if rec:
                    snap.total_chunks = rec["chunks"] or 0
                    snap.total_assertions = rec["assertions"] or 0
                    snap.total_signs = rec["signs"] or 0
                    snap.total_services = rec["services"] or 0
                    snap.total_observations = rec["obs"] or 0
                    snap.total_summaries = rec["summaries"] or 0
                    snap.total_experiences = rec["experiences"] or 0
                    snap.total_ci_diseases = rec["ci_diseases"] or 0
                    snap.total_ci_services = rec["ci_services"] or 0
            except Exception as e:
                print(f"[KnowledgeInventory] Global counts error: {e}")

            # --- Per-disease profiles ---
            disease_query = """
            MATCH (d:DiseaseEntity)
            OPTIONAL MATCH (c:RawChunk)-[:CHUNK_ABOUT_DISEASE]->(d)
            WITH d, count(DISTINCT c) AS chunk_cnt,
                 collect(DISTINCT c.section_type) AS stypes,
                 collect(DISTINCT c.source_type) AS src_types,
                 collect(DISTINCT c.hospital_name) AS hospitals

            OPTIONAL MATCH (a:ProtocolAssertion)-[:ASSERTION_ABOUT_DISEASE]->(d)
            WITH d, chunk_cnt, stypes, src_types, hospitals, count(DISTINCT a) AS assertion_cnt

            OPTIONAL MATCH (c2:RawChunk)-[:CHUNK_ABOUT_DISEASE]->(d)
            OPTIONAL MATCH (c2)-[:MENTIONS_SIGN]->(sign:RawSignMention)
            WITH d, chunk_cnt, stypes, src_types, hospitals, assertion_cnt,
                 count(DISTINCT sign) AS sign_cnt

            OPTIONAL MATCH (c3:RawChunk)-[:CHUNK_ABOUT_DISEASE]->(d)
            OPTIONAL MATCH (c3)-[:MENTIONS_SERVICE]->(svc:RawServiceMention)
            WITH d, chunk_cnt, stypes, src_types, hospitals, assertion_cnt, sign_cnt,
                 count(DISTINCT svc) AS svc_cnt

            OPTIONAL MATCH (c4:RawChunk)-[:CHUNK_ABOUT_DISEASE]->(d)
            OPTIONAL MATCH (c4)-[:MENTIONS_OBSERVATION]->(obs:RawObservationMention)
            WITH d, chunk_cnt, stypes, src_types, hospitals, assertion_cnt, sign_cnt, svc_cnt,
                 count(DISTINCT obs) AS obs_cnt

            OPTIONAL MATCH (sum:ProtocolDiseaseSummary)-[:SUMMARIZES]->(d)

            RETURN d.disease_name AS name, d.disease_id AS id,
                   chunk_cnt, assertion_cnt, sign_cnt, svc_cnt, obs_cnt,
                   sum IS NOT NULL AS has_summary,
                   [s IN stypes WHERE s IS NOT NULL] AS section_types,
                   [s IN src_types WHERE s IS NOT NULL] AS source_types,
                   [h IN hospitals WHERE h IS NOT NULL] AS hospital_names
            ORDER BY chunk_cnt DESC
            """
            try:
                for rec in session.run(disease_query):
                    name = rec["name"]
                    if not name:
                        continue
                    snap.diseases[name] = DiseaseProfile(
                        disease_name=name,
                        disease_id=rec["id"] or "",
                        chunk_count=rec["chunk_cnt"] or 0,
                        assertion_count=rec["assertion_cnt"] or 0,
                        sign_count=rec["sign_cnt"] or 0,
                        service_count=rec["svc_cnt"] or 0,
                        observation_count=rec["obs_cnt"] or 0,
                        has_summary=bool(rec["has_summary"]),
                        section_types=rec["section_types"] or [],
                        source_types=rec["source_types"] or [],
                        hospitals=rec["hospital_names"] or [],
                    )
            except Exception as e:
                print(f"[KnowledgeInventory] Disease profiles error: {e}")

            # --- Index health ---
            try:
                idx_rows = session.run(
                    "SHOW INDEXES YIELD name, state RETURN name, state"
                ).data()
                snap.indexes = idx_rows
                existing = {r["name"] for r in idx_rows}
                snap.missing_indexes = [i for i in EXPECTED_INDEXES if i not in existing]
            except Exception as e:
                print(f"[KnowledgeInventory] Index check error: {e}")

        snap.scan_time_ms = int((time.time() - t0) * 1000)
        snap.scanned_at = time.time()

        with self._lock:
            self._snapshot = snap

        print(f"[KnowledgeInventory] Scan complete: {snap.disease_count} diseases, "
              f"{snap.total_chunks} chunks, {snap.total_assertions} assertions "
              f"in {snap.scan_time_ms}ms")
        if snap.missing_indexes:
            print(f"[KnowledgeInventory] Missing indexes: {snap.missing_indexes}")

        return snap

    def check_availability(self, disease_name: str = None,
                           entity_name: str = None) -> dict[str, Any]:
        """Quick check: does Pathway have data about this topic?

        Returns a dict suitable for LLM context injection:
        {
            has_data: bool,
            disease_profile: DiseaseProfile | None,
            coverage_score: float,
            available_layers: ["chunk", "assertion", "sign", ...],
            recommendation: str  # what search strategy to use
        }
        """
        snap = self.snapshot
        result: dict[str, Any] = {
            "has_data": False,
            "disease_profile": None,
            "coverage_score": 0.0,
            "available_layers": [],
            "recommendation": "",
            "inventory_text": "",
        }

        if disease_name:
            profile = snap.get_disease(disease_name)
            if profile:
                result["has_data"] = True
                result["disease_profile"] = profile
                result["coverage_score"] = profile.coverage_score

                layers = ["chunk"]  # always have chunks if profile exists
                if profile.assertion_count > 0:
                    layers.append("assertion")
                if profile.sign_count > 0:
                    layers.append("sign")
                if profile.service_count > 0:
                    layers.append("service")
                if profile.observation_count > 0:
                    layers.append("observation")
                if profile.has_summary:
                    layers.append("summary")
                result["available_layers"] = layers

                # Recommendation
                if profile.coverage_score >= 0.7:
                    result["recommendation"] = "full_search"
                elif profile.coverage_score >= 0.4:
                    result["recommendation"] = "partial_search_warn_gaps"
                else:
                    result["recommendation"] = "limited_data_be_cautious"
            else:
                result["recommendation"] = "no_disease_data"
                # Check if claims insights has it
                if snap.total_ci_diseases > 0:
                    result["available_layers"] = ["claims_insights"]
                    result["recommendation"] = "claims_only_no_protocol"

        # Always check global layers
        if snap.total_experiences > 0:
            result["available_layers"].append("experience")
        if snap.total_ci_diseases > 0 and "claims_insights" not in result["available_layers"]:
            result["available_layers"].append("claims_insights")

        result["inventory_text"] = snap.to_context_text(disease_name)
        return result

    def get_all_diseases(self) -> list[str]:
        """List all disease names with data."""
        return list(self.snapshot.diseases.keys())

    def get_stats_summary(self) -> dict:
        """Summary stats for API/dashboard."""
        snap = self.snapshot
        return {
            "total_chunks": snap.total_chunks,
            "total_assertions": snap.total_assertions,
            "total_signs": snap.total_signs,
            "total_services": snap.total_services,
            "total_observations": snap.total_observations,
            "total_summaries": snap.total_summaries,
            "total_experiences": snap.total_experiences,
            "total_ci_diseases": snap.total_ci_diseases,
            "total_ci_services": snap.total_ci_services,
            "disease_count": snap.disease_count,
            "diseases": {
                name: {
                    "chunks": p.chunk_count,
                    "assertions": p.assertion_count,
                    "signs": p.sign_count,
                    "services": p.service_count,
                    "coverage": round(p.coverage_score, 2),
                }
                for name, p in snap.diseases.items()
            },
            "missing_indexes": snap.missing_indexes,
            "scan_time_ms": snap.scan_time_ms,
            "age_seconds": round(snap.age_seconds),
        }
