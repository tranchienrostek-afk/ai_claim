"""
AdaptiveSearchPlanner — Progressive refinement search strategy.

Inspired by Claude Code's "broad → narrow → read" pattern:
  Glob 100 files → Grep pattern → Read specific lines

Pathway equivalent:
  Round 1: Inventory check → do I have data?
  Round 2: Unified retrieve → fan out to all relevant layers
  Round 3: Evaluate coverage → are results sufficient?
  Round 4: Refine if needed → sub-queries, entity decomposition, cross-disease
  Round 5: Honest fallback → "không đủ data" instead of hallucinate

Key difference from old plan_and_search():
  - Old: 1 round, fixed strategy, no self-evaluation
  - New: Multi-round, adapts based on what it finds, honest about gaps
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any

from server_support.knowledge_inventory import KnowledgeInventory
from server_support.unified_retriever import UnifiedRetriever, RetrievalResult


@dataclass
class SearchPlan:
    """What the planner decided to do and why."""
    disease_name: str | None = None
    intent: str = "general"
    entities: list[dict] = field(default_factory=list)
    available_layers: list[str] = field(default_factory=list)
    coverage_score: float = 0.0
    has_disease_data: bool = False
    strategy: str = "full"  # full | partial | claims_only | no_data
    rounds_executed: int = 0
    refinements: list[str] = field(default_factory=list)


@dataclass
class SearchResult:
    """Complete result from adaptive search."""
    results: list[RetrievalResult]
    plan: SearchPlan
    trace: list[dict]
    total_ms: int = 0
    sufficient: bool = False  # did we find enough quality results?
    coverage_gaps: list[str] = field(default_factory=list)  # what data is missing

    def to_context_list(self) -> list[dict]:
        """Convert to medical_agent-compatible context list."""
        return [r.to_context_dict() for r in self.results]


# Minimum results to consider search "sufficient" per intent
SUFFICIENCY_THRESHOLDS = {
    "lookup": 2,
    "general": 3,
    "dosage": 2,
    "procedure": 3,
    "diagnosis": 4,
    "contraindication": 2,
    "compare": 3,
}


class AdaptiveSearchPlanner:
    """Multi-round search with progressive refinement."""

    def __init__(self, inventory: KnowledgeInventory, retriever: UnifiedRetriever,
                 embedding_fn=None, chat_fn=None):
        """
        Args:
            inventory: KnowledgeInventory instance
            retriever: UnifiedRetriever instance
            embedding_fn: text → embedding vector
            chat_fn: callable(messages) → response text, for query decomposition
        """
        self.inventory = inventory
        self.retriever = retriever
        self.embedding_fn = embedding_fn
        self.chat_fn = chat_fn

    def search(self, query: str, intent: str = "general",
               disease_name: str = None, entities: list[dict] = None,
               top_k: int = 12, max_rounds: int = 3) -> SearchResult:
        """
        Adaptive multi-round search.

        Round 1: Inventory check + initial retrieve
        Round 2: Evaluate sufficiency → refine if needed
        Round 3: Final attempt with decomposed sub-queries
        """
        t0 = time.time()
        entities = entities or []
        trace = []
        plan = SearchPlan(
            disease_name=disease_name,
            intent=intent,
            entities=entities,
        )

        # ============================================================
        # ROUND 0: Inventory check — "biết mình biết gì"
        # ============================================================
        t1 = time.time()
        avail = self.inventory.check_availability(disease_name=disease_name)
        plan.has_disease_data = avail["has_data"]
        plan.available_layers = avail["available_layers"]
        plan.coverage_score = avail["coverage_score"]

        if not avail["has_data"] and disease_name:
            plan.strategy = avail["recommendation"]  # "no_disease_data" or "claims_only_no_protocol"
        elif avail["coverage_score"] < 0.4:
            plan.strategy = "partial"
        else:
            plan.strategy = "full"

        trace.append({
            "round": 0,
            "step": "inventory_check",
            "has_data": avail["has_data"],
            "coverage": avail["coverage_score"],
            "strategy": plan.strategy,
            "available_layers": avail["available_layers"],
            "ms": int((time.time() - t1) * 1000),
        })

        # Early exit: no data at all
        if plan.strategy == "no_disease_data" and not entities:
            plan.rounds_executed = 0
            return SearchResult(
                results=[],
                plan=plan,
                trace=trace,
                total_ms=int((time.time() - t0) * 1000),
                sufficient=False,
                coverage_gaps=[f"Không có data về '{disease_name}' trong knowledge graph"],
            )

        # ============================================================
        # ROUND 1: Unified retrieve — fan out to all layers
        # ============================================================
        t1 = time.time()
        results, unified_trace = self.retriever.retrieve(
            query=query,
            intent=intent,
            disease_name=disease_name,
            entities=entities,
            top_k=top_k,
        )
        plan.rounds_executed = 1

        trace.append({
            "round": 1,
            "step": "unified_retrieve",
            "result_count": len(results),
            "layers": unified_trace.get("layers", []),
            "ms": int((time.time() - t1) * 1000),
        })

        # ============================================================
        # EVALUATE: Is this sufficient?
        # ============================================================
        threshold = SUFFICIENCY_THRESHOLDS.get(intent, 3)
        high_quality = [r for r in results if r.score > 0.01]  # RRF scores are small
        is_sufficient = len(high_quality) >= threshold

        # Check layer coverage — which layers returned nothing?
        coverage_gaps = []
        for layer_info in unified_trace.get("layers", []):
            if layer_info["count"] == 0 and layer_info["layer"] in (
                "assertion", "sign_mentions", "service_mentions"
            ):
                coverage_gaps.append(f"Không tìm thấy {layer_info['layer']}")

        if is_sufficient and len(coverage_gaps) <= 1:
            return SearchResult(
                results=results[:top_k],
                plan=plan,
                trace=trace,
                total_ms=int((time.time() - t0) * 1000),
                sufficient=True,
                coverage_gaps=coverage_gaps,
            )

        if max_rounds < 2:
            return SearchResult(
                results=results[:top_k],
                plan=plan,
                trace=trace,
                total_ms=int((time.time() - t0) * 1000),
                sufficient=is_sufficient,
                coverage_gaps=coverage_gaps,
            )

        # ============================================================
        # ROUND 2: Refinement — try different strategies
        # ============================================================
        t1 = time.time()
        refinement_results = []
        refinement_strategies = []

        # Strategy A: Entity decomposition — search each entity separately
        if entities and len(results) < threshold:
            for ent in entities[:3]:
                ent_name = ent.get("name", "")
                ent_type = ent.get("type", "")
                if ent_type in ("Symptom", "Sign"):
                    extra = self.retriever.search_sign_mentions(ent_name, top_k=3)
                elif ent_type in ("Drug", "Procedure"):
                    extra = self.retriever.search_service_mentions(ent_name, top_k=3)
                    extra += self.retriever.search_assertions(
                        ent_name, assertion_type="dosage_rule" if intent == "dosage" else None,
                        top_k=3)
                else:
                    extra = self.retriever.search_assertions(ent_name, top_k=3)
                refinement_results.extend(extra)
            refinement_strategies.append("entity_decomposition")

        # Strategy B: Cross-disease search — if disease-specific returned little
        if disease_name and len(results) < threshold:
            cross = self.retriever.search_assertions(query, top_k=5)  # no disease filter
            refinement_results.extend(cross)
            refinement_strategies.append("cross_disease_assertions")

        # Strategy C: Claims insights augmentation
        if disease_name and "claims_insights" not in [r.source_layer for r in results]:
            claims = self.retriever.search_claims_insights(disease_name=disease_name, top_k=3)
            refinement_results.extend(claims)
            refinement_strategies.append("claims_augmentation")

        # Merge refinement results, dedup
        seen_ids = {r.id for r in results}
        added = 0
        for r in refinement_results:
            if r.id not in seen_ids:
                results.append(r)
                seen_ids.add(r.id)
                added += 1

        plan.rounds_executed = 2
        plan.refinements = refinement_strategies

        trace.append({
            "round": 2,
            "step": "refinement",
            "strategies": refinement_strategies,
            "added": added,
            "total_results": len(results),
            "ms": int((time.time() - t1) * 1000),
        })

        # Re-evaluate sufficiency
        high_quality = [r for r in results if r.score > 0.01]
        is_sufficient = len(high_quality) >= threshold

        if is_sufficient or max_rounds < 3:
            return SearchResult(
                results=results[:top_k],
                plan=plan,
                trace=trace,
                total_ms=int((time.time() - t0) * 1000),
                sufficient=is_sufficient,
                coverage_gaps=coverage_gaps,
            )

        # ============================================================
        # ROUND 3: Sub-query decomposition via LLM
        # ============================================================
        if self.chat_fn and len(results) < threshold:
            t1 = time.time()
            try:
                sub_queries = self._decompose_query(query, intent, disease_name)
                sub_added = 0
                for sq in sub_queries[:3]:
                    sq_results, _ = self.retriever.retrieve(
                        query=sq, intent=intent,
                        disease_name=disease_name,
                        top_k=5
                    )
                    for r in sq_results:
                        if r.id not in seen_ids:
                            results.append(r)
                            seen_ids.add(r.id)
                            sub_added += 1

                plan.rounds_executed = 3
                plan.refinements.append("llm_sub_query_decomposition")

                trace.append({
                    "round": 3,
                    "step": "sub_query_decomposition",
                    "sub_queries": sub_queries,
                    "added": sub_added,
                    "total_results": len(results),
                    "ms": int((time.time() - t1) * 1000),
                })
            except Exception as e:
                trace.append({
                    "round": 3,
                    "step": "sub_query_decomposition",
                    "error": str(e),
                    "ms": int((time.time() - t1) * 1000),
                })

        high_quality = [r for r in results if r.score > 0.01]
        is_sufficient = len(high_quality) >= threshold

        return SearchResult(
            results=results[:top_k],
            plan=plan,
            trace=trace,
            total_ms=int((time.time() - t0) * 1000),
            sufficient=is_sufficient,
            coverage_gaps=coverage_gaps,
        )

    def _decompose_query(self, query: str, intent: str, disease_name: str = None) -> list[str]:
        """Use LLM to break a complex query into simpler sub-queries."""
        if not self.chat_fn:
            return []

        prompt = f"""Bạn là query decomposer cho hệ thống y khoa. Tách câu hỏi phức tạp thành 2-3 câu hỏi đơn giản hơn để tìm kiếm riêng biệt.

Câu hỏi gốc: {query}
Intent: {intent}
{"Bệnh: " + disease_name if disease_name else ""}

Trả về JSON: {{"sub_queries": ["câu 1", "câu 2", "câu 3"]}}
Mỗi câu hỏi phụ nên tập trung vào 1 khía cạnh cụ thể."""

        try:
            import json
            response = self.chat_fn([{"role": "user", "content": prompt}])
            data = json.loads(response)
            return data.get("sub_queries", [])[:3]
        except Exception:
            return []
