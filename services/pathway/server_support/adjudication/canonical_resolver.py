"""Canonical Service Resolver — resolves service references to CanonicalService (BYT MAANHXA).

3-tier resolution:
  1. MAANHXA exact: claim has MAANHXA → direct lookup (conf=1.0)
  2. CIService bridge: codebook service_code → pre-computed MAPS_TO_CANONICAL → CanonicalService
  3. Fuzzy fallback: text → existing ServiceTextMapper (unchanged)

This module enriches the service_info dict returned by AdjudicationMVP.recognize_service()
with canonical data (BYT price, taxonomy, MAANHXA) when available.
"""

from __future__ import annotations

import logging
import os
from functools import lru_cache
from typing import Any

from neo4j import GraphDatabase

logger = logging.getLogger(__name__)

NEO4J_URI = os.getenv("NEO4J_URI", os.getenv("neo4j_uri", "bolt://localhost:7688"))
NEO4J_AUTH = (
    os.getenv("NEO4J_USER", os.getenv("neo4j_user", "neo4j")),
    os.getenv("NEO4J_PASSWORD", os.getenv("neo4j_password", "password123")),
)


@lru_cache(maxsize=1)
def _get_driver():
    try:
        driver = GraphDatabase.driver(NEO4J_URI, auth=NEO4J_AUTH)
        driver.verify_connectivity()
        return driver
    except Exception as e:
        logger.warning("CanonicalResolver: Neo4j unavailable (%s), canonical enrichment disabled", e)
        return None


@lru_cache(maxsize=1)
def _load_bridge_cache() -> dict[str, dict]:
    """Pre-load CIService → CanonicalService bridge from Neo4j into memory."""
    driver = _get_driver()
    if not driver:
        return {}

    cache = {}
    try:
        with driver.session() as s:
            result = s.run("""
                MATCH (ci:CIService)-[r:MAPS_TO_CANONICAL]->(cs:CanonicalService)
                OPTIONAL MATCH (cs)-[:HAS_PRICE_VARIANT]->(pv:PriceVariant)
                OPTIONAL MATCH (cs)-[:CLASSIFIED_AS]->(sc:ServiceClassification)
                WITH ci, cs, r,
                     collect(DISTINCT pv.gia)[0] AS byt_price,
                     collect(DISTINCT pv.giasau)[0] AS byt_price_after,
                     collect(DISTINCT sc.name)[0] AS classification
                RETURN ci.service_code AS service_code,
                       cs.maanhxa AS maanhxa,
                       cs.canonical_name_primary AS canonical_name_byt,
                       r.confidence AS bridge_confidence,
                       r.method AS bridge_method,
                       byt_price, byt_price_after, classification
            """)
            for r in result:
                cache[r["service_code"]] = {
                    "maanhxa": r["maanhxa"],
                    "canonical_name_byt": r["canonical_name_byt"],
                    "bridge_confidence": r["bridge_confidence"],
                    "bridge_method": r["bridge_method"],
                    "byt_price": r["byt_price"],
                    "byt_price_after": r["byt_price_after"],
                    "classification": r["classification"],
                }
    except Exception as e:
        logger.warning("CanonicalResolver: failed to load bridge cache: %s", e)

    logger.info("CanonicalResolver: loaded %d bridge entries", len(cache))
    return cache


@lru_cache(maxsize=1)
def _load_maanhxa_cache() -> dict[str, dict]:
    """Pre-load CanonicalService by MAANHXA for direct lookups."""
    driver = _get_driver()
    if not driver:
        return {}

    cache = {}
    try:
        with driver.session() as s:
            result = s.run("""
                MATCH (cs:CanonicalService)
                OPTIONAL MATCH (cs)-[:HAS_PRICE_VARIANT]->(pv:PriceVariant)
                OPTIONAL MATCH (cs)-[:CLASSIFIED_AS]->(sc:ServiceClassification)
                WITH cs,
                     collect(DISTINCT pv.gia)[0] AS byt_price,
                     collect(DISTINCT pv.giasau)[0] AS byt_price_after,
                     collect(DISTINCT sc.name)[0] AS classification
                RETURN cs.maanhxa AS maanhxa,
                       cs.canonical_name_primary AS canonical_name_byt,
                       byt_price, byt_price_after, classification
            """)
            for r in result:
                cache[r["maanhxa"]] = {
                    "canonical_name_byt": r["canonical_name_byt"],
                    "byt_price": r["byt_price"],
                    "byt_price_after": r["byt_price_after"],
                    "classification": r["classification"],
                }
    except Exception as e:
        logger.warning("CanonicalResolver: failed to load MAANHXA cache: %s", e)

    logger.info("CanonicalResolver: loaded %d MAANHXA entries", len(cache))
    return cache


def resolve_and_enrich(
    service_info: dict[str, Any],
    maanhxa_from_claim: str | None = None,
) -> dict[str, Any]:
    """Enrich service_info with canonical data.

    Args:
        service_info: Result from AdjudicationMVP.recognize_service()
        maanhxa_from_claim: MAANHXA code from hospital HIS (if available)

    Returns:
        Same dict with added canonical_* fields. Original fields preserved.
    """
    result = dict(service_info)

    # ── Tầng 1: Direct MAANHXA from claim ──
    if maanhxa_from_claim:
        maanhxa_cache = _load_maanhxa_cache()
        canonical = maanhxa_cache.get(maanhxa_from_claim)
        if canonical:
            result["canonical_maanhxa"] = maanhxa_from_claim
            result["canonical_name_byt"] = canonical["canonical_name_byt"]
            result["canonical_byt_price"] = canonical["byt_price"]
            result["canonical_byt_price_after"] = canonical["byt_price_after"]
            result["canonical_classification"] = canonical["classification"]
            result["canonical_resolve_method"] = "maanhxa_direct"
            result["canonical_resolve_confidence"] = 1.0
            # Override bhyt_price if not set
            if not result.get("bhyt_price") and canonical["byt_price"]:
                result["bhyt_price"] = canonical["byt_price"]
            return result

    # ── Tầng 2: CIService bridge ──
    service_code = service_info.get("service_code", "")
    if service_code:
        bridge_cache = _load_bridge_cache()
        bridge = bridge_cache.get(service_code)
        if bridge:
            result["canonical_maanhxa"] = bridge["maanhxa"]
            result["canonical_name_byt"] = bridge["canonical_name_byt"]
            result["canonical_byt_price"] = bridge["byt_price"]
            result["canonical_byt_price_after"] = bridge["byt_price_after"]
            result["canonical_classification"] = bridge["classification"]
            result["canonical_resolve_method"] = f"bridge_{bridge['bridge_method']}"
            result["canonical_resolve_confidence"] = bridge["bridge_confidence"]
            # Override bhyt_price if not set
            if not result.get("bhyt_price") and bridge["byt_price"]:
                result["bhyt_price"] = bridge["byt_price"]
            return result

    # ── Tầng 3: No canonical match ──
    result["canonical_maanhxa"] = None
    result["canonical_resolve_method"] = "none"
    result["canonical_resolve_confidence"] = 0.0
    return result


def close():
    """Close Neo4j driver."""
    driver = _get_driver()
    if driver:
        driver.close()
