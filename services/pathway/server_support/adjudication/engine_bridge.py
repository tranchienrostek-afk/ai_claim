"""Bridge layer to import offline pipeline engines into the live API runtime.

The offline adjudication engines live in workspaces/claims_insights/pipeline/
and workspaces/claims_insights/02_standardize/. This module adds those
directories to sys.path and provides cached singleton accessors so the
engines are loaded once and reused across requests.
"""

from __future__ import annotations

import sys
from functools import lru_cache
from pathlib import Path

# Resolve workspace paths relative to this file:
# this file:      server_support/adjudication/engine_bridge.py
# notebooklm:     ../../
# claims root:    ../../workspaces/claims_insights/
_NOTEBOOKLM_DIR = Path(__file__).resolve().parent.parent.parent
_CLAIMS_ROOT = _NOTEBOOKLM_DIR / "workspaces" / "claims_insights"
_PIPELINE_DIR = _CLAIMS_ROOT / "pipeline"
_STANDARDIZE_DIR = _CLAIMS_ROOT / "02_standardize"
_ENRICH_DIR = _CLAIMS_ROOT / "03_enrich"
_INSURANCE_DIR = _CLAIMS_ROOT / "06_insurance"


def _ensure_importable() -> None:
    """Add pipeline and dependency directories to sys.path (idempotent)."""
    for directory in (_PIPELINE_DIR, _STANDARDIZE_DIR):
        path_str = str(directory)
        if path_str not in sys.path:
            sys.path.insert(0, path_str)


# ---------------------------------------------------------------------------
# Singleton accessors for offline engines
# ---------------------------------------------------------------------------

@lru_cache(maxsize=1)
def get_adjudication_mvp():
    """Return a shared AdjudicationMVP instance (service recognition + clinical necessity)."""
    _ensure_importable()
    from adjudication_mvp import AdjudicationMVP  # type: ignore[import-not-found]
    return AdjudicationMVP()


@lru_cache(maxsize=1)
def get_disease_hypothesis_engine():
    """Return a shared DiseaseHypothesisEngine instance for ontology-first case reasoning."""
    _ensure_importable()
    from disease_hypothesis_engine import DiseaseHypothesisEngine  # type: ignore[import-not-found]
    return DiseaseHypothesisEngine()


@lru_cache(maxsize=1)
def get_contract_clause_engine():
    """Return a shared ContractClauseStep2 instance (contract rule evaluation)."""
    _ensure_importable()
    from contract_clause_step2 import ContractClauseStep2  # type: ignore[import-not-found]
    return ContractClauseStep2(
        contract_rules_path=_INSURANCE_DIR / "contract_rules.json",
        clause_service_catalog_path=_INSURANCE_DIR / "contract_clause_service_catalog.json",
        benefit_pack_path=_INSURANCE_DIR / "benefit_contract_knowledge_pack.json",
        exclusion_pack_path=_INSURANCE_DIR / "exclusion_knowledge_pack.json",
    )


@lru_cache(maxsize=1)
def get_neo4j_contract_agent():
    """Return a shared Neo4j-based contract agent (Neo4j-powered evaluation)."""
    from contract_agent_neo4j import get_neo4j_contract_agent
    return get_neo4j_contract_agent()


def close_all_agents():
    """Close all agent connections (Neo4j, etc.)."""
    from contract_agent_neo4j import close_neo4j_contract_agent
    close_neo4j_contract_agent()


# Expose workspace paths for agents that need data file access
CLAIMS_ROOT = _CLAIMS_ROOT
INSURANCE_DIR = _INSURANCE_DIR
ENRICH_DIR = _ENRICH_DIR
PIPELINE_DIR = _PIPELINE_DIR
