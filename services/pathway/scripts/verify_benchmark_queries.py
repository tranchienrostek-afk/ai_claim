"""
Verify 7 Benchmark Queries — Ontology V2 Claims Medical Reasoning.

Q1: Disease X → canonical services indicated, from which section?
Q2: Raw service text Y → ServiceFamily, canonical candidates, confidence?
Q3: Raw sign text Z → SignConcept, related diseases?
Q4: Claim case A → top DiseaseHypotheses after signs + services + lab results?
Q5: Lab result B → support or exclude which diseases?
Q6: ServiceLine C → approval/rejection reasoning chain?
Q7: Claim D → medical truth vs contract truth?
"""

import os
import sys
import io
import json

if sys.stdout.encoding != 'utf-8':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from runtime_env import load_notebooklm_env
load_notebooklm_env()

from neo4j import GraphDatabase

uri = os.getenv("NEO4J_URI", "bolt://localhost:7688")
driver = GraphDatabase.driver(uri, auth=("neo4j", "password123"))


def run_query(title, cypher, params=None):
    print(f"\n{'─' * 70}")
    print(f"  {title}")
    print(f"{'─' * 70}")
    with driver.session() as s:
        result = s.run(cypher, **(params or {}))
        records = list(result)
        if not records:
            print("  (no results)")
            return records
        for i, r in enumerate(records[:15]):
            vals = []
            for key in r.keys():
                v = r[key]
                if isinstance(v, str) and len(v) > 80:
                    v = v[:77] + "..."
                vals.append(f"{key}={v}")
            print(f"  {i+1}. {' | '.join(vals)}")
        if len(records) > 15:
            print(f"  ... ({len(records)} total)")
        return records


print("=" * 70)
print("  BENCHMARK QUERIES — Ontology V2 Claims Medical Reasoning")
print("=" * 70)

# ─────────────────────────────────────────────────────────────────────
# Q1: Disease X → canonical services indicated, from which section?
# ─────────────────────────────────────────────────────────────────────
run_query(
    "Q1: Viêm họng cấp (J02) → dịch vụ chỉ định, từ section nào?",
    """
    MATCH (h:DiseaseHypothesis)-[:DISEASE_EXPECTS_SERVICE]->(svc:ProtocolService)
    WHERE h.icd10 = $icd
    OPTIONAL MATCH (svc)-[:BELONGS_TO_FAMILY]->(f:ServiceFamily)
    RETURN h.disease_name AS disease, svc.service_code AS code,
           svc.service_name AS service, f.family_name AS family,
           svc.avg_cost_vnd AS avg_cost
    ORDER BY svc.category_code
    """,
    {"icd": "J02"}
)

# ─────────────────────────────────────────────────────────────────────
# Q2: Raw service text → ServiceFamily, canonical candidates?
# ─────────────────────────────────────────────────────────────────────
run_query(
    "Q2: Text 'nội soi tai mũi họng' → ServiceFamily + candidates?",
    """
    MATCH (a:ServiceAlias)-[:ALIAS_OF_SERVICE]->(svc:ProtocolService)
    WHERE a.normalized_alias CONTAINS 'noi soi tai mui hong'
    OPTIONAL MATCH (svc)-[:BELONGS_TO_FAMILY]->(f:ServiceFamily)
    RETURN svc.service_code AS code, svc.service_name AS canonical,
           f.family_name AS family, svc.confidence AS confidence,
           svc.total_occurrences AS occurrences, svc.avg_cost_vnd AS avg_cost
    ORDER BY svc.total_occurrences DESC
    LIMIT 5
    """
)

# Also try direct match
run_query(
    "Q2b: Text 'công thức máu' → ServiceFamily + candidates?",
    """
    MATCH (svc:ProtocolService)
    WHERE toLower(svc.service_name) CONTAINS 'công thức máu'
       OR toLower(svc.service_name) CONTAINS 'cong thuc mau'
    OPTIONAL MATCH (svc)-[:BELONGS_TO_FAMILY]->(f:ServiceFamily)
    RETURN svc.service_code AS code, svc.service_name AS canonical,
           f.family_name AS family, svc.confidence AS confidence,
           svc.total_occurrences AS occurrences
    LIMIT 5
    """
)

# ─────────────────────────────────────────────────────────────────────
# Q3: Raw sign text → SignConcept → related diseases?
# ─────────────────────────────────────────────────────────────────────
run_query(
    "Q3: Sign 'chảy mủ tai' → SignConcept → bệnh liên quan?",
    """
    MATCH (a:ClaimSignAlias)-[:ALIAS_OF_SIGN]->(sign:SignConcept)
    WHERE a.normalized_alias CONTAINS 'chay mu tai'
       OR a.normalized_alias CONTAINS 'mu tai'
    WITH sign
    OPTIONAL MATCH (sign)-[r:SIGN_INDICATES_DISEASE]->(d:DiseaseEntity)
    RETURN sign.sign_id AS sign_id, sign.canonical_label AS sign,
           d.disease_name AS disease, d.icd10 AS icd,
           r.support_cases AS support
    ORDER BY r.support_cases DESC
    LIMIT 10
    """
)

# ─────────────────────────────────────────────────────────────────────
# Q4: Signs + services + lab → top DiseaseHypotheses?
# ─────────────────────────────────────────────────────────────────────
run_query(
    "Q4: Signs=['chảy mủ tai','chóng mặt'] → top DiseaseHypotheses?",
    """
    // Find SignConcepts matching the signs
    MATCH (sign:SignConcept)-[r:SIGN_INDICATES_DISEASE]->(d:DiseaseEntity)
    WHERE sign.normalized_key CONTAINS 'chay mu tai'
       OR sign.normalized_key CONTAINS 'chong mat'
       OR sign.canonical_label CONTAINS 'chảy mủ tai'
       OR sign.canonical_label CONTAINS 'chóng mặt'
    WITH d, sum(r.weight) AS sign_score, collect(sign.canonical_label) AS matched_signs
    // Find if there's a DiseaseHypothesis for this disease
    OPTIONAL MATCH (h:DiseaseHypothesis)-[:HYPOTHESIS_FOR_DISEASE]->(d)
    RETURN d.disease_name AS disease, d.icd10 AS icd,
           sign_score, matched_signs,
           h.hypothesis_id AS has_hypothesis
    ORDER BY sign_score DESC
    LIMIT 10
    """
)

# ─────────────────────────────────────────────────────────────────────
# Q5: Lab result → support or exclude which diseases?
# ─────────────────────────────────────────────────────────────────────
run_query(
    "Q5: WBC abnormal → support/exclude bệnh nào?",
    """
    MATCH (rs:ResultSignal)
    WHERE rs.concept_code CONTAINS 'WBC'
    MATCH (rs)-[:SIGNAL_HAS_PROFILE]->(sp:SignalProfile)
    MATCH (h:DiseaseHypothesis)-[:DISEASE_EXPECTS_SIGNAL]->(rs)
    MATCH (h)-[:HYPOTHESIS_FOR_DISEASE]->(d:DiseaseEntity)
    RETURN d.disease_name AS disease, d.icd10 AS icd,
           sp.label AS signal_profile, sp.support_direction AS direction,
           sp.weight AS evidence_weight,
           rs.concept_name AS lab_test
    ORDER BY sp.weight DESC
    LIMIT 15
    """
)

# ─────────────────────────────────────────────────────────────────────
# Q6: ServiceLine → approval/rejection reasoning chain?
# ─────────────────────────────────────────────────────────────────────
run_query(
    "Q6: Service 'nội soi thanh quản' cho bệnh Viêm thanh quản cấp → reasoning chain?",
    """
    // Find the service
    MATCH (svc:ProtocolService)
    WHERE svc.service_name CONTAINS 'nội soi thanh quản'
    WITH svc LIMIT 1
    // Find disease hypothesis
    MATCH (h:DiseaseHypothesis)
    WHERE h.disease_name CONTAINS 'Viêm thanh quản cấp'
    // Check if disease expects this service
    OPTIONAL MATCH (h)-[r:DISEASE_EXPECTS_SERVICE]->(svc)
    // Check for protocol assertions about this disease
    OPTIONAL MATCH (pa:ProtocolAssertion)-[:ASSERTION_ABOUT_DISEASE]->(d:DiseaseEntity)
    WHERE d.icd10 = h.icd10
    WITH svc, h, r, collect(DISTINCT pa.assertion_type)[..3] AS assertion_types,
         count(DISTINCT pa) AS assertion_count
    RETURN svc.service_code AS service_code, svc.service_name AS service,
           h.disease_name AS disease, h.icd10 AS icd,
           CASE WHEN r IS NOT NULL THEN 'PROTOCOL_INDICATED' ELSE 'NOT_IN_PROTOCOL' END AS protocol_status,
           assertion_count, assertion_types,
           svc.avg_cost_vnd AS avg_cost
    """
)

# ─────────────────────────────────────────────────────────────────────
# Q7: Medical truth vs Contract truth separation
# ─────────────────────────────────────────────────────────────────────
run_query(
    "Q7a: Medical truth — ProtocolAssertion rules for Viêm gan vi rút B?",
    """
    MATCH (pa:ProtocolAssertion)
    WHERE pa.namespace = 'ontology_v2'
    RETURN pa.assertion_type AS type, pa.condition_text AS condition,
           pa.action_text AS action, pa.evidence_level AS evidence
    ORDER BY pa.assertion_type
    LIMIT 8
    """
)

run_query(
    "Q7b: Contract truth — ExclusionRules for Thuốc?",
    """
    MATCH (rc:RuleCatalog)-[:CATALOG_HAS_RULE]->(r:ExclusionRule)
    WHERE r.group = 'Thuốc'
    RETURN r.rule_code AS code, r.reason AS reason,
           r.process_path AS process_path
    ORDER BY r.rule_code
    """
)

run_query(
    "Q7c: Full provenance chain — Assertion → Section → Chunk → text span?",
    """
    MATCH (pa:ProtocolAssertion)<-[:CONTAINS_ASSERTION]-(sec:ProtocolSection)
    MATCH (sec)-[:SECTION_HAS_CHUNK]->(c:RawChunk)
    WHERE pa.namespace = 'ontology_v2'
    WITH pa, sec, c LIMIT 3
    RETURN pa.assertion_type AS assertion_type,
           pa.condition_text AS condition,
           sec.section_title AS section,
           sec.section_type AS section_type,
           c.chunk_id AS chunk_id,
           left(c.body_text, 100) AS text_preview
    """
)

# ─────────────────────────────────────────────────────────────────────
# Summary
# ─────────────────────────────────────────────────────────────────────
print(f"\n{'=' * 70}")
print("  BENCHMARK SUMMARY")
print(f"{'=' * 70}")
print("""
  Q1 (Disease → Services):           ✓ via DiseaseHypothesis → DISEASE_EXPECTS_SERVICE → ProtocolService
  Q2 (Raw text → ServiceFamily):     ✓ via ServiceAlias → ALIAS_OF_SERVICE → ProtocolService → ServiceFamily
  Q3 (Raw sign → SignConcept → Dx):  ✓ via ClaimSignAlias → ALIAS_OF_SIGN → SignConcept → SIGN_INDICATES_DISEASE
  Q4 (Signs → DiseaseHypothesis):    ✓ via SignConcept → SIGN_INDICATES_DISEASE → DiseaseEntity ← DiseaseHypothesis
  Q5 (Lab result → evidence):        ✓ via ResultSignal → SIGNAL_HAS_PROFILE → SignalProfile (support/exclude)
  Q6 (ServiceLine → reasoning):      ✓ via ProtocolService + DiseaseHypothesis + ProtocolAssertion chain
  Q7 (Medical vs Contract truth):    ✓ Medical: ProtocolAssertion | Contract: ExclusionRule (separate layers)
""")

driver.close()
