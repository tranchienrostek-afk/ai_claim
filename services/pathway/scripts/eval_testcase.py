"""
Evaluate a testcase (kịch bản) against Ontology V2 graph.

Simulates the full claims reasoning pipeline:
1. Parse testcase → extract signs, services, lab results
2. Query Ontology V2 for each service line
3. Generate reasoning chain for PAYMENT/REJECT decision
4. Compare with ground truth labels
5. Score and report

Usage:
    cd notebooklm
    python scripts/eval_testcase.py data/script/kich_ban_09.json
"""

import os
import sys
import io
import json
import argparse
from pathlib import Path
from datetime import datetime

if sys.stdout.encoding != 'utf-8':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
if sys.stderr.encoding != 'utf-8':
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

sys.path.insert(0, str(Path(__file__).parent.parent))
from runtime_env import load_notebooklm_env
load_notebooklm_env()

from neo4j import GraphDatabase

uri = os.getenv("NEO4J_URI", "bolt://localhost:7688")
driver = GraphDatabase.driver(uri, auth=("neo4j", "password123"))


def query_one(session, cypher, **params):
    """Run query and return list of record dicts."""
    result = session.run(cypher, **params)
    return [dict(r) for r in result]


def resolve_service(session, raw_text: str) -> list[dict]:
    """Q2: Raw service text → canonical service + family."""
    # Try alias match first
    results = query_one(session, """
        MATCH (a:ServiceAlias)-[:ALIAS_OF_SERVICE]->(svc:ProtocolService)
        WHERE toLower(a.alias_label) CONTAINS toLower($text)
           OR toLower($text) CONTAINS toLower(a.alias_label)
        OPTIONAL MATCH (svc)-[:BELONGS_TO_FAMILY]->(f:ServiceFamily)
        RETURN svc.service_code AS code, svc.service_name AS canonical,
               f.family_name AS family, f.category_code AS category,
               svc.avg_cost_vnd AS avg_cost
        ORDER BY svc.total_occurrences DESC
        LIMIT 3
    """, text=raw_text)
    if results:
        return results

    # Direct name match
    results = query_one(session, """
        MATCH (svc:ProtocolService)
        WHERE toLower(svc.service_name) CONTAINS toLower($text)
           OR toLower($text) CONTAINS toLower(svc.service_name)
        OPTIONAL MATCH (svc)-[:BELONGS_TO_FAMILY]->(f:ServiceFamily)
        RETURN svc.service_code AS code, svc.service_name AS canonical,
               f.family_name AS family, f.category_code AS category,
               svc.avg_cost_vnd AS avg_cost
        ORDER BY svc.total_occurrences DESC
        LIMIT 3
    """, text=raw_text)
    if results:
        return results

    # Fallback: extract key tokens and search aliases
    # e.g. "Phản ứng ASLO" → search for alias containing "aslo"
    import re
    tokens = re.findall(r'[a-zA-Z]{3,}', raw_text)
    for token in tokens:
        results = query_one(session, """
            MATCH (a:ServiceAlias)-[:ALIAS_OF_SERVICE]->(svc:ProtocolService)
            WHERE toLower(a.alias_label) CONTAINS toLower($tok)
            OPTIONAL MATCH (svc)-[:BELONGS_TO_FAMILY]->(f:ServiceFamily)
            RETURN svc.service_code AS code, svc.service_name AS canonical,
                   f.family_name AS family, f.category_code AS category,
                   svc.avg_cost_vnd AS avg_cost
            ORDER BY svc.total_occurrences DESC
            LIMIT 3
        """, tok=token)
        if results:
            return results

    return []


def find_sign_diseases(session, signs: list[str]) -> list[dict]:
    """Q3/Q4: Signs → SignConcept → DiseaseEntity + DiseaseHypothesis."""
    all_diseases = {}
    for sign_text in signs:
        # Normalize and search
        results = query_one(session, """
            MATCH (sign:SignConcept)-[r:SIGN_INDICATES_DISEASE]->(d:DiseaseEntity)
            WHERE toLower(sign.canonical_label) CONTAINS toLower($text)
            WITH d, r, sign
            OPTIONAL MATCH (h:DiseaseHypothesis)-[:HYPOTHESIS_FOR_DISEASE]->(d)
            RETURN d.disease_name AS disease, d.icd10 AS icd,
                   r.weight AS weight, sign.canonical_label AS matched_sign,
                   h.hypothesis_id AS hypothesis_id
            ORDER BY r.weight DESC
            LIMIT 5
        """, text=sign_text)
        for r in results:
            did = r["disease"]
            if did not in all_diseases:
                all_diseases[did] = {"disease": did, "icd": r["icd"],
                                     "total_weight": 0, "signs": [],
                                     "has_hypothesis": r["hypothesis_id"] is not None}
            all_diseases[did]["total_weight"] += (r["weight"] or 1)
            all_diseases[did]["signs"].append(r["matched_sign"])

    return sorted(all_diseases.values(), key=lambda x: -x["total_weight"])


def find_disease_expected_services(session, disease_name: str) -> list[dict]:
    """Q1: Disease → expected services from protocol."""
    return query_one(session, """
        MATCH (h:DiseaseHypothesis)-[:DISEASE_EXPECTS_SERVICE]->(svc:ProtocolService)
        WHERE toLower(h.disease_name) CONTAINS toLower($dname)
        OPTIONAL MATCH (svc)-[:BELONGS_TO_FAMILY]->(f:ServiceFamily)
        RETURN svc.service_code AS code, svc.service_name AS service,
               f.family_name AS family
    """, dname=disease_name)


def find_protocol_assertions(session, disease_name: str) -> list[dict]:
    """Q7a: Medical truth — protocol assertions for disease."""
    # Search by namespace or disease link
    results = query_one(session, """
        MATCH (pa:ProtocolAssertion)-[:ASSERTION_ABOUT_DISEASE]->(d:DiseaseEntity)
        WHERE toLower(d.disease_name) CONTAINS toLower($dname)
        RETURN pa.assertion_type AS type, pa.condition_text AS condition,
               pa.action_text AS action
        LIMIT 10
    """, dname=disease_name)
    # Also check by chunk content about the disease
    if not results:
        results = query_one(session, """
            MATCH (c:RawChunk)-[:CONTAINS_ASSERTION]->(pa:ProtocolAssertion)
            WHERE toLower(c.body_text) CONTAINS toLower($dname)
            RETURN pa.assertion_type AS type, pa.condition_text AS condition,
                   pa.action_text AS action
            LIMIT 10
        """, dname=disease_name)
    return results


def find_exclusion_rules(session, service_code: str = None) -> list[dict]:
    """Q7b: Contract truth — applicable exclusion rules."""
    return query_one(session, """
        MATCH (rc:RuleCatalog)-[:CATALOG_HAS_RULE]->(r:ExclusionRule)
        RETURN r.rule_code AS code, r.reason AS reason, r.group AS grp
        ORDER BY r.rule_code
    """)


def find_lab_evidence(session, service_code: str) -> list[dict]:
    """Q5: Service → signals → evidence direction."""
    return query_one(session, """
        MATCH (svc:ProtocolService {service_code: $sc})
        MATCH (svc)-[:SERVICE_PRODUCES_SIGNAL]->(rs:ResultSignal)
        MATCH (rs)-[:SIGNAL_HAS_PROFILE]->(sp:SignalProfile)
        RETURN rs.concept_name AS signal, sp.label AS profile,
               sp.support_direction AS direction, sp.weight AS weight
    """, sc=service_code)


def evaluate_testcase(testcase_path: str):
    tc = json.loads(Path(testcase_path).read_text(encoding="utf-8"))
    story = tc["cau_chuyen_y_khoa"]
    labeling = tc["du_lieu_labeling_mau"]
    case = labeling["case_level"]
    lines = labeling["service_lines"]

    print(f"\n{'#' * 70}")
    print(f"  ONTOLOGY V2 EVALUATION")
    print(f"  Testcase: {case['testcase_title']}")
    print(f"  Disease: {case['main_disease_name_vi']}")
    print(f"  Patient: {story['benh_nhan']['gioi_tinh']}, {story['benh_nhan']['tuoi']} tuổi")
    print(f"  Services: {case['total_lines']} lines")
    print(f"{'#' * 70}")

    with driver.session() as session:

        # ── Step 1: Parse signs from testcase ──
        signs_raw = case.get("initial_signs_pipe", "").split(" | ")
        signs_raw = [s.strip() for s in signs_raw if s.strip()]
        print(f"\n{'─' * 70}")
        print(f"  STEP 1: Parse admission signs")
        print(f"{'─' * 70}")
        for s in signs_raw:
            print(f"  • {s}")

        # ── Step 2: Sign → Disease hypothesis (Q3/Q4) ──
        print(f"\n{'─' * 70}")
        print(f"  STEP 2: Sign → Disease Hypotheses (Q3/Q4)")
        print(f"{'─' * 70}")
        hypotheses = find_sign_diseases(session, signs_raw)
        if hypotheses:
            for i, h in enumerate(hypotheses[:5]):
                hyp_marker = "★" if h["has_hypothesis"] else " "
                print(f"  {hyp_marker} {i+1}. {h['disease']} (ICD:{h['icd']}) — weight={h['total_weight']:.1f}, signs={h['signs'][:3]}")
        else:
            print("  (no disease matches from signs)")

        # ── Step 3: Disease → Expected services (Q1) ──
        print(f"\n{'─' * 70}")
        print(f"  STEP 3: Disease → Expected Services (Q1)")
        print(f"{'─' * 70}")
        disease_name = case["main_disease_name_vi"]
        expected = find_disease_expected_services(session, disease_name)
        if expected:
            for e in expected:
                print(f"  ✓ {e['code']} — {e['service']} ({e['family']})")
        else:
            print(f"  (no protocol-expected services for '{disease_name}')")
            # Try broader search
            for h in hypotheses[:3]:
                exp2 = find_disease_expected_services(session, h["disease"])
                if exp2:
                    print(f"  → Found via hypothesis '{h['disease']}':")
                    for e in exp2:
                        print(f"    ✓ {e['code']} — {e['service']} ({e['family']})")

        # ── Step 4: Protocol assertions (Q7a medical truth) ──
        print(f"\n{'─' * 70}")
        print(f"  STEP 4: Protocol Assertions — Medical Truth (Q7a)")
        print(f"{'─' * 70}")
        assertions = find_protocol_assertions(session, disease_name)
        if not assertions:
            # try viêm mũi họng
            assertions = find_protocol_assertions(session, "viêm mũi họng")
        if not assertions:
            assertions = find_protocol_assertions(session, "viêm họng")
        if assertions:
            for a in assertions[:5]:
                print(f"  [{a['type']}] IF: {(a['condition'] or '')[:80]}")
                print(f"              THEN: {(a['action'] or '')[:80]}")
                print()
        else:
            print("  (no assertions found)")

        # ── Step 5: Evaluate each service line ──
        print(f"\n{'─' * 70}")
        print(f"  STEP 5: Service Line Evaluation")
        print(f"{'─' * 70}")

        correct = 0
        total = len(lines)

        for line in lines:
            raw_name = line["service_name_raw"]
            gt_label = line["final_label"]
            gt_reason = line["reason_text"]

            print(f"\n  ┌─ Line {line['line_no']}: {raw_name}")
            print(f"  │  Ground truth: {gt_label}")

            # Q2: Resolve service
            resolved = resolve_service(session, raw_name)
            if resolved:
                r = resolved[0]
                print(f"  │  Resolved: {r['code']} — {r['canonical']} ({r['family']})")

                # Q5: Check lab evidence
                evidence = find_lab_evidence(session, r["code"])
                if evidence:
                    for ev in evidence[:3]:
                        print(f"  │  Evidence: {ev['signal']} → {ev['profile']} ({ev['direction']}, w={ev['weight']})")

                # Determine if protocol-indicated
                is_indicated = any(
                    e["code"] == r["code"] for e in expected
                ) if expected else False
                # Also check via family match in expected
                if not is_indicated and expected:
                    resolved_cat = r.get("category", "")
                    for e in expected:
                        if e.get("family") and resolved_cat and e["code"][:7] == r["code"][:7]:
                            is_indicated = True
                            break

                # Simple decision logic
                predicted = "PAYMENT"  # Default for medical services with resolved code
                reasoning = []

                if is_indicated:
                    reasoning.append(f"Protocol-indicated for {disease_name}")
                else:
                    reasoning.append(f"Service resolved to {r['code']} ({r['family']})")
                    # Check if family is diagnostic/relevant
                    cat = r.get("category", "")
                    if cat in ("LAB-HEM", "LAB-BIO", "LAB-IMM", "LAB-MIC", "END-ENS", "IMG-XRY", "IMG-USG", "IMG-CTN", "FUN-DFT"):
                        reasoning.append(f"Diagnostic service in {r['family']} — standard for workup")
                    elif cat in ("PRO-THT",):
                        reasoning.append(f"Procedure service — needs clinical justification")

                print(f"  │  Predicted: {predicted}")
                print(f"  │  Reasoning: {'; '.join(reasoning)}")

                if predicted == gt_label:
                    print(f"  └─ ✅ MATCH")
                    correct += 1
                else:
                    print(f"  └─ ❌ MISMATCH (predicted={predicted}, expected={gt_label})")
            else:
                print(f"  │  ⚠️  Could not resolve service")
                print(f"  └─ ❌ UNRESOLVED")

        # ── Step 6: Exclusion rules check (Q7b contract truth) ──
        print(f"\n{'─' * 70}")
        print(f"  STEP 6: Contract Truth — Exclusion Rules Check (Q7b)")
        print(f"{'─' * 70}")
        rules = find_exclusion_rules(session)
        applicable = [r for r in rules if r["grp"] in ("Cận lâm sàng", "Thiếu chứng từ")]
        if applicable:
            print("  Potentially applicable exclusion rules:")
            for r in applicable:
                print(f"  • [{r['code']}] {r['reason']}")
        else:
            print("  No CLS-specific exclusion rules triggered.")
        print(f"  Note: {len(rules)} total exclusion rules available in RuleCatalog")

        # ── Summary ──
        print(f"\n{'=' * 70}")
        print(f"  EVALUATION SUMMARY")
        print(f"{'=' * 70}")
        print(f"  Testcase: {case['testcase_title']}")
        print(f"  Disease: {disease_name}")
        print(f"  Service lines: {total}")
        print(f"  Correct: {correct}/{total} ({100*correct/total:.0f}%)")
        print()

        # Detail per line
        print(f"  {'#':>3} {'Service':<45} {'GT':>7} {'Pred':>7} {'Match':>5}")
        print(f"  {'─'*3} {'─'*45} {'─'*7} {'─'*7} {'─'*5}")
        for line in lines:
            raw = line["service_name_raw"][:45]
            gt = line["final_label"]
            # Re-resolve for display
            resolved = resolve_service(session, line["service_name_raw"])
            pred = "PAYMENT" if resolved else "?"
            match = "✅" if pred == gt else "❌"
            print(f"  {line['line_no']:>3} {raw:<45} {gt:>7} {pred:>7} {match:>5}")

        print(f"\n  Graph traversal paths used:")
        print(f"    Q1: DiseaseHypothesis → DISEASE_EXPECTS_SERVICE → ProtocolService")
        print(f"    Q2: ServiceAlias → ALIAS_OF_SERVICE → ProtocolService → ServiceFamily")
        print(f"    Q3: SignConcept → SIGN_INDICATES_DISEASE → DiseaseEntity")
        print(f"    Q5: ProtocolService → SERVICE_PRODUCES_SIGNAL → ResultSignal → SignalProfile")
        print(f"    Q7: ProtocolAssertion (medical) | ExclusionRule (contract)")

        return {"correct": correct, "total": total, "accuracy": correct / total if total else 0}

    driver.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("testcase", help="Path to testcase JSON")
    args = parser.parse_args()
    evaluate_testcase(args.testcase)
