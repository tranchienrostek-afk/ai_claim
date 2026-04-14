"""
Insurance Query Service - Contract Compliance Helper

Quick queries for Problem 2: Contract Compliance
"""

import os
from typing import Any

from neo4j import GraphDatabase


NAMESPACE = "insurance_v1"


def get_driver():
    uri = os.getenv("NEO4J_URI", os.getenv("neo4j_uri", "bolt://localhost:7688"))
    user = os.getenv("NEO4J_USER", os.getenv("neo4j_user", "neo4j"))
    pw = os.getenv("NEO4J_PASSWORD", os.getenv("neo4j_password", "password123"))
    return GraphDatabase.driver(uri, auth=(user, pw))


def check_benefit_coverage(contract_id: str, service_name: str) -> dict[str, Any]:
    """
    Check if a service is covered by a contract's benefits.

    Returns coverage decision and evidence.
    """
    driver = get_driver()
    with driver.session() as session:
        # Find matching benefits by full-text search
        result = session.run("""
            MATCH (c:Contract {contract_id: $contract_id, namespace: $namespace})
            MATCH (c)-[:COVERS]->(b:Benefit {namespace: $namespace})
            WHERE b.benefit_name CONTAINS $service_name
               OR b.major_section CONTAINS $service_name
               OR b.subsection CONTAINS $service_name
            RETURN b.benefit_id, b.benefit_name, b.major_section
            LIMIT 5
        """, contract_id=contract_id, service_name=service_name, namespace=NAMESPACE)

        benefits = [dict(row) for row in result]

        return {
            "contract_id": contract_id,
            "service_name": service_name,
            "covered": len(benefits) > 0,
            "matching_benefits": benefits,
            "decision": "APPROVE" if benefits else "REVIEW"
        }


def check_exclusion(contract_id: str, service_name: str) -> dict[str, Any]:
    """
    Check if a service is excluded by any exclusion reason.

    Returns list of applicable exclusion reasons.
    """
    driver = get_driver()
    with driver.session() as session:
        # Find exclusion reasons that might apply
        result = session.run("""
            MATCH (c:Contract {contract_id: $contract_id, namespace: $namespace})
            MATCH (c)-[:APPLIES_EXCLUSION]->(e:Exclusion {namespace: $namespace})
            MATCH (e)-[:HAS_REASON]->(er:ExclusionReason {namespace: $namespace})
            WHERE er.reason_text CONTAINS $service_name
            RETURN er.reason_id, er.reason_text, e.exclusion_name, e.exclusion_group
            LIMIT 10
        """, contract_id=contract_id, service_name=service_name, namespace=NAMESPACE)

        exclusions = [dict(row) for row in result]

        return {
            "contract_id": contract_id,
            "service_name": service_name,
            "excluded": len(exclusions) > 0,
            "exclusion_reasons": exclusions,
            "decision": "REJECT" if exclusions else "REVIEW"
        }


def get_contract_summary(contract_id: str) -> dict[str, Any]:
    """
    Get full summary of a contract.
    """
    driver = get_driver()
    with driver.session() as session:
        # Contract info
        contract = session.run("""
            MATCH (c:Contract {contract_id: $contract_id, namespace: $namespace})
            RETURN c.contract_id, c.contract_name, c.insurer_id, c.mode
        """, contract_id=contract_id, namespace=NAMESPACE).single()

        if not contract:
            return {"error": "Contract not found"}

        # Benefits count
        benefit_count = session.run("""
            MATCH (c:Contract {contract_id: $contract_id, namespace: $namespace})
            MATCH (c)-[:COVERS]->(b:Benefit {namespace: $namespace})
            RETURN count(b) AS cnt
        """, contract_id=contract_id, namespace=NAMESPACE).single()["cnt"]

        # Exclusions
        exclusions = session.run("""
            MATCH (c:Contract {contract_id: $contract_id, namespace: $namespace})
            MATCH (c)-[:APPLIES_EXCLUSION]->(e:Exclusion {namespace: $namespace})
            MATCH (e)-[:HAS_REASON]->(er:ExclusionReason {namespace: $namespace})
            RETURN e.exclusion_name, count(er) AS reason_count
        """, contract_id=contract_id, namespace=NAMESPACE)

        exclusion_list = [{"group": row["e.exclusion_name"], "reason_count": row["reason_count"]}
                        for row in exclusions]

        # Rulebooks
        rulebooks = session.run("""
            MATCH (c:Contract {contract_id: $contract_id, namespace: $namespace})
            MATCH (c)-[:REFERENCES]->(rb:Rulebook {namespace: $namespace})
            RETURN rb.rulebook_id, rb.display_name, rb.ocr_status
        """, contract_id=contract_id, namespace=NAMESPACE)

        rulebook_list = [dict(row) for row in rulebooks]

        return {
            "contract_id": contract["c.contract_id"],
            "contract_name": contract["c.contract_name"],
            "insurer_id": contract["c.insurer_id"],
            "mode": contract["c.mode"],
            "benefit_count": benefit_count,
            "exclusions": exclusion_list,
            "rulebooks": rulebook_list
        }


def query_all_contracts() -> list[dict[str, Any]]:
    """Get list of all contracts."""
    driver = get_driver()
    with driver.session() as session:
        result = session.run("""
            MATCH (c:Contract {namespace: $namespace})
            OPTIONAL MATCH (c)-[:COVERS]->(b:Benefit {namespace: $namespace})
            WITH c, count(b) AS benefit_count
            RETURN c.contract_id, c.contract_name, c.insurer_id, benefit_count
            ORDER BY c.contract_id
        """, namespace=NAMESPACE)

        return [dict(row) for row in result]


if __name__ == "__main__":
    import sys
    import io

    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

    print("=== INSURANCE QUERY SERVICE ===")

    # List all contracts
    contracts = query_all_contracts()
    print(f"\nFound {len(contracts)} contracts:")
    for c in contracts:
        print(f"  {c['contract_id']}: {c['contract_name']} ({c['benefit_count']} benefits)")

    # Test queries
    if contracts:
        test_contract = contracts[0]["contract_id"]
        print(f"\n=== Testing with {test_contract} ===")

        summary = get_contract_summary(test_contract)
        print(f"\nContract Summary:")
        print(f"  Name: {summary['contract_name']}")
        print(f"  Benefits: {summary['benefit_count']}")
        print(f"  Exclusion groups: {len(summary['exclusions'])}")

        # Test benefit check
        benefit_result = check_benefit_coverage(test_contract, "khám")
        print(f"\nBenefit check for 'khám':")
        print(f"  Covered: {benefit_result['covered']}")
        for b in benefit_result.get("matching_benefits", []):
            print(f"    - {b['benefit_id']}: {b['benefit_name']}")

        # Test exclusion check
        exclusion_result = check_exclusion(test_contract, "cận lâm sàng")
        print(f"\nExclusion check for 'cận lâm sàng':")
        print(f"  Excluded: {exclusion_result['excluded']}")
        for e in exclusion_result.get("exclusion_reasons", []):
            print(f"    - {e['reason_id']}: {e['reason_text'][:50]}...")
