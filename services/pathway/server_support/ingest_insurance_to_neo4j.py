"""
Ingest insurance knowledge to Neo4j - Simple & Fast

Usage:
    cd server_support
    python ingest_insurance_to_neo4j.py
"""

import json
import os
from pathlib import Path

from neo4j import GraphDatabase

# Config
PROJECT_DIR = Path(__file__).parent.parent
INSURANCE_DIR = PROJECT_DIR / "workspaces" / "claims_insights" / "06_insurance"

NAMESPACE = "insurance_v1"


def get_driver():
    uri = os.getenv("NEO4J_URI", os.getenv("neo4j_uri", "bolt://localhost:7688"))
    user = os.getenv("NEO4J_USER", os.getenv("neo4j_user", "neo4j"))
    pw = os.getenv("NEO4J_PASSWORD", os.getenv("neo4j_password", "password123"))
    return GraphDatabase.driver(uri, auth=(user, pw))


def create_constraints(driver):
    """Create all constraints first"""
    constraints = [
        "CREATE CONSTRAINT IF NOT EXISTS FOR (n:Insurer) REQUIRE n.insurer_id IS UNIQUE",
        "CREATE CONSTRAINT IF NOT EXISTS FOR (n:Contract) REQUIRE n.contract_id IS UNIQUE",
        "CREATE CONSTRAINT IF NOT EXISTS FOR (n:Plan) REQUIRE n.plan_id IS UNIQUE",
        "CREATE CONSTRAINT IF NOT EXISTS FOR (n:Benefit) REQUIRE n.benefit_id IS UNIQUE",
        "CREATE CONSTRAINT IF NOT EXISTS FOR (n:Exclusion) REQUIRE n.exclusion_id IS UNIQUE",
        "CREATE CONSTRAINT IF NOT EXISTS FOR (n:ExclusionReason) REQUIRE n.reason_id IS UNIQUE",
        "CREATE CONSTRAINT IF NOT EXISTS FOR (n:Rulebook) REQUIRE n.rulebook_id IS UNIQUE",
    ]
    with driver.session() as session:
        for stmt in constraints:
            session.run(stmt)
    print(f"[OK] Created {len(constraints)} constraints")


def ingest_insurers_and_contracts(driver):
    """Ingest from contract_rules.json"""
    path = INSURANCE_DIR / "contract_rules.json"
    if not path.exists():
        print(f"[SKIP] {path.name} not found")
        return

    with open(path, encoding="utf-8") as f:
        data = json.load(f)

    contract_rules = data.get("contract_rules", [])
    insurers = set()

    with driver.session() as session:
        # Step 1: Create Contracts + Insurers
        for item in contract_rules:
            contract_id = item.get("contract_id")
            insurer_id = item.get("insurer")
            if not contract_id or not insurer_id:
                continue

            insurers.add(insurer_id)

            # Create Contract
            session.run("""
                MERGE (c:Contract {contract_id: $contract_id, namespace: $namespace})
                SET c.contract_name = $contract_name,
                    c.insurer_id = $insurer_id,
                    c.mode = $mode,
                    c.copay_percent = $copay_percent
            """, contract_id=contract_id, contract_name=item.get("product", ""),
                insurer_id=insurer_id, mode="multi_plan",
                copay_percent=item.get("copay_percent", 0),
                namespace=NAMESPACE)

        # Step 2: Create Insurers
        for insurer_id in insurers:
            names = {"FPT": "FPT Insurance", "PJICO": "Bảo hiểm PJICO",
                    "BHV": "Bảo hiểm Hỗn Nhất Việt", "TCGIns": "TCG Insurance",
                    "UIC": "UIC Insurance", "TIN": "TIN Insurance"}
            session.run("""
                MERGE (i:Insurer {insurer_id: $insurer_id, namespace: $namespace})
                SET i.insurer_name = $name
            """, insurer_id=insurer_id, name=names.get(insurer_id, insurer_id),
                namespace=NAMESPACE)

        # Step 3: Link Insurer -> Contract
        session.run("""
            MATCH (i:Insurer {namespace: $namespace})
            MATCH (c:Contract {namespace: $namespace})
            WHERE c.insurer_id = i.insurer_id
            MERGE (i)-[:ISSUES]->(c)
        """, namespace=NAMESPACE)

    print(f"[OK] Ingested {len(contract_rules)} contracts, {len(insurers)} insurers")


def ingest_benefits(driver):
    """Ingest from benefit_contract_knowledge_pack.json"""
    path = INSURANCE_DIR / "benefit_contract_knowledge_pack.json"
    if not path.exists():
        print(f"[SKIP] {path.name} not found")
        return

    with open(path, encoding="utf-8") as f:
        data = json.load(f)

    catalog = data.get("contract_catalog", {})
    contracts = catalog.get("contracts", [])
    benefit_count = 0

    with driver.session() as session:
        for contract in contracts:
            contract_id = contract.get("contract_id")
            benefits = contract.get("benefit_entries", [])

            for i, benefit in enumerate(benefits):
                benefit_id = f"BEN-{contract_id}-{i:03d}"
                benefit_name = benefit.get("entry_label", "")

                # Create Benefit
                session.run("""
                    MERGE (b:Benefit {benefit_id: $benefit_id, namespace: $namespace})
                    SET b.benefit_name = $benefit_name,
                        b.major_section = $major_section,
                        b.subsection = $subsection
                """, benefit_id=benefit_id, benefit_name=benefit_name,
                    major_section=benefit.get("major_section", ""),
                    subsection=benefit.get("subsection", ""),
                    namespace=NAMESPACE)

                # Link to Contract
                session.run("""
                    MATCH (c:Contract {contract_id: $contract_id, namespace: $namespace})
                    MATCH (b:Benefit {benefit_id: $benefit_id, namespace: $namespace})
                    MERGE (c)-[:COVERS]->(b)
                """, contract_id=contract_id, benefit_id=benefit_id, namespace=NAMESPACE)

                benefit_count += 1

    print(f"[OK] Ingested {benefit_count} benefits")


def ingest_exclusions(driver):
    """Ingest from exclusion_knowledge_pack.json"""
    path = INSURANCE_DIR / "exclusion_knowledge_pack.json"
    if not path.exists():
        print(f"[SKIP] {path.name} not found")
        return

    with open(path, encoding="utf-8") as f:
        data = json.load(f)

    # Get reason usage (list of items with atomic_reason)
    reason_usage = data.get("reason_usage", [])
    # Get contract data for exclusion groups
    main_summary = data.get("main_summary", {})
    reason_group_distribution = main_summary.get("reason_group_distribution", [])
    top_reasons_by_rows = main_summary.get("top_atomic_reasons_by_rows", [])

    # Build group name to ID mapping
    group_map = {}
    for item in reason_group_distribution:
        group_name = item.get("group", "Khác")
        # Simplify group name for ID
        group_id = group_name.replace(" ", "-").replace("/", "-")[:20].upper()
        group_map[group_name] = group_id

    with driver.session() as session:
        # Create Exclusion nodes (groups from distribution)
        for item in reason_group_distribution:
            group_name = item.get("group", "Khác")
            group_id = group_map.get(group_name, "MISC")
            exclusion_id = f"EXC-{group_id}"
            session.run("""
                MERGE (e:Exclusion {exclusion_id: $exclusion_id, namespace: $namespace})
                SET e.exclusion_name = $exclusion_name,
                    e.exclusion_group = $group_name,
                    e.row_count = $row_count
            """, exclusion_id=exclusion_id, exclusion_name=f"Loại trừ: {group_name}",
                group_name=group_name, row_count=item.get("rows", 0), namespace=NAMESPACE)

        # Create ExclusionReason nodes from top_reasons_by_rows
        reason_count = 0
        for item in top_reasons_by_rows:
            reason_text = item.get("reason", "")
            if not reason_text:
                continue

            # Generate reason_id from text (simplified)
            reason_id = reason_text[:15].strip().replace(" ", "-").upper()
            session.run("""
                MERGE (er:ExclusionReason {reason_id: $reason_id, namespace: $namespace})
                SET er.reason_text = $reason_text,
                    er.row_count = $row_count
            """, reason_id=reason_id, reason_text=reason_text,
                row_count=item.get("rows", 0), namespace=NAMESPACE)

            reason_count += 1

    print(f"[OK] Ingested {len(reason_group_distribution)} exclusion groups, {reason_count} reasons")


def ingest_rulebooks(driver):
    """Ingest from rulebook_policy_pack.json"""
    path = INSURANCE_DIR / "rulebook_policy_pack.json"
    if not path.exists():
        print(f"[SKIP] {path.name} not found")
        return

    with open(path, encoding="utf-8") as f:
        data = json.load(f)

    items = data.get("items", [])

    with driver.session() as session:
        for item in items:
            rulebook_id = item.get("rulebook_id", "")
            if not rulebook_id:
                continue

            insurer_id = item.get("insurer", "UNKNOWN")

            # Create Rulebook
            session.run("""
                MERGE (rb:Rulebook {rulebook_id: $rulebook_id, namespace: $namespace})
                SET rb.display_name = $display_name,
                    rb.rule_code = $rule_code,
                    rb.insurer_id = $insurer_id,
                    rb.page_count = $page_count,
                    rb.ocr_status = $ocr_status,
                    rb.source_file = $source_file
            """, rulebook_id=rulebook_id, display_name=item.get("display_name", ""),
                rule_code=item.get("rule_code", ""), insurer_id=insurer_id,
                page_count=item.get("page_count", 0), ocr_status=item.get("ocr_status", ""),
                source_file=item.get("source_file", ""),
                namespace=NAMESPACE)

            # Link to Insurer
            session.run("""
                MATCH (i:Insurer {insurer_id: $insurer_id, namespace: $namespace})
                MATCH (rb:Rulebook {rulebook_id: $rulebook_id, namespace: $namespace})
                MERGE (i)-[:PUBLISHES]->(rb)
            """, insurer_id=insurer_id, rulebook_id=rulebook_id, namespace=NAMESPACE)

    print(f"[OK] Ingested {len(items)} rulebooks")


def ingest_service_mappings(driver):
    """Ingest service mappings from jsonl files"""
    mapping_files = [
        INSURANCE_DIR / "benefit_detail_service_links.jsonl",
        INSURANCE_DIR / "exclusion_note_mentions_linked.jsonl",
        INSURANCE_DIR / "combined_exclusion_note_mentions_linked.jsonl",
    ]

    benefit_mappings = 0
    exclusion_mappings = 0
    unmapped_count = 0
    error_count = 0

    with driver.session() as session:
        for mapping_file in mapping_files:
            if not mapping_file.exists():
                print(f"  [SKIP] {mapping_file.name} not found")
                continue

            is_exclusion = "exclusion" in mapping_file.name.lower()
            print(f"  [PROCESSING] {mapping_file.name} (exclusion={is_exclusion})")

            with open(mapping_file, encoding="utf-8") as f:
                for line_num, line in enumerate(f, 1):
                    line = line.strip()
                    if not line:
                        continue

                    try:
                        record = json.loads(line)

                        if is_exclusion:
                            reason_id = record.get("reason_code", "")
                            service_code = record.get("service_code", "")
                            if not reason_id or not service_code:
                                unmapped_count += 1
                                continue

                            mapping_id = f"{reason_id}-{service_code}"
                            result = session.run("""
                                MATCH (er:ExclusionReason {reason_id: $reason_id, namespace: $namespace})
                                MERGE (m:ExclusionServiceMapping {mapping_id: $mapping_id, namespace: $namespace})
                                SET m.service_code = $service_code,
                                    m.service_name_raw = $service_name,
                                    m.claim_count = $count,
                                    m.gap_sum_vnd = $gap
                                MERGE (er)-[:EXCLUDES_SERVICE]->(m)
                                RETURN m.mapping_id
                            """, reason_id=reason_id, mapping_id=mapping_id,
                                service_code=service_code, service_name=record.get("service_name", ""),
                                count=record.get("claim_count", 0), gap=record.get("gap_sum_vnd", 0),
                                namespace=NAMESPACE)
                            if result.single():
                                exclusion_mappings += 1
                        else:
                            benefit_match = record.get("benefit_interpretation_match", {})
                            service_map = record.get("service_mapping", {})

                            benefit_id = benefit_match.get("entry_id", "")
                            service_code = service_map.get("service_code", "")
                            if not benefit_id or not service_code or service_map.get("mapping_status") == "unmapped":
                                unmapped_count += 1
                                continue

                            mapping_id = f"{benefit_id}-{service_code}"
                            result = session.run("""
                                MATCH (b:Benefit {benefit_id: $benefit_id, namespace: $namespace})
                                MERGE (m:BenefitServiceMapping {mapping_id: $mapping_id, namespace: $namespace})
                                SET m.service_code = $service_code,
                                    m.service_name_raw = $service_name,
                                    m.mapper_score = $score,
                                    m.mapper_confidence = $confidence
                                MERGE (b)-[:MAPS_TO_SERVICE]->(m)
                                RETURN m.mapping_id
                            """, benefit_id=benefit_id, mapping_id=mapping_id,
                                service_code=service_code, service_name=service_map.get("matched_variant", ""),
                                score=service_map.get("mapper_score", 0), confidence=service_map.get("mapper_confidence", ""),
                                namespace=NAMESPACE)
                            if result.single():
                                benefit_mappings += 1
                    except Exception as e:
                        if error_count < 5:
                            print(f"  [ERROR] Line {line_num}: {e}")
                        error_count += 1

    print(f"[OK] Ingested {benefit_mappings} benefit mappings, {exclusion_mappings} exclusion mappings ({unmapped_count} unmapped, {error_count} errors)")


def main():
    driver = get_driver()

    print("=" * 60)
    print("INGEST INSURANCE TO NEO4J")
    print("=" * 60)

    try:
        # Step 1: Constraints
        print("\n[1/5] Creating constraints...")
        create_constraints(driver)

        # Step 2: Insurers + Contracts
        print("\n[2/5] Ingesting insurers and contracts...")
        ingest_insurers_and_contracts(driver)

        # Step 3: Benefits
        print("\n[3/5] Ingesting benefits...")
        ingest_benefits(driver)

        # Step 4: Exclusions
        print("\n[4/5] Ingesting exclusions...")
        ingest_exclusions(driver)

        # Step 5: Rulebooks
        print("\n[5/5] Ingesting rulebooks...")
        ingest_rulebooks(driver)

        # Step 6: Service mappings
        print("\n[6/6] Ingesting service mappings...")
        ingest_service_mappings(driver)

        print("\n" + "=" * 60)
        print("DONE! Insurance data ingested to Neo4j")
        print("Namespace:", NAMESPACE)
        print("=" * 60)

    finally:
        driver.close()


if __name__ == "__main__":
    main()
