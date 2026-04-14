"""
Migration script: Add source_type and hospital_name to existing data.

Tags all existing Chunk and Protocol nodes as BYT (Bộ Y tế) and creates
new indexes for hospital-specific protocol filtering.

Idempotent — safe to run multiple times.

Usage:
    cd notebooklm
    python scripts/migrations/migrate_add_source_type.py
"""

import os
from dotenv import load_dotenv
from neo4j import GraphDatabase

load_dotenv()


def migrate():
    uri = os.getenv("NEO4J_URI", os.getenv("neo4j_uri", "bolt://localhost:7688"))
    user = os.getenv("NEO4J_USER", os.getenv("neo4j_user", "neo4j"))
    password = os.getenv("NEO4J_PASSWORD", os.getenv("neo4j_password", "password123"))

    driver = GraphDatabase.driver(uri, auth=(user, password))

    with driver.session() as session:
        # Tag existing Chunks as BYT
        result = session.run(
            "MATCH (c:Chunk) WHERE c.source_type IS NULL "
            "SET c.source_type = 'BYT' RETURN count(c) AS updated"
        )
        chunk_count = result.single()["updated"]
        print(f"[OK] Tagged {chunk_count} Chunk nodes with source_type='BYT'")

        # Tag existing Protocols as BYT
        result = session.run(
            "MATCH (p:Protocol) WHERE p.source_type IS NULL "
            "SET p.source_type = 'BYT' RETURN count(p) AS updated"
        )
        proto_count = result.single()["updated"]
        print(f"[OK] Tagged {proto_count} Protocol nodes with source_type='BYT'")

        # Create composite indexes for hospital-filtered search
        session.run(
            "CREATE INDEX chunk_source_idx IF NOT EXISTS "
            "FOR (n:Chunk) ON (n.disease_name, n.source_type)"
        )
        print("[OK] Index chunk_source_idx created or exists")

        session.run(
            "CREATE INDEX chunk_hospital_idx IF NOT EXISTS "
            "FOR (n:Chunk) ON (n.disease_name, n.hospital_name)"
        )
        print("[OK] Index chunk_hospital_idx created or exists")

        session.run(
            "CREATE INDEX hospital_name_idx IF NOT EXISTS "
            "FOR (n:Hospital) ON (n.name)"
        )
        print("[OK] Index hospital_name_idx created or exists")

    # Verify
    with driver.session() as session:
        result = session.run(
            "MATCH (c:Chunk) WHERE c.source_type IS NULL RETURN count(c) AS remaining"
        )
        remaining = result.single()["remaining"]
        print(f"\nVerification: {remaining} Chunk nodes still without source_type (should be 0)")

    driver.close()
    print("\nMigration complete.")


if __name__ == "__main__":
    migrate()
