from neo4j import GraphDatabase
import os
from dotenv import load_dotenv

load_dotenv()

class SemanticVerifier:
    def __init__(self):
        self.uri = os.getenv("neo4j_uri", "bolt://localhost:7688")
        self.user = os.getenv("neo4j_user", "neo4j")
        self.password = os.getenv("neo4j_password", "password123")
        self.driver = GraphDatabase.driver(self.uri, auth=(self.user, self.password))

    def verify(self):
        with self.driver.session() as session:
            # 1. Blocks mapping to Ontology Concepts
            print("\n--- Block to Ontology Concepts ---")
            results = session.run("""
                MATCH (b:Block)-[:REFERS_TO_CONCEPT]->(c)
                RETURN b.title as Block, c.code as Code, c.name as conceptName LIMIT 10
            """).values()
            for r in results: print(f"Block: {r[0]} | Code: {r[1]} ({r[2]})")

            # 2. Entities mapping to Ontology Instances
            print("\n--- Entities mapping to Concepts ---")
            results = session.run("""
                MATCH (e:Entity)-[:INSTANCE_OF]->(c)
                RETURN e.name as Entity, e.type as Type, c.code as Code LIMIT 10
            """).values()
            for r in results: print(f"Entity: {r[0]} ({r[1]}) | Concept Code: {r[2]}")

    def close(self):
        self.driver.close()

if __name__ == "__main__":
    v = SemanticVerifier()
    v.verify()
    v.close()
