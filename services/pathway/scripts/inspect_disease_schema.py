import os
from neo4j import GraphDatabase

def inspect_schema():
    uri = os.getenv("NEO4J_URI", "bolt://localhost:7688")
    user = os.getenv("NEO4J_USER", "neo4j")
    pw = os.getenv("NEO4J_PASSWORD", "password123")
    
    driver = GraphDatabase.driver(uri, auth=(user, pw))
    
    with driver.session() as session:
        print("Inspecting DiseaseEntity schema...")
        result = session.run("MATCH (n:DiseaseEntity) RETURN keys(n) as keys, n LIMIT 5")
        records = list(result)
        
        if not records:
            print("No DiseaseEntity nodes found in the database-wide query.")
            # Check labels available
            labels = session.run("CALL db.labels()")
            print(f"Available labels: {[r['label'] for r in labels]}")
        else:
            for r in records:
                print(f"\nKeys: {r['keys']}")
                print(f"Values: {r['n']}")

    driver.close()

if __name__ == "__main__":
    inspect_schema()
