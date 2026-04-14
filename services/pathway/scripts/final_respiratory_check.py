import os
from neo4j import GraphDatabase

def check_respiratory_by_name():
    uri = os.getenv("NEO4J_URI", "bolt://localhost:7688")
    user = os.getenv("NEO4J_USER", "neo4j")
    pw = os.getenv("NEO4J_PASSWORD", "password123")
    
    driver = GraphDatabase.driver(uri, auth=(user, pw))
    name_parts = [
        "Viêm mũi họng", "Viêm phế quản", "Viêm họng", "Viêm mũi xoang", "Viêm amidan",
        "Nhiễm trùng hô hấp", "Viêm mũi dị ứng", "Viêm phổi", "Viêm thanh quản", "Hen phế quản"
    ]
    
    with driver.session() as session:
        print("Searching for respiratory diseases by name in Neo4j...")
        # Check by name containment
        query = """
        MATCH (n:DiseaseEntity)
        WHERE any(part IN $parts WHERE n.disease_name CONTAINS part)
        RETURN n.disease_name as name, n.disease_id as id
        """
        
        # Also check with normalized case-insensitive search if possible
        # or simplified match
        
        res = session.run(query, parts=name_parts)
        found = list(res)
        
        found_names = [r["name"] for r in found]
        print(f"\nFound {len(found_names)} matches:")
        for name in found_names:
            print(f"- {name}")
            
        not_found = [p for p in name_parts if not any(p.lower() in f.lower() for f in found_names)]
        print(f"\nPotential gapping in names: {not_found}")

    driver.close()

if __name__ == "__main__":
    check_respiratory_by_name()
