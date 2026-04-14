import os
from neo4j import GraphDatabase

def check_respiratory_diseases():
    uri = os.getenv("NEO4J_URI", "bolt://localhost:7688")
    user = os.getenv("NEO4J_USER", "neo4j")
    pw = os.getenv("NEO4J_PASSWORD", "password123")
    
    driver = GraphDatabase.driver(uri, auth=(user, pw))
    diseases = ["J00", "J20", "J02", "J01", "J03", "J06", "J30", "J18", "J04", "J45"]
    
    with driver.session() as session:
        print("Checking for respiratory diseases in Neo4j...")
        # Check by ICD prefix or name
        query = """
        MATCH (n:DiseaseEntity)
        WHERE any(prefix IN $prefixes WHERE n.icd10 STARTS WITH prefix)
        OR any(name_part IN $name_parts WHERE n.disease_name CONTAINS name_part)
        RETURN n.icd10 as icd, n.disease_name as name, labels(n) as labels
        """
        
        # Also check RawChunks just in case
        chunk_query = """
        MATCH (n:RawChunk)
        WHERE any(prefix IN $prefixes WHERE n.disease_name STARTS WITH prefix)
        RETURN n.disease_name as name, count(n) as chunks
        """
        
        name_parts = ["Viêm mũi họng", "Viêm phế quản", "Viêm họng", "Viêm mũi xoang", "Viêm amidan", 
                      "Nhiễm trùng hô hấp", "Viêm mũi dị ứng", "Viêm phổi", "Viêm thanh quản", "Hen phế quản"]
        
        res = session.run(query, prefixes=diseases, name_parts=name_parts)
        found_entities = list(res)
        
        res_chunks = session.run(chunk_query, prefixes=diseases)
        found_chunks = {r["name"]: r["chunks"] for r in res_chunks}
        
        print(f"\nFound {len(found_entities)} DiseaseEntity nodes matching respiratory master list:")
        for r in found_entities:
            chunk_count = found_chunks.get(r["name"], 0)
            print(f"  [{r['icd']}] {r['name']} — Chunks: {chunk_count}")
            
        # Summary by missing
        found_icds = {r["icd"][:3] for r in found_entities}
        missing = [d for d in diseases if d not in found_icds]
        print(f"\nMISSING from Master List: {', '.join(missing)}")

    driver.close()

if __name__ == "__main__":
    check_respiratory_diseases()
