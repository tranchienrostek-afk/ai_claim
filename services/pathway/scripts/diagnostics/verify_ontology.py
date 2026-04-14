import os
from neo4j import GraphDatabase
from dotenv import load_dotenv

load_dotenv()

uri = os.getenv("neo4j_uri", "bolt://localhost:7688")
user = os.getenv("neo4j_user", "neo4j")
password = os.getenv("neo4j_password", "password123")
driver = GraphDatabase.driver(uri, auth=(user, password))

def verify_ontology():
    with driver.session() as session:
        print("--- Ontology Verification ---")
        
        # 1. Check Classes
        classes = session.run("MATCH (c:OntologyClass) RETURN c.label as label, c.uri as uri").values()
        print(f"Ontology Classes found: {[c[0] for c in classes]}")
        
        # 2. Check Instance Mappings
        instances = session.run("""
            MATCH (e:Entity)-[:INSTANCE_OF]->(c:OntologyClass)
            RETURN c.label as class, count(e) as count
        """)
        print("\nEntity Mappings by Class:")
        for record in instances:
            print(f" - {record['class']}: {record['count']} entities")
            
        # 3. Check Semantic Mappings (MAPS_TO)
        print("\nSemantic Mappings (Protocol -> Entity -> Ontology):")
        results = session.run("""
            MATCH (p:Protocol)-[:HAS_BLOCK]->(b:Block)-[:REFERENCES]->(e:Entity)-[:MAPS_TO]->(c)
            RETURN p.name as protocol, b.title as block, e.name as entity, labels(c)[0] as concept_type, c.name as concept_name
            LIMIT 10
        """)
        for record in results:
            print(f" - [{record['protocol']}] Block: '{record['block']}' references '{record['entity']}' which maps to {record['concept_type']}: '{record['concept_name']}'")
            
        # 4. Check Inference Path (In-view of Rule R_DIAZEPAM_G47)
        print("\nChecking Inference Path for Rule R_DIAZEPAM_G47:")
        inference = session.run("""
            MATCH (r:ClinicalRule {rule_id: 'R_DIAZEPAM_G47'})-[:REQUIRES_CONDITION]->(cond:Condition {type: 'HAS_DIAGNOSIS'})
            MATCH (benh:ICD_Category {code: cond.value})
            MATCH (thuoc:DrugConcept)-[:TREATS]->(benh)
            RETURN r.name as rule, benh.name as target_disease, thuoc.name as drug
        """)
        for record in inference:
            print(f" - SUCCESS: Rule '{record['rule']}' targets disease '{record['target_disease']}' which is treated by '{record['drug']}'")

if __name__ == "__main__":
    try:
        verify_ontology()
    finally:
        driver.close()
