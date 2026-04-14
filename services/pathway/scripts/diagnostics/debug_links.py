from neo4j import GraphDatabase
import os
from dotenv import load_dotenv

load_dotenv()

def debug_ontology_links():
    uri = "bolt://localhost:7688"
    driver = GraphDatabase.driver(uri, auth=("neo4j", "password123"))
    
    with driver.session() as session:
        # Check links
        print("Checking REFERS_TO_CONCEPT links...")
        query = """
        MATCH (b:Block)-[:REFERS_TO_CONCEPT]->(c)<-[:REFERS_TO_CONCEPT]-(other)
        RETURN labels(b) as b_labels, labels(c) as c_labels, labels(other) as other_labels, 
               b.title as b_title, other.title as other_title, keys(other) as other_keys
        LIMIT 5
        """
        results = session.run(query).values()
        for r in results:
            print(f"B({r[0]}): {r[3]} -> Concept({r[1]}) <- Other({r[2]}): {r[4]} (Keys: {r[5]})")
            
    driver.close()

if __name__ == "__main__":
    debug_ontology_links()
