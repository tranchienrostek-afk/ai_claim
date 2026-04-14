from neo4j import GraphDatabase
import os
from dotenv import load_dotenv

load_dotenv()

class NeoVerifier:
    def __init__(self):
        self.uri = os.getenv("neo4j_uri", "bolt://localhost:7688")
        self.user = os.getenv("neo4j_user", "neo4j")
        self.password = os.getenv("neo4j_password", "password123")
        self.driver = GraphDatabase.driver(self.uri, auth=(self.user, self.password))

    def check_ontology(self):
        with self.driver.session() as session:
            # Check for OntologyClass
            classes = session.run("MATCH (n:OntologyClass) RETURN n.label as label").values()
            print(f"Ontology Classes: {[c[0] for c in classes]}")
            
            # Check for ICD nodes
            icd = session.run("MATCH (n:ICD_Category) RETURN n.code as code, n.name as name LIMIT 5").values()
            print(f"ICD Categories Sample: {icd}")
            
            # Check for ATC nodes
            atc = session.run("MATCH (n:ATC_Level1) RETURN n.code as code, n.name as name LIMIT 5").values()
            print(f"ATC Level 1 Sample: {atc}")

    def close(self):
        self.driver.close()

if __name__ == "__main__":
    v = NeoVerifier()
    v.check_ontology()
    v.close()
