from neo4j import GraphDatabase
import os
from dotenv import load_dotenv

load_dotenv()

class OntologyExplorer:
    def __init__(self):
        self.uri = os.getenv("neo4j_uri", "bolt://localhost:7688")
        self.user = os.getenv("neo4j_user", "neo4j")
        self.password = os.getenv("neo4j_password", "password123")
        self.driver = GraphDatabase.driver(self.uri, auth=(self.user, self.password))

    def close(self):
        self.driver.close()

    def get_sleep_ontology(self):
        query = """
        // Get the ICD hierarchy around G47
        MATCH (root:ICD_Block {code: 'G40-G47'})
        OPTIONAL MATCH path = (root)<-[:IS_A*]-(child)
        WITH root, collect(path) as paths
        
        // Get related drugs
        OPTIONAL MATCH (d:DrugConcept)-[:TREATS]->(target:ICD_Category {code: 'G47'})
        OPTIONAL MATCH (d)-[:IS_A*]->(atc:ATC_Level1)
        
        RETURN root, paths, collect(DISTINCT {drug: d.name, atc: atc.name}) as drugs
        """
        with self.driver.session() as session:
            result = session.run(query)
            return result.single()

    def generate_mermaid(self):
        # More comprehensive query to get all links
        query = """
        MATCH (n) WHERE (n:ICD_Chapter OR n:ICD_Block OR n:ICD_Category OR n:DrugConcept OR n:OntologyClass)
        MATCH (n)-[r:IS_A|INSTANCE_OF|TREATS]->(m)
        WHERE (n.code STARTS WITH 'G4' OR n.code STARTS WITH 'N' OR n.uri STARTS WITH 'onto')
        RETURN n.name as start_node, type(r) as rel, m.name as end_node, labels(n)[0] as start_label, labels(m)[0] as end_label
        """
        with self.driver.session() as session:
            records = session.run(query)
            mermaid = "mermaid\ngraph TD\n"
            for record in records:
                s = record['start_node'] or "Unnamed"
                e = record['end_node'] or "Unnamed"
                r = record['rel']
                mermaid += f'    "{s}" -- {r} --> "{e}"\n'
            return mermaid

if __name__ == "__main__":
    explorer = OntologyExplorer()
    try:
        print(explorer.generate_mermaid())
    finally:
        explorer.close()
