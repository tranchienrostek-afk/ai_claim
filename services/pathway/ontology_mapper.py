import os
from neo4j import GraphDatabase
from dotenv import load_dotenv

load_dotenv()

class OntologyMapper:
    def __init__(self):
        self.uri = os.getenv("neo4j_uri", "bolt://localhost:7688")
        self.user = os.getenv("neo4j_user", "neo4j")
        self.password = os.getenv("neo4j_password", "password123")
        self.driver = GraphDatabase.driver(self.uri, auth=(self.user, self.password))

    def close(self):
        self.driver.close()

    def map_entities(self):
        with self.driver.session() as session:
            print("Mapping entities to Ontology Classes...")

            # Map by type
            mappings = [
                ("Disease", "onto:Disease"),
                ("Symptom", "onto:Symptom"),
                ("Med", "onto:Drug"),
                ("Herb", "onto:ActiveIngredient"),
                ("AcupuncturePoint", "onto:AcupuncturePoint"),
                ("Formula", "onto:HerbalFormula"),
                ("Syndrome", "onto:TCMSyndrome"),
                ("TreatmentMethod", "onto:TreatmentMethod"),
            ]

            for ent_type, onto_uri in mappings:
                session.run("""
                    MATCH (e:Entity {type: $etype}), (c:OntologyClass {uri: $uri})
                    MERGE (e)-[:INSTANCE_OF]->(c)
                """, etype=ent_type, uri=onto_uri)

            # Specific mappings for Insomnia Protocol
            print("Running specific domain mappings...")

            # Map Diazepam Entity to DrugConcept
            session.run("""
                MATCH (e:Entity), (d:DrugConcept {name: 'Diazepam'})
                WHERE e.name CONTAINS 'Diazepam'
                MERGE (e)-[:MAPS_TO]->(d)
            """)

            # Map Insomnia-related entities to ICD G47 and F51
            session.run("""
                MATCH (e:Entity), (i:ICD_Category {code: 'G47'})
                WHERE e.name CONTAINS 'Mất ngủ' OR e.name CONTAINS 'Rối loạn giấc ngủ' OR e.name CONTAINS 'Thất miên'
                   OR e.name CONTAINS 'mất ngủ' OR e.name CONTAINS 'thất miên' OR e.name CONTAINS 'Bất mị'
                MERGE (e)-[:MAPS_TO]->(i)
            """)
            session.run("""
                MATCH (e:Entity), (i:ICD_Category {code: 'F51'})
                WHERE e.name CONTAINS 'Mất ngủ' OR e.name CONTAINS 'Rối loạn giấc ngủ' OR e.name CONTAINS 'Thất miên'
                   OR e.name CONTAINS 'mất ngủ' OR e.name CONTAINS 'thất miên' OR e.name CONTAINS 'Bất mị'
                MERGE (e)-[:MAPS_TO]->(i)
            """)

            # Map Formula entities to FormulaConcept nodes via Vietnamese synonym keywords
            formula_keywords = {
                "YHCT_F01": ["Quy tỳ", "quy tỳ", "Qui tỳ"],
                "YHCT_F02": ["Hoàng liên a giao", "hoàng liên a giao"],
                "YHCT_F03": ["An thần định chí", "an thần định chí"],
                "YHCT_F04": ["Ôn đởm", "ôn đởm"],
                "YHCT_F05": ["Long đởm tả can", "long đởm tả can"],
                "YHCT_F06": ["Thiên vương bổ tâm", "thiên vương bổ tâm"],
                "YHCT_F07": ["Toan táo nhân", "toan táo nhân"],
                "YHCT_F08": ["Chu sa an thần", "chu sa an thần"],
                "YHCT_F09": ["Gia vị tiêu dao", "gia vị tiêu dao", "Tiêu dao"],
            }
            for code, keywords in formula_keywords.items():
                for kw in keywords:
                    session.run("""
                        MATCH (e:Entity), (f:FormulaConcept {code: $code})
                        WHERE e.name CONTAINS $keyword
                        MERGE (e)-[:MAPS_TO]->(f)
                    """, code=code, keyword=kw)

            # Map Syndrome entities to SyndromeConcept nodes
            syndrome_keywords = {
                "YHCT_S01": ["Tâm tỳ lưỡng hư", "tâm tỳ lưỡng hư", "tâm tỳ"],
                "YHCT_S02": ["Âm hư hỏa vượng", "âm hư hỏa vượng"],
                "YHCT_S03": ["Tâm đởm khí hư", "tâm đởm khí hư", "tâm đởm"],
                "YHCT_S04": ["Đàm nhiệt nội nhiễu", "đàm nhiệt nội nhiễu", "đàm nhiệt"],
                "YHCT_S05": ["Can uất hóa hỏa", "can uất hóa hỏa", "can uất"],
            }
            for code, keywords in syndrome_keywords.items():
                for kw in keywords:
                    session.run("""
                        MATCH (e:Entity), (s:SyndromeConcept {code: $code})
                        WHERE e.name CONTAINS $keyword
                        MERGE (e)-[:MAPS_TO]->(s)
                    """, code=code, keyword=kw)

            # Map drug entities to DrugConcept nodes
            drug_keywords = {
                "N06BX03": ["Piracetam", "piracetam"],
                "N05CM_VN01": ["Rotundin", "rotundin"],
                "N06DX_VN01": ["Tanakan", "tanakan"],
            }
            for code, keywords in drug_keywords.items():
                for kw in keywords:
                    session.run("""
                        MATCH (e:Entity), (d:DrugConcept {code: $code})
                        WHERE e.name CONTAINS $keyword
                        MERGE (e)-[:MAPS_TO]->(d)
                    """, code=code, keyword=kw)

            print("Mapping complete.")

if __name__ == "__main__":
    mapper = OntologyMapper()
    try:
        mapper.map_entities()
    finally:
        mapper.close()
