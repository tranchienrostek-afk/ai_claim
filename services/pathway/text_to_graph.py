import os
import json
from pathlib import Path
from openai import AzureOpenAI
from neo4j import GraphDatabase
from runtime_env import load_notebooklm_env

load_notebooklm_env()

class TextToGraph:
    def __init__(self):
        # Azure OpenAI Configuration
        self.client = AzureOpenAI(
            azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT").strip(),
            api_key=os.getenv("AZURE_OPENAI_API_KEY").strip(),
            api_version=os.getenv("AZURE_OPENAI_API_VERSION").strip()
        )
        self.model = os.getenv("MODEL1", "gpt-4o-mini").strip()
        
        # Neo4j Configuration
        self.neo4j_uri = "bolt://localhost:7688"
        self.neo4j_user = "neo4j"
        self.neo4j_password = "password123"
        self.driver = GraphDatabase.driver(self.neo4j_uri, auth=(self.neo4j_user, self.neo4j_password))

    def close(self):
        self.driver.close()

    def chunk_text(self, text, chunk_size=4000):
        """Simple chunking to fit LLM context limits."""
        return [text[i:i + chunk_size] for i in range(0, len(text), chunk_size)]

    def extract_from_text(self, text):
        """Extracts nodes and relationships from clinical text using LLM."""
        print("Extracting clinical knowledge using LLM...")
        
        system_prompt = """
        You are a clinical knowledge engineer. Extract medical entities and their interactions from the text.
        Output ONLY a JSON object with:
        - 'nodes': list of {id, label, properties}
        - 'relationships': list of {source_id, target_id, type, properties}
        
        Labels: Disease, Drug, Symptom, Procedure, Guideline, Recommendation.
        Types: TREATS, HAS_SYMPTOM, RECOMMENDS, DIAGNOSED_BY, CAUSES.
        """
        
        # We'll process the first chunk for demonstration; a loop would handle more.
        chunks = self.chunk_text(text)
        sample_text = chunks[0] if chunks else ""
        
        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": f"Text:\n{sample_text}"}
                ],
                response_format={"type": "json_object"},
                temperature=0
            )
            return json.loads(response.choices[0].message.content)
        except Exception as e:
            print(f"LLM Extraction failed: {e}")
            return {"nodes": [], "relationships": []}

    def ingest(self, data):
        """Ingests structured data into Neo4j."""
        print(f"Ingesting {len(data.get('nodes', []))} nodes and {len(data.get('relationships', []))} relations...")
        with self.driver.session() as session:
            # Nodes
            for node in data.get('nodes', []):
                session.run(f"""
                    MERGE (n:{node['label']} {{id: $id}})
                    SET n += $props
                """, id=node['id'], props=node.get('properties', {}))
            
            # Relationships
            for rel in data.get('relationships', []):
                session.run(f"""
                    MATCH (a {{id: $source}})
                    MATCH (b {{id: $target}})
                    MERGE (a)-[r:{rel['type']}]->(b)
                    SET r += $props
                """, source=rel['source_id'], target=rel['target_id'], props=rel.get('properties', {}))
        print("Ingestion complete.")

    def run(self, text_file_path):
        path = Path(text_file_path)
        if not path.exists():
            print(f"File not found: {path}")
            return
            
        with open(path, "r", encoding="utf-8") as f:
            text = f.read()
            
        data = self.extract_from_text(text)
        self.ingest(data)

if __name__ == "__main__":
    converter = TextToGraph()
    try:
        # Default to a file in the data/extracted_text directory
        target = Path("data") / "extracted_text" / "phac-do-dieu-tri-mat-ngu-theo-yhct-2023.txt"
        converter.run(target)
        print("Text to Graph conversion successful.")
    finally:
        converter.close()
