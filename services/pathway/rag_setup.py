import os
from openai import AzureOpenAI
from neo4j import GraphDatabase
import time
from runtime_env import load_notebooklm_env

# Load environment variables from notebooklm/.env
load_notebooklm_env()

class RAGSetup:
    def __init__(self):
        # Azure OpenAI for Embeddings
        self.embedding_client = AzureOpenAI(
            azure_endpoint=os.getenv("AZURE_EMBEDDINGS_ENDPOINT").strip(),
            api_key=os.getenv("AZURE_EMBEDDINGS_API_KEY").strip(),
            api_version=os.getenv("AZURE_OPENAI_API_VERSION").strip()
        )
        self.embedding_model = "text-embedding-ada-002" 
        
        # Neo4j
        self.neo4j_uri = "bolt://localhost:7688"
        self.neo4j_user = "neo4j"
        self.neo4j_password = "password123"
        self.driver = GraphDatabase.driver(self.neo4j_uri, auth=(self.neo4j_user, self.neo4j_password))

    def close(self):
        self.driver.close()

    def get_embedding(self, text):
        text = text.replace("\n", " ")
        try:
            return self.embedding_client.embeddings.create(input=[text], model=self.embedding_model).data[0].embedding
        except Exception as e:
            print(f"Error getting embedding: {e}")
            return None

    def setup_vector_index(self):
        # Create vector index in Neo4j (for Page nodes on property 'embedding')
        # We use 1536 dimensions for text-embedding-3-small
        index_query = """
        CREATE VECTOR INDEX `clinical_vector_index` IF NOT EXISTS
        FOR (n:Page)
        ON (n.embedding)
        OPTIONS {indexConfig: {
         `vector.dimensions`: 1536,
         `vector.similarity_function`: 'cosine'
        }}
        """
        with self.driver.session() as session:
            session.run(index_query)
            print("Vector index 'clinical_vector_index' created or already exists.")

    def populate_embeddings(self):
        # Fetch pages that have descriptions but no embeddings yet
        fetch_query = """
        MATCH (n:Page) 
        WHERE n.description IS NOT NULL AND n.embedding IS NULL
        RETURN id(n) as id, n.description as description, n.title as title
        LIMIT 100
        """
        
        with self.driver.session() as session:
            records = list(session.run(fetch_query))
            print(f"Found {len(records)} nodes to embed.")
            
            for record in records:
                node_id = record['id']
                description = f"{record['title']}: {record['description']}"
                embedding = self.get_embedding(description)
                
                if embedding:
                    session.run(
                        "MATCH (n:Page) WHERE id(n) = $id SET n.embedding = $embedding",
                        id=node_id, embedding=embedding
                    )
                    print(f"Embedded node {node_id}: {record['title'][:30]}...")
                
                time.sleep(0.1) # Avoid rate limits if any

if __name__ == "__main__":
    setup = RAGSetup()
    try:
        setup.setup_vector_index()
        setup.populate_embeddings()
        print("Embeddings population complete.")
    finally:
        setup.close()
