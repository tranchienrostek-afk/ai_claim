import os
import json
import traceback
from pathlib import Path
from openai import AzureOpenAI
from neo4j import GraphDatabase
from runtime_env import load_notebooklm_env

# Load environment variables from notebooklm/.env
load_notebooklm_env()

class AtomicIngest:
    def __init__(self):
        self.client = AzureOpenAI(
            api_key=os.getenv("AZURE_OPENAI_API_KEY"),
            api_version=os.getenv("AZURE_OPENAI_API_VERSION"),
            azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT")
        )
        self.model = os.getenv("MODEL1", "gpt-4o-mini")
        self.uri = os.getenv("neo4j_uri", "bolt://localhost:7688")
        self.user = os.getenv("neo4j_user", "neo4j")
        self.password = os.getenv("neo4j_password", "password123")
        self.driver = GraphDatabase.driver(self.uri, auth=(self.user, self.password))
        
        # Embedding settings
        self.embedding_endpoint = os.getenv("AZURE_EMBEDDINGS_ENDPOINT")
        self.embedding_key = os.getenv("AZURE_EMBEDDINGS_API_KEY")
        self.embedding_client = AzureOpenAI(
            azure_endpoint=self.embedding_endpoint.strip(),
            api_key=self.embedding_key.strip(),
            api_version=os.getenv("AZURE_OPENAI_API_VERSION").strip()
        )
        self.embedding_model = "text-embedding-ada-002"

    def close(self):
        self.driver.close()

    def get_embedding(self, text):
        text = text.replace("\n", " ")
        try:
            return self.embedding_client.embeddings.create(input=[text], model=self.embedding_model).data[0].embedding
        except Exception as e:
            print(f"Error getting embedding: {e}")
            return None

    def chunk_text(self, text, chunk_size=6000, overlap=500):
        """Split text into overlapping chunks on paragraph boundaries."""
        paragraphs = text.split("\n\n")
        chunks = []
        current_chunk = ""

        for para in paragraphs:
            if len(current_chunk) + len(para) > chunk_size and current_chunk:
                chunks.append(current_chunk.strip())
                # Keep overlap from end of previous chunk
                overlap_text = current_chunk[-overlap:] if len(current_chunk) > overlap else current_chunk
                current_chunk = overlap_text + "\n\n" + para
            else:
                current_chunk = current_chunk + "\n\n" + para if current_chunk else para

        if current_chunk.strip():
            chunks.append(current_chunk.strip())

        return chunks if chunks else [text]

    def create_index(self):
        index_query = """
        CREATE VECTOR INDEX `block_vector_index` IF NOT EXISTS
        FOR (n:Block)
        ON (n.embedding)
        OPTIONS {indexConfig: {
         `vector.dimensions`: 1536,
         `vector.similarity_function`: 'cosine'
        }}
        """
        fulltext_query = """
        CREATE FULLTEXT INDEX `block_fulltext` IF NOT EXISTS
        FOR (n:Block)
        ON EACH [n.title, n.content]
        """
        with self.driver.session() as session:
            session.run(index_query)
            print("Vector index 'block_vector_index' created or already exists.")
            session.run(fulltext_query)
            print("Fulltext index 'block_fulltext' created or already exists.")

    def parse_to_blocks(self, text):
        """
        Uses LLM to split text into atomic blocks inspired by Logseq and maps them to ontology.
        Processes all chunks of the text to avoid truncation.
        """
        chunks = self.chunk_text(text)
        all_blocks = []

        for chunk_idx, chunk in enumerate(chunks):
            prompt = f"""
Analyze the following medical protocol text (chunk {chunk_idx + 1}/{len(chunks)}) and split it into logical "Atomic Blocks".
Each block should be GRANULAR: one TCM syndrome = 1 block, one herbal formula = 1 block, one treatment method = 1 block.

For each block, extract:
1. title: Short descriptive title.
2. content: The FULL verbatim text content of the block — include all dosages, formulas, acupuncture points.
3. type: One of [Introduction, Diagnosis, Treatment, FollowUp, Prevention, Reference, Formula, Syndrome, TreatmentMethod].
4. entities: A list of objects {{"name": "...", "type": "Disease|Symptom|Med|Herb|AcupuncturePoint|Formula|Syndrome|TreatmentMethod", "mapping": "ICD-10 or ATC code if applicable"}}.
5. page_number: The page number where this block starts.
6. semantic_labels: A list of formal medical codes (ICD-10, ATC) that this block primarily discusses.

IMPORTANT:
- Each TCM syndrome (thể bệnh) must be its own block with full bát cương classification.
- Each herbal formula (bài thuốc) must be its own block with ALL ingredients and dosages.
- Each acupuncture prescription must be its own block with ALL points listed.
- Do NOT summarize or truncate content — preserve the original detail.

Text:
{chunk}

Return the result as a JSON object with a "blocks" key.
"""
            print(f"Sending semantic request to Azure OpenAI (chunk {chunk_idx + 1}/{len(chunks)})...")
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[{"role": "system", "content": "You are a medical knowledge graph architect. Map clinical text to formal ontology (ICD-10, ATC). Be EXHAUSTIVE — extract every syndrome, formula, drug, and acupuncture point as separate blocks."},
                          {"role": "user", "content": prompt}],
                response_format={"type": "json_object"}
            )

            raw_content = response.choices[0].message.content
            data = json.loads(raw_content)
            chunk_blocks = data.get("blocks", [])
            all_blocks.extend(chunk_blocks)
            print(f"  Chunk {chunk_idx + 1}: extracted {len(chunk_blocks)} blocks")

        print(f"Total blocks extracted across {len(chunks)} chunks: {len(all_blocks)}")
        return all_blocks

    def ingest(self, protocol_name, blocks):
        if not isinstance(blocks, list): return

        with self.driver.session() as session:
            session.run("MERGE (p:Protocol {name: $name})", name=protocol_name)
            
            prev_block_id = None
            for i, block in enumerate(blocks):
                block_id = f"{protocol_name}_B{i}"
                embedding = self.get_embedding(block['content'])
                
                # 1. Create/Update Block
                session.run("""
                    MERGE (b:Block {id: $id})
                    SET b.title = $title, b.content = $content, b.type = $type, b.order = $order, 
                        b.embedding = $embedding, b.page_number = $page
                    WITH b
                    MATCH (p:Protocol {name: $pname})
                    MERGE (p)-[:HAS_BLOCK]->(b)
                """, id=block_id, title=block['title'], content=block['content'], 
                     type=block['type'], order=i, pname=protocol_name, 
                     embedding=embedding, page=int(block.get('page_number', 1)))
                
                # 2. Sequential link
                if prev_block_id:
                    session.run("MATCH (p:Block {id: $pid}), (c:Block {id: $cid}) MERGE (p)-[:NEXT_BLOCK]->(c)", 
                                pid=prev_block_id, cid=block_id)
                prev_block_id = block_id
                
                # 3. Ontology Mapping for Block
                labels = block.get('semantic_labels', [])
                for code in labels:
                    # Link to ICD Category if it matches
                    session.run("""
                        MATCH (b:Block {id: $bid})
                        MATCH (target) WHERE target.code = $code
                        MERGE (b)-[:REFERS_TO_CONCEPT]->(target)
                    """, bid=block_id, code=code)

                # 4. Process Entities with Ontology Instance Mapping
                entities = block.get('entities', [])
                for ent in entities:
                    session.run("""
                        MATCH (b:Block {id: $bid})
                        MERGE (e:Entity {name: $ename})
                        ON CREATE SET e.type = $etype, e.code = $ecode
                        MERGE (b)-[:REFERENCES]->(e)
                        WITH e
                        // If entity has a code, link it to the formal Ontology Concept
                        MATCH (concept) WHERE concept.code = $ecode
                        MERGE (e)-[:INSTANCE_OF]->(concept)
                    """, bid=block_id, ename=ent['name'], etype=ent['type'], ecode=ent.get('mapping'))

    def run(self, text_file):
        protocol_name = Path(text_file).stem.replace("-", " ").title()
        with open(text_file, "r", encoding="utf-8") as f:
            text = f.read()
        
        print(f"Parsing protocol: {protocol_name}")
        blocks = self.parse_to_blocks(text)
        print(f"Extracted {len(blocks)} blocks. Creating index and ingesting to Neo4j...")
        self.create_index()
        self.ingest(protocol_name, blocks)
        print("Ingestion complete.")

if __name__ == "__main__":
    ingestor = AtomicIngest()
    try:
        target = Path("data") / "extracted_text" / "phac-do-dieu-tri-mat-ngu-theo-yhct-2023.txt"
        ingestor.run(target)
    except Exception:
        traceback.print_exc()
    finally:
        ingestor.close()
