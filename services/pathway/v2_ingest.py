"""
V2 Ingest Pipeline — Semantic chunking + disease-scoped knowledge graph.

Designed for scalability (10,000+ diseases). Uses :Chunk nodes (separate from V1 :Block),
typed entity nodes (:Drug, :LabTest, :Stage, etc.), and :ABOUT_DISEASE relationships.

Usage:
    cd notebooklm
    python v2_ingest.py
"""

import os
import re
import json
import unicodedata
import traceback
from pathlib import Path

import fitz  # PyMuPDF
from openai import AzureOpenAI
from neo4j import GraphDatabase
from runtime_env import load_notebooklm_env

load_notebooklm_env()

__all__ = ["clean_ocr_text", "SemanticChunker", "V2Ingest"]


# ---------------------------------------------------------------------------
# OCR Text Cleanup
# ---------------------------------------------------------------------------

def clean_ocr_text(text: str) -> str:
    """Clean scanned/OCR Vietnamese text: normalize unicode, fix line joins, strip junk."""
    # NFC normalization — recompose decomposed Vietnamese diacritics
    text = unicodedata.normalize("NFC", text)

    # Strip control characters (keep newline, tab, space)
    text = re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]', '', text)

    # Fix line joins: if a line ends with a lowercase letter and the next starts lowercase,
    # they were likely one sentence split by OCR line-wrap.
    text = re.sub(r'([a-zàáảãạăắằẳẵặâấầẩẫậđèéẻẽẹêếềểễệìíỉĩịòóỏõọôốồổỗộơớờởỡợùúủũụưứừửữựỳýỷỹỵ,;])\n([a-zàáảãạăắằẳẵặâấầẩẫậđèéẻẽẹêếềểễệìíỉĩịòóỏõọôốồổỗộơớờởỡợùúủũụưứừửữựỳýỷỹỵ])',
                  r'\1 \2', text)

    # Collapse multiple spaces (but keep newlines)
    text = re.sub(r'[^\S\n]+', ' ', text)

    # Collapse 3+ consecutive blank lines into 2
    text = re.sub(r'\n{3,}', '\n\n', text)

    return text.strip()


# ---------------------------------------------------------------------------
# Semantic Chunker
# ---------------------------------------------------------------------------

class SemanticChunker:
    """Split Vietnamese clinical text into semantically meaningful chunks based on headings."""

    HEADING_PATTERNS = [
        (re.compile(r'^[IVX]+\.\s*[A-ZĐÀÁẢÃẠĂẮẰẲẴẶÂẤẦẨẪẬÈÉẺẼẸÊẾỀỂỄỆÌÍỈĨỊÒÓỎÕỌÔỐỒỔỖỘƠỚỜỞỠỢÙÚỦŨỤƯỨỪỬỮỰỲÝỶỸỴa-z]'), 'section'),
        (re.compile(r'^\d+\.\s+[A-ZĐÀÁa-zàáảãạ]'), 'section'),
        (re.compile(r'^\d+\.\d+\.?\s+'), 'subsection'),
        (re.compile(r'^[a-z]\)\s+'), 'paragraph'),
        (re.compile(r'^[-•+]\s+'), 'paragraph'),
        (re.compile(r'^Bảng\s+\d+|^Phụ lục\s*\d*', re.IGNORECASE), 'section'),
        (re.compile(r'^Lưu đồ\s+\d+', re.IGNORECASE), 'section'),
    ]

    LEVEL_ORDER = {'section': 1, 'subsection': 2, 'paragraph': 3}

    MAX_CHUNK = 3000
    MIN_CHUNK = 100

    def _detect_heading(self, line: str):
        stripped = line.strip()
        if not stripped:
            return None, None
        for pattern, level in self.HEADING_PATTERNS:
            if pattern.match(stripped):
                return stripped, level
        return None, None

    def _split_at_sentence(self, text: str, max_len: int):
        """Split text at the last sentence boundary before max_len."""
        if len(text) <= max_len:
            return text, ""
        # Find last sentence-ending punctuation before max_len
        cut = text[:max_len]
        last_period = max(cut.rfind('. '), cut.rfind('.\n'), cut.rfind('? '), cut.rfind('! '))
        if last_period > max_len // 3:
            return text[:last_period + 1].strip(), text[last_period + 1:].strip()
        # Fallback: split at last space
        last_space = cut.rfind(' ')
        if last_space > max_len // 3:
            return text[:last_space].strip(), text[last_space:].strip()
        return text[:max_len].strip(), text[max_len:].strip()

    def chunk(self, text: str, skip_first_page: bool = True) -> list[dict]:
        """
        Parse text into semantic chunks.

        Returns list of dicts:
            {chunk_id, content, title, level, section_path, page_number, parent_context}
        """
        lines = text.split('\n')
        chunks: list[dict] = []

        current_content_lines: list[str] = []
        current_title = "Untitled"
        current_level = "section"
        section_stack: list[str] = []  # breadcrumb of heading titles
        current_page = 1
        chunk_start_page = 1

        def _flush():
            nonlocal current_content_lines
            body = '\n'.join(current_content_lines).strip()
            if not body:
                current_content_lines = []
                return
            parent_ctx = ' > '.join(section_stack[:-1]) if len(section_stack) > 1 else ""
            chunk = {
                'content': body,
                'title': current_title,
                'level': current_level,
                'section_path': ' > '.join(section_stack),
                'page_number': chunk_start_page,
                'parent_context': parent_ctx,
            }
            chunks.append(chunk)
            current_content_lines = []

        in_first_page = True

        for line in lines:
            # Track page markers from PyMuPDF extraction
            page_match = re.match(r'^---\s*Page\s+(\d+)\s*---$', line.strip())
            if page_match:
                current_page = int(page_match.group(1))
                if current_page > 1:
                    in_first_page = False
                continue

            if skip_first_page and in_first_page:
                continue

            heading_text, heading_level = self._detect_heading(line)

            if heading_text and heading_level:
                # Flush previous chunk
                _flush()
                chunk_start_page = current_page

                # Update section stack based on level hierarchy
                hlevel = self.LEVEL_ORDER.get(heading_level, 1)
                # Pop stack until we're at or above the new heading level
                while section_stack and len(section_stack) >= hlevel:
                    section_stack.pop()
                section_stack.append(heading_text)

                current_title = heading_text
                current_level = heading_level
                current_content_lines = [line]
            else:
                if not current_content_lines:
                    chunk_start_page = current_page
                current_content_lines.append(line)

        # Flush last chunk
        _flush()

        # Post-processing: split oversized chunks, merge tiny ones
        processed: list[dict] = []
        for chunk in chunks:
            content = chunk['content']
            if len(content) > self.MAX_CHUNK:
                # Split into sub-chunks at sentence boundaries
                remainder = content
                part_idx = 0
                while remainder:
                    part, remainder = self._split_at_sentence(remainder, self.MAX_CHUNK)
                    sub = dict(chunk)
                    sub['content'] = part
                    if part_idx > 0:
                        sub['title'] = f"{chunk['title']} (cont.)"
                    part_idx += 1
                    processed.append(sub)
            else:
                processed.append(chunk)

        # Merge tiny chunks into previous
        merged: list[dict] = []
        for chunk in processed:
            if merged and len(chunk['content']) < self.MIN_CHUNK:
                merged[-1]['content'] += '\n' + chunk['content']
            else:
                merged.append(chunk)

        # Assign chunk IDs
        for idx, chunk in enumerate(merged):
            chunk['chunk_id'] = f"chunk_{idx:03d}"

        return merged


# ---------------------------------------------------------------------------
# V2 Ingest Pipeline
# ---------------------------------------------------------------------------

class V2Ingest:
    """Ingest a Vietnamese clinical PDF into Neo4j using V2 architecture."""

    def __init__(self):
        self.client = AzureOpenAI(
            api_key=os.getenv("AZURE_OPENAI_API_KEY"),
            api_version=os.getenv("AZURE_OPENAI_API_VERSION"),
            azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT")
        )
        self.model = os.getenv("MODEL1", "gpt-4o-mini")

        self.embedding_client = AzureOpenAI(
            azure_endpoint=os.getenv("AZURE_EMBEDDINGS_ENDPOINT").strip(),
            api_key=os.getenv("AZURE_EMBEDDINGS_API_KEY").strip(),
            api_version=os.getenv("AZURE_OPENAI_API_VERSION").strip()
        )
        self.embedding_model = "text-embedding-ada-002"

        uri = os.getenv("neo4j_uri", "bolt://localhost:7688")
        user = os.getenv("neo4j_user", "neo4j")
        password = os.getenv("neo4j_password", "password123")
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

        self.chunker = SemanticChunker()

    def close(self):
        self.driver.close()

    # -- Text extraction --

    def extract_text(self, pdf_path: str) -> str:
        """Extract text from PDF using PyMuPDF block-based extraction."""
        doc = fitz.open(pdf_path)
        full_text = []
        for page_num, page in enumerate(doc):
            blocks = page.get_text("blocks")
            blocks.sort(key=lambda b: (b[1], b[0]))
            page_text = f"--- Page {page_num + 1} ---\n"
            for b in blocks:
                page_text += b[4] + "\n"
            full_text.append(page_text)
        doc.close()
        return "\n".join(full_text)

    # -- Embedding --

    def get_embedding(self, text: str) -> list[float] | None:
        text = text.replace("\n", " ")[:8000]
        try:
            return self.embedding_client.embeddings.create(
                input=[text], model=self.embedding_model
            ).data[0].embedding
        except Exception as e:
            print(f"  [WARN] Embedding error: {e}")
            return None

    # -- Neo4j indexes --

    def create_indexes(self):
        """Create vector and fulltext indexes for :Chunk nodes (separate from V1 :Block)."""
        with self.driver.session() as session:
            session.run("""
                CREATE VECTOR INDEX `chunk_vector_index` IF NOT EXISTS
                FOR (n:Chunk) ON (n.embedding)
                OPTIONS {indexConfig: {
                    `vector.dimensions`: 1536,
                    `vector.similarity_function`: 'cosine'
                }}
            """)
            print("[OK] Vector index 'chunk_vector_index' created or exists.")

            session.run("""
                CREATE FULLTEXT INDEX `chunk_fulltext` IF NOT EXISTS
                FOR (n:Chunk) ON EACH [n.title, n.content]
            """)
            print("[OK] Fulltext index 'chunk_fulltext' created or exists.")

    # -- Ontology from Catalog --

    def setup_ontology_from_catalog(self, catalog_path: str | None = None):
        """Load ontology seed data from JSON catalog file.

        If catalog_path is None, tries to find a matching catalog for the
        disease being ingested. Falls back to creating minimal Disease node.
        """
        if catalog_path is None:
            # Auto-discover from config/ontology_catalog/
            catalog_dir = Path(__file__).parent / "config" / "ontology_catalog"
            if catalog_dir.exists():
                for f in catalog_dir.glob("*.json"):
                    try:
                        data = json.loads(f.read_text(encoding="utf-8"))
                        if data.get("disease_name"):
                            catalog_path = str(f)
                            break
                    except Exception:
                        continue

        if not catalog_path or not Path(catalog_path).exists():
            print("  [SKIP] No ontology catalog found, using minimal setup.")
            return

        catalog = json.loads(Path(catalog_path).read_text(encoding="utf-8"))
        print(f"  Loading ontology from: {Path(catalog_path).name}")

        with self.driver.session() as session:
            # ICD hierarchy
            icd_nodes = {}
            for item in catalog.get("icd_hierarchy", []):
                level = item.get("level", "category")
                label = {"chapter": "ICD_Chapter", "category": "ICD_Category",
                         "subcategory": "ICD_Category"}.get(level, "ICD_Category")
                session.run(
                    f"MERGE (n:{label} {{code: $code}}) ON CREATE SET n.name = $name",
                    code=item["code"], name=item["name"]
                )
                icd_nodes[item["code"]] = label
                # Link to parent
                parent_code = item.get("parent")
                if parent_code and parent_code in icd_nodes:
                    parent_label = icd_nodes[parent_code]
                    rel = "HAS_CATEGORY" if level == "category" else "HAS_SUBCATEGORY"
                    session.run(
                        f"MATCH (p:{parent_label} {{code: $pc}}) "
                        f"MATCH (c:{label} {{code: $cc}}) "
                        f"MERGE (p)-[:{rel}]->(c)",
                        pc=parent_code, cc=item["code"]
                    )

            # Disease node
            disease = catalog.get("disease", {})
            if disease:
                session.run("""
                    MERGE (d:Disease {name: $name})
                    ON CREATE SET d.icd_code = $icd, d.category = $cat, d.aliases = $aliases
                """, name=disease["name"], icd=disease.get("icd_code", ""),
                     cat=disease.get("category", ""), aliases=disease.get("aliases", []))
                if disease.get("icd_code") and disease["icd_code"] in icd_nodes:
                    session.run("""
                        MATCH (d:Disease {name: $dname})
                        MATCH (icd:ICD_Category {code: $icd})
                        MERGE (d)-[:CLASSIFIED_AS]->(icd)
                    """, dname=disease["name"], icd=disease["icd_code"])

            # Drugs
            for drug in catalog.get("drugs", []):
                session.run("""
                    MERGE (d:Drug {name: $name})
                    ON CREATE SET d.code = $code
                """, name=drug["name"], code=drug.get("code", ""))

            # LabTests
            for name in catalog.get("lab_tests", []):
                session.run("MERGE (lt:LabTest {name: $name})", name=name)

            # Stages
            for stage in catalog.get("stages", []):
                session.run("""
                    MERGE (s:Stage {name: $code})
                    ON CREATE SET s.description = $desc, s.system = $sys
                """, code=stage["code"], desc=stage.get("description", ""),
                     sys=stage.get("system", ""))

            print(f"[OK] Ontology loaded from catalog: {len(catalog.get('drugs', []))} drugs, "
                  f"{len(catalog.get('lab_tests', []))} lab tests, "
                  f"{len(catalog.get('stages', []))} stages.")

    # -- LLM Entity Extraction --

    def extract_entities_and_relations(self, chunk_content: str, section_path: str) -> dict:
        """Use LLM to extract medical entities AND relationships from a chunk.

        Returns dict with 'entities' list and 'relations' list.
        Relations capture: INDICATION_FOR, CONTRA_INDICATES, DOSE_OF, RULE_OUT_FOR.
        """
        prompt = f"""Analyze the following Vietnamese medical text.
Extract all medical entities AND relationships between them.

Entity types:
- Drug: medication names (e.g., Tenofovir, Entecavir, Lamivudine)
- LabTest: lab tests and biomarkers (e.g., HBsAg, ALT, AST, HBV DNA)
- Disease: disease names (e.g., Viêm gan vi rút B, Xơ gan)
- Symptom: clinical signs (e.g., Vàng da, Mệt mỏi, Gan to)
- Stage: staging/classification (e.g., F0, F1, F2, F3, F4)
- Procedure: medical procedures (e.g., Sinh thiết gan, Siêu âm)

Relationship types (only extract if EXPLICITLY stated):
- INDICATION_FOR: Drug/Procedure is indicated for Disease/Stage
- CONTRA_INDICATES: Drug/Procedure is contraindicated for Disease/condition
- DOSE_OF: specific dosage mentioned for a Drug
- RULE_OUT_FOR: LabTest/Procedure is used to rule out Disease

Section context: {section_path}

Text:
{chunk_content}

Return JSON:
{{
  "entities": [{{"name": "...", "type": "Drug|LabTest|Disease|Symptom|Stage|Procedure"}}],
  "relations": [{{"from_name": "...", "to_name": "...", "rel_type": "INDICATION_FOR|CONTRA_INDICATES|DOSE_OF|RULE_OUT_FOR", "detail": "optional dosage or context"}}]
}}
Only include entities/relations EXPLICITLY mentioned in the text."""

        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": "You are a medical NER system. Extract entities and relationships precisely from Vietnamese clinical text. Return valid JSON only."},
                    {"role": "user", "content": prompt}
                ],
                response_format={"type": "json_object"},
                temperature=0.0
            )
            data = json.loads(response.choices[0].message.content)
            return {
                "entities": data.get("entities", []),
                "relations": data.get("relations", []),
            }
        except Exception as e:
            print(f"  [WARN] Entity extraction failed: {e}")
            return {"entities": [], "relations": []}

    # -- Neo4j Entity Merge --

    def merge_entity(self, session, entity: dict) -> str | None:
        """MERGE a typed entity node. Returns the node label used."""
        name = entity.get("name", "").strip()
        etype = entity.get("type", "").strip()
        if not name or not etype:
            return None

        # Map entity type to Neo4j label
        label_map = {
            "Drug": "Drug",
            "LabTest": "LabTest",
            "Disease": "Disease",
            "Symptom": "Symptom",
            "Stage": "Stage",
            "Procedure": "Procedure",
        }
        label = label_map.get(etype)
        if not label:
            return None

        # Use parameterized query with dynamic label
        session.run(
            f"MERGE (n:{label} {{name: $name}})",
            name=name
        )
        return label

    # -- Chunk Ingestion --

    # Allowed semantic relationship types for entity-to-entity links
    ALLOWED_RELATION_TYPES = {"INDICATION_FOR", "CONTRA_INDICATES", "DOSE_OF", "RULE_OUT_FOR"}

    def ingest_chunk(self, session, chunk: dict, disease_name: str,
                     protocol_name: str, entities: list[dict], embedding: list[float] | None,
                     relations: list[dict] | None = None):
        """Create a :Chunk node and link it to Disease, Protocol, entity nodes, and semantic relations."""
        # Create Chunk
        session.run("""
            CREATE (c:Chunk {
                chunk_id: $chunk_id,
                content: $content,
                title: $title,
                level: $level,
                section_path: $section_path,
                disease_name: $disease_name,
                page_number: $page_number,
                parent_context: $parent_context,
                embedding: $embedding
            })
        """, chunk_id=chunk['chunk_id'], content=chunk['content'],
             title=chunk['title'], level=chunk['level'],
             section_path=chunk['section_path'], disease_name=disease_name,
             page_number=chunk['page_number'],
             parent_context=chunk.get('parent_context', ''),
             embedding=embedding)

        # Link to Disease
        session.run("""
            MATCH (c:Chunk {chunk_id: $chunk_id})
            MATCH (d:Disease {name: $disease_name})
            MERGE (c)-[:ABOUT_DISEASE]->(d)
        """, chunk_id=chunk['chunk_id'], disease_name=disease_name)

        # Link to Protocol
        session.run("""
            MATCH (c:Chunk {chunk_id: $chunk_id})
            MATCH (p:Protocol {name: $protocol_name})
            MERGE (p)-[:HAS_BLOCK]->(c)
        """, chunk_id=chunk['chunk_id'], protocol_name=protocol_name)

        # Link to entities via :MENTIONS
        for ent in entities:
            label = self.merge_entity(session, ent)
            if label:
                session.run(f"""
                    MATCH (c:Chunk {{chunk_id: $chunk_id}})
                    MATCH (e:{label} {{name: $ename}})
                    MERGE (c)-[:MENTIONS]->(e)
                """, chunk_id=chunk['chunk_id'], ename=ent['name'].strip())

        # Create semantic entity-to-entity relations (INDICATION_FOR, CONTRA_INDICATES, etc.)
        if relations:
            entity_labels = {ent['name'].strip(): self.merge_entity(session, ent)
                             for ent in entities if ent.get('name')}
            for rel in relations:
                rel_type = rel.get("rel_type", "")
                if rel_type not in self.ALLOWED_RELATION_TYPES:
                    continue
                from_name = rel.get("from_name", "").strip()
                to_name = rel.get("to_name", "").strip()
                if not from_name or not to_name:
                    continue
                from_label = entity_labels.get(from_name)
                to_label = entity_labels.get(to_name)
                if not from_label or not to_label:
                    continue
                detail = rel.get("detail", "")
                session.run(f"""
                    MATCH (a:{from_label} {{name: $fname}})
                    MATCH (b:{to_label} {{name: $tname}})
                    MERGE (a)-[r:{rel_type}]->(b)
                    ON CREATE SET r.detail = $detail, r.source_chunk = $cid
                """, fname=from_name, tname=to_name,
                     detail=detail, cid=chunk['chunk_id'])

    # -- Hierarchy Links --

    def build_hierarchy(self, session, chunks: list[dict]):
        """Create NEXT_CHUNK sequential links between chunks."""
        for i in range(len(chunks) - 1):
            session.run("""
                MATCH (a:Chunk {chunk_id: $a_id})
                MATCH (b:Chunk {chunk_id: $b_id})
                MERGE (a)-[:NEXT_CHUNK]->(b)
            """, a_id=chunks[i]['chunk_id'], b_id=chunks[i + 1]['chunk_id'])

        # HAS_CHILD: link section-level chunks to their subsection children
        # based on section_path containment
        for i, parent in enumerate(chunks):
            if parent['level'] != 'section':
                continue
            parent_path = parent['section_path']
            for j in range(i + 1, len(chunks)):
                child = chunks[j]
                if child['level'] == 'section' and child['section_path'] != parent_path:
                    break  # hit next top-level section
                if child['section_path'].startswith(parent_path + ' > '):
                    session.run("""
                        MATCH (p:Chunk {chunk_id: $pid})
                        MATCH (c:Chunk {chunk_id: $cid})
                        MERGE (p)-[:HAS_CHILD]->(c)
                    """, pid=parent['chunk_id'], cid=child['chunk_id'])

        print("[OK] Hierarchy links (NEXT_CHUNK + HAS_CHILD) created.")

    # -- Main Pipeline --

    def run(self, pdf_path: str, disease_name: str, icd_code: str):
        """Full V2 ingest pipeline."""
        print(f"{'='*60}")
        print(f"V2 Ingest: {disease_name} (ICD: {icd_code})")
        print(f"PDF: {pdf_path}")
        print(f"{'='*60}")

        # 1. Extract text
        print("\n[1/7] Extracting text from PDF...")
        raw_text = self.extract_text(pdf_path)
        print(f"  Raw text length: {len(raw_text)} chars")

        # 2. Clean OCR
        print("[2/7] Cleaning OCR text...")
        cleaned_text = clean_ocr_text(raw_text)
        print(f"  Cleaned text length: {len(cleaned_text)} chars")

        # 3. Semantic chunking
        print("[3/7] Semantic chunking...")
        chunks = self.chunker.chunk(cleaned_text, skip_first_page=True)
        print(f"  Produced {len(chunks)} chunks")
        for c in chunks[:5]:
            print(f"    - [{c['chunk_id']}] {c['title'][:60]}  ({len(c['content'])} chars, p.{c['page_number']})")
        if len(chunks) > 5:
            print(f"    ... and {len(chunks) - 5} more")

        # 4. Create indexes
        print("\n[4/7] Creating Neo4j indexes...")
        self.create_indexes()

        # 5. Setup ontology from catalog (data-driven, not hardcoded)
        print("[5/7] Setting up ontology from catalog...")
        self.setup_ontology_from_catalog()

        # 6. Merge Disease + Protocol
        protocol_name = f"Phác đồ điều trị {disease_name} - Bộ Y tế 2019"
        with self.driver.session() as session:
            session.run("MERGE (d:Disease {name: $name}) ON CREATE SET d.icd_code = $icd",
                        name=disease_name, icd=icd_code)
            session.run("MERGE (p:Protocol {name: $name})", name=protocol_name)

        # 7. Ingest chunks with entities and embeddings
        print(f"\n[6/7] Ingesting {len(chunks)} chunks (LLM entities + embeddings)...")
        with self.driver.session() as session:
            for i, chunk in enumerate(chunks):
                print(f"  Chunk {i+1}/{len(chunks)}: {chunk['title'][:50]}...", end=" ")

                # Extract entities + relations via LLM
                extraction = self.extract_entities_and_relations(chunk['content'], chunk['section_path'])
                entities = extraction["entities"]
                relations = extraction["relations"]
                ent_names = [e['name'] for e in entities]
                print(f"[{len(entities)} ent, {len(relations)} rel: {', '.join(ent_names[:4])}{'...' if len(ent_names)>4 else ''}]", end=" ")

                # Generate embedding
                embed_text = f"{chunk['title']}\n{chunk.get('parent_context', '')}\n{chunk['content']}"
                embedding = self.get_embedding(embed_text)
                print("[emb OK]" if embedding else "[emb FAIL]")

                # Ingest to Neo4j
                self.ingest_chunk(session, chunk, disease_name, protocol_name, entities, embedding, relations)

            # 8. Build hierarchy
            print("\n[7/7] Building hierarchy links...")
            self.build_hierarchy(session, chunks)

        # Stats
        print(f"\n{'='*60}")
        print("INGESTION COMPLETE — Stats:")
        with self.driver.session() as session:
            chunk_count = session.run(
                "MATCH (c:Chunk {disease_name: $d}) RETURN count(c) as cnt",
                d=disease_name
            ).single()["cnt"]
            drug_count = session.run("MATCH (d:Drug) RETURN count(d) as cnt").single()["cnt"]
            lab_count = session.run("MATCH (lt:LabTest) RETURN count(lt) as cnt").single()["cnt"]
            stage_count = session.run("MATCH (s:Stage) RETURN count(s) as cnt").single()["cnt"]
            rel_count = session.run(
                "MATCH (c:Chunk)-[:ABOUT_DISEASE]->(d:Disease {name: $d}) RETURN count(c) as cnt",
                d=disease_name
            ).single()["cnt"]
            mentions = session.run(
                "MATCH (c:Chunk {disease_name: $d})-[:MENTIONS]->(e) RETURN count(e) as cnt",
                d=disease_name
            ).single()["cnt"]

        print(f"  Chunks:          {chunk_count}")
        print(f"  Drugs:           {drug_count}")
        print(f"  LabTests:        {lab_count}")
        print(f"  Stages:          {stage_count}")
        print(f"  ABOUT_DISEASE:   {rel_count}")
        print(f"  MENTIONS rels:   {mentions}")
        print(f"{'='*60}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    ingestor = V2Ingest()
    try:
        ingestor.run(
            pdf_path="Hướng dẫn điều trị viêm gan virus B của Bộ Y tế 2019.pdf",
            disease_name="Viêm gan vi rút B",
            icd_code="B18"
        )
    except Exception:
        traceback.print_exc()
    finally:
        ingestor.close()
