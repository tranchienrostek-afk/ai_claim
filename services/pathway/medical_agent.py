import os
import sys
import io
import json
import time
import unicodedata
import re

# Fix Windows cp1252 encoding for Vietnamese output
if sys.stdout and hasattr(sys.stdout, 'buffer'):
    try:
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    except Exception:
        pass

from openai import AzureOpenAI
from neo4j import GraphDatabase
from runtime_env import load_notebooklm_env


def _rrf_score(rank: int, k: int = 60) -> float:
    """Reciprocal Rank Fusion score: 1/(k + rank). Rank-based, scale-invariant."""
    return 1.0 / (k + rank)


def _reciprocal_rank_fusion(result_lists: list[list[dict]], k: int = 60) -> list[dict]:
    """Merge multiple ranked result lists using RRF.

    Each list is independently ranked. Items are identified by 'block_id'.
    Returns merged list sorted by fused score descending.
    """
    fused_scores: dict[str, float] = {}
    items: dict[str, dict] = {}

    for result_list in result_lists:
        for rank, item in enumerate(result_list):
            bid = item.get('block_id') or f"_anon_{id(item)}"
            fused_scores[bid] = fused_scores.get(bid, 0.0) + _rrf_score(rank, k)
            # Keep the item with the highest original score
            if bid not in items or (item.get('score', 0) > items[bid].get('score', 0)):
                items[bid] = item

    ranked = sorted(fused_scores.items(), key=lambda x: x[1], reverse=True)
    result = []
    for bid, fused_sc in ranked:
        if bid in items:
            entry = dict(items[bid])
            entry['rrf_score'] = fused_sc
            result.append(entry)
    return result


def _expand_context(chunk: dict) -> dict:
    """Expand a chunk's description with adjacent NEXT_CHUNK content.

    Uses prev_block_content and next_block_content already fetched by Cypher.
    Returns chunk with 'expanded_description' field added.
    """
    parts = []
    prev = chunk.get('prev_block_content')
    if prev and isinstance(prev, str) and prev.strip():
        parts.append(f"[Ngữ cảnh trước] {prev.strip()[:500]}")
    parts.append(chunk.get('description', ''))
    nxt = chunk.get('next_block_content')
    if nxt and isinstance(nxt, str) and nxt.strip():
        parts.append(f"[Ngữ cảnh sau] {nxt.strip()[:500]}")
    chunk['expanded_description'] = '\n'.join(parts)
    return chunk


def _strip_diacritics(text: str) -> str:
    """Remove Vietnamese diacritical marks for fuzzy matching.
    e.g. 'Ung thư vú' → 'ung thu vu', 'sốt xuất huyết' → 'sot xuat huyet'"""
    text = text.lower()
    # Vietnamese specific: đ → d
    text = text.replace('đ', 'd').replace('Đ', 'd')
    # Decompose unicode, strip combining marks
    nfkd = unicodedata.normalize('NFKD', text)
    return ''.join(c for c in nfkd if not unicodedata.combining(c))

# Load environment variables from notebooklm/.env
load_notebooklm_env()

class MedicalAgent:
    def __init__(self):
        # Azure OpenAI for Embeddings
        self.embedding_client = AzureOpenAI(
            azure_endpoint=os.getenv("AZURE_EMBEDDINGS_ENDPOINT").strip(),
            api_key=os.getenv("AZURE_EMBEDDINGS_API_KEY").strip(),
            api_version=os.getenv("AZURE_OPENAI_API_VERSION").strip()
        )
        
        # Azure OpenAI for Chat
        self.chat_client = AzureOpenAI(
            azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT").strip(),
            api_key=os.getenv("AZURE_OPENAI_API_KEY").strip(),
            api_version=os.getenv("AZURE_OPENAI_API_VERSION").strip()
        )
        
        self.model = os.getenv("MODEL2", "gpt-5-mini").strip()
        self.embedding_model = "text-embedding-ada-002" 
        
        # Neo4j
        self.neo4j_uri = os.getenv("NEO4J_URI", "bolt://localhost:7688")
        self.neo4j_user = os.getenv("NEO4J_USER", "neo4j")
        self.neo4j_password = os.getenv("NEO4J_PASSWORD", "password123")
        self.driver = GraphDatabase.driver(self.neo4j_uri, auth=(self.neo4j_user, self.neo4j_password))

        # Unified knowledge retriever — queries ALL Neo4j data layers
        from server_support.unified_retriever import UnifiedRetriever
        self.unified = UnifiedRetriever(
            driver=self.driver,
            embedding_fn=self.get_query_embedding
        )

        # Knowledge inventory — "biết mình biết gì" (inspired by Claude Code pre-context)
        from server_support.knowledge_inventory import KnowledgeInventory
        self.inventory = KnowledgeInventory(driver=self.driver, ttl_seconds=300)

        # Query memory — learn from past queries
        from server_support.query_memory import QueryMemory
        self.query_memory = QueryMemory(
            driver=self.driver,
            embedding_fn=self.get_query_embedding
        )

        # Adaptive search planner — progressive refinement
        from server_support.adaptive_search_planner import AdaptiveSearchPlanner
        self.planner = AdaptiveSearchPlanner(
            inventory=self.inventory,
            retriever=self.unified,
            embedding_fn=self.get_query_embedding,
            chat_fn=self._chat_json,
        )

    def _chat_json(self, messages: list[dict]) -> str:
        """Simple chat completion for internal use (query decomposition, etc.)."""
        resp = self.chat_client.chat.completions.create(
            model=self.model, messages=messages,
            response_format={"type": "json_object"}
        )
        return resp.choices[0].message.content

    def close(self):
        self.driver.close()

    def get_query_embedding(self, query):
        print(f"DEBUG: Generating embedding for query using {self.embedding_model} on {os.getenv('AZURE_EMBEDDINGS_ENDPOINT')}")
        try:
            return self.embedding_client.embeddings.create(
                input=[query], model=self.embedding_model
            ).data[0].embedding
        except Exception as e:
            print(f"CRITICAL ERROR in get_query_embedding: {e}")
            raise

    def hybrid_search(self, query_text, top_k=3):
        """Vector search on Ontology V2 RawChunk nodes with context expansion."""
        query_vector = self.get_query_embedding(query_text)
        print("DEBUG: Vector search query started (rawchunk_vector_idx)...")

        cypher_query = """
        CALL db.index.vector.queryNodes('rawchunk_vector_idx', $top_k, $query_vector)
        YIELD node, score
        OPTIONAL MATCH (node)-[:CHUNK_ABOUT_DISEASE]->(d:DiseaseEntity)
        OPTIONAL MATCH (node)-[:MENTIONS_SIGN]->(sign:RawSignMention)
        RETURN
            node.section_title as title,
            node.body_preview as description,
            d.disease_name as disease_name,
            node.chunk_id as block_id,
            node.page_numbers as page_numbers,
            score,
            collect(DISTINCT sign.mention_text)[..5] as context_links
        ORDER BY score DESC
        """

        with self.driver.session() as session:
            result = session.run(cypher_query, query_vector=query_vector, top_k=top_k)
            return [dict(record) for record in result]

    def graph_rag_search(self, query_text, top_k=5):
        """
        Vector search on RawChunk with NEXT_CHUNK context and assertion expansion.
        """
        query_vector = self.get_query_embedding(query_text)

        cypher_query = """
        CALL db.index.vector.queryNodes('rawchunk_vector_idx', $top_k, $query_vector)
        YIELD node, score
        OPTIONAL MATCH (node)-[:CHUNK_ABOUT_DISEASE]->(d:DiseaseEntity)

        // NEXT_CHUNK traversal: get preceding and following chunks for context continuity
        OPTIONAL MATCH (prev:RawChunk)-[:NEXT_CHUNK]->(node)
        OPTIONAL MATCH (node)-[:NEXT_CHUNK]->(next:RawChunk)

        // Assertions from this chunk
        OPTIONAL MATCH (node)-[:CONTAINS_ASSERTION]->(a:ProtocolAssertion)

        RETURN
            node.section_title as title,
            node.body_preview as description,
            d.disease_name as source,
            node.page_numbers as page_number,
            node.chunk_id as block_id,
            score,
            collect(DISTINCT {
                title: coalesce(a.assertion_type, 'assertion'),
                content: coalesce(a.assertion_text, '')
            })[..3] as related_context,
            prev.body_preview as prev_block_content,
            next.body_preview as next_block_content
        ORDER BY score DESC
        """

        with self.driver.session() as session:
            result = session.run(cypher_query, query_vector=query_vector, top_k=top_k)
            return [dict(record) for record in result]

    def atomic_search(self, query_text, top_k=5):
        """Alias for graph_rag_search — used by academic_agent.py and diagnostic_test.py."""
        return self.graph_rag_search(query_text, top_k=top_k)

    def fulltext_search(self, query_text, top_k=5):
        """Fulltext search on RawChunk body_text and section_title fields."""
        boosted_query = self._build_boosted_fulltext_query(query_text)

        cypher_query = """
        CALL db.index.fulltext.queryNodes('raw_chunk_fulltext', $search_query)
        YIELD node, score
        OPTIONAL MATCH (node)-[:CHUNK_ABOUT_DISEASE]->(d:DiseaseEntity)
        RETURN
            node.section_title as title,
            node.body_preview as description,
            d.disease_name as source,
            node.page_numbers as page_number,
            node.chunk_id as block_id,
            score,
            [] as related_context,
            null as prev_block_content,
            null as next_block_content
        ORDER BY score DESC
        LIMIT $limit_k
        """

        with self.driver.session() as session:
            try:
                result = session.run(cypher_query, search_query=boosted_query, limit_k=top_k)
                return [dict(record) for record in result]
            except Exception as e:
                print(f"Fulltext search error (raw_chunk_fulltext may not exist): {e}")
                return []

    def ontology_context_search(self, query_text, top_k=3):
        """
        Search for RawChunks connected to sign/service concepts matching query keywords.
        Traverses RawSignMention, RawServiceMention via MENTIONS_SIGN/MENTIONS_SERVICE paths.
        """
        cypher_query = """
        CALL db.index.fulltext.queryNodes('raw_chunk_fulltext', $query_text)
        YIELD node, score
        OPTIONAL MATCH (node)-[:CHUNK_ABOUT_DISEASE]->(d:DiseaseEntity)
        OPTIONAL MATCH (node)-[:MENTIONS_SIGN]->(sign:RawSignMention)
        OPTIONAL MATCH (node)-[:MENTIONS_SERVICE]->(svc:RawServiceMention)
        WITH node, d, score,
             collect(DISTINCT sign.mention_text)[..3] as signs,
             collect(DISTINCT svc.mention_text)[..3] as services
        WHERE size(signs) > 0 OR size(services) > 0
        RETURN DISTINCT
            node.section_title as title,
            node.body_preview as description,
            d.disease_name as source,
            node.page_numbers as page_number,
            node.chunk_id as block_id,
            score,
            [] as related_context,
            null as prev_block_content,
            null as next_block_content
        ORDER BY score DESC
        LIMIT $limit_k
        """
        with self.driver.session() as session:
            try:
                result = session.run(cypher_query, query_text=query_text, limit_k=top_k)
                return [dict(record) for record in result]
            except Exception as e:
                print(f"Ontology context search error: {e}")
                return []

    # ------------------------------------------------------------------
    # V2: Disease-scoped search on :Chunk nodes
    # ------------------------------------------------------------------

    def scoped_search(self, query_text, disease_name, top_k=8,
                      source_type=None, hospital_name=None):
        """V2: Combined vector + fulltext search filtered by disease_name on Chunk nodes.

        Falls back to V1 graph_rag_search() if no Chunk results are found.

        Args:
            source_type: "hospital" | "BYT" | None (no filter)
            hospital_name: filter by specific hospital (when source_type="hospital")
        """
        sub_k = max(top_k, 10)
        vector_results = self._scoped_vector_search(
            query_text, disease_name, top_k=sub_k,
            source_type=source_type, hospital_name=hospital_name)
        fulltext_results = self.scoped_fulltext_search(
            query_text, disease_name, top_k=sub_k,
            source_type=source_type, hospital_name=hospital_name)

        # Reciprocal Rank Fusion — scale-invariant merge of vector + fulltext
        combined = _reciprocal_rank_fusion([vector_results, fulltext_results])[:top_k]

        # Fallback to V1 if no Chunk results (may not exist in fresh databases)
        if not combined:
            print(f"[scoped_search] No Chunk results for '{disease_name}', trying V1 Block search")
            try:
                return self.graph_rag_search(query_text, top_k=top_k)
            except Exception:
                return []

        return combined

    def _scoped_vector_search(self, query_text, disease_name, top_k=5,
                              source_type=None, hospital_name=None):
        """Vector search on rawchunk_vector_idx filtered by disease via DiseaseEntity relationship."""
        query_vector = self.get_query_embedding(query_text)

        params = {"qv": query_vector, "disease": disease_name, "top_k": top_k}

        cypher_query = """
        CALL db.index.vector.queryNodes('rawchunk_vector_idx', 120, $qv)
        YIELD node AS chunk, score
        MATCH (chunk)-[:CHUNK_ABOUT_DISEASE]->(d:DiseaseEntity)
        WHERE toLower(d.disease_name) CONTAINS toLower($disease)
           OR toLower(d.disease_id) CONTAINS toLower($disease)
        OPTIONAL MATCH (prev:RawChunk)-[:NEXT_CHUNK]->(chunk)
        OPTIONAL MATCH (chunk)-[:NEXT_CHUNK]->(nxt:RawChunk)
        RETURN chunk.section_title as title, chunk.body_preview as description,
               chunk.parent_section_path as section_path, chunk.chunk_id as block_id,
               chunk.page_numbers as page_number,
               d.disease_name as disease_name,
               score, prev.body_preview as prev_block_content, nxt.body_preview as next_block_content
        ORDER BY score DESC LIMIT $top_k
        """

        with self.driver.session() as session:
            try:
                result = session.run(cypher_query, **params)
                return [dict(record) for record in result]
            except Exception as e:
                print(f"[scoped_vector_search] Error (rawchunk_vector_idx): {e}")
                return []

    def _unscoped_chunk_vector_search(self, query_text, top_k=8):
        """Vector search on rawchunk_vector_idx without disease filter (cross-disease fallback)."""
        query_vector = self.get_query_embedding(query_text)
        cypher_query = """
        CALL db.index.vector.queryNodes('rawchunk_vector_idx', $top_k, $qv)
        YIELD node AS chunk, score
        OPTIONAL MATCH (chunk)-[:CHUNK_ABOUT_DISEASE]->(d:DiseaseEntity)
        RETURN chunk.section_title as title, chunk.body_preview as description,
               chunk.parent_section_path as section_path, chunk.chunk_id as block_id,
               chunk.page_numbers as page_number, d.disease_name as disease_name,
               score
        ORDER BY score DESC LIMIT $top_k
        """
        with self.driver.session() as session:
            result = session.run(cypher_query, qv=query_vector, top_k=top_k)
            return [dict(record) for record in result]

    def _escape_lucene(self, text: str) -> str:
        """Escape Lucene special characters and truncate."""
        special_chars = r'+-&|!(){}[]^"~*?:\/'
        escaped = text
        for ch in special_chars:
            escaped = escaped.replace(ch, f'\\{ch}')
        return escaped[:200]

    def _build_boosted_fulltext_query(self, query_text: str, disease_name: str = None) -> str:
        """Build Lucene query with section_title boost (3x) + disease expansion.

        Fields indexed: body_text, section_title (from raw_chunk_fulltext).
        """
        escaped = self._escape_lucene(query_text)
        parts = [f"section_title:{escaped}^3", f"body_text:{escaped}"]

        # Query Expansion: add disease name as keyword
        if disease_name:
            d = self._escape_lucene(disease_name)
            if d and len(d) > 2:
                parts.append(f"body_text:{d}^0.5")

        return " OR ".join(parts)

    def scoped_fulltext_search(self, query_text, disease_name, top_k=5,
                               source_type=None, hospital_name=None):
        """Fulltext search on raw_chunk_fulltext index filtered by disease via DiseaseEntity."""
        boosted_query = self._build_boosted_fulltext_query(query_text, disease_name=disease_name)

        params = {"search_query": boosted_query, "disease": disease_name, "limit_k": top_k}

        cypher_query = """
        CALL db.index.fulltext.queryNodes('raw_chunk_fulltext', $search_query)
        YIELD node, score
        MATCH (node)-[:CHUNK_ABOUT_DISEASE]->(d:DiseaseEntity)
        WHERE toLower(d.disease_name) CONTAINS toLower($disease)
           OR toLower(d.disease_id) CONTAINS toLower($disease)
        OPTIONAL MATCH (prev:RawChunk)-[:NEXT_CHUNK]->(node)
        OPTIONAL MATCH (node)-[:NEXT_CHUNK]->(nxt:RawChunk)
        RETURN
            node.section_title as title,
            node.body_preview as description,
            node.parent_section_path as section_path,
            node.chunk_id as block_id,
            node.page_numbers as page_number,
            d.disease_name as disease_name,
            score,
            prev.body_preview as prev_block_content,
            nxt.body_preview as next_block_content
        ORDER BY score DESC
        LIMIT $limit_k
        """

        with self.driver.session() as session:
            try:
                result = session.run(cypher_query, **params)
                return [dict(record) for record in result]
            except Exception as e:
                print(f"[scoped_fulltext_search] Error (raw_chunk_fulltext): {e}")
                return []

    def priority_search(self, query_text, disease_name, hospital_name=None, top_k=8):
        """Search with priority: hospital-specific > BYT > other hospitals.

        Returns (results, source_priority) where source_priority indicates
        which tier the results came from.
        """
        if hospital_name:
            # Tier 1: Hospital-specific protocol
            results = self.scoped_search(
                query_text, disease_name, top_k,
                source_type="hospital", hospital_name=hospital_name)
            if results:
                return results, "hospital"

            # Tier 2: Fallback to BYT
            results = self.scoped_search(
                query_text, disease_name, top_k,
                source_type="BYT")
            if results:
                return results, "BYT"

            # Tier 3: Any source (other hospitals)
            results = self.scoped_search(query_text, disease_name, top_k)
            return results, "other"
        else:
            # No hospital specified → BYT first, then all
            results = self.scoped_search(
                query_text, disease_name, top_k,
                source_type="BYT")
            if results:
                return results, "BYT"
            results = self.scoped_search(query_text, disease_name, top_k)
            return results, "all"

    def resolve_disease_name(self, query: str) -> str | None:
        """Find the best matching disease_name from Neo4j Disease nodes.

        Extracts keywords from the query and matches against Disease node names
        and aliases. Ranks by match quality (shorter name = more specific match).
        Returns None if no match found (caller should fall back to
        enhanced_search unscoped).
        """
        # Only match diseases that have actual RawChunk data (Ontology V2)
        cypher = """
        MATCH (d:DiseaseEntity)<-[:CHUNK_ABOUT_DISEASE]-(:RawChunk)
        WHERE toLower(d.disease_name) CONTAINS toLower($keyword)
           OR toLower(d.disease_id) CONTAINS toLower($keyword)
        RETURN DISTINCT d.disease_name AS name
        """
        # Extract meaningful keywords (2+ Vietnamese syllable phrases)
        # Remove common question words to isolate the medical term
        stop_words = {
            'là', 'gì', 'thế', 'nào', 'như', 'có', 'được', 'không', 'của', 'và',
            'trong', 'cho', 'với', 'các', 'một', 'những', 'này', 'đó', 'theo',
            'điều', 'trị', 'chẩn', 'đoán', 'hướng', 'dẫn', 'phác', 'đồ',
            'bệnh', 'nhân', 'triệu', 'chứng', 'thuốc', 'xét', 'nghiệm',
            'cách', 'sao', 'khi', 'nên', 'cần', 'phải', 'hay', 'hoặc',
        }
        words = query.lower().split()
        # Build candidate phrases: longer first (more specific)
        candidates = []
        cleaned = [w for w in words if w not in stop_words]
        if cleaned:
            candidates.append(' '.join(cleaned))
        for n in range(min(5, len(words)), 1, -1):
            for i in range(len(words) - n + 1):
                phrase = ' '.join(words[i:i+n])
                if not all(w in stop_words for w in words[i:i+n]):
                    candidates.append(phrase)

        with self.driver.session() as session:
            for keyword in candidates:
                if len(keyword) < 4:
                    continue
                result = session.run(cypher, keyword=keyword)
                matches = [rec['name'] for rec in result]
                if not matches:
                    continue
                # Prefer: (1) exact match, (2) best coverage ratio
                kw_lower = keyword.lower()
                for m in matches:
                    if m.lower() == kw_lower:
                        return m
                # Sort by how well the keyword covers the disease name
                # Higher ratio = keyword covers more of the name = better match
                matches.sort(key=lambda m: len(keyword) / max(len(m), 1), reverse=True)
                best = matches[0]
                # Require keyword covers at least 40% of the disease name
                # to avoid weak partial matches (e.g. "chảy máu" matching "Polyp chảy máu")
                coverage = len(keyword) / max(len(best), 1)
                if coverage >= 0.4:
                    return best

        # ── Fallback: diacritical-stripped fuzzy match ──
        # Handles ASCII input ("ung thu vu") or typos ("ung thứ vú")
        # matching Vietnamese names ("Ung thư vú")
        with self.driver.session() as session:
            all_diseases = session.run(
                "MATCH (d:DiseaseEntity)<-[:CHUNK_ABOUT_DISEASE]-(:RawChunk) RETURN DISTINCT d.disease_name AS name"
            )
            disease_names = [rec['name'] for rec in all_diseases]

        if not disease_names:
            return None

        query_stripped = _strip_diacritics(query)
        best_match = None
        best_coverage = 0.0

        for dname in disease_names:
            dname_stripped = _strip_diacritics(dname)
            # Check if disease name appears in query (stripped)
            if dname_stripped in query_stripped:
                coverage = len(dname_stripped) / max(len(dname_stripped), 1)
                if coverage > best_coverage or (coverage == best_coverage and
                        (best_match is None or len(dname) < len(best_match))):
                    best_match = dname
                    best_coverage = coverage
            # Also check keyword candidates against stripped disease name
            for keyword in candidates:
                kw_stripped = _strip_diacritics(keyword)
                if len(kw_stripped) < 4:
                    continue
                if kw_stripped in dname_stripped or dname_stripped in kw_stripped:
                    coverage = len(kw_stripped) / max(len(dname_stripped), 1)
                    if coverage >= 0.4 and coverage > best_coverage:
                        best_match = dname
                        best_coverage = coverage

        return best_match

    def enhanced_search(self, query_text, top_k=8, disease_name=None):
        """
        Combines vector search + fulltext search + ontology context,
        deduplicates by block_id, keeps highest score, returns top_k results.

        If disease_name is provided, also searches V2 Chunk nodes scoped to that disease.
        """
        # V1 Block-based search (may not exist in fresh databases)
        try:
            vector_results = self.graph_rag_search(query_text, top_k=5)
        except Exception:
            vector_results = []
        try:
            fulltext_results = self.fulltext_search(query_text, top_k=5)
        except Exception:
            fulltext_results = []

        # V2: Include scoped Chunk results
        if disease_name:
            vector_results += self._scoped_vector_search(query_text, disease_name, top_k=5)
            fulltext_results += self.scoped_fulltext_search(query_text, disease_name, top_k=5)

        # If no V1 results found, try V2 unscoped chunk search as fallback
        if not vector_results and not fulltext_results and not disease_name:
            try:
                vector_results = self._unscoped_chunk_vector_search(query_text, top_k=top_k)
            except Exception:
                pass

        # Reciprocal Rank Fusion — scale-invariant merge of vector + fulltext
        combined = _reciprocal_rank_fusion([vector_results, fulltext_results])
        return combined[:top_k]

    # ==================================================================
    #  AGENTIC RAG ENGINE — ReAct (Reason + Act) Loop
    # ==================================================================

    def preprocess_query(self, raw_question: str, history: list = None) -> dict:
        """Step 1: LLM phân tích câu hỏi → intent, entities, search plan."""
        history_ctx = ""
        if history:
            history_ctx = "\nLịch sử hội thoại gần đây:\n" + "\n".join(
                f"- Bác sĩ: {h['q'][:100]}\n  AI: {h['a'][:100]}" for h in history[-3:]
            ) + "\n"

        try:
            resp = self.chat_client.chat.completions.create(
                model=self.model,
                messages=[{"role": "user", "content": f"""Phân tích câu hỏi y khoa. Trả về JSON:
{{
  "intent": "lookup|compare|contraindication|dosage|procedure|diagnosis|general",
  "entities": [{{"name": "tên", "type": "Drug|Disease|Symptom|Stage|Biomarker|Procedure"}}],
  "disease_hint": "tên bệnh nếu phát hiện hoặc null",
  "sub_queries": ["câu hỏi phụ giúp trả lời đầy đủ hơn"],
  "needs_graph_traversal": true nếu hỏi về tương tác thuốc/chống chỉ định/mối quan hệ,
  "needs_verification": true nếu liên quan liều lượng/chống chỉ định/an toàn
}}
{history_ctx}
Câu hỏi: {raw_question}"""}],
                response_format={"type": "json_object"}
            )
            return json.loads(resp.choices[0].message.content)
        except Exception as e:
            print(f"[preprocess_query] Error: {e}")
            return {"intent": "general", "entities": [], "disease_hint": None,
                    "sub_queries": [], "needs_graph_traversal": False, "needs_verification": False}

    def graph_entity_search(self, entity_name: str, entity_type: str = None) -> list:
        """Search Neo4j graph for entity relationships using unified retriever.

        Searches across V2 ontology: signs, services, assertions, and graph traversal.
        Falls back gracefully if unified retriever returns empty.
        """
        context = []
        try:
            # Use unified retriever for entity-level search
            if entity_type in ("Symptom", "Sign"):
                results = self.unified.search_sign_mentions(entity_name, top_k=8)
            elif entity_type in ("Drug", "Procedure"):
                results = self.unified.search_service_mentions(entity_name, top_k=8)
            else:
                # Search both signs and services
                results = self.unified.search_sign_mentions(entity_name, top_k=4)
                results += self.unified.search_service_mentions(entity_name, top_k=4)

            # Also search assertions for this entity (contraindications, dosage rules)
            assertions = self.unified.search_assertions(entity_name, top_k=4)
            results += assertions

            # Convert to legacy search-result format
            for r in results:
                context.append(r.to_context_dict())

            return context
        except Exception as e:
            print(f"[graph_entity_search] Error: {e}")
            return []

    def llm_rerank(self, question: str, candidates: list, top_k: int = 8) -> list:
        """LLM re-ranks search results by relevance to the question."""
        if len(candidates) <= top_k:
            return candidates

        summaries = []
        for i, c in enumerate(candidates[:20]):  # Max 20 candidates
            summaries.append(f"[{i}] {c.get('title','')} | {c.get('description','')[:200]}")

        try:
            resp = self.chat_client.chat.completions.create(
                model=self.model,
                messages=[{"role": "user", "content": f"""Chọn {top_k} đoạn văn LIÊN QUAN NHẤT để trả lời câu hỏi y khoa.
Trả về JSON: {{"selected": [0, 3, 5, ...]}} (chỉ số của đoạn được chọn, theo thứ tự ưu tiên)

Câu hỏi: {question}

Các đoạn:
""" + "\n".join(summaries)}],
                response_format={"type": "json_object"}
            )
            result = json.loads(resp.choices[0].message.content)
            indices = result.get('selected', list(range(top_k)))
            return [candidates[i] for i in indices if i < len(candidates)][:top_k]
        except Exception:
            return candidates[:top_k]

    # ── Adaptive Retrieval Strategy Map (P2) ──
    # Graph traversal now enabled for all intents via unified retriever.
    # The "graph" flag here controls legacy graph_entity_search only.
    _STRATEGY_MAP = {
        "lookup":           {"graph": True,  "rerank": False, "sub_query_threshold": 3},
        "general":          {"graph": True,  "rerank": False, "sub_query_threshold": 5},
        "dosage":           {"graph": True,  "rerank": False, "sub_query_threshold": 3},
        "procedure":        {"graph": True,  "rerank": True,  "sub_query_threshold": 5},
        "diagnosis":        {"graph": True,  "rerank": True,  "sub_query_threshold": 5},
        "contraindication": {"graph": True,  "rerank": True,  "sub_query_threshold": 3},
        "compare":          {"graph": True,  "rerank": True,  "sub_query_threshold": 3},
    }

    def plan_and_search(self, analysis: dict, raw_question: str) -> tuple:
        """Step 2: Multi-step search dựa trên plan từ preprocess.
        Returns (context_nodes, search_log) for transparency.
        Uses Adaptive Retrieval Strategy based on intent (P2)."""
        search_log = []
        all_context = []
        t0 = time.time()

        intent = analysis.get('intent', 'general')
        strategy = self._STRATEGY_MAP.get(intent, self._STRATEGY_MAP['general'])
        search_log.append({"step": "strategy_select", "intent": intent,
                           "strategy": str(strategy)})

        # Step 1: Resolve disease
        disease = analysis.get('disease_hint')
        if disease:
            disease = self.resolve_disease_name(disease) or disease
        if not disease:
            disease = self.resolve_disease_name(raw_question)
        search_log.append({"step": "disease_routing", "result": disease or "none",
                           "ms": int((time.time()-t0)*1000)})

        # Step 2: Primary search
        t1 = time.time()
        if disease:
            primary = self.scoped_search(raw_question, disease, top_k=10)
            search_log.append({"step": "scoped_search", "disease": disease,
                               "results": len(primary), "ms": int((time.time()-t1)*1000)})
        else:
            primary = self.enhanced_search(raw_question, top_k=10)
            search_log.append({"step": "enhanced_search", "results": len(primary),
                               "ms": int((time.time()-t1)*1000)})
        all_context.extend(primary)

        # Step 3: Sub-query expansion (adaptive threshold)
        sq_threshold = strategy.get('sub_query_threshold', 5)
        if len(all_context) < sq_threshold and analysis.get('sub_queries'):
            seen_ids = {c.get('block_id') for c in all_context}
            for sq in analysis['sub_queries'][:2]:
                t2 = time.time()
                extra = self.scoped_search(sq, disease, top_k=5) if disease \
                        else self.enhanced_search(sq, top_k=5)
                added = 0
                for e in extra:
                    if e.get('block_id') not in seen_ids:
                        all_context.append(e)
                        seen_ids.add(e.get('block_id'))
                        added += 1
                search_log.append({"step": "sub_query", "query": sq[:60],
                                   "added": added, "ms": int((time.time()-t2)*1000)})

        # Step 4: Graph traversal (adaptive — only for intents that need it)
        use_graph = strategy.get('graph', False) or analysis.get('needs_graph_traversal')
        if use_graph and analysis.get('entities'):
            for ent in analysis['entities'][:3]:
                t3 = time.time()
                graph_ctx = self.graph_entity_search(ent['name'], ent.get('type'))
                seen_ids = {c.get('block_id') for c in all_context}
                added = 0
                for g in graph_ctx:
                    if g.get('block_id') not in seen_ids:
                        all_context.append(g)
                        added += 1
                search_log.append({"step": "graph_traversal", "entity": ent['name'],
                                   "added": added, "ms": int((time.time()-t3)*1000)})

        # Step 5: Unified retriever — queries assertions, summaries, entity mentions,
        #         claims insights, experience memory, and graph traversal
        t5 = time.time()
        try:
            unified_results, unified_trace = self.unified.retrieve(
                query=raw_question,
                intent=intent,
                disease_name=disease,
                entities=analysis.get('entities', []),
                top_k=8
            )
            seen_ids = {c.get('block_id') for c in all_context}
            added = 0
            for ur in unified_results:
                ctx = ur.to_context_dict()
                if ctx['block_id'] not in seen_ids:
                    all_context.append(ctx)
                    seen_ids.add(ctx['block_id'])
                    added += 1
            search_log.append({
                "step": "unified_retriever",
                "layers_searched": unified_trace.get("layers_searched", 0),
                "total_from_layers": unified_trace.get("total_results", 0),
                "added": added,
                "ms": int((time.time()-t5)*1000),
                "layer_details": unified_trace.get("layers", [])
            })
        except Exception as e:
            print(f"[plan_and_search:unified] Error: {e}")
            search_log.append({"step": "unified_retriever", "error": str(e),
                               "ms": int((time.time()-t5)*1000)})

        # Step 6: LLM Re-ranking (adaptive — skip for simple lookups to save LLM call)
        use_rerank = strategy.get('rerank', False) or len(all_context) > 12
        if use_rerank and len(all_context) > 8:
            t4 = time.time()
            all_context = self.llm_rerank(raw_question, all_context, top_k=8)
            search_log.append({"step": "llm_rerank", "final_count": len(all_context),
                               "ms": int((time.time()-t4)*1000)})
        elif len(all_context) > 8:
            all_context = all_context[:8]  # Simple truncation for non-rerank strategies

        search_log.append({"step": "total", "ms": int((time.time()-t0)*1000),
                           "final_results": len(all_context)})
        return all_context, search_log

    def reason_and_verify(self, question: str, answer: str, context: list) -> dict:
        """Step 4: Reflection — LLM tự kiểm tra câu trả lời trước khi gửi."""
        ctx_text = "\n".join(
            f"[{i+1}] {c.get('title','')}: {c.get('description','')[:400]}"
            for i, c in enumerate(context[:8])
        )
        try:
            resp = self.chat_client.chat.completions.create(
                model=self.model,
                messages=[{"role": "user", "content": f"""Bạn là chuyên gia kiểm chứng y khoa. Đánh giá câu trả lời:

CÂU HỎI: {question}
CÂU TRẢ LỜI: {answer[:1500]}
PHÁC ĐỒ GỐC:
{ctx_text}

Kiểm tra:
1. Câu trả lời có MÂU THUẪN với phác đồ không?
2. Liều lượng/chống chỉ định có CHÍNH XÁC theo phác đồ không?
3. Có thông tin QUAN TRỌNG trong phác đồ mà câu trả lời BỎ SÓT không?
4. Câu trả lời có suy diễn NGOÀI phác đồ không?

Trả về JSON:
{{
  "is_safe": true/false,
  "confidence": 0.0-1.0,
  "issues": ["vấn đề 1 nếu có"],
  "needs_more_search": true/false,
  "additional_query": "câu hỏi bổ sung nếu cần tìm thêm",
  "correction": "phần cần sửa nếu có, null nếu OK"
}}"""}],
                response_format={"type": "json_object"}
            )
            return json.loads(resp.choices[0].message.content)
        except Exception as e:
            print(f"[reason_and_verify] Error: {e}")
            return {"is_safe": True, "confidence": 0.5, "issues": [],
                    "needs_more_search": False, "additional_query": None, "correction": None}

    def agentic_ask(self, question: str, history: list = None, max_reflect: int = 2) -> dict:
        """Full ReAct loop with 3 Claude Code-inspired enhancements:
          1. KnowledgeInventory — check what data exists BEFORE searching
          2. AdaptiveSearchPlanner — multi-round progressive refinement
          3. QueryMemory — learn from past queries, recall relevant lessons

        Flow: Preprocess → Inventory → Recall → Adaptive Search → Generate → Verify → Save Lesson
        """
        trace = {"question": question, "steps": [], "iterations": 0}
        t_start = time.time()

        # ── Step 1: Preprocess (Understand) ──
        t0 = time.time()
        analysis = self.preprocess_query(question, history)
        intent = analysis.get('intent', 'general')
        entities = analysis.get('entities', [])
        trace["steps"].append({
            "phase": "1. Phan tich cau hoi",
            "detail": f"Intent: {intent} | Disease: {analysis.get('disease_hint','?')} | "
                      f"Entities: {[e['name'] for e in entities]} | "
                      f"Graph: {'Co' if analysis.get('needs_graph_traversal') else 'Khong'} | "
                      f"Verify: {'Co' if analysis.get('needs_verification') else 'Khong'}",
            "ms": int((time.time()-t0)*1000)
        })

        # ── Step 2: Knowledge Inventory — "biết mình biết gì" ──
        t0 = time.time()
        disease = analysis.get('disease_hint')
        if disease:
            disease = self.resolve_disease_name(disease) or disease
        if not disease:
            disease = self.resolve_disease_name(question)

        avail = self.inventory.check_availability(disease_name=disease)
        inventory_text = avail.get("inventory_text", "")
        trace["steps"].append({
            "phase": "2. Kiem tra kho tri thuc",
            "detail": f"Disease data: {'Co' if avail['has_data'] else 'KHONG'} | "
                      f"Coverage: {avail['coverage_score']:.0%} | "
                      f"Strategy: {avail['recommendation']} | "
                      f"Layers: {', '.join(avail['available_layers'])}",
            "ms": int((time.time()-t0)*1000),
            "inventory": avail,
        })

        # ── Step 3: Query Memory Recall — "bài học từ quá khứ" ──
        t0 = time.time()
        past_lessons = self.query_memory.recall(question, disease_name=disease or "", intent=intent)
        lesson_hints = []
        for lesson in past_lessons:
            hint = lesson.to_hint_text()
            if hint:
                lesson_hints.append(hint)
        trace["steps"].append({
            "phase": "3. Nho bai hoc cu",
            "detail": f"Tim thay {len(past_lessons)} bai hoc" +
                      (f" | {lesson_hints[0][:100]}" if lesson_hints else ""),
            "ms": int((time.time()-t0)*1000),
        })

        # ── Step 4: Adaptive Search — progressive multi-round retrieval ──
        t0 = time.time()
        search_result = self.planner.search(
            query=question,
            intent=intent,
            disease_name=disease,
            entities=entities,
            top_k=12,
            max_rounds=3,
        )
        context = search_result.to_context_list()

        # Also run legacy chunk search for backward compatibility
        try:
            legacy_context, legacy_log = self.plan_and_search(analysis, question)
            # Merge: add any legacy results not already in context
            seen_ids = {c.get('block_id') for c in context}
            for lc in legacy_context:
                if lc.get('block_id') not in seen_ids:
                    context.append(lc)
                    seen_ids.add(lc.get('block_id'))
        except Exception:
            legacy_log = []

        trace["steps"].append({
            "phase": "4. Tim kiem thich ung",
            "detail": f"Rounds: {search_result.plan.rounds_executed} | "
                      f"Results: {len(context)} | "
                      f"Sufficient: {'Co' if search_result.sufficient else 'Chua du'} | "
                      f"Strategy: {search_result.plan.strategy} | "
                      f"Refinements: {', '.join(search_result.plan.refinements) or 'none'}",
            "ms": int((time.time()-t0)*1000),
            "search_trace": search_result.trace,
            "coverage_gaps": search_result.coverage_gaps,
        })

        # ── Step 4b: Inject inventory awareness into context ──
        # Tell the LLM what Pathway knows/doesn't know
        if inventory_text and not search_result.sufficient:
            # Add inventory as a special context item
            context.insert(0, {
                "title": "[Pathway Knowledge Inventory]",
                "description": inventory_text,
                "block_id": "inventory_awareness",
                "score": 1.0,
                "source": "inventory",
            })

        # ── ReAct Loop: Generate → Verify → (Re-search if needed) ──
        final_answer = None
        verification = None

        for iteration in range(max_reflect + 1):
            trace["iterations"] = iteration + 1

            # Generate answer
            t1 = time.time()
            from academic_agent import AcademicAgent
            if isinstance(self, AcademicAgent):
                answer = self.generate_academic_response(question, context_nodes=context)
            else:
                answer = self.generate_response(question, override_context=context)
            trace["steps"].append({
                "phase": f"5. Sinh cau tra loi (lan {iteration+1})",
                "detail": f"Dua tren {len(context)} nguon | {len(answer)} ky tu",
                "ms": int((time.time()-t1)*1000)
            })

            # Verify (only if analysis says it needs verification, or first iteration)
            if iteration < max_reflect and (analysis.get('needs_verification') or iteration == 0):
                t2 = time.time()
                verification = self.reason_and_verify(question, answer, context)
                v_detail = f"Confidence: {verification.get('confidence',0):.0%} | " \
                           f"Safe: {'V' if verification.get('is_safe') else 'X'}"
                if verification.get('issues'):
                    v_detail += f" | Issues: {', '.join(verification['issues'][:2])}"
                trace["steps"].append({
                    "phase": f"6. Kiem chung (lan {iteration+1})",
                    "detail": v_detail,
                    "ms": int((time.time()-t2)*1000),
                    "verification": verification
                })

                # If safe and confident → done
                if verification.get('is_safe') and verification.get('confidence', 0) >= 0.7:
                    final_answer = answer
                    break

                # If needs more search → re-search and loop
                if verification.get('needs_more_search') and verification.get('additional_query'):
                    t3 = time.time()
                    extra_q = verification['additional_query']
                    extra = self.scoped_search(extra_q, disease, top_k=5) if disease \
                            else self.enhanced_search(extra_q, top_k=5)
                    seen_ids = {c.get('block_id') for c in context}
                    added = 0
                    for e in extra:
                        if e.get('block_id') not in seen_ids:
                            context.append(e)
                            added += 1
                    trace["steps"].append({
                        "phase": f"🔄 Tìm bổ sung (lần {iteration+1})",
                        "detail": f"Query: '{extra_q[:60]}' → +{added} nguồn mới",
                        "ms": int((time.time()-t3)*1000)
                    })

                # If has correction → apply it
                if verification.get('correction'):
                    answer = answer + "\n\n**⚠️ Bổ sung kiểm chứng:** " + verification['correction']
                    final_answer = answer
                    break
            else:
                final_answer = answer
                break

        if final_answer is None:
            final_answer = answer

        trace["total_ms"] = int((time.time()-t_start)*1000)
        confidence = verification.get('confidence', 0.5) if verification else 0.5
        trace["final_confidence"] = confidence

        # ── Confidence-gated Response (P1: an toàn y khoa) ──
        if confidence < 0.5:
            final_answer = (
                "⚠️ **Không đủ thông tin để trả lời chính xác.**\n\n"
                "Hệ thống đánh giá mức độ tin cậy thấp "
                f"({confidence:.0%}) cho câu trả lời này. "
                "Các phác đồ hiện có không chứa đủ thông tin để đảm bảo "
                "tính chính xác. Đề nghị:\n"
                "- Tham khảo trực tiếp phác đồ gốc\n"
                "- Hỏi ý kiến bác sĩ chuyên khoa\n\n"
                "---\n*Nội dung tham khảo (chưa kiểm chứng đầy đủ):*\n\n"
                + final_answer
            )
        elif confidence < 0.8:
            final_answer += (
                f"\n\n---\n⚠️ *Mức độ tin cậy: trung bình ({confidence:.0%}). "
                "Đề nghị đối chiếu với phác đồ gốc trước khi áp dụng.*"
            )

        # ── Step 7: Save lesson to QueryMemory ──
        try:
            effective_layers = list({
                c.get('source', 'chunk') for c in context
                if c.get('block_id') != 'inventory_awareness'
            })
            empty_layers = [
                layer["layer"] for layer in search_result.trace
                if isinstance(layer, dict) and layer.get("count") == 0
            ] if search_result.trace else []

            self.query_memory.save_lesson(
                query=question,
                disease_name=disease or "",
                intent=intent,
                confidence=confidence,
                was_sufficient=search_result.sufficient,
                effective_layers=effective_layers,
                empty_layers=empty_layers,
                refinements_used=search_result.plan.refinements,
                coverage_gaps=search_result.coverage_gaps,
            )
        except Exception as e:
            print(f"[agentic_ask:save_lesson] {e}")

        return {
            "answer": final_answer,
            "context": context,
            "disease_detected": disease,
            "trace": trace,
            "verification": verification,
            "knowledge_inventory": avail,
            "search_plan": {
                "strategy": search_result.plan.strategy,
                "rounds": search_result.plan.rounds_executed,
                "sufficient": search_result.sufficient,
                "coverage_gaps": search_result.coverage_gaps,
                "refinements": search_result.plan.refinements,
            },
            "past_lessons": len(past_lessons),
        }

    def generate_response(self, user_query, override_context=None):
        context_data = override_context if override_context is not None else self.hybrid_search(user_query)

        if not context_data:
            return (
                "Chưa có đủ tri thức trong Pathway để trả lời chắc chắn. "
                "Hệ thống chưa truy được đoạn protocol hoặc evidence graph phù hợp, nên không nên kết luận lúc này."
            )

        # Expand context with adjacent chunks (P0: Context Window Expansion)
        context_data = [_expand_context(item) for item in context_data]

        # Build prompt
        context_str = "Dưới đây là các thông tin phác đồ lâm sàng được tìm thấy trong hệ thống:\n\n"
        for i, item in enumerate(context_data):
            country_info = f" (Nguồn: {item['country']})" if item.get('country') else ""
            context_str += f"[{i+1}] {item['title']}{country_info}\n"
            # Use expanded description (includes prev/next context) if available
            content = item.get('expanded_description') or item.get('description', '')
            context_str += f"Chi tiết: {content}\n"
            if item.get('context_links'):
                links = ", ".join([link['neighbor_title'] for link in item['context_links'][:5] if link['neighbor_title']])
                context_str += f"Các liên kết liên quan: {links}\n"
            context_str += "---\n"

        system_prompt = """Bạn là một Trợ lý AI Y khoa chuyên nghiệp bên trong Pathway.
Nhiệm vụ của bạn là hỗ trợ bác sĩ nhưng CHỈ được kết luận từ các nguồn đã được cung cấp trong ngữ cảnh.
Hãy trả lời chính xác, thận trọng, và chỉ ra rõ ý nào có bằng chứng.
Nếu thông tin không có trong ngữ cảnh hoặc evidence chưa đủ mạnh, phải nói rõ là chưa đủ bằng chứng trong Pathway và không được suy diễn thành kết luận chắc chắn.
Nếu phù hợp, nêu ngắn gọn Pathway nên truy thêm chunk/assertion/ontology nào."""

        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": f"Ngữ cảnh:\n{context_str}\n\nCâu hỏi: {user_query}"}
        ]

        response = self.chat_client.chat.completions.create(
            model=self.model,
            messages=messages,
        )
        
        return response.choices[0].message.content

    def _query_ontology_v2_context(self, disease_name: str, namespace: str = "ontology_v2") -> tuple[list[dict], list[str], list[dict]]:
        """
        Query Ontology V2 graph for RawChunk + ProtocolAssertion context.
        Returns (context_items, graph_node_ids, reasoning_trace) where:
        - graph_node_ids use the prefixed format matching the frontend graph
        - reasoning_trace is a list of step dicts for live UI rendering
        """
        import time
        context_items = []
        graph_node_ids = []
        reasoning_trace = []

        disease_node_id = f"disease:{disease_name}"

        with self.driver.session() as session:
            # Step 1: Resolve DiseaseEntity
            t0 = time.time()
            disease_row = session.run(
                """
                MATCH (d:DiseaseEntity {namespace:$ns})
                WHERE toLower(d.disease_name) CONTAINS toLower($disease)
                   OR toLower(d.disease_id) CONTAINS toLower($disease)
                RETURN d.disease_id AS disease_id, d.disease_name AS disease_name
                LIMIT 1
                """,
                ns=namespace,
                disease=disease_name,
            ).single()
            dt = round((time.time() - t0) * 1000, 1)

            if disease_row:
                disease_node_id = f"disease:{disease_row['disease_id']}"
                reasoning_trace.append({
                    "phase": "disease_resolve",
                    "action": f"Xác định bệnh: {disease_row['disease_name']}",
                    "node_ids": [disease_node_id],
                    "edge_keys": [],
                    "details": {"disease_id": disease_row["disease_id"]},
                    "duration_ms": dt,
                })
            else:
                reasoning_trace.append({
                    "phase": "disease_resolve",
                    "action": f"Không tìm thấy DiseaseEntity cho '{disease_name}'",
                    "node_ids": [],
                    "edge_keys": [],
                    "duration_ms": dt,
                })
                return context_items, graph_node_ids, reasoning_trace

            # Step 2: Traverse CHUNK_ABOUT_DISEASE edges → RawChunk nodes
            t0 = time.time()
            chunk_rows = session.run(
                """
                MATCH (c:RawChunk {namespace:$ns})-[:CHUNK_ABOUT_DISEASE]->(d:DiseaseEntity)
                WHERE toLower(d.disease_name) CONTAINS toLower($disease)
                   OR toLower(d.disease_id) CONTAINS toLower($disease)
                RETURN c.chunk_id AS chunk_id,
                       c.section_title AS section_title,
                       c.section_type AS section_type,
                       c.body_preview AS body_preview,
                       c.parent_section_path AS parent_section_path
                ORDER BY c.section_title
                LIMIT 30
                """,
                ns=namespace,
                disease=disease_name,
            ).data()
            dt = round((time.time() - t0) * 1000, 1)

            chunk_node_ids = [f"chunk:{r['chunk_id']}" for r in chunk_rows]
            chunk_edge_keys = [f"chunk:{r['chunk_id']}→{disease_node_id}" for r in chunk_rows]

            for row in chunk_rows:
                context_items.append({
                    "title": row.get("section_title") or row.get("chunk_id"),
                    "content": row.get("body_preview") or "",
                    "section_type": row.get("section_type"),
                    "source": "RawChunk",
                })
                graph_node_ids.append(f"chunk:{row['chunk_id']}")

            reasoning_trace.append({
                "phase": "chunk_traverse",
                "action": f"Duyệt CHUNK_ABOUT_DISEASE → {len(chunk_rows)} RawChunk nodes",
                "node_ids": chunk_node_ids,
                "edge_keys": chunk_edge_keys,
                "details": {
                    "sections": [r.get("section_title", "?") for r in chunk_rows[:8]],
                    "total": len(chunk_rows),
                },
                "duration_ms": dt,
            })

            # Step 3: Traverse CONTAINS_ASSERTION edges → ProtocolAssertion nodes
            t0 = time.time()
            assertion_rows = session.run(
                """
                MATCH (a:ProtocolAssertion {namespace:$ns})-[:ASSERTION_ABOUT_DISEASE]->(d:DiseaseEntity)
                WHERE toLower(d.disease_name) CONTAINS toLower($disease)
                   OR toLower(d.disease_id) CONTAINS toLower($disease)
                OPTIONAL MATCH (src:RawChunk {chunk_id: a.source_chunk_id})
                RETURN a.assertion_id AS assertion_id,
                       a.assertion_type AS assertion_type,
                       a.assertion_text AS assertion_text,
                       a.condition_text AS condition_text,
                       a.action_text AS action_text,
                       a.source_chunk_id AS source_chunk_id
                LIMIT 20
                """,
                ns=namespace,
                disease=disease_name,
            ).data()
            dt = round((time.time() - t0) * 1000, 1)

            assertion_node_ids = [f"assertion:{r['assertion_id']}" for r in assertion_rows]
            assertion_edge_keys = []
            for row in assertion_rows:
                assertion_edge_keys.append(f"assertion:{row['assertion_id']}→{disease_node_id}")
                if row.get("source_chunk_id"):
                    assertion_edge_keys.append(f"chunk:{row['source_chunk_id']}→assertion:{row['assertion_id']}")

            for row in assertion_rows:
                parts = []
                if row.get("condition_text"):
                    parts.append(f"Điều kiện: {row['condition_text']}")
                if row.get("action_text"):
                    parts.append(f"Hành động: {row['action_text']}")
                if row.get("assertion_text"):
                    parts.append(row["assertion_text"])
                context_items.append({
                    "title": f"[{row.get('assertion_type', 'rule')}] Assertion",
                    "content": " | ".join(parts),
                    "source": "ProtocolAssertion",
                })
                graph_node_ids.append(f"assertion:{row['assertion_id']}")

            reasoning_trace.append({
                "phase": "assertion_traverse",
                "action": f"Duyệt ASSERTION_ABOUT_DISEASE → {len(assertion_rows)} ProtocolAssertion rules",
                "node_ids": assertion_node_ids,
                "edge_keys": assertion_edge_keys,
                "details": {
                    "types": [r.get("assertion_type", "?") for r in assertion_rows[:8]],
                    "total": len(assertion_rows),
                },
                "duration_ms": dt,
            })

            # Step 4: Traverse MENTIONS_SERVICE edges → RawServiceMention nodes
            t0 = time.time()
            service_rows = session.run(
                """
                MATCH (c:RawChunk {namespace:$ns})-[:CHUNK_ABOUT_DISEASE]->(d:DiseaseEntity)
                WHERE toLower(d.disease_name) CONTAINS toLower($disease)
                   OR toLower(d.disease_id) CONTAINS toLower($disease)
                MATCH (c)-[:MENTIONS_SERVICE]->(m:RawServiceMention {namespace:$ns})
                OPTIONAL MATCH (m)-[:MAPS_TO_SERVICE]->(svc)
                RETURN m.mention_id AS mention_id,
                       m.mention_text AS mention_text,
                       m.medical_role AS medical_role,
                       m.context_text AS context_text,
                       coalesce(svc.service_name, svc.name) AS canonical_service,
                       svc.service_code AS service_code,
                       m.source_chunk_id AS source_chunk_id
                LIMIT 30
                """,
                ns=namespace,
                disease=disease_name,
            ).data()
            dt = round((time.time() - t0) * 1000, 1)

            svc_node_ids = [f"servicemention:{r['mention_id']}" for r in service_rows]
            svc_edge_keys = []
            for row in service_rows:
                if row.get("source_chunk_id"):
                    svc_edge_keys.append(f"chunk:{row['source_chunk_id']}→servicemention:{row['mention_id']}")
                    graph_node_ids.append(f"chunk:{row['source_chunk_id']}")

                context_items.append({
                    "title": f"Dịch vụ: {row.get('mention_text', '')}",
                    "content": f"Vai trò: {row.get('medical_role', '?')} | Ngữ cảnh: {row.get('context_text', '')}",
                    "source": "ServiceMention",
                    "canonical_service": row.get("canonical_service"),
                })
                graph_node_ids.append(f"servicemention:{row['mention_id']}")

            reasoning_trace.append({
                "phase": "service_traverse",
                "action": f"Duyệt MENTIONS_SERVICE → {len(service_rows)} dịch vụ y tế",
                "node_ids": svc_node_ids,
                "edge_keys": svc_edge_keys,
                "details": {
                    "services": [r.get("mention_text", "?") for r in service_rows[:8]],
                    "mapped": sum(1 for r in service_rows if r.get("canonical_service")),
                    "total": len(service_rows),
                },
                "duration_ms": dt,
            })

            # Step 5: Traverse MENTIONS_SIGN edges → RawSignMention
            t0 = time.time()
            sign_rows = session.run(
                """
                MATCH (c:RawChunk {namespace:$ns})-[:CHUNK_ABOUT_DISEASE]->(d:DiseaseEntity)
                WHERE toLower(d.disease_name) CONTAINS toLower($disease)
                   OR toLower(d.disease_id) CONTAINS toLower($disease)
                MATCH (c)-[:MENTIONS_SIGN]->(m:RawSignMention {namespace:$ns})
                OPTIONAL MATCH (m)-[:MAPS_TO_SIGN]->(sc)
                RETURN m.mention_id AS mention_id,
                       m.mention_text AS mention_text,
                       coalesce(sc.canonical_label, sc.text, sc.sign_id, sc.claim_sign_id) AS concept_label,
                       m.source_chunk_id AS source_chunk_id
                LIMIT 20
                """,
                ns=namespace,
                disease=disease_name,
            ).data()
            dt = round((time.time() - t0) * 1000, 1)

            sign_node_ids = [f"signmention:{r['mention_id']}" for r in sign_rows]
            sign_edge_keys = []
            for row in sign_rows:
                if row.get("source_chunk_id"):
                    sign_edge_keys.append(f"chunk:{row['source_chunk_id']}→signmention:{row['mention_id']}")

            reasoning_trace.append({
                "phase": "sign_traverse",
                "action": f"Duyệt MENTIONS_SIGN → {len(sign_rows)} triệu chứng/dấu hiệu",
                "node_ids": sign_node_ids,
                "edge_keys": sign_edge_keys,
                "details": {
                    "signs": [r.get("mention_text", "?") for r in sign_rows[:8]],
                    "total": len(sign_rows),
                },
                "duration_ms": dt,
            })

        return context_items, list(dict.fromkeys(graph_node_ids)), reasoning_trace

    def adjudicate_claim(self, claim_text: str, disease_name: str = None) -> dict:
        """
        Adjudicate a medical claim. Returns detailed reasoning trace
        showing every graph traversal step for live UI rendering.
        """
        import json
        import time
        t0_total = time.time()

        context_items = []
        graph_node_ids = []
        reasoning_trace = []

        # Phase 1: Query Ontology V2 graph (with trace)
        if disease_name:
            try:
                context_items, graph_node_ids, reasoning_trace = self._query_ontology_v2_context(disease_name)
            except Exception as e:
                print(f"Ontology V2 query failed, falling back to legacy: {e}")
                reasoning_trace.append({
                    "phase": "error",
                    "action": f"Ontology V2 lỗi: {e}. Chuyển sang legacy search.",
                    "node_ids": [], "edge_keys": [],
                })

        # Phase 2: Fallback if no Ontology V2 data
        if not context_items:
            t0 = time.time()
            search_query = f"{disease_name} {claim_text[:100]}" if disease_name else claim_text
            legacy_context = (
                self.scoped_search(search_query, disease_name, top_k=15)
                if disease_name
                else self.enhanced_search(search_query, top_k=15)
            )
            for c in legacy_context:
                context_items.append({
                    "title": c.get("title", "N/A"),
                    "content": c.get("expanded_description") or c.get("description", ""),
                    "source": "LegacyChunk",
                })
                if c.get("block_id"):
                    graph_node_ids.append(c["block_id"])
            dt = round((time.time() - t0) * 1000, 1)
            reasoning_trace.append({
                "phase": "vector_search",
                "action": f"Vector+Fulltext search (fallback) → {len(legacy_context)} kết quả",
                "node_ids": [c.get("block_id") for c in legacy_context if c.get("block_id")],
                "edge_keys": [],
                "details": {"titles": [c.get("title", "?") for c in legacy_context[:5]]},
                "duration_ms": dt,
            })

        # Phase 3: Context selection summary
        reasoning_trace.append({
            "phase": "context_select",
            "action": f"Tổng hợp {len(context_items)} nguồn tri thức cho LLM thẩm định",
            "node_ids": graph_node_ids[:20],
            "edge_keys": [],
            "details": {
                "by_type": {
                    "RawChunk": sum(1 for c in context_items if c.get("source") == "RawChunk"),
                    "Assertion": sum(1 for c in context_items if c.get("source") == "ProtocolAssertion"),
                    "Service": sum(1 for c in context_items if c.get("source") == "ServiceMention"),
                },
            },
        })

        # Phase 4: Format context for LLM
        context_str = "Danh sách các kiến thức Y khoa (Phác đồ/Chỉ định) từ Hệ thống:\n"
        for i, item in enumerate(context_items):
            context_str += f"[{i+1}] {item.get('title', 'N/A')}\n{item.get('content', '')}\n---\n"

        system_prompt = """Bạn là một Chuyên gia Thẩm định Bồi thường Y tế ảo (Medical Claim Adjudicator AI).
Dựa vào 'Hồ sơ Claim' (danh sách dịch vụ yêu cầu) và 'Ngữ cảnh' (Phác đồ, hướng dẫn điều trị từ Hệ thống), hãy thẩm định xem từng dịch vụ có được chấp thuận chi trả hay không.
Trả về dữ liệu dưới định dạng JSON duy nhất gồm:
{
  "summary": "Đoạn văn tóm tắt kết quả thẩm định chung.",
  "items": [
    {
      "service_name": "Tên dịch vụ gốc trong hồ sơ",
      "status": "Approved | Denied | Need Review",
      "reason": "Lý do chấp thuận hoặc từ chối, trích dẫn quy định/phác đồ nếu có."
    }
  ]
}
Luật:
- Approved: Có trong phác đồ/Hợp lý về mặt y khoa.
- Denied: Chống chỉ định hoặc hoàn toàn không liên quan/không cần thiết.
- Need Review: Mơ hồ, cần bác sĩ con người xem xét thêm.
KHÔNG TRẢ VỀ BẤT KỲ VĂN BẢN NÀO NGOÀI JSON."""

        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": f"=== NGỮ CẢNH TỪ HỆ THỐNG ===\n{context_str}\n\n=== HỒ SƠ CLAIM ===\n{claim_text}\n\nBệnh lý chẩn đoán: {disease_name or 'Không rõ'}\n\nHãy thẩm định và trả về JSON chuẩn."}
        ]

        # Phase 5: LLM adjudication
        t0 = time.time()
        try:
            response = self.chat_client.chat.completions.create(
                model=self.model,
                messages=messages,
                response_format={"type": "json_object"}
            )
            raw_content = response.choices[0].message.content
            data = json.loads(raw_content)
        except Exception as e:
            print(f"Error in adjudicate_claim LLM step: {e}")
            data = {
                "summary": f"Lỗi trong quá trình thẩm định: {str(e)}",
                "items": []
            }
        dt = round((time.time() - t0) * 1000, 1)

        items_summary = [
            f"{it.get('service_name','?')}: {it.get('status','?')}"
            for it in data.get("items", [])
        ]
        reasoning_trace.append({
            "phase": "llm_adjudicate",
            "action": f"LLM thẩm định {len(data.get('items', []))} dịch vụ",
            "node_ids": [],
            "edge_keys": [],
            "details": {"results": items_summary},
            "duration_ms": dt,
        })

        data["context_nodes"] = [{"id": nid} for nid in graph_node_ids]
        data["reasoning_trace"] = reasoning_trace
        data["_timing_s"] = round(time.time() - t0_total, 2)
        return data

if __name__ == "__main__":
    agent = MedicalAgent()
    try:
        # Example queries to test
        queries = [
            "Hướng dẫn chẩn đoán và điều trị tăng huyết áp của Bộ Y tế Việt Nam có những lưu ý gì?",
            "Quy trình xử trí người nhiễm COVID-19 tại nhà theo Bộ Y tế Việt Nam?"
        ]
        
        for q in queries:
            print(f"\nQUERY: {q}")
            answer = agent.generate_response(q)
            print(f"ANSWER:\n{answer}")
            print("-" * 50)
    finally:
        agent.close()
