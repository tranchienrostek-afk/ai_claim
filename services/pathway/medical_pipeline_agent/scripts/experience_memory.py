"""
Experience Memory — Persistent learning system for the Medical Pipeline Agent.

Stores pipeline experiences (ontology templates, run history, optimization lessons,
system prompts) in Neo4j with vector embeddings for RAG retrieval.

The system learns from every pipeline run:
  - Before a run: queries past experience for similar diseases/domains
  - After a run: saves all experience (what worked, what failed, how it was fixed)

Neo4j Node Types:
  :Experience — Unified vector-searchable entry point (has embedding)
  :OntologyTemplate — Successful ontology configs per disease
  :PipelineRunLog — Execution history (config, accuracy, duration)
  :OptimizationLesson — Error patterns and fixes
  :SystemPromptVersion — Prompt evolution with accuracy tracking
"""

import os
import sys
import json
import hashlib
import uuid
import time
from pathlib import Path
from datetime import datetime

from dotenv import load_dotenv
from openai import AzureOpenAI
from neo4j import GraphDatabase

SCRIPT_DIR = Path(__file__).parent
NOTEBOOKLM_DIR = SCRIPT_DIR.parent.parent
load_dotenv(NOTEBOOKLM_DIR / '.env')


class ExperienceMemory:
    """Store and retrieve pipeline experiences from Neo4j with vector embeddings."""

    def __init__(self):
        # Neo4j
        self.driver = GraphDatabase.driver(
            os.getenv("NEO4J_URI", "bolt://localhost:7688"),
            auth=(os.getenv("NEO4J_USER", "neo4j"), os.getenv("NEO4J_PASSWORD", "password123"))
        )
        # Azure OpenAI Embeddings
        self.embedding_client = AzureOpenAI(
            azure_endpoint=os.getenv("AZURE_EMBEDDINGS_ENDPOINT", "").strip(),
            api_key=os.getenv("AZURE_EMBEDDINGS_API_KEY", "").strip(),
            api_version=os.getenv("AZURE_OPENAI_API_VERSION", "").strip()
        )
        # Azure OpenAI Chat (for generating recommendations)
        self.chat_client = AzureOpenAI(
            azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT", "").strip(),
            api_key=os.getenv("AZURE_OPENAI_API_KEY", "").strip(),
            api_version=os.getenv("AZURE_OPENAI_API_VERSION", "").strip()
        )
        self.model = os.getenv("MODEL2", "gpt-5-mini").strip()
        self.embedding_model = "text-embedding-ada-002"

        self.ensure_indexes()

    def close(self):
        self.driver.close()

    # ─── Indexes ───

    def ensure_indexes(self):
        """Create vector and fulltext indexes for experience nodes."""
        with self.driver.session() as s:
            # Vector index
            try:
                s.run("""
                    CREATE VECTOR INDEX `experience_vector_index` IF NOT EXISTS
                    FOR (n:Experience) ON (n.embedding)
                    OPTIONS {indexConfig: {
                        `vector.dimensions`: 1536,
                        `vector.similarity_function`: 'cosine'
                    }}
                """)
            except Exception:
                pass  # Index may already exist

            # Fulltext index
            try:
                s.run("""
                    CREATE FULLTEXT INDEX `experience_fulltext` IF NOT EXISTS
                    FOR (n:Experience) ON EACH [n.search_text]
                """)
            except Exception:
                pass

            # Uniqueness constraints
            try:
                s.run("CREATE CONSTRAINT exp_id IF NOT EXISTS FOR (n:Experience) REQUIRE n.experience_id IS UNIQUE")
            except Exception:
                pass

    # ─── Embedding ───

    def _embed(self, text: str) -> list:
        """Generate 1536-dim embedding. Truncates to 8000 chars."""
        text = text[:8000]
        try:
            return self.embedding_client.embeddings.create(
                input=[text], model=self.embedding_model
            ).data[0].embedding
        except Exception as e:
            print(f"  [MEMORY] Embedding error: {e}")
            return [0.0] * 1536

    def _build_search_text(self, **kwargs) -> str:
        """Concatenate relevant fields into searchable string."""
        parts = []
        for k, v in kwargs.items():
            if v and v not in ('None', 'null', ''):
                if isinstance(v, (list, dict)):
                    v = json.dumps(v, ensure_ascii=False)
                parts.append(f"{k}: {v}")
        return " | ".join(parts)

    # ─── Save Methods ───

    def save_ontology_template(self, disease_name: str, medical_domain: str,
                                icd_code: str, entity_types: list,
                                ontology_json: str, extraction_prompts: dict,
                                accuracy_score: float, chunk_count: int,
                                run_id: str) -> str:
        """Save a successful ontology template to Neo4j."""
        template_id = str(uuid.uuid4())[:12]
        search_text = self._build_search_text(
            disease=disease_name, domain=medical_domain, icd=icd_code,
            entities=entity_types, accuracy=f"{accuracy_score}%"
        )
        embedding = self._embed(search_text)

        with self.driver.session() as s:
            s.run("""
                CREATE (e:Experience {
                    experience_id: $eid,
                    type: 'ontology_template',
                    search_text: $search_text,
                    embedding: $embedding,
                    created_at: $ts
                })
                CREATE (t:OntologyTemplate {
                    template_id: $tid,
                    disease_name: $disease,
                    medical_domain: $domain,
                    icd_code: $icd,
                    entity_types_json: $entity_types,
                    ontology_json: $ontology,
                    extraction_system_prompt: $sys_prompt,
                    extraction_user_template: $user_tmpl,
                    accuracy_score: $acc,
                    chunk_count: $chunks,
                    run_id: $run_id,
                    created_at: $ts
                })
                CREATE (e)-[:HAS_TEMPLATE]->(t)
            """, eid=f"exp_{template_id}", tid=template_id,
                search_text=search_text, embedding=embedding,
                disease=disease_name, domain=medical_domain, icd=icd_code,
                entity_types=json.dumps(entity_types, ensure_ascii=False),
                ontology=ontology_json,
                sys_prompt=extraction_prompts.get('system', ''),
                user_tmpl=extraction_prompts.get('user_template', ''),
                acc=accuracy_score, chunks=chunk_count,
                run_id=run_id, ts=datetime.now().isoformat())

        print(f"  [MEMORY] Saved ontology template: {disease_name} ({medical_domain}) acc={accuracy_score}%")
        return template_id

    def save_pipeline_run(self, run_summary: dict, config: dict,
                          test_report: dict) -> str:
        """Save pipeline execution record."""
        run_id = str(uuid.uuid4())[:12]
        search_text = self._build_search_text(
            pdf=run_summary.get('pdf_name', ''),
            classification=run_summary.get('classification', ''),
            strategy=run_summary.get('strategy', ''),
            diseases=config.get('diseases_to_process', []),
            accuracy=f"{run_summary.get('test_accuracy_pct', 0)}%",
            chunks=run_summary.get('total_chunks', 0),
            passed=run_summary.get('test_passed', False),
        )
        embedding = self._embed(search_text)

        # Extract failure analysis if available
        failure_analysis = json.dumps(
            test_report.get('failure_analysis', {}), ensure_ascii=False
        )

        with self.driver.session() as s:
            s.run("""
                CREATE (e:Experience {
                    experience_id: $eid,
                    type: 'pipeline_run',
                    search_text: $search_text,
                    embedding: $embedding,
                    created_at: $ts
                })
                CREATE (r:PipelineRunLog {
                    run_id: $rid,
                    pdf_name: $pdf,
                    classification: $classification,
                    strategy: $strategy,
                    diseases_ingested: $diseases,
                    total_chunks: $chunks,
                    accuracy_pct: $acc,
                    target_accuracy_pct: $target,
                    passed: $passed,
                    optimization_iterations: $opt_iters,
                    duration_seconds: $duration,
                    failure_analysis: $failures,
                    config_json: $config_json,
                    created_at: $ts
                })
                CREATE (e)-[:HAS_RUN]->(r)
            """, eid=f"exp_{run_id}", rid=run_id,
                search_text=search_text, embedding=embedding,
                pdf=run_summary.get('pdf_name', ''),
                classification=run_summary.get('classification', ''),
                strategy=run_summary.get('strategy', ''),
                diseases=json.dumps(config.get('diseases_to_process', []), ensure_ascii=False),
                chunks=run_summary.get('total_chunks', 0),
                acc=run_summary.get('test_accuracy_pct', 0),
                target=run_summary.get('test_accuracy_pct', 85),
                passed=run_summary.get('test_passed', False),
                opt_iters=run_summary.get('optimization_iterations', 0),
                duration=run_summary.get('total_duration_seconds', 0),
                failures=failure_analysis,
                config_json=json.dumps(config, ensure_ascii=False)[:5000],
                ts=datetime.now().isoformat())

        print(f"  [MEMORY] Saved pipeline run: {run_summary.get('pdf_name', '?')} acc={run_summary.get('test_accuracy_pct', 0)}%")
        return run_id

    def save_optimization_lesson(self, opt_log: dict, disease_name: str,
                                  medical_domain: str, run_id: str) -> str:
        """Save what went wrong and what fixed it."""
        lesson_id = str(uuid.uuid4())[:12]

        strategies = opt_log.get('fixes_applied', [])
        strategy_names = [s.get('strategy', '') for s in strategies]
        analysis = opt_log.get('analysis', '')

        search_text = self._build_search_text(
            disease=disease_name, domain=medical_domain,
            error_analysis=analysis,
            strategies=strategy_names,
            accuracy_before=opt_log.get('accuracy_before', 0),
            iteration=opt_log.get('iteration', 0),
        )
        embedding = self._embed(search_text)

        with self.driver.session() as s:
            s.run("""
                CREATE (e:Experience {
                    experience_id: $eid,
                    type: 'optimization_lesson',
                    search_text: $search_text,
                    embedding: $embedding,
                    created_at: $ts
                })
                CREATE (l:OptimizationLesson {
                    lesson_id: $lid,
                    disease_name: $disease,
                    medical_domain: $domain,
                    error_analysis: $analysis,
                    strategies_applied: $strategies,
                    accuracy_before: $acc_before,
                    iteration: $iteration,
                    fixes_json: $fixes,
                    created_at: $ts
                })
                CREATE (e)-[:HAS_LESSON]->(l)
            """, eid=f"exp_{lesson_id}", lid=lesson_id,
                search_text=search_text, embedding=embedding,
                disease=disease_name, domain=medical_domain,
                analysis=analysis[:2000],
                strategies=json.dumps(strategy_names, ensure_ascii=False),
                acc_before=opt_log.get('accuracy_before', 0),
                iteration=opt_log.get('iteration', 0),
                fixes=json.dumps(strategies, ensure_ascii=False)[:3000],
                ts=datetime.now().isoformat())

        print(f"  [MEMORY] Saved optimization lesson: {disease_name} iter={opt_log.get('iteration', '?')}")
        return lesson_id

    def save_system_prompt(self, prompt_text: str, accuracy_score: float,
                           disease_name: str, medical_domain: str,
                           iteration: int, is_best: bool, run_id: str) -> str:
        """Save a system prompt version with its accuracy. Dedup by hash."""
        prompt_hash = hashlib.sha256(prompt_text.encode()).hexdigest()[:16]

        # Check if this exact prompt already exists
        with self.driver.session() as s:
            existing = s.run(
                "MATCH (p:SystemPromptVersion {prompt_hash: $h}) RETURN p.prompt_id AS id",
                h=prompt_hash
            ).single()
            if existing:
                # Update accuracy if better
                s.run("""
                    MATCH (p:SystemPromptVersion {prompt_hash: $h})
                    WHERE p.accuracy_score < $acc
                    SET p.accuracy_score = $acc, p.is_best = $best
                """, h=prompt_hash, acc=accuracy_score, best=is_best)
                return existing['id']

        prompt_id = str(uuid.uuid4())[:12]
        search_text = self._build_search_text(
            disease=disease_name, domain=medical_domain,
            accuracy=f"{accuracy_score}%", is_best=str(is_best),
            prompt_preview=prompt_text[:300]
        )
        embedding = self._embed(search_text)

        with self.driver.session() as s:
            s.run("""
                CREATE (e:Experience {
                    experience_id: $eid,
                    type: 'system_prompt',
                    search_text: $search_text,
                    embedding: $embedding,
                    created_at: $ts
                })
                CREATE (p:SystemPromptVersion {
                    prompt_id: $pid,
                    prompt_text: $prompt,
                    prompt_hash: $hash,
                    accuracy_score: $acc,
                    disease_name: $disease,
                    medical_domain: $domain,
                    iteration: $iter,
                    is_best: $best,
                    run_id: $run_id,
                    created_at: $ts
                })
                CREATE (e)-[:HAS_PROMPT]->(p)
            """, eid=f"exp_{prompt_id}", pid=prompt_id,
                search_text=search_text, embedding=embedding,
                prompt=prompt_text[:5000], hash=prompt_hash,
                acc=accuracy_score, disease=disease_name,
                domain=medical_domain, iter=iteration,
                best=is_best, run_id=run_id,
                ts=datetime.now().isoformat())

        print(f"  [MEMORY] Saved system prompt: {disease_name} acc={accuracy_score}% best={is_best}")
        return prompt_id

    def save_after_run(self, run_summary: dict, config: dict,
                       test_report: dict, optimization_logs: list,
                       ingest_config: dict = None,
                       system_prompt_text: str = None) -> dict:
        """Convenience: save all experience from a completed pipeline run."""
        print(f"\n  [MEMORY] === Saving Experience ===")
        ids = {}

        # 1. Save pipeline run
        run_id = self.save_pipeline_run(run_summary, config, test_report)
        ids['run_id'] = run_id

        # 2. Save ontology template (if accuracy >= 70%)
        accuracy = run_summary.get('test_accuracy_pct', 0)
        if ingest_config and accuracy >= 70:
            template_id = self.save_ontology_template(
                disease_name=ingest_config.get('disease_name', 'unknown'),
                medical_domain=ingest_config.get('medical_domain', 'General'),
                icd_code=ingest_config.get('icd_code', ''),
                entity_types=ingest_config.get('entity_types', []),
                ontology_json=json.dumps(ingest_config.get('ontology', {}), ensure_ascii=False),
                extraction_prompts=ingest_config.get('extraction_prompts', {}),
                accuracy_score=accuracy,
                chunk_count=run_summary.get('total_chunks', 0),
                run_id=run_id,
            )
            ids['template_id'] = template_id

        # 3. Save optimization lessons
        disease_name = config.get('diseases_to_process', ['unknown'])[0] if config.get('diseases_to_process') else 'unknown'
        medical_domain = ingest_config.get('medical_domain', 'General') if ingest_config else 'General'
        lesson_ids = []
        for opt_log in (optimization_logs or []):
            lid = self.save_optimization_lesson(
                opt_log=opt_log,
                disease_name=disease_name,
                medical_domain=medical_domain,
                run_id=run_id,
            )
            lesson_ids.append(lid)
        ids['lesson_ids'] = lesson_ids

        # 4. Save system prompt
        if system_prompt_text:
            prompt_id = self.save_system_prompt(
                prompt_text=system_prompt_text,
                accuracy_score=accuracy,
                disease_name=disease_name,
                medical_domain=medical_domain,
                iteration=run_summary.get('optimization_iterations', 0),
                is_best=True,
                run_id=run_id,
            )
            ids['prompt_id'] = prompt_id

        # 5. Link nodes via relationships
        with self.driver.session() as s:
            if ids.get('template_id'):
                s.run("""
                    MATCH (r:PipelineRunLog {run_id: $rid})
                    MATCH (t:OntologyTemplate {template_id: $tid})
                    MERGE (r)-[:USED_TEMPLATE]->(t)
                """, rid=run_id, tid=ids['template_id'])

            if ids.get('prompt_id'):
                s.run("""
                    MATCH (r:PipelineRunLog {run_id: $rid})
                    MATCH (p:SystemPromptVersion {prompt_id: $pid})
                    MERGE (r)-[:USED_PROMPT]->(p)
                """, rid=run_id, pid=ids['prompt_id'])

        print(f"  [MEMORY] === Experience saved: {len(ids)} items ===\n")
        return ids

    # ─── Query Methods ───

    def _vector_search(self, query_text: str, type_filter: str = None,
                       top_k: int = 5) -> list:
        """Vector similarity search on Experience nodes."""
        embedding = self._embed(query_text)
        with self.driver.session() as s:
            if type_filter:
                result = s.run("""
                    CALL db.index.vector.queryNodes('experience_vector_index', $k, $vec)
                    YIELD node, score
                    WHERE node.type = $type
                    RETURN node, score
                    ORDER BY score DESC
                    LIMIT $k
                """, k=top_k * 2, vec=embedding, type=type_filter)
            else:
                result = s.run("""
                    CALL db.index.vector.queryNodes('experience_vector_index', $k, $vec)
                    YIELD node, score
                    RETURN node, score
                    ORDER BY score DESC
                    LIMIT $k
                """, k=top_k, vec=embedding)

            results = []
            for rec in result:
                node = dict(rec['node'])
                node.pop('embedding', None)  # Don't return the large embedding
                node['_score'] = rec['score']
                results.append(node)
            return results[:top_k]

    def find_similar_templates(self, disease_name: str, medical_domain: str,
                                top_k: int = 3) -> list:
        """Find ontology templates from similar diseases/domains."""
        query = f"disease: {disease_name} domain: {medical_domain} ontology template entity types"
        exp_nodes = self._vector_search(query, type_filter='ontology_template', top_k=top_k)

        templates = []
        with self.driver.session() as s:
            for exp in exp_nodes:
                eid = exp.get('experience_id', '')
                result = s.run("""
                    MATCH (e:Experience {experience_id: $eid})-[:HAS_TEMPLATE]->(t:OntologyTemplate)
                    RETURN t
                """, eid=eid)
                rec = result.single()
                if rec:
                    t = dict(rec['t'])
                    t['_score'] = exp.get('_score', 0)
                    templates.append(t)

        return sorted(templates, key=lambda x: x.get('accuracy_score', 0), reverse=True)

    def find_similar_runs(self, classification: str, medical_domain: str,
                          top_k: int = 5) -> list:
        """Find past pipeline runs with similar characteristics."""
        query = f"pipeline run classification: {classification} domain: {medical_domain}"
        exp_nodes = self._vector_search(query, type_filter='pipeline_run', top_k=top_k)

        runs = []
        with self.driver.session() as s:
            for exp in exp_nodes:
                eid = exp.get('experience_id', '')
                result = s.run("""
                    MATCH (e:Experience {experience_id: $eid})-[:HAS_RUN]->(r:PipelineRunLog)
                    RETURN r
                """, eid=eid)
                rec = result.single()
                if rec:
                    r = dict(rec['r'])
                    r['_score'] = exp.get('_score', 0)
                    runs.append(r)

        return runs

    def find_relevant_lessons(self, disease_name: str, medical_domain: str,
                               top_k: int = 5) -> list:
        """Find optimization lessons from similar diseases."""
        query = f"optimization lesson disease: {disease_name} domain: {medical_domain} error fix strategy"
        exp_nodes = self._vector_search(query, type_filter='optimization_lesson', top_k=top_k)

        lessons = []
        with self.driver.session() as s:
            for exp in exp_nodes:
                eid = exp.get('experience_id', '')
                result = s.run("""
                    MATCH (e:Experience {experience_id: $eid})-[:HAS_LESSON]->(l:OptimizationLesson)
                    RETURN l
                """, eid=eid)
                rec = result.single()
                if rec:
                    l = dict(rec['l'])
                    l['_score'] = exp.get('_score', 0)
                    lessons.append(l)

        return lessons

    def find_best_prompt(self, medical_domain: str,
                         disease_name: str = None) -> dict:
        """Find highest-scoring system prompt for this domain."""
        with self.driver.session() as s:
            # Try exact disease match first
            if disease_name:
                result = s.run("""
                    MATCH (p:SystemPromptVersion {disease_name: $disease, is_best: true})
                    RETURN p ORDER BY p.accuracy_score DESC LIMIT 1
                """, disease=disease_name)
                rec = result.single()
                if rec:
                    return dict(rec['p'])

            # Fallback: best prompt in same domain
            result = s.run("""
                MATCH (p:SystemPromptVersion {medical_domain: $domain, is_best: true})
                RETURN p ORDER BY p.accuracy_score DESC LIMIT 1
            """, domain=medical_domain)
            rec = result.single()
            if rec:
                return dict(rec['p'])

        return None

    def query_before_run(self, disease_name: str, medical_domain: str,
                         classification: str, pdf_name: str = "") -> dict:
        """Main pre-run query: aggregate all relevant past experience."""
        print(f"\n  [MEMORY] === Querying Experience ===")
        print(f"  [MEMORY] Disease: {disease_name}, Domain: {medical_domain}")

        best_template = None
        similar_runs = []
        relevant_lessons = []
        best_prompt = None
        recommendations = ""

        try:
            # 1. Find similar ontology templates
            templates = self.find_similar_templates(disease_name, medical_domain, top_k=3)
            if templates:
                best_template = templates[0]
                print(f"  [MEMORY] Found {len(templates)} similar ontology templates (best: {best_template.get('disease_name', '?')} acc={best_template.get('accuracy_score', 0)}%)")

            # 2. Find similar pipeline runs
            similar_runs = self.find_similar_runs(classification, medical_domain, top_k=5)
            if similar_runs:
                print(f"  [MEMORY] Found {len(similar_runs)} similar past runs")

            # 3. Find relevant optimization lessons
            relevant_lessons = self.find_relevant_lessons(disease_name, medical_domain, top_k=5)
            if relevant_lessons:
                print(f"  [MEMORY] Found {len(relevant_lessons)} relevant optimization lessons")

            # 4. Find best system prompt
            best_prompt = self.find_best_prompt(medical_domain, disease_name)
            if best_prompt:
                print(f"  [MEMORY] Found best prompt: {best_prompt.get('disease_name', '?')} acc={best_prompt.get('accuracy_score', 0)}%")

            # 5. Generate LLM recommendations from experience
            if templates or relevant_lessons or similar_runs:
                recommendations = self._generate_recommendations(
                    disease_name, medical_domain, classification,
                    templates, similar_runs, relevant_lessons, best_prompt
                )

        except Exception as e:
            print(f"  [MEMORY] Error querying experience: {e}")

        if not any([best_template, similar_runs, relevant_lessons, best_prompt]):
            print(f"  [MEMORY] No prior experience found — starting fresh")

        print(f"  [MEMORY] === Query complete ===\n")

        return {
            "best_template": best_template,
            "similar_runs": similar_runs,
            "relevant_lessons": relevant_lessons,
            "best_prompt": best_prompt,
            "recommendations": recommendations,
        }

    def _generate_recommendations(self, disease_name, medical_domain,
                                   classification, templates, runs, lessons,
                                   best_prompt) -> str:
        """Generate actionable advice from past experience using LLM."""
        context_parts = []

        if templates:
            t = templates[0]
            context_parts.append(
                f"BEST ONTOLOGY TEMPLATE: Disease '{t.get('disease_name')}' "
                f"({t.get('medical_domain')}), acc={t.get('accuracy_score')}%, "
                f"entities={t.get('entity_types_json', '[]')[:200]}"
            )

        if runs:
            run_summaries = []
            for r in runs[:3]:
                run_summaries.append(
                    f"  - {r.get('pdf_name', '?')}: {r.get('strategy')}, "
                    f"acc={r.get('accuracy_pct')}%, chunks={r.get('total_chunks')}, "
                    f"opt_iters={r.get('optimization_iterations')}"
                )
            context_parts.append("SIMILAR PAST RUNS:\n" + "\n".join(run_summaries))

        if lessons:
            lesson_summaries = []
            for l in lessons[:3]:
                lesson_summaries.append(
                    f"  - {l.get('disease_name', '?')}: {l.get('error_analysis', '')[:150]}... "
                    f"Fixed by: {l.get('strategies_applied', '[]')}"
                )
            context_parts.append("OPTIMIZATION LESSONS:\n" + "\n".join(lesson_summaries))

        if best_prompt:
            context_parts.append(
                f"BEST SYSTEM PROMPT: From '{best_prompt.get('disease_name')}', "
                f"acc={best_prompt.get('accuracy_score')}%"
            )

        prompt = f"""Based on past experience processing medical PDFs, provide 3-5 concise recommendations
for processing a new PDF about "{disease_name}" ({medical_domain}, {classification}).

PAST EXPERIENCE:
{chr(10).join(context_parts)}

Return actionable advice in Vietnamese (3-5 bullet points). Focus on:
- Which ontology entity types work best for this domain
- Common failure patterns and how to avoid them
- Chunk size and worker recommendations
- Prompt tuning tips"""

        try:
            response = self.chat_client.chat.completions.create(
                model=self.model,
                messages=[{"role": "user", "content": prompt}],
                max_completion_tokens=500,
            )
            result = response.choices[0].message.content
            if result and result.strip():
                return result.strip()
        except Exception as e:
            print(f"  [MEMORY] LLM recommendation failed: {e}")

        # Fallback: generate structured summary without LLM
        summary_parts = []
        if runs:
            accs = [r.get('accuracy_pct', 0) for r in runs]
            summary_parts.append(f"- Runs tuong tu: {len(runs)} lan, accuracy trung binh: {sum(accs)/len(accs):.1f}%")
        if lessons:
            strategies = set()
            for l in lessons:
                try:
                    s = json.loads(l.get('strategies_applied', '[]'))
                    strategies.update(s)
                except:
                    pass
            if strategies:
                summary_parts.append(f"- Strategies da dung thanh cong: {', '.join(strategies)}")
        if best_prompt:
            summary_parts.append(f"- Best prompt tu '{best_prompt.get('disease_name', '?')}' dat {best_prompt.get('accuracy_score', 0)}%")
        return "\n".join(summary_parts) if summary_parts else ""

    # ─── Utility ───

    def get_stats(self) -> dict:
        """Get experience memory statistics."""
        with self.driver.session() as s:
            stats = {}
            for label in ['Experience', 'OntologyTemplate', 'PipelineRunLog',
                          'OptimizationLesson', 'SystemPromptVersion']:
                r = s.run(f"MATCH (n:{label}) RETURN count(n) AS c")
                stats[label] = r.single()['c']
            return stats

    def list_templates(self) -> list:
        """List all stored ontology templates."""
        with self.driver.session() as s:
            result = s.run("""
                MATCH (t:OntologyTemplate)
                RETURN t.disease_name AS disease, t.medical_domain AS domain,
                       t.accuracy_score AS accuracy, t.chunk_count AS chunks,
                       t.created_at AS created
                ORDER BY t.accuracy_score DESC
            """)
            return [dict(r) for r in result]


def backfill_from_existing_runs(memory: ExperienceMemory):
    """Backfill experience from all existing pipeline_runs directories."""
    runs_dir = NOTEBOOKLM_DIR / 'data' / 'pipeline_runs'
    if not runs_dir.exists():
        print("  No pipeline_runs directory found")
        return

    count = 0
    for run_dir in sorted(runs_dir.iterdir()):
        if not run_dir.is_dir():
            continue

        summary_file = run_dir / 'run_summary.json'
        config_file = run_dir / 'pipeline_config.json'
        report_file = run_dir / 'test_report.json'

        if not summary_file.exists() or not report_file.exists():
            continue

        try:
            with open(summary_file, 'r', encoding='utf-8') as f:
                summary = json.load(f)
            with open(config_file, 'r', encoding='utf-8') as f:
                config = json.load(f)
            with open(report_file, 'r', encoding='utf-8') as f:
                report = json.load(f)

            # Collect optimization logs
            opt_logs = []
            for i in range(1, 20):
                opt_file = run_dir / f'optimization_log_iter{i}.json'
                if opt_file.exists():
                    with open(opt_file, 'r', encoding='utf-8') as f:
                        opt_logs.append(json.load(f))

            # Read ingest config if exists
            ingest_config = None
            for cfg_file in (NOTEBOOKLM_DIR / 'config' / 'ingest_configs').glob('*.json'):
                try:
                    with open(cfg_file, 'r', encoding='utf-8') as f:
                        ic = json.load(f)
                    # Match by disease name in summary
                    if ic.get('disease_name', '') in str(summary):
                        ingest_config = ic
                        break
                except:
                    pass

            # Read system prompt
            prompt_text = None
            prompt_file = run_dir / 'improved_system_prompt.txt'
            if prompt_file.exists():
                prompt_text = prompt_file.read_text(encoding='utf-8').strip()

            memory.save_after_run(
                run_summary=summary,
                config=config,
                test_report=report,
                optimization_logs=opt_logs,
                ingest_config=ingest_config,
                system_prompt_text=prompt_text,
            )
            count += 1
            print(f"  Backfilled: {run_dir.name}")

        except Exception as e:
            print(f"  Error backfilling {run_dir.name}: {e}")

    print(f"\n  Total backfilled: {count} runs")


# ─── CLI for testing ───

if __name__ == "__main__":
    mem = ExperienceMemory()

    if len(sys.argv) > 1 and sys.argv[1] == "stats":
        stats = mem.get_stats()
        print("\nExperience Memory Stats:")
        for k, v in stats.items():
            print(f"  {k}: {v}")

    elif len(sys.argv) > 1 and sys.argv[1] == "templates":
        templates = mem.list_templates()
        print(f"\nStored Ontology Templates ({len(templates)}):")
        for t in templates:
            print(f"  {t['disease']}: {t['domain']} acc={t['accuracy']}% chunks={t['chunks']}")

    elif len(sys.argv) > 1 and sys.argv[1] == "query":
        disease = sys.argv[2] if len(sys.argv) > 2 else "Ung thư vú"
        domain = sys.argv[3] if len(sys.argv) > 3 else "Oncology"
        result = mem.query_before_run(disease, domain, "single_disease")
        print(json.dumps({k: str(v)[:200] for k, v in result.items()}, ensure_ascii=False, indent=2))

    elif len(sys.argv) > 1 and sys.argv[1] == "backfill":
        print("\nBackfilling from existing pipeline runs...")
        backfill_from_existing_runs(mem)

    else:
        print("Usage: python experience_memory.py [stats|templates|query <disease> <domain>|backfill]")

    mem.close()
