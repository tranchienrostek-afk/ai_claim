"""
Test script for unified knowledge retriever.
Verifies that ALL data layers are searchable.

Usage:
    cd pathway/notebooklm
    python test_unified_retriever.py
"""

import sys
import io
import os
import time
import json

if sys.stdout and hasattr(sys.stdout, 'buffer'):
    try:
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    except Exception:
        pass

from runtime_env import load_notebooklm_env
load_notebooklm_env()

from neo4j import GraphDatabase
from server_support.unified_retriever import UnifiedRetriever, ensure_indexes

# --- Setup ---
neo4j_uri = os.getenv("NEO4J_URI", "bolt://localhost:7688")
neo4j_user = os.getenv("NEO4J_USER", "neo4j")
neo4j_password = os.getenv("NEO4J_PASSWORD", "password123")

print(f"Connecting to Neo4j at {neo4j_uri}...")
driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))

# Verify connectivity
try:
    driver.verify_connectivity()
    print("[OK] Neo4j connected")
except Exception as e:
    print(f"[FAIL] Cannot connect to Neo4j: {e}")
    sys.exit(1)

# --- Check what data exists ---
print("\n=== NEO4J DATA INVENTORY ===")
with driver.session() as session:
    for label in ["RawChunk", "ProtocolAssertion", "ProtocolDiseaseSummary",
                  "RawSignMention", "RawServiceMention", "RawObservationMention",
                  "DiseaseEntity", "Experience", "CIDisease", "CIService", "CISign"]:
        try:
            count = session.run(f"MATCH (n:{label}) RETURN count(n) AS c").single()["c"]
            status = "OK" if count > 0 else "EMPTY"
            print(f"  [{status}] :{label} = {count}")
        except Exception as e:
            print(f"  [ERR] :{label} — {e}")

# --- Check indexes ---
print("\n=== NEO4J INDEXES ===")
with driver.session() as session:
    indexes = session.run("SHOW INDEXES YIELD name, type, labelsOrTypes, properties, state").data()
    for idx in indexes:
        print(f"  [{idx['state']}] {idx['name']} ({idx['type']}) on {idx['labelsOrTypes']} {idx['properties']}")

# --- Ensure new indexes ---
print("\n=== ENSURING NEW INDEXES ===")
created = ensure_indexes(driver)
print(f"  Created/ensured: {created}")


# --- Embedding function (optional — skip if no Azure creds) ---
embedding_fn = None
try:
    from openai import AzureOpenAI
    client = AzureOpenAI(
        azure_endpoint=os.getenv("AZURE_EMBEDDINGS_ENDPOINT", "").strip(),
        api_key=os.getenv("AZURE_EMBEDDINGS_API_KEY", "").strip(),
        api_version=os.getenv("AZURE_OPENAI_API_VERSION", "").strip()
    )
    def _embed(text):
        return client.embeddings.create(input=[text], model="text-embedding-ada-002").data[0].embedding
    # Quick test
    _embed("test")
    embedding_fn = _embed
    print("\n[OK] Embedding function available (vector search enabled)")
except Exception as e:
    print(f"\n[WARN] No embedding function: {e}")
    print("  Vector-based searches will be skipped. Fulltext searches still work.")


# --- Create retriever ---
retriever = UnifiedRetriever(driver=driver, embedding_fn=embedding_fn)


# --- Test scenarios ---
TEST_CASES = [
    {
        "name": "1. Assertion search: contraindications",
        "method": "search_assertions",
        "args": {"query": "chong chi dinh", "top_k": 5},
        "expect_layer": "assertion",
    },
    {
        "name": "2. Assertion search: treatment rules for dengue",
        "method": "search_assertions",
        "args": {"query": "dieu tri sot xuat huyet", "disease_name": "sot xuat huyet dengue", "top_k": 5},
        "expect_layer": "assertion",
    },
    {
        "name": "3. Disease summary search",
        "method": "search_summaries",
        "args": {"query": "viem gan B", "top_k": 3},
        "expect_layer": "summary",
    },
    {
        "name": "4. Sign mention search (symptom lookup)",
        "method": "search_sign_mentions",
        "args": {"query": "sot", "top_k": 5},
        "expect_layer": "sign",
    },
    {
        "name": "5. Service mention search (drug/procedure)",
        "method": "search_service_mentions",
        "args": {"query": "Peniciline", "top_k": 5},
        "expect_layer": "service",
    },
    {
        "name": "6. Observation mention search (lab tests)",
        "method": "search_observation_mentions",
        "args": {"query": "tieu cau", "top_k": 5},
        "expect_layer": "observation",
    },
    {
        "name": "7. Graph traversal: disease overview",
        "method": "traverse_disease_graph",
        "args": {"disease_name": "sot xuat huyet dengue", "top_k": 10},
        "expect_layer": "graph_traversal",
    },
    {
        "name": "8. Reverse sign lookup (differential diagnosis)",
        "method": "reverse_lookup_by_signs",
        "args": {"sign_names": ["sot", "dau dau", "phat ban"], "top_k": 5},
        "expect_layer": "graph_reasoning",
    },
    {
        "name": "9. Claims Insights: disease data",
        "method": "search_claims_insights",
        "args": {"disease_name": "sot xuat huyet", "top_k": 3},
        "expect_layer": "claims_insights",
    },
    {
        "name": "10. Experience memory search",
        "method": "search_experience",
        "args": {"query": "dengue ingestion pipeline", "top_k": 3},
        "expect_layer": "experience",
    },
]

print("\n" + "="*60)
print("UNIFIED RETRIEVER — LAYER-BY-LAYER TESTS")
print("="*60)

passed = 0
failed = 0
empty = 0

for tc in TEST_CASES:
    name = tc["name"]
    method = getattr(retriever, tc["method"])
    args = tc["args"]

    t0 = time.time()
    try:
        results = method(**args)
        ms = int((time.time() - t0) * 1000)

        if results:
            passed += 1
            print(f"\n[PASS] {name} — {len(results)} results in {ms}ms")
            for i, r in enumerate(results[:3]):
                print(f"  [{i+1}] ({r.source_layer}) {r.title[:70]}")
                print(f"      score={r.score:.3f} disease={r.disease_name}")
                print(f"      {r.content[:120]}...")
        else:
            empty += 1
            print(f"\n[EMPTY] {name} — 0 results in {ms}ms (data may not exist)")
    except Exception as e:
        failed += 1
        ms = int((time.time() - t0) * 1000)
        print(f"\n[FAIL] {name} — {e} ({ms}ms)")


# --- Full unified retrieve test ---
print("\n" + "="*60)
print("UNIFIED RETRIEVE — FULL PIPELINE TEST")
print("="*60)

FULL_TESTS = [
    {
        "name": "A. General query about dengue",
        "query": "trieu chung va dieu tri sot xuat huyet dengue",
        "intent": "general",
        "disease_name": "sot xuat huyet dengue",
        "entities": [{"name": "sot xuat huyet", "type": "Disease"}],
    },
    {
        "name": "B. Contraindication query",
        "query": "chong chi dinh dung aspirin cho benh nhan sot xuat huyet",
        "intent": "contraindication",
        "disease_name": "sot xuat huyet dengue",
        "entities": [{"name": "aspirin", "type": "Drug"}, {"name": "sot xuat huyet", "type": "Disease"}],
    },
    {
        "name": "C. Diagnosis from symptoms",
        "query": "benh nhan sot cao, dau dau, phat ban do, tieu cau giam",
        "intent": "diagnosis",
        "entities": [
            {"name": "sot cao", "type": "Symptom"},
            {"name": "dau dau", "type": "Symptom"},
            {"name": "phat ban do", "type": "Symptom"},
        ],
    },
    {
        "name": "D. Drug dosage query",
        "query": "lieu luong Entecavir dieu tri viem gan B man",
        "intent": "dosage",
        "disease_name": "viem gan vi rut B",
        "entities": [{"name": "Entecavir", "type": "Drug"}],
    },
]

for tc in FULL_TESTS:
    t0 = time.time()
    try:
        results, trace = retriever.retrieve(
            query=tc["query"],
            intent=tc["intent"],
            disease_name=tc.get("disease_name"),
            entities=tc.get("entities", []),
            top_k=10
        )
        ms = int((time.time() - t0) * 1000)

        print(f"\n[{'PASS' if results else 'EMPTY'}] {tc['name']} — {len(results)} results in {ms}ms")
        print(f"  Layers searched: {trace.get('layers_searched', 0)}")
        for layer in trace.get("layers", []):
            status = "+" if layer["count"] > 0 else "-"
            print(f"    [{status}] {layer['layer']}: {layer['count']} results ({layer['ms']}ms)")

        if results:
            passed += 1
            print(f"  Top 3 results:")
            for i, r in enumerate(results[:3]):
                print(f"    [{i+1}] ({r.source_layer}) {r.title[:60]} — score={r.score:.3f}")
        else:
            empty += 1
    except Exception as e:
        failed += 1
        print(f"\n[FAIL] {tc['name']} — {e}")


# --- Summary ---
print("\n" + "="*60)
total = passed + failed + empty
print(f"SUMMARY: {passed} passed, {empty} empty (no data), {failed} failed / {total} total")
if failed == 0:
    print("All tests passed (empty results may indicate missing data, not code errors)")
else:
    print(f"WARNING: {failed} tests FAILED — check error messages above")
print("="*60)

driver.close()
