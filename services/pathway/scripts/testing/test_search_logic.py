import sys
from pathlib import Path

NOTEBOOKLM_DIR = Path(__file__).resolve().parents[2]
if str(NOTEBOOKLM_DIR) not in sys.path:
    sys.path.insert(0, str(NOTEBOOKLM_DIR))

from medical_agent import MedicalAgent
import json
import os
from dotenv import load_dotenv

load_dotenv()

def test_search():
    agent = MedicalAgent()
    try:
        q = "Bệnh nhân nam, 65 tuổi, có mã bệnh G47"
        print(f"Testing Graph-RAG Search for: {q}")
        results = agent.graph_rag_search(q, top_k=2)
        print(f"Results Count: {len(results)}")
        for i, r in enumerate(results):
            print(f"--- Result {i+1} ---")
            print(f"Title: {r['title']}")
            # print(f"Content: {r['description'][:100]}...")
            print(f"Related: {len(r.get('related_context', []))}")
            if r.get('related_context'):
                for rel in r['related_context']:
                    print(f"  - Related Title: {rel.get('title')}")
    except Exception as e:
        print(f"SEARCH FAILED: {e}")
    finally:
        agent.close()

if __name__ == "__main__":
    test_search()
