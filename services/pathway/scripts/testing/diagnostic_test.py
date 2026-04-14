import json
import time
import os
import sys
from pathlib import Path
from dotenv import load_dotenv
from server_support.paths import DATATEST_CASES_DIR

NOTEBOOKLM_DIR = Path(__file__).resolve().parents[2]
if str(NOTEBOOKLM_DIR) not in sys.path:
    sys.path.insert(0, str(NOTEBOOKLM_DIR))

from medical_agent import MedicalAgent
from openai import AzureOpenAI

load_dotenv()

class PerformanceDiagnostic:
    def __init__(self):
        self.agent = MedicalAgent()
        self.judge_client = AzureOpenAI(
            azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT").strip(),
            api_key=os.getenv("AZURE_OPENAI_API_KEY").strip(),
            api_version=os.getenv("AZURE_OPENAI_API_VERSION").strip()
        )
        self.judge_model = os.getenv("MODEL1", "gpt-4o-mini").strip()

    def run_diagnostic(self, limit=5):
        with open(DATATEST_CASES_DIR / "data_test_10.json", 'r', encoding='utf-8') as f:
            data = json.load(f)[:limit]

        for i, item in enumerate(data):
            q = item['cau_hoi']
            print(f"\n[{i+1}/{limit}] Case: {q[:50]}...")
            
            # Step 1: Search
            start = time.time()
            context = self.agent.atomic_search(q, top_k=5)
            search_time = time.time() - start
            print(f"  - Search Time: {search_time:.2f}s (Fetched {len(context)} blocks)")
            
            # Step 2: Generation
            start = time.time()
            sys_prompt = "Trả lời ngắn gọn, gạch đầu dòng từ khóa y khoa. Không giải thích."
            messages = [
                {"role": "system", "content": sys_prompt},
                {"role": "user", "content": f"Ngữ cảnh:\n{context}\n\nCâu hỏi: {q}"}
            ]
            resp = self.agent.chat_client.chat.completions.create(
                model=self.agent.model, messages=messages, temperature=0.1
            )
            answer = resp.choices[0].message.content
            gen_time = time.time() - start
            print(f"  - Generation Time: {gen_time:.2f}s")
            
            # Step 3: Judge
            start = time.time()
            eval_prompt = f"Chấm điểm 0-1. Trả về JSON: {{\"score\": 0..1, \"reason\": \"...\"}}\nQ: {q}\nGT: {item['dap_an']}\nAI: {answer}"
            resp = self.judge_client.chat.completions.create(
                model=self.judge_model, messages=[{"role": "user", "content": eval_prompt}], 
                temperature=0.0, response_format={"type": "json_object"}
            )
            judge_time = time.time() - start
            print(f"  - Judge Time: {judge_time:.2f}s")

if __name__ == "__main__":
    diag = PerformanceDiagnostic()
    diag.run_diagnostic()
    diag.agent.close()
