"""
V2 Hepatitis B Test Runner — Benchmark scoped_search() against vgb_01.json.

Usage:
    cd notebooklm
    python scripts/testing/test_v2_hepb.py
"""

import json
import time
import os
import sys
from pathlib import Path
from dotenv import load_dotenv

NOTEBOOKLM_DIR = Path(__file__).resolve().parents[2]
if str(NOTEBOOKLM_DIR) not in sys.path:
    sys.path.insert(0, str(NOTEBOOKLM_DIR))

from medical_agent import MedicalAgent
from openai import AzureOpenAI
from concurrent.futures import ThreadPoolExecutor, as_completed

load_dotenv()

DISEASE_NAME = "Viêm gan vi rút B"
INPUT_FILE = str(NOTEBOOKLM_DIR / "data" / "datatest" / "vgb_01.json")
OUTPUT_FILE = str(NOTEBOOKLM_DIR / "data" / "datatest" / "report_vgb_01.json")


class V2HepBRunner:
    def __init__(self, max_workers=3):
        self.agent = MedicalAgent()
        self.max_workers = max_workers
        self.judge_client = AzureOpenAI(
            azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT").strip(),
            api_key=os.getenv("AZURE_OPENAI_API_KEY").strip(),
            api_version=os.getenv("AZURE_OPENAI_API_VERSION").strip()
        )
        self.judge_model = os.getenv("MODEL1", "gpt-4o-mini").strip()

    def get_answer(self, question):
        """Search V2 Chunk nodes scoped to HepB, then generate answer."""
        context_data = self.agent.scoped_search(question, disease_name=DISEASE_NAME, top_k=8)

        context_str = "DỮ LIỆU LÂM SÀNG TỪ PHÁC ĐỒ VIÊM GAN B (BYT 2019):\n"
        for i, item in enumerate(context_data):
            section = item.get('section_path') or ''
            page = item.get('page_number') or '?'
            context_str += f"\n[{i+1}] {item['title']} (Mục: {section}, Trang {page})\n"
            if item.get('prev_block_content'):
                context_str += f"  [Ngữ cảnh trước]: {str(item['prev_block_content'])[:300]}\n"
            context_str += f"Nội dung: {item['description']}\n"
            if item.get('next_block_content'):
                context_str += f"  [Ngữ cảnh sau]: {str(item['next_block_content'])[:300]}\n"
            context_str += "---\n"

        sys_prompt = (
            "Bạn là chuyên gia Gan mật trả lời câu hỏi thi Y khoa về Viêm gan B. "
            "Dựa trên dữ liệu phác đồ Bộ Y tế 2019, hãy trả lời ĐẦY ĐỦ và CHI TIẾT. "
            "BAO GỒM: tên thuốc kháng virus kèm liều lượng, chỉ số xét nghiệm, "
            "mã ICD-10, chỉ định/chống chỉ định, cơ chế, giai đoạn METAVIR. "
            "Nếu ngữ cảnh không đủ, suy luận dựa trên kiến thức y khoa."
        )

        messages = [
            {"role": "system", "content": sys_prompt},
            {"role": "user", "content": f"Ngữ cảnh:\n{context_str}\n\nCâu hỏi: {question}"}
        ]

        try:
            response = self.agent.chat_client.chat.completions.create(
                model=self.agent.model, messages=messages, temperature=0.1
            )
        except Exception:
            # Some models (e.g. gpt-5-mini) don't support temperature != 1
            response = self.agent.chat_client.chat.completions.create(
                model=self.agent.model, messages=messages
            )
        return response.choices[0].message.content, len(context_data)

    def evaluate(self, question, ground_truth, ai_answer):
        """LLM judge scoring 0-1 in 5 levels."""
        eval_prompt = f"""Bạn là giám khảo chấm thi Y khoa Viêm gan B với thang điểm 5 bậc.
So sánh NỘI DUNG Y KHOA, KHÔNG chấm theo format.

QUY TẮC CHẤM:
- 1.0: Bao phủ >80% các ý chính trong đáp án chuẩn, đúng về mặt y khoa
- 0.75: Bao phủ 50-80% ý chính, thiếu một số chi tiết (liều lượng, chỉ số cụ thể)
- 0.5: Bao phủ <50% ý chính, hoặc đúng chẩn đoán nhưng thiếu/sai phần điều trị
- 0.25: Rất ít thông tin đúng
- 0.0: Hoàn toàn sai hoặc không liên quan

QUY TẮC ĐẶC BIỆT:
- Chấp nhận từ đồng nghĩa y khoa: NAs = nucleos(t)ide analogues, TDF = Tenofovir disoproxil, ETV = Entecavir
- KHÔNG trừ điểm nếu AI cung cấp thêm thông tin đúng ngoài đáp án chuẩn
- Đánh giá NỘI DUNG y khoa, KHÔNG đánh giá format hay cách trình bày

Trả về DUY NHẤT JSON: {{"score": 0|0.25|0.5|0.75|1.0, "reason": "..."}}

- Câu hỏi: {question}
- Đáp án chuẩn: {ground_truth}
- Câu trả lời AI: {ai_answer}"""

        try:
            response = self.judge_client.chat.completions.create(
                model=self.judge_model,
                messages=[{"role": "user", "content": eval_prompt}],
                response_format={"type": "json_object"},
                temperature=0.0
            )
            result = json.loads(response.choices[0].message.content)
            return float(result.get('score', 0)), result.get('reason', 'N/A')
        except Exception as e:
            return 0.0, f"ERROR_JUDGE: {e}"

    def process_case(self, item, index, total):
        question = item['cau_hoi']
        ground_truth = item.get('dap_an_goi_y', '')
        category = item.get('chu_de', 'Khác')

        start = time.time()

        # Get answer with retry
        ai_answer, n_chunks = None, 0
        for attempt in range(3):
            try:
                ai_answer, n_chunks = self.get_answer(question)
                break
            except Exception as e:
                if "429" in str(e) and attempt < 2:
                    time.sleep(5 * (attempt + 1))
                    continue
                ai_answer = f"ERROR_GEN: {e}"
                break
        if ai_answer is None:
            ai_answer = "ERROR_GEN: All retries failed"

        # Judge
        try:
            score, reason = self.evaluate(question, ground_truth, ai_answer)
        except Exception as e:
            score, reason = 0.0, f"ERROR_JUDGE: {e}"

        elapsed = time.time() - start
        print(f"  [{index+1}/{total}] {score:.2f} | {elapsed:.1f}s | {n_chunks} chunks | {question[:50]}...")

        return {
            "id": item.get('id', index + 1),
            "chu_de": category,
            "cau_hoi": question,
            "dap_an_chuan": ground_truth,
            "ai_tra_loi": ai_answer,
            "diem": score,
            "ly_do_cham": reason,
            "chunks_found": n_chunks,
            "time_sec": round(elapsed, 2)
        }

    def run(self, limit=50):
        with open(INPUT_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)[:limit]

        print(f"\n{'='*60}")
        print(f"V2 BENCHMARK: Viêm gan B — {len(data)} câu hỏi")
        print(f"Search: scoped_search(disease_name='{DISEASE_NAME}')")
        print(f"{'='*60}\n")

        results = []
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {
                executor.submit(self.process_case, item, i, len(data)): i
                for i, item in enumerate(data)
            }
            for future in as_completed(futures):
                results.append(future.result())

        results.sort(key=lambda x: x['id'])

        # === Stats ===
        scores = [r['diem'] for r in results]
        total = sum(scores)
        avg = total / len(scores) if scores else 0
        perfect = sum(1 for s in scores if s == 1.0)
        good = sum(1 for s in scores if s >= 0.75)
        fail = sum(1 for s in scores if s <= 0.25)
        avg_time = sum(r['time_sec'] for r in results) / len(results) if results else 0
        avg_chunks = sum(r['chunks_found'] for r in results) / len(results) if results else 0

        # Group by category
        cat_scores = {}
        for r in results:
            cat = r['chu_de']
            cat_scores.setdefault(cat, []).append(r['diem'])

        print(f"\n{'='*60}")
        print(f"KẾT QUẢ V2 BENCHMARK — VIÊM GAN B")
        print(f"{'='*60}")
        print(f"Tổng câu:           {len(data)}")
        print(f"Tổng điểm:          {total:.2f}/{len(data)}")
        print(f"Độ chính xác:       {avg*100:.1f}%")
        print(f"Điểm tuyệt đối:    {perfect}/{len(data)} ({perfect/len(data)*100:.0f}%)")
        print(f"Điểm >= 0.75:       {good}/{len(data)} ({good/len(data)*100:.0f}%)")
        print(f"Điểm <= 0.25:       {fail}/{len(data)} ({fail/len(data)*100:.0f}%)")
        print(f"TB chunks/câu:      {avg_chunks:.1f}")
        print(f"TB thời gian/câu:   {avg_time:.1f}s")

        print(f"\nTHEO CHỦ ĐỀ:")
        for cat, cat_s in cat_scores.items():
            cat_avg = sum(cat_s) / len(cat_s)
            print(f"  {cat}: {cat_avg*100:.0f}% ({len(cat_s)} câu)")

        # Export
        report = {
            "summary": {
                "total_questions": len(data),
                "accuracy_pct": round(avg * 100, 1),
                "perfect_count": perfect,
                "good_count": good,
                "fail_count": fail,
                "avg_chunks": round(avg_chunks, 1),
                "avg_time_sec": round(avg_time, 1),
                "by_category": {
                    cat: round(sum(s) / len(s) * 100, 1)
                    for cat, s in cat_scores.items()
                }
            },
            "results": results
        }

        with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
            json.dump(report, f, ensure_ascii=False, indent=2)
        print(f"\nBáo cáo: {OUTPUT_FILE}")

        return avg


if __name__ == "__main__":
    runner = V2HepBRunner(max_workers=3)
    try:
        runner.run(limit=50)
    finally:
        runner.agent.close()
