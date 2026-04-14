"""
Test runner cho data_test_19.json — 20 câu hỏi lâm sàng TMH.
Sử dụng disease routing (resolve_disease_name → scoped_search) + LLM judge.
"""
import json
import pandas as pd
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
from server_support.paths import (
    DATATEST_CASES_DIR,
    DATATEST_REPORTS_DIR,
    ensure_datatest_layout,
)

load_dotenv()

# Fix Windows console encoding
import io
if sys.stdout.encoding != 'utf-8':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

DATA_FILE = str(DATATEST_CASES_DIR / "data_test_19.json")
REPORT_FILE = str(DATATEST_REPORTS_DIR / "report_dt19_tmh.xlsx")


class TMHTestRunner:
    def __init__(self, max_workers=5):
        self.agent = MedicalAgent()
        self.max_workers = max_workers
        self.judge_client = AzureOpenAI(
            azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT").strip(),
            api_key=os.getenv("AZURE_OPENAI_API_KEY").strip(),
            api_version=os.getenv("AZURE_OPENAI_API_VERSION").strip()
        )
        self.judge_model = os.getenv("MODEL2", "gpt-5-mini").strip()

    def get_clinical_answer(self, scenario, question, topic):
        """Generate answer using disease routing + Graph-RAG context."""
        # Combine scenario + question for search
        full_query = f"{scenario} {question}"

        # Disease routing: try resolve from topic first, then from full query
        disease_name = self.agent.resolve_disease_name(topic)
        if not disease_name:
            disease_name = self.agent.resolve_disease_name(full_query)

        # Scoped or enhanced search
        if disease_name:
            context_data = self.agent.scoped_search(full_query, disease_name, top_k=8)
            search_mode = f"scoped:{disease_name}"
        else:
            context_data = self.agent.enhanced_search(full_query, top_k=8)
            search_mode = "enhanced"

        # Build context string
        context_str = "DỮ LIỆU LÂM SÀNG TỪ PHÁC ĐỒ:\n"
        for i, item in enumerate(context_data):
            context_str += f"[{i+1}] {item['title']} (Trang {item.get('page_number', '?')})\n"
            if item.get('prev_block_content'):
                context_str += f"  [Ngữ cảnh trước]: {item['prev_block_content'][:300]}\n"
            context_str += f"Nội dung: {item['description']}\n"
            if item.get('next_block_content'):
                context_str += f"  [Ngữ cảnh sau]: {item['next_block_content'][:300]}\n"
            if item.get('related_context'):
                for rel in item['related_context']:
                    rel_title = rel.get('title') or "N/A"
                    rel_content = rel.get('content') or ""
                    context_str += f"  (Liên quan: {rel_title} - {rel_content[:500]}...)\n"
            context_str += "---\n"

        sys_prompt = (
            "Bạn là chuyên gia Tai Mũi Họng trả lời câu hỏi lâm sàng. "
            "Dựa trên dữ liệu phác đồ BYT, hãy trả lời ĐẦY ĐỦ và CHI TIẾT. "
            "BAO GỒM: chẩn đoán, phác đồ điều trị, thuốc kèm liều lượng, "
            "chỉ định phẫu thuật, biến chứng, tiên lượng nếu liên quan. "
            "Nếu ngữ cảnh không đủ, suy luận dựa trên kiến thức y khoa."
        )

        user_content = f"Ngữ cảnh:\n{context_str}\n\nTình huống: {scenario}\n\nCâu hỏi: {question}"

        response = self.agent.chat_client.chat.completions.create(
            model=self.agent.model,
            messages=[
                {"role": "system", "content": sys_prompt},
                {"role": "user", "content": user_content}
            ],
        )
        return response.choices[0].message.content, search_mode, len(context_data)

    def evaluate_response(self, question, scenario, ground_truth, ai_answer):
        """LLM judge scoring."""
        eval_prompt = f"""
Bạn là giám khảo chấm thi Y khoa TMH với thang điểm 5 bậc. So sánh NỘI DUNG Y KHOA, KHÔNG chấm theo format.

QUY TẮC CHẤM:
- 1.0: Bao phủ >80% các ý chính trong đáp án chuẩn, đúng về mặt y khoa (chấp nhận từ đồng nghĩa)
- 0.75: Bao phủ 50-80% ý chính, thiếu một số chi tiết cụ thể
- 0.5: Bao phủ <50% ý chính, hoặc đúng chẩn đoán nhưng thiếu/sai phần điều trị
- 0.25: Rất ít thông tin đúng
- 0.0: Hoàn toàn sai hoặc không liên quan

QUY TẮC ĐẶC BIỆT:
- Chấp nhận từ đồng nghĩa y khoa: các thuật ngữ tương đương trong chuyên ngành TMH
- KHÔNG trừ điểm nếu AI cung cấp thêm thông tin đúng ngoài đáp án chuẩn
- Đánh giá NỘI DUNG y khoa, KHÔNG đánh giá format hay cách trình bày
- Nếu đáp án chuẩn liệt kê N ý chính, đếm số ý AI trả lời đúng để tính tỷ lệ

Trả về DUY NHẤT định dạng JSON chứa 'score' (một trong 0, 0.25, 0.5, 0.75, 1.0) và 'reason' (văn bản ngắn gọn).

- Tình huống: {scenario}
- Câu hỏi: {question}
- Đáp án chuẩn: {ground_truth}
- Câu trả lời của AI: {ai_answer}
"""
        try:
            response = self.judge_client.chat.completions.create(
                model=self.judge_model,
                messages=[{"role": "user", "content": eval_prompt}],
                response_format={"type": "json_object"}
            )
            result = json.loads(response.choices[0].message.content)
            return float(result.get('score', 0)), result.get('reason', 'N/A')
        except Exception as e:
            return 0.0, f"Error: {str(e)}"

    def process_single_case(self, item, index, total):
        """Process one test case: answer + judge."""
        topic = item['topic']
        scenario = item['scenario']
        question = item['question']
        ground_truth = item['answer']
        tags = item.get('tags', [])

        start_time = time.time()

        # Generate answer with retry
        ai_answer, search_mode, n_chunks = None, "N/A", 0
        for attempt in range(3):
            try:
                ai_answer, search_mode, n_chunks = self.get_clinical_answer(scenario, question, topic)
                break
            except Exception as e:
                if "429" in str(e) and attempt < 2:
                    time.sleep(5 * (attempt + 1))
                    continue
                ai_answer = f"ERROR_GEN: {str(e)}"
                break
        if ai_answer is None:
            ai_answer = "ERROR_GEN: All retries failed"

        # Judge
        try:
            score, reason = self.evaluate_response(question, scenario, ground_truth, ai_answer)
        except Exception as e:
            score, reason = 0.0, f"ERROR_JUDGE: {str(e)}"

        elapsed = time.time() - start_time
        print(f"[{index+1}/{total}] {elapsed:.1f}s | Score: {score} | {search_mode} | {topic}: {question[:50]}...")

        return {
            "id": item.get('id', index + 1),
            "topic": topic,
            "tags": ", ".join(tags),
            "scenario": scenario,
            "question": question,
            "ground_truth": ground_truth,
            "ai_answer": ai_answer,
            "score": score,
            "judge_reason": reason,
            "search_mode": search_mode,
            "n_chunks": n_chunks,
            "time_sec": round(elapsed, 2)
        }

    def run(self, input_file=DATA_FILE, output_file=REPORT_FILE, limit=None):
        """Run full benchmark."""
        with open(input_file, 'r', encoding='utf-8') as f:
            data = json.load(f)

        if limit:
            data = data[:limit]

        total = len(data)
        print(f"\n{'='*60}")
        print(f"  TMH BENCHMARK — {total} câu hỏi, {self.max_workers} workers")
        print(f"{'='*60}\n")

        results = []
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {
                executor.submit(self.process_single_case, item, i, total): i
                for i, item in enumerate(data)
            }
            for future in as_completed(futures):
                results.append(future.result())

        results.sort(key=lambda x: x['id'])
        df = pd.DataFrame(results)

        # Stats
        total_score = df['score'].sum()
        accuracy = (total_score / total) * 100
        avg_time = df['time_sec'].mean()

        print(f"\n{'='*60}")
        print(f"  KẾT QUẢ TỔNG HỢP")
        print(f"{'='*60}")
        print(f"  Tổng câu:       {total}")
        print(f"  Tổng điểm:      {total_score}/{total}")
        print(f"  Độ chính xác:   {accuracy:.1f}%")
        print(f"  Thời gian TB:   {avg_time:.1f}s/câu")

        # Per-topic breakdown
        print(f"\n  {'─'*50}")
        print(f"  THEO CHỦ ĐỀ:")
        topic_stats = df.groupby('topic')['score'].agg(['mean', 'count'])
        for topic, row in topic_stats.iterrows():
            pct = row['mean'] * 100
            n = int(row['count'])
            bar = "█" * int(pct // 10) + "░" * (10 - int(pct // 10))
            print(f"    {topic:<40} {bar} {pct:.0f}% ({n}q)")

        # Search mode breakdown
        print(f"\n  {'─'*50}")
        print(f"  SEARCH MODE:")
        for mode, group in df.groupby('search_mode'):
            avg = group['score'].mean() * 100
            print(f"    {mode:<40} {avg:.0f}% avg ({len(group)}q)")

        # Export
        try:
            df.to_excel(output_file, index=False)
            print(f"\n  Báo cáo: {output_file}")
        except PermissionError:
            json_out = output_file.replace('.xlsx', '.json')
            df.to_json(json_out, orient='records', force_ascii=False, indent=2)
            print(f"\n  Excel bị khóa, xuất JSON: {json_out}")

        print(f"{'='*60}\n")
        return accuracy


if __name__ == "__main__":
    runner = TMHTestRunner(max_workers=5)
    try:
        ensure_datatest_layout()
        runner.run()
    finally:
        runner.agent.close()
