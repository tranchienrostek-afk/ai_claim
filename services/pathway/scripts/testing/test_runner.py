import json
import pandas as pd
import time
import os
import sys
from pathlib import Path

NOTEBOOKLM_DIR = Path(__file__).resolve().parents[2]
if str(NOTEBOOKLM_DIR) not in sys.path:
    sys.path.insert(0, str(NOTEBOOKLM_DIR))

from medical_agent import MedicalAgent
from openai import AzureOpenAI
from concurrent.futures import ThreadPoolExecutor, as_completed
from runtime_env import load_notebooklm_env
from server_support.paths import (
    DATATEST_CASES_DIR,
    DATATEST_REPORTS_DIR,
    ensure_datatest_layout,
)

# Load configurations
load_notebooklm_env()

class ParallelClinicalRunner:
    def __init__(self, max_workers=5):
        self.agent = MedicalAgent()
        self.max_workers = max_workers
        self.judge_client = AzureOpenAI(
            azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT").strip(),
            api_key=os.getenv("AZURE_OPENAI_API_KEY").strip(),
            api_version=os.getenv("AZURE_OPENAI_API_VERSION").strip()
        )
        self.judge_model = os.getenv("MODEL2", "gpt-5-mini").strip()

    def get_clinical_answer(self, question):
        # 1. Search for context (Graph-RAG)
        context_data = self.agent.enhanced_search(question, top_k=8)
        
        # 2. Build context string with prev/next block continuity
        context_str = "DỮ LIỆU LÂM SÀNG TỪ PHÁC ĐỒ:\n"
        for i, item in enumerate(context_data):
            context_str += f"[{i+1}] {item['title']} (Trang {item.get('page_number', '?')})\n"
            # Include preceding block for context continuity
            if item.get('prev_block_content'):
                context_str += f"  [Ngữ cảnh trước]: {item['prev_block_content'][:300]}\n"
            context_str += f"Nội dung: {item['description']}\n"
            # Include following block for context continuity
            if item.get('next_block_content'):
                context_str += f"  [Ngữ cảnh sau]: {item['next_block_content'][:300]}\n"
            if item.get('related_context'):
                for rel in item['related_context']:
                    rel_title = rel.get('title') or "N/A"
                    rel_content = rel.get('content') or ""
                    context_str += f"  (Liên quan: {rel_title} - {rel_content[:500]}...)\n"
            context_str += "---\n"

        # 3. Refined System Prompt: Clinical completeness + Detail
        sys_prompt = (
            "Bạn là chuyên gia Y tế trả lời câu hỏi thi Y khoa. "
            "Nếu câu hỏi dạng đúng/sai, trả lời True hoặc False TRƯỚC rồi giải thích. "
            "Dựa trên dữ liệu phác đồ, hãy trả lời ĐẦY ĐỦ và CHI TIẾT. "
            "BAO GỒM: tên bài thuốc/vị thuốc kèm liều lượng, huyệt châm cứu, "
            "mã bệnh ICD, pháp trị, chẩn đoán bát cương, chống chỉ định, cơ chế. "
            "Nếu ngữ cảnh không đủ, suy luận dựa trên kiến thức y khoa."
        )

        messages = [
            {"role": "system", "content": sys_prompt},
            {"role": "user", "content": f"Ngữ cảnh:\n{context_str}\n\nCâu hỏi: {question}"}
        ]

        response = self.agent.chat_client.chat.completions.create(
            model=self.agent.model,
            messages=messages,
        )
        return response.choices[0].message.content

    def evaluate_response(self, question, ground_truth, ai_answer):
        eval_prompt = f"""
        Bạn là giám khảo chấm thi Y khoa với thang điểm 5 bậc. So sánh NỘI DUNG Y KHOA, KHÔNG chấm theo format.

        QUY TẮC CHẤM:
        - 1.0: Bao phủ >80% các ý chính trong đáp án chuẩn, đúng về mặt y khoa (chấp nhận từ đồng nghĩa)
        - 0.75: Bao phủ 50-80% ý chính, thiếu một số chi tiết (liều lượng, huyệt vị cụ thể)
        - 0.5: Bao phủ <50% ý chính, hoặc đúng chẩn đoán nhưng thiếu/sai phần điều trị
        - 0.25: Rất ít thông tin đúng
        - 0.0: Hoàn toàn sai hoặc không liên quan

        QUY TẮC ĐẶC BIỆT:
        - Chấp nhận từ đồng nghĩa y khoa Việt Nam: kiện tỳ = bổ tỳ, an thần = trấn tĩnh, dưỡng tâm = bổ tâm,
          bình can = thanh can, hóa đàm = trừ đàm, hoạt huyết = thông huyết
        - KHÔNG trừ điểm nếu AI cung cấp thêm thông tin đúng ngoài đáp án chuẩn
        - Đánh giá NỘI DUNG y khoa, KHÔNG đánh giá format hay cách trình bày
        - Nếu đáp án chuẩn liệt kê N ý chính, đếm số ý AI trả lời đúng để tính tỷ lệ

        Trả về DUY NHẤT định dạng JSON chứa 'score' (một trong 0, 0.25, 0.5, 0.75, 1.0) và 'reason' (văn bản ngắn gọn).

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
        # Support both Vietnamese keys (cau_hoi/dap_an) and English keys (question/answer)
        question = item.get('cau_hoi') or item.get('question', '')
        # If scenario exists, prepend it to question for richer context
        scenario = item.get('scenario', '')
        if scenario and scenario not in question:
            question = f"{scenario}\n{question}"
        ground_truth = item.get('dap_an') or item.get('dap_an_goi_y') or item.get('answer', '')
        category = item.get('phan_loai') or item.get('chu_de') or item.get('topic', 'Khác')
        
        start_time = time.time()
        # 1. Get Answer (with retry for rate limits)
        ai_answer = None
        for attempt in range(3):
            try:
                ai_answer = self.get_clinical_answer(question)
                break
            except Exception as e:
                if "429" in str(e) and attempt < 2:
                    time.sleep(5 * (attempt + 1))
                    continue
                ai_answer = f"ERROR_GEN: {str(e)}"
                break
        if ai_answer is None:
            ai_answer = "ERROR_GEN: All retries failed"
        
        # 2. Judge
        try:
            score, reason = self.evaluate_response(question, ground_truth, ai_answer)
        except Exception as e:
            score, reason = 0.0, f"ERROR_JUDGE: {str(e)}"
            
        elapsed = time.time() - start_time
        print(f"[{index+1}/{total}] Done in {elapsed:.2f}s | Score: {score} | Q: {question[:40]}...")
        
        return {
            "id": item.get('id', index+1),
            "phan_loai": category,
            "cau_hoi": question,
            "dap_an_chuan": ground_truth,
            "ai_tra_loi": ai_answer,
            "diem": score,
            "ly_do_cham": reason,
            "time_sec": elapsed
        }

    def run_benchmark(self, input_file, output_file, limit=20):
        print(f"\n--- NHÓM TEST: {limit} CÂU HỎI ---")
        with open(input_file, 'r', encoding='utf-8') as f:
            full_data = json.load(f)

        test_data = full_data[:limit]
        results = []

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_item = {executor.submit(self.process_single_case, item, i, len(test_data)): i for i, item in enumerate(test_data)}
            for future in as_completed(future_to_item):
                results.append(future.result())

        # Sort results by ID to keep order
        results.sort(key=lambda x: x['id'])
        
        df = pd.DataFrame(results)
        total_score = df['diem'].sum()
        accuracy = (total_score / len(test_data)) * 100

        print("\n" + "="*50)
        print(f"KẾT QUẢ THỐNG KÊ (N={limit})")
        print("="*50)
        print(f"Độ chính xác: {accuracy:.2f}%")
        print(f"Thời gian TB/câu: {df['time_sec'].mean():.2f}s")
        
        # Export (use json fallback if xlsx is locked)
        try:
            df.to_excel(output_file, index=False)
            print(f"Đã xuất báo cáo: {output_file}")
        except PermissionError:
            json_file = output_file.replace('.xlsx', '.json')
            df.to_json(json_file, orient='records', force_ascii=False, indent=2)
            print(f"Excel bị khóa, đã xuất JSON: {json_file}")
        
        return accuracy

if __name__ == "__main__":
    runner = ParallelClinicalRunner(max_workers=5)
    ensure_datatest_layout()
    input_p = str(DATATEST_CASES_DIR / "data_test_13.json")

    # Phased logic
    phases = [20]
    
    try:
        for phase_limit in phases:
            out_p = str(DATATEST_REPORTS_DIR / f"report_dt13_phase_{phase_limit}.xlsx")
            acc = runner.run_benchmark(input_p, out_p, limit=phase_limit)
            
            if acc < 50: # Example threshold
                print(f"\n[!] Độ chính xác {acc:.2f}% thấp. Dừng lại để cải thiện prompt!")
                break
            else:
                print(f"[OK] Kết quả ổn ({acc:.2f}%). Tiếp tục phase tiếp theo...")
                
    finally:
        runner.agent.close()
