"""
Gemini Judge: Re-score Azure AI benchmark results using Gemini as an independent judge.
Usage: python scripts/testing/gemini_mark_a_ai.py
  - Reads Azure benchmark results from report_phase_100.json
  - Uses Gemini 2.5 Flash to re-evaluate each AI answer
  - Outputs comparison report
"""
import json
import os
import time
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv

NOTEBOOKLM_DIR = Path(__file__).resolve().parents[2]

# Load env from both notebooklm/.env and root .env (for GEMINI_API_KEY)
load_dotenv()
load_dotenv(NOTEBOOKLM_DIR / ".env")

from google import genai

# Init Gemini client
client = genai.Client(api_key=os.getenv("GEMINI_API_KEY"))
GEMINI_MODEL = "gemini-3.1-pro-preview"


def gemini_evaluate(question, ground_truth, ai_answer, retries=3):
    """Use Gemini to evaluate an AI answer against ground truth."""
    eval_prompt = f"""Bạn là giám khảo chấm thi Y khoa với thang điểm 5 bậc. So sánh NỘI DUNG Y KHOA, KHÔNG chấm theo format.

QUY TẮC CHẤM:
- 1.0: Bao phủ >80% các ý chính trong đáp án chuẩn, đúng về mặt y khoa (chấp nhận từ đồng nghĩa)
- 0.75: Bao phủ 50-80% ý chính, thiếu một số chi tiết (liều lượng, huyệt vị cụ thể)
- 0.5: Bao phủ <50% ý chính, hoặc đúng chẩn đoán nhưng thiếu/sai phần điều trị
- 0.25: Rất ít thông tin đúng
- 0.0: Hoàn toàn sai hoặc không liên quan

QUY TẮC ĐẶC BIỆT:
- Chấp nhận từ đồng nghĩa y khoa Việt Nam: kiện tỳ = bổ tỳ, an thần = trấn tĩnh, dưỡng tâm = bổ tâm, bình can = thanh can, hóa đàm = trừ đàm, hoạt huyết = thông huyết
- KHÔNG trừ điểm nếu AI cung cấp thêm thông tin đúng ngoài đáp án chuẩn
- Đánh giá NỘI DUNG y khoa, KHÔNG đánh giá format hay cách trình bày
- Nếu đáp án chuẩn liệt kê N ý chính, đếm số ý AI trả lời đúng để tính tỷ lệ

Trả về DUY NHẤT định dạng JSON chứa "score" (một trong 0, 0.25, 0.5, 0.75, 1.0) và "reason" (văn bản ngắn gọn).

- Câu hỏi: {question}
- Đáp án chuẩn: {ground_truth}
- Câu trả lời của AI: {ai_answer}"""

    for attempt in range(retries):
        try:
            response = client.models.generate_content(
                model=GEMINI_MODEL,
                contents=eval_prompt,
                config={
                    "temperature": 0.0,
                    "response_mime_type": "application/json",
                },
            )
            result = json.loads(response.text)
            # Handle array responses: Gemini sometimes returns [{...}] instead of {...}
            if isinstance(result, list) and len(result) > 0:
                result = result[0]
            return float(result.get("score", 0)), result.get("reason", "N/A")
        except Exception as e:
            if attempt < retries - 1:
                wait = 3 * (attempt + 1)
                print(f"  Retry {attempt+1} after error: {e} (waiting {wait}s)")
                time.sleep(wait)
            else:
                return 0.0, f"GEMINI_ERROR: {str(e)}"


def process_item(item, index, total):
    """Evaluate a single item with Gemini."""
    question = item["cau_hoi"]
    ground_truth = item["dap_an_chuan"]
    ai_answer = item["ai_tra_loi"]

    start = time.time()
    score, reason = gemini_evaluate(question, ground_truth, ai_answer)
    elapsed = time.time() - start

    print(f"[{index+1}/{total}] {elapsed:.1f}s | Azure={item['diem']} → Gemini={score} | Q: {question[:50]}...")

    return {
        **item,
        "gemini_score": score,
        "gemini_reason": reason,
        "azure_score": item["diem"],
        "score_diff": score - item["diem"],
    }


def main():
    base_dir = NOTEBOOKLM_DIR / "data" / "datatest"

    # Find the latest Azure benchmark results
    json_file = base_dir / "report_dt12_phase_50.json"
    xlsx_file = base_dir / "report_dt12_phase_50.xlsx"

    if json_file.exists():
        print(f"Loading Azure results from: {json_file}")
        with open(json_file, "r", encoding="utf-8") as f:
            data = json.load(f)
    elif xlsx_file.exists():
        import pandas as pd
        print(f"Loading Azure results from: {xlsx_file}")
        df = pd.read_excel(xlsx_file)
        data = df.to_dict(orient="records")
    else:
        print("ERROR: No benchmark results found. Run test_runner.py first.")
        return

    print(f"Loaded {len(data)} results. Re-judging with Gemini ({GEMINI_MODEL})...\n")

    results = []
    # Use 5 workers to parallelize Gemini calls
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {
            executor.submit(process_item, item, i, len(data)): i
            for i, item in enumerate(data)
        }
        for future in as_completed(futures):
            results.append(future.result())

    # Sort by ID
    results.sort(key=lambda x: x.get("id", 0))

    # Calculate stats
    n = len(results)
    azure_total = sum(r["azure_score"] for r in results)
    gemini_total = sum(r["gemini_score"] for r in results)
    azure_acc = azure_total / n * 100
    gemini_acc = gemini_total / n * 100

    print("\n" + "=" * 60)
    print(f"SO SÁNH KẾT QUẢ CHẤM ĐIỂM (N={n})")
    print("=" * 60)
    print(f"Azure Judge (gpt-4o-mini):  {azure_acc:.2f}%")
    print(f"Gemini Judge ({GEMINI_MODEL}): {gemini_acc:.2f}%")
    print(f"Chênh lệch:                 {gemini_acc - azure_acc:+.2f}pp")
    print()

    # Score distribution
    from collections import Counter
    azure_dist = Counter(r["azure_score"] for r in results)
    gemini_dist = Counter(r["gemini_score"] for r in results)
    print("Phân bố điểm:")
    print(f"  Score | Azure | Gemini")
    for s in [0.0, 0.25, 0.5, 0.75, 1.0]:
        print(f"  {s:.2f}  |  {azure_dist.get(s, 0):3d}  |  {gemini_dist.get(s, 0):3d}")
    print()

    # Cases where Gemini scored higher/lower
    higher = sum(1 for r in results if r["gemini_score"] > r["azure_score"])
    lower = sum(1 for r in results if r["gemini_score"] < r["azure_score"])
    same = sum(1 for r in results if r["gemini_score"] == r["azure_score"])
    print(f"Gemini chấm CAO hơn Azure: {higher} câu")
    print(f"Gemini chấm THẤP hơn Azure: {lower} câu")
    print(f"Gemini chấm BẰNG Azure:     {same} câu")

    # Export
    out_file = base_dir / "report_dt12_gemini_judge.json"
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    print(f"\nĐã xuất báo cáo: {out_file}")

    # Also export xlsx
    try:
        import pandas as pd
        df = pd.DataFrame(results)
        xlsx_out = base_dir / "report_dt12_gemini_judge.xlsx"
        df.to_excel(xlsx_out, index=False)
        print(f"Đã xuất Excel: {xlsx_out}")
    except Exception as e:
        print(f"Không xuất được Excel: {e}")


if __name__ == "__main__":
    main()
