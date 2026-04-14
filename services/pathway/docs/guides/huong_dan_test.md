1. Chiến lược Prompt (Cốt lõi để tránh AI lan man)

Để hệ thống trả lời giống một bài thi y khoa (ngắn gọn, trọng tâm), thầy cần ép hệ thống bằng một **System Prompt** cực kỳ nghiêm ngặt.

- **System Prompt cho AI Trả lời (Answerer):** > _"Bạn là một chuyên gia Y tế/Giám khảo chấm thi. Nhiệm vụ của bạn là trả lời câu hỏi dựa trên dữ liệu được cung cấp. YÊU CẦU BẮT BUỘC: Trả lời cực kỳ ngắn gọn, chỉ gạch đầu dòng các từ khóa chính, mã bệnh, liều lượng thuốc hoặc tên phác đồ. TUYỆT ĐỐI KHÔNG giải thích dài dòng, KHÔNG dùng từ ngữ đưa đẩy (dạ, vâng, thưa), KHÔNG phân tích nếu không được hỏi."_
- **System Prompt cho AI Chấm điểm (Evaluator/Judge):**
  > _"Bạn là giám khảo chấm thi Y khoa. Bạn sẽ được cung cấp Câu hỏi, Đáp án chuẩn, và Câu trả lời của sinh viên (AI). Hãy so sánh ý nghĩa ngữ nghĩa và từ khóa y khoa. Cho điểm từ 0 đến 1 (0: Sai hoàn toàn, 0.5: Đúng một phần/Thiếu liều lượng, 1: Đúng và đủ ý). Trả về duy nhất định dạng JSON chứa 'score' và 'reason'."_

### 2. Kiến trúc Hệ thống Test Tự động

KIẾN TRÚC HỆ THỐNG TEST TỰ ĐỘNG (MÔ HÌNH LLM-AS-A-JUDGE)

Hệ thống được thiết kế theo dạng Pipeline tuần tự, tách biệt rõ ràng giữa module sinh văn bản (Generation) và module đánh giá (Evaluation) để đảm bảo tính khách quan khi kiểm thử dữ liệu y khoa.

#### 1. Các thành phần cốt lõi (System Components)

- **Bộ dữ liệu chuẩn (Ground Truth Dataset):** \* _Đầu vào:_ Tập file JSON (ví dụ: `data_test_10.json`).
  - _Cấu trúc:_ Chứa danh sách các đối tượng gồm `cau_hoi` (Query) và `dap_an_chuan` (Ground Truth), được gán nhãn `phan_loai` rõ ràng.
- **AI Trả lời (Answerer LLM):**
  - _Nhiệm vụ:_ Đóng vai trò là hệ thống RAG/AI đang được kiểm thử.
  - _Ràng buộc:_ Bị kiểm soát bởi một System Prompt cực kỳ khắt khe, ép buộc mô hình chỉ xuất ra các từ khóa y khoa, mã bệnh, liều lượng, tuyệt đối không giải thích lan man.
  - _Đầu ra:_ `ai_tra_loi` (Generated Response).
- **AI Giám khảo (Evaluator/Judge LLM):**
  - _Nhiệm vụ:_ Đóng vai trò là người chấm thi độc lập.
  - _Cơ chế hoạt động:_ Tiếp nhận đồng thời 3 luồng thông tin (Câu hỏi, Đáp án chuẩn, Câu trả lời của AI). Đối chiếu mức độ tương đồng về mặt ngữ nghĩa (Semantic Similarity) và độ bao phủ từ khóa chuyên môn.
  - _Đầu ra:_ Trả về cấu trúc JSON bắt buộc chứa `diem` (Score: 0, 0.5, hoặc 1) và `ly_do_cham` (Reasoning).
- **Module Thống kê & Báo cáo (Statistical & Reporting Module):**
  - _Nhiệm vụ:_ Thu thập, tổng hợp kết quả từ AI Giám khảo.
  - _Đầu ra:_ Tính toán độ chính xác tổng thể (Accuracy), phân tích điểm số theo từng `phan_loai` (để tìm ra điểm yếu của hệ thống), và xuất kết quả ra file định dạng Excel/CSV.

#### 2. Luồng thực thi dữ liệu (Data Flow)

1. **Trích xuất (Fetch):** Hệ thống đọc file dữ liệu test và đẩy từng `cau_hoi` vào Pipeline.
2. **Sinh đáp án (Generate):** `cau_hoi` được đưa qua **AI Trả lời** để lấy `ai_tra_loi`.
3. **Chấm điểm (Evaluate):** Gói dữ liệu `[cau_hoi, dap_an_chuan, ai_tra_loi]` được chuyển tới **AI Giám khảo** . Giám khảo xử lý và trả về `[diem, ly_do_cham]`.
4. **Lưu trữ & Phân tích (Log & Analyze):** Kết quả của từng câu được lưu vào DataFrame. Sau khi hoàn tất vòng lặp, hệ thống tính toán các metric thống kê và xuất báo cáo cuối cùng.

### 3. Mã nguồn Python thực thi (Giai đoạn 1)

Thầy cần cài đặt các thư viện: `pip install pandas openai` (hoặc thư viện API AI mà thầy đang dùng). Dưới đây là script chuẩn mực để thầy chạy ngay trên file JSON của mình:

**Python**

```
import json
import pandas as pd
import time
# Thay thế bằng thư viện AI thầy đang dùng (VD: google.generativeai, openai, anthropic)
import openai

# Cấu hình API Key của thầy
API_KEY = "YOUR_API_KEY"
openai.api_key = API_KEY

def get_ai_answer(question):
    """Gọi hệ thống AI của thầy để lấy câu trả lời ngắn gọn"""
    sys_prompt = "Trả lời ngắn gọn, trọng tâm, chỉ nêu từ khóa y khoa, liều lượng. Không giải thích lan man."

    response = openai.ChatCompletion.create(
        model="gpt-4", # Hoặc model Gemini thầy đang dùng
        messages=[
            {"role": "system", "content": sys_prompt},
            {"role": "user", "content": question}
        ],
        temperature=0.1 # Để temperature thấp để câu trả lời chính xác, không văn vẻ
    )
    return response['choices'][0]['message']['content']

def evaluate_with_ai(question, ground_truth, ai_answer):
    """Dùng AI làm Giám khảo chấm điểm"""
    eval_prompt = f"""
    Chấm điểm câu trả lời Y khoa.
    - Câu hỏi: {question}
    - Đáp án chuẩn: {ground_truth}
    - Câu trả lời của AI: {ai_answer}

    Dựa trên ngữ nghĩa và từ khóa cốt lõi (liều lượng, tên thuốc, huyệt đạo), hãy chấm điểm.
    Trả về ĐÚNG định dạng JSON: {{"score": <0, 0.5, hoặc 1>, "reason": "<lý do ngắn gọn>"}}
    """

    response = openai.ChatCompletion.create(
        model="gpt-4",
        messages=[{"role": "user", "content": eval_prompt}],
        temperature=0.0
    )

    try:
        result = json.loads(response['choices'][0]['message']['content'])
        return result.get('score', 0), result.get('reason', 'Lỗi parse JSON')
    except:
        return 0, "Lỗi hệ thống chấm điểm"

def run_phase_1_test():
    # 1. Lấy dữ liệu test (200 câu đầu)
    file_path = r"D:\desktop_folder\12_Claude_Code\pathway\notebooklm\data\datatest\data_test_10.json"
    with open(file_path, 'r', encoding='utf-8') as f:
        full_data = json.load(f)

    test_data = full_data[:200] # Cắt 200 câu đầu tiên

    results = []

    print(f"Bắt đầu test {len(test_data)} câu hỏi...")

    # 2. Vòng lặp Test
    for i, item in enumerate(test_data):
        question = item['cau_hoi']
        ground_truth = item['dap_an']
        phan_loai = item.get('phan_loai', 'Khác')

        print(f"Đang xử lý câu {i+1}/200...")

        # Lấy câu trả lời
        ai_answer = get_ai_answer(question)

        # Chấm điểm
        score, reason = evaluate_with_ai(question, ground_truth, ai_answer)

        results.append({
            "id": item.get('id', i+1),
            "phan_loai": phan_loai,
            "cau_hoi": question,
            "dap_an_chuan": ground_truth,
            "ai_tra_loi": ai_answer,
            "diem": score,
            "ly_do_cham": reason
        })

        time.sleep(1) # Tránh rate limit của API

    # 3. Báo cáo thống kê
    df = pd.DataFrame(results)

    total_score = df['diem'].sum()
    accuracy = (total_score / 200) * 100

    print("\n" + "="*50)
    print("BÁO CÁO THỐNG KÊ - GIAI ĐOẠN 1")
    print("="*50)
    print(f"Tổng số câu test: {len(test_data)}")
    print(f"Điểm tuyệt đối (1đ): {len(df[df['diem'] == 1])} câu")
    print(f"Điểm bán phần (0.5đ): {len(df[df['diem'] == 0.5])} câu")
    print(f"Sai hoàn toàn (0đ): {len(df[df['diem'] == 0])} câu")
    print(f"ĐỘ CHÍNH XÁC TỔNG THỂ: {accuracy:.2f}%")

    # Phân tích theo phân loại
    print("\nĐộ chính xác theo từng loại câu hỏi:")
    category_stats = df.groupby('phan_loai')['diem'].mean() * 100
    print(category_stats)

    # Xuất file báo cáo
    df.to_excel(r"D:\desktop_folder\12_Claude_Code\pathway\notebooklm\data\datatest\report_phase1.xlsx", index=False)
    print("\nĐã xuất file báo cáo chi tiết ra report_phase1.xlsx")

# Chạy chương trình
if __name__ == "__main__":
    run_phase_1_test()
```

### Cách thức hoạt động của kịch bản này:

1. **Ép tính ngắn gọn (Temperature = 0.1):** Hàm `get_ai_answer` sử dụng System Prompt chặn đứng sự lan man và dùng nhiệt độ thấp để AI chỉ tập trung xuất ra sự thật khách quan (facts).
2. **Khắc phục điểm yếu Free-text:** Hàm `evaluate_with_ai` biến một LLM thành giám khảo. Dù AI có dùng từ đồng nghĩa (VD: Đáp án là "Bất mị", AI trả lời "Chứng thất miên"), Giám khảo AI vẫn hiểu được ngữ nghĩa y khoa và cho 1 điểm, thay vì chấm 0 điểm như các thuật toán so sánh chuỗi (string matching) cũ.
3. **Thống kê chuyên sâu:** Hệ thống dùng `Pandas` để nhóm điểm số theo `phan_loai` (VD: Chẩn đoán bát cương được 90%, Liều lượng thuốc Tây y chỉ được 60%). Điều này giúp thầy biết ngay hệ thống RAG đang bị yếu ở mảng nào để tinh chỉnh (finetune) tiếp.
