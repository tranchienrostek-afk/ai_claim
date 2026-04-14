# Report 03

## 2026-03-29

### 1. Những gì đã hoàn thành

- Đã dựng và chạy được UI `ontology_v2_pdf_inspector` trên server source `9618`.
- Đã dựng thêm UI `claims_adjudication_trace` để import testcase JSON, xem plan, reasoning, adjudication trace và artifact log.
- Đã xác minh `Viêm phổi` thực sự đã được ingest trong namespace `ontology_v2` trên nhánh server `9618`.
- Đã xác minh graph retriever kéo được `Viêm phổi` từ ontology khi query theo hint bệnh phù hợp.
- Đã nối `seed_disease_hints` vào `DiseaseHypothesisEngine`.
- Đã vá `testcase_trace_runner.py` theo hướng:
  - nếu case đã có bệnh/chẩn đoán biết trước thì trace thẩm định sẽ ưu tiên `anchored disease`
  - không còn phụ thuộc hoàn toàn vào `free top-1` từ sign-engine

### 2. Vấn đề cốt lõi đã tìm ra

- Người dùng nhớ đúng: `Viêm phổi` không hề bị mất.
- Lỗi nằm ở tầng trace/adjudication:
  - sign-engine tạo nhiều hypothesis TMH nhiễu
  - UI trace trước đó lấy `free top-1 hypothesis` để dẫn adjudication
  - vì vậy dù graph đã có `Viêm phổi`, trace vẫn nhìn như đi sai bệnh

### 3. Tình trạng code hiện tại

- File chính đã chỉnh:
  - [testcase_trace_runner.py](D:/desktop_folder/12_Claude_Code/pathway/notebooklm/server_support/testcase_trace_runner.py)
  - [disease_hypothesis_engine.py](D:/desktop_folder/12_Claude_Code/pathway/notebooklm/workspaces/claims_insights/pipeline/disease_hypothesis_engine.py)
- Logic mới đã vào code:
  - chọn `focus hypothesis` theo `expected disease`
  - dùng `focus hypothesis` để trace support cho từng service line
  - giữ lại `free top-1` như tham chiếu phụ

### 4. Điểm còn dang dở

- Lần rerun đầy đủ trên `9618` sau patch cuối chưa hoàn tất vì phiên test bị người dùng cắt ngang giữa chừng.
- Vì vậy, ngày mai chỉ cần làm một việc rất ngắn:
  - rerun lại `kich_ban_11.json` trên `9618`
  - xác nhận `top1_disease` của trace đã neo theo `Viêm phổi`
  - kiểm tra lại service-line adjudication có hợp lý hơn hay chưa

### 5. Trạng thái trước khi tắt máy

- Docker đang có nhiều container chạy song song, gồm:
  - `pathway-api-1`
  - `notebooklm-neo4j-1`
  - các stack khác như `16_app_math_*`, `drug_icd_*`, `elearning_db`, `n8n`
- Sau báo cáo này sẽ dừng toàn bộ container đang chạy.
