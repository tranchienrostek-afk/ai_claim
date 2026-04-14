# tc_onto_004 Disease Hypothesis Report

- Input: `D:\desktop_folder\12_Claude_Code\pathway\notebooklm\data\script\testcase_11.json`
- Story title: `Test 4: Dấu hiệu cấp cứu 'Red Flag' vùng cổ`
- Expected disease: `Nhiễm trùng khoang cổ sâu`
- Top-1 hit: `True`
- Top-3 hit: `True`

## Top Hypotheses

- `` Nhiễm trùng khoang cổ sâu | status `active` | score `0.7` | confidence `0.8235`
  memory: `Rà lại link Disease -> Expected Services cho 'Nhiễm trùng khoang cổ sâu', đồng thời kiểm tra sign decomposition với các dấu hiệu: Khối sưng vùng cổ, Nóng, đỏ, đau, Mất lọc cọc thanh quản cột sống, Khó thở.`
- `J18.0` Viêm phế quản phổi, không đặc hiệu | status `active` | score `0.2447` | confidence `0.62`
- `J18` Viêm phổi, tác nhân không xác định
(Điều chỉnh lại "Tên chẩn đoán bệnh" của I18 theo ICD10) | status `active` | score `0.2401` | confidence `0.6155`
- `J02` Viêm họng cấp | status `active` | score `0.2347` | confidence `0.6101`
- `J18.9` Viêm phổi, không đặc hiệu | status `active` | score `0.2287` | confidence `0.6039`
  graph_context: `assertion: Bổ sung thuốc kháng virus, như oseltamivir cho bệnh nhân VPMPCĐ có kết quả xét nghiệm dương tính với cúm, không phụ thuộc vào thời gian bệnh; assertion: Xét nghiệm kháng nguyên trong nước tiểu là có thể là phương pháp chẩn đoán bổ sung hoặc thay thế để phát hiện S. pneumoniae và L. pneumophil`
