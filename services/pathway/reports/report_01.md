# Report 01

## 2026-03-28

- Đã phát triển tiếp nhánh `sign/modifier -> disease` theo hướng data-driven, không hardcode trong engine.
- Đã thêm catalog profile y khoa tại [tmh_ontology_disease_profiles.json](D:/desktop_folder/12_Claude_Code/pathway/notebooklm/workspaces/claims_insights/05_reference/signs/tmh_ontology_disease_profiles.json).
- Đã nối `raw_sign_mentions`, `ontology_extraction.sign_concepts`, `modifiers`, `patient context` vào flow test ở [test_kich_ban_json_case.py](D:/desktop_folder/12_Claude_Code/pathway/notebooklm/data/script/test_kich_ban_json_case.py).
- Đã bổ sung alias dịch vụ vào [service_mapping_policy.json](D:/desktop_folder/12_Claude_Code/pathway/notebooklm/workspaces/claims_insights/02_standardize/service_mapping_policy.json).

## Kết quả chính

- Batch test [testcase_11.json](D:/desktop_folder/12_Claude_Code/pathway/notebooklm/data/script/testcase_11.json) đã chạy xong tại [testcase_11_batch](D:/desktop_folder/12_Claude_Code/pathway/notebooklm/data/script/testcase_11_batch).
- `Disease top-1 hit`: `0/5 -> 5/5`
- `Disease top-3 hit`: `0/5 -> 5/5`
- `Sign/service overlap`: `1/5 -> 3/5`
- `Family-resolved mapping`: base `13/15`, hybrid `15/15`

## Đánh giá nhanh

- Tầng `sign/modifier -> disease hypothesis` đã bật lên rõ rệt.
- Tầng `service -> exact service_code` vẫn là nút thắt chính.
- Hiện hệ đã hiểu tốt hơn các ca ontology khó như:
  - `cholesteatoma`
  - `u xơ mạch vòm mũi họng`
  - `croup`
  - `nhiễm trùng khoang cổ sâu`
  - `Ménière`

## Việc còn lại

- Tăng chất lượng `service concept -> service_code`
- Bổ sung thêm ontology/service alias cho các thủ thuật TMH đặc hiệu
- Giảm các dòng còn `REVIEW` ở service mapping

## Thông tin thêm cho chuyên gia

- Đã rà lại pipeline `PDF -> graph` và xác minh được một số điểm mất chất lượng thật từ code hiện tại.
- Lưu ý: các tỷ lệ ảnh hưởng như `~30%`, `~25%` là nhận định chuyên gia, chưa phải benchmark định lượng end-to-end mình đã đo xong.

### Điểm đã xác minh từ code

- Chunking trong [v2_ingest.py](D:/desktop_folder/12_Claude_Code/pathway/notebooklm/v2_ingest.py:74) đang dùng `MAX_CHUNK = 3000`, [v2_ingest.py](D:/desktop_folder/12_Claude_Code/pathway/notebooklm/v2_ingest.py:75) dùng `MIN_CHUNK = 100`, và merge tiny chunks theo kiểu nối vào chunk trước ở [v2_ingest.py](D:/desktop_folder/12_Claude_Code/pathway/notebooklm/v2_ingest.py:194). Đây là threshold cố định, chưa theo semantic unit/domain.
- `skip_first_page` mặc định trong chunker là `True` ở [v2_ingest.py](D:/desktop_folder/12_Claude_Code/pathway/notebooklm/v2_ingest.py:101), nhưng nhánh multi-disease đã override về `False` ở [multi_disease_ingest.py](D:/desktop_folder/12_Claude_Code/pathway/notebooklm/multi_disease_ingest.py:312).
- Entity extraction trong [v2_ingest.py](D:/desktop_folder/12_Claude_Code/pathway/notebooklm/v2_ingest.py:356) đang cắt `chunk_content[:2000]` ở [v2_ingest.py](D:/desktop_folder/12_Claude_Code/pathway/notebooklm/v2_ingest.py:372).
- `V2Ingest` đang hardcode ontology Hepatitis B trong [v2_ingest.py](D:/desktop_folder/12_Claude_Code/pathway/notebooklm/v2_ingest.py:293), gồm danh sách `Drug`, `LabTest`, `Stage` ở [v2_ingest.py](D:/desktop_folder/12_Claude_Code/pathway/notebooklm/v2_ingest.py:316), [v2_ingest.py](D:/desktop_folder/12_Claude_Code/pathway/notebooklm/v2_ingest.py:336), [v2_ingest.py](D:/desktop_folder/12_Claude_Code/pathway/notebooklm/v2_ingest.py:339).
- Embedding đang truncate `8000` ký tự ở [v2_ingest.py](D:/desktop_folder/12_Claude_Code/pathway/notebooklm/v2_ingest.py:259), [experience_memory.py](D:/desktop_folder/12_Claude_Code/pathway/notebooklm/medical_pipeline_agent/scripts/experience_memory.py:101), [experience_memory.py](D:/desktop_folder/12_Claude_Code/pathway/notebooklm/medical_pipeline_agent/scripts/experience_memory.py:103), và [orchestrator.py](D:/desktop_folder/12_Claude_Code/pathway/notebooklm/medical_pipeline_agent/scripts/orchestrator.py:848).
- RRF đang hardcode `k=60` ở [medical_agent.py](D:/desktop_folder/12_Claude_Code/pathway/notebooklm/medical_agent.py:21) và merge ở [medical_agent.py](D:/desktop_folder/12_Claude_Code/pathway/notebooklm/medical_agent.py:26). `scoped_search` mặc định `top_k=8` ở [medical_agent.py](D:/desktop_folder/12_Claude_Code/pathway/notebooklm/medical_agent.py:262), không phải `top_k=5` cố định.
- Ở ontology hiện tại, các relation đã có chủ yếu là `PROTO_*` như [ontology_healthcare.md](D:/desktop_folder/12_Claude_Code/pathway/notebooklm/data/ontology_healthcare.md:68). Các relation sâu hơn như `DOSE_OF`, `INDICATION_FOR`, `CONTRA_INDICATES` hiện chưa thấy trong runtime graph hiện tại.

### Tóm tắt cho chuyên gia

- Điểm yếu chắc chắn nhất hiện nay là:
  - `chunking cố định`
  - `entity extraction bị truncate`
  - `embedding bị truncate`
  - `hardcode domain ở v2_ingest`
  - `ontology relation chưa đủ sâu`
- Điểm cần nói chính xác hơn:
  - `skip_first_page` không còn là lỗi của mọi pipeline vì đã có nhánh override
  - `top_k=5 cố định` là chưa đúng với code hiện tại; hiện phổ biến là `top_k=8`, còn vector/fulltext sub-search dùng các ngưỡng khác nhau

### Hướng sửa — ĐÃ HOÀN THÀNH (2026-03-28)

#### Fix 1: Bỏ truncate entity extraction `[:2000]`
- [universal_ingest.py](D:/desktop_folder/12_Claude_Code/pathway/notebooklm/universal_ingest.py) — dòng 756: `chunk_content[:2000]` → `chunk_content` (full chunk)
- [v2_ingest.py](D:/desktop_folder/12_Claude_Code/pathway/notebooklm/v2_ingest.py) — dòng 372: tương tự
- **Impact**: NER giờ thấy 100% chunk content thay vì 67%

#### Fix 2: Chunking domain-aware (không hardcode 3000/100)
- Thêm `DOMAIN_CHUNK_PROFILES` dict trong `PipelineConfigurator` — mỗi domain có `max_chunk_size` và `min_chunk_size` riêng
- TCM: 4000/150, Oncology: 3500/200, Surgery: 3000/150, Pediatrics: 2500/100
- Docs lớn >50 trang tự động +500 chars
- Thêm nhận diện flowchart heading pattern (`Lưu đồ \d+`)

#### Fix 3: Tách HepB ontology ra JSON catalog
- Tạo [config/ontology_catalog/hepb_ontology.json](D:/desktop_folder/12_Claude_Code/pathway/notebooklm/config/ontology_catalog/hepb_ontology.json) — chứa ICD hierarchy, drugs, lab_tests, stages
- `v2_ingest.py`: `setup_hepb_ontology()` → `setup_ontology_from_catalog()` — đọc từ JSON, không hardcode
- Hỗ trợ auto-discover catalog file theo bệnh

#### Fix 4: Thêm semantic relation types
- Entity extraction giờ trả về cả `entities` + `relations`
- 4 relation types mới: `INDICATION_FOR`, `CONTRA_INDICATES`, `DOSE_OF`, `RULE_OUT_FOR`
- Relations được MERGE vào graph với `detail` và `source_chunk`
- Áp dụng cho cả `v2_ingest.py` và `universal_ingest.py`
- Allowlist `ALLOWED_RELATION_TYPES` để prevent injection
