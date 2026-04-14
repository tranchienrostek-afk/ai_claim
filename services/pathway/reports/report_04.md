# Report 04

## 2026-03-29

## Update 2026-03-30

### Tình hình mới sau khi chuyên gia vá graph adjudication

Bản báo cáo bên dưới không còn đúng hoàn toàn cho trạng thái mới nhất.
Sau khi kiểm lại code và artifact mới, mình xác nhận hệ đã có tiến bộ thật ở adjudication layer.

Nguồn kiểm:
- [testcase_trace_runner.py](D:/desktop_folder/12_Claude_Code/pathway/notebooklm/server_support/testcase_trace_runner.py)
- [trace_post_graph_fix_kich_ban_11/trace_summary.json](D:/desktop_folder/12_Claude_Code/pathway/notebooklm/data/script/testcase_trace_runs/trace_post_graph_fix_kich_ban_11/trace_summary.json)
- [trace_post_graph_fix_kich_ban_11/tc_onto_007_trace.json](D:/desktop_folder/12_Claude_Code/pathway/notebooklm/data/script/testcase_trace_runs/trace_post_graph_fix_kich_ban_11/tc_onto_007_trace.json)
- [trace_post_graph_fix_batch](D:/desktop_folder/12_Claude_Code/pathway/notebooklm/data/script/testcase_trace_runs/trace_post_graph_fix_batch)

### 1. Những gì đã tốt lên thật

#### 1.1. `tc_onto_007` đã tăng từ `0/4` lên `3/4`

Từ [trace_post_graph_fix_kich_ban_11/trace_summary.json](D:/desktop_folder/12_Claude_Code/pathway/notebooklm/data/script/testcase_trace_runs/trace_post_graph_fix_kich_ban_11/trace_summary.json):
- `service_label_match_total = 3`
- `service_label_accuracy = 0.75`

Từ [trace_post_graph_fix_kich_ban_11/tc_onto_007_trace.json](D:/desktop_folder/12_Claude_Code/pathway/notebooklm/data/script/testcase_trace_runs/trace_post_graph_fix_kich_ban_11/tc_onto_007_trace.json):
- line 1 `AFB đờm và PCR lao` -> `PAYMENT`
- line 2 `CT Scan lồng ngực` -> `PAYMENT`
- line 3 `Nuôi cấy đờm + KSĐ` -> `PAYMENT`
- line 4 `Ceftazidim` -> `REVIEW`

Điều này xác nhận chuyên gia nói đúng ở ý chính:
- graph expected service đã cứu được 3 line đầu
- thất bại còn lại tập trung ở service mapping upstream

#### 1.2. Adjudication layer đã thật sự dùng graph

Trong [testcase_trace_runner.py](D:/desktop_folder/12_Claude_Code/pathway/notebooklm/server_support/testcase_trace_runner.py):
- có `_query_graph_expected_services(...)`
- query Neo4j bằng `DISEASE_EXPECTS_SERVICE`
- `_build_service_line_trace(...)` có boost support theo `role`

Vai trò đã thấy trong explanation của `tc_onto_007`:
- `rule_out`
- `severity`
- `confirmatory`

Tức là phần graph-enhanced adjudication không còn là ý tưởng nữa; nó đã đi vào code chạy thật.

#### 1.3. Specialty gating cũng đã vào code

Trong [testcase_trace_runner.py](D:/desktop_folder/12_Claude_Code/pathway/notebooklm/server_support/testcase_trace_runner.py), `_select_focus_hypothesis(...)` hiện đã có:
- lookup specialty từ graph
- `_SPECIALTY_GROUPS`
- fallback sang hypothesis cùng nhóm specialty nếu free top-1 lệch domain

### 2. Những gì vẫn còn tệ

#### 2.1. Nút thắt lớn nhất vẫn là service mapping upstream

`tc_onto_007` line 4 vẫn fail vì:
- `Ceftazidim` bị map thành `IMG-USG-136`
- tức là thuốc kháng sinh bị map sang nhánh siêu âm

Các case khác trong batch cũng lộ lỗi tương tự:
- `Phản ứng ASLO` bị kéo sang `CRP`
- một số vi sinh/phết dịch họng vẫn đi sai code

Nghĩa là:
- adjudication layer đã đỡ hơn rõ
- nhưng nếu code dịch vụ sai, graph lookup sẽ không cứu được

#### 2.2. Batch aggregate đúng, nhưng summary artifact đang stale

Chuyên gia nói `18/33 = 54.5%`.
Mình đã tự cộng lại từ các file `*_trace.json` trong [trace_post_graph_fix_batch](D:/desktop_folder/12_Claude_Code/pathway/notebooklm/data/script/testcase_trace_runs/trace_post_graph_fix_batch) và xác nhận:
- `18/33`
- `accuracy = 0.5455`

Nhưng file [trace_post_graph_fix_batch/trace_summary.json](D:/desktop_folder/12_Claude_Code/pathway/notebooklm/data/script/testcase_trace_runs/trace_post_graph_fix_batch/trace_summary.json) hiện đang stale:
- chỉ ghi `case_count = 1`
- `service_line_count = 4`

Trong khi folder thật chứa `8` file `*_trace.json`.

#### 2.3. Một phần graph evidence mới chỉ nằm trong explanation text

Ở `tc_onto_007_trace.json`, explanation đã có:
- `role=rule_out`
- `role=severity`
- `role=confirmatory`

Nhưng các field cấu trúc:
- `graph_match_role`
- `graph_match_category`
- `graph_match_service_code`

hiện vẫn là `null`.

### 3. Đánh giá cập nhật

Bản `report_04` gốc đã quá bi quan cho trạng thái mới nhất.
Đánh giá công bằng hơn lúc này là:

- adjudication layer đã tốt lên thật
- graph lookup đang tạo giá trị thật
- bottleneck chính đã dịch chuyển lên service mapping upstream

### 4. Việc nên làm tiếp ngay

1. Rebuild summary cho [trace_post_graph_fix_batch](D:/desktop_folder/12_Claude_Code/pathway/notebooklm/data/script/testcase_trace_runs/trace_post_graph_fix_batch) để aggregate không còn stale.
2. Ghi `graph_match_role / graph_match_service_code / graph_match_category` thành field thật trong trace JSON, không chỉ để trong explanation string.
3. Vá upstream mapper cho nhóm:
   - `Ceftazidim`
   - `ASLO`
   - `Phết dịch họng nuôi cấy`
4. Tách hẳn `drug/medication` ra khỏi mapper dịch vụ thủ thuật/cận lâm sàng.

### Kết luận ngắn

Kết quả hiện tại **chưa đạt yêu cầu thực chiến**.  
Hệ đã có nhiều lớp công cụ đẹp như `ontology_v2`, `graph retriever`, `memory`, `planning trace`, `adjudication trace`, nhưng chất lượng quyết định cuối vẫn yếu ở các ca khó, và đang có dấu hiệu:

- kiến trúc mạnh hơn dữ liệu thật
- UI/trace nhìn được nhiều hơn, nhưng chưa kéo được chất lượng suy luận lên tương xứng
- một số benchmark trước đây nhìn đẹp nhưng **không đại diện** cho ca khó thực tế

### 1. Bằng chứng thất bại mới nhất

Nguồn chính:
- [trace_summary.json](D:/desktop_folder/12_Claude_Code/pathway/notebooklm/data/script/testcase_trace_runs/testcase_trace_kich_ban_11_20260329_232911/trace_summary.json)
- [tc_onto_007_trace.json](D:/desktop_folder/12_Claude_Code/pathway/notebooklm/data/script/testcase_trace_runs/testcase_trace_kich_ban_11_20260329_232911/tc_onto_007_trace.json)
- [tc_onto_007_trace.md](D:/desktop_folder/12_Claude_Code/pathway/notebooklm/data/script/testcase_trace_runs/testcase_trace_kich_ban_11_20260329_232911/tc_onto_007_trace.md)

Case `tc_onto_007`:
- Bệnh đích: `Viêm phổi do Burkholderia pseudomallei (Melioidosis)`
- Sau khi vá anchor, trace đã neo đúng về `Viêm phổi`
- Nhưng kết quả thẩm định vẫn **rất kém**

Số liệu:
- `top1_disease = Viêm phổi`
- `free_top1_disease = Viêm tai giữa mạn tính có cholesteatoma`
- `anchor_mode = known_disease`
- `service_label_accuracy = 0.0`
- `ground_truth_match_count = 0/4`
- `proposed_payment_count = 0`
- `proposed_review_count = 4`

Nói thẳng:
- hệ đã biết phải nhìn về `Viêm phổi`
- nhưng **vẫn không biết vì sao 4 dịch vụ đều nên được PAYMENT**
- tức là phần “focus disease” chỉ mới sửa hướng nhìn, chưa sửa được năng lực adjudication thật

### 2. Các vấn đề chính

#### 2.1. Graph retrieval có, nhưng disease-specific reasoning còn rất nông

Hiện tại graph retriever đã kéo được `Viêm phổi`, nhưng evidence còn quá mỏng:
- `top_hypothesis.score = 0.36`
- evidence chính chỉ là `graph_context_seed`
- chưa có đủ `ProtocolAssertion` hay `expected service` đủ đặc hiệu cho ca Melioidosis

Hậu quả:
- hệ biết “đây là viêm phổi”
- nhưng chưa biết “trong ca này, AFB/PCR lao, CT ngực, nuôi cấy đờm, Ceftazidime là những service hợp lý y khoa”

#### 2.2. Free reasoning còn nhiễu nặng

Trong cùng case:
- `free_top1_disease = Viêm tai giữa mạn tính có cholesteatoma`

Đây là nhiễu rất nặng, vì case là hô hấp/truyền nhiễm nhưng sign-engine vẫn kéo lên bệnh TMH.

Điều này cho thấy:
- sign reasoning hiện chưa được ràng specialty/domain tốt
- hệ vẫn có xu hướng match pattern text rời rạc, chưa đủ ngữ cảnh lâm sàng tổng thể

#### 2.3. Service mapping còn yếu đúng chỗ quan trọng

Ví dụ ở `tc_onto_007`:
- `AFB đờm và PCR (Gen Xpert) lao`
  - bị map sang `LAB-MIC-093`
  - `mapping_resolution = unknown`
  - `mapping_confidence = REVIEW`
- `CT Scan lồng ngực`
  - map code được `IMG-CTN-053`
  - nhưng không kéo được strong support từ disease/service layer

Nghĩa là:
- có service map được code
- nhưng code đó chưa nối tốt với `disease-specific expected service`
- và với service khó hơn thì mapping vẫn còn lửng

#### 2.4. Adjudication layer vẫn quá bảo thủ

Hiện line nào không có support đủ mạnh thì hệ đẩy sang `REVIEW`.

Về an toàn thì ổn, nhưng về năng lực nghiệp vụ thì kém:
- `0 PAYMENT`
- `4 REVIEW`

Trong một case mà ground truth là `4/4 PAYMENT`, hệ hiện tại **chưa làm được adjudication thật**, mới làm được triage an toàn.

#### 2.5. Memory gần như vô tác dụng ở ca khó này

Trong run mới:
- `memory_match_count = 0`

Điều này cho thấy:
- memory hiện hữu ích hơn ở các ca TMH đã thấy trước
- chưa giúp được gì đáng kể cho case hô hấp/truyền nhiễm mới, đặc biệt là disease-specific adjudication

### 3. Những benchmark trước đây đang “đẹp hơn thực lực”

Nguồn:
- [testcase_11_hypothesis_batch/batch_summary.json](D:/desktop_folder/12_Claude_Code/pathway/notebooklm/data/script/testcase_11_hypothesis_batch/batch_summary.json)
- [testcase_11_batch/batch_summary.json](D:/desktop_folder/12_Claude_Code/pathway/notebooklm/data/script/testcase_11_batch/batch_summary.json)

Các số đẹp trước đây:
- `5/5 top1 hit`
- `5/5 top3 hit`

Nhưng cần nói thật:
- đó là trên bộ `testcase_11` cũ thiên về TMH, nhiều case đã được tune/profile hóa
- ngay trong batch này, `graph_context = 0`
- `service mapping` vẫn yếu:
  - `base_high_confidence_total = 1/15`
  - `hybrid_high_confidence_total = 1/15`

Nghĩa là:
- disease label benchmark có thể đẹp
- nhưng service adjudication benchmark vẫn yếu
- và khi gặp case khác domain hơn như `Melioidosis mimicking TB`, hệ lộ điểm yếu ngay

### 4. Chẩn đoán nguyên nhân gốc

#### 4.1. Ontology có hình, chưa đủ chiều sâu

Hiện ta đã có:
- disease
- summary
- assertion
- graph retriever
- memory
- trace UI

Nhưng chưa có đủ độ dày ở:
- `disease -> expected services`
- `service -> indication / rule-out purpose`
- `differential diagnosis workflow`
- `organism-specific treatment logic`

Ca này đòi hỏi đúng những lớp đó:
- dùng AFB/PCR để loại trừ lao
- dùng nuôi cấy để xác định vi khuẩn
- dùng CT ngực để đánh giá tổn thương
- dùng Ceftazidime vì nghi Melioidosis

Hệ chưa biểu diễn sâu được chuỗi này.

#### 4.2. Mapping và reasoning vẫn chưa tách sạch

Hiện nhiều chỗ vẫn là:
- map alias yếu
- rồi reasoning cố cứu bằng fuzzy overlap

Đây là lý do:
- trace nhìn có vẻ thông minh
- nhưng support score thực ra rất thấp, ví dụ `0.05`

#### 4.3. Trace tốt hơn model

UI, logs, planning, chat trace đã khá tốt.
Nhưng thực tế:
- khả năng “trình bày quá trình suy luận” đang đi nhanh hơn
- khả năng “suy luận đúng” đang đi chậm hơn

Nguy cơ:
- hệ trông thông minh
- nhưng chất lượng decision chưa tương xứng

### 5. Đánh giá thẳng tay

Hiện trạng không thể gọi là:
- production-ready
- reviewer-ready
- adjudication-grade

Hiện chỉ có thể gọi là:
- `good research prototype`
- `debuggable reasoning sandbox`
- `ontology-first experimental system`

### 6. Ưu tiên sửa đúng

#### Ưu tiên 1: Service adjudication theo disease-specific protocol

Phải build thật:
- `Disease -> ExpectedService`
- `Disease -> RuleOutService`
- `Disease -> ConfirmatoryService`
- `Disease -> TreatmentService`

Không đủ lớp này thì adjudication sẽ mãi rơi vào `REVIEW`.

#### Ưu tiên 2: Respiratory / infectious ontology fill

Không thể tiếp tục chỉ khỏe ở TMH rồi kỳ vọng suy luận tốt cho ca hô hấp.

Cần ingest sâu thêm cho:
- viêm phổi
- lao phổi
- melioidosis
- viêm phế quản phổi
- nhiễm khuẩn huyết liên quan

#### Ưu tiên 3: Service purpose modeling

Một service không chỉ là “có trong protocol hay không”.
Nó phải có vai trò:
- screening
- rule-out
- confirmatory
- severity assessment
- treatment

Ví dụ:
- `AFB/PCR lao` trong case này là service hợp lý vì differential diagnosis, không phải vì nó “xác nhận melioidosis” trực tiếp

#### Ưu tiên 4: Specialty/domain gating cho sign reasoning

Không thể để case hô hấp bị kéo top-1 tự do về bệnh TMH.
Phải có:
- specialty prior
- disease family gating
- contradiction penalty theo organ system

### 7. Câu chốt

Hệ hiện tại **đã có bộ khung rất tốt để phát triển tiếp**, nhưng kết quả ở ca khó mới nhất chứng minh rõ:

- search có rồi
- graph có rồi
- memory có rồi
- reasoning trace có rồi

nhưng **medical adjudication quality vẫn chưa đủ tốt**.

Nói cách khác:

> Ta đã xây được một hệ thống biết “tự kể mình đang nghĩ gì”,  
> nhưng chưa đủ giỏi để nghĩ đúng một cách ổn định ở ca khó.

Đó là vấn đề cần sửa ngay.
