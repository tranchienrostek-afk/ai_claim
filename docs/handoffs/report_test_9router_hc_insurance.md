# Báo cáo khó khăn khi test `agent_claude` cho bài toán y tế, bảo hiểm qua `9router + Azure + MCP Neo4j`

## 1. Trạng thái hiện tại

- Ngày kiểm tra: `2026-04-13`
- Hạ tầng đã lên và chạy được:
  - `9router` sống
  - `azure proxy` sống
  - `Pathway API` sống
  - `Neo4j` sống và có dữ liệu thật
- Chuỗi kỹ thuật đã đi được:
  - `Claude Code -> 9router -> Azure -> MCP pathway-neo4j -> final JSON`

## 2. Các run đã dùng để đánh giá

- Ménière:
  - `D:\desktop_folder\01_claudecodeleak\pathway\notebooklm\data\duel_runs\pathway_vs_agent_claude\20260413_094909_duel_meniere_001`
- Pneumonia:
  - `D:\desktop_folder\01_claudecodeleak\pathway\notebooklm\data\duel_runs\pathway_vs_agent_claude\20260413_095219_duel_pneumonia_002`

## 3. Khó khăn lớn nhất hiện nay

### 3.1. Hạ tầng đã thông, nhưng benchmark vẫn chưa pass

- Đây không còn là lỗi kết nối.
- `agent_claude` đã gọi MCP thật trong case `pneumonia`, không còn kẹt ở `tool_calls`.
- Tuy nhiên chưa có testcase nào pass hoàn toàn theo benchmark hiện tại.

### 3.2. Lệch schema quyết định giữa `agent_claude` và benchmark

- Benchmark mong các nhãn kiểu:
  - `approve`
  - `deny`
  - `review`
  - `partial_pay`
- `agent_claude` hiện trả nhiều nhãn khác:
  - `medically_reasonable`
  - `medically_necessary`
  - `pay`
  - `pay_full`
  - `not_medically_indicated`
  - `not_indicated`
  - `conditional_indication`
- Vì vậy nhiều line có logic khá đúng nhưng vẫn bị chấm `mismatch`.

### 3.3. Graph coverage y khoa còn thiếu ở đúng các chỗ benchmark cần

- Ở case `pneumonia`, ngay lượt MCP đầu:
  - `mcp__pathway-neo4j__query_disease_services` với `disease_name = pneumonia`
  - trả về `[]`
- Điều này cho thấy graph hiện chưa có coverage bệnh - dịch vụ đủ tốt cho ca này, dù các lớp bảo hiểm vẫn trả được dữ liệu.
- Kết quả là model phải suy luận nhiều hơn từ text và benefit chung, thay vì bám disease-service edges mạnh.

### 3.4. Case Ménière ra kết quả khá hợp lý nhưng chưa thật sự graph-grounded

- Run Ménière cho kết quả đầu ra dùng được, thậm chí claim-level đúng.
- Nhưng run này không phải mẫu MCP/Neo4j đẹp để dùng làm bằng chứng “đã grounded hoàn toàn”.
- Vì vậy nó không phải run chốt để chứng minh hệ đã ổn định end-to-end.

### 3.5. Dữ liệu insurance query còn nhiễu

- Query `query_benefits_for_contract(FPT-NV)` trả về rất nhiều dòng benefit không liên quan trực tiếp ca bệnh.
- Model vẫn xoay xở được, nhưng retrieval chưa đủ sắc.
- Cần lớp lọc benefit/clause theo ngữ cảnh y khoa, dịch vụ, plan và role tốt hơn để bớt nhiễu khi reasoning.

### 3.6. Một số quyết định lâm sàng còn bảo thủ hơn gold

- Case `pneumonia`:
  - `CBC`, `CRP`, `X-quang ngực` được đánh giá hợp lý
  - nhưng `Cấy đàm và kháng sinh đồ` bị đưa về `review`
- Gold benchmark lại muốn `approve`.
- Đây không hẳn là lỗi kỹ thuật thuần túy; nó là chênh lệch giữa:
  - tiêu chuẩn suy luận của model
  - tiêu chuẩn đáp án benchmark

### 3.7. Encoding tiếng Việt trong artifact vẫn còn xấu

- Nhiều file log/result có lỗi hiển thị dấu tiếng Việt.
- Điều này không làm hỏng logic chính, nhưng gây khó đọc khi bàn giao và kiểm tra evidence.

## 4. Kết quả chấm điểm hiện tại

### 4.1. Ménière

- `agent_claude.weighted_score = 0.50`
- `accuracy = 0.3333`
- `claim_level_match = true`
- `disease_match = true`
- `line_all_matches = 0 / 4`

Nhận xét:
- đầu ra có giá trị sử dụng
- nhưng chưa pass benchmark
- chưa phải run MCP-grounded đẹp

### 4.2. Pneumonia

- `agent_claude.weighted_score = 0.41`
- `accuracy = 0.2857`
- `claim_level_match = true`
- `disease_match = true`
- `line_all_matches = 0 / 5`

Nhận xét:
- đây là run tốt nhất về mặt kỹ thuật vì MCP Neo4j đã được dùng thật
- nhưng vẫn chưa pass do:
  - lệch label schema
  - graph disease-service còn thiếu
  - 1 line bị `review` thay vì `approve`

## 5. Điều gì đã được giải quyết xong

- `9router` đã bật được và route được model
- Azure proxy đã trả lời ổn
- MCP server Neo4j không còn chết giả kiểu handshake OK nhưng tool call hỏng
- Neo4j đã được bật lại và `graph-operating/health` trả dữ liệu sống
- `agent_claude` đã đi hết chuỗi tool-use với MCP ở case `pneumonia`

## 6. Điều gì vẫn còn là khó khăn thật

- Chưa có testcase nào pass hoàn toàn theo benchmark
- Graph y khoa chưa phủ đủ các disease-service edges cho các case benchmark
- Scoring layer chưa normalize verdict của `agent_claude` sang schema benchmark
- Retrieval insurance còn nhiễu, chưa đủ context-aware
- Một phần output tốt về chuyên môn nhưng vẫn bị benchmark chấm trượt vì khác nhãn

## 7. Kết luận ngắn

- Hệ thống hiện đã chạy được end-to-end.
- Khó khăn còn lại nằm ở:
  - chất lượng graph
  - chuẩn hóa verdict
  - độ sắc của retrieval
- Không nên tiếp tục đổ lỗi cho server hay route.
- Muốn pass benchmark, ưu tiên đúng là:
  1. normalize label của `agent_claude`
  2. bơm thêm coverage disease-service vào Neo4j
  3. tinh lại retrieval/grounding cho benefit, exclusion, clause

