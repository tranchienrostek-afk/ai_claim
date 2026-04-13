# Vì sao Pathway và agent_claude chênh lệch tư duy

## Kết luận ngắn

Pathway và `agent_claude` không khác nhau vì "một bên thông minh hơn tuyệt đối". Chúng khác nhau vì:

1. **bề mặt tri thức được mở ra khác nhau**;
2. **vòng lặp suy luận khác nhau**;
3. **telemetry và tool-use quan sát được khác nhau**;
4. **Pathway hiện nghiêng về deterministic graph reasoning**, còn `agent_claude` mạnh ở **planner + search + synthesis**.

## Pathway đang làm gì

Pathway hiện mạnh ở:

- chuẩn hoá dịch vụ, dấu hiệu, bệnh, quyền lợi, loại trừ;
- đưa tài liệu vào Neo4j khá ngăn nắp;
- giữ evidence, reasoning trace, verification plan;
- ra quyết định audit-friendly khi graph đã đủ mạnh.

Nhưng Pathway dễ hụt khi:

- disease coverage còn mỏng;
- ontology thiếu edge suy luận;
- cần nối nhiều nguồn dữ liệu song song;
- cần planner tự chọn nguồn nào phải mở trước.

## agent_claude đang làm gì

`agent_claude` trong benchmark cũ mạnh hơn vì:

- biết lập search plan trước;
- biết song song nhiều truy vấn MCP;
- khi graph không đủ, nó vẫn khái quát vấn đề từ pattern triệu chứng;
- biết synthesis và chấp nhận `review` khi clause bảo hiểm xung đột.

## Kết luận cơ chế

### Pathway

`input -> mapper -> graph query -> matrix/rule -> decision`

### agent_claude

`input -> mission lock -> search plan -> multi-query -> evidence ledger -> synthesis -> decision`

## Hàm ý kiến trúc cho ai_claim

`ai_claim` phải lấy:

- **graph discipline** từ Pathway;
- **planner discipline** từ `agent_claude`;
- và buộc mọi câu trả lời phải có:
  - tầng y khoa;
  - tầng bảo hiểm;
  - tầng bằng chứng;
  - tầng human review.
