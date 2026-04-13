# Ingest nên để Pathway hay agent

## Kết luận ngắn

### Đưa tài liệu vào Neo4j

Ưu tiên **Pathway**.

Lý do:

- Pathway mạnh ở ingest có cấu trúc;
- đã có mapper dịch vụ, dấu hiệu, phác đồ, insurance;
- dễ giữ provenance, version, report;
- deterministic hơn, phù hợp build graph.

### Suy luận trên graph

Ưu tiên **agent runtime kiểu agent_claude**.

Lý do:

- planner tốt hơn;
- biết mở nhiều nguồn evidence song song;
- synthesis linh hoạt hơn;
- xử lý case phức tạp và conflict clause tốt hơn.

## Quy tắc vận hành đích

- `Pathway = graph compiler`
- `agent runtime = graph reasoner`

Đây là cách chia vai an toàn và hiệu quả nhất.
