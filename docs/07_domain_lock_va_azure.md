# Domain lock và Azure OpenAI

## Domain lock

Không để agent có toàn quyền trên hệ thống.

Agent chỉ nên được:

- đọc prompt/case packet;
- gọi MCP Neo4j đã chỉ định;
- đọc kho tri thức trong `ai_claim/data/knowledge`;
- không được edit hệ thống production;
- không được web search ngoài domain;
- không được mở plugins, skills hay MCP ngoài domain.

## Azure OpenAI

Vì `agent_claude` bản release là binary của nhà sản xuất, không nên cố vá source để biến nó thành OpenAI client.

Hướng đúng trong `ai_claim`:

1. Giữ `agent_claude` release binary như một runner bị khoá chặt nếu còn cần benchmark.
2. Xây runtime suy luận mới của `ai_claim` dùng Azure OpenAI trực tiếp.
3. Di chuyển dần planner/synthesis sang runtime mới đó.

## Ý nghĩa

Như vậy:

- bảo mật tốt hơn;
- kiểm soát token tốt hơn;
- không phụ thuộc GLM hay Claude native;
- cloud deploy dễ hơn.
