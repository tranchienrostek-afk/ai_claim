# 📘 Cẩm Nang Sử Dụng ai_claim (Vietnamese)

Chào mừng bạn đến với **ai_claim** - Hệ thống thẩm định bồi thường bảo hiểm thông minh. Tài liệu này giúp bạn hiểu rõ các tính năng, cách vận hành và cấu hình của hệ thống.

---

## 🚀 1. Tổng Quan Hệ Thống

`ai_claim` không phải là một chatbot thông thường. Nó là một **Reasoning Agent** (Tác nhân suy luận) kết hợp giữa hai công nghệ mạnh mẽ nhất hiện nay:
1.  **Knowledge Graph (Neo4j)**: Đồ thị tri thức chứa các mối liên hệ cứng giữa bệnh tật, dịch vụ y tế và điều khoản bảo hiểm.
2.  **RAG (Pathway)**: Hệ thống tìm kiếm theo ngữ nghĩa trong các tài liệu văn bản (PDF, Quy trình y khoa).

**Mục tiêu**: Giúp các chuyên viên bồi thường ra quyết định chính xác, có dẫn chứng và minh bạch.

---

## 🎨 2. Các Tính Năng Cốt Lõi

### 📚 Quản Lý Tri Thức (Knowledge Registry)
- **Chức năng**: Nơi tập hợp các "nguyên liệu" tri thức của bạn. 
- **Cách dùng**: Upload tài liệu (Quy trình y khoa, Bảng giá, Hợp đồng mẫu) vào các nhóm (Roots) tương ứng.
- **Tại sao cần?**: Để AI có dữ liệu gốc để đối chiếu khi suy luận.

### 🔗 Cầu Nối Dữ Liệu (Pathway Bridge)
- **Chức năng**: Đẩy tài liệu từ thư viện nội bộ lên hệ thống tìm kiếm Vector của Pathway.
- **Cách dùng**: Nhấp vào "Bridge to Pathway".
- **Tại sao cần?**: Để AI có thể "đọc hiểu" và trích dẫn trực tiếp từ văn bản khi trả lời.

### 🤖 Trung Tâm Thẩm Định (Command Center)
- **Chức năng**: Chạy thử nghiệm suy luận trên một hồ sơ bệnh án (Case JSON).
- **Cách dùng**: Nhập đường dẫn file Case và nhấn "Chạy Duel Live".
- **Kết quả**: Bạn sẽ thấy sự so sánh giữa 2 cách tiếp cận: AI suy luận theo đồ thị tri thức vs AI tìm kiếm văn bản đơn thuần.

### 🔍 Tìm Kiếm Bề Mặt (Knowledge Surface)
- **Chức năng**: Tìm nhanh thông tin liên quan đến một bệnh lý hoặc mã ICD10.
- **Lợi ích**: Giúp bạn kiểm tra nhanh xem trong kho dữ liệu của mình có hướng dẫn gì về bệnh đó không.

### 🎯 Kiểm Toán Đồ Thị (Graph Audit)
- **Chức năng**: Kiểm tra tính chính xác của dữ liệu trong Neo4j.
- **Lợi ích**: Phát hiện các mã bệnh bị trùng hoặc thiếu thông tin ánh xạ trước khi đưa vào vận hành thực tế.

---

## ⚙️ 3. Hướng Dẫn Cấu Hình

Mọi thông tin kết nối được lưu trong file `.env` (hoặc `.env.local` khi chạy máy cá nhân).

| Biển Môi Trường | Ý Nghĩa | Cần lấy ở đâu? |
| :--- | :--- | :--- |
| `AZURE_OPENAI_API_KEY` | Chìa khóa bộ não AI | Azure OpenAI Studio |
| `AZURE_OPENAI_ENDPOINT`| Địa chỉ kết nối AI | Azure OpenAI Studio |
| `NEO4J_URI` | Địa chỉ database đồ thị | Instance Neo4j (Local hoặc Aura) |
| `PATHWAY_API_BASE_URL` | Địa chỉ dịch vụ tìm kiếm | Server Pathway đang chạy |

---

## 🛠 4. Luồng Công Việc Chuẩn (Standard Workflow)

1.  **BƯỚC 1: NẠP LIÊU** -> Upload tài liệu y khoa vào `protocols` hoặc `diseases`.
2.  **BƯỚC 2: ĐỒNG BỘ** -> Nhấn "Bridge to Pathway" để AI học dữ liệu mới.
3.  **BƯỚC 3: KIỂM TRA** -> Chạy "Graph Audit" để đảm bảo dữ liệu đồ thị Neo4j đã sẵn sàng.
4.  **BƯỚC 4: VẬN HÀNH** -> Chạy "Live Duel" để bắt đầu thẩm định hồ sơ bệnh án.

---

> [!TIP]
> Nếu hệ thống báo **"Degraded"** trên thanh trạng thái, hãy kiểm tra lại kết nối mạng tới Neo4j hoặc Pathway trong Dashboard Technical Pulse.
