# Logseq Graph Architecture Research Report

Dựa trên việc phân tích mã nguồn repo **Logseq**, tôi đã trích xuất được những "bí kíp" kiến trúc giúp họ trở thành công cụ Knowledge Graph mạnh nhất hiện nay. Đây là những bài học có thể áp dụng trực tiếp để nâng cấp **AI Clinical Engine** của chúng ta.

## 1. Triết lý "Atomic Graph" (Mọi thứ là một Block)
Trong Logseq, trang (Page) thực chất cũng chỉ là một Block đặc biệt. 
- **Ưu điểm**: Cho phép trích dẫn đến từng dòng văn bản (Block Reference) thay vì phải trích dẫn cả file.
- **Ứng dụng cho Y khoa**: Chúng ta có thể trích dẫn chính xác một "liều dùng" hoặc "chỉ định" cụ thể trong phác đồ, thay vì chỉ dẫn link đến cả trang PDF dài 50 trang.

## 2. Datalog Engine (Datascript)
Logseq không dùng SQL truyền thống mà dùng **Datascript** - một cơ sở dữ liệu đồ thị trong bộ nhớ sử dụng ngôn ngữ truy vấn **Datalog**.
- **Truy vấn Đồ thị cực mạnh**: Datalog cho phép thực hiện các truy vấn đệ quy (recursive queries) như: "Tìm tất cả các bệnh có liên quan đến triệu chứng X, và các triệu chứng đó lại liên quan đến thuốc Y".
- **Hiệu năng**: Vì chạy trong RAM, việc chuyển đổi giữa các ghi chú diễn ra tức thì.

## 3. Kiến trúc Đồng bộ File-Mirroring
Logseq coi File Markdown là "Nguồn chân lý" (Source of Truth) và Đồ thị (Graph) là một "Cái gương" phản chiếu.
- **Parser thông minh**: Họ có một bộ `graph-parser` cực mạnh (tôi đã phân tích trong `deps/graph-parser`) chuyên quét markdown để tìm `[[link]]`, `#tag` và `key:: value`.
- **Properties**: Việc hỗ trợ thuộc tính dạng `dosage:: 500mg` giúp biến văn bản thuần túy thành dữ liệu có cấu trúc có thể tính toán được.

## 4. Đề xuất cho AI Clinical Engine (NotebookLM Clone)
Từ nghiên cứu này, chúng ta có thể cải tiến hệ thống hiện tại theo các hướng:
- **Granular Ingestion**: Thay vì nạp cả phác đồ vào Neo4j dưới dạng 1 nút lớn, hãy chia nhỏ thành các `Block` nodes (Chẩn đoán, Điều trị, Thuốc, v.v.)
- **Bi-directional Linking**: Tự động tạo liên kết ngược từ thuốc về phác đồ và từ triệu chứng về bệnh một cách tự động khi người dùng hỏi.
- **Datalog Querying**: Nghiên cứu sử dụng Datalog (qua thư mục `db` của Logseq) để thực hiện các suy luận y khoa phức tạp hơn là chỉ dùng Vector Search đơn thuần.

---
Báo cáo thực hiện bởi Antigravity Academic Agent.
