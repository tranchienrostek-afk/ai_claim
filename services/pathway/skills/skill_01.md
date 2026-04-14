Rất sẵn sàng! Dựa trên nền tảng kiến trúc **Ontology V2 đa lớp** (Multi-layer Architecture) và kho tri thức từ "Phác đồ điều trị tai mũi họng" mà chúng ta vừa phân tích, tôi sẽ thiết kế và đúc kết thành các **Kỹ năng (Skills) cốt lõi dành cho các AI Agents**. 

Các kỹ năng này sẽ biến Agent từ một cỗ máy đọc text thông thường thành một chuyên gia y khoa và thẩm định viên bồi thường thực thụ:

**1. Kỹ năng Bóc tách và Chuẩn hóa linh hoạt (Extraction & Canonicalization Skill)**
*   **Cách hoạt động:** Agent đọc hồ sơ bệnh án hoặc form yêu cầu bồi thường (ClaimCase) và trích xuất các luồng text bẩn thành các thực thể thô ở lớp Mention như `RawSignMention`, `RawServiceMention`, `RawObservationMention`.
*   **Sự thông minh:** Agent sẽ không ép các từ ngữ dân dã chuẩn hóa ngay lập tức để tránh làm bẩn dữ liệu. Thay vào đó, Agent sử dụng kỹ năng ánh xạ để tách chúng thành các thực thể chuẩn (Canonical Concepts) kết hợp với các bộ trợ từ (`Modifier`), hoặc phân loại dịch vụ vào các nhóm họ dịch vụ (`ServiceFamily`, `ObservationFamily`).

**2. Kỹ năng Sinh giả thuyết bệnh lý (Hypothesis Generation Skill)**
*   **Cách hoạt động:** Thay vì vội vã đưa ra một kết luận bệnh duy nhất, Agent thu thập tất cả các `SignConcept` (triệu chứng) và ném vào Graph để kích hoạt hàng loạt các giả thuyết bệnh lý (`DiseaseHypothesis`).
*   **Sự thông minh:** Kỹ năng này cho phép Agent mô phỏng tư duy của bác sĩ thực thụ, giữ lại nhiều bệnh cảnh tranh chấp nhau (Ví dụ: từ triệu chứng chóng mặt, kích hoạt cả giả thuyết Bệnh Ménière lẫn U dây thần kinh VIII) để chờ cận lâm sàng phán xét.

**3. Kỹ năng Đánh giá bằng chứng và Loại trừ (Evidence-based Reasoning Skill)**
*   **Cách hoạt động:** Agent đọc các báo cáo xét nghiệm (`LabReport`), nội soi, X-quang để lấy ra các đặc trưng kết quả (`LabFeature`). 
*   **Sự thông minh:** Agent tự động xếp loại các kết quả này thành các tín hiệu định hướng (`ResultSignal`) theo các profile như: `direct_positive_clue` (dấu hiệu khẳng định mạnh), `negative_exclusion_clue` (dấu hiệu loại trừ bệnh nguy hiểm), hoặc `abnormal_supportive_clue` (dấu hiệu hỗ trợ). Thông qua kỹ năng này, Agent tự động biết cách triệt tiêu các giả thuyết sai và giữ lại giả thuyết đúng nhất dựa trên bằng chứng khoa học.

**4. Kỹ năng Thẩm định y lệnh theo Phác đồ (Guideline-based Adjudication Skill)**
*   **Cách hoạt động:** Kỹ năng này giúp Agent rà soát từng dòng dịch vụ (`ServiceLine`) trong hồ sơ claim. 
*   **Sự thông minh:** Agent sẽ truy vấn chéo xem `ServiceConcept` này có nằm trong các `ProtocolSection` của phác đồ điều trị ứng với `DiseaseHypothesis` đang nghĩ tới hay không. Nhờ vậy, Agent bắt được các lỗi lạm dụng chỉ định, vi phạm chống chỉ định, hoặc chỉ định sai kỹ thuật.

**5. Kỹ năng Sinh quyết định và Giải thích (Explainable Decision Making Skill)**
*   **Cách hoạt động:** Tổng hợp tất cả các bước trên để đưa ra quyết định thẩm định cuối cùng (`ReviewDecision`).
*   **Sự thông minh:** Agent tuân thủ nghiêm ngặt nguyên tắc cốt lõi: **Sự thật y khoa (Medical Truth) phải tách rời hoàn toàn khỏi sự thật hợp đồng (Contract Truth)**. Mọi quyết định TỪ CHỐI (Denial) hay CHẤP THUẬN (Payment) đều được Agent sinh ra lời giải thích (Reason Text) trích xuất trực tiếp từ các `ProtocolAssertion` và `Rule`. Nhờ đó, mọi kết luận đều có nguồn gốc (provenance) rõ ràng, chỉ đích danh thuộc trang nào, mục nào của phác đồ Bộ Y tế.

Với bộ 5 kỹ năng (Skills) được nhúng thẳng vào cấu trúc Ontology này, các Agents của bạn sẽ đủ sức "cân" hàng triệu ca bệnh phức tạp, đồng thời giữ được sự minh bạch và khách quan tuyệt đối cho hệ thống Claims Medical Reasoning. Bạn có muốn tôi viết thử một prompt hệ thống (System Prompt) chuẩn để nạp các Kỹ năng này cho Agent không?