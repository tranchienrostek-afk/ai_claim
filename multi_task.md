Hãy cho tôi biết vì sao lại có sự chênh lệch giữa tư duy của pathway và agent_claude đến thế.
Tôi cho cả 2 cùng đọc neo4j và agent_claude lại tìm nó thông minh và sáng suốt hơn.
Tôi là người mạnh về logic và toán học, tôi cần hiểu cơ chế suy luận chính xác của 2 thứ đó.

Sau đó tôi cần bạn thực hiện các kế hoạch sau.
Tính toán thời gian, số lượng tokens của pathway và agent_claude khi đã thực hiện nhiệm vụ đó, số vòng lặp, số lần call thực hiện thử thách khi call vào neo4j và call vào mô hình LLM.
Call vào neo4j thì thoải mái, vì của nhà làm được. Call vào llm thì mất tiền nên tôi cũng cần tính toán số tokens input và output.

Sau đó thôi cần thực hiện xây dựng kiến trúc lấy hết luồng suy nghĩ của pathway để đưa vào agent_claude, vì thực sự chỉ có agent_claude mới giải quyết được những bài toán phức tạp này. 
Chứ pathway có sửa mãi, có tối ưu thì vẫn kiểu không thông minh bằng agent-claude ấy.
Khi chuyển toàn bộ quy trình nghiệp vụ của pathway vào agent_claude, tiến hành tắt toàn bộ các tool, các skills, các mcp không liên quan đến nhiệm vụ y tế và bảo hiểm của ta.
Chứ cho agent_claude toàn quyền vào hệ thống, thêm, sửa, xoá, lùng sục khắp nơi đôi khi lại nguy hiểm.
Tôi cần tắt và buộc những cánh tay, nhưng năng lực vượt ngoài phạm vi domain y tế vào bảo hiểm lại.
Sau khi ghép nối thành công, tiến hành thay bộ não cho agent_claude. Vì sao, vì api call llm của agent_claude đang có thể là: GLM 5, Claude AI Opus, sonnet. Đó là những mô hình đắt tiền hoặc chưa kiểm định được tính bảo mật.
Hãy cấu hình cho tôi call vào mô hình LLM OPENAI đang đặt trên azure, đó là các mô hình mà pathway đang dùng.

Sau đó kiểm tra tính năng của agent_claude khi nhận nhiệm vụ mới.
Đưa phác đồ pdf, tiến hành chuyển vào neo4j => nên chọn pathway hay agent_claude
Đưa danh sách dịch vụ excel, đưa điều khoản bảo biểm, hợp đồng bảo hiểm, loại trừ... vào neo4j. Hiện hình như pathway làm luồng đưa vào neo4j ngon rồi thì phải.

Đưa hết vào và kiểm tra mapper, các liên kết y tế, hợp đồng...
Tóm lại tất cả các API đưa tài liệu y tế vào neo4j để lưu trữ pathway làm cũng được, agent_claude làm cũng được.
Nhưng trước khi đưa vào cần lên rõ danh sách các tiêu chí để ta đánh giá, về những gì ta mong muốn khi chuyển vào neo4j, để sau này ta ta suy luận trong đó cho dê dàng, chính xác, và có đường có hướng, có phương pháp luận.

Bài toán số 2, cung cấp thông tin đầu vào và tiến hành suy luận.
Đây là phần mà chúng ta cần chuyển toàn bộ pathway vào agent_claude. 
Một người khi mắc phải triệu chứng, kết hợp tiền sử bệnh lý vào viện khám.
Bác sĩ chưa biết đó là bệnh gì, bác sĩ cho đi làm xét nghiệm, làm nhiều xét nghiệm.
Khi nhận kết quả xét nghiệm, có các chỉ số, có thứ âm tính, có thứ dương tính.
Bác sĩ chuẩn đoán người đó bị bệnh gì. và loại đi các trường hợp bệnh khác.

Việc trong y tế khi chưa phát hiện ra bệnh, từ triệu chứng ban đầu, bác sĩ cho khám các dịch vụ là hợp lý về mặt y khoa.
Ví dụ sốt cao có thể do virut, do cúm, so stress hay do thiếu máu não hay do thiếu dinh dương, hay vô vàn thứ khác.
Bác sĩ cho đi khám xét nghiệm nhiều. Thì hợp lý thôi.
nhưng khi kết luận bệnh thì người này chỉ sốt do cảm cúm thông thường.

Vậy là nhiều dịch vụ không nhằm phát hiện ra cảm cúm và chỉ là để loại trừ rằng người đó không phải sốt do các bệnh khác.

Điều này dẫn đến việc nhiều công ty bảo hiểm không đồng ý chi trả toàn bộ danh sách dịch vụ, có công ty bảo hiểm chỉ trả những dịch vụ trực tiếp phát hiện ra cảm cúm thông thường, có cty bảo hiểm chỉ chi trả cho các dịch vụ khác khi người đó mắc phải 1 số bệnh nhất định, rồi còn tuỳ vào hợp đồng, gói bảo hiểm, quy tắc bảo hiểm của công ty đó.
Khiến cho bài toán của ta chia thành 2 bài toán rất rõ ràng.

Hợp lý về mặt y khoa không.
Tức bệnh này khám dịch vụ này là ok
Tức với triệu chứng này, dấu hiệu này khám dịch vụ này cũng là ok

Còn đâu phải xem điều khoản bảo hiểm nói sao.
Tiến hành suy luận, bóc tác từng câu xem các quy tắc bảo hiểm đang bao chùm điều gì của việc chi trả cho dịch vụ.

và rồi quy tắc bảo hiểm, điều khoản loại trừ các thể loại nó là quy tắc vô cùng mạnh mẽ và quyết định đến mọi thứ về việ có được chi trả hay không.
Có những công ty yêu cầu tất cả các dịch nếu được chi trả đều là hình thức đồng chi trả, tứng người bệnh 20%, bảo hiểm 20%, ví dụ vậy.
Rồi còn nhiều quy tắc khác ví dụ không bồi thường nếu có nồng độ cồn trong máu.
Thì dù người đó bị ngã, bị tai nạn mất chi này, chi kia, khám xét nghiệm dịch vụ các thể loai đều hợp lý, nhưng phát hiện có dấu hiệu uống rượu, thì mọi thứ về mặt y khoa trước đó đều không có giá trị.
Đó chỉ là 1 ví dụ để bạn biết quy tắc bảo hiểm tuy ngắn nhưng bao phủ và ảnh hưởng rất phức tạp đến các hình thức chi trả bảo hiểm.
Mọi quyết định chi trả đều cần cung cấp nguyên nhân, lý do, căn cứ vào tài liệu nào để thẩm định viên có thể kiểm tra sau này.

Sau khi đã chuyển cho agent_claude đảm nhận phần suy luận, truy cập neo4j. Ta nhớ bó tay, bó các năng lực khác của nó lại, khoá các năng lực khác không có giá trị với bào toán của ta.

Toàn bộ những thứ trên người dùng đều có thể thiết kế qua UI/UX để tương tác, để truy vết, để điều chỉnh, để làm giàu dữ liệu, đánh đánh giá.

Sau đó lên những kế hoạch chuẩn chỉ cho những tình huống dữ liệu nghèo nàn hoặc thay đổi dữ liệu như sau.
Nếu có phác đồ mới của bệnh đó
Bệnh đó đã có ở hệ thống
Nếu chưa có phác đồ bệnh đó, thì agent_claude nên làm gì.
Các bổ sung các phác đồ, các tài liệu cũ khi đã có version 1 và làm tiếp cải thiện nó sang version 2.
Rồi những tài liệu mới.
Rồi cách suy luận, điều chỉnh suy luận, feedback, memory.
Rồi mỗi bệnh, mỗi dịch vụ đôi khi mỗi thẩm định viên điều có những ghi chú note thêm cần để ý. Nên lưu nó vào đâu.
Làm sao để không bao giờ sai những cái mà thẩm định viên đã feedback

Chú ý toàn bộ source code cần thiết cho dự án cuối cùng được lưu trữ tại: D:\desktop_folder\01_claudecodeleak\ai_claim để đưa lên server cloud, chú ý.