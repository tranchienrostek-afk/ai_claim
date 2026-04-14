**Câu chuyện y khoa: Bí ẩn đằng sau những "triệu chứng mượn"**

Bệnh nhân nam, 50 tuổi, đến phòng khám Tai Mũi Họng với những than phiền tưởng chừng rất rời rạc: **đau nửa đầu bên trái dai dẳng, ù tai trái, thỉnh thoảng ngạt mũi và thi thoảng khịt khạc ra chút nhầy lẫn tia máu**. Bệnh nhân cho biết các triệu chứng này đã xuất hiện âm ỉ vài tháng nay, bệnh nhân tự mua thuốc cảm cúm uống nhưng không đỡ.

Tiếp nhận bệnh nhân, bác sĩ chuyên khoa nhận định đây là một ca bệnh không điển hình. Các biểu hiện này được gọi là **các "triệu chứng mượn"**, chúng chỉ xuất hiện ở một bên (bên trái). Ở thời điểm ban đầu, bác sĩ chưa thể xác định ngay đây là bệnh gì, bởi các triệu chứng này có thể là biểu hiện của viêm xoang mạn tính, polyp mũi, khối u lành tính như u xơ vòm mũi họng, hoặc nguy hiểm nhất là một khối u ác tính. 

Để tìm ra nguyên nhân gốc rễ, bác sĩ đã chỉ định một **tổ hợp các xét nghiệm và thăm dò cận lâm sàng**:
1. **Thăm khám nội soi phóng đại mũi - vòm họng**: Bác sĩ phát hiện một khối u màu sẫm, bề mặt sùi và dễ chảy máu nằm che lấp một phần cửa mũi sau bên trái.
2. **Siêu âm hệ thống hạch vùng cổ**: Phát hiện có hạch cổ to ở nhóm II bên trái, hạch cứng và kém di động.
3. **Chụp cắt lớp vi tính (CT scan) sọ mặt - mũi xoang**: Hình ảnh cho thấy khối u đã thâm nhiễm từ vòm họng lan ra hố chân bướm hàm và có dấu hiệu ăn mòn đáy sọ nhẹ.
4. **Sinh thiết khối u vòm họng**: Bác sĩ tiến hành bấm một mảnh mô từ khối u sùi ở vòm họng để gửi đi xét nghiệm giải phẫu bệnh.

**Kết quả và Chẩn đoán:**
Khi có kết quả sinh thiết, phòng giải phẫu bệnh trả về: **Ung thư biểu mô không biệt hóa (Undiffenrenciated Carcinoma Nasopharyngeal Type - UCNT)**. 
Kết hợp với hình ảnh CT và siêu âm, bác sĩ đưa ra chẩn đoán xác định: **Ung thư vòm mũi họng giai đoạn T3N1M0**.

---

**Thống kê vai trò của các chỉ định khám/xét nghiệm:**

**1. Khám/xét nghiệm trực tiếp tìm ra bệnh (Khẳng định chẩn đoán):**
*   **Nội soi phóng đại mũi - vòm họng:** Giúp bác sĩ nhìn thấy trực tiếp tổn thương u nguyên phát (T) tại vòm họng mà khám thông thường không thấy được.
*   **Sinh thiết khối u vòm:** Đây là "tiêu chuẩn vàng", xét nghiệm mang tính quyết định trực tiếp tìm ra bệnh bằng cách chẩn đoán xác định mô bệnh học của khối u. 

**2. Khám/xét nghiệm mang tính kết quả để loại trừ bệnh và đánh giá lan tràn:**
*   **Chụp cắt lớp vi tính (CT scan) sọ mặt - mũi xoang:** Xét nghiệm này giúp **loại trừ** các bệnh lý lành tính có tính chất phá hủy xương khác (như u xơ, polyp xơ hóa, u nguyên sống đáy sọ). Đồng thời nó đánh giá mức độ lan tràn của tổn thương u vòm vào mũi xoang, đáy sọ.
*   **Siêu âm hệ thống hạch vùng cổ / Ổ bụng:** Xét nghiệm này dùng để loại trừ các nguyên nhân viêm hạch thông thường và đánh giá tình trạng di căn hạch vùng (N) hoặc di căn xa (gan, lách).

---

**Data y học mẫu chuẩn chỉ cho trường hợp này (Demo CSV)**

Dưới đây là 2 bảng dữ liệu CSV theo đúng chuẩn định dạng yêu cầu để đưa vào hệ thống Labeling cho trường hợp Ung thư vòm mũi họng này:

### Bảng 1: Case Level (form_input_lable_case.csv)

```csv
request_id,selected_profile_id,testcase_title,main_disease_name_vi,primary_icd10,secondary_icd10_list_pipe,specialty,message_hash_id,claim_id_or_sohoso,why_this_case_was_selected,claim_raw_json,icd_source,signs_history_source,lab_result_source,reviewer_or_insurer_note_source,contract_or_plan_source,chief_complaint,initial_signs_pipe,medical_history_pipe,initial_clinical_question,total_lines,target_lines_for_labeling,panel_services_present_pipe,delivery_deadline,employee_name,review_status,manager_note
tc_256,prof_256,TC256_NasoPharyngeal_Carcinoma,Ung thư vòm mũi họng,<chua_co_ICD>,,Đầu mặt cổ,msg_256,claim_256,Ca bệnh mô tả quá trình chẩn đoán ung thư vòm mũi họng điển hình bắt đầu từ các triệu chứng mượn (đau đầu, ù tai, khạc máu) cần các xét nghiệm đặc hiệu để loại trừ u lành tính.,claim_256.json,phac_do_tmh_2015,phac_do_tmh_2015,phac_do_tmh_2015,note_256,plan_256,Đau nửa đầu trái dai dẳng ù tai trái và thi thoảng khịt khạc ra máu.,Khối u sùi vòm họng trái | Hạch cổ trái nhóm II cứng cố định,Hút thuốc lá và hay ăn thức ăn lên men chua muối,Cần chẩn đoán xác định bản chất khối u ở vòm họng và loại trừ u xơ hoặc u nguyên sống đáy sọ.,4,4,Nội soi mũi họng | Chụp CT sọ mặt | Siêu âm hạch vùng cổ | Sinh thiết khối u vòm,2026-03-28,bs_demo_1,submitted,Ví dụ chuẩn chỉ cho chẩn đoán phân biệt ung thư vòm.
```

### Bảng 2: Service Line Level (form_input_lable_service_lines.csv)

```csv
request_id,claim_id_or_sohoso,message_hash_id,line_no,service_name_raw,service_code_if_any,amount_if_any,medical_necessity_view,final_label,reason_layer,reason_text,evidence_source,allocation_note,confidence,reviewer_note
tc_256,claim_256,msg_256,1,Nội soi mũi họng phóng đại,,,supported_by_both,PAYMENT,medical,Chỉ định cần thiết đầu tiên để thăm khám và đánh giá tổn thương u nguyên phát tại vòm họng đối với bệnh nhân có triệu chứng mượn (ù tai đau đầu khạc máu một bên).,phac_do_tmh_2015,,strong,Thăm dò hình ảnh trực tiếp đầu tay.
tc_256,claim_256,msg_256,2,Sinh thiết khối u vòm,,,supported_by_both,PAYMENT,medical,Là chỉ định bắt buộc mang tính quyết định để chẩn đoán xác định mô bệnh học (thường là UCNT) sau khi soi thấy khối u sùi ở vòm họng.,phac_do_tmh_2015,,strong,Tiêu chuẩn vàng tìm ra bệnh.
tc_256,claim_256,msg_256,3,Chụp CT Scan sọ mặt có cản quang,,,supported_by_both,PAYMENT,medical,Chỉ định cần thiết để đánh giá mức độ lan tràn của tổn thương u vòm vào đáy sọ hố chân bướm hàm và để chẩn đoán phân biệt loại trừ với các u ác/lành tính khác.,phac_do_tmh_2015,,strong,Xét nghiệm loại trừ và đánh giá lan tràn.
tc_256,claim_256,msg_256,4,Siêu âm hệ thống hạch vùng cổ,,,supported_by_both,PAYMENT,medical,Chỉ định hợp lý để phát hiện và đánh giá tình trạng di căn hạch cổ (N) từ khối u nguyên phát ở vòm mũi họng.,phac_do_tmh_2015,,strong,Đánh giá di căn vùng.
```