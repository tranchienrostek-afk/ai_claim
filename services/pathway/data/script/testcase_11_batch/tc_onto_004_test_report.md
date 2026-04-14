# tc_onto_004 Test Report

- Input: `D:\desktop_folder\12_Claude_Code\pathway\notebooklm\data\script\testcase_11.json`
- Story title: `Test 4: Dấu hiệu cấp cứu 'Red Flag' vùng cổ`
- Expected disease: `Nhiễm trùng khoang cổ sâu`
- Base high-confidence: `0/3`
- Hybrid high-confidence: `0/3`
- Base family-resolved: `2/3`
- Hybrid family-resolved: `3/3`
- Top-1 disease token hit: `True`
- Top-3 disease token hit: `True`
- Sign/service overlap codes: `none`

## Top Suspected Diseases

- `None` Nhiễm trùng khoang cổ sâu | score `11.6`
- `None` Viêm thanh quản cấp tính hạ thanh môn | score `5.5`
- `J18.0` Viêm phế quản phổi, không đặc hiệu | score `4.0549`
- `J18` Viêm phổi, tác nhân không xác định
(Điều chỉnh lại "Tên chẩn đoán bệnh" của I18 theo ICD10) | score `3.9787`
- `J02` Viêm họng cấp | score `3.8896`

## Service Mapping

- `CT Scan cổ có cản quang` -> base `IMG-CTN-018` `ct scan phổi` `REVIEW`
- `Mở khí quản cấp cứu` -> base `` `` ``
- `Phẫu thuật rạch dẫn lưu áp xe cổ` -> base `END-ENS-138` `phẫu thuật nội soi sinh thiết u chẩn đoán` `REVIEW`

## Experience Memory

- Expand service family taxonomy/aliases cho 'Nhiễm trùng khoang cổ sâu'. Dịch vụ còn hở: CT Scan cổ có cản quang, Mở khí quản cấp cứu, Phẫu thuật rạch dẫn lưu áp xe cổ.
- Giữ family-first, nhưng bổ sung alias/canonical code cho 'Nhiễm trùng khoang cổ sâu'. Ưu tiên code hóa các dịch vụ: CT Scan cổ có cản quang, Mở khí quản cấp cứu, Phẫu thuật rạch dẫn lưu áp xe cổ.
- Rà lại link Disease -> Expected Services cho 'Nhiễm trùng khoang cổ sâu', đồng thời kiểm tra sign decomposition với các dấu hiệu: Khối sưng vùng cổ, Nóng, đỏ, đau, Mất lọc cọc thanh quản cột sống, Khó thở.
