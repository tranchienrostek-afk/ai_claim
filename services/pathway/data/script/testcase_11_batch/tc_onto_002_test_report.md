# tc_onto_002 Test Report

- Input: `D:\desktop_folder\12_Claude_Code\pathway\notebooklm\data\script\testcase_11.json`
- Story title: `Test 2: Phân biệt bệnh lý theo 'Modifier' thời gian và độ tuổi`
- Expected disease: `U xơ mạch vòm mũi họng`
- Base high-confidence: `0/3`
- Hybrid high-confidence: `0/3`
- Base family-resolved: `3/3`
- Hybrid family-resolved: `3/3`
- Top-1 disease token hit: `True`
- Top-3 disease token hit: `True`
- Sign/service overlap codes: `IMG-XRY-270`

## Top Suspected Diseases

- `None` U xơ mạch vòm mũi họng | score `11.3`
- `J00` Viêm mũi họng cấp [cảm thường] | score `6.8445`
- `None` Bệnh Ménière | score `6.2`
- `R04.0` Chảy máu cam | score `3.4099`
- `None` Viêm thanh quản cấp tính hạ thanh môn | score `3.2`

## Service Mapping

- `CT Scan sọ mặt có cản quang tĩnh mạch` -> base `IMG-CTN-039` `ct scan sọ não` `REVIEW`
- `Sinh thiết khối u hốc mũi` -> base `END-ENS-138` `sinh thiet khoi u hoc mui` `MEDIUM`
- `Chụp mạch xóa nền (DSA) và nút mạch` -> base `IMG-XRY-270` `chup mach xoa nen dsa va nut mach` `REVIEW`

## Experience Memory

- Giữ family-first, nhưng bổ sung alias/canonical code cho 'U xơ mạch vòm mũi họng'. Ưu tiên code hóa các dịch vụ: CT Scan sọ mặt có cản quang tĩnh mạch, Sinh thiết khối u hốc mũi, Chụp mạch xóa nền (DSA) và nút mạch.
- Giữ pattern hiện tại cho 'U xơ mạch vòm mũi họng': signs Chảy máu mũi, Tự phát, Tự cầm, Ồ ạt -> services CT Scan sọ mặt có cản quang tĩnh mạch, Sinh thiết khối u hốc mũi, Chụp mạch xóa nền (DSA) và nút mạch.
- Expand service family taxonomy/aliases cho 'Nhiễm trùng khoang cổ sâu'. Dịch vụ còn hở: CT Scan cổ có cản quang, Mở khí quản cấp cứu, Phẫu thuật rạch dẫn lưu áp xe cổ.
