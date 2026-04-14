# tc_onto_002 Disease Hypothesis Report

- Input: `D:\desktop_folder\12_Claude_Code\pathway\notebooklm\data\script\testcase_11.json`
- Story title: `Test 2: Phân biệt bệnh lý theo 'Modifier' thời gian và độ tuổi`
- Expected disease: `U xơ mạch vòm mũi họng`
- Top-1 hit: `True`
- Top-3 hit: `True`

## Top Hypotheses

- `` U xơ mạch vòm mũi họng | status `confirmed` | score `1.0` | confidence `0.8696`
  matched_services: `Chụp mạch xóa nền (DSA) và nút mạch -> Chụp mạch xóa nền (DSA)`
  memory: `Giữ pattern hiện tại cho 'U xơ mạch vòm mũi họng': signs Chảy máu mũi, Tự phát, Tự cầm, Ồ ạt -> services CT Scan sọ mặt có cản quang tĩnh mạch, Sinh thiết khối u hốc mũi, Chụp mạch xóa nền (DSA) và nút mạch.`
- `` Bệnh Ménière | status `active` | score `0.4078` | confidence `0.7311`
  memory: `Giữ pattern hiện tại cho 'Bệnh Ménière': signs Nghe kém / Điếc, Đột ngột, Tiếp nhận, Một bên -> services Đo thính lực đồ, Điện động nhãn đồ (ENG), MRI sọ não - xương đá có tiêm thuốc.`
- `J00` Viêm mũi họng cấp [cảm thường] | status `active` | score `0.3398` | confidence `0.6938`
- `H65` Viêm tai giữa không nung mủ | status `active` | score `0.1936` | confidence `0.5634`
  memory: `Giữ pattern hiện tại cho 'Viêm tai giữa mạn tính có cholesteatoma': signs Chảy mủ tai, Thối khẳn, Lổn nhổn trắng, Mảnh óng ánh như xà cừ -> services Khám tai dưới kính hiển vi, Nghiệm pháp rò mê nhĩ, CT Scan xương thái dương.`
- `J01` Viêm mũi xoang cấp tính | status `active` | score `0.1804` | confidence `0.546`
