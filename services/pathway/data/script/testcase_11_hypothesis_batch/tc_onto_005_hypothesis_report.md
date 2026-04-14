# tc_onto_005 Disease Hypothesis Report

- Input: `D:\desktop_folder\12_Claude_Code\pathway\notebooklm\data\script\testcase_11.json`
- Story title: `Test 5: Sự giao thoa triệu chứng (Overlap Symptoms) và loại trừ`
- Expected disease: `Bệnh Ménière`
- Top-1 hit: `True`
- Top-3 hit: `True`

## Top Hypotheses

- `` Bệnh Ménière | status `confirmed` | score `1.0` | confidence `0.8696`
  matched_services: `Đo thính lực đồ -> Đo thính lực đồ; MRI sọ não - xương đá có tiêm thuốc -> MRI sọ não - xương đá`
  memory: `Giữ pattern hiện tại cho 'Bệnh Ménière': signs Nghe kém / Điếc, Đột ngột, Tiếp nhận, Một bên -> services Đo thính lực đồ, Điện động nhãn đồ (ENG), MRI sọ não - xương đá có tiêm thuốc.`
- `H81.1` Chóng mặt kịch phát lành tính | status `active` | score `0.6329` | confidence `0.8084`
- `` Viêm tai giữa mạn tính có cholesteatoma | status `active` | score `0.2805` | confidence `0.6516`
  memory: `Giữ pattern hiện tại cho 'Viêm tai giữa mạn tính có cholesteatoma': signs Chảy mủ tai, Thối khẳn, Lổn nhổn trắng, Mảnh óng ánh như xà cừ -> services Khám tai dưới kính hiển vi, Nghiệm pháp rò mê nhĩ, CT Scan xương thái dương.`
- `` U xơ mạch vòm mũi họng | status `active` | score `0.2101` | confidence `0.5834`
  memory: `Giữ pattern hiện tại cho 'U xơ mạch vòm mũi họng': signs Chảy máu mũi, Tự phát, Tự cầm, Ồ ạt -> services CT Scan sọ mặt có cản quang tĩnh mạch, Sinh thiết khối u hốc mũi, Chụp mạch xóa nền (DSA) và nút mạch.`
- `` Viêm thanh quản cấp tính hạ thanh môn | status `active` | score `0.1937` | confidence `0.5636`
  memory: `Rà lại link Disease -> Expected Services cho 'Viêm thanh quản cấp tính hạ thanh môn', đồng thời kiểm tra sign decomposition với các dấu hiệu: Ho, Ông ổng, Tiếng ho cứng, Như chó sủa.`
