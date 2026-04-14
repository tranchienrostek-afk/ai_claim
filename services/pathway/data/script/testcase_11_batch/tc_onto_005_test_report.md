# tc_onto_005 Test Report

- Input: `D:\desktop_folder\12_Claude_Code\pathway\notebooklm\data\script\testcase_11.json`
- Story title: `Test 5: Sự giao thoa triệu chứng (Overlap Symptoms) và loại trừ`
- Expected disease: `Bệnh Ménière`
- Base high-confidence: `1/3`
- Hybrid high-confidence: `1/3`
- Base family-resolved: `3/3`
- Hybrid family-resolved: `3/3`
- Top-1 disease token hit: `True`
- Top-3 disease token hit: `True`
- Sign/service overlap codes: `FUN-DFT-006, FUN-DFT-028, IMG-CTN-013`

## Top Suspected Diseases

- `None` Bệnh Ménière | score `15.9`
- `H81.1` Chóng mặt kịch phát lành tính | score `15.7581`
- `None` Viêm thanh quản cấp tính hạ thanh môn | score `6.2`
- `None` Viêm tai giữa mạn tính có cholesteatoma | score `4.1`
- `None` U xơ mạch vòm mũi họng | score `2.5`

## Service Mapping

- `Đo thính lực đồ` -> base `FUN-DFT-006` `đo thính lực đơn âm` `HIGH`
- `Điện động nhãn đồ (ENG)` -> base `FUN-DFT-028` `điện não đồ` `REVIEW`
- `MRI sọ não - xương đá có tiêm thuốc` -> base `IMG-CTN-013` `mri so nao xuong da co tiem thuoc` `LOW`

## Experience Memory

- Giữ family-first, nhưng bổ sung alias/canonical code cho 'Bệnh Ménière'. Ưu tiên code hóa các dịch vụ: Điện động nhãn đồ (ENG), MRI sọ não - xương đá có tiêm thuốc.
- Giữ pattern hiện tại cho 'Bệnh Ménière': signs Nghe kém / Điếc, Đột ngột, Tiếp nhận, Một bên -> services Đo thính lực đồ, Điện động nhãn đồ (ENG), MRI sọ não - xương đá có tiêm thuốc.
- Giữ family-first, nhưng bổ sung alias/canonical code cho 'Viêm tai giữa mạn tính có cholesteatoma'. Ưu tiên code hóa các dịch vụ: Khám tai dưới kính hiển vi, Nghiệm pháp rò mê nhĩ, CT Scan xương thái dương.
