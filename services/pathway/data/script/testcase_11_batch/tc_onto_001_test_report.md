# tc_onto_001 Test Report

- Input: `D:\desktop_folder\12_Claude_Code\pathway\notebooklm\data\script\testcase_11.json`
- Story title: `Test 1: Lời khai mang tính 'đặc hiệu' ẩn trong từ ngữ dân dã`
- Expected disease: `Viêm tai giữa mạn tính có cholesteatoma`
- Base high-confidence: `0/3`
- Hybrid high-confidence: `0/3`
- Base family-resolved: `3/3`
- Hybrid family-resolved: `3/3`
- Top-1 disease token hit: `True`
- Top-3 disease token hit: `True`
- Sign/service overlap codes: `END-ENS-041`

## Top Suspected Diseases

- `None` Viêm tai giữa mạn tính có cholesteatoma | score `14.1`
- `H81.1` Chóng mặt kịch phát lành tính | score `9.4526`
- `None` Bệnh Ménière | score `4.5`
- `J00` Viêm mũi họng cấp [cảm thường] | score `3.2384`
- `None` Viêm thanh quản cấp tính hạ thanh môn | score `3.2`

## Service Mapping

- `Khám tai dưới kính hiển vi` -> base `END-ENS-041` `kham tai duoi kinh hien vi` `MEDIUM`
- `Nghiệm pháp rò mê nhĩ` -> base `FUN-DFT-084` `nghiệm pháp rượu (nghiệm pháp ethanol)` `REVIEW`
- `CT Scan xương thái dương` -> base `IMG-CTN-018` `ct scan phổi` `REVIEW`

## Experience Memory

- Giữ family-first, nhưng bổ sung alias/canonical code cho 'Viêm tai giữa mạn tính có cholesteatoma'. Ưu tiên code hóa các dịch vụ: Khám tai dưới kính hiển vi, Nghiệm pháp rò mê nhĩ, CT Scan xương thái dương.
- Giữ pattern hiện tại cho 'Viêm tai giữa mạn tính có cholesteatoma': signs Chảy mủ tai, Thối khẳn, Lổn nhổn trắng, Mảnh óng ánh như xà cừ -> services Khám tai dưới kính hiển vi, Nghiệm pháp rò mê nhĩ, CT Scan xương thái dương.
- Giữ family-first, nhưng bổ sung alias/canonical code cho 'Bệnh Ménière'. Ưu tiên code hóa các dịch vụ: Điện động nhãn đồ (ENG), MRI sọ não - xương đá có tiêm thuốc.
