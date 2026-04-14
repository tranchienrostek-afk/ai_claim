# tc_onto_003 Test Report

- Input: `D:\desktop_folder\12_Claude_Code\pathway\notebooklm\data\script\testcase_11.json`
- Story title: `Test 3: Dấu hiệu thanh quản đặc trưng ở trẻ em`
- Expected disease: `Viêm thanh quản cấp tính hạ thanh môn`
- Base high-confidence: `0/3`
- Hybrid high-confidence: `0/3`
- Base family-resolved: `2/3`
- Hybrid family-resolved: `3/3`
- Top-1 disease token hit: `True`
- Top-3 disease token hit: `True`
- Sign/service overlap codes: `none`

## Top Suspected Diseases

- `None` Viêm thanh quản cấp tính hạ thanh môn | score `11.9`
- `None` Viêm tai giữa mạn tính có cholesteatoma | score `9.1`
- `None` Nhiễm trùng khoang cổ sâu | score `8.5`
- `None` Bệnh Ménière | score `5.4`
- `J18.0` Viêm phế quản phổi, không đặc hiệu | score `4.9048`

## Service Mapping

- `Khí dung Adrenaline 1/1000` -> base `` `` ``
- `Tiêm tĩnh mạch Corticoid` -> base `IMG-USG-136` `siêu âm động tĩnh mạch chi dưới` `REVIEW`
- `Soi thanh quản bằng ống cứng` -> base `END-ENS-045` `soi thanh quan bang ong cung` `LOW`

## Experience Memory

- Expand service family taxonomy/aliases cho 'Viêm thanh quản cấp tính hạ thanh môn'. Dịch vụ còn hở: Khí dung Adrenaline 1/1000, Tiêm tĩnh mạch Corticoid, Soi thanh quản bằng ống cứng.
- Giữ family-first, nhưng bổ sung alias/canonical code cho 'Viêm thanh quản cấp tính hạ thanh môn'. Ưu tiên code hóa các dịch vụ: Khí dung Adrenaline 1/1000, Tiêm tĩnh mạch Corticoid, Soi thanh quản bằng ống cứng.
- Rà lại link Disease -> Expected Services cho 'Viêm thanh quản cấp tính hạ thanh môn', đồng thời kiểm tra sign decomposition với các dấu hiệu: Ho, Ông ổng, Tiếng ho cứng, Như chó sủa.
