# Kich Ban 01 Test Report

- Input: `D:\desktop_folder\12_Claude_Code\pathway\notebooklm\data\script\kich_ban_01.md`
- Case CSV rows: `1`
- Service CSV rows: `4`
- Case CSV issues: `1`
- Base service mapper high-confidence: `2/4`
- Hybrid service mapper high-confidence: `0/4`
- Base service mapper family-resolved: `4/4`
- Hybrid service mapper family-resolved: `4/4`
- Sign-engine recommended/mapped overlap codes: `none`

## Story Signs

- `đau nửa đầu`
- `ù tai`
- `ngạt mũi`
- `khịt khạc nhầy lẫn tia máu`

## Top Suspected Diseases

- `J00` Viêm mũi họng cấp [cảm thường] | score `3.3685`
- `J03` Viêm amidan cấp | score `1.6011`
- `J30.3` Viêm mũi dị ứng khác | score `1.2615`
- `K21` Bệnh trào ngược dạ dày- thực quản | score `1.2199`
- `J02` Viêm họng cấp | score `1.1411`

## Service Mapping

- `Nội soi mũi họng phóng đại` -> base `END-ENS-002` `nội soi mũi` `HIGH` | family `endoscopy` `coded`
- `Sinh thiết khối u vòm` -> base `END-ENS-138` `sinh thiet khoi u vom` `MEDIUM` | family `procedure_surgery` `coded`
- `Chụp CT Scan sọ mặt có cản quang` -> base `IMG-CTN-001` `chụp clvt sọ não không tiêm thuốc cản quang (từ 1-32 dãy)` `REVIEW` | family `imaging` `coded`
- `Siêu âm hệ thống hạch vùng cổ` -> base `IMG-USG-010` `siêu âm hạch vùng cổ` `HIGH` | family `imaging` `coded`
