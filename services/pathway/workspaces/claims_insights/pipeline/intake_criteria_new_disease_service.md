# Tiêu Chí Intake: Bệnh Mới + Dịch Vụ Mới

Mục tiêu: khi onboard bệnh mới/dịch vụ mới, dữ liệu đủ chuẩn để hệ thống suy luận đúng và lưu vết đầy đủ.

## Nguyên tắc kiến trúc rất quan trọng

- Không nhập tay quan hệ `bệnh -> điều khoản hợp đồng`.
- Không yêu cầu người dùng chỉ ra "bệnh này bị loại trừ bởi clause nào".
- Chiều đúng phải là:
  - `contract/product -> clause library -> rule abstraction`
  - `disease/service -> medical knowledge`
  - engine sẽ tự suy luận `claim line -> medical necessity -> applicable clauses -> final decision`
- Lý do:
  - hợp đồng tồn tại trước bệnh cụ thể và áp lên rất nhiều bệnh/dịch vụ
  - nếu map tay disease-to-clause thì hệ thống sẽ không scale khi lên hàng chục ngàn bệnh/phác đồ/dịch vụ
  - clause thường áp theo `category`, `service role`, `preauth`, `result dependency`, `waiting period`, `document completeness`, không áp theo 1 bệnh duy nhất

## 1) Thông tin bắt buộc cho **bệnh mới**

- `disease_id`: mã nội bộ duy nhất (slug, không dấu, không khoảng trắng).
- `disease_name_vi`: tên bệnh tiếng Việt chuẩn.
- `icd10_primary`: mã ICD-10 chính (ví dụ `J18.9`).
- `icd10_secondary[]`: ICD liên quan (nếu có).
- `specialty`: chuyên khoa chính (`TMH`, `Nội`, `Nhi`, ...).
- `clinical_context`:
  - `core_symptoms[]`: triệu chứng cốt lõi.
  - `red_flags[]`: dấu hiệu cảnh báo.
  - `suspected_vs_final_logic`: mô tả logic nghi ngờ ban đầu vs chẩn đoán cuối.
- `guideline_sources[]`: ít nhất 1 nguồn phác đồ/tài liệu y khoa.
  - `source_type` (`BYT`, `hospital_guideline`, `textbook`, ...)
  - `source_name`
  - `location` (file path/sheet/page nếu có)
- `expected_services[]`: danh sách dịch vụ cận lâm sàng kỳ vọng cho bệnh này.
  - `service_code` (nếu đã có) hoặc `service_name_raw` (nếu dịch vụ mới)
  - `role` (`screening|diagnostic|confirmatory|monitoring|rule_out`)
  - `evidence` (`guideline|statistical|expert`)
  - `priority` (`core|optional`)
- Không nhập:
  - `excluded_by_contract_clause`
  - `covered_by_contract`
  - bất kỳ field nào gắn cứng bệnh với một điều khoản bảo hiểm cụ thể

## 2) Thông tin bắt buộc cho **dịch vụ mới**

- `service_code`: mã chuẩn mới (format như `LAB-BIO-xxx`, `IMG-XRY-xxx`, ...).
- `canonical_name`: tên chuẩn.
- `category_code`, `category_name`.
- `variants[]`: các cách viết thực tế (không dấu + có dấu nếu có).
- `service_nature`:
  - `specimen` (máu, nước tiểu, đờm...)
  - `modality` (xét nghiệm, X-quang, siêu âm, nội soi...)
  - `unit` (nếu là xét nghiệm định lượng)
- `clinical_definition`:
  - `description`
  - `clinical_indications[]`
  - `contraindications[]` (nếu có)
  - `pre_conditions[]` (nhịn ăn, thời điểm lấy mẫu...)
- `reference_ranges[]` (nếu là LAB):
  - `population` (người lớn, trẻ em, thai kỳ...)
  - `lower`, `upper`, `unit`
  - `source`
- `interpretation`:
  - `increased_meaning[]`
  - `decreased_meaning[]`
  - `critical_values` (nếu có)
- `related_conditions[]`:
  - `icd10`
  - `relevance` (`primary|secondary`)
  - `evidence` (`guideline|statistical|hybrid`)
  - `score` (0-1)
- `billing_context`:
  - `bhyt_code` (nếu có)
  - `bhyt_price_vnd` (nếu có)
  - `typical_price_range_vnd` (`min`, `max`)
- Chỉ nhập đặc tính để contract engine tự match:
  - `service_role`
  - `category_code`
  - `specimen`
  - `modality`
  - `pre_conditions`
  - `result_type` (`qualitative|quantitative|imaging_findings|procedure`)
- Không nhập:
  - danh sách điều khoản loại trừ áp riêng cho dịch vụ này
  - mapping thủ công `service_code -> exclusion_code`

## 3) Thông tin bắt buộc cho **hợp đồng/quyền lợi** theo hướng clause-first

Phần này chỉ cần khi có hợp đồng mới, product mới, hoặc wording/quyền lợi thay đổi.

- `contract_id`, `insurer`, `product`.
- `effective_from`, `effective_to`.
- `clause_library[]`: mỗi điều khoản là một unit suy luận độc lập.
  - `clause_id`: mã điều khoản duy nhất trong product.
  - `clause_type`:
    - `coverage`
    - `exclusion`
    - `condition`
    - `waiting_period`
    - `preauthorization`
    - `positive_result_dependency`
    - `sublimit`
    - `copay`
    - `document_requirement`
  - `title`
  - `raw_text`
  - `normalized_rule`
  - `priority`
  - `applies_to`:
    - `service_categories[]`
    - `service_code_patterns[]`
    - `clinical_roles[]`
    - `care_setting[]` (`outpatient|inpatient|dental|maternity|screening`)
    - `result_dependency` (`none|positive_only|abnormal_only`)
    - `requires_preauth`
    - `requires_documents[]`
    - `age_range` / `gender_scope` / `hospital_scope` nếu có
  - `decision_effect`:
    - `allow`
    - `deny`
    - `manual_review`
    - `partial_pay`
  - `financial_terms`:
    - `copay_percent`
    - `sublimit_amount_vnd`
    - `annual_limit_vnd`
  - `source_anchor`:
    - file / sheet / page / clause reference
- `conflict_resolution_policy`:
  - ví dụ `specific_clause_overrides_general_clause`
- `taxonomy_bindings`:
  - map clause vào các taxonomy engine hiểu được như:
    - `screening_not_covered`
    - `diagnostic_workup_allowed`
    - `drug_non_treatment_excluded`
    - `preauth_required`

## 4) Engine cần suy luận hợp đồng theo chiều ngược lại như thế nào

Thay vì:

```text
disease -> manually linked contract clause
```

Phải là:

```text
claim line
-> standardized service / category / role
-> clinical necessity at order time
-> retrieve applicable contract clauses
-> evaluate clause precedence
-> final adjudication
```

Ví dụ:

```text
service = Dengue NS1
symptom = sốt
diagnosis suspected = sốt xuất huyết / viêm họng
contract clause = "screening not covered except diagnostic workup for suspected acute infectious disease"
=> engine tự suy luận clause này có áp hay không
```

## 5) Metadata bắt buộc để lưu vết (audit)

- `created_by`, `reviewed_by`, `approved_by`.
- `created_at`, `approved_at` (ISO datetime).
- `version`, `change_note`.
- `source_documents[]`: đường dẫn file/sheet/page cụ thể.
- `confidence_overall` (0-1).

## 6) Tiêu chí chất lượng trước khi nhập hệ thống

- Không trùng `service_code` và không trùng `disease_id`.
- ICD hợp lệ định dạng.
- Mỗi bệnh mới có ít nhất 1 `expected_service`.
- Mỗi dịch vụ mới có ít nhất 1 `related_condition`.
- Dịch vụ LAB bắt buộc có `unit` + ít nhất 1 `reference_range`.
- Có ít nhất 1 nguồn bằng chứng cho mọi liên kết disease↔service.
- Không được có field map tay `disease/service -> contract clause`.
- Mỗi clause hợp đồng phải có `clause_type`, `decision_effect`, `priority`, `source_anchor`.
- Mỗi clause phải đủ thông tin để engine match bằng đặc tính chung, không phụ thuộc bệnh cụ thể.
- Mọi bản ghi có metadata audit đầy đủ.

## 7) Mapping vào hệ thống hiện tại

- Chuẩn hóa dịch vụ: `02_standardize/service_codebook.json`
- Liên kết dịch vụ-bệnh: `03_enrich/service_disease_matrix.json`
- Tri thức dịch vụ (P3): `03_enrich/enriched_codebook_p3.json`
- Quy tắc hợp đồng: `06_insurance/contract_rules.json`
- Chạy lại benchmark/pipeline: `pipeline/adjudication_mvp.py`

## 8) Quy trình nhập khuyến nghị

1. Điền mẫu JSON intake.
2. Validate đủ trường bắt buộc + định dạng.
3. Merge vào `service_codebook` (nếu có dịch vụ mới).
4. Chạy lại `enrich_icd_correlation.py` để cập nhật matrix.
5. Nếu có hợp đồng mới hoặc wording mới, ingest vào `clause_library` và chạy `build_contract_rules.py`.
6. Chạy benchmark và lưu kết quả trước/sau.
