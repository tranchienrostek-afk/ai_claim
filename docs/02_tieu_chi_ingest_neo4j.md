# Tiêu chí đánh giá ingest vào Neo4j

Muốn suy luận tốt, ingest vào Neo4j phải đạt các tiêu chí sau.

## 1. Chuẩn hoá canonical

- Dịch vụ không được tạo trùng canonical.
- Dấu hiệu không được tạo trùng canonical.
- Bệnh không được tạo trùng canonical.
- Raw mention được phép trùng, nhưng phải map về canonical đang sống.

## 2. Provenance đầy đủ

Mọi node/edge quan trọng phải truy ngược được:

- file nguồn;
- sheet;
- dòng;
- chunk;
- section;
- version tài liệu;
- thời điểm ingest;
- script ingest nào sinh ra.

## 3. Tách lớp dữ liệu

- `raw`
- `canonical`
- `assertion/rule`
- `reasoning edge`
- `review/memory`

Không được trộn hết vào một lớp node generic.

## 4. Quan hệ phải phục vụ suy luận

Ví dụ:

- `Disease -> indicates_service`
- `Disease -> has_sign`
- `Service -> supported_by_benefit`
- `Service -> excluded_by_contract`
- `Benefit -> grounded_in_clause`
- `Exclusion -> supported_by_rulebook`

## 5. Không đủ chắc thì gắn review

Nếu mapper không chắc:

- tạo candidate match;
- lưu confidence;
- bật `needs_human_review`;
- không đẻ canonical mới bừa bãi.

## 6. Version hoá

Khi có tài liệu `v2`:

- không xoá dấu vết `v1`;
- phải biết node/edge nào đến từ version nào;
- phải tính được impact khi đổi version.

## 7. Dùng được cho adjudication

Graph ingest tốt phải giúp trả lời được:

1. dịch vụ này có hợp lý về y khoa không;
2. dịch vụ này có nằm trong quyền lợi không;
3. dịch vụ này có vướng loại trừ, hạn mức, đồng chi trả không;
4. quyết định cuối cùng căn cứ vào clause nào, phác đồ nào, note nào.
