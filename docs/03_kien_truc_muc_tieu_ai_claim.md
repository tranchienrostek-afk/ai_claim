# Kiến trúc mục tiêu cho ai_claim

## Tầng 1: Knowledge Surface

Nguồn dữ liệu:

- phác đồ PDF / text / markdown;
- bảng dịch vụ;
- bảng triệu chứng;
- quyền lợi;
- quy tắc bảo hiểm;
- hợp đồng;
- loại trừ;
- văn bản pháp lý;
- ghi chú thẩm định viên;
- feedback và memory.

## Tầng 2: Ingest Compiler

Ba pipeline lõi:

1. `medical_ingest`
2. `insurance_ingest`
3. `memory_ingest`

Mỗi pipeline đều phải sinh:

- manifest;
- version;
- mapping report;
- impact report;
- review queue.

## Tầng 3: Graph Core

Neo4j là lõi tri thức, với ba không gian chính:

- medical graph;
- insurance graph;
- bridge graph.

## Tầng 4: Graph Operating Layer

Lớp này cung cấp:

- search;
- trace;
- health;
- impact;
- report;
- reverse trace từ graph về file.

## Tầng 5: Reasoning Runtime

Runtime không được nhảy cóc.
Phải đi qua:

1. mission lock
2. case understanding
3. hypothesis generation
4. knowledge access plan
5. evidence ledger
6. contradiction check
7. benefit/exclusion resolution
8. decision gate
9. audit summary

## Tầng 6: Human Review

Con người có thể:

- sửa text;
- sửa workbook;
- sửa label;
- thêm note;
- confirm mapping;
- reject mapping;
- thêm clause interpretation;
- lưu feedback để không quên.
