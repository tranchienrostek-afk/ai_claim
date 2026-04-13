# Version, feedback và memory

## Khi có phác đồ mới cho bệnh đã có

- tạo `version mới`;
- không ghi đè version cũ;
- chạy impact report:
  - dịch vụ nào thay đổi;
  - dấu hiệu nào thay đổi;
  - assertion nào thay đổi;
  - case benchmark nào có nguy cơ lệch;
  - decision nào cần review lại.

## Khi bệnh chưa có trong hệ thống

Hệ thống phải:

1. tạo workspace bệnh mới;
2. tạo guide `CLAUDE.md`;
3. tách raw text;
4. bóc dịch vụ, dấu hiệu, bệnh phân biệt;
5. đánh dấu phần nào còn thiếu canonical;
6. đẩy các candidate chưa chắc vào human review.

## Khi có feedback của thẩm định viên

Feedback không được chỉ nằm ở chat.
Nó phải thành object quản lý được:

- disease note;
- service note;
- contract note;
- rule interpretation note;
- decision precedent.

## Nguyên tắc "không bao giờ quên feedback"

- feedback đã confirm phải vào graph/memory;
- runtime phải đọc memory trước khi chốt quyết định;
- feedback phải có version và tác giả;
- có thể hết hiệu lực, nhưng không được biến mất.
