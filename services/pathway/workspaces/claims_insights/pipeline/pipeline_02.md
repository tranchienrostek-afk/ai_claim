Ví dụ khi khách hàng bị sốt: Bác sĩ cho kh đi khám sốt xuất huyết và viêm họng, nhưng thực tế khách hàng bị viêm họng, thì đại đa số các bệnh viện sẽ không chi trả cho các khám xét nghiệm liên quan đến sốt, nhưng có 1 vài hợp đồng có chi trả cho các xét nghiệm dự trù chia, do đó còn dựa vào dấu hiệu và hợp đồng bảo hiểm của 1 vài công ty bảo hiểm nữa. khó vãi, làm thuật toán cho azinsu khó thế, giờ đây tôi cần lấy dữ liệu, xây dựng hệ thống xử lý dư liệu bắt đầu từ đâu và xây quy trình sau này sẽ như nào.

Nguyên tắc phải giữ:

- Không map tay `bệnh -> điều khoản hợp đồng`.
- Hợp đồng là lớp tri thức có trước, độc lập, và phải được ingest thành `clause library`.
- Disease/service chỉ cung cấp tri thức y khoa và đặc tính thực thể.
- Engine adjudication sẽ suy luận theo chiều:

```text
claim line
-> standardized service
-> clinical role + medical necessity
-> retrieve applicable contract clauses
-> clause precedence / exceptions
-> final adjudication
```

Muốn scale lên hàng chục ngàn bệnh, hàng chục ngàn phác đồ, hàng chục ngàn dịch vụ thì chỉ có cách này mới bền.
