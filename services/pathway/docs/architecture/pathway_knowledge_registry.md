# Pathway Knowledge Registry

## Mục tiêu

`PathwayKnowledgeRegistryStore` biến toàn bộ tri thức vận hành thành tài sản có quản trị:

- file nguồn
- cấu hình ingest theo từng file
- version theo nội dung
- trạng thái đã vào graph hay chưa
- reverse trace từ graph quay lại file

Đây là lớp bắc cầu giữa:

- thư mục tri thức
- pipeline ingest
- Neo4j graph
- dashboard quản trị

## Bố cục thư mục

```
data/knowledge_registry/
  knowledge_registry.json
  manifests/
  snapshots/
  assets/
    protocol_pdfs/
    protocol_texts/
    insurance_rulebooks/
    benefit_tables/
    legal_documents/
    benchmarks/
    memory/
    misc/
```

## Chu trình vận hành

1. File được đặt vào thư mục quản trị hoặc được upload qua dashboard.
2. Registry quét thư mục tri thức và tạo `asset`.
3. Mỗi `asset` sinh:
   - `asset_id`
   - `domain`
   - `kind`
   - `config`
   - `versions[]`
   - `ingest`
4. Khi chạy ingest:
   - protocol PDF có thể vào `single` hoặc `multi`
   - text được đưa trực tiếp vào `ontology_v2`
5. Sau ingest:
   - run manifest được lưu
   - registry cập nhật trạng thái
   - graph trace được cache lại
6. Dashboard đọc registry và cho phép:
   - lọc tài sản
   - sửa config
   - chạy ingest
- xem evidence reverse trace
- export workbook nhiá»u sheet tá»« graph
- sync chá»‰nh sá»­a workbook ngÆ°á»£c vá» Neo4j

## Các ý tưởng làm Pathway vượt Graphify

- Asset ID dựa trên `root + relative path`, không dựa trên `stem` mơ hồ.
- Version dựa trên `sha1 nội dung`, không chỉ `mtime`.
- Graph không chỉ để khám phá, mà còn để truy ngược bằng chứng về file nguồn.
- Cùng một file có thể có nhiều version trong registry.
- `config` được gắn với từng asset, không phải config rời rạc theo lần chạy.
- Protocol PDF có đường đi tự động vào `ontology_v2`.

## Flow ưu tiên cao

### 1. Protocol PDF

`protocol_pdf -> extract text -> optional split disease -> ontology_v2 -> reverse trace`

### 2. Protocol text

`protocol_text -> classify text -> ontology_v2 -> reverse trace`

### 2.5. Workbook hai chiá»u

`graph -> workbook sheets -> chinh sua bang -> sync lai graph`

Workbook hiá»‡n táº¡i Ä‘Æ°á»£c táº¡o ra tá»« cÃ¡c lá»›p:

- `meta`
- `documents`
- `diseases`
- `sections`
- `chunks`
- `assertions`
- `sign_mentions`
- `service_mentions`
- `observation_mentions`
- `summaries`

Cá»™t `__op` cho phÃ©p:

- Ä‘á»ƒ trá»‘ng: giá»¯ / update
- `delete`: xÃ³a node / quan há»‡ liÃªn quan

### 2.6. Text workspace hai chiá»u

`pdf/source -> text workspace -> sua text -> tai sinh graph -> export workbook`

Flow nÃ y cho phÃ©p:

- bóc text tá»« PDF thÃ nh workspace cÃ³ thá»ƒ sá»­a tay
- giá»¯ file source gá»‘c vÃ  file text lÃ m viá»‡c tÃ¡ch riÃªng
- táº¡i ingest má»›i, Æ°u tiÃªn dÃ¹ng text workspace Ä‘Ã£ hiá»‡u chá»‰nh
- soi ngÆ°á»£c text Ä‘á»ƒ highlight cÃ¡c sign/service/observation mÃ  graph Ä‘ang hiá»ƒu

MÃ´ hÃ¬nh nÃ y giÃºp ngÆ°á»i váº­n hÃ nh nhÃ¬n ráº¥t rÃµ:

- text nguá»“n ra sao
- Ä‘oáº¡n nÃ o hÃ»‡ thá»‘ng Ä‘ang báº¯t thÃ nh entity
- entity Ä‘Ã³ Ä‘ang map vá» canonical/service/sign nÃ o trong graph

### 3. Insurance / legal sources

Hiện tại registry đã catalog và reverse trace được theo `source_file`.
Pha tiếp theo nên chuẩn hóa ingest trực tiếp các nguồn này thành `assertion/rule graph` giống `ontology_v2`.
