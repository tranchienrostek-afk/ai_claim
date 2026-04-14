# Quy trinh mapping text -> ma dich vu

## Muc tieu

Quy trinh nay dung de map ten dich vu can lam sang dang text tu do ve `service_code`
mot cach on dinh, co the truy vet va co nguong confidence ro rang.

Repo hien tai da co 2 lop nen:

1. `chuan_hoa_dich_vu.py`
   Tao codebook tu du lieu lich su.
2. `auto_review.py`
   Danh dau cac merge sai va tong hop vao `review_summary.json`.

Muc bo sung trong turn nay la `service_text_mapper.py`, dung codebook hien co de map
text moi theo pipeline inference chuan hoa.

## Du lieu hien tai cho thay dieu gi

Tu `service_codebook.json` va `review_summary.json`:

- 5,666 ten raw unique
- 5,113 ten unique sau normalize
- 2,551 cluster / ma dich vu tam thoi
- 384 cluster confidence `LOW`
- auto review da phat hien 15 merge sai trong 13 cluster

Dieu nay cho thay du lieu co 4 nhom nhieu:

- Nhiu OCR / typo: `tai` vs `tại`, `dem laser` vs `đếm laser`
- Nhiu prefix/suffix cua benh vien: `[DV]`, `(BHYT)`, ma noi bo, phong kham
- Cung 1 dich vu nhung khac cach viet: `xquang`, `x-quang`, `x quang`
- Ten rat giong nhau nhung y nghia khac han: `ALT` vs `AST`, `creatinin` vs `albumin`,
  `XQ nguc` vs `XQ khung chau`

Vi vay, mapping chinh xac khong the chi dua vao 1 fuzzy score duy nhat.

## Quy trinh de xuat

### 1. Tao codebook nen

Chay theo thu tu:

```powershell
$env:PYTHONIOENCODING='utf-8'
python chuan_hoa_dich_vu.py
python auto_review.py
```

Ket qua dung cho inference:

- `service_codebook.json`: bang ma + variant
- `review_summary.json`: blacklist cac variant da biet la merge sai

### 2. Normalize text dau vao

Text moi di qua cung rule voi pipeline goc:

- lowercase + Unicode normalize
- bo prefix/suffix nhieu nhu `[DV]`, thong tin phong kham
- sua OCR/typo theo bang `OCR_FIXES`
- collapse whitespace

Muc tieu cua buoc nay la dua cac bien the be mat ve cung mot truc chung.

### 3. Tao dang "skeleton" de hieu du lieu

Sau normalize, text tiep tuc duoc:

- strip noise nghiep vu
- bo dau va giu lai ky tu chu/so

Dang skeleton giup nhin ra cac cum OCR gan nhau va giup fuzzy matching ben vung hon
khi van ban y te bi loi dau, sai ky tu, hoac thua ma noi bo.

### 4. Trich xuat tin hieu y khoa

Mapper trich 4 nhom tin hieu:

- `analytes`: ALT, AST, creatinin, CRP, TSH, HBsAg...
- `body_parts`: nguc, bung, cot song co, tuyen giap, tai mui hong...
- `modalities`: x-quang, sieu am, CT, MRI, noi soi, dien tim...
- `specimens`: mau, nuoc tieu, dam...

Day la lop "hieu du lieu" quan trong nhat. No giup tach:

- cung hinh thuc cau truc nhung khac chat xet nghiem
- cung modality nhung khac vung co the
- cung ten gan nhau nhung khac benh pham

### 5. Candidate retrieval

Khong fuzzy tren toan bo codebook mot cach mu quang. Mapper lay candidate theo nhieu lop:

1. exact match theo cleaned text
2. exact match theo stripped text
3. exact match theo skeleton
4. cung category goi y
5. cung medical signals
6. fuzzy preselect bang `token_sort_ratio` va `token_set_ratio`

Buoc nay giong "blocking" trong record linkage, giup nhanh hon va dung hon.

### 6. Scoring da tieu chi

Moi candidate duoc cham bang ket hop:

- `token_sort_ratio`
- `token_set_ratio`
- `partial_ratio`
- ratio tren text da strip noise
- ratio tren skeleton
- `char 3-gram Jaccard`
- do trung khop medical signals

Bonus:

- exact cleaned match
- exact stripped match
- cung discriminator y khoa
- cung category

Penalty:

- khac analyte
- khac body part
- khac modality
- khac specimen

## Cac thuat toan chuan hoa dang tham khao

Day la bo thuat toan nen co trong bai toan mapping ma dich vu tu text y te:

1. Unicode normalization + case folding
2. Rule-based text normalization
3. Accent folding / skeletonization
4. Levenshtein family
   Dung qua `token_sort_ratio`, `token_set_ratio`, `partial_ratio`
5. Character n-gram similarity
   Huu ich khi OCR lam vo token
6. Feature-based conflict detection
   Dung token y khoa de chan merge sai co nghia
7. Human-in-the-loop thresholds
   Khong ep auto-map moi truong hop

## Confidence va cach dung

Mapper tra 4 muc:

- `HIGH`: co the auto-map
- `MEDIUM`: map duoc, nen log audit
- `LOW`: map tam duoc, nen review trong cac quy trinh nhay cam
- `REVIEW`: khong nen auto-commit

## Cach chay

Map 1 text:

```powershell
$env:PYTHONIOENCODING='utf-8'
python service_text_mapper.py --text "XN sinh hóa - định lượng Creatinin (máu)"
```

Map ca file:

```powershell
$env:PYTHONIOENCODING='utf-8'
python service_text_mapper.py --input review_needed.xlsx --column tên_biến_thể --output mapped_service_codes.xlsx
```

## Luu y van hanh

- Mapper co doc `review_summary.json` de loai cac variant da biet la merge sai.
- Neu codebook duoc sinh lai, nen chay lai `auto_review.py` truoc khi map batch lon.
- Cac truong hop generic nhu `xét nghiệm`, `chẩn đoán hình ảnh` se de roi vao `LOW` hoac `REVIEW`;
  day la hanh vi mong muon vi text khong du thong tin de map an toan.
