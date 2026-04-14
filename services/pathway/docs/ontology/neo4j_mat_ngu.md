Dạ thưa Thầy, tuân thủ nguyên tắc đi từ tổng quan đến chi tiết, em xin phép trình bày toàn bộ mã lệnh Cypher để số hóa 100% phác đồ điều trị mất ngủ này vào Neo4j. Dữ liệu được bóc tách và phân lớp nghiêm ngặt thành các node (Bệnh lý, Triệu chứng, Thể lâm sàng, Bài thuốc, Vị thuốc, Huyệt đạo, Phương pháp) và các relationship tương ứng để hệ thống có thể truy vấn chéo toàn diện.

Dưới đây là các khối lệnh chi tiết:

### 1. Khởi tạo Thông tin Đại cương, Bệnh lý và Y học Hiện đại (YHHĐ)

**Phần này thiết lập node trung tâm là chứng Rối loạn giấc ngủ, gắn các mã ICD, định nghĩa khái quát, các nguyên nhân gây bệnh, triệu chứng lâm sàng, phương pháp cận lâm sàng và các hướng điều trị theo YHHĐ**^^. **Đồng thời, em cũng số hóa các phương pháp điều trị YHHĐ bao gồm cả việc dùng tân dược và các phương pháp không dùng thuốc (vệ sinh giấc ngủ), cùng các nguyên tắc theo dõi và phòng bệnh**^^^^^^^^.

**Cypher**

```
// Node Bệnh lý chính và YHHĐ
MERGE (b:BenhLy {id: "G47", id_phu: "F51", ten: "RỐI LOẠN GIẤC NGỦ", ten_yhct: "THẤT MIÊN",
    dinh_nghia_yhhd: "Là một bệnh rối loạn chức năng của vỏ não, chủ yếu là sự mất thăng bằng nơi hai quá trình hoạt động hưng phấn và ức chế",
    dinh_nghia_yhct: "Mất ngủ là tình trạng khó ngủ hoặc giảm về thời gian ngủ hoặc giảm chất lượng giấc ngủ. Theo y học cổ truyền: mất ngủ thuộc chứng thất miên do hoạt động không điều hòa của ngũ chí (thần, hồn, phách, ý, trí)"
})

// Nguyên nhân YHHĐ
FOREACH (nn IN [
    "Nhịp sinh học hoạt động như đồng hồ bên trong, hướng dẫn cơ thể vận động chu kỳ ngủ - thức, sự trao đổi chất và nhiệt độ cơ thể",
    "Căng thẳng", "Thói quen ngủ kém", "Ăn quá nhiều vào buổi tối", "Mắc bệnh khác", "Tuổi già"
] | MERGE (n:NguyenNhan_YHHD {ten: nn}) MERGE (b)-[:CO_NGUYEN_NHAN_YHHD]->(n))

// Triệu chứng YHHĐ
FOREACH (tc IN [
    "Khó ngủ vào ban đêm", "Thức suốt đêm", "Dậy quá sớm", "Cảm giác ngủ chưa đã một đêm",
    "Ban ngày mệt mỏi, buồn ngủ", "Khó chịu, trầm cảm hoặc lo lắng", "Không tập trung, xảy ra tai nạn"
] | MERGE (t:TrieuChung_YHHD {ten: tc}) MERGE (b)-[:CO_TRIEU_CHUNG_YHHD]->(t))

// Cận lâm sàng YHHĐ
MERGE (cls:CanLamSang {ten: "Đo điện não đồ (chuyển tuyến)"})
MERGE (b)-[:CAN_LAM_SANG]->(cls)

// Phương pháp điều trị YHHĐ (Không dùng thuốc)
MERGE (pp_yhd_khongthuoc:PhuongPhap {ten: "Điều trị YHHĐ không dùng thuốc"})
MERGE (b)-[:DIEU_TRI_YHHD_KHONG_THUOC]->(pp_yhd_khongthuoc)
FOREACH (ct IN [
    "Chạy từ trường, Chạy ion khí",
    "Vệ sinh giấc ngủ",
    "Cố gắng giữ một thời gian ngủ và thức giấc hằng định ngay cả trong các ngày cuối tuần",
    "Không nằm trên giường xem ti vi, đọc báo hoặc làm việc. Nếu chưa ngủ được sau khi đi nằm một thời gian phải nên rời khỏi giường cho đến khi buồn ngủ",
    "Tránh ngủ chợp mắt",
    "Tập thể dục 3-4 lần trong tuần nhưng tránh tập vào buổi chiều nếu điều đó ảnh hưởng đấn giấc ngủ",
    "Ngừng hoặc giảm sử dụng rượu, cà phê, thuốc lá và các chất khác cản trở đến giấc ngủ",
    "Đặt giường ngủ ở nơi thoáng mát, yên tĩnh và làm các động tác thư giãn trước khi đi ngủ",
    "Giữ môi trường thoáng mát, yên tĩnh khi ngủ"
] | MERGE (chi_tiet:ChiTietPP {ten: ct}) MERGE (pp_yhd_khongthuoc)-[:GOM_BUOC]->(chi_tiet))

// Phương pháp điều trị YHHĐ (Dùng thuốc)
MERGE (pp_yhd_thuoc:PhuongPhap {ten: "Điều trị YHHĐ dùng thuốc", nguyen_tac: "Phối hợp khi mất ngủ kéo dài, tiến triển điều trị chậm, hay thức trắng đêm. Cần tìm hiểu rõ căn nguyên gây mất ngủ. Đó có thể là một bệnh lý nội khoa, ngoại khoa, nhiễm khuẩn... hoặc căn nguyên tâm lý để có phương pháp điều trị cho phù hợp."})
MERGE (b)-[:DIEU_TRI_YHHD_DUNG_THUOC]->(pp_yhd_thuoc)
FOREACH (thuoc_info IN [
    {ten: "Diazepam 5mg", lieu: "Liều 1-2 viên x 01 lần/ngày uống lúc 20 giờ"},
    {ten: "Piracetam 800mg", lieu: "Liều 01-02 viên x 03 lần/ngày uống"},
    {ten: "Tanakan 40mg (Ginkgo biloba)", lieu: "Liều 01 viên x 01-03 lần/ngày uống"},
    {ten: "Rotundin 60mg", lieu: "uống 01-02 viên/ ngày"},
    {ten: "Hapacol Codein (Paracetamol 500mg)", lieu: "Liều 01 viên x 02-03 lần/ngày uống"}
] | MERGE (t:TanDuoc {ten: thuoc_info.ten}) MERGE (pp_yhd_thuoc)-[:SU_DUNG_THUOC {lieu_dung: thuoc_info.lieu}]->(t))

// Theo dõi & Phòng bệnh
MERGE (td:TheoDoi {ten: "Bệnh nhân phải được điều trị lâu dài và theo dõi sinh hiệu trong suốt quá trình điều trị. Xử trí các tai biến xảy ra kịp thời."})
MERGE (b)-[:YEU_CAU_THEO_DOI]->(td)
FOREACH (pb IN ["Tránh căng thăng", "Ngủ đúng giờ", "Tránh dùng chất kích thích: cà phê, trà,..", "Không ăn no vào ban đêm"] |
MERGE (n:PhongBenh {ten: pb}) MERGE (b)-[:Cach_Phong_Benh]->(n))

// Tài liệu tham khảo
FOREACH (tl IN [
    "Bộ Y tế (2013). Quy trình khám bệnh, chữa bệnh chuyên ngành châm cứu, Quyết định 792/QĐ-BYT ban hành ngày 12/3/2013.",
    "Bộ Y tế (2017). Quy trình kỹ thuật khám bệnh, chữa bệnh cấy chỉ và laser châm chuyên ngành châm cứu.",
    "Phác đồ điều trị Bệnh viện Y học cổ truyền TP. HCM (2020), trang 72-78"
] | MERGE (tailieu:TaiLieu {ten: tl}) MERGE (b)-[:THAM_KHAO]->(tailieu))
```

### 2. Triệu chứng chung YHCT & Thể Tâm tỳ lưỡng hư

**Em đưa vào chẩn đoán chung của YHCT, phân định biểu hiện của chứng "Bất mị/Thất miên", sau đó ánh xạ toàn bộ thông tin của Thể Tâm tỳ lưỡng hư (triệu chứng, chẩn đoán bát cương, kinh lạc, pháp trị, phương pháp châm cứu, cấy chỉ, xoa bóp, thủy châm và cấu trúc chi tiết bài Quy tỳ thang)**^^^^^^^^.

**Cypher**

```
MATCH (b:BenhLy {id: "G47"})

// Triệu chứng chung YHCT
MERGE (tc_chung:TrieuChung_YHCT {ten: "Bất mị, thường gọi là thất miên, là một chứng bệnh biểu hiện đặc trưng là vào giấc ngủ rất khó khăn. Trường hợp nhẹ vào giấc ngủ khó, khi ngủ lại dễ tỉnh giấc, tỉnh rồi khó ngủ lại, trường hợp nặng thì mất ngủ cả đêm. Biểu hiện lâm sàng còn có: chóng mặt, đau đầu, hồi hộp, hay quên, bứt rứt không yên."})
MERGE (b)-[:CO_TRIEU_CHUNG_CHUNG_YHCT]->(tc_chung)

// Thể Tâm tỳ lưỡng hư
MERGE (the1:TheLamSang {
    ten: "Tâm tỳ lưỡng hư",
    trieu_chung: "Hay mê, dễ tỉnh, hồi hộp, hay quên (nổi bật là triệu chứng buyết hư). Chất lưỡi nhợt, rêu trăng mỏng hoặc hoạt nhớt. Mạch vi nhược hoặc nhu hoạt. Kèm theo: hoa mắt, chóng mặt, mỏi tay chân, ăn không ngon miệng, sắc mặt không tươi nhuận, hoặc đầy tức bụng.",
    bat_cuong: "Lý hư hàn",
    kinh_lac: "Bệnh tại cân cơ kinh lạc và tạng tâm tỷ",
    nguyen_nhan: "Nội nhân",
    phap_dieu_tri: "Bổ dưỡng tâm tỳ để sinh khí huyết."
})
MERGE (b)-[:BAO_GOM_THE]->(the1)

// Không dùng thuốc: Châm cứu & Cấy chỉ & Thủy châm & Xoa bóp
MERGE (pp_cham1:PhuongPhap {ten: "Châm cứu", lieu_trinh: "20 phút/lần/ngày. Số lần châm cứu phụ thuộc vào tình trạng bệnh của bệnh nhân."})
MERGE (pp_caychi1:PhuongPhap {ten: "Cấy chỉ", lieu_trinh: "(Như công thức huyệt châm cứu). 02 tuần cấy 01 lần. Số lần cấy phụ thuộc vào tình trạng bệnh của bệnh nhân."})
MERGE (pp_thuycham1:PhuongPhap {ten: "Thủy châm", lieu_trinh: "hai bên Phong trì, Tâm du, Cách du. Thủy châm một ngày một lần, mỗi lần thủy châm 2-3 huyệt. Một liệu trình điều trị từ 10-15 lần, có thể tiến hành 2-3 liệu trình liên tục."})
MERGE (pp_xoabop1:PhuongPhap {ten: "Xoa bóp", lieu_trinh: "(thực hiện xoa, xát, miết, day, bóp, lăn các vùng đầu, cổ, vai, tay, chân). Bấm tả các huyệt chung: Bách hội, Thượng tỉnh, Thái dương, Phong trì. Day bổ: Nội quan, Tâm du, cách du, Huyết hải, Thái xung, Trung đô. Xoa bóp 30 phút/lần/ngày. Số lần xoa bóp tùy theo mức độ và diễn tiến của bệnh."})
MERGE (the1)-[:YHCT_KHONG_THUOC]->(pp_cham1)
MERGE (the1)-[:YHCT_KHONG_THUOC]->(pp_caychi1)
MERGE (the1)-[:YHCT_KHONG_THUOC]->(pp_thuycham1)
MERGE (the1)-[:YHCT_KHONG_THUOC]->(pp_xoabop1)

// Chi tiết huyệt châm cứu thể 1
FOREACH (h IN ["Nội quan", "Bách hội", "Thần môn", "An miên", "Phong trì"] | MERGE (huyet:Huyet {ten: h}) MERGE (pp_cham1)-[:HUYET_CHINH]->(huyet))
FOREACH (h IN ["Tỳ du", "Tâm du", "Tam âm giao"] | MERGE (huyet:Huyet {ten: h}) MERGE (pp_cham1)-[:CHAM_BO]->(huyet))

// Dùng thuốc: Quy tỳ thang
MERGE (bt_quyty:BaiThuoc {ten: "Quy tỳ thang", tac_dung: "Tác dụng bổ khí kiện tỳ để tăng cường sinh huyết"})
MERGE (the1)-[:YHCT_DUNG_THUOC]->(bt_quyty)
FOREACH (vi IN [
    {ten: "Đảng sâm", lieu: "12-20g"}, {ten: "Hoàng kỳ", lieu: "12-16g"}, {ten: "Đương quy", lieu: "12-16g"},
    {ten: "Bạch truật", lieu: "08-12g"}, {ten: "Cam thảo bắc", lieu: "04-08g"}, {ten: "Mộc hương", lieu: "06-12g"},
    {ten: "Long nhãn", lieu: "12-16g"}, {ten: "Phục thần", lieu: "12-16g"}, {ten: "Viễn chí", lieu: "04-08g"},
    {ten: "Táo nhân", lieu: "08-16g"}, {ten: "Đại táo", lieu: "12-20g"}, {ten: "Gừng tươi", lieu: "04-08g"}
] | MERGE (v:ViThuoc {ten: vi.ten}) MERGE (bt_quyty)-[:GOM_VI {lieu: vi.lieu}]->(v))

// Gia giảm Quy tỳ thang
MERGE (gia_nang:GiaGiam {dieu_kien: "Nếu mất ngủ tương đối nặng thì có thể gia thêm các vị dưỡng tâm an thần"})
MERGE (bt_quyty)-[:GIA_GIAM]->(gia_nang)
FOREACH (vi IN ["Dạ giao đằng", "Bá tử nhân"] | MERGE (v:ViThuoc {ten: vi}) MERGE (gia_nang)-[:GIA_VI]->(v))

MERGE (gia_ty:GiaGiam {dieu_kien: "Nếu tỷ mất kiện vận, đàm thấp nội trệ, bụng đầy, ăn kém, rêu lưỡi hoạt nhớt, mạch nhu hoạt thì gia để ôn vận tỳ dương mà hóa đàm thấp"})
MERGE (bt_quyty)-[:GIA_GIAM]->(gia_ty)
FOREACH (vi_gia IN [
    {ten: "Trần bì", lieu: "0408g"}, {ten: "Bán hạ", lieu: "04-12g"}, {ten: "Phục linh", lieu: "08-16g"}, {ten: "Nhục quế", lieu: "02-08g"}
] | MERGE (v:ViThuoc {ten: vi_gia.ten}) MERGE (gia_ty)-[:GIA_VI {lieu: vi_gia.lieu}]->(v))

// Thuốc thành phẩm chung (dùng cho các thể)
MERGE (tp:ThanhPhamYHCT {ten: "Danh mục thuốc thành phẩm (Có thể thay thế loại thuốc khác có tác dụng tương đương)"})
MERGE (the1)-[:DUNG_THUOC_THANH_PHAM]->(tp)
FOREACH (thuoc_tp IN [
    "Mimosa: Liều 1-2 viên/ngày uống tối (20 giờ)",
    "An thần: (u) 2-3 viên x 3 lần/ngày. Đợt dùng 2 - 4 tuần",
    "Dưỡng tâm an thần: 2 viên x 03 lần/ngày."
] | MERGE (chitiet_tp:ChiTietThanhPham {ten: thuoc_tp}) MERGE (tp)-[:BAO_GOM]->(chitiet_tp))
```

### 3. Thể Âm hư hỏa vượng

**Toàn bộ thông tin mô tả chứng trạng, mạch lý, pháp trị và các phương pháp vật lý trị liệu, cũng như 2 bài thuốc cốt lõi (Hoàng liên a giao thang, Thiên vương bổ tâm đan) đều được tạo node**^^^^^^^^.

**Cypher**

```
MATCH (b:BenhLy {id: "G47"}) MATCH (tp:ThanhPhamYHCT {ten: "Danh mục thuốc thành phẩm (Có thể thay thế loại thuốc khác có tác dụng tương đương)"})

MERGE (the2:TheLamSang {
    ten: "Âm hư hỏa vượng",
    trieu_chung: "Bứt rứt, mất ngủ, hồi hộp không yên. Chất lưỡi hồng, ít rêu hoặc không rêu. Mạch vì sác. Kèm theo: đau đầu, ù tai, hay quên, đau lưng, mộng tinh, ngũ tâm phiền nhiệt, miệng khô ít tân.",
    bat_cuong: "Lý hư hỏa",
    kinh_lac: "Bệnh tại cân cơ kinh lạc.",
    nguyen_nhan: "Nội nhân.",
    phap_dieu_tri: "Tư âm giáng hỏa, dưỡng tâm an thần."
})
MERGE (b)-[:BAO_GOM_THE]->(the2)
MERGE (the2)-[:DUNG_THUOC_THANH_PHAM]->(tp) // Kế thừa thuốc thành phẩm

MERGE (pp_cham2:PhuongPhap {ten: "Châm cứu", lieu_trinh: "20 phút/lần/ngày. Số lần châm cứu phụ thuộc vào tình trạng bệnh của bệnh nhân."})
MERGE (pp_caychi2:PhuongPhap {ten: "Cấy chỉ", lieu_trinh: "(Như công thức huyệt châm cứu). 02 tuần cấy 01 lần. Số lần cấy phụ thuộc vào tình trạng bệnh của bệnh nhân."})
MERGE (pp_xoabop2:PhuongPhap {ten: "Xoa bóp", lieu_trinh: "(thực hiện xoa, xát, miết, day, bóp, lăn các vùng đầu, cổ, vai, tay, chân). Bấm tả các huyệt chung: Bách hội, Thượng tinh, Thái dương, Phong trì. Bấm tả thần môn, Nội quan, Hợp cốc, Giải khê. Xoa bóp 30 phút/lần/ngày. Số lần xoa bóp tùy theo mức độ và diễn tiến của bệnh."})
MERGE (pp_thuycham2:PhuongPhap {ten: "Thủy châm", lieu_trinh: "hai bên Phong trì, Thái xung. Thủy châm một ngày một lần, mỗi lần thủy châm 2-3 huyệt. Một liệu trình điều trị từ 10-15 lần, có thể tiến hành 2-3 liệu trình liên tục."})
MERGE (the2)-[:YHCT_KHONG_THUOC]->(pp_cham2)
MERGE (the2)-[:YHCT_KHONG_THUOC]->(pp_caychi2)
MERGE (the2)-[:YHCT_KHONG_THUOC]->(pp_xoabop2)
MERGE (the2)-[:YHCT_KHONG_THUOC]->(pp_thuycham2)

FOREACH (h IN ["Nội quan", "Bách hội", "Thần môn", "An miên"] | MERGE (huyet:Huyet {ten: h}) MERGE (pp_cham2)-[:HUYET_CHINH]->(huyet))
FOREACH (h IN ["Đại lăng", "Thái xung"] | MERGE (huyet:Huyet {ten: h}) MERGE (pp_cham2)-[:CHAM_TA]->(huyet))
MERGE (h_tk:Huyet {ten: "Thái khê"}) MERGE (pp_cham2)-[:CHAM_BO]->(h_tk)

MERGE (bt_hoanglien:BaiThuoc {ten: "Hoàng liên a giao thang (Nghiệm phương tân biên)"})
MERGE (the2)-[:YHCT_DUNG_THUOC]->(bt_hoanglien)
FOREACH (vi IN [
    {ten: "A giao", lieu: "08-12g"}, {ten: "Bạch linh", lieu: "08-16g"}, {ten: "Bạch truật", lieu: "08-12g"},
    {ten: "Can khương", lieu: "02-06g"}, {ten: "Chích thảo", lieu: "04-08g"}, {ten: "Hoàng liên", lieu: "04-08g"},
    {ten: "Mộc hương", lieu: "04-08g"}, {ten: "Đảng sâm", lieu: "08-16g"}, {ten: "Ô mai", lieu: "04-08g"}, {ten: "Đại táo", lieu: "08-12g"}
] | MERGE (v:ViThuoc {ten: vi.ten}) MERGE (bt_hoanglien)-[:GOM_VI {lieu: vi.lieu}]->(v))

MERGE (gia_nong:GiaGiam {dieu_kien: "Nếu mặt nóng hơi hồng, chóng mặt, ù tai có thể gia"})
MERGE (bt_hoanglien)-[:GIA_GIAM]->(gia_nong)
FOREACH (vi_gia IN [{ten: "Mẫu lệ", lieu: "04-12g"}, {ten: "Quy bản", lieu: "04-12g"}] | MERGE (v:ViThuoc {ten: vi_gia.ten}) MERGE (gia_nong)-[:GIA_VI {lieu: vi_gia.lieu}]->(v))

MERGE (bt_thienvuong:BaiThuoc {ten: "Thiên vương bổ âm (Vạn bệnh hồi xuân)", tac_dung: "Tư âm dưỡng huyết"})
MERGE (the2)-[:YHCT_DUNG_THUOC]->(bt_thienvuong)
FOREACH (vi IN [
    {ten: "Bá tử nhân", lieu: "08-12g"}, {ten: "Hoàng liên", lieu: "04-10g"}, {ten: "Cát cánh", lieu: "04-10g"},
    {ten: "Đan sâm", lieu: "08-16g"}, {ten: "Đương quy", lieu: "08-20g"}, {ten: "Huyền sâm", lieu: "06-12"},
    {ten: "Mạch môn", lieu: "08-16g"}, {ten: "Ngũ vị tử", lieu: "04-08g"}, {ten: "Đảng sâm", lieu: "08-16g"},
    {ten: "Thạch xương bồ", lieu: "04-08g"}, {ten: "Phục thần", lieu: "08-16g"}, {ten: "Sinh địa", lieu: "08-12g"},
    {ten: "Thiên môn", lieu: "08-12g"}, {ten: "Táo nhân", lieu: "08-12g"}, {ten: "Viễn chí", lieu: "04-08g"}
] | MERGE (v:ViThuoc {ten: vi.ten}) MERGE (bt_thienvuong)-[:GOM_VI {lieu: vi.lieu}]->(v))
```

### 4. Thể Tâm đởm khí hư

**Tương tự, em bóc tách chẩn đoán bát cương, kinh lạc tạng phủ và ánh xạ 2 bài An thần định chí thang, Toan táo nhân thang cùng lưu ý phối hợp Quy tỳ thang theo phác đồ**^^^^^^^^.

**Cypher**

```
MATCH (b:BenhLy {id: "G47"}) MATCH (tp:ThanhPhamYHCT {ten: "Danh mục thuốc thành phẩm (Có thể thay thế loại thuốc khác có tác dụng tương đương)"}) MATCH (bt_quyty:BaiThuoc {ten: "Quy tỳ thang"})

MERGE (the3:TheLamSang {
    ten: "Tâm đởm khí hư",
    trieu_chung: "Mất ngủ hay mê, dễ kinh mà tỉnh giấc. Mạch vì huyền hoặc huyền nhược. Kèm theo: Hốt hoảng, sợ hãi, gặp việc dễ kinh, hồi hộp, hụt hơi, mệt mỏi, tiểu nhiều trong dài. Hoặc bứt rứt khó ngủ, người gầy, sắc mặt nhợt nhạt, dễ mệt mỏi, hoặc hồi hộp mất ngủ, bứt rứt không yên, hoa mắt, chóng mặt, miệng khô, họng khô.",
    bat_cuong: "Lý hư nhiệt.",
    kinh_lac: "Bệnh tại cân cơ kinh lạc và tạng tâm, phủ đởm.",
    nguyen_nhan: "nội nhân.",
    phap_dieu_tri: "Ích khí trấn kinh, an thần định chí."
})
MERGE (b)-[:BAO_GOM_THE]->(the3)
MERGE (the3)-[:DUNG_THUOC_THANH_PHAM]->(tp)

MERGE (pp_cham3:PhuongPhap {ten: "Châm cứu", lieu_trinh: "20 phút/lần/ngày. Số lần châm cứu phụ thuộc vào tình trạng bệnh của bệnh nhân."})
MERGE (pp_caychi3:PhuongPhap {ten: "Cấy chỉ", lieu_trinh: "(Như công thức huyệt châm cứu). 02 tuần cấy 01 lần. Số lần cấy phụ thuộc vào tình trạng bệnh của bệnh nhân."})
MERGE (pp_xoabop3:PhuongPhap {ten: "Xoa bóp", lieu_trinh: "(thực hiện xoa, xát, miết, day, bóp, lăn các vùng đầu, cổ, vai, tay, chân). Bấm tả các huyệt chung: Bách hội, Thượng tỉnh, Thái dương, Phong trì. Bấm tả thần môn, Nội quan, Hợp cốc, Giải khê. Xoa bóp 30 phút/lần/ngày. Số lần xoa bóp tùy theo mức độ và diễn tiến của bệnh."})
MERGE (pp_thuycham3:PhuongPhap {ten: "Thủy châm", lieu_trinh: "hai bên Phong trì, Ta6mm du, Đởm du. Thủy châm một ngày một lần, mỗi lần thủy châm 2-3 huyệt. Một liệu trình điều trị từ 10-15 lần, có thể tiến hành 2-3 liệu trình liên tục."})
MERGE (the3)-[:YHCT_KHONG_THUOC]->(pp_cham3)
MERGE (the3)-[:YHCT_KHONG_THUOC]->(pp_caychi3)
MERGE (the3)-[:YHCT_KHONG_THUOC]->(pp_xoabop3)
MERGE (the3)-[:YHCT_KHONG_THUOC]->(pp_thuycham3)

FOREACH (h IN ["Nội quan", "Bách hội", "Thần môn", "An miên"] | MERGE (huyet:Huyet {ten: h}) MERGE (pp_cham3)-[:HUYET_CHINH]->(huyet))
FOREACH (h IN ["Đại lăng", "Túc khiếu âm", "Hành gian", "Phong trì", "Phong long"] | MERGE (huyet:Huyet {ten: h}) MERGE (pp_cham3)-[:CHAM_TA]->(huyet))

MERGE (bt_anthan:BaiThuoc {ten: "An thần định chí thang (Y học tâm ngộ)"})
MERGE (the3)-[:YHCT_DUNG_THUOC]->(bt_anthan)
FOREACH (vi IN [
    {ten: "Long cốt", lieu: "04-10g"}, {ten: "Phục linh", lieu: "08-16g"}, {ten: "Thạch xương bồ", lieu: "04-08g"},
    {ten: "Đảng sâm", lieu: "08-12g"}, {ten: "Phục thần", lieu: "08-16g"}, {ten: "Viễn chí", lieu: "04-08g"}
] | MERGE (v:ViThuoc {ten: vi.ten}) MERGE (bt_anthan)-[:GOM_VI {lieu: vi.lieu}]->(v))

MERGE (dk_quyty:GiaGiam {dieu_kien: "Nếu bồn chồn mất ngủ, người gầy là do khí huyết bất túc. Có thể dùng phối hợp để ích khí, dưỡng huyết, an thần, trấn tỉnh."})
MERGE (the3)-[:PHOI_HOP_THUOC]->(dk_quyty)
MERGE (dk_quyty)-[:SU_DUNG_BAI]->(bt_quyty)

MERGE (bt_toantao:BaiThuoc {ten: "Toan táo nhân thang (Tạp bệnh nguyên lưu Tề chúc)"})
MERGE (dk_toantao:GiaGiam {dieu_kien: "Nếu âm huyết thiên hư gây bồn chồn, hồi hộp, bức rứt không yên, hoa mắt chóng mặt, miệng họng khô khát, chất lưỡi hồng, mạch huyền vi thì nên dùng"})
MERGE (the3)-[:YHCT_DUNG_THUOC]->(dk_toantao)
MERGE (dk_toantao)-[:SU_DUNG_BAI]->(bt_toantao)
FOREACH (vi IN [
    {ten: "Chích thảo", lieu: "04-08g"}, {ten: "Đương quy", lieu: "08-20g"}, {ten: "Hoàng kỳ", lieu: "08-12g"},
    {ten: "Liên nhục", lieu: "08-20g"}, {ten: "Đảng sâm", lieu: "08-12g"}, {ten: "Phục linh", lieu: "08-16g"},
    {ten: "Phục thần", lieu: "08-12g"}, {ten: "Táo nhân", lieu: "08-16g"}, {ten: "Trần bì", lieu: "04-08g"}, {ten: "Viễn chí", lieu: "04-08g"}
] | MERGE (v:ViThuoc {ten: vi.ten}) MERGE (bt_toantao)-[:GOM_VI {lieu: vi.lieu}]->(v))
```

### 5. Thể Đàm nhiệt nội nhiễu

Thể bệnh do Ngoại nhân, áp dụng pháp Thanh hóa nhiệt đàm và sử dụng Ôn đởm thang / Bảo hòa thang gia vị. **Mọi thông tin thuốc và huyệt đều được giữ nguyên vẹn**^^^^^^^^.

**Cypher**

```
MATCH (b:BenhLy {id: "G47"}) MATCH (tp:ThanhPhamYHCT {ten: "Danh mục thuốc thành phẩm (Có thể thay thế loại thuốc khác có tác dụng tương đương)"})

MERGE (the4:TheLamSang {
    ten: "Đàm nhiệt nội nhiễu",
    trieu_chung: "Mất ngủ, đau đầu, tức ngực, đàm nhiễu, bứt rứt. Chất lưỡi hồng, rêu vàng nhớt, mạch hoạt sác. Kèm theo: Buồn nôn, ợ hơi đắng miệng, hoa mắt hoặc đại tiện táo, mất ngủ cả đêm.",
    bat_cuong: "Biểu thực nhiệt.",
    kinh_lac: "Bệnh tại cân cơ kinh lạc.",
    nguyen_nhan: "ngoại nhân.",
    phap_dieu_tri: "Thanh hóa nhiệt đàm, hòa trung an thần."
})
MERGE (b)-[:BAO_GOM_THE]->(the4)
MERGE (the4)-[:DUNG_THUOC_THANH_PHAM]->(tp)

MERGE (pp_cham4:PhuongPhap {ten: "Châm cứu", lieu_trinh: "20 phút/lần/ngày. Số lần châm cứu phụ thuộc vào tình trạng bệnh của bệnh nhân."})
MERGE (pp_caychi4:PhuongPhap {ten: "Cấy chỉ", lieu_trinh: "(Như công thức huyệt châm cứu). 02 tuần cấy 01 lần. Số lần cấy phụ thuộc vào tình trạng bệnh của bệnh nhân."})
MERGE (pp_xoabop4:PhuongPhap {ten: "Xoa bóp", lieu_trinh: "(thực hiện xoa, xát, miết, day, bóp, lăn các vùng đầu, cổ, vai, tay, chân). Bấm tả các huyệt chung: Bách hội, Thượng tinh, Thái dương, Phong trì. Bấm tả thần môn, Nội quan, Hợp cốc, Phong long, Lệ đoài, Ân bạch. Xoa bóp 30 phút/lần/ngày. Số lần xoa bóp tùy theo mức độ và diễn tiến của bệnh."})
MERGE (pp_thuycham4:PhuongPhap {ten: "Thủy châm", lieu_trinh: "hai bên Phong trì, Tỳ du, Phong long. Thủy châm một ngày một lần, mỗi lần thủy châm 2-3 huyệt. Một liệu trình điều trị từ 10-15 lần, có thể tiến hành 2-3 liệu trình liên tục."})
MERGE (the4)-[:YHCT_KHONG_THUOC]->(pp_cham4)
MERGE (the4)-[:YHCT_KHONG_THUOC]->(pp_caychi4)
MERGE (the4)-[:YHCT_KHONG_THUOC]->(pp_xoabop4)
MERGE (the4)-[:YHCT_KHONG_THUOC]->(pp_thuycham4)

FOREACH (h IN ["Nội quan", "Bách hội", "Thần môn", "An miên"] | MERGE (huyet:Huyet {ten: h}) MERGE (pp_cham4)-[:HUYET_CHINH]->(huyet))
FOREACH (h IN ["Trung quản", "Phong long", "Lệ đoài", "Ân bạch"] | MERGE (huyet:Huyet {ten: h}) MERGE (pp_cham4)-[:CHAM_TA]->(huyet))

MERGE (bt_ondom:BaiThuoc {ten: "Ôn đởm thang (Bị cấp Thiên Kim Yếu phương) gia Qua lâu, Hoàng liên"})
MERGE (the4)-[:YHCT_DUNG_THUOC]->(bt_ondom)
FOREACH (vi IN [
    {ten: "Bán hạ chế", lieu: "04-12g"}, {ten: "Chỉ thực", lieu: "04-12g"}, {ten: "Chích thảo", lieu: "04-08g"},
    {ten: "Phục linh", lieu: "08-16g"}, {ten: "Trần bì", lieu: "04-08"}, {ten: "Trúc nhự", lieu: "08-12g"},
    {ten: "Qua lâu", lieu: "08-12g"}, {ten: "Hoàng liên", lieu: "04-08g"}
] | MERGE (v:ViThuoc {ten: vi.ten}) MERGE (bt_ondom)-[:GOM_VI {lieu: vi.lieu}]->(v))

MERGE (bt_baohoa:BaiThuoc {ten: "Bảo hòa thang gia vị để tiêu đạo, hòa trung, an thần"})
MERGE (dk_baohoa:GiaGiam {dieu_kien: "Nếu thực tích tương đối nặng, ợ chua nồng, bụng đầy trướng có thể dùng bài thuốc sau"})
MERGE (the4)-[:YHCT_DUNG_THUOC]->(dk_baohoa)
MERGE (dk_baohoa)-[:SU_DUNG_BAI]->(bt_baohoa)
FOREACH (vi IN [
    {ten: "Cam thảo", lieu: "04-08g"}, {ten: "Hậu phác", lieu: "06-12g"}, {ten: "Hương phụ", lieu: "04-08g"},
    {ten: "La bặc tử", lieu: "04-08g"}, {ten: "Liên kiều", lieu: "08-12g"}, {ten: "Mạch nha", lieu: "08-12g"},
    {ten: "Sơn tra", lieu: "08-12g"}, {ten: "Trần bì", lieu: "04-08g"}
] | MERGE (v:ViThuoc {ten: vi.ten}) MERGE (bt_baohoa)-[:GOM_VI {lieu: vi.lieu}]->(v))
```

### 6. Thể Can uất hóa hỏa & Dưỡng sinh lâm sàng

**Khối lệnh cuối cùng bao hàm Thể bệnh thứ 5 và các nguyên tắc dưỡng sinh chung được ghi chép trong phác đồ, nổi bật với sự kết hợp điều trị oxy cao áp**^^^^^^^^.

**Cypher**

```
MATCH (b:BenhLy {id: "G47"}) MATCH (tp:ThanhPhamYHCT {ten: "Danh mục thuốc thành phẩm (Có thể thay thế loại thuốc khác có tác dụng tương đương)"})

MERGE (the5:TheLamSang {
    ten: "Can uất hóa hỏa",
    trieu_chung: "Mất ngủ, bực bội dễ cáu, nếu nặng thì mất ngủ cả đêm. Đau tức mạng sườn, khát nước, thích uống, không muốn ăn, mặt đỏ, ù tai, tiểu tiện sẫm màu, hoặc hoa mắt, chóng mặt, đau đầu dữ dội, đại tiện táo bón. Rêu lưỡi vàng khô, mạch huyền hoặc sác đều là biểu hiện của thực nhiệt nội thịnh, lá chứng của can uất hóa hỏa.",
    bat_cuong: "Lý thực nhiệt.",
    kinh_lac: "Bệnh tại cân cơ kinh lạc và tạng can",
    nguyen_nhan: "nội nhân.",
    phap_dieu_tri: "Thanh can hỏa để an thần.."
})
MERGE (b)-[:BAO_GOM_THE]->(the5)
MERGE (the5)-[:DUNG_THUOC_THANH_PHAM]->(tp)

MERGE (pp_cham5:PhuongPhap {ten: "Châm cứu", lieu_trinh: "20 phút/lần/ngày. Số lần châm cứu phụ thuộc vào tình trạng bệnh của bệnh nhân."})
MERGE (pp_caychi5:PhuongPhap {ten: "Cấy chỉ", lieu_trinh: "(Như công thức huyệt châm cứu). 02 tuần cấy 01 lần. Số lần cấy phụ thuộc vào tình trạng bệnh của bệnh nhân."})
MERGE (pp_xoabop5:PhuongPhap {ten: "Xoa bóp", lieu_trinh: "(thực hiện xoa, xát, miết, day, bóp, lăn các vùng đầu, cổ, vai, tay, chân). Bấm tả các huyệt chung: Bách hội, Thượng tỉnh, Thái dương, Phong trì. Day bô Tam âm giao, Quan nguyên, Khí hải, Can du, Thận du. Xoa bóp 30 phút/lần/ngày. Số lần xoa bóp tùy theo mức độ và diễn tiến của bệnh."})
MERGE (pp_thuycham5:PhuongPhap {ten: "Thủy châm", lieu_trinh: "hai bên Phong trì, Thận du, Can du, Cách du. Thủy châm một ngày một lần, mỗi lần thủy châm 2-3 huyệt. Một liệu trình điều trị từ 10-15 lần, có thể tiến hành 2-3 liệu trình liên tục."})
MERGE (pp_oxy5:PhuongPhap {ten: "Điều trị bằng oxy cao áp", lieu_trinh: "1,6- 2.0 ATA x 60 phút, mỗi liệu trình 10-15 ngày. Số liệu trình tùy theo mức độ và diễn tiến của bệnh."})
MERGE (the5)-[:YHCT_KHONG_THUOC]->(pp_cham5)
MERGE (the5)-[:YHCT_KHONG_THUOC]->(pp_caychi5)
MERGE (the5)-[:YHCT_KHONG_THUOC]->(pp_xoabop5)
MERGE (the5)-[:YHCT_KHONG_THUOC]->(pp_thuycham5)
MERGE (the5)-[:YHCT_KHONG_THUOC]->(pp_oxy5)

FOREACH (h IN ["Nội quan", "Bách hội", "Thần môn", "An miên"] | MERGE (huyet:Huyet {ten: h}) MERGE (pp_cham5)-[:HUYET_CHINH]->(huyet))
FOREACH (h IN ["Can du", "Hành gian", "Đại lăng", "Tam âm giao"] | MERGE (huyet:Huyet {ten: h}) MERGE (pp_cham5)-[:CHAM_THEM]->(huyet))

MERGE (bt_longdom:BaiThuoc {ten: "Long đởm tả can thang gia vị"})
MERGE (the5)-[:YHCT_DUNG_THUOC]->(bt_longdom)
FOREACH (vi IN [
    {ten: "Long đờm thảo", lieu: "08-12g"}, {ten: "Hoàng câm", lieu: "04-12g"}, {ten: "Chi tử", lieu: "04-12g"},
    {ten: "Sài hồ", lieu: "04-08g"}, {ten: "Mộc thông", lieu: "08-12g"}, {ten: "Trạch tả", lieu: "08-12g"},
    {ten: "Xa tiền tử", lieu: "08-12g"}, {ten: "Sinh địa", lieu: "06-12g"}, {ten: "Đương quy", lieu: "08-12g"}, {ten: "Cam thảo", lieu: "04-06g"}
] | MERGE (v:ViThuoc {ten: vi.ten}) MERGE (bt_longdom)-[:GOM_VI {lieu: vi.lieu}]->(v))

MERGE (bt_duongquy:BaiThuoc {ten: "Đương quy long hội thang gia giảm để thanh tả can đởm thực hỏa"})
MERGE (dk_duongquy:GiaGiam {dieu_kien: "Nếu can đởm thực hỏa, can hỏa thượng viêm thì mất ngủ cả đêm, đau đầu dữ dội, hoa mắt chóng mặt, đại tiện táo bón, có thể dùng bài"})
MERGE (the5)-[:YHCT_DUNG_THUOC]->(dk_duongquy)
MERGE (dk_duongquy)-[:SU_DUNG_BAI]->(bt_duongquy)
FOREACH (vi IN [
    {ten: "Chi tử", lieu: "04-08g"}, {ten: "Đại hoàng", lieu: "02-08g"}, {ten: "Đương qui", lieu: "08-12g"},
    {ten: "Hoàng bá", lieu: "04-08g"}, {ten: "Hoàng câm", lieu: "04-08g"}, {ten: "Hoàng liên", lieu: "04-08g"},
    {ten: "Long đởm thảo", lieu: "04-12g"}, {ten: "Mộc hương", lieu: "08-10g"}
] | MERGE (v:ViThuoc {ten: vi.ten}) MERGE (bt_duongquy)-[:GOM_VI {lieu: vi.lieu}]->(v))

// Dưỡng sinh chung
MERGE (ds:DuongSinh {ten: "Dưỡng sinh cho các thể lâm sàng"})
MERGE (b)-[:YEU_CAU_DUONG_SINH]->(ds)
FOREACH (ds_ct IN ["Thư giãn", "Thở 04 thời", "Xoa ấm vùng đầu- mặt-cổ"] | MERGE (n:ChiTietDS {ten: ds_ct}) MERGE (ds)-[:BAO_GOM]->(n))
```

Dạ thưa Thầy, bộ script này khi chạy nạp vào Neo4j sẽ tạo thành một đồ thị khép kín và phân cấp rõ ràng, giữ lại tuyệt đối mọi câu chữ y khoa để phục vụ chính xác cho việc tra cứu hệ chuyên gia. Thầy có cần em hiệu chỉnh hoặc tối ưu hóa lại Relationship nào cho phù hợp với nghiệp vụ query của dự án không ạ?
