"""
Chuẩn hoá tên dịch vụ cận lâm sàng (CLS) — Pipeline 3 tầng
=============================================================
Input:  subclinical_01.json → subclinical_20.json (3.793 hồ sơ, 25.911 dịch vụ)
Output:
  1. service_codebook.json          — bảng mã chuẩn (machine-readable)
  2. audit_trace.xlsx               — file truy vết cho nghiệp vụ BHYT review
  3. review_needed.xlsx             — các cụm cần người review (confidence thấp)

Chạy:
  python chuan_hoa_dich_vu.py
  python chuan_hoa_dich_vu.py --threshold 80   # điều chỉnh ngưỡng fuzzy
"""

import json
import re
import unicodedata
import hashlib
import argparse
from pathlib import Path
from collections import defaultdict, Counter
from datetime import datetime

import pandas as pd
from rapidfuzz import fuzz, process

# ============================================================
# CONFIG
# ============================================================
PROJECT_DIR = Path(__file__).parent.parent  # ho_so_kxn/
DATA_DIR = PROJECT_DIR / "01_claims"
OUTPUT_DIR = Path(__file__).parent  # 02_standardize/
NUM_FILES = 20
DEFAULT_FUZZY_THRESHOLD = 85  # Levenshtein token_sort_ratio

# Ngưỡng confidence cho review
CONFIDENCE_HIGH = 95      # Tự động accept
CONFIDENCE_MEDIUM = 85    # Accept nhưng flag
CONFIDENCE_LOW = 80       # Cần người review

# ============================================================
# TẦNG 1: NORMALIZE — Rule-based text cleaning
# ============================================================

# Bảng sửa lỗi OCR / typo phổ biến — mỗi entry có lý do để audit
OCR_FIXES = [
    # (sai, đúng, lý_do)
    ("tế bộ máu",    "tế bào máu",    "OCR nhầm bào→bộ"),
    ("tế bảo máu",   "tế bào máu",    "OCR nhầm bào→bảo"),
    ("tế bảo màu",   "tế bào máu",    "OCR nhầm bào→bảo, máu→màu"),
    ("tế bảo mẫu",   "tế bào máu",    "OCR nhầm bào→bảo, máu→mẫu"),
    ("sẻ bảo màu",   "tế bào máu",    "OCR nhầm tế→sẻ, bào→bảo"),
    ("ngoại vị",     "ngoại vi",      "OCR nhầm vi→vị"),
    ("ngoại vì",     "ngoại vi",      "OCR nhầm vi→vì"),
    ("ngoại vĩ",     "ngoại vi",      "OCR nhầm vi→vĩ"),
    ("ngoại ví",     "ngoại vi",      "OCR nhầm vi→ví"),
    ("băng quang",   "bàng quang",    "OCR nhầm dấu"),
    ("bảng quang",   "bàng quang",    "OCR nhầm dấu"),
    ("đêm laser",    "đếm laser",     "OCR nhầm dấu"),
    ("đểm laser",    "đếm laser",     "OCR nhầm dấu"),
    ("dem laser",     "đếm laser",     "OCR mất dấu đếm"),
    ("đến tổng",     "đếm tổng",     "OCR nhầm đến→đếm"),
    ("điểm laser",   "đếm laser",     "OCR nhầm đếm→điểm"),
    ("máy dem ",     "máy đếm ",     "OCR mất dấu đếm"),
    ("tại mũi họng", "tai mũi họng",  "Typo tại→tai"),
    ("tại mỗi họng", "tai mũi họng",  "Typo tại mỗi→tai mũi"),
    ("c- reactive",  "c-reactive",    "Khoảng trắng thừa"),
    ("1 máu ngoại",  "máu ngoại",    "OCR thêm số 1"),
    ("hoạt đô ",     "hoạt độ ",     "OCR nhầm độ→đô"),
    ("nước tiền",    "nước tiểu",    "OCR nhầm tiểu→tiền"),
    ("siệu âm",     "siêu âm",      "OCR nhầm siêu→siệu"),
    ("siêu ăm",     "siêu âm",      "OCR nhầm âm→ăm"),
    ("nội sơi",     "nội soi",      "OCR nhầm soi→sơi"),
    ("huỳnh quảng", "huỳnh quang",  "OCR nhầm quang→quảng"),
    ("ổng cứng",    "ống cứng",     "OCR nhầm ống→ổng"),
    ("ông cứng",    "ống cứng",     "OCR nhầm ống→ông"),
    ("tổng quất",   "tổng quát",    "OCR nhầm quát→quất"),
    ("tổng quật",   "tổng quát",    "OCR nhầm quát→quật"),
    ("căng tay",    "cẳng tay",     "OCR nhầm cẳng→căng"),
    ("cảng tay",    "cẳng tay",     "OCR nhầm cẳng→cảng"),
]


def normalize_service_name(raw: str) -> tuple[str, list[str]]:
    """
    Normalize tên dịch vụ. Trả về (cleaned_name, list[applied_rules]).
    applied_rules để truy vết mỗi bước đã làm gì.
    """
    rules_applied = []
    s = raw.strip()

    # R1: Bỏ prefix [DV]
    new_s = re.sub(r'^\[DV\]\s*', '', s, flags=re.IGNORECASE)
    if new_s != s:
        rules_applied.append("R1:bỏ_prefix_[DV]")
        s = new_s

    # R2: Bỏ suffix phòng khám / mã phòng: "| PTM 3-038 - Phòng khám..."
    new_s = re.sub(r'\s*\|.*$', '', s)
    if new_s != s:
        rules_applied.append("R2:bỏ_suffix_phòng_khám")
        s = new_s

    # R3: Chuẩn hoá ngoặc [] → ()
    new_s = s.replace('[', '(').replace(']', ')')
    if new_s != s:
        rules_applied.append("R3:ngoặc_[]→()")
        s = new_s

    # R4: Lowercase + NFC normalize
    new_s = unicodedata.normalize('NFC', s.lower().strip())
    if new_s != s:
        rules_applied.append("R4:lowercase+NFC")
        s = new_s

    # R5: Fix OCR / typo
    for wrong, right, reason in OCR_FIXES:
        if wrong in s:
            s = s.replace(wrong, right)
            rules_applied.append(f"R5:OCR_fix({reason})")

    # R6: Collapse whitespace
    new_s = re.sub(r'\s+', ' ', s).strip()
    if new_s != s:
        rules_applied.append("R6:whitespace")
        s = new_s

    return s, rules_applied


# ============================================================
# TẦNG 2: CLUSTER — Fuzzy matching
# ============================================================

# Bảng phân loại category dựa trên keyword
CATEGORY_RULES = [
    # (keywords_trong_tên, category_code, category_name)
    (["tổng phân tích tế bào máu", "công thức máu", "huyết đồ",
      "đếm tiểu cầu", "đông máu", "prothrombin", "aptt", "fibrinogen",
      "nhóm máu", "tốc độ lắng máu", "hồng cầu lưới"],
     "LAB-HEM", "Huyết học"),

    (["alt ", "alt(", "ast ", "ast(", "got)", "gpt)", "ggt",
      "creatinin", "glucose", "urê ", "ure ", "bilirubin",
      "cholesterol", "triglycerid", "hdl", "ldl", "protein toàn",
      "albumin", "acid uric", "calci", "phospho", "sắt huyết",
      "ferritin", "transferrin", "amylase", "lipase", "ldh",
      "ck ", "cpk", "điện giải", "na, k", "hba1c"],
     "LAB-BIO", "Sinh hoá"),

    (["crp", "c-reactive", "ige", "anti-hb", "hbsag", "anti-hcv",
      "hiv", "iga", "igm", "igg", "ana", "anti-ccp", "rf ",
      "miễn dịch", "kháng thể", "tự kháng thể", "complement",
      "interleukin", "tnf", "thyroglobulin"],
     "LAB-IMM", "Miễn dịch"),

    (["vi khuẩn", "cấy máu", "cấy nước", "cấy đờm", "pcr",
      "realtime", "virus test", "test nhanh", "influenza",
      "adeno", "rsv", "sars", "covid", "dengue", "toxocara",
      "helicobacter", "mycoplasma", "chlamydia", "lao",
      "afb", "genexpert", "nhuộm gram", "kháng sinh đồ"],
     "LAB-MIC", "Vi sinh"),

    (["nước tiểu", "tổng phân tích nước tiểu", "cặn lắng nước tiểu",
      "microalbumin niệu", "protein niệu"],
     "LAB-URI", "Nước tiểu"),

    (["x-quang", "xquang", "x quang", "chụp x"],
     "IMG-XRY", "X-quang"),

    (["siêu âm"],
     "IMG-USG", "Siêu âm"),

    (["clvt", "cắt lớp vi tính", "ct ", "ct(", "cone beam",
      "mri", "cộng hưởng từ", "pet"],
     "IMG-CTN", "CT/MRI"),

    (["nội soi"],
     "END-ENS", "Nội soi"),

    (["điện tim", "ecg", "ekg", "holter", "điện não", "eeg",
      "đo chức năng hô hấp", "phế dung", "đo thính lực",
      "nhĩ lượng", "thăm dò chức năng"],
     "FUN-DFT", "Thăm dò chức năng"),

    (["giải phẫu bệnh", "tế bào học", "sinh thiết", "fna",
      "chọc hút"],
     "PAT-PAT", "Giải phẫu bệnh"),
]


def classify_category(name: str) -> tuple[str, str]:
    """Phân loại dịch vụ vào category. Trả về (code, name)."""
    lower = name.lower()
    for keywords, code, cat_name in CATEGORY_RULES:
        for kw in keywords:
            if kw in lower:
                return code, cat_name
    return "GEN-OTH", "Chưa phân loại"


def generate_service_code(category_code: str, seq: int) -> str:
    """Sinh mã dịch vụ: LAB-HEM-001."""
    return f"{category_code}-{seq:03d}"


# ---- Token chốt y khoa (discriminator) ----
# Các token này PHẢI khớp để 2 tên được gộp cùng cluster.
# Ngăn gộp nhầm: "định lượng creatinin" vs "định lượng albumin"
#                 "đo hoạt độ ALT" vs "đo hoạt độ AST"
MEDICAL_DISCRIMINATORS = [
    # ============================================================
    # THỨ TỰ QUAN TRỌNG: dài → ngắn, cụ thể → chung.
    # Token đầu tiên match sẽ được dùng.
    # ============================================================

    # Enzyme / sinh hoá — bao gồm biến thể OCR
    "transferase", "transferrin",  # transferase (ALT) ≠ transferrin (protein) — ĐẶT ĐẦU
    "alt (gpt)", "alt(gpt)", "alt (sgpt)", "alt(sgpt)", "gptalt", "sgpt",
    "ast (got)", "ast(got)", "ast (sgot)", "ast(sgot)", "gotast", "sgot",
    "ggt", "gamma gt", "gama glutamyl", "gama gt",
    "amylase",
    "ldh", "ck-mb", "ck ", "cpk", "alp ",
    "troponin t", "troponin i", "troponin",
    "d-dimer",
    "bnp", "nt-probnp", "procalcitonin",
    "tacrolimus",
    "egfr",

    # Lipid panel — HDL/LDL trước cholesterol (cụ thể hơn)
    "hdl-c", "hdl - c", "hdl",
    "ldl-c", "ldl - c", "ldl",
    "cholesterol toàn phần", "cholesterol",
    "triglycerid",

    # Đường huyết — HbA1c tách riêng glucose
    "hba1c", "hbalc", "hbale", "hbaic",
    "fructosamin",
    "glucose",

    # Thận
    "creatinin", "creatinine", "crestinit", "creatine",
    "urê", "ure ", "urea",

    # Gan
    "bilirubin trực tiếp", "bilirubin toàn phần", "bilirubin",

    # Protein
    "protein toàn phần", "protein",
    "albumin",

    # Khoáng chất / ion — canxi trước calci
    "canxi toàn phần", "canxi",
    "acid uric",
    "calci toàn phần", "calci ",
    "phospho",
    "sắt huyết thanh", "sắt huyết", " sắt",  # " sắt" (leading space) tránh match "mắt"
    "ferritin",
    "magie", " mg",  # " mg" (leading space) tránh match "imaging" etc.
    "kẽm", " zn",

    # Hormone
    "anti-tpo", "anti-tg",
    "cortisol",
    "insulin",
    "c-peptid",
    "calcitonin",
    "thyroglobulin",
    "fsh", "lh ",
    "tsh",
    "ft4", "ft3", "ft 4", "ft 3",
    "t3", "t4",
    "estradiol", "progesterone", "testosterone",
    "prolactin",
    "amh",
    "vitamin d",

    # Điện giải
    "na, k", "điện giải",

    # Miễn dịch — CRP trước C3/C4 (tránh "crp" match "c")
    "c-reactive",
    "crp hs", "crp",
    "complement c3", "complement c4", " c3", " c4",
    "cyfra", "cea", "afp",
    "ca 125", "ca125", "ca 15-3", "ca 19-9", "ca 72-4",
    "psa",
    "ige", "iga", "igm", "igg",
    "hbsab", "anti-hbs", "hbsag", "hbc total", "hbc ",
    "anti-hcv", "hcv", "anti-hbe", "hbeag",
    "hiv",
    "anti-ccp", "ana ", "rf ",
    "amibe",

    # Vi sinh — cụ thể trước (norovirus trước virus)
    "influenza",
    "norovirus", "rotavirus", "adenovirus", "adeno",
    "rsv", "sars", "covid", "dengue",
    "giang mai", "treponema",
    "toxocara", "helicobacter", "mycoplasma", "chlamydia",
    "vi nấm", "vi khuẩn",
    "hsv", "cmv", "measles", "ev71",

    # Huyết học — thrombin trước prothrombin (prothrombin chứa thrombin)
    "tế bào máu ngoại vi", "tế bào máu", "công thức máu",
    "tiểu cầu", "đông máu",
    "thromboplastin",  # APTT full name
    "prothrombin",     # PT — match trước "thrombin"
    "thrombin",        # TT — chỉ match nếu KHÔNG có "pro" phía trước
    "aptt", "fibrinogen",
    "nhóm máu abo", "nhóm máu rh", "nhóm máu",
    "hồng cầu lưới", "tốc độ lắng",

    # Nước tiểu
    "nước tiểu", "microalbumin",

    # X-quang — vùng cơ thể cụ thể trước (dài → ngắn)
    "ngực thẳng", "ngực nghiêng", "lồng ngực", "ngực ",
    "khung chậu", "khớp cùng chậu", "cùng chậu",
    "cột sống cùng cụt", "cột sống thắt lưng", "cột sống cổ",
    "cột sống ngực", "cột sống",
    "bụng không chuẩn bị", "bụng",
    "khớp gối", "khớp vai", "khớp háng", "khớp hàng",
    "khớp cổ tay", "khớp cổ chân",
    "xoang", "hàm mặt", "sọ não", "sọ",
    "bàn ngón tay", "bàn tay", "bàn chân",
    "khuỷu tay", "cẳng chân", "cẳng tay", "căng tay",
    "cổ tay", "cổ chân",
    "xương đòn",
    "đùi", "cánh tay",
    "x-quang", "xquang", "x quang",

    # Siêu âm — cụ thể trước
    "đàn hồi mô gan", "đàn hồi mô vú", "đàn hồi mô",
    "siêu âm doppler tuyến giáp", "siêu âm doppler tuyến vú",
    "siêu âm màu tuyến giáp", "siêu âm màu tuyến vú",
    "siêu âm tuyến giáp", "siêu âm tuyến vú",
    "siêu âm ổ bụng", "siêu âm tim",
    "siêu âm tử cung", "siêu âm vú", "siêu âm khớp",
    "3 tháng đầu", "3 tháng giữa", "3 tháng cuối",
    "siêu âm doppler", "siêu âm phần mềm",
    "siêu âm",

    # CT/MRI — vùng cơ thể cụ thể
    "cắt lớp vi tính", "clvt", "cone beam",
    "cộng hưởng từ khớp", "cộng hưởng từ", "mri",

    # Nội soi — cụ thể trước
    "nội soi tai mũi họng huỳnh quang",
    "nội soi tai mũi họng", "nội soi mũi", "nội soi họng",
    "nội soi thực quản", "nội soi dạ dày",
    "nội soi đại trực tràng", "nội soi trực tràng", "nội soi đại tràng",
    "nội soi thanh quản",
    "nội soi tiêu hoá", "nội soi can thiệp", "nội soi",

    # Thăm dò chức năng
    "điện tâm đồ", "điện tim", "ecg", "holter",
    "điện não", "eeg",
    "thính lực", "nhĩ lượng",
    "phế dung", "chức năng hô hấp",

    # Generic lab department names — ngăn gộp nhầm "xn vi sinh" vs "xn sinh hóa"
    "vi sinh", "sinh hóa", "hóa sinh", "sinh học",
]


def extract_discriminator(name: str) -> str:
    """
    Trích token chốt y khoa từ tên dịch vụ.
    Trả về discriminator string (lowercase) hoặc "" nếu không tìm thấy.
    Dùng để ngăn gộp nhầm các xét nghiệm khác loại.
    """
    # Collapse whitespace + strip spaces inside parens trước khi match
    lower = re.sub(r'\s+', ' ', name.lower().strip())
    lower = re.sub(r'\(\s+', '(', lower)
    lower = re.sub(r'\s+\)', ')', lower)
    for token in MEDICAL_DISCRIMINATORS:
        if token in lower:
            return token
    return ""


def build_clusters(names_with_counts: dict[str, int],
                   threshold: int) -> list[dict]:
    """
    Gom tên dịch vụ thành clusters bằng fuzzy matching,
    CÓ KIỂM TRA token chốt y khoa để ngăn gộp nhầm.
    Trả về list[cluster_dict] với đầy đủ thông tin truy vết.
    """
    # Bước 1: Pre-compute discriminator cho mỗi tên
    name_discriminator = {name: extract_discriminator(name)
                          for name in names_with_counts}

    # Sắp xếp theo frequency giảm dần — tên phổ biến nhất làm canonical
    sorted_names = sorted(names_with_counts.items(), key=lambda x: -x[1])

    clusters = []  # list of {canonical, members: [{name, count, score}]}
    assigned = set()

    for name, count in sorted_names:
        if name in assigned:
            continue

        canon_disc = name_discriminator[name]

        # Tên này chưa thuộc cluster nào → tạo cluster mới, nó là canonical
        cluster = {
            "canonical": name,
            "members": [{"name": name, "count": count, "score": 100}],
            "total_count": count,
        }

        # Tìm các tên còn lại match với canonical
        remaining = [n for n, _ in sorted_names if n not in assigned and n != name]
        if remaining:
            matches = process.extract(
                name, remaining,
                scorer=fuzz.token_sort_ratio,
                limit=None,
                score_cutoff=CONFIDENCE_LOW,
            )
            for match_name, score, _ in matches:
                if match_name in assigned:
                    continue

                # === KIỂM TRA TOKEN CHỐT Y KHOA ===
                match_disc = name_discriminator[match_name]
                if canon_disc and match_disc and canon_disc != match_disc:
                    # Cả hai đều có discriminator nhưng KHÁC nhau
                    # → KHÔNG gộp (ví dụ: ALT vs AST, Creatinin vs Albumin)
                    continue

                cluster["members"].append({
                    "name": match_name,
                    "count": names_with_counts[match_name],
                    "score": round(score, 1),
                })
                cluster["total_count"] += names_with_counts[match_name]
                assigned.add(match_name)

        assigned.add(name)
        clusters.append(cluster)

    return clusters


def assign_confidence(cluster: dict, threshold: int) -> str:
    """Gán confidence level cho cluster."""
    if len(cluster["members"]) == 1:
        return "HIGH"  # chỉ 1 member, chắc chắn

    scores = [m["score"] for m in cluster["members"] if m["score"] < 100]
    if not scores:
        return "HIGH"

    min_score = min(scores)
    if min_score >= CONFIDENCE_HIGH:
        return "HIGH"
    elif min_score >= CONFIDENCE_MEDIUM:
        return "MEDIUM"
    else:
        return "LOW"


# ============================================================
# LOAD DATA
# ============================================================

def load_all_services() -> list[dict]:
    """
    Load toàn bộ dịch vụ từ 20 file. Giữ nguyên context để truy vết.
    Trả về list[{raw_service, amount, claim_id, record_claim_id,
                  clinic_name, diagnosis, file_source}]
    """
    all_services = []
    for i in range(1, NUM_FILES + 1):
        fpath = DATA_DIR / f"subclinical_{i:02d}.json"
        with open(fpath, encoding="utf-8") as f:
            records = json.load(f)

        for rec in records:
            claim_info = rec.get("input", {}).get("claim_info", {})
            clinic = claim_info.get("clinic_name", "")
            diagnosis = claim_info.get("diagnosis", "")
            record_claim_id = rec.get("claim_id", "")

            for svc in rec.get("input", {}).get("claims", []):
                all_services.append({
                    "raw_service": svc.get("service", ""),
                    "amount": svc.get("amount", 0),
                    "service_claim_id": svc.get("claim_id", ""),
                    "record_claim_id": record_claim_id,
                    "clinic_name": clinic,
                    "diagnosis": diagnosis,
                    "file_source": fpath.name,
                })

    return all_services


# ============================================================
# MAIN PIPELINE
# ============================================================

def run_pipeline(threshold: int = DEFAULT_FUZZY_THRESHOLD):
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    print(f"[{ts}] Bắt đầu pipeline chuẩn hoá dịch vụ CLS")
    print(f"  Fuzzy threshold: {threshold}")
    print()

    # --- Load ---
    print("📂 Đang load dữ liệu...")
    all_services = load_all_services()
    print(f"  Tổng dịch vụ: {len(all_services):,}")
    raw_unique = len(set(s["raw_service"] for s in all_services))
    print(f"  Tên unique (raw): {raw_unique:,}")
    print()

    # --- Tầng 1: Normalize ---
    print("🔧 Tầng 1: Normalize (rule-based)...")
    normalize_log = []  # full trace cho mỗi raw → cleaned

    for svc in all_services:
        cleaned, rules = normalize_service_name(svc["raw_service"])
        svc["cleaned_service"] = cleaned
        svc["normalize_rules"] = "; ".join(rules) if rules else "(không đổi)"

    # Thống kê normalize
    cleaned_unique = len(set(s["cleaned_service"] for s in all_services))
    reduced_pct = (1 - cleaned_unique / raw_unique) * 100
    print(f"  Tên unique sau normalize: {cleaned_unique:,} (giảm {reduced_pct:.1f}%)")
    print()

    # Đếm frequency mỗi cleaned name
    cleaned_counts = Counter(s["cleaned_service"] for s in all_services)

    # --- Tầng 2: Cluster ---
    print(f"🔗 Tầng 2: Fuzzy clustering (threshold={threshold})...")
    clusters = build_clusters(dict(cleaned_counts), threshold)
    print(f"  Số clusters: {len(clusters):,}")

    # Gán confidence + category + service_code
    cat_seqs = defaultdict(int)  # category_code → next seq

    for cluster in clusters:
        cluster["confidence"] = assign_confidence(cluster, threshold)
        cat_code, cat_name = classify_category(cluster["canonical"])
        cluster["category_code"] = cat_code
        cluster["category_name"] = cat_name
        cat_seqs[cat_code] += 1
        cluster["service_code"] = generate_service_code(cat_code, cat_seqs[cat_code])

        # Tính avg cost
        member_names = {m["name"] for m in cluster["members"]}
        amounts = [s["amount"] for s in all_services
                   if s["cleaned_service"] in member_names and s["amount"] > 0]
        cluster["avg_cost"] = round(sum(amounts) / len(amounts)) if amounts else 0
        cluster["min_cost"] = min(amounts) if amounts else 0
        cluster["max_cost"] = max(amounts) if amounts else 0

    # Build lookup: cleaned_name → cluster
    name_to_cluster = {}
    for cluster in clusters:
        for m in cluster["members"]:
            name_to_cluster[m["name"]] = cluster

    # Gán service_code vào mỗi service record
    for svc in all_services:
        cl = name_to_cluster.get(svc["cleaned_service"])
        if cl:
            svc["service_code"] = cl["service_code"]
            svc["canonical_name"] = cl["canonical"]
            svc["category"] = cl["category_name"]
            svc["cluster_confidence"] = cl["confidence"]
            svc["discriminator"] = extract_discriminator(svc["cleaned_service"])
            svc["fuzzy_score"] = next(
                (m["score"] for m in cl["members"]
                 if m["name"] == svc["cleaned_service"]),
                100
            )
        else:
            svc["service_code"] = "UNKNOWN"
            svc["canonical_name"] = svc["cleaned_service"]
            svc["category"] = "?"
            svc["cluster_confidence"] = "NONE"
            svc["discriminator"] = extract_discriminator(svc["cleaned_service"])
            svc["fuzzy_score"] = 0

    # Thống kê confidence
    conf_counts = Counter(c["confidence"] for c in clusters)
    print(f"  HIGH confidence:   {conf_counts.get('HIGH', 0):,} clusters")
    print(f"  MEDIUM confidence: {conf_counts.get('MEDIUM', 0):,} clusters")
    print(f"  LOW confidence:    {conf_counts.get('LOW', 0):,} clusters (→ cần review)")
    print()

    # --- Output ---
    print("📤 Xuất kết quả...")

    # 1. service_codebook.json
    codebook = {
        "version": "1.0",
        "generated": datetime.now().isoformat(),
        "config": {
            "fuzzy_threshold": threshold,
            "confidence_high": CONFIDENCE_HIGH,
            "confidence_medium": CONFIDENCE_MEDIUM,
            "confidence_low": CONFIDENCE_LOW,
        },
        "stats": {
            "total_services_processed": len(all_services),
            "raw_unique_names": raw_unique,
            "cleaned_unique_names": cleaned_unique,
            "total_clusters": len(clusters),
            "clusters_high": conf_counts.get("HIGH", 0),
            "clusters_medium": conf_counts.get("MEDIUM", 0),
            "clusters_low": conf_counts.get("LOW", 0),
        },
        "codebook": [],
    }

    for cl in sorted(clusters, key=lambda c: -c["total_count"]):
        codebook["codebook"].append({
            "service_code": cl["service_code"],
            "canonical_name": cl["canonical"],
            "category_code": cl["category_code"],
            "category_name": cl["category_name"],
            "confidence": cl["confidence"],
            "total_occurrences": cl["total_count"],
            "avg_cost_vnd": cl["avg_cost"],
            "min_cost_vnd": cl["min_cost"],
            "max_cost_vnd": cl["max_cost"],
            "variants": [
                {
                    "cleaned_name": m["name"],
                    "fuzzy_score": m["score"],
                    "occurrences": m["count"],
                }
                for m in sorted(cl["members"], key=lambda x: -x["count"])
            ],
        })

    codebook_path = OUTPUT_DIR / "service_codebook.json"
    with open(codebook_path, "w", encoding="utf-8") as f:
        json.dump(codebook, f, ensure_ascii=False, indent=2)
    print(f"  ✅ {codebook_path.name}")

    # 2. audit_trace.xlsx — TRUY VẾT NGƯỢC đầy đủ cho BHYT
    print("  📋 Đang tạo audit_trace.xlsx (file truy vết cho nghiệp vụ)...")

    # Sheet 1: Toàn bộ dịch vụ với mapping (1 dòng = 1 dịch vụ gốc)
    audit_rows = []
    for svc in all_services:
        audit_rows.append({
            "file_nguồn": svc["file_source"],
            "mã_hồ_sơ": svc["record_claim_id"],
            "mã_dịch_vụ_gốc": svc["service_claim_id"],
            "bệnh_viện": svc["clinic_name"],
            "chẩn_đoán": svc["diagnosis"],
            "tên_dịch_vụ_GỐC": svc["raw_service"],
            "tên_sau_normalize": svc["cleaned_service"],
            "luật_normalize": svc["normalize_rules"],
            "mã_chuẩn_hoá": svc["service_code"],
            "tên_chuẩn_hoá": svc["canonical_name"],
            "nhóm_dịch_vụ": svc["category"],
            "token_chốt_y_khoa": svc["discriminator"],
            "fuzzy_score": svc["fuzzy_score"],
            "confidence": svc["cluster_confidence"],
            "số_tiền_VNĐ": svc["amount"],
        })
    df_audit = pd.DataFrame(audit_rows)

    # Sheet 2: Bảng mã (1 dòng = 1 service_code)
    code_rows = []
    for cl in sorted(clusters, key=lambda c: c["service_code"]):
        variant_list = " | ".join(
            f"{m['name']} (×{m['count']}, score={m['score']})"
            for m in sorted(cl["members"], key=lambda x: -x["count"])
        )
        code_rows.append({
            "mã_chuẩn_hoá": cl["service_code"],
            "tên_chuẩn_hoá": cl["canonical"],
            "nhóm": cl["category_name"],
            "confidence": cl["confidence"],
            "tổng_lần_xuất_hiện": cl["total_count"],
            "số_biến_thể": len(cl["members"]),
            "chi_phí_TB": cl["avg_cost"],
            "chi_phí_min": cl["min_cost"],
            "chi_phí_max": cl["max_cost"],
            "danh_sách_biến_thể": variant_list,
        })
    df_codes = pd.DataFrame(code_rows)

    # Sheet 3: Normalize rules summary — thống kê mỗi rule áp dụng bao nhiêu lần
    rule_counter = Counter()
    for svc in all_services:
        if svc["normalize_rules"] != "(không đổi)":
            for r in svc["normalize_rules"].split("; "):
                rule_counter[r] += 1
    rule_rows = [{"luật": rule, "số_lần_áp_dụng": count}
                 for rule, count in rule_counter.most_common()]
    df_rules = pd.DataFrame(rule_rows) if rule_rows else pd.DataFrame(
        columns=["luật", "số_lần_áp_dụng"])

    audit_path = OUTPUT_DIR / "audit_trace.xlsx"
    with pd.ExcelWriter(audit_path, engine="xlsxwriter") as writer:
        df_audit.to_excel(writer, sheet_name="Chi tiết truy vết", index=False)
        df_codes.to_excel(writer, sheet_name="Bảng mã chuẩn hoá", index=False)
        df_rules.to_excel(writer, sheet_name="Thống kê luật normalize", index=False)

        # Auto-adjust column widths
        for sheet_name, df in [("Chi tiết truy vết", df_audit),
                                ("Bảng mã chuẩn hoá", df_codes),
                                ("Thống kê luật normalize", df_rules)]:
            ws = writer.sheets[sheet_name]
            for idx, col in enumerate(df.columns):
                max_len = max(
                    df[col].astype(str).str.len().max() if len(df) > 0 else 0,
                    len(col)
                )
                ws.set_column(idx, idx, min(max_len + 2, 60))

            # Freeze top row
            ws.freeze_panes(1, 0)

            # Auto-filter
            if len(df) > 0:
                ws.autofilter(0, 0, len(df), len(df.columns) - 1)

    print(f"  ✅ {audit_path.name}")

    # 3. review_needed.xlsx — chỉ các cluster LOW confidence cần người kiểm tra
    low_clusters = [cl for cl in clusters if cl["confidence"] == "LOW"]
    if low_clusters:
        print(f"  ⚠️  Đang tạo review_needed.xlsx ({len(low_clusters)} clusters cần review)...")
        review_rows = []
        for cl in sorted(low_clusters, key=lambda c: -c["total_count"]):
            for m in sorted(cl["members"], key=lambda x: -x["count"]):
                # Lấy vài ví dụ hồ sơ dùng tên này
                examples = [
                    s for s in all_services
                    if s["cleaned_service"] == m["name"]
                ][:3]
                example_str = " | ".join(
                    f"BV:{e['clinic_name']}, CĐ:{e['diagnosis'][:50]}, "
                    f"Giá:{e['amount']:,}đ"
                    for e in examples
                )
                review_rows.append({
                    "mã_chuẩn_hoá": cl["service_code"],
                    "tên_chuẩn_hoá_đề_xuất": cl["canonical"],
                    "tên_biến_thể": m["name"],
                    "fuzzy_score": m["score"],
                    "số_lần": m["count"],
                    "nhóm_đề_xuất": cl["category_name"],
                    "ví_dụ_hồ_sơ": example_str,
                    "ĐÚNG_SAI": "",          # ← cột để người review điền
                    "TÊN_ĐÚNG_NẾU_SAI": "",  # ← cột để người sửa
                    "GHI_CHÚ": "",            # ← cột ghi chú
                })

        df_review = pd.DataFrame(review_rows)
        review_path = OUTPUT_DIR / "review_needed_v2.xlsx"
        with pd.ExcelWriter(review_path, engine="xlsxwriter") as writer:
            df_review.to_excel(writer, sheet_name="Cần review", index=False)
            ws = writer.sheets["Cần review"]

            for idx, col in enumerate(df_review.columns):
                max_len = max(
                    df_review[col].astype(str).str.len().max(),
                    len(col)
                )
                ws.set_column(idx, idx, min(max_len + 2, 60))

            ws.freeze_panes(1, 0)
            ws.autofilter(0, 0, len(df_review), len(df_review.columns) - 1)

            # Highlight cột ĐÚNG_SAI bằng màu vàng để reviewer dễ thấy
            fmt_yellow = writer.book.add_format({"bg_color": "#FFFF00"})
            col_idx = list(df_review.columns).index("ĐÚNG_SAI")
            for row_idx in range(1, len(df_review) + 1):
                ws.write(row_idx, col_idx, "", fmt_yellow)
            col_idx2 = list(df_review.columns).index("TÊN_ĐÚNG_NẾU_SAI")
            for row_idx in range(1, len(df_review) + 1):
                ws.write(row_idx, col_idx2, "", fmt_yellow)

        print(f"  ✅ {review_path.name}")
    else:
        print("  ✅ Không có cluster nào cần review (tất cả HIGH/MEDIUM)")

    # --- Summary ---
    print()
    print("=" * 60)
    print("📊 TÓM TẮT")
    print("=" * 60)
    print(f"  Dịch vụ đã xử lý:        {len(all_services):>8,}")
    print(f"  Tên unique (raw):         {raw_unique:>8,}")
    print(f"  Tên unique (normalized):  {cleaned_unique:>8,}")
    print(f"  Số clusters (mã chuẩn):   {len(clusters):>8,}")
    print(f"    ├─ HIGH confidence:      {conf_counts.get('HIGH', 0):>8,}")
    print(f"    ├─ MEDIUM confidence:    {conf_counts.get('MEDIUM', 0):>8,}")
    print(f"    └─ LOW (cần review):     {conf_counts.get('LOW', 0):>8,}")
    print()
    print("  Output files:")
    print(f"    📁 service_codebook.json  — bảng mã (cho hệ thống)")
    print(f"    📁 audit_trace.xlsx       — truy vết ngược (cho BHYT)")
    print(f"    📁 review_needed.xlsx     — danh sách cần review")
    print()

    # Top 10 clusters lớn nhất
    print("  Top 10 dịch vụ phổ biến nhất:")
    for i, cl in enumerate(sorted(clusters, key=lambda c: -c["total_count"])[:10], 1):
        conf_mark = {"HIGH": "✅", "MEDIUM": "⚡", "LOW": "⚠️"}[cl["confidence"]]
        print(f"    {i:>2}. [{cl['service_code']}] {cl['canonical'][:55]}"
              f"  ({cl['total_count']:,}×) {conf_mark}")

    return codebook


# ============================================================
# ENTRY
# ============================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Chuẩn hoá tên dịch vụ CLS")
    parser.add_argument("--threshold", type=int, default=DEFAULT_FUZZY_THRESHOLD,
                        help=f"Ngưỡng fuzzy matching (default={DEFAULT_FUZZY_THRESHOLD})")
    args = parser.parse_args()

    run_pipeline(threshold=args.threshold)
