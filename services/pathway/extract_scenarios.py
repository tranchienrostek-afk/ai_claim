import json, glob, os

files = sorted(glob.glob("data/script/kich_ban_*.json"))
results = []
for f in files:
    bn = os.path.basename(f)
    num = bn.replace("kich_ban_","").replace(".json","")
    # skip non-numeric files like test reports
    if not num.isdigit():
        continue
    with open(f, encoding="utf-8") as fp:
        d = json.load(fp)
    if isinstance(d, list):
        d = d[0]
    story = d.get("cau_chuyen_y_khoa", {})
    lab = d.get("du_lieu_labeling_mau", {})
    title = story.get("tieu_de", "N/A")
    patient = story.get("benh_nhan", {})
    signs = story.get("benh_su_va_trieu_chung", {})
    raw = signs.get("raw_sign_mentions", signs.get("trieu_chung_co_nang", []))
    hist = signs.get("tien_su", "")
    svcs = [s["service_name_raw"] for s in lab.get("service_lines", [])]
    disease = lab.get("case_level", {}).get("main_disease_name_vi", "")

    ctx = []
    if patient:
        ctx.append("BN " + patient.get("gioi_tinh","") + " " + str(patient.get("tuoi","")) + " tuoi.")
    if isinstance(raw, list):
        ctx.extend(raw)
    if hist:
        ctx.append("Tien su: " + hist)

    results.append({
        "num": int(num),
        "title": title,
        "disease": disease,
        "services": svcs,
        "context": "\n".join(ctx)
    })

with open("scenarios_output.json", "w", encoding="utf-8") as out:
    json.dump(results, out, ensure_ascii=False, indent=2)
print("OK: wrote", len(results), "scenarios")
