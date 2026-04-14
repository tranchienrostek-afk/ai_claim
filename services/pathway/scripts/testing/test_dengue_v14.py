"""
Test runner for data_test_14.json (Dengue SXH) — concurrent, fast iteration.

Usage:
    python scripts/testing/test_dengue_v14.py 10          # test 10 câu, 10 workers
    python scripts/testing/test_dengue_v14.py 20 15       # test 20 câu, 15 workers
    python scripts/testing/test_dengue_v14.py             # full 100 câu, 10 workers
"""

import json
import time
import os
import sys
import io
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

NOTEBOOKLM_DIR = Path(__file__).resolve().parents[2]
if str(NOTEBOOKLM_DIR) not in sys.path:
    sys.path.insert(0, str(NOTEBOOKLM_DIR))

if sys.stdout.encoding != 'utf-8':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
if sys.stderr.encoding != 'utf-8':
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

from dotenv import load_dotenv
from medical_agent import MedicalAgent
from openai import AzureOpenAI
from server_support.paths import DATATEST_CASES_DIR

load_dotenv()

DISEASE_NAME = "Sốt xuất huyết Dengue"

SYSTEM_PROMPT = """Bạn là bác sĩ hồi sức cấp cứu chuyên sốt xuất huyết Dengue. Trả lời dựa trên ngữ cảnh được cung cấp.

QUY TẮC BẮT BUỘC:
- Tối đa 100 từ.
- Đi thẳng vào câu trả lời: thuốc gì, liều bao nhiêu, khi nào.
- Nêu con số cụ thể (liều mg/kg, ngưỡng mmol/L, %).
- Khi câu hỏi hỏi "tại sao" hoặc "hậu quả": BẮT BUỘC nêu cơ chế/nguy cơ từ ngữ cảnh.
- Khi câu hỏi nói "người lớn": CHỈ trả lời phần người lớn, KHÔNG trộn với trẻ em.
- Khi câu hỏi nói "trẻ em": CHỈ trả lời phần trẻ em.
- KHÔNG mở đầu bằng "Theo phác đồ...", "Dựa trên nguồn...". Trả lời trực tiếp.
- Khi ngữ cảnh có thông tin LIÊN QUAN nhưng không trực tiếp: suy luận từ dữ liệu có sẵn.
- CHỈ nói "Không có thông tin" khi ngữ cảnh HOÀN TOÀN không liên quan đến câu hỏi.
- Trích dẫn ĐÚNG con số từ ngữ cảnh, không tự thay đổi liều lượng."""


def build_context_str(context_nodes):
    parts = []
    for i, node in enumerate(context_nodes):
        title = node.get('title', 'N/A')
        content = node.get('description', '')
        section = node.get('section_path', '')
        prev_content = node.get('prev_block_content') or ''
        next_content = node.get('next_block_content') or ''

        part = f"[{i+1}] {title}"
        if section:
            part += f" | {section}"
        part += f"\n{content}"
        if prev_content:
            part += f"\n[Truoc]: {prev_content[:300]}"
        if next_content:
            part += f"\n[Sau]: {next_content[:300]}"
        parts.append(part)
    return "\n---\n".join(parts)


def process_one(item, agent, judge_client, judge_model):
    """Process a single question: search → generate → judge. Thread-safe."""
    q = item['cau_hoi']
    expected = item['dap_an']
    t_start = time.time()

    # 1. Search
    context = agent.scoped_search(q, DISEASE_NAME, top_k=15)
    context_str = build_context_str(context)
    search_time = time.time() - t_start

    # 2. Generate
    t_gen = time.time()
    messages = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": f"Ngu canh:\n{context_str}\n\nCau hoi: {q}"}
    ]
    try:
        resp = agent.chat_client.chat.completions.create(
            model=agent.model, messages=messages, temperature=0.1
        )
        answer = resp.choices[0].message.content
    except Exception as gen_err:
        err_str = str(gen_err)
        if 'content_filter' in err_str or 'content_management' in err_str:
            answer = "Nội dung bị lọc bởi hệ thống an toàn."
        elif 'temperature' in err_str.lower():
            try:
                resp = agent.chat_client.chat.completions.create(
                    model=agent.model, messages=messages
                )
                answer = resp.choices[0].message.content
            except Exception:
                answer = "Lỗi tạo câu trả lời."
        else:
            answer = "Lỗi tạo câu trả lời."
    gen_time = time.time() - t_gen

    # 3. Judge
    t_judge = time.time()
    eval_prompt = f"""So sanh cau tra loi AI voi dap an chuan. Cham diem 0.0 - 1.0.

TIEU CHI:
- 1.0: dung hoan toan y cot loi (thuoc, lieu, chi dinh, ly do deu khop)
- 0.8: dung y chinh, thieu chi tiet phu HOAC dien dat khac nhung DUNG noi dung
- 0.5: dung huong nhung sai con so hoac thieu y quan trong
- 0.2: sai phan lon
- 0.0: sai hoan toan hoac tra loi "Khong co thong tin"

NGUYEN TAC CHAM DIEM:
1. Neu AI tra loi DUNG hanh dong/thuoc/lieu nhung dien dat khac => 0.8 tro len
2. Neu AI them thong tin bo sung hop ly (khong sai) => KHONG tru diem
3. Neu AI tra loi dung y chinh nhung them chi tiet khac => 0.8
4. Chi cho 0.5 khi THIEU y cot loi QUAN TRONG nhat
5. Neu AI noi dung con so/lieu luong/nguong nhung them thong tin => 0.8+
6. Neu AI noi "Khong co thong tin" nhung dap an co trong phac do => 0.0

Cau hoi: {q}
Dap an chuan: {expected}
AI tra loi: {answer}

Tra ve JSON: {{"score": 0.0, "correct_key": "...", "missing": "..."}}"""

    try:
        judge_resp = judge_client.chat.completions.create(
            model=judge_model,
            messages=[{"role": "user", "content": eval_prompt}],
            temperature=0.0,
            response_format={"type": "json_object"}
        )
        judge_raw = judge_resp.choices[0].message.content
    except Exception as judge_err:
        judge_raw = '{"score": 0.0, "correct_key": "judge error", "missing": "' + str(judge_err)[:80] + '"}'
    judge_time = time.time() - t_judge

    try:
        judge_data = json.loads(judge_raw)
        score = float(judge_data.get("score", 0))
    except:
        score = 0.0
        judge_data = {"score": 0, "correct_key": "parse error", "missing": ""}

    total_time = time.time() - t_start
    word_count = len(answer.split())

    return {
        "id": item["id"],
        "phan": item.get('phan', ''),
        "score": score,
        "word_count": word_count,
        "search_time": round(search_time, 2),
        "gen_time": round(gen_time, 2),
        "judge_time": round(judge_time, 2),
        "total_time": round(total_time, 2),
        "question": q,
        "expected": expected,
        "answer": answer,
        "judge": judge_data,
    }


def run_test(limit=None, workers=10):
    test_file = str(DATATEST_CASES_DIR / "data_test_14.json")
    with open(test_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    if limit:
        data = data[:limit]

    n = len(data)
    agent = MedicalAgent()
    judge_client = AzureOpenAI(
        azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT").strip(),
        api_key=os.getenv("AZURE_OPENAI_API_KEY").strip(),
        api_version=os.getenv("AZURE_OPENAI_API_VERSION").strip()
    )
    judge_model = os.getenv("MODEL1", "gpt-4o-mini").strip()

    print(f"{'=' * 70}")
    print(f"  DENGUE TEST | {n} questions | {workers} workers | scoped_search")
    print(f"{'=' * 70}")

    results = [None] * n
    done_count = 0
    t_global = time.time()

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(process_one, item, agent, judge_client, judge_model): i
                   for i, item in enumerate(data)}
        for future in as_completed(futures):
            idx = futures[future]
            try:
                result = future.result()
                results[idx] = result
                done_count += 1
                score = result['score']
                flag = "OK" if score >= 0.8 else "!!" if score >= 0.5 else "XX"
                elapsed = time.time() - t_global
                print(f"  [{flag}] #{result['id']:>3} s={score:.1f} {result['word_count']:>3}w {result['total_time']:.0f}s | {result['question'][:55]}... [{done_count}/{n} {elapsed:.0f}s]")
                if score < 0.8:
                    print(f"       Exp: {result['expected'][:90]}")
                    print(f"       Got: {result['answer'][:90]}")
                    miss = result['judge'].get('missing', '')
                    if miss:
                        print(f"       Miss: {miss[:80]}")
            except Exception as e:
                done_count += 1
                print(f"  [ERR] idx={idx}: {e}")
                results[idx] = {"id": data[idx]["id"], "score": 0, "error": str(e)}

    total_elapsed = time.time() - t_global

    # Filter valid results
    valid = [r for r in results if r and 'score' in r]
    total_score = sum(r['score'] for r in valid)
    avg_score = total_score / len(valid) if valid else 0
    pass_count = sum(1 for r in valid if r['score'] >= 0.8)
    fail_count = sum(1 for r in valid if r['score'] < 0.5)
    avg_words = sum(r.get('word_count', 0) for r in valid) / len(valid) if valid else 0

    print(f"\n{'=' * 70}")
    print(f"  RESULTS | {total_elapsed:.0f}s total ({total_elapsed/n:.1f}s/q effective)")
    print(f"{'=' * 70}")
    print(f"  Avg score:     {avg_score:.2f}")
    print(f"  Pass (>=0.8):  {pass_count}/{n} ({100*pass_count/n:.0f}%)")
    print(f"  Fail (<0.5):   {fail_count}/{n} ({100*fail_count/n:.0f}%)")
    print(f"  Avg words:     {avg_words:.0f}")

    # Per-section
    sections = {}
    for r in valid:
        sec = r.get('phan', '')
        if sec not in sections:
            sections[sec] = []
        sections[sec].append(r['score'])

    print(f"\n  Per-section:")
    for sec, scores in sections.items():
        avg = sum(scores) / len(scores)
        p = sum(1 for s in scores if s >= 0.8)
        print(f"    {avg:.2f} ({p}/{len(scores)} pass) {sec[:55]}")

    # Worst questions
    sorted_results = sorted(valid, key=lambda r: r['score'])
    print(f"\n  Worst 5:")
    for r in sorted_results[:5]:
        print(f"    #{r['id']:>3} s={r['score']:.1f} | {r['question'][:70]}")

    print(f"{'=' * 70}")

    # Save
    report_path = str(NOTEBOOKLM_DIR / "data" / "reports" / "test_dengue_v14_results.json")
    os.makedirs(str((NOTEBOOKLM_DIR / "data" / "reports")), exist_ok=True)
    with open(report_path, 'w', encoding='utf-8') as f:
        json.dump({
            "summary": {
                "total": n, "avg_score": round(avg_score, 3),
                "pass_rate": round(pass_count / n, 3) if n else 0,
                "fail_rate": round(fail_count / n, 3) if n else 0,
                "avg_words": round(avg_words, 1),
                "total_time_s": round(total_elapsed, 1),
                "workers": workers,
            },
            "results": [r for r in results if r]
        }, f, ensure_ascii=False, indent=2)
    print(f"  Saved: {report_path}")

    agent.close()
    return avg_score


if __name__ == "__main__":
    limit = int(sys.argv[1]) if len(sys.argv) > 1 else None
    workers = int(sys.argv[2]) if len(sys.argv) > 2 else 10
    run_test(limit=limit, workers=workers)
