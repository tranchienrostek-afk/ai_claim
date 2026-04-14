---
name: Test Strategies
description: "Use when creating test questions, running quality benchmarks, configuring LLM-as-judge scoring, analyzing test failures, or improving accuracy. Triggers on 'test questions', 'benchmark', 'accuracy test', 'LLM judge', 'quality testing', 'test generation', 'scoring rubric'."
version: 1.0.0
---

# Test Strategies for Medical Knowledge Graph Q&A

## Test Data Format

### User-Provided Tests
```json
[
  {
    "id": 1,
    "topic": "Disease name (for routing)",
    "scenario": "Clinical scenario description",
    "question": "Specific clinical question",
    "answer": "Expected ground truth answer",
    "tags": ["category1", "category2"]
  }
]
```

### Auto-Generated Tests
For each ingested disease, generate 3 question types:

**Type 1: Diagnostic** (What/How to diagnose)
```
Template: "Bệnh nhân {age} tuổi, {symptoms}. Chẩn đoán xác định là gì?"
Ground truth: Extract from CHẨN ĐOÁN section of disease chunks
```

**Type 2: Treatment** (How to treat)
```
Template: "Phác đồ điều trị {disease_name} bao gồm những gì?"
Ground truth: Extract from ĐIỀU TRỊ section of disease chunks
```

**Type 3: Complication** (What can go wrong)
```
Template: "Biến chứng nguy hiểm của {disease_name} là gì?"
Ground truth: Extract from BIẾN CHỨNG section of disease chunks
```

## LLM-as-Judge Scoring

### 5-Tier Scale
| Score | Meaning | Criteria |
|-------|---------|---------|
| 1.0 | Excellent | >80% key points covered, medically correct |
| 0.75 | Good | 50-80% key points, missing some details |
| 0.5 | Partial | <50% key points, or right diagnosis but wrong treatment |
| 0.25 | Poor | Very little correct information |
| 0.0 | Wrong | Completely incorrect or irrelevant |

### Judge Prompt
```
Bạn là giám khảo chấm thi Y khoa với thang điểm 5 bậc.
So sánh NỘI DUNG Y KHOA, KHÔNG chấm theo format.

QUY TẮC:
- Chấp nhận từ đồng nghĩa y khoa Việt Nam
- KHÔNG trừ điểm nếu AI cung cấp thêm thông tin đúng
- Đánh giá NỘI DUNG, KHÔNG đánh giá format
- Đếm số ý AI trả lời đúng / tổng ý trong đáp án chuẩn

Trả về JSON: {"score": 0-1.0, "reason": "..."}
```

## Test Runner Configuration

```python
class TestRunner:
    max_workers = 5       # Parallel test execution
    retry_on_429 = 3      # Rate limit retries
    retry_delay = 5       # Seconds between retries
    timeout = 90          # Per-question timeout
```

## Failure Analysis Categories

After running tests, categorize failures:

### 1. Disease Routing Failures
- `resolve_disease_name()` returned wrong disease or None
- **Fix**: Add aliases to Disease node

### 2. Search Relevance Failures
- Right disease detected but chunks returned are irrelevant
- **Fix**: Re-embed chunks, check vector index

### 3. Context Gap Failures
- Relevant chunks found but missing the specific information asked
- **Fix**: Check if chunk boundaries split key content

### 4. LLM Generation Failures
- Good context but LLM generated wrong answer
- **Fix**: Improve system prompt, reduce noise in context

## Accuracy Targets

| Stage | Target | Action if Below |
|-------|--------|----------------|
| Initial ingestion | 70% | Expected — proceed to optimize |
| After optimization 1 | 80% | Acceptable — can continue |
| After optimization 2 | 85% | Good — can stop |
| After optimization 3 | 85%+ | If still below, report to user |

## Benchmark Best Practices

1. **Minimum 10 questions** per disease for statistical significance
2. **Mix question types**: diagnostic, treatment, complication
3. **Include edge cases**: rare diseases, overlapping symptoms
4. **Track trends**: compare accuracy across pipeline runs
5. **Save all reports**: `data/datatest/reports/report_{name}_{date}.xlsx`
