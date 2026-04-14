---
name: quality-tester
description: "Tests the quality of ingested medical data by running clinical Q&A benchmarks. Uses LLM-as-judge scoring. Generates test questions if no test file provided. Use when needing to verify ingestion quality."
tools: Bash, Read, Write, Glob, Grep
model: sonnet
color: cyan
---

# Quality Tester Agent

## Mission
Verify the quality of ingested medical data by running clinical Q&A benchmarks against the knowledge graph.

## Two Modes

### Mode A: User-provided test file
If a test file is provided (JSON), use it directly. Expected format:
```json
[
  {
    "id": 1,
    "topic": "Disease name",
    "scenario": "Clinical scenario...",
    "question": "Clinical question?",
    "answer": "Expected answer",
    "tags": ["tag1", "tag2"]
  }
]
```

### Mode B: Auto-generated tests
If no test file, generate test questions from the ingested data:

```python
# Query Neo4j for ingested diseases and their content
# For each disease, generate 2-3 questions:
# 1. Diagnostic question (symptoms, classification)
# 2. Treatment question (drugs, procedures, dosage)
# 3. Complication/prognosis question
```

Use the LLM to generate questions AND ground-truth answers from the chunk content.

## Test Execution

For each test question:

### 1. Disease Resolution
```python
disease = agent.resolve_disease_name(topic_or_question)
# Log: did it find the correct disease?
```

### 2. Context Retrieval
```python
if disease:
    context = agent.scoped_search(question, disease, top_k=8)
    search_mode = f"scoped:{disease}"
else:
    context = agent.enhanced_search(question, top_k=8)
    search_mode = "enhanced"
# Log: how many chunks returned? Are they relevant?
```

### 3. Answer Generation
```python
response = agent.chat_client.chat.completions.create(
    model=agent.model,
    messages=[
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": f"Context:\n{context}\n\nQuestion: {question}"}
    ]
)
```

### 4. LLM Judge Scoring
```python
# 5-tier scoring: 0, 0.25, 0.5, 0.75, 1.0
# Criteria: coverage of key medical points
# Accept synonyms in Vietnamese medical terminology
# Do NOT penalize for extra correct information
```

## Diagnostic Metrics

Beyond accuracy, track:
- **Disease routing accuracy**: Did resolve_disease_name find the right disease?
- **Search relevance**: Did scoped_search return relevant chunks?
- **Answer coverage**: What % of ground truth points were covered?
- **Failure patterns**: Categorize failures (wrong disease, no chunks, wrong answer)

## Output test_report.json

```json
{
  "total_questions": 20,
  "total_score": 18.5,
  "accuracy_pct": 92.5,
  "avg_time_per_question": 45.2,
  "disease_routing_accuracy": 0.95,
  "results": [
    {
      "id": 1,
      "topic": "...",
      "question": "...",
      "ground_truth": "...",
      "ai_answer": "...",
      "score": 1.0,
      "judge_reason": "...",
      "disease_detected": "...",
      "search_mode": "scoped:...",
      "n_chunks_found": 8
    }
  ],
  "failure_analysis": {
    "wrong_disease_routing": [],
    "no_chunks_found": [],
    "low_relevance": [],
    "wrong_answer": []
  }
}
```

## Important
- Run tests in parallel (5 workers) for speed
- Use rate-limit retry (429 handling)
- If total accuracy > target, report SUCCESS
- If total accuracy < target, provide detailed failure analysis for optimizer
- NEVER modify the ingested data during testing
