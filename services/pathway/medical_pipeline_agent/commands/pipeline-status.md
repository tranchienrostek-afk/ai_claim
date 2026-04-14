---
description: "Show status of all pipeline runs and current Neo4j data"
argument-hint: "[--last N]"
---

# Pipeline Status

Show the current state of the medical knowledge graph and recent pipeline runs.

## Step 1: Neo4j Stats

```!
cd "$(dirname "${CLAUDE_PLUGIN_ROOT}")/.." && python3 -u -c "
import sys, io, json
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
from neo4j import GraphDatabase
driver = GraphDatabase.driver('bolt://localhost:7688', auth=('neo4j', 'password123'))
with driver.session() as s:
    diseases = s.run('MATCH (d:Disease)<-[:ABOUT_DISEASE]-(:Chunk) RETURN count(DISTINCT d) AS c').single()['c']
    chunks = s.run('MATCH (c:Chunk) RETURN count(c) AS c').single()['c']
    protocols = s.run('MATCH (p:Protocol)-[:COVERS_DISEASE]->(:Disease) RETURN count(DISTINCT p) AS c').single()['c']
    entities = s.run('MATCH (n) WHERE n:Drug OR n:Symptom OR n:LabTest OR n:Procedure RETURN count(n) AS c').single()['c']
    print(json.dumps({'diseases': diseases, 'chunks': chunks, 'protocols': protocols, 'entities': entities}, ensure_ascii=False))

    print('\n--- Diseases by Protocol ---')
    r = s.run('MATCH (p:Protocol)-[:COVERS_DISEASE]->(d:Disease)<-[:ABOUT_DISEASE]-(c:Chunk) RETURN p.name AS proto, count(DISTINCT d) AS diseases, count(c) AS chunks ORDER BY p.name')
    for rec in r:
        print(f'{rec[\"proto\"]}: {rec[\"diseases\"]} diseases, {rec[\"chunks\"]} chunks')

    r = s.run('MATCH (d:Disease)<-[:ABOUT_DISEASE]-(c:Chunk) WHERE NOT (d)<-[:COVERS_DISEASE]-(:Protocol) RETURN d.name AS disease, count(c) AS chunks ORDER BY d.name')
    for rec in r:
        print(f'  [standalone] {rec[\"disease\"]}: {rec[\"chunks\"]} chunks')
driver.close()
"
```

## Step 2: Recent Pipeline Runs

```!
cd "$(dirname "${CLAUDE_PLUGIN_ROOT}")/.." && ls -lt data/pipeline_runs/ 2>/dev/null | head -20 || echo "No pipeline runs yet"
```

List the most recent runs and their summaries. Read `run_summary.json` from the last few runs to show accuracy trends.
