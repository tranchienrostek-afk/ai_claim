# Neo4j Disease Explorer

Generated from:

- `03_enrich\service_disease_matrix.json`
- `02_standardize\service_codebook.json`
- `09_unified_story_testcase\parallel_merged\data\unified_case_testset.jsonl`
- `05_observations\lab_observations.jsonl`

## Outputs

- `neo4j_disease_explorer.html`: giao diện xem bệnh theo graph
- `disease_graph_explorer_data.json`: dataset đầy đủ cho UI và debug
- `disease_graph_explorer_data.js`: local JS bundle để HTML mở trực tiếp từ file system
- `neo4j_disease_explorer_exports/*.csv`: node/relationship CSV để nạp Neo4j

## Snapshot

- Disease nodes: 224
- Diseases with case evidence: 186
- Disease -> Service edges: 2311
- Disease -> Sign edges: 976
- Service -> Observation edges: 536
- Disease -> Observation edges: 6118

## Neo4j import hint

Import các file CSV trong `neo4j_disease_explorer_exports` thành:

- `(:Disease)`
- `(:Service)`
- `(:Sign)`
- `(:Observation)`
- `(:Disease)-[:INDICATES_SERVICE]->(:Service)`
- `(:Disease)-[:HAS_SIGN]->(:Sign)`
- `(:Service)-[:HAS_OBSERVATION]->(:Observation)`
- `(:Disease)-[:HAS_OBSERVATION]->(:Observation)`
