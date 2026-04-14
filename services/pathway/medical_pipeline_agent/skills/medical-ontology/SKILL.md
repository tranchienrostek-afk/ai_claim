---
name: Medical Ontology
description: "Use when working with medical entity types, Neo4j graph model, ICD codes, Vietnamese medical terminology, disease naming conventions, or knowledge graph schema. Triggers on 'entity types', 'Neo4j schema', 'ICD code', 'medical ontology', 'disease naming', 'Vietnamese medical terms'."
version: 1.0.0
---

# Medical Ontology for Knowledge Graph

## Neo4j Node Types

### Core Content Nodes

| Label | Purpose | Key Properties |
|-------|---------|---------------|
| `:Chunk` | PDF-ingested content block | `disease_name`, `block_id`, `page_number`, `embedding`, `content`, `title`, `level`, `section_path` |
| `:Disease` | Disease entity | `name`, `icd_code`, `aliases` (list) |
| `:Protocol` | Umbrella for multi-disease PDF | `name` |
| `:Page` | Web-crawled content (V1 legacy) | `title`, `description`, `url`, `embedding` |

### Entity Nodes (extracted by LLM)

| Label | Examples | Properties |
|-------|----------|-----------|
| `:Drug` | Amoxicillin, Corticoid, PPI | `name`, `dosage`, `route` |
| `:Symptom` | Đau tai, Chảy mủ tai, Ù tai | `name` |
| `:LabTest` | CT scan, Thính lực đồ, Nội soi | `name` |
| `:Procedure` | Phẫu thuật nội soi, Mở khí quản | `name` |
| `:Complication` | Liệt mặt, Viêm màng não | `name` |
| `:OntologyClass` | Schema definition | `name`, `type` |

## Key Relationships

```
(:Chunk)-[:ABOUT_DISEASE]->(:Disease)          # Disease scoping
(:Chunk)-[:NEXT_CHUNK]->(:Chunk)               # Reading order
(:Chunk)-[:HAS_CHILD]->(:Chunk)                # Hierarchy
(:Protocol)-[:COVERS_DISEASE]->(:Disease)      # Multi-disease grouping
(:Chunk)-[:MENTIONS]->(:Drug|:Symptom|:LabTest) # Entity extraction
(:Page)-[:LINKS_TO|HAS_REFERENCE]->(:Page)     # V1 web links
```

## Vector Indexes

| Index Name | Label | Property | Dimensions | Similarity |
|------------|-------|----------|------------|------------|
| `chunk_vector_index` | Chunk | embedding | 1536 | cosine |
| `clinical_vector_index` | Page | embedding | 1536 | cosine |

## Fulltext Indexes

| Index Name | Label | Properties |
|------------|-------|-----------|
| `chunk_fulltext` | Chunk | content, title |

## Vietnamese Medical Naming Conventions

### Disease Names
- Official: from BYT (Bộ Y tế) protocols
- Pattern: Vietnamese name, sometimes with English/Latin in parentheses
- Examples: "Viêm tai giữa cấp tính trẻ em", "Xốp xơ tai (Otosclerosis)"

### Common Aliases to Add
| Official Name | Common Aliases |
|--------------|---------------|
| Viêm tai giữa cấp tính trẻ em | viêm tai giữa cấp, viêm tai giữa, VTG cấp |
| Viêm mũi xoang mạn tính | viêm xoang mạn, viêm xoang mạn tính |
| Ung thư vòm mũi họng | ung thư vòm họng, K vòm, NPC |
| Sốt xuất huyết Dengue | sốt xuất huyết, SXH, SXHD, dengue |

### Sub-section Names (NOT diseases — filter these out)
```
ĐỊNH NGHĨA, ĐẠI CƯƠNG, NGUYÊN NHÂN, TRIỆU CHỨNG,
CHẨN ĐOÁN, ĐIỀU TRỊ, TIÊN LƯỢNG, BIẾN CHỨNG,
PHÒNG BỆNH, DỰ PHÒNG, THEO DÕI, TÀI LIỆU THAM KHẢO
```

## Entity Extraction Prompt Template

```
Given this medical text chunk, extract entities into these categories:
- Drug: medication names with dosage if mentioned
- Symptom: clinical signs and symptoms
- LabTest: diagnostic tests, imaging, lab work
- Procedure: surgical or clinical procedures
- Complication: adverse outcomes or complications

Return JSON: {"drugs": [...], "symptoms": [...], "lab_tests": [...], "procedures": [...], "complications": [...]}

Text: {chunk_content}
```

## Disease Routing Logic

`resolve_disease_name(query)` in medical_agent.py:
1. Extract keywords from query (remove Vietnamese stop words)
2. Match against Disease nodes that have Chunk data (V2 only)
3. Use CONTAINS matching with 40% coverage threshold
4. Return best match or None (fallback to enhanced_search)
