"""
Universal Auto-Pipeline Orchestrator — AI-driven medical PDF ingestion.

Hospital uploads any clinical protocol PDF → system auto-analyzes, designs pipeline, ingests to Neo4j.
No developer intervention needed.

Architecture:
    Phase 1: DocumentAnalyzer  — LLM classifies disease/domain/ICD from sample pages
    Phase 2: PipelineConfigurator — LLM generates entity types + ontology + extraction prompts
    Phase 3: UniversalIngest — Config-driven engine (reuses V2 components)

Usage:
    cd notebooklm
    python universal_ingest.py "path/to/any_medical_protocol.pdf"

    # Or in Python:
    from universal_ingest import UniversalIngest
    result = UniversalIngest.auto_ingest("Phac_do_tang_huyet_ap_2022.pdf")
"""

import os
import re
import json
import sys
import unicodedata
import traceback
import io

# Force UTF-8 output on Windows to handle Vietnamese characters
if sys.stdout.encoding != 'utf-8':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
if sys.stderr.encoding != 'utf-8':
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')
from datetime import datetime
from pathlib import Path
from typing import Any

import fitz  # PyMuPDF
from openai import AzureOpenAI
from neo4j import GraphDatabase
from pydantic import BaseModel, Field, field_validator
from runtime_env import load_notebooklm_env

from concurrent.futures import ThreadPoolExecutor, as_completed
from v2_ingest import clean_ocr_text, SemanticChunker

load_notebooklm_env()


# ---------------------------------------------------------------------------
# Data Models (Pydantic)
# ---------------------------------------------------------------------------

class DocumentProfile(BaseModel):
    """Phase 1 output — metadata about the PDF document."""
    disease_name: str
    disease_aliases: list[str] = []
    icd_code: str
    medical_domain: str  # TCM | Internal_Medicine | Surgery | Pediatrics | OBGYN | Oncology | Mixed
    document_type: str   # treatment_guideline | diagnostic_guideline | drug_protocol
    pdf_quality: str     # digital | scanned_ocr | mixed
    has_tables: bool = False
    has_flowcharts: bool = False
    has_appendices: bool = False
    heading_style: str = "mixed"  # roman_numeral | arabic_numeral | mixed
    estimated_pages: int = 0
    publisher: str = ""
    year: int | None = None
    summary: str = ""
    source_type: str = "BYT"           # "BYT" | "hospital"
    hospital_name: str | None = None   # e.g. "Vinmec", "Bạch Mai"

    @field_validator("medical_domain")
    @classmethod
    def validate_domain(cls, v):
        allowed = {"TCM", "Internal_Medicine", "Surgery", "Pediatrics", "OBGYN", "Oncology", "Mixed"}
        if v not in allowed:
            return "Mixed"
        return v


class EntityTypeConfig(BaseModel):
    """Configuration for one entity type to extract."""
    label: str
    examples: list[str] = []


class OntologyNode(BaseModel):
    """A seed node to MERGE into Neo4j."""
    label: str
    name: str
    properties: dict[str, str] = {}


class OntologyRelationship(BaseModel):
    """A seed relationship to MERGE into Neo4j."""
    from_label: str
    from_name: str
    to_label: str
    to_name: str
    rel_type: str


class IngestConfig(BaseModel):
    """Phase 2 output — complete pipeline configuration."""
    # Identity
    disease_name: str
    icd_code: str
    protocol_name: str

    # Chunking
    max_chunk_size: int = 3000
    min_chunk_size: int = 100
    skip_first_page: bool = True
    extra_heading_patterns: list[str] = []
    skip_pages: list[int] = []

    # Entity extraction
    entity_types: list[EntityTypeConfig]
    extraction_system_prompt: str
    extraction_user_prompt_template: str  # Must contain {chunk_content} and {section_path}

    # Ontology
    ontology_nodes: list[OntologyNode] = []
    ontology_relationships: list[OntologyRelationship] = []

    # Source
    source_type: str = "BYT"           # "BYT" | "hospital"
    hospital_name: str | None = None   # e.g. "Vinmec", "Bạch Mai"

    # Processing
    needs_ocr_cleanup: bool = True


# Label allowlist for Neo4j — prevents injection via LLM-generated labels
ALLOWED_LABELS = {
    "Drug", "Disease", "Symptom", "LabTest", "Stage", "Procedure",
    "Herb", "Formula", "Syndrome", "AcupuncturePoint",
    "ICD_Chapter", "ICD_Category", "ICD_Block",
    "Instrument", "Complication", "Protocol", "Hospital",
    "RiskFactor", "Biomarker", "Treatment", "DiagnosticCriteria",
}


# ---------------------------------------------------------------------------
# Phase 1: DocumentAnalyzer
# ---------------------------------------------------------------------------

class DocumentAnalyzer:
    """Analyze a PDF to produce a DocumentProfile using heuristics + 1 LLM call."""

    def __init__(self, client: AzureOpenAI, model: str):
        self.client = client
        self.model = model

    def extract_sample_text(self, pdf_path: str) -> tuple[str, int]:
        """Extract text from strategic pages: [0,1,2, mid, last]. Returns (text, total_pages)."""
        doc = fitz.open(pdf_path)
        total_pages = len(doc)
        # Strategic page selection
        sample_indices = [0, 1, 2]
        if total_pages > 6:
            sample_indices.append(total_pages // 2)
        if total_pages > 3:
            sample_indices.append(total_pages - 1)
        # Deduplicate and bound
        sample_indices = sorted(set(i for i in sample_indices if 0 <= i < total_pages))

        parts = []
        for idx in sample_indices:
            page = doc[idx]
            blocks = page.get_text("blocks")
            blocks.sort(key=lambda b: (b[1], b[0]))
            page_text = f"--- Page {idx + 1} ---\n"
            for b in blocks:
                page_text += b[4] + "\n"
            parts.append(page_text)
        doc.close()
        return "\n".join(parts), total_pages

    def detect_pdf_quality(self, pdf_path: str) -> str:
        """Detect PDF text quality using Vietnamese-aware heuristics.

        Checks multiple sample pages for garbled fonts, missing diacritics,
        and scanned content. Returns: 'digital' | 'mixed' | 'scanned_ocr' | 'garbled_font'.
        """
        from server_support.pdf_vision_reader import _assess_text_quality

        doc = fitz.open(pdf_path)
        total = len(doc)
        # Sample pages: 3rd page + mid page (skip cover/TOC)
        sample_indices = [min(2, total - 1)]
        if total > 10:
            sample_indices.append(total // 2)

        scores = []
        has_images_any = False
        for idx in sample_indices:
            page = doc[idx]
            text = page.get_text("text")
            images = page.get_images()
            if images:
                has_images_any = True
            score = _assess_text_quality(text)
            scores.append((len(text.strip()), score))
        doc.close()

        avg_chars = sum(s[0] for s in scores) / len(scores)
        avg_quality = sum(s[1] for s in scores) / len(scores)

        if avg_chars < 50 and has_images_any:
            return "scanned_ocr"
        elif avg_quality < 0.4:
            return "garbled_font"  # Vietnamese font encoding broken
        elif avg_quality < 0.7 or (avg_chars < 200 and has_images_any):
            return "mixed"
        return "digital"

    def detect_structure(self, sample_text: str) -> dict:
        """Regex-based detection of tables, flowcharts, appendices, heading style."""
        has_tables = bool(re.search(r'Bảng\s+\d+|bảng\s+\d+|\|.*\|.*\|', sample_text))
        has_flowcharts = bool(re.search(r'[Ll]ưu đồ\s+\d+|[Ss]ơ đồ\s+\d+', sample_text))
        has_appendices = bool(re.search(r'[Pp]hụ lục\s*\d*', sample_text))

        roman = bool(re.search(r'^[IVX]+\.\s', sample_text, re.MULTILINE))
        arabic = bool(re.search(r'^\d+\.\d+\.?\s', sample_text, re.MULTILINE))
        if roman and arabic:
            heading_style = "mixed"
        elif roman:
            heading_style = "roman_numeral"
        elif arabic:
            heading_style = "arabic_numeral"
        else:
            heading_style = "mixed"

        return {
            "has_tables": has_tables,
            "has_flowcharts": has_flowcharts,
            "has_appendices": has_appendices,
            "heading_style": heading_style,
        }

    def classify_text(self, sample_text: str, total_pages: int = 1,
                       quality: str = "digital", structure: dict | None = None) -> dict | None:
        """LLM classification of medical text sample → metadata dict.

        Returns dict with: disease_name, disease_aliases, icd_code, medical_domain,
        document_type, publisher, year, summary. Or None on failure.
        """
        if structure is None:
            structure = self.detect_structure(sample_text)

        system_prompt = (
            "Bạn là hệ thống phân loại tài liệu y khoa Việt Nam. "
            "Phân tích văn bản mẫu và trả về metadata. Trả về JSON duy nhất."
        )
        user_prompt = f"""Phân tích tài liệu y khoa. Trả về JSON:
{{
  "disease_name": "tên bệnh chính bằng tiếng Việt",
  "disease_aliases": ["tên tiếng Anh", "tên viết tắt"],
  "icd_code": "mã ICD-10 chính xác nhất",
  "medical_domain": "chọn từ: TCM, Internal_Medicine, Surgery, Pediatrics, OBGYN, Oncology, Mixed",
  "document_type": "treatment_guideline hoặc diagnostic_guideline hoặc drug_protocol",
  "publisher": "cơ quan ban hành",
  "year": null hoặc năm ban hành (số nguyên),
  "summary": "2-3 câu mô tả nội dung chính"
}}

medical_domain: TCM = Y học cổ truyền, Internal_Medicine = Nội khoa, Surgery = Ngoại khoa, Pediatrics = Nhi khoa, OBGYN = Sản phụ khoa, Oncology = Ung bướu.
icd_code: mã ICD-10 chính xác nhất cho bệnh chủ đề.

Heuristic đã phát hiện: quality={quality}, structure={json.dumps(structure, ensure_ascii=False)}
Tổng trang: {total_pages}

Văn bản mẫu:
{sample_text[:5000]}"""

        for attempt in range(3):
            try:
                response = self.client.chat.completions.create(
                    model=self.model,
                    messages=[
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_prompt},
                    ],
                    response_format={"type": "json_object"},
                    temperature=0.0,
                )
                return json.loads(response.choices[0].message.content)
            except Exception as e:
                if attempt == 2:
                    print(f"  [WARN] LLM classification failed: {e}")
                    return None

    def analyze(self, pdf_path: str) -> DocumentProfile:
        """Full analysis: heuristics + 1 LLM call → DocumentProfile."""
        print("[Phase 1] Analyzing PDF...")

        # Step 1: Extract sample text
        sample_text, total_pages = self.extract_sample_text(pdf_path)
        print(f"  Sampled {len(sample_text)} chars from {total_pages} pages")

        # Step 2: Heuristic checks (no LLM)
        quality = self.detect_pdf_quality(pdf_path)
        structure = self.detect_structure(sample_text)
        print(f"  Quality: {quality}, Structure: {structure}")

        # Step 3: LLM classification
        system_prompt = (
            "Bạn là hệ thống phân loại tài liệu y khoa Việt Nam. "
            "Phân tích văn bản mẫu và trả về metadata. Trả về JSON duy nhất."
        )
        user_prompt = f"""Phân tích tài liệu y khoa. Trả về JSON:
{{
  "disease_name": "tên bệnh chính bằng tiếng Việt",
  "disease_aliases": ["tên tiếng Anh", "tên viết tắt"],
  "icd_code": "mã ICD-10 chính xác nhất",
  "medical_domain": "chọn từ: TCM, Internal_Medicine, Surgery, Pediatrics, OBGYN, Oncology, Mixed",
  "document_type": "treatment_guideline hoặc diagnostic_guideline hoặc drug_protocol",
  "publisher": "cơ quan ban hành",
  "year": null hoặc năm ban hành (số nguyên),
  "summary": "2-3 câu mô tả nội dung chính"
}}

medical_domain: TCM = Y học cổ truyền, Internal_Medicine = Nội khoa, Surgery = Ngoại khoa, Pediatrics = Nhi khoa, OBGYN = Sản phụ khoa, Oncology = Ung bướu.
icd_code: mã ICD-10 chính xác nhất cho bệnh chủ đề.

Heuristic đã phát hiện: quality={quality}, structure={json.dumps(structure, ensure_ascii=False)}
Tổng trang: {total_pages}

Văn bản mẫu:
{sample_text[:5000]}"""

        for attempt in range(3):
            try:
                response = self.client.chat.completions.create(
                    model=self.model,
                    messages=[
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_prompt},
                    ],
                    response_format={"type": "json_object"},
                    temperature=0.0,
                )
                data = json.loads(response.choices[0].message.content)

                publisher = data.get("publisher", "")

                # Auto-detect hospital source from publisher name
                source_type = "BYT"
                hospital_name = None
                byt_keywords = ["bộ y tế", "ministry of health", "byt"]
                publisher_lower = publisher.lower()
                if publisher and not any(k in publisher_lower for k in byt_keywords):
                    # Publisher is not BYT → likely a hospital
                    hospital_keywords = ["bệnh viện", "bv ", "viện", "trung tâm", "hospital", "vinmec", "bạch mai", "chợ rẫy"]
                    if any(k in publisher_lower for k in hospital_keywords):
                        source_type = "hospital"
                        hospital_name = publisher

                profile = DocumentProfile(
                    disease_name=data.get("disease_name", "Unknown"),
                    disease_aliases=data.get("disease_aliases", []),
                    icd_code=data.get("icd_code", ""),
                    medical_domain=data.get("medical_domain", "Mixed"),
                    document_type=data.get("document_type", "treatment_guideline"),
                    pdf_quality=quality,
                    has_tables=structure["has_tables"],
                    has_flowcharts=structure["has_flowcharts"],
                    has_appendices=structure["has_appendices"],
                    heading_style=structure["heading_style"],
                    estimated_pages=total_pages,
                    publisher=publisher,
                    year=data.get("year"),
                    summary=data.get("summary", ""),
                    source_type=source_type,
                    hospital_name=hospital_name,
                )
                print(f"  => Detected: \"{profile.disease_name}\" ({profile.icd_code}), {profile.medical_domain}, source={source_type}" +
                      (f", hospital={hospital_name}" if hospital_name else ""))
                return profile
            except Exception as e:
                print(f"  [WARN] Phase 1 attempt {attempt + 1} failed: {e}")
                if attempt == 2:
                    raise RuntimeError(f"Phase 1 failed after 3 attempts: {e}")


# ---------------------------------------------------------------------------
# Phase 2: PipelineConfigurator
# ---------------------------------------------------------------------------

class PipelineConfigurator:
    """Generate IngestConfig from DocumentProfile using 3 LLM calls + heuristics."""

    def __init__(self, client: AzureOpenAI, model: str):
        self.client = client
        self.model = model

    def _llm_call(self, system: str, user: str, retry_errors: list[str] | None = None) -> dict:
        """Make an LLM call with JSON response format. Retry up to 2x on parse failure."""
        messages = [
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ]
        if retry_errors:
            messages.append({
                "role": "user",
                "content": f"Previous attempts failed with these errors: {'; '.join(retry_errors)}. Fix and return valid JSON."
            })

        response = self.client.chat.completions.create(
            model=self.model,
            messages=messages,
            response_format={"type": "json_object"},
            temperature=0.0,
        )
        return json.loads(response.choices[0].message.content)

    def generate_entity_config(self, profile: DocumentProfile) -> list[EntityTypeConfig]:
        """LLM call 1: Generate entity types + examples for the disease domain."""
        system = "You are a medical NLP system designer. Generate entity type configurations for NER extraction."
        user = f"""Document: {profile.disease_name} (ICD: {profile.icd_code}), Domain: {profile.medical_domain}
Summary: {profile.summary}

Generate entity types to extract from this medical document. For each type provide:
- label: entity type name (e.g. "Drug", "LabTest")
- examples: 3-8 example entity names likely found in this document

Rules:
- ALWAYS include: Drug, Disease, Symptom
- If domain is TCM: add Herb, Formula, Syndrome, AcupuncturePoint
- If domain is Surgery: add Procedure, Instrument, Complication
- If domain is Oncology: add Stage, Biomarker, Procedure
- Always consider: LabTest, Stage, Procedure as relevant
- Max 10 types total
- Use standard labels: {', '.join(sorted(ALLOWED_LABELS))}

Return JSON: {{"entity_types": [{{"label": "...", "examples": ["...", "..."]}}]}}"""

        errors = []
        for attempt in range(3):
            try:
                data = self._llm_call(system, user, errors if errors else None)
                types = [EntityTypeConfig(**et) for et in data.get("entity_types", [])]
                # Validate labels
                types = [t for t in types if t.label in ALLOWED_LABELS]
                if not types:
                    raise ValueError("No valid entity types generated")
                return types
            except Exception as e:
                errors.append(str(e))
                if attempt == 2:
                    print(f"  [WARN] Entity config failed, using defaults")
                    return self._default_entity_types(profile)

    def _default_entity_types(self, profile: DocumentProfile) -> list[EntityTypeConfig]:
        """Fallback entity types."""
        types = [
            EntityTypeConfig(label="Drug", examples=["Aspirin", "Paracetamol"]),
            EntityTypeConfig(label="Disease", examples=[profile.disease_name]),
            EntityTypeConfig(label="Symptom", examples=["Đau đầu", "Sốt"]),
            EntityTypeConfig(label="LabTest", examples=["Xét nghiệm máu"]),
            EntityTypeConfig(label="Procedure", examples=["Siêu âm"]),
            EntityTypeConfig(label="Stage", examples=["Giai đoạn I"]),
        ]
        if profile.medical_domain == "TCM":
            types.extend([
                EntityTypeConfig(label="Herb", examples=["Hoàng kỳ"]),
                EntityTypeConfig(label="Formula", examples=["Bài thuốc"]),
                EntityTypeConfig(label="Syndrome", examples=["Thận âm hư"]),
                EntityTypeConfig(label="AcupuncturePoint", examples=["Hợp cốc"]),
            ])
        return types

    def generate_ontology(self, profile: DocumentProfile, entity_types: list[EntityTypeConfig]
                          ) -> tuple[list[OntologyNode], list[OntologyRelationship]]:
        """LLM call 2: Generate ontology seed nodes and relationships."""
        system = "You are a medical ontology designer. Generate seed nodes for a Neo4j knowledge graph."
        user = f"""Generate ontology seed nodes for: {profile.disease_name} (ICD: {profile.icd_code})
Domain: {profile.medical_domain}
Entity types available: {[t.label for t in entity_types]}

Generate:
1. ICD hierarchy: ICD_Chapter → ICD_Category (with codes)
2. Key drugs commonly used (with ATC codes if known) — 3-8 drugs
3. Key lab tests — 3-10 tests
4. Staging/classification systems if applicable
5. The Disease node itself with aliases

Only use labels from: {', '.join(sorted(ALLOWED_LABELS))}

Return JSON:
{{
  "nodes": [{{"label": "...", "name": "...", "properties": {{"code": "..."}}}}, ...],
  "relationships": [{{"from_label": "...", "from_name": "...", "to_label": "...", "to_name": "...", "rel_type": "..."}}, ...]
}}

rel_type options: CLASSIFIED_AS, HAS_CATEGORY, HAS_SUBCATEGORY, INSTANCE_OF"""

        errors = []
        for attempt in range(3):
            try:
                data = self._llm_call(system, user, errors if errors else None)
                nodes = []
                for n in data.get("nodes", []):
                    if n.get("label") in ALLOWED_LABELS:
                        nodes.append(OntologyNode(**n))
                rels = []
                for r in data.get("relationships", []):
                    if r.get("from_label") in ALLOWED_LABELS and r.get("to_label") in ALLOWED_LABELS:
                        rels.append(OntologyRelationship(**r))
                return nodes, rels
            except Exception as e:
                errors.append(str(e))
                if attempt == 2:
                    print(f"  [WARN] Ontology generation failed, using minimal ontology")
                    return self._default_ontology(profile)

    def _default_ontology(self, profile: DocumentProfile) -> tuple[list[OntologyNode], list[OntologyRelationship]]:
        """Minimal fallback ontology — just the disease node."""
        nodes = [
            OntologyNode(label="Disease", name=profile.disease_name,
                         properties={"icd_code": profile.icd_code}),
        ]
        return nodes, []

    def generate_extraction_prompt(self, profile: DocumentProfile, entity_types: list[EntityTypeConfig]
                                   ) -> tuple[str, str]:
        """LLM call 3: Generate extraction system prompt + user template."""
        entity_desc = "\n".join(
            f"- {et.label}: {', '.join(et.examples)}" for et in entity_types
        )
        system = "You are a prompt engineer specializing in medical NER systems."
        user = f"""Generate an NER extraction prompt for Vietnamese medical text about {profile.disease_name}.
Domain: {profile.medical_domain}

Entity types and examples:
{entity_desc}

Return JSON:
{{
  "system_prompt": "System prompt for the NER model (in English). Must instruct to return valid JSON with entities array.",
  "user_prompt_template": "User prompt template. MUST contain {{chunk_content}} and {{section_path}} placeholders. Should list all entity types with examples."
}}

The user_prompt_template must:
1. Include the entity types and examples
2. Have {{chunk_content}} placeholder for the text to analyze
3. Have {{section_path}} placeholder for section context
4. Instruct to return JSON: {{"entities": [{{"name": "...", "type": "..."}}]}}
5. Say "Only include entities EXPLICITLY mentioned in the text"
"""

        errors = []
        for attempt in range(3):
            try:
                data = self._llm_call(system, user, errors if errors else None)
                sys_prompt = data.get("system_prompt", "")
                usr_template = data.get("user_prompt_template", "")
                # Validate placeholders exist
                if "{chunk_content}" not in usr_template or "{section_path}" not in usr_template:
                    raise ValueError("user_prompt_template missing required placeholders")
                return sys_prompt, usr_template
            except Exception as e:
                errors.append(str(e))
                if attempt == 2:
                    print(f"  [WARN] Prompt generation failed, using default prompts")
                    return self._default_extraction_prompts(profile, entity_types)

    def _default_extraction_prompts(self, profile: DocumentProfile, entity_types: list[EntityTypeConfig]
                                    ) -> tuple[str, str]:
        """Fallback extraction prompts."""
        entity_desc = "\n".join(
            f"- {et.label}: {', '.join(et.examples)}" for et in entity_types
        )
        sys_prompt = "You are a medical NER system. Extract entities precisely from Vietnamese clinical text. Return valid JSON only."
        usr_template = f"""Analyze the following Vietnamese medical text about {profile.disease_name}.
Extract all medical entities found in the text.

Entity types and examples:
{entity_desc}

Section context: {{section_path}}

Text:
{{chunk_content}}

Return JSON: {{"entities": [{{"name": "...", "type": "..."}}]}}
Only include entities that are EXPLICITLY mentioned in the text. Do not infer or guess."""
        return sys_prompt, usr_template

    # Domain-specific chunking profiles — data-driven, not hardcoded per-disease
    DOMAIN_CHUNK_PROFILES: dict[str, dict[str, Any]] = {
        "TCM": {"max_chunk_size": 4000, "min_chunk_size": 150},           # formulas can be long
        "Oncology": {"max_chunk_size": 3500, "min_chunk_size": 200},      # staging tables, regimens
        "Surgery": {"max_chunk_size": 3000, "min_chunk_size": 150},       # procedure steps
        "Internal_Medicine": {"max_chunk_size": 3000, "min_chunk_size": 100},
        "Pediatrics": {"max_chunk_size": 2500, "min_chunk_size": 100},    # shorter protocols
        "OBGYN": {"max_chunk_size": 3000, "min_chunk_size": 100},
        "Mixed": {"max_chunk_size": 3000, "min_chunk_size": 100},
    }

    def generate_chunking_hints(self, profile: DocumentProfile) -> dict:
        """Deterministic (no LLM): domain → chunking params from profile table."""
        domain_profile = self.DOMAIN_CHUNK_PROFILES.get(
            profile.medical_domain,
            self.DOMAIN_CHUNK_PROFILES["Mixed"]
        )

        hints: dict[str, Any] = {
            "max_chunk_size": domain_profile["max_chunk_size"],
            "min_chunk_size": domain_profile["min_chunk_size"],
            "skip_first_page": True,
            "extra_heading_patterns": [],
            "needs_ocr_cleanup": True,
        }

        # Large documents (>50 pages) benefit from larger chunks to preserve context
        if profile.estimated_pages > 50:
            hints["max_chunk_size"] = min(hints["max_chunk_size"] + 500, 5000)

        if profile.has_tables:
            hints["extra_heading_patterns"].append(r"^Bảng\s+\d+")

        if profile.has_flowcharts:
            hints["extra_heading_patterns"].append(r"^[Ll]ưu đồ\s+\d+")

        return hints

    def configure(self, profile: DocumentProfile) -> IngestConfig:
        """Orchestrate all steps → IngestConfig."""
        print("[Phase 2] Configuring pipeline...")

        # LLM Call 1: Entity types
        print("  [2.1] Generating entity types...")
        entity_types = self.generate_entity_config(profile)
        print(f"    =>{len(entity_types)} types: {[t.label for t in entity_types]}")

        # LLM Call 2: Ontology
        print("  [2.2] Generating ontology...")
        ontology_nodes, ontology_rels = self.generate_ontology(profile, entity_types)
        print(f"    =>{len(ontology_nodes)} nodes, {len(ontology_rels)} relationships")

        # LLM Call 3: Extraction prompts
        print("  [2.3] Generating extraction prompts...")
        sys_prompt, usr_template = self.generate_extraction_prompt(profile, entity_types)
        print(f"    =>System prompt: {len(sys_prompt)} chars, User template: {len(usr_template)} chars")

        # Heuristic: Chunking params
        chunking = self.generate_chunking_hints(profile)

        # Build protocol name — distinguish source
        year_str = f" {profile.year}" if profile.year else ""
        if profile.source_type == "hospital" and profile.hospital_name:
            protocol_name = f"Phác đồ {profile.disease_name} - BV {profile.hospital_name}{year_str}"
        else:
            publisher_str = f" - {profile.publisher}" if profile.publisher else ""
            protocol_name = f"Phác đồ {profile.disease_name}{publisher_str}{year_str}"

        config = IngestConfig(
            disease_name=profile.disease_name,
            icd_code=profile.icd_code,
            protocol_name=protocol_name,
            max_chunk_size=chunking["max_chunk_size"],
            min_chunk_size=chunking["min_chunk_size"],
            skip_first_page=chunking["skip_first_page"],
            extra_heading_patterns=chunking["extra_heading_patterns"],
            entity_types=entity_types,
            extraction_system_prompt=sys_prompt,
            extraction_user_prompt_template=usr_template,
            ontology_nodes=ontology_nodes,
            ontology_relationships=ontology_rels,
            needs_ocr_cleanup=chunking["needs_ocr_cleanup"],
            source_type=profile.source_type,
            hospital_name=profile.hospital_name,
        )

        print(f"  =>Config ready for \"{config.disease_name}\"")
        return config


# ---------------------------------------------------------------------------
# Phase 3: UniversalIngest
# ---------------------------------------------------------------------------

def _slugify(text: str) -> str:
    """Convert Vietnamese text to URL-safe slug for chunk IDs."""
    text = unicodedata.normalize("NFD", text)
    text = re.sub(r'[\u0300-\u036f]', '', text)  # strip diacritics
    text = text.lower().replace("đ", "d").replace("Đ", "D")
    text = re.sub(r'[^a-z0-9]+', '_', text)
    return text.strip('_')


class UniversalIngest:
    """Config-driven ingest engine. Reuses V2 components, parameterized by IngestConfig."""

    def __init__(self):
        self.client = AzureOpenAI(
            api_key=os.getenv("AZURE_OPENAI_API_KEY"),
            api_version=os.getenv("AZURE_OPENAI_API_VERSION"),
            azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT"),
        )
        self.model = os.getenv("MODEL1", "gpt-4o-mini")

        self.embedding_client = AzureOpenAI(
            azure_endpoint=os.getenv("AZURE_EMBEDDINGS_ENDPOINT").strip(),
            api_key=os.getenv("AZURE_EMBEDDINGS_API_KEY").strip(),
            api_version=os.getenv("AZURE_OPENAI_API_VERSION").strip(),
        )
        self.embedding_model = "text-embedding-ada-002"

        uri = os.getenv("NEO4J_URI", os.getenv("neo4j_uri", "bolt://localhost:7688"))
        user = os.getenv("NEO4J_USER", os.getenv("neo4j_user", "neo4j"))
        password = os.getenv("NEO4J_PASSWORD", os.getenv("neo4j_password", "password123"))
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    # -- Text extraction (vision-aware) --

    def extract_text(self, pdf_path: str, force_vision: bool = False) -> str:
        """Extract text from PDF with automatic vision fallback for garbled pages.

        Uses PDFVisionReader for quality-aware extraction:
          1. PyMuPDF extracts all pages (fast, free)
          2. Quality assessment detects garbled Vietnamese fonts (Ƣ, missing diacritics)
          3. Low-quality pages → re-extracted via GPT vision OCR
          4. Returns merged text with page markers

        Args:
            pdf_path: Path to PDF file.
            force_vision: If True, use vision for ALL pages (slow but highest quality).
        """
        from server_support.pdf_vision_reader import PDFVisionReader

        reader = PDFVisionReader(
            quality_threshold=0.5,
            vision_dpi=150,
            max_vision_pages=80,
        )
        result = reader.extract(pdf_path, force_vision=force_vision)

        if result.errors:
            for err in result.errors:
                print(f"  [WARN] Vision reader: {err}")

        vision_count = len(result.vision_pages)
        if vision_count > 0:
            print(f"  Vision OCR used for {vision_count}/{result.total_pages} pages "
                  f"(garbled font detected): {result.vision_pages[:10]}{'...' if vision_count > 10 else ''}")

        # Convert to page-marker format expected by chunker
        full_text = []
        for p in result.pages:
            full_text.append(f"--- Page {p.page_num} ---\n{p.text}")
        return "\n".join(full_text)

    def extract_text_basic(self, pdf_path: str) -> str:
        """Fast PyMuPDF-only extraction (no vision). Use when quality is known good."""
        doc = fitz.open(pdf_path)
        full_text = []
        for page_num, page in enumerate(doc):
            blocks = page.get_text("blocks")
            blocks.sort(key=lambda b: (b[1], b[0]))
            page_text = f"--- Page {page_num + 1} ---\n"
            for b in blocks:
                page_text += b[4] + "\n"
            full_text.append(page_text)
        doc.close()
        return "\n".join(full_text)

    # -- Embedding (reused from V2) --

    def get_embedding(self, text: str) -> list[float] | None:
        text = text.replace("\n", " ")[:8000]
        try:
            return self.embedding_client.embeddings.create(
                input=[text], model=self.embedding_model
            ).data[0].embedding
        except Exception as e:
            print(f"  [WARN] Embedding error: {e}")
            return None

    # -- Indexes (reused from V2) --

    def create_indexes(self):
        """Create vector and fulltext indexes for :Chunk nodes."""
        with self.driver.session() as session:
            session.run("""
                CREATE VECTOR INDEX `chunk_vector_index` IF NOT EXISTS
                FOR (n:Chunk) ON (n.embedding)
                OPTIONS {indexConfig: {
                    `vector.dimensions`: 1536,
                    `vector.similarity_function`: 'cosine'
                }}
            """)
            print("[OK] Vector index 'chunk_vector_index' created or exists.")

            session.run("""
                CREATE FULLTEXT INDEX `chunk_fulltext` IF NOT EXISTS
                FOR (n:Chunk) ON EACH [n.title, n.content]
            """)
            print("[OK] Fulltext index 'chunk_fulltext' created or exists.")

            # Hospital-specific indexes
            session.run(
                "CREATE INDEX chunk_source_idx IF NOT EXISTS "
                "FOR (n:Chunk) ON (n.disease_name, n.source_type)"
            )
            session.run(
                "CREATE INDEX chunk_hospital_idx IF NOT EXISTS "
                "FOR (n:Chunk) ON (n.disease_name, n.hospital_name)"
            )
            session.run(
                "CREATE INDEX hospital_name_idx IF NOT EXISTS "
                "FOR (n:Hospital) ON (n.name)"
            )
            print("[OK] Hospital-specific indexes created or exist.")

    # -- Config-driven methods --

    def build_chunker(self, config: IngestConfig) -> SemanticChunker:
        """Create a SemanticChunker with config-driven parameters."""
        chunker = SemanticChunker()
        chunker.MAX_CHUNK = config.max_chunk_size
        chunker.MIN_CHUNK = config.min_chunk_size

        # Add extra heading patterns from config
        for pattern_str in config.extra_heading_patterns:
            try:
                compiled = re.compile(pattern_str, re.IGNORECASE)
                chunker.HEADING_PATTERNS.append((compiled, 'section'))
            except re.error as e:
                print(f"  [WARN] Invalid heading pattern '{pattern_str}': {e}")

        return chunker

    def setup_ontology(self, config: IngestConfig):
        """MERGE ontology seed nodes and relationships from config."""
        if not config.ontology_nodes and not config.ontology_relationships:
            print("  [SKIP] No ontology nodes configured.")
            return

        with self.driver.session() as session:
            for node in config.ontology_nodes:
                if node.label not in ALLOWED_LABELS:
                    print(f"  [SKIP] Disallowed label: {node.label}")
                    continue
                # Build properties string safely using parameters
                props = {"name": node.name}
                props.update(node.properties)
                prop_keys = list(props.keys())
                prop_str = ", ".join(f"{k}: ${k}" for k in prop_keys)
                query = f"MERGE (n:{node.label} {{{prop_str}}})"
                session.run(query, **props)

            for rel in config.ontology_relationships:
                if rel.from_label not in ALLOWED_LABELS or rel.to_label not in ALLOWED_LABELS:
                    continue
                # Validate rel_type (alphanumeric + underscore only)
                if not re.match(r'^[A-Z_]+$', rel.rel_type):
                    print(f"  [SKIP] Invalid rel_type: {rel.rel_type}")
                    continue
                query = f"""
                    MATCH (a:{rel.from_label} {{name: $from_name}})
                    MATCH (b:{rel.to_label} {{name: $to_name}})
                    MERGE (a)-[:{rel.rel_type}]->(b)
                """
                session.run(query, from_name=rel.from_name, to_name=rel.to_name)

            print(f"[OK] Ontology: {len(config.ontology_nodes)} nodes, {len(config.ontology_relationships)} relationships.")

    # Allowed semantic relationship types for entity-to-entity links
    ALLOWED_RELATION_TYPES = {"INDICATION_FOR", "CONTRA_INDICATES", "DOSE_OF", "RULE_OUT_FOR"}

    def extract_entities(self, chunk_content: str, section_path: str, config: IngestConfig) -> dict:
        """Use config-driven prompts for entity + relation extraction.

        Returns dict with 'entities' list and 'relations' list.
        """
        # Use replace instead of .format() to avoid KeyError on literal braces in LLM-generated prompts
        user_prompt = config.extraction_user_prompt_template
        user_prompt = user_prompt.replace("{chunk_content}", chunk_content)
        user_prompt = user_prompt.replace("{section_path}", section_path)

        # Append relation extraction instruction to user prompt
        relation_instruction = (
            "\n\nAlso extract relationships between entities if EXPLICITLY stated:\n"
            "- INDICATION_FOR: Drug/Procedure indicated for Disease/Stage\n"
            "- CONTRA_INDICATES: Drug/Procedure contraindicated\n"
            "- DOSE_OF: specific dosage for a Drug\n"
            "- RULE_OUT_FOR: LabTest/Procedure used to rule out Disease\n\n"
            'Add to your JSON: "relations": [{"from_name": "...", "to_name": "...", '
            '"rel_type": "INDICATION_FOR|CONTRA_INDICATES|DOSE_OF|RULE_OUT_FOR", "detail": "context"}]'
        )
        user_prompt += relation_instruction

        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": config.extraction_system_prompt},
                    {"role": "user", "content": user_prompt},
                ],
                response_format={"type": "json_object"},
                temperature=0.0,
            )
            data = json.loads(response.choices[0].message.content)
            entities = data.get("entities", [])
            relations = data.get("relations", [])
            # Filter to allowed labels only
            valid_labels = {et.label for et in config.entity_types}
            entities = [e for e in entities if e.get("type") in valid_labels]
            relations = [r for r in relations if r.get("rel_type") in self.ALLOWED_RELATION_TYPES]
            return {"entities": entities, "relations": relations}
        except Exception as e:
            print(f"  [WARN] Entity extraction failed: {e}")
            return {"entities": [], "relations": []}

    def merge_entity(self, session, entity: dict, config: IngestConfig) -> str | None:
        """MERGE a typed entity node using config-driven label map."""
        name = entity.get("name", "").strip()
        etype = entity.get("type", "").strip()
        if not name or not etype:
            return None

        # Build label map from config entity types
        valid_labels = {et.label for et in config.entity_types}
        if etype not in valid_labels or etype not in ALLOWED_LABELS:
            return None

        session.run(f"MERGE (n:{etype} {{name: $name}})", name=name)
        return etype

    def ingest_chunk(self, session, chunk: dict, config: IngestConfig,
                     entities: list[dict], embedding: list[float] | None,
                     relations: list[dict] | None = None):
        """Create a :Chunk node and link to Disease, Protocol, entities, and semantic relations."""
        # Create Chunk
        session.run("""
            CREATE (c:Chunk {
                chunk_id: $chunk_id,
                content: $content,
                title: $title,
                level: $level,
                section_path: $section_path,
                disease_name: $disease_name,
                page_number: $page_number,
                parent_context: $parent_context,
                embedding: $embedding,
                source_type: $source_type,
                hospital_name: $hospital_name
            })
        """, chunk_id=chunk['chunk_id'], content=chunk['content'],
             title=chunk['title'], level=chunk['level'],
             section_path=chunk['section_path'], disease_name=config.disease_name,
             page_number=chunk['page_number'],
             parent_context=chunk.get('parent_context', ''),
             embedding=embedding,
             source_type=config.source_type,
             hospital_name=config.hospital_name)

        # Link to Disease
        session.run("""
            MATCH (c:Chunk {chunk_id: $chunk_id})
            MATCH (d:Disease {name: $disease_name})
            MERGE (c)-[:ABOUT_DISEASE]->(d)
        """, chunk_id=chunk['chunk_id'], disease_name=config.disease_name)

        # Link to Protocol
        session.run("""
            MATCH (c:Chunk {chunk_id: $chunk_id})
            MATCH (p:Protocol {name: $protocol_name})
            MERGE (p)-[:HAS_BLOCK]->(c)
        """, chunk_id=chunk['chunk_id'], protocol_name=config.protocol_name)

        # Link to entities via :MENTIONS
        entity_labels = {}
        for ent in entities:
            label = self.merge_entity(session, ent, config)
            if label:
                entity_labels[ent['name'].strip()] = label
                session.run(f"""
                    MATCH (c:Chunk {{chunk_id: $chunk_id}})
                    MATCH (e:{label} {{name: $ename}})
                    MERGE (c)-[:MENTIONS]->(e)
                """, chunk_id=chunk['chunk_id'], ename=ent['name'].strip())

        # Create semantic entity-to-entity relations (INDICATION_FOR, CONTRA_INDICATES, etc.)
        if relations:
            for rel in relations:
                rel_type = rel.get("rel_type", "")
                if rel_type not in self.ALLOWED_RELATION_TYPES:
                    continue
                from_name = rel.get("from_name", "").strip()
                to_name = rel.get("to_name", "").strip()
                if not from_name or not to_name:
                    continue
                from_label = entity_labels.get(from_name)
                to_label = entity_labels.get(to_name)
                if not from_label or not to_label:
                    continue
                detail = rel.get("detail", "")
                session.run(f"""
                    MATCH (a:{from_label} {{name: $fname}})
                    MATCH (b:{to_label} {{name: $tname}})
                    MERGE (a)-[r:{rel_type}]->(b)
                    ON CREATE SET r.detail = $detail, r.source_chunk = $cid
                """, fname=from_name, tname=to_name,
                     detail=detail, cid=chunk['chunk_id'])

    def build_hierarchy(self, session, chunks: list[dict]):
        """Create NEXT_CHUNK + HAS_CHILD relationships (reused from V2)."""
        for i in range(len(chunks) - 1):
            session.run("""
                MATCH (a:Chunk {chunk_id: $a_id})
                MATCH (b:Chunk {chunk_id: $b_id})
                MERGE (a)-[:NEXT_CHUNK]->(b)
            """, a_id=chunks[i]['chunk_id'], b_id=chunks[i + 1]['chunk_id'])

        for i, parent in enumerate(chunks):
            if parent['level'] != 'section':
                continue
            parent_path = parent['section_path']
            for j in range(i + 1, len(chunks)):
                child = chunks[j]
                if child['level'] == 'section' and child['section_path'] != parent_path:
                    break
                if child['section_path'].startswith(parent_path + ' > '):
                    session.run("""
                        MATCH (p:Chunk {chunk_id: $pid})
                        MATCH (c:Chunk {chunk_id: $cid})
                        MERGE (p)-[:HAS_CHILD]->(c)
                    """, pid=parent['chunk_id'], cid=child['chunk_id'])

        print("[OK] Hierarchy links (NEXT_CHUNK + HAS_CHILD) created.")

    def _filter_pages(self, text: str, skip_pages: list[int]) -> str:
        """Remove content between Page markers for specified page numbers."""
        if not skip_pages:
            return text
        lines = text.split('\n')
        filtered = []
        skip_current = False
        for line in lines:
            page_match = re.match(r'^---\s*Page\s+(\d+)\s*---$', line.strip())
            if page_match:
                page_num = int(page_match.group(1))
                skip_current = page_num in skip_pages
            if not skip_current:
                filtered.append(line)
        return '\n'.join(filtered)

    def run(self, pdf_path: str, config: IngestConfig,
            pre_extracted_text: str | None = None,
            force_vision: bool = False):
        """Full pipeline: extract → clean → chunk → index → ontology → ingest → hierarchy → stats.

        Args:
            pre_extracted_text: If provided, skip PDF extraction and use this text directly.
                Used by MultiDiseaseIngest to avoid re-reading the PDF for each disease.
            force_vision: If True, use vision OCR for all PDF pages (for garbled font PDFs).
        """
        slug = _slugify(config.disease_name)

        print(f"\n{'=' * 60}")
        print(f"Universal Ingest: {config.disease_name} (ICD: {config.icd_code})")
        print(f"PDF: {pdf_path}")
        print(f"{'=' * 60}")

        # 1. Extract text
        if pre_extracted_text:
            print("\n[1/7] Using pre-extracted text...")
            raw_text = pre_extracted_text
        else:
            print(f"\n[1/7] Extracting text from PDF{'  (force_vision=True)' if force_vision else ' (vision-aware)'}...")
            raw_text = self.extract_text(pdf_path, force_vision=force_vision)
        print(f"  Raw text length: {len(raw_text)} chars")

        # 2. Clean OCR
        print("[2/7] Cleaning OCR text...")
        if config.needs_ocr_cleanup:
            cleaned_text = clean_ocr_text(raw_text)
        else:
            cleaned_text = raw_text
        print(f"  Cleaned text length: {len(cleaned_text)} chars")

        # Filter pages if configured
        if config.skip_pages:
            cleaned_text = self._filter_pages(cleaned_text, config.skip_pages)
            print(f"  After page filtering: {len(cleaned_text)} chars (skipped pages {config.skip_pages})")

        # 3. Semantic chunking
        print("[3/7] Semantic chunking...")
        chunker = self.build_chunker(config)
        chunks = chunker.chunk(cleaned_text, skip_first_page=config.skip_first_page)

        # Namespace chunk IDs to avoid collision between diseases
        for chunk in chunks:
            chunk['chunk_id'] = f"{slug}_{chunk['chunk_id']}"

        print(f"  Produced {len(chunks)} chunks")
        for c in chunks[:5]:
            print(f"    - [{c['chunk_id']}] {c['title'][:60]}  ({len(c['content'])} chars, p.{c['page_number']})")
        if len(chunks) > 5:
            print(f"    ... and {len(chunks) - 5} more")

        # 4. Create indexes
        print("\n[4/7] Creating Neo4j indexes...")
        self.create_indexes()

        # 5. Setup ontology
        print("[5/7] Setting up ontology...")
        self.setup_ontology(config)

        # 6. Merge Disease + Protocol + Hospital
        with self.driver.session() as session:
            session.run(
                "MERGE (d:Disease {name: $name}) ON CREATE SET d.icd_code = $icd",
                name=config.disease_name, icd=config.icd_code,
            )
            session.run("MERGE (p:Protocol {name: $name})", name=config.protocol_name)

            # Set source metadata on Protocol
            session.run(
                "MATCH (p:Protocol {name: $name}) "
                "SET p.source_type = $st, p.hospital_name = $hn",
                name=config.protocol_name,
                st=config.source_type,
                hn=config.hospital_name,
            )

            # If hospital → create :Hospital node + :FROM_HOSPITAL relationship
            if config.source_type == "hospital" and config.hospital_name:
                session.run(
                    "MERGE (h:Hospital {name: $hname})",
                    hname=config.hospital_name,
                )
                session.run(
                    "MATCH (p:Protocol {name: $pn}) "
                    "MATCH (h:Hospital {name: $hn}) "
                    "MERGE (p)-[:FROM_HOSPITAL]->(h)",
                    pn=config.protocol_name,
                    hn=config.hospital_name,
                )

        # 7. Ingest chunks with entities and embeddings (PARALLEL)
        max_workers = min(20, len(chunks))
        print(f"\n[6/7] Ingesting {len(chunks)} chunks with {max_workers} parallel workers...")
        entity_stats: dict[str, int] = {}
        import threading
        _stats_lock = threading.Lock()
        _print_lock = threading.Lock()

        def _process_one_chunk(idx_chunk):
            """Process a single chunk: extract entities + relations + embed + ingest."""
            i, chunk = idx_chunk
            try:
                # Extract entities + relations via LLM
                extraction = self.extract_entities(chunk['content'], chunk['section_path'], config)
                entities = extraction["entities"]
                relations = extraction["relations"]
                ent_names = [e['name'] for e in entities]

                # Generate embedding
                embed_text = f"{chunk['title']}\n{chunk.get('parent_context', '')}\n{chunk['content']}"
                embedding = self.get_embedding(embed_text)

                # Ingest to Neo4j (each worker gets its own session)
                with self.driver.session() as worker_session:
                    self.ingest_chunk(worker_session, chunk, config, entities, embedding, relations)

                # Update stats thread-safe
                with _stats_lock:
                    for e in entities:
                        etype = e.get("type", "Unknown")
                        entity_stats[etype] = entity_stats.get(etype, 0) + 1

                with _print_lock:
                    status = "OK" if embedding else "emb FAIL"
                    print(f"  [{i+1}/{len(chunks)}] {chunk['title'][:45]}... "
                          f"[{len(entities)} ent, {len(relations)} rel] [{status}]")
                return True
            except Exception as exc:
                with _print_lock:
                    print(f"  [{i+1}/{len(chunks)}] ERROR: {exc}")
                return False

        # Run parallel
        success = 0
        failed = 0
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(_process_one_chunk, (i, c)): i
                       for i, c in enumerate(chunks)}
            for future in as_completed(futures):
                if future.result():
                    success += 1
                else:
                    failed += 1
        print(f"  Parallel ingest done: {success} OK, {failed} failed")

        # Build hierarchy (must be sequential after all chunks ingested)
        with self.driver.session() as session:
            print("\n[7/7] Building hierarchy links...")
            self.build_hierarchy(session, chunks)

        # Stats
        print(f"\n{'=' * 60}")
        print("INGESTION COMPLETE — Stats:")
        with self.driver.session() as session:
            chunk_count = session.run(
                "MATCH (c:Chunk {disease_name: $d}) RETURN count(c) as cnt",
                d=config.disease_name,
            ).single()["cnt"]
            rel_count = session.run(
                "MATCH (c:Chunk)-[:ABOUT_DISEASE]->(d:Disease {name: $d}) RETURN count(c) as cnt",
                d=config.disease_name,
            ).single()["cnt"]
            mentions = session.run(
                "MATCH (c:Chunk {disease_name: $d})-[:MENTIONS]->(e) RETURN count(e) as cnt",
                d=config.disease_name,
            ).single()["cnt"]

        print(f"  Disease:         {config.disease_name}")
        print(f"  Chunks:          {chunk_count}")
        print(f"  ABOUT_DISEASE:   {rel_count}")
        print(f"  MENTIONS rels:   {mentions}")
        print(f"  Entity breakdown: {json.dumps(entity_stats, ensure_ascii=False)}")
        print(f"{'=' * 60}")

        return {
            "disease_name": config.disease_name,
            "chunks": chunk_count,
            "entities": entity_stats,
            "mentions": mentions,
            "status": "success",
        }

    @classmethod
    def auto_ingest(cls, pdf_path: str) -> dict:
        """ONE-CALL entry point: analyze → configure → ingest."""
        print(f"\n{'#' * 60}")
        print(f"  UNIVERSAL AUTO-INGEST")
        print(f"  PDF: {pdf_path}")
        print(f"{'#' * 60}\n")

        # Setup clients (shared for Phase 1 + 2)
        client = AzureOpenAI(
            api_key=os.getenv("AZURE_OPENAI_API_KEY"),
            api_version=os.getenv("AZURE_OPENAI_API_VERSION"),
            azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT"),
        )
        model = os.getenv("MODEL1", "gpt-4o-mini")

        # Phase 1: Analyze
        analyzer = DocumentAnalyzer(client, model)
        profile = analyzer.analyze(pdf_path)
        print(f"\n  Profile: {profile.disease_name} ({profile.icd_code}), {profile.medical_domain}\n")

        # Phase 2: Configure
        configurator = PipelineConfigurator(client, model)
        config = configurator.configure(profile)

        # Save config for audit trail
        slug = _slugify(config.disease_name)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        config_dir = Path(__file__).parent / "config" / "ingest_configs"
        config_dir.mkdir(exist_ok=True)
        config_path = config_dir / f"{slug}_{timestamp}.json"
        config_path.write_text(
            config.model_dump_json(indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
        print(f"\n  Config saved: {config_path}\n")

        # Phase 3: Ingest (auto-detect if vision is needed)
        force_vision = profile.pdf_quality == "garbled_font"
        if force_vision:
            print("  [!] Garbled font detected — using full vision OCR")

        ingestor = cls()
        try:
            result = ingestor.run(pdf_path, config, force_vision=force_vision)
        finally:
            ingestor.close()

        return result


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Universal auto-pipeline for medical PDF/text ingestion")
    parser.add_argument("input", help="Path to PDF file or .txt file")
    parser.add_argument("--source-type", choices=["BYT", "hospital"], default=None,
                        help="Source type override (default: auto-detect from publisher)")
    parser.add_argument("--hospital-name", default=None,
                        help="Hospital name (required if --source-type=hospital)")
    parser.add_argument("--force-vision", action="store_true",
                        help="Force vision OCR for all pages (useful for garbled font PDFs)")
    parser.add_argument("--disease-name", default=None,
                        help="Override disease name (required for .txt input)")
    parser.add_argument("--icd-code", default=None,
                        help="Override ICD-10 code")
    args = parser.parse_args()

    input_path = Path(args.input)
    if not input_path.exists():
        print(f"Error: File not found: {input_path}")
        sys.exit(1)

    if args.source_type == "hospital" and not args.hospital_name:
        print("Error: --hospital-name is required when --source-type=hospital")
        sys.exit(1)

    is_text_input = input_path.suffix.lower() in (".txt", ".md")

    try:
        # Setup clients
        client = AzureOpenAI(
            api_key=os.getenv("AZURE_OPENAI_API_KEY"),
            api_version=os.getenv("AZURE_OPENAI_API_VERSION"),
            azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT"),
        )
        model = os.getenv("MODEL1", "gpt-4o-mini")

        if is_text_input:
            # --- Text file input: skip PDF analysis, use LLM to classify from text ---
            print(f"\n  [TEXT MODE] Reading: {input_path.name}")
            text_content = input_path.read_text(encoding="utf-8")
            print(f"  Text: {len(text_content)} chars")

            # Build profile from text content (LLM classifies disease from text sample)
            disease_name = args.disease_name or input_path.stem
            sample = text_content[:2000]

            profile = DocumentProfile(
                disease_name=disease_name,
                disease_aliases=[],
                icd_code=args.icd_code or "",
                medical_domain="Internal_Medicine",
                document_type="treatment_guideline",
                pdf_quality="digital",
                estimated_pages=max(1, len(text_content) // 2000),
                publisher="",
                summary=sample[:500],
                source_type=args.source_type or "BYT",
                hospital_name=args.hospital_name,
            )

            # If no ICD code provided, let LLM classify from text
            if not args.icd_code:
                analyzer = DocumentAnalyzer(client, model)
                try:
                    data = analyzer.classify_text(sample, total_pages=len(text_content) // 2000)
                    if data:
                        profile.icd_code = data.get("icd_code", "")
                        profile.disease_name = args.disease_name or data.get("disease_name", disease_name)
                        profile.disease_aliases = data.get("disease_aliases", [])
                        profile.medical_domain = data.get("medical_domain", "Internal_Medicine")
                except Exception:
                    pass  # Keep defaults

            print(f"  Profile: {profile.disease_name} ({profile.icd_code})")

        else:
            # --- PDF input: full analysis pipeline ---
            analyzer = DocumentAnalyzer(client, model)
            profile = analyzer.analyze(str(input_path))

            # Override from CLI
            if args.disease_name:
                profile.disease_name = args.disease_name
            if args.icd_code:
                profile.icd_code = args.icd_code

        # Override source info from CLI if provided
        if args.source_type:
            profile.source_type = args.source_type
            profile.hospital_name = args.hospital_name

        # Phase 2: Configure
        configurator = PipelineConfigurator(client, model)
        config = configurator.configure(profile)

        # Save config
        slug = _slugify(config.disease_name)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        config_dir = Path(__file__).parent / "config" / "ingest_configs"
        config_dir.mkdir(exist_ok=True)
        config_path = config_dir / f"{slug}_{timestamp}.json"
        config_path.write_text(
            config.model_dump_json(indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
        print(f"\n  Config saved: {config_path}\n")

        # Phase 3: Ingest
        force_vision = args.force_vision or profile.pdf_quality == "garbled_font"
        ingestor = UniversalIngest()
        try:
            if is_text_input:
                config.skip_first_page = False
                result = ingestor.run(str(input_path), config, pre_extracted_text=text_content)
            else:
                result = ingestor.run(str(input_path), config, force_vision=force_vision)
        finally:
            ingestor.close()

        print(f"\nResult: {json.dumps(result, ensure_ascii=False, indent=2)}")
    except Exception:
        traceback.print_exc()
        sys.exit(1)
