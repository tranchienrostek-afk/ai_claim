"""
Ontology V2 Ingest Pipeline — Dual Representation with Provenance.

Extract from Vietnamese medical PDF into 6-layer knowledge graph:
  Raw (RawChunk) → Mention (RawSignMention, RawServiceMention, RawObservationMention)
  → Alias → Concept (canonical) → Assertion (clinical rules) → Summary

Key principles:
  - Ontology-guided extraction (top-down), NOT random graph (bottom-up)
  - Dual representation: raw mention + canonical concept, linked via MAPS_TO
  - Synonyms become graph nodes (Alias), not just text fields
  - Assertions are condition→action rules extracted from protocol text
  - Summaries are acceleration layer, NOT source of truth
  - Full provenance: Decision → Assertion → Section → Chunk → text span

Usage:
    cd notebooklm
    python ontology_v2_ingest.py "path/to/protocol.pdf"
"""

import os
import re
import json
import sys
import io
import unicodedata
import traceback
from datetime import datetime
from pathlib import Path
from typing import Any
from dataclasses import dataclass, field, asdict

from openai import AzureOpenAI
from neo4j import GraphDatabase
from runtime_env import load_notebooklm_env
from v2_ingest import clean_ocr_text, SemanticChunker

PIPELINE_DIR = Path(__file__).resolve().parent / "workspaces" / "claims_insights" / "pipeline"
if str(PIPELINE_DIR) not in sys.path:
    sys.path.insert(0, str(PIPELINE_DIR))
from reasoning_experience_memory import ReasoningExperienceMemory

STANDARDIZE_DIR = Path(__file__).resolve().parent / "workspaces" / "claims_insights" / "02_standardize"
if str(STANDARDIZE_DIR) not in sys.path:
    sys.path.insert(0, str(STANDARDIZE_DIR))
try:
    from service_text_mapper import ServiceTextMapper
except Exception:
    ServiceTextMapper = None

if sys.stdout.encoding != 'utf-8':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
if sys.stderr.encoding != 'utf-8':
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

load_notebooklm_env()


# ---------------------------------------------------------------------------
# Data classes for extracted entities
# ---------------------------------------------------------------------------

@dataclass
class RawChunkData:
    chunk_id: str
    disease_id: str
    section_type: str
    section_title: str
    body_text: str
    page_numbers: list[int] = field(default_factory=list)
    parent_section_path: str = ""

@dataclass
class RawSignMentionData:
    mention_id: str
    mention_text: str
    context_text: str = ""
    modifier_raw: str = ""
    extraction_confidence: float = 0.0
    source_chunk_id: str = ""
    source_page: int = 0

@dataclass
class RawServiceMentionData:
    mention_id: str
    mention_text: str
    context_text: str = ""
    medical_role: str = "unknown"
    condition_to_apply: str = ""
    extraction_confidence: float = 0.0
    source_chunk_id: str = ""
    source_page: int = 0

@dataclass
class RawObservationMentionData:
    mention_id: str
    mention_text: str
    context_text: str = ""
    result_semantics: str = "unknown"
    extraction_confidence: float = 0.0
    source_chunk_id: str = ""
    source_page: int = 0

@dataclass
class ProtocolAssertionData:
    assertion_id: str
    assertion_text: str
    assertion_type: str  # treatment_rule | diagnostic_rule | contraindication | indication | dosage_rule | monitoring_rule
    condition_text: str
    action_text: str
    status: str = "ACTIVE"
    evidence_level: str = "expert_opinion"
    source_chunk_id: str = ""
    source_page: int = 0
    related_signs: list[str] = field(default_factory=list)
    related_services: list[str] = field(default_factory=list)

@dataclass
class DiseaseSummaryData:
    summary_id: str
    disease_id: str
    summary_text: str
    key_signs: list[str] = field(default_factory=list)
    key_services: list[str] = field(default_factory=list)
    key_drugs: list[str] = field(default_factory=list)
    differential_diseases: list[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Section type detection
# ---------------------------------------------------------------------------

SECTION_TYPE_PATTERNS = {
    "trieu_chung": [
        re.compile(r'triệu chứng', re.IGNORECASE),
        re.compile(r'lâm sàng', re.IGNORECASE),
        re.compile(r'biểu hiện', re.IGNORECASE),
        re.compile(r'dấu hiệu', re.IGNORECASE),
        # OCR variants (scanned PDFs produce garbled Vietnamese)
        re.compile(r'tri[~e]u ch[uứ]ng', re.IGNORECASE),
        re.compile(r'l[aâ]m s[aà]ng', re.IGNORECASE),
    ],
    "chan_doan": [
        re.compile(r'chẩn đoán', re.IGNORECASE),
        re.compile(r'phân biệt', re.IGNORECASE),
        re.compile(r'tiêu chuẩn', re.IGNORECASE),
        re.compile(r'ch[~a]n [dđ]o[aá]n', re.IGNORECASE),
        re.compile(r'ph[aâ]n bi[~e]t', re.IGNORECASE),
    ],
    "dieu_tri": [
        re.compile(r'điều trị', re.IGNORECASE),
        re.compile(r'phác đồ', re.IGNORECASE),
        re.compile(r'thuốc', re.IGNORECASE),
        re.compile(r'phẫu thuật', re.IGNORECASE),
        re.compile(r'kháng sinh', re.IGNORECASE),
        re.compile(r'[dđ]i[~ề]u tr[iị\]]', re.IGNORECASE),
        re.compile(r'thu[~ố][cb]', re.IGNORECASE),
        re.compile(r'khang vi rut', re.IGNORECASE),
        re.compile(r'chi [dđ]inh', re.IGNORECASE),
    ],
    "xet_nghiem": [
        re.compile(r'xét nghiệm', re.IGNORECASE),
        re.compile(r'cận lâm sàng', re.IGNORECASE),
        re.compile(r'hình ảnh', re.IGNORECASE),
        re.compile(r'nội soi', re.IGNORECASE),
        re.compile(r'x[eé]t nghi[~e]m', re.IGNORECASE),
        re.compile(r'c[~a]n l[aâ]m', re.IGNORECASE),
        re.compile(r'sang loc', re.IGNORECASE),
        re.compile(r'theo d[oõ]i', re.IGNORECASE),
    ],
    "dai_cuong": [
        re.compile(r'đại cương', re.IGNORECASE),
        re.compile(r'định nghĩa', re.IGNORECASE),
        re.compile(r'nguyên nhân', re.IGNORECASE),
        re.compile(r'dịch tễ', re.IGNORECASE),
        re.compile(r'[dđ][~a]i c[uư][oơ]ng', re.IGNORECASE),
        re.compile(r'D~ICUONG', re.IGNORECASE),
    ],
    "tien_luong": [
        re.compile(r'tiên lượng', re.IGNORECASE),
        re.compile(r'biến chứng', re.IGNORECASE),
        re.compile(r'theo dõi', re.IGNORECASE),
        re.compile(r'bi[~ế]n ch[uứ]ng', re.IGNORECASE),
        re.compile(r'ti[eê]n l[uư][oơ]ng', re.IGNORECASE),
    ],
    "phong_benh": [
        re.compile(r'phòng bệnh', re.IGNORECASE),
        re.compile(r'dự phòng', re.IGNORECASE),
        re.compile(r'phòng ngừa', re.IGNORECASE),
        re.compile(r'ph[oò]ng b[~e]nh', re.IGNORECASE),
        re.compile(r'phong ng[uừ]a', re.IGNORECASE),
    ],
}


def detect_section_type(title: str, body_preview: str = "") -> str:
    """Detect section type from Vietnamese heading text."""
    text = f"{title} {body_preview[:200]}".lower()
    for stype, patterns in SECTION_TYPE_PATTERNS.items():
        for p in patterns:
            if p.search(text):
                return stype
    return "other"


def strip_diacritics(text: str) -> str:
    """Strip Vietnamese diacritics for normalization. Handles đ/Đ correctly."""
    text = str(text or "").replace("đ", "d").replace("Đ", "D")
    return "".join(
        char for char in unicodedata.normalize("NFD", text)
        if unicodedata.category(char) != "Mn"
    ).lower().strip()


# ---------------------------------------------------------------------------
# Section-type specific extraction prompts
# ---------------------------------------------------------------------------

# What to extract per section type
SECTION_EXTRACTION_MAP = {
    "trieu_chung": ["signs", "modifiers"],
    "chan_doan": ["signs", "services", "assertions"],
    "dieu_tri": ["services", "assertions"],
    "xet_nghiem": ["services", "observations"],
    "dai_cuong": ["signs"],
    "tien_luong": ["observations", "assertions"],
    "phong_benh": ["assertions"],
    "other": ["signs", "services"],
}


def build_extraction_prompt(section_type: str, chunk_text: str,
                            section_title: str, disease_name: str) -> str:
    """Build ontology-guided extraction prompt based on section type."""
    targets = SECTION_EXTRACTION_MAP.get(section_type, ["signs", "services"])

    parts = []
    parts.append(f"Analyze this Vietnamese medical text about {disease_name}.")
    parts.append(f"Section: {section_title} (type: {section_type})")
    parts.append("")
    parts.append("Extract the following INTO EXACT JSON format:")
    parts.append("")

    if "signs" in targets:
        parts.append("""**signs**: Array of clinical signs/symptoms found in text.
Each sign: {"text": "exact text from source", "modifier": "qualifier if any (severity, laterality, course)", "confidence": 0.0-1.0}
Example: {"text": "sốt đột ngột 38-39°C", "modifier": "đột ngột, 38-39°C", "confidence": 0.95}""")
        parts.append("")

    if "services" in targets:
        parts.append("""**services**: Array of medical services/drugs/procedures found in text.
Each service: {"text": "exact text from source", "role": "diagnostic|therapeutic|monitoring|preventive", "condition": "when to apply if stated", "confidence": 0.0-1.0}
Example: {"text": "Peniciline V 1MUI x 2 lần/ngày", "role": "therapeutic", "condition": "khi nghi liên cầu", "confidence": 0.9}""")
        parts.append("")

    if "observations" in targets:
        parts.append("""**observations**: Array of lab/test results or expected findings.
Each observation: {"text": "exact text from source", "semantics": "positive|negative|abnormal|normal|narrative", "confidence": 0.0-1.0}
Example: {"text": "bạch cầu tăng, đa nhân trung tính", "semantics": "abnormal", "confidence": 0.9}""")
        parts.append("")

    if "assertions" in targets:
        parts.append("""**assertions**: Array of clinical rules/guidelines stated in text.
Each assertion: {"assertion_text": "full rule as stated", "type": "treatment_rule|diagnostic_rule|contraindication|indication|dosage_rule|monitoring_rule", "condition": "when this applies", "action": "what to do", "evidence_level": "strong|moderate|weak|expert_opinion", "related_signs": ["sign names"], "related_services": ["service names"]}
Example: {"assertion_text": "Viêm mũi họng có mủ trắng phải điều trị như liên cầu", "type": "treatment_rule", "condition": "có mủ trắng trên amiđan", "action": "điều trị như do liên cầu", "evidence_level": "expert_opinion", "related_signs": ["mủ trắng trên amiđan"], "related_services": ["kháng sinh nhóm Peniciline"]}""")
        parts.append("")

    if "modifiers" in targets:
        parts.append("""**modifiers**: Already included in signs[].modifier field.""")
        parts.append("")

    parts.append("Rules:")
    parts.append("- Only include items EXPLICITLY mentioned in the text")
    parts.append("- Keep exact Vietnamese wording from source for 'text' fields")
    parts.append("- Do not infer or hallucinate")
    parts.append("- Return empty arrays [] if nothing found for a category")
    parts.append("")
    parts.append(f"Text:\n{chunk_text}")
    parts.append("")
    result_keys = [k for k in ["signs", "services", "observations", "assertions"] if k in targets]
    json_template = ", ".join(f'"{k}": [...]' for k in result_keys)
    parts.append(f"Return JSON: {{{json_template}}}")

    return "\n".join(parts)


def build_gleaning_prompt(section_type: str, chunk_text: str,
                          already_extracted: dict, disease_name: str) -> str:
    """Build multi-pass gleaning prompt — ask LLM to find missed entities."""
    summary_parts = []
    for key in ["signs", "services", "observations", "assertions"]:
        items = already_extracted.get(key, [])
        if items:
            texts = [i.get("text", i.get("assertion_text", "?"))[:60] for i in items]
            summary_parts.append(f"  {key}: {', '.join(texts)}")

    return f"""Review this Vietnamese medical text about {disease_name} again.
Section type: {section_type}

Already extracted:
{chr(10).join(summary_parts) if summary_parts else "  (nothing found yet)"}

Text:
{chunk_text}

Are there any clinical signs, services, lab results, or clinical rules that were MISSED in the first extraction?
Only list NEW items not already in the list above.
Return JSON with same format. Return empty arrays if nothing was missed.
"""


# ---------------------------------------------------------------------------
# Ontology V2 Ingest Pipeline
# ---------------------------------------------------------------------------

class OntologyV2Ingest:
    """Ontology-guided extraction pipeline with dual representation."""

    def __init__(self, namespace: str = "ontology_v2"):
        self.namespace = namespace

        self.client = AzureOpenAI(
            api_key=os.getenv("AZURE_OPENAI_API_KEY"),
            api_version=os.getenv("AZURE_OPENAI_API_VERSION"),
            azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT"),
        )
        self.model = os.getenv("MODEL1", "gpt-4o-mini")

        self.embedding_client = AzureOpenAI(
            azure_endpoint=os.getenv("AZURE_EMBEDDINGS_ENDPOINT", "").strip(),
            api_key=os.getenv("AZURE_EMBEDDINGS_API_KEY", "").strip(),
            api_version=os.getenv("AZURE_OPENAI_API_VERSION", "").strip(),
        )
        self.embedding_model = "text-embedding-ada-002"

        uri = os.getenv("NEO4J_URI", os.getenv("neo4j_uri", "bolt://localhost:7688"))
        user = os.getenv("NEO4J_USER", os.getenv("neo4j_user", "neo4j"))
        password = os.getenv("NEO4J_PASSWORD", os.getenv("neo4j_password", "password123"))
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        self.experience_memory = ReasoningExperienceMemory(
            Path(__file__).resolve().parent / "data" / "script" / "experience_memory" / "reasoning_experience_memory.jsonl"
        )
        self.service_text_mapper = ServiceTextMapper() if ServiceTextMapper is not None else None

        # Counters for stats
        self._stats: dict[str, int] = {}

    def close(self):
        self.driver.close()

    # -- Helpers --

    def _inc(self, key: str, n: int = 1):
        self._stats[key] = self._stats.get(key, 0) + n

    def _llm_json(self, system: str, user: str) -> dict:
        """Call LLM with JSON response format."""
        kwargs = dict(
            model=self.model,
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
            response_format={"type": "json_object"},
        )
        try:
            kwargs["temperature"] = 0.0
            response = self.client.chat.completions.create(**kwargs)
        except Exception as e:
            if "temperature" in str(e).lower():
                kwargs.pop("temperature", None)
                response = self.client.chat.completions.create(**kwargs)
            else:
                raise
        return json.loads(response.choices[0].message.content)

    def _get_embedding(self, text: str) -> list[float] | None:
        text = text.replace("\n", " ")[:8000]
        try:
            return self.embedding_client.embeddings.create(
                input=[text], model=self.embedding_model
            ).data[0].embedding
        except Exception as e:
            print(f"  [WARN] Embedding error: {e}")
            return None

    def _slugify(self, text: str) -> str:
        text = strip_diacritics(text)
        text = re.sub(r'[^a-z0-9]+', '_', text)
        return text.strip('_')

    def _infer_source_type(self, pdf_path: str | None) -> str | None:
        """Best-effort source classification for protocol documents."""
        if not pdf_path:
            return None
        folded = strip_diacritics(Path(pdf_path).as_posix())
        if any(token in folded for token in ("extracted_protocols", "roadmap_master_data")):
            return "BYT"
        if any(token in folded for token in ("byt", "bo_y_te", "quyet_dinh", "qd_")):
            return "BYT"
        if any(token in folded for token in ("benh_vien", "vinmec", "hospital")):
            return "hospital"
        return None

    def _infer_document_surface_type(self, source_path: str | None) -> str:
        """Classify the upstream storage surface of the protocol source."""
        if not source_path:
            return "protocol_document"
        suffix = Path(source_path).suffix.lower()
        if suffix == ".pdf":
            return "protocol_pdf"
        if suffix == ".txt":
            return "protocol_text"
        if suffix in {".md", ".markdown"}:
            return "protocol_markdown"
        return "protocol_document"

    def _build_protocol_sections(self, disease_slug: str, chunks: list[RawChunkData]) -> tuple[list[dict[str, Any]], dict[str, str]]:
        """Group chunks into protocol sections and keep chunk -> section mapping."""
        sections: list[dict[str, Any]] = []
        section_by_key: dict[tuple[str, str, str], dict[str, Any]] = {}
        chunk_to_section_id: dict[str, str] = {}

        for chunk in chunks:
            section_title = str(chunk.section_title or chunk.section_type or "section").strip() or "section"
            section_type = str(chunk.section_type or "other").strip() or "other"
            parent_path = str(chunk.parent_section_path or "").strip()
            key = (parent_path, section_type, section_title)

            record = section_by_key.get(key)
            if record is None:
                section_index = len(sections)
                title_slug = self._slugify(section_title) or f"section_{section_index:03d}"
                section_id = f"{disease_slug}_section_{section_index:03d}_{title_slug[:48]}"
                record = {
                    "section_id": section_id,
                    "section_title": section_title,
                    "section_type": section_type,
                    "parent_section_path": parent_path,
                    "chunk_ids": [],
                    "page_numbers": [],
                }
                section_by_key[key] = record
                sections.append(record)

            record["chunk_ids"].append(chunk.chunk_id)
            record["page_numbers"].extend(
                int(page)
                for page in chunk.page_numbers
                if isinstance(page, int) and page >= 0
            )
            chunk_to_section_id[chunk.chunk_id] = record["section_id"]

        for record in sections:
            ordered_pages = list(dict.fromkeys(record["page_numbers"]))
            record["page_numbers"] = ordered_pages
            record["page_start"] = min(ordered_pages) if ordered_pages else 0
            record["page_end"] = max(ordered_pages) if ordered_pages else 0

        return sections, chunk_to_section_id

    def _build_assertion_mapping_lookup(self, mentions: list[Any], mappings: dict) -> dict[str, list[str]]:
        """Create normalized-text -> canonical-id lookup from resolved mention mappings."""
        lookup: dict[str, list[str]] = {}
        for mention in mentions:
            mapping = mappings.get(mention.mention_id, {})
            canonical_id = str(mapping.get("concept_id") or "").strip()
            if not canonical_id or mapping.get("status") == "unknown":
                continue
            normalized_key = strip_diacritics(getattr(mention, "mention_text", ""))
            if not normalized_key:
                continue
            bucket = lookup.setdefault(normalized_key, [])
            if canonical_id not in bucket:
                bucket.append(canonical_id)
        return lookup

    def _resolve_sign_ids_for_assertion(
        self,
        session,
        normalized_key: str,
        cache: dict[str, list[str]],
    ) -> list[str]:
        if not normalized_key:
            return []
        if normalized_key in cache:
            return cache[normalized_key]

        sign_ids: list[str] = []
        for query, params in (
            (
                """
                MATCH (s)
                WHERE (s:SignConcept OR s:ClaimSignConcept OR s:CISign)
                  AND coalesce(s.sign_id, s.claim_sign_id, '') <> ''
                  AND s.normalized_key = $nkey
                RETURN DISTINCT coalesce(s.sign_id, s.claim_sign_id) AS sign_id
                """,
                {"nkey": normalized_key},
            ),
            (
                """
                MATCH (a)-[r]-(s)
                WHERE a.normalized_alias = $nkey
                  AND coalesce(s.sign_id, s.claim_sign_id, '') <> ''
                  AND type(r) IN ['ALIAS_OF_SIGN', 'CLAIM_SIGN_HAS_ALIAS']
                RETURN DISTINCT coalesce(s.sign_id, s.claim_sign_id) AS sign_id
                """,
                {"nkey": normalized_key},
            ),
        ):
            for row in session.run(query, **params):
                sign_id = str(row.get("sign_id") or "").strip()
                if sign_id and sign_id not in sign_ids:
                    sign_ids.append(sign_id)

        cache[normalized_key] = sign_ids
        return sign_ids

    def _resolve_service_codes_for_assertion(
        self,
        session,
        service_text: str,
        normalized_key: str,
        cache: dict[str, list[str]],
    ) -> list[str]:
        cache_key = normalized_key or strip_diacritics(service_text)
        if not cache_key:
            return []
        if cache_key in cache:
            return cache[cache_key]

        service_codes: list[str] = []
        for query, params in (
            (
                """
                MATCH (s)
                WHERE (s:ProtocolService OR s:CIService)
                  AND (
                        toLower(coalesce(s.service_name, s.name, '')) = toLower($service_text)
                     OR toLower(coalesce(s.bhyt_name, '')) = toLower($service_text)
                  )
                RETURN DISTINCT s.service_code AS service_code
                """,
                {"service_text": service_text},
            ),
            (
                """
                MATCH (a:ServiceAlias)-[:ALIAS_OF_SERVICE]->(s)
                WHERE a.normalized_alias = $nkey
                  AND (s:ProtocolService OR s:CIService)
                RETURN DISTINCT s.service_code AS service_code
                """,
                {"nkey": cache_key},
            ),
            (
                """
                MATCH (s)
                WHERE (s:ProtocolService OR s:CIService)
                  AND $service_text <> ''
                  AND toLower(coalesce(s.service_name, s.name, '')) CONTAINS toLower($service_text)
                RETURN DISTINCT s.service_code AS service_code
                LIMIT 5
                """,
                {"service_text": service_text},
            ),
            (
                """
                MATCH (ci:CIService)-[:MAPS_TO_CANONICAL]->(cs:CanonicalService)
                WHERE $service_text <> ''
                  AND toLower(coalesce(cs.canonical_name_primary, '')) CONTAINS toLower($service_text)
                RETURN DISTINCT ci.service_code AS service_code
                LIMIT 3
                """,
                {"service_text": service_text},
            ),
        ):
            for row in session.run(query, **params):
                service_code = str(row.get("service_code") or "").strip()
                if service_code and service_code not in service_codes:
                    service_codes.append(service_code)

        cache[cache_key] = service_codes
        return service_codes

    def _assertion_sign_role(self, assertion_type: str) -> str:
        folded = strip_diacritics(assertion_type).replace(" ", "_")
        if folded in {"diagnostic_rule", "indication"}:
            return "required"
        if folded == "contraindication":
            return "exclude"
        if folded == "monitoring_rule":
            return "monitor"
        return "supportive"

    def _is_contraindication_assertion(self, assertion: ProtocolAssertionData) -> bool:
        if strip_diacritics(assertion.assertion_type) == "contraindication":
            return True
        folded_text = strip_diacritics(f"{assertion.assertion_text} {assertion.action_text}")
        return any(
            marker in folded_text
            for marker in (
                "chong chi dinh",
                "khong chi dinh",
                "khong nen",
                "tranh",
                "cam dung",
                "khong duoc",
            )
        )

    def _build_experience_advisory(
        self,
        disease_name: str,
        *,
        sign_terms: list[str] | None = None,
        service_terms: list[str] | None = None,
        scopes: list[str] | None = None,
        memory_kinds: list[str] | None = None,
        top_k: int = 6,
    ) -> dict[str, Any]:
        matches = self.experience_memory.query(
            disease_name=disease_name,
            sign_terms=sign_terms or [],
            service_terms=service_terms or [],
            scopes=scopes or ["ingest", "shared"],
            memory_kinds=memory_kinds or ["procedural", "semantic", "episodic"],
            min_importance=0.35,
            top_k=top_k,
        )
        by_category: dict[str, int] = {}
        by_severity: dict[str, int] = {}
        by_kind: dict[str, int] = {}
        for item in matches:
            category = str(item.get("category") or "unknown")
            severity = str(item.get("severity") or "unknown")
            memory_kind = str(item.get("memory_kind") or "unknown")
            by_category[category] = by_category.get(category, 0) + 1
            by_severity[severity] = by_severity.get(severity, 0) + 1
            by_kind[memory_kind] = by_kind.get(memory_kind, 0) + 1
        return {
            "memory_path": str(self.experience_memory.memory_path),
            "match_count": len(matches),
            "recommendations": self.experience_memory.summarize_matches(matches, limit=4),
            "by_category": by_category,
            "by_severity": by_severity,
            "by_kind": by_kind,
            "matches": matches,
        }

    def _write_experience_advisory_artifact(
        self,
        disease_name: str,
        pdf_path: str,
        advisory_payload: dict[str, Any],
    ) -> dict[str, str]:
        artifact_dir = Path(__file__).resolve().parent / "data" / "pipeline_runs" / "ontology_v2_experience_advice"
        artifact_dir.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        stem = f"{self._slugify(disease_name)}_{timestamp}"
        json_path = artifact_dir / f"{stem}.json"
        md_path = artifact_dir / f"{stem}.md"

        json_path.write_text(json.dumps(advisory_payload, ensure_ascii=False, indent=2), encoding="utf-8")

        lines = [
            f"# Ontology V2 Experience Advisory: {disease_name}",
            "",
            f"- Namespace: `{self.namespace}`",
            f"- PDF: `{pdf_path}`",
            f"- Pre-run matches: `{(advisory_payload.get('pre_run') or {}).get('match_count', 0)}`",
            f"- Contextual matches: `{(advisory_payload.get('contextual') or {}).get('match_count', 0)}`",
            "",
            "## Recommendations",
            "",
        ]
        seen: list[str] = []
        for section in ("pre_run", "contextual"):
            for item in (advisory_payload.get(section) or {}).get("recommendations", []):
                if item and item not in seen:
                    seen.append(item)
        if seen:
            for item in seen:
                lines.append(f"- {item}")
        else:
            lines.append("- No relevant experience found.")
        md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        return {"json": str(json_path), "md": str(md_path)}

    # -- Phase 1: Text Extraction + Section-Aware Chunking --

    def extract_and_chunk(self, pdf_path: str, disease_name: str,
                          skip_first_page: bool = False) -> list[RawChunkData]:
        """Extract text from PDF → section-aware chunking with type detection."""
        import fitz
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

        raw_text = "\n".join(full_text)
        cleaned = clean_ocr_text(raw_text)

        chunker = SemanticChunker()
        chunks = chunker.chunk(cleaned, skip_first_page=skip_first_page)

        disease_slug = self._slugify(disease_name)
        result = []
        for i, c in enumerate(chunks):
            stype = detect_section_type(c['title'], c['content'][:200])
            chunk_id = f"{disease_slug}_{stype}_{i:03d}"
            result.append(RawChunkData(
                chunk_id=chunk_id,
                disease_id=disease_slug,
                section_type=stype,
                section_title=c['title'],
                body_text=c['content'],
                page_numbers=[c.get('page_number', 0)],
                parent_section_path=c.get('section_path', ''),
            ))

        return result

    def extract_from_text(self, text: str, disease_name: str,
                          skip_first_page: bool = False) -> list[RawChunkData]:
        """Extract from pre-extracted text (for multi-disease PDFs)."""
        cleaned = clean_ocr_text(text)
        chunker = SemanticChunker()
        chunks = chunker.chunk(cleaned, skip_first_page=skip_first_page)

        disease_slug = self._slugify(disease_name)
        result = []
        for i, c in enumerate(chunks):
            stype = detect_section_type(c['title'], c['content'][:200])
            chunk_id = f"{disease_slug}_{stype}_{i:03d}"
            result.append(RawChunkData(
                chunk_id=chunk_id,
                disease_id=disease_slug,
                section_type=stype,
                section_title=c['title'],
                body_text=c['content'],
                page_numbers=[c.get('page_number', 0)],
                parent_section_path=c.get('section_path', ''),
            ))
        return result

    # -- Phase 2: Ontology-Typed Extraction --

    def extract_from_chunk(self, chunk: RawChunkData, disease_name: str
                           ) -> dict:
        """Extract ontology-typed entities from a single chunk.

        Returns dict with keys: signs, services, observations, assertions
        Each containing list of dataclass instances.
        """
        system_prompt = (
            "You are a medical NER system for Vietnamese clinical protocols. "
            "Extract entities precisely into the requested categories. "
            "Return valid JSON only. Keep exact Vietnamese wording."
        )

        # Pass 1: Main extraction
        prompt = build_extraction_prompt(
            chunk.section_type, chunk.body_text,
            chunk.section_title, disease_name
        )
        try:
            raw = self._llm_json(system_prompt, prompt)
        except Exception as e:
            print(f"  [WARN] Extraction failed for {chunk.chunk_id}: {e}")
            raw = {}

        # Pass 2: Gleaning — ask for missed entities
        if chunk.body_text and len(chunk.body_text) > 100:
            try:
                gleaning_prompt = build_gleaning_prompt(
                    chunk.section_type, chunk.body_text, raw, disease_name
                )
                extra = self._llm_json(system_prompt, gleaning_prompt)
                # Merge new items
                for key in ["signs", "services", "observations", "assertions"]:
                    existing_texts = {
                        item.get("text", item.get("assertion_text", "")).lower()
                        for item in raw.get(key, [])
                    }
                    for item in extra.get(key, []):
                        item_text = item.get("text", item.get("assertion_text", "")).lower()
                        if item_text and item_text not in existing_texts:
                            raw.setdefault(key, []).append(item)
                            self._inc("gleaning_found")
            except Exception:
                pass  # Gleaning is best-effort

        # Convert to typed data classes
        disease_slug = self._slugify(disease_name)
        result: dict[str, list] = {"signs": [], "services": [], "observations": [], "assertions": []}

        for i, s in enumerate(raw.get("signs", [])):
            mid = f"{chunk.chunk_id}_sign_{i:02d}"
            result["signs"].append(RawSignMentionData(
                mention_id=mid,
                mention_text=s.get("text", ""),
                context_text=s.get("context", ""),
                modifier_raw=s.get("modifier", ""),
                extraction_confidence=float(s.get("confidence", 0.8)),
                source_chunk_id=chunk.chunk_id,
                source_page=chunk.page_numbers[0] if chunk.page_numbers else 0,
            ))

        for i, s in enumerate(raw.get("services", [])):
            mid = f"{chunk.chunk_id}_svc_{i:02d}"
            result["services"].append(RawServiceMentionData(
                mention_id=mid,
                mention_text=s.get("text", ""),
                context_text=s.get("context", ""),
                medical_role=s.get("role", "unknown"),
                condition_to_apply=s.get("condition", ""),
                extraction_confidence=float(s.get("confidence", 0.8)),
                source_chunk_id=chunk.chunk_id,
                source_page=chunk.page_numbers[0] if chunk.page_numbers else 0,
            ))

        for i, o in enumerate(raw.get("observations", [])):
            mid = f"{chunk.chunk_id}_obs_{i:02d}"
            result["observations"].append(RawObservationMentionData(
                mention_id=mid,
                mention_text=o.get("text", ""),
                context_text=o.get("context", ""),
                result_semantics=o.get("semantics", "unknown"),
                extraction_confidence=float(o.get("confidence", 0.8)),
                source_chunk_id=chunk.chunk_id,
                source_page=chunk.page_numbers[0] if chunk.page_numbers else 0,
            ))

        for i, a in enumerate(raw.get("assertions", [])):
            aid = f"{chunk.chunk_id}_assert_{i:02d}"
            result["assertions"].append(ProtocolAssertionData(
                assertion_id=aid,
                assertion_text=a.get("assertion_text", ""),
                assertion_type=a.get("type", "treatment_rule"),
                condition_text=a.get("condition", ""),
                action_text=a.get("action", ""),
                evidence_level=a.get("evidence_level", "expert_opinion"),
                source_chunk_id=chunk.chunk_id,
                source_page=chunk.page_numbers[0] if chunk.page_numbers else 0,
                related_signs=a.get("related_signs", []),
                related_services=a.get("related_services", []),
            ))

        return result

    # -- Phase 3: Canonical Resolution --

    def resolve_canonical(self, mentions: dict, disease_name: str) -> dict:
        """Map raw mentions → canonical concepts with confidence scoring.

        Returns mapping dict: {mention_id: {concept_id, confidence, method, status}}
        """
        mappings = {}

        # Load existing canonical concepts from Neo4j
        existing_signs = {}
        existing_services = {}
        try:
            with self.driver.session() as session:
                # Load sign concepts
                records = session.run(
                    """
                    MATCH (s)
                    WHERE (s:SignConcept OR s:ClaimSignConcept OR s:CISign)
                      AND coalesce(s.sign_id, s.claim_sign_id, '') <> ''
                    RETURN coalesce(s.sign_id, s.claim_sign_id) AS id,
                           coalesce(s.canonical_label, s.text, s.label, s.sign_id, s.claim_sign_id) AS label,
                           s.normalized_key AS nkey
                    """
                )
                for r in records:
                    existing_signs[r["nkey"] or strip_diacritics(r["label"])] = {
                        "sign_id": r["id"], "label": r["label"]
                    }

                # Load sign aliases
                records = session.run(
                    """
                    MATCH (a)-[r]-(c)
                    WHERE a.normalized_alias IS NOT NULL
                      AND coalesce(c.sign_id, c.claim_sign_id, '') <> ''
                      AND type(r) IN ['ALIAS_OF_SIGN', 'CLAIM_SIGN_HAS_ALIAS']
                    RETURN a.normalized_alias AS nkey,
                           coalesce(c.sign_id, c.claim_sign_id) AS cid,
                           coalesce(c.canonical_label, c.text, c.label, c.sign_id, c.claim_sign_id) AS clabel
                    """
                )
                for r in records:
                    if r["nkey"] and r["nkey"] not in existing_signs:
                        existing_signs[r["nkey"]] = {"sign_id": r["cid"], "label": r["clabel"]}

                # Load services
                records = session.run(
                    """
                    MATCH (s)
                    WHERE (s:ProtocolService OR s:CIService)
                      AND coalesce(s.service_code, '') <> ''
                    RETURN s.service_code AS code,
                           coalesce(s.service_name, s.name) AS name
                    """
                )
                for r in records:
                    nkey = strip_diacritics(r["name"])
                    existing_services[nkey] = {"code": r["code"], "name": r["name"]}

                records = session.run(
                    """
                    MATCH (a:ServiceAlias)-[:ALIAS_OF_SERVICE]->(s)
                    WHERE s:ProtocolService OR s:CIService
                    RETURN a.normalized_alias AS nkey,
                           s.service_code AS code,
                           coalesce(s.service_name, s.name) AS name
                    """
                )
                for r in records:
                    nkey = r["nkey"]
                    if nkey and nkey not in existing_services:
                        existing_services[nkey] = {"code": r["code"], "name": r["name"]}
                records = session.run(
                    """
                    MATCH (ci:CIService)-[:MAPS_TO_CANONICAL]->(cs:CanonicalService)
                    WHERE coalesce(cs.canonical_name_primary, '') <> ''
                    RETURN DISTINCT ci.service_code AS code,
                           coalesce(cs.canonical_name_primary, cs.maanhxa) AS canonical_name,
                           coalesce(ci.service_name, cs.canonical_name_primary) AS preferred_name
                    """
                )
                for r in records:
                    nkey = strip_diacritics(r["canonical_name"])
                    if nkey and nkey not in existing_services:
                        existing_services[nkey] = {"code": r["code"], "name": r["preferred_name"]}
        except Exception as e:
            print(f"  [WARN] Could not load existing concepts: {e}")

        # Map signs
        for sign in mentions.get("signs", []):
            nkey = strip_diacritics(sign.mention_text)
            if nkey in existing_signs:
                mappings[sign.mention_id] = {
                    "concept_id": existing_signs[nkey]["sign_id"],
                    "concept_label": existing_signs[nkey]["label"],
                    "confidence": 1.0, "method": "exact", "status": "exact",
                }
                self._inc("map_exact")
            else:
                # Fuzzy: check if normalized key is substring of existing
                best_match = None
                best_score = 0.0
                for ekey, edata in existing_signs.items():
                    if len(nkey) >= 4 and (nkey in ekey or ekey in nkey):
                        overlap = len(set(nkey.split()) & set(ekey.split()))
                        total = max(len(nkey.split()), len(ekey.split()))
                        score = overlap / total if total > 0 else 0
                        if score > best_score:
                            best_score = score
                            best_match = edata
                if best_match and best_score >= 0.5:
                    status = "probable" if best_score >= 0.7 else "ambiguous"
                    mappings[sign.mention_id] = {
                        "concept_id": best_match["sign_id"],
                        "concept_label": best_match["label"],
                        "confidence": best_score, "method": "fuzzy_match", "status": status,
                    }
                    self._inc(f"map_{status}")
                else:
                    mappings[sign.mention_id] = {
                        "concept_id": None, "concept_label": sign.mention_text,
                        "confidence": 0.0, "method": "none", "status": "unknown",
                    }
                    self._inc("map_unknown")

        # Map services (similar logic)
        for svc in mentions.get("services", []):
            nkey = strip_diacritics(svc.mention_text)
            if nkey in existing_services:
                mappings[svc.mention_id] = {
                    "concept_id": existing_services[nkey]["code"],
                    "concept_label": existing_services[nkey]["name"],
                    "confidence": 1.0, "method": "exact", "status": "exact",
                }
                self._inc("map_exact")
            else:
                best_match = None
                best_score = 0.0
                n_tokens = set(nkey.split())
                for ekey, edata in existing_services.items():
                    if len(nkey) < 4:
                        continue
                    if nkey in ekey or ekey in nkey:
                        overlap = len(n_tokens & set(ekey.split()))
                        total = max(len(n_tokens), len(ekey.split()))
                        score = overlap / total if total > 0 else 0.0
                        if score > best_score:
                            best_score = score
                            best_match = edata
                if best_match and best_score >= 0.5:
                    status = "probable" if best_score >= 0.7 else "ambiguous"
                    mappings[svc.mention_id] = {
                        "concept_id": best_match["code"],
                        "concept_label": best_match["name"],
                        "confidence": best_score,
                        "method": "fuzzy_match",
                        "status": status,
                    }
                    self._inc(f"map_{status}")
                else:
                    mapper_suggestion = None
                    if self.service_text_mapper is not None:
                        try:
                            result = self.service_text_mapper.score_text(svc.mention_text, top_k=1)
                            suggestions = result.get("suggestions") or []
                            mapper_suggestion = suggestions[0] if suggestions else None
                        except Exception as exc:
                            print(f"  [WARN] ServiceTextMapper fallback failed for '{svc.mention_text}': {exc}")

                    if mapper_suggestion:
                        confidence = str(mapper_suggestion.get("confidence") or "REVIEW").upper()
                        score = float(mapper_suggestion.get("score") or 0.0)
                        if confidence in {"HIGH", "MEDIUM"}:
                            status = "probable"
                        elif confidence == "LOW":
                            status = "ambiguous"
                        else:
                            status = "unknown"

                        mappings[svc.mention_id] = {
                            "concept_id": mapper_suggestion.get("service_code") if status != "unknown" else None,
                            "concept_label": mapper_suggestion.get("canonical_name") or svc.mention_text,
                            "confidence": round(score / 100.0, 4),
                            "method": "service_text_mapper",
                            "status": status,
                        }
                        self._inc(f"map_{status}")
                    else:
                        mappings[svc.mention_id] = {
                            "concept_id": None, "concept_label": svc.mention_text,
                            "confidence": 0.0, "method": "none", "status": "unknown",
                        }
                        self._inc("map_unknown")

        # Observations — mostly new, map as unknown for now
        for obs in mentions.get("observations", []):
            mappings[obs.mention_id] = {
                "concept_id": None, "concept_label": obs.mention_text,
                "confidence": 0.0, "method": "none", "status": "unknown",
            }

        return mappings

    # -- Phase 4: Summary Generation --

    def generate_disease_summary(self, chunks: list[RawChunkData],
                                 all_mentions: dict,
                                 disease_name: str) -> DiseaseSummaryData:
        """Generate disease summary from all chunks. Acceleration layer only."""
        # Collect all extracted items
        all_signs = [s.mention_text for s in all_mentions.get("signs", [])]
        all_services = [s.mention_text for s in all_mentions.get("services", [])]
        all_assertions = [a.assertion_text[:100] for a in all_mentions.get("assertions", [])]

        # Collect section previews
        section_previews = []
        for c in chunks[:20]:
            section_previews.append(f"[{c.section_type}] {c.section_title}: {c.body_text[:200]}...")

        prompt = f"""Summarize this Vietnamese medical protocol for {disease_name}.

Sections found:
{chr(10).join(section_previews)}

Signs extracted: {', '.join(set(all_signs[:20]))}
Services extracted: {', '.join(set(all_services[:15]))}

Generate a JSON summary:
{{
  "summary_text": "2-4 paragraph Vietnamese summary covering: definition, key symptoms, diagnosis approach, main treatment",
  "key_signs": ["top 5-10 most important signs"],
  "key_services": ["top 5-10 key services/drugs"],
  "key_drugs": ["main drugs mentioned"],
  "differential_diseases": ["diseases to differentiate from"]
}}"""

        try:
            data = self._llm_json(
                "You are a medical summarizer for Vietnamese clinical protocols. Return JSON.",
                prompt
            )
            return DiseaseSummaryData(
                summary_id=f"{self._slugify(disease_name)}_summary",
                disease_id=self._slugify(disease_name),
                summary_text=data.get("summary_text", ""),
                key_signs=data.get("key_signs", []),
                key_services=data.get("key_services", []),
                key_drugs=data.get("key_drugs", []),
                differential_diseases=data.get("differential_diseases", []),
            )
        except Exception as e:
            print(f"  [WARN] Summary generation failed: {e}")
            return DiseaseSummaryData(
                summary_id=f"{self._slugify(disease_name)}_summary",
                disease_id=self._slugify(disease_name),
                summary_text=f"Summary generation failed for {disease_name}",
            )

    # -- Phase 5: Neo4j Ingest (Dual Representation) --

    def create_indexes(self):
        """Create Neo4j indexes for V2 ontology nodes."""
        with self.driver.session() as session:
            for statement in [
                "CREATE CONSTRAINT protocol_service_service_code_unique IF NOT EXISTS FOR (n:ProtocolService) REQUIRE n.service_code IS UNIQUE",
                "CREATE CONSTRAINT sign_concept_sign_id_unique IF NOT EXISTS FOR (n:SignConcept) REQUIRE n.sign_id IS UNIQUE",
                "CREATE CONSTRAINT disease_entity_disease_id_unique IF NOT EXISTS FOR (n:DiseaseEntity) REQUIRE n.disease_id IS UNIQUE",
                "CREATE CONSTRAINT protocol_assertion_assertion_id_unique IF NOT EXISTS FOR (n:ProtocolAssertion) REQUIRE n.assertion_id IS UNIQUE",
            ]:
                try:
                    session.run(statement)
                except Exception:
                    pass

            # Vector indexes
            for label, prop in [
                ("RawChunk", "embedding"),
                ("ProtocolAssertion", "embedding"),
                ("ProtocolDiseaseSummary", "summary_embedding"),
            ]:
                try:
                    session.run(f"""
                        CREATE VECTOR INDEX `{label.lower()}_vector_idx` IF NOT EXISTS
                        FOR (n:{label}) ON (n.{prop})
                        OPTIONS {{indexConfig: {{
                            `vector.dimensions`: 1536,
                            `vector.similarity_function`: 'cosine'
                        }}}}
                    """)
                except Exception:
                    pass  # Index may already exist

            # Fulltext indexes
            try:
                session.run("""
                    CREATE FULLTEXT INDEX `raw_chunk_fulltext` IF NOT EXISTS
                    FOR (n:RawChunk) ON EACH [n.body_text, n.section_title]
                """)
            except Exception:
                pass
            try:
                session.run("""
                    CREATE FULLTEXT INDEX `assertion_fulltext` IF NOT EXISTS
                    FOR (n:ProtocolAssertion) ON EACH [n.assertion_text, n.condition_text, n.action_text]
                """)
            except Exception:
                pass

            # Property indexes
            for label, prop in [
                ("RawChunk", "chunk_id"),
                ("RawChunk", "disease_id"),
                ("RawDocument", "doc_id"),
                ("ProtocolBook", "book_id"),
                ("ProtocolSection", "section_id"),
                ("RawSignMention", "mention_id"),
                ("RawServiceMention", "mention_id"),
                ("RawObservationMention", "mention_id"),
                ("ProtocolAssertion", "assertion_id"),
            ]:
                try:
                    session.run(f"""
                        CREATE INDEX `{label.lower()}_{prop}_idx` IF NOT EXISTS
                        FOR (n:{label}) ON (n.{prop})
                    """)
                except Exception:
                    pass

        print("[OK] Neo4j indexes created.")

    def ingest_to_neo4j(self, disease_name: str, chunks: list[RawChunkData],
                        all_mentions: dict, mappings: dict,
                        summary: DiseaseSummaryData,
                        pdf_path: str | None = None,
                        source_type: str | None = None):
        """Ingest all 6 layers into Neo4j with full provenance."""
        disease_slug = self._slugify(disease_name)
        sections, chunk_to_section_id = self._build_protocol_sections(disease_slug, chunks)
        doc_title = Path(pdf_path).stem if pdf_path else disease_name
        doc_slug = self._slugify(doc_title) or disease_slug or "document"
        doc_id = f"{self.namespace}_{doc_slug}_doc"
        book_id = f"{self.namespace}_{doc_slug}_book"
        resolved_source_type = source_type or self._infer_source_type(pdf_path)
        doc_type = self._infer_document_surface_type(pdf_path)
        ordered_pages = sorted({
            page for chunk in chunks for page in chunk.page_numbers
            if isinstance(page, int) and page >= 0
        })
        page_count = len(ordered_pages)
        sign_lookup = self._build_assertion_mapping_lookup(all_mentions.get("signs", []), mappings)
        service_lookup = self._build_assertion_mapping_lookup(all_mentions.get("services", []), mappings)
        sign_resolution_cache = dict(sign_lookup)
        service_resolution_cache = dict(service_lookup)

        with self.driver.session() as session:
            # -- DiseaseEntity --
            session.run("""
                MERGE (d:DiseaseEntity {disease_id: $did})
                ON CREATE SET d.disease_name = $name, d.namespace = $ns
            """, did=disease_slug, name=disease_name, ns=self.namespace)

            # -- RawDocument --
            session.run("""
                MERGE (doc:RawDocument {doc_id: $doc_id})
                ON CREATE SET
                    doc.namespace = $ns,
                    doc.doc_type = $doc_type,
                    doc.title = $title,
                    doc.file_path = $file_path,
                    doc.page_count = $page_count,
                    doc.ingested_at = datetime()
            """, doc_id=doc_id, ns=self.namespace, title=doc_title,
                 file_path=pdf_path or "", page_count=page_count, doc_type=doc_type)
            if resolved_source_type:
                session.run("""
                    MATCH (doc:RawDocument {doc_id: $doc_id})
                    SET doc.source_type = $source_type
                """, doc_id=doc_id, source_type=resolved_source_type)
            self._inc("documents")

            # -- ProtocolBook --
            session.run("""
                MERGE (b:ProtocolBook {book_id: $book_id})
                ON CREATE SET
                    b.namespace = $ns,
                    b.book_name = $book_name
            """, book_id=book_id, ns=self.namespace, book_name=doc_title)
            if resolved_source_type:
                session.run("""
                    MATCH (b:ProtocolBook {book_id: $book_id})
                    SET b.source_type = $source_type
                """, book_id=book_id, source_type=resolved_source_type)
            self._inc("books")

            # -- RawChunks --
            for chunk in chunks:
                embedding = self._get_embedding(
                    f"{chunk.section_title}\n{chunk.body_text}"
                )
                session.run("""
                    MERGE (c:RawChunk {chunk_id: $cid})
                    ON CREATE SET
                        c.namespace = $ns,
                        c.disease_id = $did,
                        c.section_type = $stype,
                        c.section_title = $stitle,
                        c.body_text = $body,
                        c.body_preview = $preview,
                        c.page_numbers = $pages,
                        c.parent_section_path = $ppath,
                        c.embedding = $emb
                """, cid=chunk.chunk_id, ns=self.namespace, did=disease_slug,
                     stype=chunk.section_type, stitle=chunk.section_title,
                     body=chunk.body_text, preview=chunk.body_text[:200],
                     pages=chunk.page_numbers, ppath=chunk.parent_section_path,
                     emb=embedding)

                # Link chunk → disease
                session.run("""
                    MATCH (c:RawChunk {chunk_id: $cid})
                    MATCH (d:DiseaseEntity {disease_id: $did})
                    MERGE (c)-[:CHUNK_ABOUT_DISEASE]->(d)
                """, cid=chunk.chunk_id, did=disease_slug)

                session.run("""
                    MATCH (c:RawChunk {chunk_id: $cid})
                    MATCH (doc:RawDocument {doc_id: $doc_id})
                    MERGE (c)-[:FROM_DOCUMENT]->(doc)
                """, cid=chunk.chunk_id, doc_id=doc_id)
                self._inc("chunks")

            # -- NEXT_CHUNK sequential links --
            for i in range(len(chunks) - 1):
                session.run("""
                    MATCH (a:RawChunk {chunk_id: $a_id})
                    MATCH (b:RawChunk {chunk_id: $b_id})
                    MERGE (a)-[:NEXT_CHUNK]->(b)
                """, a_id=chunks[i].chunk_id, b_id=chunks[i+1].chunk_id)

            # -- ProtocolSections --
            for section in sections:
                session.run("""
                    MERGE (s:ProtocolSection {section_id: $sid})
                    ON CREATE SET
                        s.namespace = $ns,
                        s.section_title = $stitle,
                        s.section_type = $stype,
                        s.disease_id = $did,
                        s.page_start = $pstart,
                        s.page_end = $pend
                """, sid=section["section_id"], ns=self.namespace,
                     stitle=section["section_title"], stype=section["section_type"],
                     did=disease_slug, pstart=section["page_start"], pend=section["page_end"])

                session.run("""
                    MATCH (b:ProtocolBook {book_id: $book_id})
                    MATCH (s:ProtocolSection {section_id: $sid})
                    MERGE (b)-[:BOOK_HAS_SECTION]->(s)
                """, book_id=book_id, sid=section["section_id"])

                session.run("""
                    MATCH (s:ProtocolSection {section_id: $sid})
                    MATCH (d:DiseaseEntity {disease_id: $did})
                    MERGE (s)-[:SECTION_COVERS_DISEASE]->(d)
                """, sid=section["section_id"], did=disease_slug)

                for chunk_id in section["chunk_ids"]:
                    session.run("""
                        MATCH (s:ProtocolSection {section_id: $sid})
                        MATCH (c:RawChunk {chunk_id: $cid})
                        MERGE (s)-[:SECTION_HAS_CHUNK]->(c)
                    """, sid=section["section_id"], cid=chunk_id)
                    self._inc("section_chunk_links")

                self._inc("sections")

            # -- RawSignMentions --
            for sign in all_mentions.get("signs", []):
                session.run("""
                    MERGE (m:RawSignMention {mention_id: $mid})
                    ON CREATE SET
                        m.namespace = $ns,
                        m.mention_text = $text,
                        m.normalized_key = $nkey,
                        m.context_text = $ctx,
                        m.modifier_raw = $mod,
                        m.extraction_confidence = $conf,
                        m.source_chunk_id = $scid,
                        m.source_page = $sp,
                        m.mapping_status = $ms
                """, mid=sign.mention_id, ns=self.namespace,
                     text=sign.mention_text, nkey=strip_diacritics(sign.mention_text),
                     ctx=sign.context_text, mod=sign.modifier_raw,
                     conf=sign.extraction_confidence,
                     scid=sign.source_chunk_id, sp=sign.source_page,
                     ms=mappings.get(sign.mention_id, {}).get("status", "pending"))

                # Chunk → Mention
                session.run("""
                    MATCH (c:RawChunk {chunk_id: $cid})
                    MATCH (m:RawSignMention {mention_id: $mid})
                    MERGE (c)-[:MENTIONS_SIGN]->(m)
                """, cid=sign.source_chunk_id, mid=sign.mention_id)

                # Mention → Concept (MAPS_TO) if resolved
                mapping = mappings.get(sign.mention_id, {})
                if mapping.get("concept_id") and mapping.get("status") != "unknown":
                    session.run("""
                        MATCH (m:RawSignMention {mention_id: $mid})
                        MATCH (c)
                        WHERE (c:SignConcept OR c:ClaimSignConcept OR c:CISign)
                          AND coalesce(c.sign_id, c.claim_sign_id) = $cid
                        MERGE (m)-[r:MAPS_TO_SIGN]->(c)
                        ON CREATE SET r.confidence = $conf, r.method = $method,
                                      r.status = $status, r.mapped_at = datetime()
                    """, mid=sign.mention_id, cid=mapping["concept_id"],
                         conf=mapping["confidence"], method=mapping["method"],
                         status=mapping["status"])
                    self._inc("maps_to_sign")

                self._inc("sign_mentions")

            # -- RawServiceMentions --
            for svc in all_mentions.get("services", []):
                session.run("""
                    MERGE (m:RawServiceMention {mention_id: $mid})
                    ON CREATE SET
                        m.namespace = $ns,
                        m.mention_text = $text,
                        m.normalized_key = $nkey,
                        m.context_text = $ctx,
                        m.medical_role = $role,
                        m.condition_to_apply = $cond,
                        m.extraction_confidence = $conf,
                        m.source_chunk_id = $scid,
                        m.source_page = $sp,
                        m.mapping_status = $ms
                """, mid=svc.mention_id, ns=self.namespace,
                     text=svc.mention_text, nkey=strip_diacritics(svc.mention_text),
                     ctx=svc.context_text, role=svc.medical_role,
                     cond=svc.condition_to_apply, conf=svc.extraction_confidence,
                     scid=svc.source_chunk_id, sp=svc.source_page,
                     ms=mappings.get(svc.mention_id, {}).get("status", "pending"))

                session.run("""
                    MATCH (c:RawChunk {chunk_id: $cid})
                    MATCH (m:RawServiceMention {mention_id: $mid})
                    MERGE (c)-[:MENTIONS_SERVICE]->(m)
                """, cid=svc.source_chunk_id, mid=svc.mention_id)

                mapping = mappings.get(svc.mention_id, {})
                if mapping.get("concept_id") and mapping.get("status") != "unknown":
                    session.run("""
                        MATCH (m:RawServiceMention {mention_id: $mid})
                        MATCH (c)
                        WHERE (c:ProtocolService OR c:CIService)
                          AND c.service_code = $cid
                        MERGE (m)-[r:MAPS_TO_SERVICE]->(c)
                        ON CREATE SET r.confidence = $conf, r.method = $method,
                                      r.status = $status, r.mapped_at = datetime()
                    """, mid=svc.mention_id, cid=mapping["concept_id"],
                         conf=mapping["confidence"], method=mapping["method"],
                         status=mapping["status"])
                    self._inc("maps_to_service")

                self._inc("service_mentions")

            # -- RawObservationMentions --
            for obs in all_mentions.get("observations", []):
                session.run("""
                    MERGE (m:RawObservationMention {mention_id: $mid})
                    ON CREATE SET
                        m.namespace = $ns,
                        m.mention_text = $text,
                        m.normalized_key = $nkey,
                        m.context_text = $ctx,
                        m.result_semantics = $sem,
                        m.extraction_confidence = $conf,
                        m.source_chunk_id = $scid,
                        m.source_page = $sp,
                        m.mapping_status = 'pending'
                """, mid=obs.mention_id, ns=self.namespace,
                     text=obs.mention_text, nkey=strip_diacritics(obs.mention_text),
                     ctx=obs.context_text, sem=obs.result_semantics,
                     conf=obs.extraction_confidence,
                     scid=obs.source_chunk_id, sp=obs.source_page)

                session.run("""
                    MATCH (c:RawChunk {chunk_id: $cid})
                    MATCH (m:RawObservationMention {mention_id: $mid})
                    MERGE (c)-[:MENTIONS_OBSERVATION]->(m)
                """, cid=obs.source_chunk_id, mid=obs.mention_id)

                self._inc("observation_mentions")

            # -- ProtocolAssertions --
            for assertion in all_mentions.get("assertions", []):
                embedding = self._get_embedding(assertion.assertion_text)
                session.run("""
                    MERGE (a:ProtocolAssertion {assertion_id: $aid})
                    ON CREATE SET
                        a.namespace = $ns,
                        a.assertion_text = $text,
                        a.assertion_type = $atype,
                        a.condition_text = $cond,
                        a.action_text = $action,
                        a.status = $status,
                        a.evidence_level = $elevel,
                        a.source_chunk_id = $scid,
                        a.source_page = $sp,
                        a.embedding = $emb
                """, aid=assertion.assertion_id, ns=self.namespace,
                     text=assertion.assertion_text, atype=assertion.assertion_type,
                     cond=assertion.condition_text, action=assertion.action_text,
                     status=assertion.status, elevel=assertion.evidence_level,
                     scid=assertion.source_chunk_id, sp=assertion.source_page,
                     emb=embedding)

                # Link chunk → assertion
                session.run("""
                    MATCH (c:RawChunk {chunk_id: $cid})
                    MATCH (a:ProtocolAssertion {assertion_id: $aid})
                    MERGE (c)-[:CONTAINS_ASSERTION]->(a)
                """, cid=assertion.source_chunk_id, aid=assertion.assertion_id)

                # Link assertion → disease
                session.run("""
                    MATCH (a:ProtocolAssertion {assertion_id: $aid})
                    MATCH (d:DiseaseEntity {disease_id: $did})
                    MERGE (a)-[:ASSERTION_ABOUT_DISEASE]->(d)
                """, aid=assertion.assertion_id, did=disease_slug)

                section_id = chunk_to_section_id.get(assertion.source_chunk_id)
                if section_id:
                    session.run("""
                        MATCH (s:ProtocolSection {section_id: $sid})
                        MATCH (a:ProtocolAssertion {assertion_id: $aid})
                        MERGE (s)-[:CONTAINS_ASSERTION]->(a)
                    """, sid=section_id, aid=assertion.assertion_id)
                    self._inc("section_assertion_links")

                sign_role = self._assertion_sign_role(assertion.assertion_type)
                for sign_text in assertion.related_signs:
                    normalized_sign = strip_diacritics(sign_text)
                    sign_ids = sign_resolution_cache.get(normalized_sign)
                    if sign_ids is None:
                        sign_ids = self._resolve_sign_ids_for_assertion(
                            session, normalized_sign, sign_resolution_cache
                        )
                    for sign_id in sign_ids:
                        session.run("""
                            MATCH (a:ProtocolAssertion {assertion_id: $aid})
                            MATCH (s)
                            WHERE coalesce(s.sign_id, s.claim_sign_id) = $sign_id
                              AND (s:SignConcept OR s:ClaimSignConcept OR s:CISign)
                            MERGE (a)-[r:ASSERTION_REQUIRES_SIGN]->(s)
                            ON CREATE SET r.role = $role
                        """, aid=assertion.assertion_id, sign_id=sign_id, role=sign_role)
                        self._inc("assertion_requires_sign")

                relation_type = (
                    "ASSERTION_CONTRAINDICATES"
                    if self._is_contraindication_assertion(assertion)
                    else "ASSERTION_INDICATES_SERVICE"
                )
                for service_text in assertion.related_services:
                    normalized_service = strip_diacritics(service_text)
                    service_codes = service_resolution_cache.get(normalized_service)
                    if service_codes is None:
                        service_codes = self._resolve_service_codes_for_assertion(
                            session,
                            service_text=service_text,
                            normalized_key=normalized_service,
                            cache=service_resolution_cache,
                        )
                    for service_code in service_codes:
                        session.run(f"""
                            MATCH (a:ProtocolAssertion {{assertion_id: $aid}})
                            MATCH (svc)
                            WHERE (svc:ProtocolService OR svc:CIService)
                              AND svc.service_code = $service_code
                            MERGE (a)-[:{relation_type}]->(svc)
                        """, aid=assertion.assertion_id, service_code=service_code)
                        if relation_type == "ASSERTION_CONTRAINDICATES":
                            self._inc("assertion_contraindicates_service")
                        else:
                            self._inc("assertion_indicates_service")

                self._inc("assertions")

            # -- ProtocolDiseaseSummary --
            summary_embedding = self._get_embedding(summary.summary_text)
            session.run("""
                MERGE (s:ProtocolDiseaseSummary {summary_id: $sid})
                ON CREATE SET
                    s.namespace = $ns,
                    s.disease_id = $did,
                    s.summary_text = $text,
                    s.key_signs = $signs,
                    s.key_services = $services,
                    s.key_drugs = $drugs,
                    s.differential_diseases = $diff,
                    s.summary_embedding = $emb
            """, sid=summary.summary_id, ns=self.namespace,
                 did=disease_slug, text=summary.summary_text,
                 signs=summary.key_signs, services=summary.key_services,
                 drugs=summary.key_drugs, diff=summary.differential_diseases,
                 emb=summary_embedding)

            session.run("""
                MATCH (s:ProtocolDiseaseSummary {summary_id: $sid})
                MATCH (d:DiseaseEntity {disease_id: $did})
                MERGE (s)-[:SUMMARIZES]->(d)
            """, sid=summary.summary_id, did=disease_slug)

            self._inc("summaries")

        print(f"[OK] Neo4j ingest complete for {disease_name}")

    # -- Main Pipeline --

    def run(self, pdf_path: str, disease_name: str,
            pre_extracted_text: str | None = None,
            skip_first_page: bool = False,
            source_type: str | None = None) -> dict:
        """Full ontology V2 pipeline: extract → type → resolve → assert → summarize → ingest."""
        self._stats = {}
        print(f"\n{'=' * 70}")
        print(f"  ONTOLOGY V2 INGEST: {disease_name}")
        print(f"  PDF: {pdf_path}")
        print(f"{'=' * 70}")

        # Phase 0: Experience advisory
        print("\n[Phase 0] Experience advisory (pre-run)...")
        pre_run_advisory = self._build_experience_advisory(disease_name)
        print(f"  Memory matches: {pre_run_advisory['match_count']}")
        for item in pre_run_advisory["recommendations"][:3]:
            print(f"    - {item}")

        # Phase 1: Chunk
        print("\n[Phase 1] Section-aware chunking...")
        if pre_extracted_text:
            chunks = self.extract_from_text(pre_extracted_text, disease_name, skip_first_page)
        else:
            chunks = self.extract_and_chunk(pdf_path, disease_name, skip_first_page)
        print(f"  {len(chunks)} chunks extracted")
        for stype in set(c.section_type for c in chunks):
            count = sum(1 for c in chunks if c.section_type == stype)
            print(f"    {stype}: {count} chunks")

        # Phase 2: Ontology-typed extraction + gleaning
        print("\n[Phase 2] Ontology-typed extraction (with gleaning)...")
        all_mentions: dict[str, list] = {"signs": [], "services": [], "observations": [], "assertions": []}
        for i, chunk in enumerate(chunks):
            extracted = self.extract_from_chunk(chunk, disease_name)
            for key in all_mentions:
                all_mentions[key].extend(extracted.get(key, []))
            n_items = sum(len(extracted.get(k, [])) for k in all_mentions)
            print(f"  [{i+1}/{len(chunks)}] {chunk.section_type:15s} | {chunk.section_title[:40]:40s} | {n_items} items")

        print(f"\n  Total extracted:")
        for key, items in all_mentions.items():
            if items:
                print(f"    {key}: {len(items)}")

        # Phase 2.5: Contextual experience advisory
        print("\n[Phase 2.5] Experience advisory (contextual)...")
        contextual_sign_terms = [item.mention_text for item in all_mentions["signs"][:24]]
        contextual_service_terms = [item.mention_text for item in all_mentions["services"][:24]]
        contextual_advisory = self._build_experience_advisory(
            disease_name,
            sign_terms=contextual_sign_terms,
            service_terms=contextual_service_terms,
        )
        print(f"  Contextual memory matches: {contextual_advisory['match_count']}")
        for item in contextual_advisory["recommendations"][:3]:
            print(f"    - {item}")

        # Phase 3: Canonical resolution
        print("\n[Phase 3] Canonical resolution...")
        mappings = self.resolve_canonical(all_mentions, disease_name)
        status_counts: dict[str, int] = {}
        for m in mappings.values():
            s = m.get("status", "unknown")
            status_counts[s] = status_counts.get(s, 0) + 1
        for status, count in sorted(status_counts.items()):
            print(f"    {status}: {count}")

        # Phase 4: Summary generation
        print("\n[Phase 4] Summary generation...")
        summary = self.generate_disease_summary(chunks, all_mentions, disease_name)
        print(f"  Summary: {len(summary.summary_text)} chars")
        print(f"  Key signs: {summary.key_signs[:5]}")
        print(f"  Key drugs: {summary.key_drugs[:5]}")

        # Phase 5: Neo4j ingest
        print("\n[Phase 5] Neo4j ingest (dual representation)...")
        self.create_indexes()
        self.ingest_to_neo4j(
            disease_name,
            chunks,
            all_mentions,
            mappings,
            summary,
            pdf_path=pdf_path,
            source_type=source_type,
        )

        experience_advisory = {
            "generated_at": datetime.now().astimezone().isoformat(),
            "namespace": self.namespace,
            "disease_name": disease_name,
            "pdf_path": pdf_path,
            "pre_run": pre_run_advisory,
            "contextual": contextual_advisory,
        }
        artifact_paths = self._write_experience_advisory_artifact(disease_name, pdf_path, experience_advisory)
        print(f"  Experience advisory: {artifact_paths['json']}")

        # Stats
        print(f"\n{'=' * 70}")
        print("ONTOLOGY V2 INGEST COMPLETE")
        print(f"  Disease: {disease_name}")
        for key, val in sorted(self._stats.items()):
            print(f"  {key}: {val}")
        print(f"{'=' * 70}")

        return {
            "disease_name": disease_name,
            "chunks": len(chunks),
            "signs": len(all_mentions["signs"]),
            "services": len(all_mentions["services"]),
            "observations": len(all_mentions["observations"]),
            "assertions": len(all_mentions["assertions"]),
            "mappings": status_counts,
            "stats": dict(self._stats),
            "experience_advisory": {
                "pre_run_match_count": pre_run_advisory["match_count"],
                "contextual_match_count": contextual_advisory["match_count"],
                "pre_run_recommendations": pre_run_advisory["recommendations"],
                "contextual_recommendations": contextual_advisory["recommendations"],
                "pre_run_by_category": pre_run_advisory["by_category"],
                "contextual_by_category": contextual_advisory["by_category"],
                "pre_run_by_severity": pre_run_advisory["by_severity"],
                "contextual_by_severity": contextual_advisory["by_severity"],
                "pre_run_by_kind": pre_run_advisory["by_kind"],
                "contextual_by_kind": contextual_advisory["by_kind"],
                "artifact_paths": artifact_paths,
            },
        }


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Ontology V2 Ingest Pipeline")
    parser.add_argument("pdf_path", help="Path to medical protocol PDF")
    parser.add_argument("--disease", required=True, help="Disease name (Vietnamese)")
    parser.add_argument("--namespace", default="ontology_v2", help="Graph namespace")
    parser.add_argument("--skip-first-page", action="store_true")
    args = parser.parse_args()

    pipeline = OntologyV2Ingest(namespace=args.namespace)
    try:
        result = pipeline.run(args.pdf_path, args.disease,
                              skip_first_page=args.skip_first_page)
        print(f"\nResult: {json.dumps(result, ensure_ascii=False, indent=2)}")
    except Exception:
        traceback.print_exc()
    finally:
        pipeline.close()
