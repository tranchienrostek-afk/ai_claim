"""
Multi-Disease PDF Splitter & Ingestor.

Handles PDFs containing multiple diseases (e.g., ENT guidelines with ~63 diseases).
Splits by disease headings, then delegates each to UniversalIngest.

Usage:
    cd notebooklm
    python multi_disease_ingest.py "7._HD_CD_TMH.pdf"

    # Or in Python:
    from multi_disease_ingest import MultiDiseaseIngest
    result = MultiDiseaseIngest.auto_ingest("7._HD_CD_TMH.pdf")
"""

import os
import re
import json
import sys
import io
import traceback
from datetime import datetime
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

import fitz  # PyMuPDF
from openai import AzureOpenAI
from neo4j import GraphDatabase
from pydantic import BaseModel
from runtime_env import load_notebooklm_env

from universal_ingest import (
    DocumentAnalyzer,
    DocumentProfile,
    PipelineConfigurator,
    IngestConfig,
    UniversalIngest,
    _slugify,
)

# Force UTF-8 output on Windows
if sys.stdout.encoding != 'utf-8':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
if sys.stderr.encoding != 'utf-8':
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

load_notebooklm_env()


# ---------------------------------------------------------------------------
# Data Models
# ---------------------------------------------------------------------------

class DiseaseSection(BaseModel):
    """One disease extracted from a multi-disease PDF."""
    disease_name: str
    icd_code: str = ""
    part_name: str = ""       # e.g. "Tai", "Mũi xoang", "Họng - Thanh quản"
    text_content: str = ""
    start_page: int = 0
    end_page: int = 0


# ---------------------------------------------------------------------------
# Phase 0: MultiDiseaseSplitter — TOC-based approach
# ---------------------------------------------------------------------------

# Page marker from PyMuPDF extraction
PAGE_MARKER = re.compile(r'^---\s*Page\s+(\d+)\s*---$', re.MULTILINE)

# Part heading in TOC: "Phần 1: Tai" or "Phần 5: Đầu mặt cổ"
TOC_PART_PATTERN = re.compile(r'^Phần\s+\d+:\s*(.+)', re.IGNORECASE)


class MultiDiseaseSplitter:
    """Split a multi-disease PDF into individual DiseaseSection objects.

    Strategy: Parse the Table of Contents (TOC) pages to get disease names
    and page numbers, then slice the full text by page boundaries.
    This is far more reliable than regex on body text.
    """

    def __init__(self, client: AzureOpenAI, model: str):
        self.client = client
        self.model = model

    def split(self, full_text: str, pdf_path: str) -> list[DiseaseSection]:
        """
        2-pass splitting:
          Pass 1: Parse TOC pages for disease names + page numbers
          Pass 2: LLM enriches with ICD-10 codes
        Then slice full_text by page boundaries.
        """
        # Pass 1: Parse TOC
        toc_entries = self._parse_toc(pdf_path)
        print(f"  [Pass 1] TOC parsed: {len(toc_entries)} diseases")

        if not toc_entries:
            print("  [WARN] No diseases found in TOC")
            return []

        # Build page→position map from full_text
        page_positions = {}  # page_num → char position
        for match in PAGE_MARKER.finditer(full_text):
            pnum = int(match.group(1))
            if pnum not in page_positions:
                page_positions[pnum] = match.start()

        # Slice text between disease page boundaries
        sections = []
        for i, entry in enumerate(toc_entries):
            start_page = entry['page']
            # End page = start of next disease, or end of text
            if i + 1 < len(toc_entries):
                end_page = toc_entries[i + 1]['page']
            else:
                end_page = max(page_positions.keys()) + 1 if page_positions else start_page + 10

            # Find text positions
            start_pos = page_positions.get(start_page, 0)
            end_pos = page_positions.get(end_page, len(full_text))

            text_content = full_text[start_pos:end_pos].strip()

            sections.append(DiseaseSection(
                disease_name=entry['name'],
                part_name=entry['part'],
                text_content=text_content,
                start_page=start_page,
                end_page=end_page - 1,
            ))

        # Pass 2: LLM enrichment (ICD codes)
        toc_text_for_llm = "\n".join(
            f"{i+1}. [{s.part_name}] {s.disease_name} (p.{s.start_page})"
            for i, s in enumerate(sections)
        )
        sections = self._llm_enrich(sections, toc_text_for_llm)
        print(f"  [Pass 2] LLM enriched {len(sections)} sections with ICD codes")

        return sections

    def _parse_toc(self, pdf_path: str) -> list[dict]:
        """Extract disease names and page numbers from TOC pages.

        TOC format (pages 9-10 in this PDF):
          Phần 1: Tai
          11
          Liệt dây thần kinh VII ngoại biên
          13
          Nghe kém ở trẻ em
          17
          ...
        """
        doc = fitz.open(pdf_path)
        total_pages = len(doc)

        # Find TOC pages — look for "MỤC LỤC" heading (including garbled variants)
        # Vietnamese docx→PDF conversion can garble: "MỤC  ỤC", "MUC LUC", "MỤC LỤC"
        toc_page_indices = []
        for i in range(min(15, total_pages)):
            text = doc[i].get_text('text')
            text_upper = text.upper()
            if ('MỤC LỤC' in text or 'MỤC  ỤC' in text or 'MUC LUC' in text_upper
                    or re.search(r'M[ỤU]C\s+L[ỤU]C', text_upper)):
                toc_page_indices.append(i)
                # TOC usually spans 2-3 pages
                if i + 1 < total_pages:
                    toc_page_indices.append(i + 1)
                if i + 2 < total_pages:
                    # Check if page i+2 also has disease-like entries
                    next_text = doc[i + 2].get_text('text')
                    lines = [l.strip() for l in next_text.split('\n') if l.strip()]
                    digit_lines = sum(1 for l in lines if l.isdigit())
                    if digit_lines >= 3:  # likely continuation of TOC
                        toc_page_indices.append(i + 2)
                break

        if not toc_page_indices:
            doc.close()
            return []

        # Extract TOC text
        toc_lines = []
        for idx in toc_page_indices:
            text = doc[idx].get_text('text')
            toc_lines.extend(l.strip() for l in text.split('\n') if l.strip())
        doc.close()

        # Parse: disease name followed by page number
        SKIP_LINES = {'MỤC LỤC', 'Lời giới thiệu', 'TÀI LIỆU THAM KHẢO'}
        diseases = []
        current_part = ''
        i = 0

        while i < len(toc_lines):
            line = toc_lines[i]

            # Skip known non-disease lines
            if line in SKIP_LINES:
                i += 1
                # Skip page number after skipped line
                if i < len(toc_lines) and toc_lines[i].isdigit():
                    i += 1
                continue

            # Skip standalone page numbers (page numbers of the TOC page itself)
            if line.isdigit() and int(line) <= 10:
                i += 1
                continue

            # Detect Part headings
            pm = TOC_PART_PATTERN.match(line)
            if pm:
                current_part = pm.group(1).strip()
                i += 1
                # Skip the part's page number
                if i < len(toc_lines) and toc_lines[i].isdigit():
                    i += 1
                continue

            # Disease entry: name line followed by page number line
            if i + 1 < len(toc_lines) and toc_lines[i + 1].isdigit():
                page_num = int(toc_lines[i + 1])
                # Page numbers in content start after TOC (typically > 10)
                if page_num > 10 and not line.isdigit():
                    diseases.append({
                        'name': line,
                        'part': current_part,
                        'page': page_num,
                    })
                    i += 2
                    continue

            i += 1

        return diseases

    def _llm_enrich(self, sections: list[DiseaseSection], toc_text: str) -> list[DiseaseSection]:
        """Single LLM call: add ICD-10 codes to each disease."""
        disease_list = "\n".join(
            f"{i+1}. {s.disease_name}" for i, s in enumerate(sections)
        )

        prompt = f"""Dưới đây là danh sách 63 bệnh lý từ tài liệu hướng dẫn chẩn đoán điều trị Tai Mũi Họng (BYT 2015).
Hãy thêm mã ICD-10 phù hợp nhất cho mỗi bệnh.

Danh sách:
{disease_list}

Trả về JSON:
{{
  "diseases": [
    {{"index": 1, "disease_name": "tên bệnh (giữ nguyên)", "icd_code": "mã ICD-10"}},
    ...
  ]
}}

Lưu ý:
- Giữ nguyên tên bệnh tiếng Việt, KHÔNG sửa
- ICD-10 cho TMH: Tai H60-H95, Mũi xoang J00-J34, Họng J02-J39, Thanh quản J04-J38, U C30-C32/C10-C14/D14
- Nếu không chắc ICD, dùng mã chương gần nhất"""

        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": "Bạn là hệ thống phân loại bệnh y khoa. Trả về JSON chính xác."},
                    {"role": "user", "content": prompt},
                ],
                response_format={"type": "json_object"},
                temperature=0.0,
            )
            data = json.loads(response.choices[0].message.content)
            llm_diseases = {d['index']: d for d in data.get('diseases', [])}

            for i, section in enumerate(sections):
                enrichment = llm_diseases.get(i + 1, {})
                if enrichment.get('icd_code'):
                    section.icd_code = enrichment['icd_code']

        except Exception as e:
            print(f"  [WARN] LLM enrichment failed: {e}")

        return sections


# ---------------------------------------------------------------------------
# MultiDiseaseIngest
# ---------------------------------------------------------------------------

class MultiDiseaseIngest:
    """Orchestrator for multi-disease PDF ingestion with parallel workers."""

    # Print lock for thread-safe console output
    _print_lock = threading.Lock()

    @classmethod
    def _safe_print(cls, msg: str):
        with cls._print_lock:
            print(msg, flush=True)

    @classmethod
    def _ingest_one_disease(
        cls, idx: int, total: int, section: DiseaseSection,
        base_config: IngestConfig, pdf_path: str, config_dir: Path, timestamp: str,
    ) -> dict:
        """Worker function: ingest a single disease. Each worker creates its own
        UniversalIngest instance (own API clients) to avoid thread-safety issues."""
        disease_tag = f"[{idx+1}/{total}] {section.disease_name}"
        cls._safe_print(f"  >> START {disease_tag}")

        # Each worker gets its own UniversalIngest (own OpenAI + Neo4j clients)
        ingestor = UniversalIngest()
        try:
            # Clone base config, override disease-specific fields
            config_data = base_config.model_dump()
            config_data['disease_name'] = section.disease_name
            config_data['icd_code'] = section.icd_code or base_config.icd_code
            config_data['protocol_name'] = f"HD Chẩn đoán Điều trị TMH - {section.disease_name}"
            config_data['skip_first_page'] = False
            config = IngestConfig(**config_data)

            # Save per-disease config
            slug = _slugify(section.disease_name)
            disease_config_path = config_dir / f"{slug}_{timestamp}.json"
            disease_config_path.write_text(
                config.model_dump_json(indent=2, ensure_ascii=False),
                encoding="utf-8",
            )

            result = ingestor.run(pdf_path, config, pre_extracted_text=section.text_content)
            cls._safe_print(f"  << DONE  {disease_tag} => {result.get('chunks', 0)} chunks")
            return result

        except Exception as e:
            cls._safe_print(f"  !! ERROR {disease_tag}: {e}")
            traceback.print_exc()
            return {
                "disease_name": section.disease_name,
                "status": "error",
                "error": str(e),
            }
        finally:
            ingestor.close()

    @classmethod
    def auto_ingest(cls, pdf_path: str, max_workers: int = 10,
                    source_type_override: str | None = None,
                    hospital_name_override: str | None = None,
                    force_vision: bool = False) -> dict:
        """
        Main entry point with parallel workers:
        1. Extract full text (vision-aware — auto-detects garbled Vietnamese fonts)
        2. Split into diseases (TOC + 1 LLM call)
        3. Analyze PDF (1 LLM call — shared DocumentProfile)
        4. Configure pipeline (3 LLM calls — shared IngestConfig)
        5. Ingest diseases in PARALLEL (N workers, each with own API clients)
        6. Create umbrella Protocol node
        """
        print(f"\n{'#' * 60}")
        print(f"  MULTI-DISEASE AUTO-INGEST (parallel, {max_workers} workers)")
        print(f"  PDF: {pdf_path}")
        print(f"{'#' * 60}\n")

        # Setup shared clients (for Phase 1-2 only)
        client = AzureOpenAI(
            api_key=os.getenv("AZURE_OPENAI_API_KEY"),
            api_version=os.getenv("AZURE_OPENAI_API_VERSION"),
            azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT"),
        )
        model = os.getenv("MODEL1", "gpt-4o-mini")

        # Step 1: Extract full text once (vision-aware — auto-detects garbled fonts)
        print(f"[Step 1] Extracting full text from PDF (vision-aware{', force_vision=True' if force_vision else ''})...")
        tmp_ingestor = UniversalIngest()
        full_text = tmp_ingestor.extract_text(pdf_path, force_vision=force_vision)
        tmp_ingestor.close()
        print(f"  Full text: {len(full_text)} chars")

        # Step 2: Split into diseases (TOC-based)
        print("\n[Step 2] Splitting PDF into diseases (TOC-based)...")
        splitter = MultiDiseaseSplitter(client, model)
        sections = splitter.split(full_text, pdf_path)

        if not sections:
            print("[ERROR] No diseases found in PDF. Falling back to single-disease ingest.")
            return UniversalIngest.auto_ingest(pdf_path)

        print(f"\n  Found {len(sections)} diseases:")
        for i, s in enumerate(sections[:10]):
            print(f"    {i+1}. {s.disease_name} ({s.icd_code}) — pp.{s.start_page}-{s.end_page}, {len(s.text_content)} chars")
        if len(sections) > 10:
            print(f"    ... and {len(sections) - 10} more")

        # Step 2.5: Skip diseases already ingested
        already_done = cls._get_ingested_diseases()
        pending = [(i, s) for i, s in enumerate(sections) if s.disease_name not in already_done]
        if len(pending) < len(sections):
            print(f"\n  Skipping {len(sections) - len(pending)} already-ingested diseases")
        print(f"  Pending: {len(pending)} diseases")

        if not pending:
            print("  All diseases already ingested!")
            return {"pdf_path": pdf_path, "total_diseases": len(sections),
                    "successful": len(sections), "errors": 0, "total_chunks": 0,
                    "results": [], "skipped": len(sections)}

        # Step 3: Analyze PDF (1 LLM call — shared profile)
        print("\n[Step 3] Analyzing PDF (shared profile)...")
        analyzer = DocumentAnalyzer(client, model)
        profile = analyzer.analyze(pdf_path)
        print(f"  Profile: {profile.disease_name} ({profile.icd_code}), {profile.medical_domain}")

        # Override source info from CLI if provided
        if source_type_override:
            profile.source_type = source_type_override
            profile.hospital_name = hospital_name_override

        # Step 4: Configure pipeline (3 LLM calls — shared config)
        print("\n[Step 4] Configuring shared pipeline...")
        configurator = PipelineConfigurator(client, model)
        base_config = configurator.configure(profile)

        # Save base config
        config_dir = Path(__file__).parent / "config" / "ingest_configs"
        config_dir.mkdir(exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        base_config_path = config_dir / f"multi_disease_base_{timestamp}.json"
        base_config_path.write_text(
            base_config.model_dump_json(indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
        print(f"  Base config saved: {base_config_path}")

        # Step 5: Ingest diseases in PARALLEL
        actual_workers = min(max_workers, len(pending))
        print(f"\n[Step 5] Ingesting {len(pending)} diseases with {actual_workers} parallel workers...")
        results = []
        total_chunks = 0

        with ThreadPoolExecutor(max_workers=actual_workers) as executor:
            futures = {}
            for idx, section in pending:
                future = executor.submit(
                    cls._ingest_one_disease,
                    idx, len(sections), section,
                    base_config, pdf_path, config_dir, timestamp,
                )
                futures[future] = section

            for future in as_completed(futures):
                section = futures[future]
                try:
                    result = future.result()
                    results.append(result)
                    total_chunks += result.get('chunks', 0)
                except Exception as e:
                    cls._safe_print(f"  !! WORKER CRASH for {section.disease_name}: {e}")
                    results.append({
                        "disease_name": section.disease_name,
                        "status": "error",
                        "error": str(e),
                    })

        # Step 6: Create umbrella Protocol node
        print("\n[Step 6] Creating umbrella Protocol node...")
        umbrella_ingestor = UniversalIngest()
        try:
            with umbrella_ingestor.driver.session() as session:
                year_str = f" {profile.year}" if profile.year else ""
                publisher_str = f" - {profile.publisher}" if profile.publisher else ""
                umbrella_name = f"HD Chẩn đoán Điều trị TMH{publisher_str}{year_str}"

                session.run(
                    "MERGE (p:Protocol {name: $name}) "
                    "ON CREATE SET p.type = 'multi_disease', p.disease_count = $count, p.created_at = datetime()",
                    name=umbrella_name, count=len(sections),
                )

                # Set source metadata on umbrella Protocol
                session.run(
                    "MATCH (p:Protocol {name: $name}) "
                    "SET p.source_type = $st, p.hospital_name = $hn",
                    name=umbrella_name,
                    st=profile.source_type,
                    hn=profile.hospital_name,
                )

                # If hospital → create :Hospital node + :FROM_HOSPITAL
                if profile.source_type == "hospital" and profile.hospital_name:
                    session.run(
                        "MERGE (h:Hospital {name: $hname})",
                        hname=profile.hospital_name,
                    )
                    session.run(
                        "MATCH (p:Protocol {name: $pn}) "
                        "MATCH (h:Hospital {name: $hn}) "
                        "MERGE (p)-[:FROM_HOSPITAL]->(h)",
                        pn=umbrella_name,
                        hn=profile.hospital_name,
                    )

                for section in sections:
                    session.run("""
                        MATCH (p:Protocol {name: $pname})
                        MATCH (d:Disease {name: $dname})
                        MERGE (p)-[:COVERS_DISEASE]->(d)
                    """, pname=umbrella_name, dname=section.disease_name)

            print(f"  Umbrella Protocol: {umbrella_name}")
        finally:
            umbrella_ingestor.close()

        # Summary
        success_count = sum(1 for r in results if r.get('status') == 'success')
        error_count = sum(1 for r in results if r.get('status') == 'error')

        print(f"\n{'=' * 60}")
        print("MULTI-DISEASE INGESTION COMPLETE")
        print(f"  Total diseases:  {len(sections)}")
        print(f"  Skipped (done):  {len(sections) - len(pending)}")
        print(f"  Processed:       {len(pending)}")
        print(f"  Successful:      {success_count}")
        print(f"  Errors:          {error_count}")
        print(f"  Total chunks:    {total_chunks}")
        print(f"  Workers used:    {actual_workers}")
        print(f"{'=' * 60}")

        return {
            "pdf_path": pdf_path,
            "total_diseases": len(sections),
            "successful": success_count,
            "errors": error_count,
            "total_chunks": total_chunks,
            "results": results,
        }

    @staticmethod
    def _get_ingested_diseases() -> set[str]:
        """Query Neo4j for disease names that already have Chunk nodes."""
        try:
            driver = GraphDatabase.driver(
                os.getenv("NEO4J_URI", os.getenv("neo4j_uri", "bolt://localhost:7688")),
                auth=(os.getenv("NEO4J_USER", os.getenv("neo4j_user", "neo4j")), os.getenv("NEO4J_PASSWORD", os.getenv("neo4j_password", "password123"))),
            )
            with driver.session() as s:
                result = s.run(
                    "MATCH (d:Disease)<-[:ABOUT_DISEASE]-(:Chunk) RETURN DISTINCT d.name AS name"
                )
                names = {r['name'] for r in result}
            driver.close()
            return names
        except Exception:
            return set()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Multi-disease PDF ingest with parallel workers")
    parser.add_argument("pdf", help="Path to PDF file")
    parser.add_argument("--workers", "-w", type=int, default=10,
                        help="Number of parallel workers (default: 10)")
    parser.add_argument("--source-type", choices=["BYT", "hospital"], default=None,
                        help="Source type override (default: auto-detect)")
    parser.add_argument("--hospital-name", default=None,
                        help="Hospital name (required if --source-type=hospital)")
    parser.add_argument("--force-vision", action="store_true",
                        help="Force vision OCR for all pages (useful for garbled font PDFs)")
    args = parser.parse_args()

    if not Path(args.pdf).exists():
        print(f"Error: File not found: {args.pdf}")
        sys.exit(1)

    if args.source_type == "hospital" and not args.hospital_name:
        print("Error: --hospital-name is required when --source-type=hospital")
        sys.exit(1)

    try:
        result = MultiDiseaseIngest.auto_ingest(
            args.pdf, max_workers=args.workers,
            source_type_override=args.source_type,
            hospital_name_override=args.hospital_name,
            force_vision=args.force_vision,
        )
        print(f"\nResult: {json.dumps(result, ensure_ascii=False, indent=2)}")
    except Exception:
        traceback.print_exc()
        sys.exit(1)
