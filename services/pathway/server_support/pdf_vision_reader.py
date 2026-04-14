"""
PDFVisionReader — Extract text from Vietnamese medical PDFs using GPT vision.

Inspired by Claude Code's PDF handling:
  Path A: Model reads PDF directly (small files)
  Path B: PDF → JPEG images → model reads via vision (large files, complex layouts)

Pathway's approach:
  1. PyMuPDF text extraction first (fast, free)
  2. Detect low-quality pages (tables, scanned images, broken text)
  3. For low-quality pages: render → JPEG → GPT-5-mini vision extraction
  4. Merge results: PyMuPDF text + vision-enhanced pages

Why not just PyMuPDF?
  - Docx→PDF files often have weird font encodings
  - Vietnamese diacritics get mangled in some PDF generators
  - Tables and multi-column layouts lose structure
  - Scanned PDFs have zero extractable text

Usage:
    from server_support.pdf_vision_reader import PDFVisionReader

    reader = PDFVisionReader()
    result = reader.extract("phac_do_hen_phe_quan.pdf")
    print(result.full_text)
    print(f"Pages: {result.total_pages}, Vision-enhanced: {len(result.vision_pages)}")

    # Or just specific pages:
    result = reader.extract("big_protocol.pdf", pages=range(1, 11))
"""

from __future__ import annotations

import base64
import io
import os
import re
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import fitz  # PyMuPDF

# Lazy import — only needed if vision path is used
_openai_client = None


def _get_openai_client():
    global _openai_client
    if _openai_client is None:
        from openai import AzureOpenAI
        _openai_client = AzureOpenAI(
            azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT", "").strip(),
            api_key=os.getenv("AZURE_OPENAI_API_KEY", "").strip(),
            api_version=os.getenv("AZURE_OPENAI_API_VERSION", "2024-06-01").strip(),
        )
    return _openai_client


# ---------------------------------------------------------------------------
# Quality heuristics — detect pages where PyMuPDF text is unreliable
# ---------------------------------------------------------------------------

# Vietnamese diacritics that should appear in valid Vietnamese text
_VN_DIACRITICS = set("àáảãạăắằẳẵặâấầẩẫậèéẻẽẹêếềểễệìíỉĩịòóỏõọôốồổỗộơớờởỡợùúủũụưứừửữựỳýỷỹỵđ")

# Minimum ratio of alphabetic chars in a "text" page
_MIN_ALPHA_RATIO = 0.3

# Minimum Vietnamese diacritics per 100 chars for Vietnamese text
_MIN_VN_DENSITY = 1.5

# Below this char count, page is likely image-only or near-empty
_MIN_CHARS_PER_PAGE = 50

# Garbled text indicators
_GARBLED_PATTERNS = [
    re.compile(r'[\ufffd\ufffe\uffff]{2,}'),           # replacement chars
    re.compile(r'[^\x00-\x7f\u0080-\u024f\u1e00-\u1eff\u0300-\u036f]{5,}'),  # long runs of unusual unicode
    re.compile(r'(\w)\1{4,}'),                           # repeated chars aaaaa
    re.compile(r'[A-Z]{20,}'),                           # very long uppercase runs
]

# Common docx→pdf font substitution errors in Vietnamese
# Ƣ (U+01A2) = garbled Ư, Ơ missing diacritics, etc.
_FONT_GARBLE_CHARS = set("ƢƣȪȫ")

# Known garbled→correct mappings from docx→pdf conversion
_FONT_FIX_MAP = {
    "Ƣ": "Ư", "ƣ": "ư",
    "Ơ ": "Ớ",  # Ơ followed by space often = Ớ with lost combining mark
}


@dataclass
class PageResult:
    """Extraction result for a single page."""
    page_num: int           # 1-based
    text: str
    method: str             # "pymupdf" | "vision" | "vision_fallback"
    quality_score: float    # 0.0–1.0, how good the text looks
    char_count: int = 0
    has_images: bool = False
    vision_cost_ms: int = 0


@dataclass
class PDFExtractionResult:
    """Complete extraction result."""
    file_path: str
    file_name: str
    total_pages: int
    pages: list[PageResult] = field(default_factory=list)
    vision_pages: list[int] = field(default_factory=list)  # pages that needed vision
    total_ms: int = 0
    errors: list[str] = field(default_factory=list)

    @property
    def full_text(self) -> str:
        """All pages concatenated."""
        parts = []
        for p in self.pages:
            parts.append(f"--- Trang {p.page_num} ---")
            parts.append(p.text)
        return "\n\n".join(parts)

    @property
    def page_texts(self) -> dict[int, str]:
        """Dict of page_num → text."""
        return {p.page_num: p.text for p in self.pages}

    def text_for_pages(self, page_range: range | list[int]) -> str:
        """Get text for specific pages."""
        nums = set(page_range)
        return "\n\n".join(
            f"--- Trang {p.page_num} ---\n{p.text}"
            for p in self.pages if p.page_num in nums
        )

    def summary(self) -> dict[str, Any]:
        return {
            "file": self.file_name,
            "total_pages": self.total_pages,
            "extracted_pages": len(self.pages),
            "vision_enhanced_pages": len(self.vision_pages),
            "total_chars": sum(p.char_count for p in self.pages),
            "avg_quality": round(
                sum(p.quality_score for p in self.pages) / max(len(self.pages), 1), 2
            ),
            "total_ms": self.total_ms,
            "errors": self.errors,
        }


def _assess_text_quality(text: str) -> float:
    """Score 0.0–1.0: how readable/valid this extracted text is."""
    if not text or len(text.strip()) < _MIN_CHARS_PER_PAGE:
        return 0.0

    score = 1.0
    total_chars = len(text)

    # Check alphabetic ratio
    alpha_count = sum(1 for c in text if c.isalpha())
    alpha_ratio = alpha_count / total_chars
    if alpha_ratio < _MIN_ALPHA_RATIO:
        score -= 0.3

    # Check Vietnamese diacritics density
    vn_count = sum(1 for c in text.lower() if c in _VN_DIACRITICS)
    vn_density = (vn_count / total_chars) * 100
    if vn_density < _MIN_VN_DENSITY:
        score -= 0.2  # might be non-Vietnamese or garbled

    # Check for garbled patterns
    for pattern in _GARBLED_PATTERNS:
        if pattern.search(text):
            score -= 0.25
            break

    # Check for docx→pdf font substitution errors (Ƣ = garbled Ư, etc.)
    garble_count = sum(1 for c in text if c in _FONT_GARBLE_CHARS)
    if garble_count > 0:
        score -= min(0.6, garble_count * 0.15)  # font garble = vision needed

    # Check for missing chars: "PH QUẢN" should be "PHẾ QUẢN", "MỤC ỤC" = "MỤC LỤC"
    # Two consecutive uppercase words where second starts with space = likely missing char
    missing_char_count = len(re.findall(r'[A-ZĐ] {2,}[A-ZĐa-zđ]', text))
    if missing_char_count > 0:
        score -= min(0.3, missing_char_count * 0.1)

    # Check word-like token ratio
    tokens = text.split()
    if tokens:
        short_tokens = sum(1 for t in tokens if len(t) <= 1)
        if short_tokens / len(tokens) > 0.4:
            score -= 0.2  # too many single-char "words"

    return max(0.0, min(1.0, score))


def _page_has_images(page: fitz.Page) -> bool:
    """Check if page contains significant image content."""
    images = page.get_images(full=True)
    if not images:
        return False
    # Check if images cover significant area
    page_area = page.rect.width * page.rect.height
    for img in images:
        try:
            xref = img[0]
            img_rect = page.get_image_rects(xref)
            if img_rect:
                for rect in img_rect:
                    img_area = rect.width * rect.height
                    if img_area / page_area > 0.3:  # image covers >30% of page
                        return True
        except Exception:
            pass
    return len(images) >= 3  # multiple images = likely image-heavy


def _render_page_jpeg(page: fitz.Page, dpi: int = 150) -> bytes:
    """Render a PDF page to JPEG bytes."""
    mat = fitz.Matrix(dpi / 72, dpi / 72)
    pix = page.get_pixmap(matrix=mat, alpha=False)
    return pix.tobytes("jpeg", jpg_quality=85)


def _vision_extract_page(jpeg_bytes: bytes, page_num: int,
                          model: str = None) -> tuple[str, int]:
    """Send page image to GPT-5-mini vision for text extraction.

    Returns (extracted_text, duration_ms).
    """
    client = _get_openai_client()
    model = model or os.getenv("MODEL2", "gpt-5-mini").strip()

    b64 = base64.b64encode(jpeg_bytes).decode("utf-8")

    t0 = time.time()
    # GPT-5-mini uses max_completion_tokens; older models use max_tokens
    token_param = "max_completion_tokens" if "gpt-5" in model or "gpt-4o" in model else "max_tokens"
    response = client.chat.completions.create(
        model=model,
        messages=[{
            "role": "user",
            "content": [
                {
                    "type": "text",
                    "text": (
                        "Trích xuất TOÀN BỘ nội dung văn bản tiếng Việt từ trang tài liệu y khoa này. "
                        "Giữ nguyên cấu trúc: tiêu đề, danh sách, bảng (dùng markdown table), "
                        "số liệu, đơn vị, tên thuốc, liều lượng. "
                        "Nếu có bảng, chuyển thành markdown table. "
                        "Chỉ trả về nội dung văn bản, không thêm bình luận."
                    ),
                },
                {
                    "type": "image_url",
                    "image_url": {
                        "url": f"data:image/jpeg;base64,{b64}",
                        "detail": "high",
                    },
                },
            ],
        }],
        **{token_param: 4096},
    )
    text = response.choices[0].message.content or ""
    ms = int((time.time() - t0) * 1000)
    return text.strip(), ms


class PDFVisionReader:
    """Extract text from Vietnamese medical PDFs with vision fallback.

    Strategy:
      1. PyMuPDF extracts text from all pages (fast, free)
      2. Assess quality of each page's text
      3. Pages with quality < threshold → re-extract via GPT vision
      4. Return merged result with per-page provenance
    """

    def __init__(self, quality_threshold: float = 0.5,
                 vision_dpi: int = 150,
                 model: str = None,
                 max_vision_pages: int = 50):
        """
        Args:
            quality_threshold: below this score, use vision extraction
            vision_dpi: DPI for rendering pages to JPEG
            model: Azure OpenAI model name (default: MODEL2 env var)
            max_vision_pages: max pages to send to vision (cost control)
        """
        self.quality_threshold = quality_threshold
        self.vision_dpi = vision_dpi
        self.model = model or os.getenv("MODEL2", "gpt-5-mini").strip()
        self.max_vision_pages = max_vision_pages

    def extract(self, pdf_path: str | Path,
                pages: range | list[int] | None = None,
                force_vision: bool = False) -> PDFExtractionResult:
        """Extract text from PDF.

        Args:
            pdf_path: path to PDF file
            pages: specific pages to extract (1-based), None = all
            force_vision: skip PyMuPDF, use vision for all pages
        """
        t0 = time.time()
        pdf_path = Path(pdf_path)

        if not pdf_path.exists():
            return PDFExtractionResult(
                file_path=str(pdf_path),
                file_name=pdf_path.name,
                total_pages=0,
                errors=[f"File not found: {pdf_path}"],
            )

        try:
            doc = fitz.open(str(pdf_path))
        except Exception as e:
            return PDFExtractionResult(
                file_path=str(pdf_path),
                file_name=pdf_path.name,
                total_pages=0,
                errors=[f"Cannot open PDF: {e}"],
            )

        total_pages = len(doc)
        target_pages = list(pages) if pages else list(range(1, total_pages + 1))
        # Clamp to valid range
        target_pages = [p for p in target_pages if 1 <= p <= total_pages]

        result = PDFExtractionResult(
            file_path=str(pdf_path),
            file_name=pdf_path.name,
            total_pages=total_pages,
        )

        # Phase 1: PyMuPDF extraction for all target pages
        pymupdf_results: dict[int, PageResult] = {}
        needs_vision: list[int] = []

        for page_num in target_pages:
            page = doc[page_num - 1]  # 0-based index

            if force_vision:
                needs_vision.append(page_num)
                continue

            # Extract text with layout preservation
            blocks = page.get_text("blocks")
            blocks.sort(key=lambda b: (b[1], b[0]))  # top→bottom, left→right
            text = "\n".join(b[4].strip() for b in blocks if b[4].strip())

            has_imgs = _page_has_images(page)
            quality = _assess_text_quality(text)
            char_count = len(text)

            pr = PageResult(
                page_num=page_num,
                text=text,
                method="pymupdf",
                quality_score=quality,
                char_count=char_count,
                has_images=has_imgs,
            )
            pymupdf_results[page_num] = pr

            # Decide if vision is needed
            if quality < self.quality_threshold:
                needs_vision.append(page_num)
            elif has_imgs and char_count < _MIN_CHARS_PER_PAGE * 2:
                needs_vision.append(page_num)

        # Phase 2: Vision extraction for low-quality pages
        vision_count = 0
        if needs_vision:
            # Cost control
            if len(needs_vision) > self.max_vision_pages:
                result.errors.append(
                    f"Too many pages need vision ({len(needs_vision)}), "
                    f"capping at {self.max_vision_pages}"
                )
                needs_vision = needs_vision[:self.max_vision_pages]

            for page_num in needs_vision:
                try:
                    page = doc[page_num - 1]
                    jpeg_bytes = _render_page_jpeg(page, dpi=self.vision_dpi)
                    vision_text, vision_ms = _vision_extract_page(
                        jpeg_bytes, page_num, model=self.model
                    )

                    if vision_text and len(vision_text) > _MIN_CHARS_PER_PAGE:
                        vision_quality = _assess_text_quality(vision_text)
                        pymupdf_quality = pymupdf_results.get(page_num, PageResult(
                            page_num=page_num, text="", method="", quality_score=0.0
                        )).quality_score

                        # Use vision result if it's better
                        if vision_quality > pymupdf_quality or force_vision:
                            pymupdf_results[page_num] = PageResult(
                                page_num=page_num,
                                text=vision_text,
                                method="vision" if force_vision else "vision_fallback",
                                quality_score=vision_quality,
                                char_count=len(vision_text),
                                has_images=_page_has_images(page),
                                vision_cost_ms=vision_ms,
                            )
                            vision_count += 1
                            result.vision_pages.append(page_num)
                except Exception as e:
                    result.errors.append(f"Vision extraction failed for page {page_num}: {e}")

        # Assemble final result in page order
        for page_num in target_pages:
            if page_num in pymupdf_results:
                result.pages.append(pymupdf_results[page_num])

        doc.close()
        result.total_ms = int((time.time() - t0) * 1000)
        return result

    def extract_to_text_file(self, pdf_path: str | Path,
                              output_path: str | Path = None,
                              pages: range | list[int] | None = None,
                              force_vision: bool = False) -> Path:
        """Extract and save to .txt file. Returns output path."""
        result = self.extract(pdf_path, pages=pages, force_vision=force_vision)

        if output_path is None:
            pdf_p = Path(pdf_path)
            output_path = pdf_p.parent / f"{pdf_p.stem}_extracted.txt"

        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        with open(output_path, "w", encoding="utf-8") as f:
            f.write(f"# Extracted from: {result.file_name}\n")
            f.write(f"# Total pages: {result.total_pages}\n")
            f.write(f"# Vision-enhanced pages: {result.vision_pages}\n")
            f.write(f"# Extraction time: {result.total_ms}ms\n\n")
            f.write(result.full_text)

        return output_path


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import json

    if len(sys.argv) < 2:
        print("Usage: python -m server_support.pdf_vision_reader <pdf_path> [--force-vision] [--pages 1-10]")
        sys.exit(1)

    pdf_file = sys.argv[1]
    force = "--force-vision" in sys.argv
    page_range = None

    for i, arg in enumerate(sys.argv):
        if arg == "--pages" and i + 1 < len(sys.argv):
            spec = sys.argv[i + 1]
            if "-" in spec:
                start, end = spec.split("-", 1)
                page_range = list(range(int(start), int(end) + 1))
            else:
                page_range = [int(spec)]

    # Load env
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    try:
        from runtime_env import load_notebooklm_env
        load_notebooklm_env()
    except ImportError:
        from dotenv import load_dotenv
        load_dotenv(Path(__file__).resolve().parents[1] / ".env")

    reader = PDFVisionReader()
    result = reader.extract(pdf_file, pages=page_range, force_vision=force)

    print(json.dumps(result.summary(), ensure_ascii=False, indent=2))
    print(f"\n{'='*60}")
    print(result.full_text[:3000])
    if len(result.full_text) > 3000:
        print(f"\n... ({len(result.full_text)} total chars)")
