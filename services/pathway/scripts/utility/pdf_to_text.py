import fitz  # PyMuPDF
from pathlib import Path

NOTEBOOKLM_DIR = Path(__file__).resolve().parents[2]


class PDFToText:
    def __init__(self, output_dir=None):
        self.output_dir = Path(output_dir) if output_dir else NOTEBOOKLM_DIR / "data" / "extracted_text"
        self.output_dir.mkdir(exist_ok=True)

    def extract(self, pdf_path):
        """
        Extracts text from a PDF file and saves it to a .txt file.
        Uses layout preservation for better clinical data integrity.
        """
        pdf_path = Path(pdf_path)
        if not pdf_path.exists():
            print(f"Error: File {pdf_path} does not exist.")
            return None

        print(f"Processing: {pdf_path.name}...")
        try:
            doc = fitz.open(pdf_path)
            full_text = []
            
            for page_num, page in enumerate(doc):
                # Using 'blocks' to better handle multi-column layouts often found in medical journals/protocols
                blocks = page.get_text("blocks")
                # Sort blocks: top-to-bottom, then left-to-right
                blocks.sort(key=lambda b: (b[1], b[0]))
                
                page_text = f"--- Page {page_num + 1} ---\n"
                for b in blocks:
                    page_text += b[4] + "\n"
                full_text.append(page_text)
            
            output_file = self.output_dir / f"{pdf_path.stem}.txt"
            with open(output_file, "w", encoding="utf-8") as f:
                f.write("\n".join(full_text))
            
            print(f"Successfully extracted to: {output_file}")
            return output_file
        except Exception as e:
            print(f"Failed to extract {pdf_path.name}: {e}")
            return None

if __name__ == "__main__":
    import sys
    
    # Standalone execution in /notebooklm
    target = (
        sys.argv[1]
        if len(sys.argv) > 1
        else str(NOTEBOOKLM_DIR / "assets" / "reference_pdfs" / "phac-do-dieu-tri-mat-ngu-theo-yhct-2023.pdf")
    )
    
    converter = PDFToText()
    converter.extract(target)
