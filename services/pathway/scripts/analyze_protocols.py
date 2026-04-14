import fitz
import sys
import os

def analyze_pdf(pdf_path):
    print(f"\nAnalysing: {pdf_path}")
    try:
        doc = fitz.open(pdf_path)
        print(f"Total Pages: {len(doc)}")
        
        # 1. Try TOC
        toc = doc.get_toc()
        if toc:
            print(f"TOC found: {len(toc)} items")
            for item in toc[:20]:
                print(f"  Level {item[0]}: {item[1]} (Page {item[2]})")
        else:
            print("No internal TOC found. Scanning for 'MỤC LỤC' in text...")
            # 2. Manual TOC Scan (first 10 pages)
            for i in range(min(10, len(doc))):
                text = doc[i].get_text()
                if "MỤC LỤC" in text.upper():
                    print(f"TOC found manually on Page {i+1}")
                    print(text[:1000]) # Preview
                    break
        
        # 3. Sample header format (Page 5)
        if len(doc) > 5:
            print(f"\nSample content (Page 5):\n{doc[4].get_text()[:500]}")
            
        doc.close()
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    pdfs = [
        r"D:\desktop_folder\01_claudecodeleak\roadmap_master_data\Phác đồ hen phế quản.pdf",
        r"D:\desktop_folder\01_claudecodeleak\roadmap_master_data\Phác đồ hô hấp.pdf",
        r"D:\desktop_folder\01_claudecodeleak\roadmap_master_data\Phác đồ tai mũi họng.pdf",
        r"D:\desktop_folder\01_claudecodeleak\roadmap_master_data\Phác đồ viêm phổi.pdf"
    ]
    for pdf in pdfs:
        analyze_pdf(pdf)
