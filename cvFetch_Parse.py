import re
import requests
from io import BytesIO
from pdfminer.high_level import extract_text

# No longer used
URL = "google.com"

def fetch_pdf_bytes(url: str) -> bytes:
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    return r.content

def normalize_text(s: str) -> str:
    if not s:
        return ""
    s = s.replace('\x00', '')
    s = s.replace('•', ' ')
    s = s.replace('-', ' ')
    s = s.replace('\n', ' ')
    s = re.sub(r'[ \t]+', ' ', s)
    s = re.sub(r'\s+', ' ', s)
    s = s.strip()
    return s

def pdf_to_text(url: str) -> str:
    pdf_bytes = fetch_pdf_bytes(url)
    text = extract_text(BytesIO(pdf_bytes))
    return normalize_text(text)

if __name__ == "__main__":
    text = pdf_to_text(URL)
    out_path = "resume_extracted.txt"
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(text)
    print(f"Wrote plain text to {out_path}")
