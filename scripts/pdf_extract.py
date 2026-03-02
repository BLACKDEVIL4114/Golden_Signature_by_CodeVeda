import sys
import re
from pathlib import Path
from pdfminer.high_level import extract_text

def main() -> int:
    if len(sys.argv) < 2:
        print("Usage: python scripts/pdf_extract.py <pdf_path> [regex_filter]")
        return 2
    pdf_path = Path(sys.argv[1])
    pattern = sys.argv[2] if len(sys.argv) > 2 else None
    if not pdf_path.exists():
        print(f"Error: file not found: {pdf_path}")
        return 1
    text = extract_text(str(pdf_path))
    if pattern:
        rx = re.compile(pattern, re.IGNORECASE)
        lines = [ln for ln in text.splitlines() if rx.search(ln)]
        print("\n".join(lines))
    else:
        print(text)
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
