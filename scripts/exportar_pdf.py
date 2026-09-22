"""Convierte un Markdown de docs/ a PDF (Markdown -> HTML -> Chrome headless).

Uso: uv run --with markdown python scripts/exportar_pdf.py docs/documento_tecnico.md
"""

from __future__ import annotations

import shutil
import subprocess
import sys
from pathlib import Path

import markdown

CSS = """
body { font-family: 'DejaVu Sans', Arial, sans-serif; font-size: 10.5pt; line-height: 1.4;
       max-width: 19cm; margin: 0 auto; color: #222; }
h1 { font-size: 20pt; border-bottom: 2px solid #444; padding-bottom: 4px; }
h2 { font-size: 15pt; margin-top: 1.6em; border-bottom: 1px solid #bbb; }
h3 { font-size: 12pt; margin-top: 1.2em; }
table { border-collapse: collapse; width: 100%; font-size: 9pt; margin: 0.6em 0;
        page-break-inside: auto; }
th, td { border: 1px solid #999; padding: 4px 6px; vertical-align: top; text-align: left; }
th { background: #eee; }
tr { page-break-inside: avoid; }
code { font-family: 'DejaVu Sans Mono', monospace; font-size: 8.8pt; background: #f3f3f3;
       padding: 0 2px; }
pre { background: #f3f3f3; border: 1px solid #ddd; padding: 8px; font-size: 8.5pt;
      overflow-x: auto; white-space: pre-wrap; }
pre code { background: none; padding: 0; }
img { max-width: 100%; }
blockquote { border-left: 3px solid #bbb; margin: 0; padding-left: 10px; color: #555; }
@page { size: A4; margin: 18mm 16mm; }
"""


def main() -> None:
    origen = Path(sys.argv[1])
    destino = origen.with_suffix(".pdf")
    html = markdown.markdown(
        origen.read_text(encoding="utf-8"),
        extensions=["tables", "fenced_code", "toc", "sane_lists"],
    )
    pagina = origen.with_suffix(".html")
    pagina.write_text(
        f"<!doctype html><html lang='es'><head><meta charset='utf-8'><style>{CSS}</style></head>"
        f"<body>{html}</body></html>",
        encoding="utf-8",
    )
    chrome = next(
        (c for c in map(shutil.which, ("google-chrome", "chromium", "chromium-browser")) if c), None
    )
    if chrome is None:
        raise SystemExit("no se encontró Chrome/Chromium para imprimir el PDF")
    subprocess.run(
        [
            chrome,
            "--headless=new",
            "--no-sandbox",
            "--disable-gpu",
            "--no-pdf-header-footer",
            f"--print-to-pdf={destino.resolve()}",
            pagina.resolve().as_uri(),
        ],
        check=True,
        timeout=120,
        capture_output=True,
    )
    pagina.unlink()
    print(f"PDF generado: {destino} ({destino.stat().st_size // 1024} KiB)")


if __name__ == "__main__":
    main()
