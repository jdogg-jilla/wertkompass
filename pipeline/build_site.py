#!/usr/bin/env python3
"""Baut den Ordner _site/ für GitHub Pages: index.html bekommt das HTML-Grundgerüst,
die Aktiendaten werden daneben kopiert."""
import shutil
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SITE = ROOT / "_site"
shutil.rmtree(SITE, ignore_errors=True)
SITE.mkdir(parents=True)

body = (ROOT / "index.html").read_text(encoding="utf-8")
html = (
    '<!doctype html>\n<html lang="de">\n<head>\n<meta charset="utf-8">\n'
    '<meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover">\n'
    "<style>[hidden]{display:none!important}body{margin:0}img{max-width:100%}</style>\n"
    "</head>\n<body>\n" + body + "\n</body>\n</html>\n"
)
(SITE / "index.html").write_text(html, encoding="utf-8")
shutil.copytree(ROOT / "data", SITE / "data")
print("Seite gebaut in", SITE)
