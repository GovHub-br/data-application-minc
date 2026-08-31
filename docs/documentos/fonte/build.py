"""Gera os HTML dos documentos de entrega e, em seguida, os PDFs.

Uso:
    python3 build.py            # todos os documentos
    python3 build.py 01 04      # só os documentos indicados
"""

from __future__ import annotations

import importlib
import re
import subprocess
import sys
from pathlib import Path

import diagrama
import render

AQUI = Path(__file__).resolve().parent
DESTINO_PDF = AQUI.parent

CHROME = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"

DOCUMENTOS = ["01", "02", "03", "04", "05", "06", "07", "08"]


def gerar_pdf(html: Path) -> Path:
    # a pasta usa nomes sem o prefixo numérico do arquivo-fonte
    nome = re.sub(r"^\d{2}-", "", html.stem)
    pdf = DESTINO_PDF / f"{nome}.pdf"
    subprocess.run(
        [
            CHROME,
            "--headless=new",
            "--disable-gpu",
            "--no-pdf-header-footer",
            f"--print-to-pdf={pdf}",
            html.as_uri(),
        ],
        check=True,
        capture_output=True,
    )
    return pdf


def main() -> None:
    alvos = sys.argv[1:] or DOCUMENTOS
    print("Gerando documentos de entrega\n")
    for numero in alvos:
        modulo = importlib.import_module(f"conteudo_{numero}")
        html = render.escrever(modulo.DOC, AQUI)
        pdf = gerar_pdf(html)
        tamanho = pdf.stat().st_size / 1024
        print(f"  {'→ ' + pdf.name:44s} {tamanho:6.0f} KB")

    if diagrama.AVISOS:
        print(f"\n{len(diagrama.AVISOS)} colisão(ões) de diagrama:")
        for aviso in diagrama.AVISOS:
            print(f"  ! {aviso}")
    print("\nConferência visual obrigatória: bash verificar.sh <slug>")


if __name__ == "__main__":
    main()
