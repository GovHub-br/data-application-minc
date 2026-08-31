#!/usr/bin/env bash
# Rasteriza um PDF gerado e conta as páginas, para a conferência visual
# obrigatória descrita em references/print-pages.md da skill de identidade
# visual: `overflow: hidden` descarta em silêncio o que não couber na página.
set -euo pipefail
SLUG="${1:?uso: bash verificar.sh <slug-sem-extensao>}"
OUT="${2:-/tmp/gh-verificacao/$SLUG}"
mkdir -p "$OUT"
rm -f "$OUT"/pagina-*.png
pdftoppm -png -r 90 "../$SLUG.pdf" "$OUT/pagina"
echo "$(ls "$OUT"/pagina-*.png | wc -l | tr -d ' ') páginas em $OUT"
