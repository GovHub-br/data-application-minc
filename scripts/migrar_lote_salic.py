#!/usr/bin/env python3
"""Migra um lote gerado do salic_dbt/bronze/<origem>/ para models/salic/.

Mecânico, e só o que a migração permite (ADR 0010/0016, plano de 2026-10-27):
  - git mv de cada <origem>__<tabela>.sql para salic/silver_<origem>_<tabela>.sql,
    trocando só a primeira linha de comentário (vira duas); o corpo SQL fica
    igual. Rode `poetry run sqlfmt` nos arquivos movidos depois: nomes longos
    fazem o cabeçalho passar de 88 colunas e o sqlfmt só reflui o comentário;
  - git mv do schema.yml para salic/schema_<origem>.yml, renomeando os blocos
    `- name:` e a frase "Camada bronze tipada" da descrição;
  - git mv do sources_<origem>.yml para salic/;
  - reescreve ref('<origem>__x') -> ref('silver_<origem>_x') em models/, tests/
    e macros/ do projeto dbt;
  - apaga a subpasta vazia.

Imprime tudo que fez e tudo que NÃO conseguiu fazer sozinho (bloco de schema
sem a frase padrão, por exemplo), para a pessoa terminar à mão.

Uso: python3 scripts/migrar_lote_salic.py controledeacesso
"""

from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[1]
PROJETO = RAIZ / "dbt" / "minc"
ORIGEM_DIR = PROJETO / "models" / "salic_dbt" / "bronze"
DESTINO = PROJETO / "models" / "salic"
ORIGENS = ("controledeacesso", "bdcorporativo", "agentes", "tabelas", "sac")

RE_HEADER = re.compile(
    r"^-- Bronze SALIC — (?P<tabela>\S+?)(?P<view> \(VIEW na origem\))?\.$", re.M
)
RE_DESC = re.compile(r"Camada bronze tipada a partir de\s+salic_bronze\.")
LINHA_ADR = "-- Lê salic_bronze, a Bronze do SALIC (ADR 0016)."
DESC_NOVA = (
    "Camada Silver tipada (ADR 0016: a Bronze é a source) a partir de salic_bronze."
)


def git_mv(de: Path, para: Path) -> None:
    subprocess.run(["git", "mv", str(de), str(para)], check=True, cwd=RAIZ)


def novo_nome(stem: str, origem: str) -> str:
    assert stem.startswith(f"{origem}__"), stem
    return f"silver_{origem}_{stem[len(origem) + 2:]}"


def migrar_sql(origem: str) -> dict[str, str]:
    renomes: dict[str, str] = {}
    for sql in sorted((ORIGEM_DIR / origem).glob("*.sql")):
        novo = novo_nome(sql.stem, origem)
        renomes[sql.stem] = novo
        destino = DESTINO / f"{novo}.sql"
        assert not destino.exists(), f"já existe {destino}"
        git_mv(sql, destino)
        texto = destino.read_text(encoding="utf-8")
        texto, n = RE_HEADER.subn(
            lambda m: f"-- Silver SALIC — {novo} (ex-{m.group('tabela')}"
            f"{m.group('view') or ''}).\n{LINHA_ADR}",
            texto,
            count=1,
        )
        if n != 1:
            print(f"  ATENÇÃO cabeçalho fora do padrão, revisar à mão: {destino.name}")
        destino.write_text(texto, encoding="utf-8")
    print(f"{len(renomes)} .sql movidos")
    return renomes


def migrar_schema(origem: str, renomes: dict[str, str]) -> None:
    de = ORIGEM_DIR / origem / "schema.yml"
    para = DESTINO / f"schema_{origem}.yml"
    git_mv(de, para)
    texto = para.read_text(encoding="utf-8")
    faltam = []
    for velho, novo in renomes.items():
        texto, n = re.subn(
            rf"^(  - name: ){re.escape(velho)}$", rf"\g<1>{novo}", texto, flags=re.M
        )
        if n != 1:
            faltam.append(velho)
    texto, n_desc = RE_DESC.subn(DESC_NOVA, texto)
    para.write_text(texto, encoding="utf-8")
    print(
        f"schema: {len(renomes) - len(faltam)} blocos renomeados, "
        f"{n_desc} descrições ajustadas"
    )
    if faltam:
        print(f"  ATENÇÃO blocos não encontrados no schema.yml: {faltam}")
    if n_desc != len(renomes):
        print(
            f"  ATENÇÃO {len(renomes) - n_desc} descrição(ões) sem a frase padrão; "
            "revisar à mão"
        )


def migrar_sources(origem: str) -> None:
    de = ORIGEM_DIR / f"sources_{origem}.yml"
    if de.exists():
        git_mv(de, DESTINO / de.name)
        print(f"sources_{origem}.yml movido")


def reescrever_refs(renomes: dict[str, str]) -> None:
    alvos = (
        list((PROJETO / "models").rglob("*.sql"))
        + list((PROJETO / "tests").glob("*.sql"))
        + list((PROJETO / "macros").rglob("*.sql"))
    )
    nomes = "|".join(map(re.escape, renomes))
    padrao = re.compile(r"""ref\(\s*(['"])(""" + nomes + r""")\1\s*\)""")
    for arq in alvos:
        texto = arq.read_text(encoding="utf-8")
        novo, n = padrao.subn(
            lambda m: f"ref({m.group(1)}{renomes[m.group(2)]}{m.group(1)})", texto
        )
        if n:
            arq.write_text(novo, encoding="utf-8")
            print(f"  {n} ref() reescrito(s) em {arq.relative_to(RAIZ)}")


def main(argv: list[str]) -> int:
    origem = argv[1] if len(argv) > 1 else ""
    if origem not in ORIGENS:
        print(f"uso: {argv[0]} <{'|'.join(ORIGENS)}>")
        return 2
    renomes = migrar_sql(origem)
    migrar_schema(origem, renomes)
    migrar_sources(origem)
    reescrever_refs(renomes)
    pasta = ORIGEM_DIR / origem
    sobras = [p.name for p in pasta.iterdir()] if pasta.exists() else []
    if sobras:
        print(f"  ATENÇÃO sobrou em {pasta}: {sobras}")
    elif pasta.exists():
        pasta.rmdir()
        print(f"{pasta.relative_to(RAIZ)} apagada")
    else:
        print(f"{pasta.relative_to(RAIZ)} já não existe (git mv apagou)")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
