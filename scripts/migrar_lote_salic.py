#!/usr/bin/env python3
"""Migra um lote gerado do salic_dbt/bronze/<origem>/ para models/salic/ (ADR 0017).

Mecânico, e só o que a migração permite (ADR 0010/0017, plano de 2026-10-27):
  - git mv de cada <origem>__<tabela>.sql para salic/silver_<origem>_<tabela>.sql.
    No arquivo mudam só o cabeçalho de comentário, a menção
    salic_bronze.<origem>__<t> (vira salic.bronze_<origem>_<t>) e a linha do
    source(): source("bronze_<origem>", "<origem>__<t>") vira
    source("salic", "bronze_<origem>_<t>"). O resto do SQL fica igual. Rode
    `poetry run sqlfmt` nos movidos depois: nome longo faz o cabeçalho passar
    de 88 colunas e o sqlfmt só reflui comentário e a linha do from;
  - git mv do schema.yml para salic/schema_<origem>.yml, renomeando os blocos
    `- name:` e a frase "Camada bronze tipada a partir de salic_bronze.x";
  - git mv do sources_<origem>.yml para salic/, com name: salic, schema: salic
    e cada tabela <origem>__<t> renomeada para bronze_<origem>_<t>. Se já
    existe um salic/sources_<origem>.yml (o mínimo do PR B, no caso do sac),
    o movido o substitui;
  - reescreve ref('<origem>__x') -> ref('silver_<origem>_x') e
    source('bronze_<origem>', '<origem>__x') -> source('salic', 'bronze_<origem>_x')
    nos .sql de models/, tests/ e macros/ e nos .yml de models/ (os
    relationships dos schema.yml), porque a source bronze_<origem> deixa de
    existir;
  - apaga a subpasta vazia.

Imprime tudo que fez e tudo que NÃO conseguiu fazer sozinho (bloco de schema
sem a frase padrão, source() que sobrou com a source velha), para a pessoa
terminar à mão.

Antes de mexer em qualquer arquivo, para com erro se algum nome novo de modelo
passar de 63 caracteres, o limite do Postgres: esses precisam de nome à mão.

Uso: python3 scripts/migrar_lote_salic.py controledeacesso

Lote parcial, para um banco grande demais para um PR só (o sac): só as tabelas
listadas migram. Os blocos delas saem do schema.yml e do sources_<origem>.yml
legados, que ficam com o resto; o schema vai para salic/schema_<origem>_<assunto>.yml
e as tabelas entram no salic/sources_<origem>.yml (a versão do legado substitui
uma já declarada lá):

    python3 scripts/migrar_lote_salic.py sac --assunto nucleo --tabelas aprovacao,captacao
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
LIMITE_NOME = 63

RE_HEADER = re.compile(
    r"^-- Bronze SALIC — (?P<tabela>\S+?)(?P<view> \(VIEW na origem\))?\.$", re.M
)
RE_DESC = re.compile(r"Camada\s+bronze\s+tipada")
DESC_NOVA = "Camada Silver tipada"
LINHA_FONTE = (
    "-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>."
)
# A source é uma só (salic), declarada em vários sources_<origem>.yml. A descrição
# por origem ficaria pendurada na source inteira; vai a mesma em todos.
DESC_SOURCE = """    description: >
      SALIC (Sistema de Apoio às Leis de Incentivo à Cultura). Bronze: réplica do
      sistema de origem via Trino, tudo text, sem tratamento, com a coluna técnica
      _fatia da carga em fatias. Uma tabela por <banco>__<tabela> da origem, como
      salic.bronze_<banco>_<tabela>. As descrições das tabelas vêm do dicionário
      de dados do SALIC (export SchemaSpy) cruzado com o perfil do banco; o
      catálogo completo está em dbt/minc/docs/salic/.
"""
RE_DESC_SOURCE = re.compile(
    r"^    description: >\n(?:      .*\n)+?(?=    tables:$)", re.M
)


def git(*args: str) -> None:
    subprocess.run(["git", *args], check=True, cwd=RAIZ)


def novo_nome(stem: str, origem: str) -> str:
    assert stem.startswith(f"{origem}__"), stem
    return f"silver_{origem}_{stem[len(origem) + 2:]}"


def nomes_longos(origem: str, tabelas: set[str] | None = None) -> list[tuple[str, str]]:
    """Modelos cujo nome novo passa do limite do Postgres, com o nome velho."""
    return sorted(
        (p.stem, novo)
        for p in (ORIGEM_DIR / origem).glob("*.sql")
        if (tabelas is None or p.stem[len(origem) + 2 :] in tabelas)
        and len(novo := novo_nome(p.stem, origem)) > LIMITE_NOME
    )


def nome_bronze(origem: str, tabela: str) -> str:
    """``bronze_<origem>_<tabela>``, o mesmo que plugins.trino_bronze gera."""
    return f"bronze_{origem}_{tabela}"


def re_source(origem: str) -> re.Pattern[str]:
    return re.compile(
        r"source\(\s*(['\"])bronze_"
        + re.escape(origem)
        + r"\1\s*,\s*(['\"])"
        + re.escape(origem)
        + r"__(?P<t>[^'\"]+)\2\s*\)"
    )


def trocar_source(
    texto: str, origem: str, tabelas: set[str] | None = None
) -> tuple[str, int]:
    """Troca a source velha pela salic; com ``tabelas``, só a dessas tabelas."""

    def sub(m: re.Match[str]) -> str:
        if tabelas is not None and m.group("t") not in tabelas:
            return m.group(0)
        q = m.group(1)
        return f"source({q}salic{q}, {q}{nome_bronze(origem, m.group('t'))}{q})"

    novo = re_source(origem).sub(sub, texto)
    return novo, sum(a != b for a, b in zip(texto.split("\n"), novo.split("\n")))


def trocar_mencao(texto: str, origem: str) -> str:
    """``salic_bronze.<origem>__x`` em comentário vira ``salic.bronze_<origem>_x``."""
    return re.sub(
        r"salic_bronze\." + re.escape(origem) + r"__",
        f"salic.{nome_bronze(origem, '')}",
        texto,
    )


def migrar_sql(origem: str, tabelas: set[str] | None = None) -> dict[str, str]:
    renomes: dict[str, str] = {}
    for sql in sorted((ORIGEM_DIR / origem).glob("*.sql")):
        if tabelas is not None and sql.stem[len(origem) + 2 :] not in tabelas:
            continue
        novo = novo_nome(sql.stem, origem)
        renomes[sql.stem] = novo
        destino = DESTINO / f"{novo}.sql"
        assert not destino.exists(), f"já existe {destino}"
        git("mv", str(sql), str(destino))
        texto = destino.read_text(encoding="utf-8")
        texto, n = RE_HEADER.subn(
            lambda m: f"-- Silver SALIC — {novo} (ex-{m.group('tabela')}"
            f"{m.group('view') or ''}).\n{LINHA_FONTE}",
            texto,
            count=1,
        )
        if n != 1:
            print(f"  ATENÇÃO cabeçalho fora do padrão, revisar à mão: {destino.name}")
        texto = trocar_mencao(texto, origem)
        texto, n_src = trocar_source(texto, origem)
        if n_src != 1:
            print(f"  ATENÇÃO {n_src} source() trocado(s) em {destino.name}; esperava 1")
        destino.write_text(texto, encoding="utf-8")
    print(f"{len(renomes)} .sql movidos")
    return renomes


def migrar_schema(origem: str, renomes: dict[str, str]) -> None:
    de = ORIGEM_DIR / origem / "schema.yml"
    para = DESTINO / f"schema_{origem}.yml"
    git("mv", str(de), str(para))
    texto = para.read_text(encoding="utf-8")
    faltam = []
    for velho, novo in renomes.items():
        texto, n = re.subn(
            rf"^(  - name: ){re.escape(velho)}$", rf"\g<1>{novo}", texto, flags=re.M
        )
        if n != 1:
            faltam.append(velho)
    texto, n_desc = RE_DESC.subn(DESC_NOVA, texto)
    texto = trocar_mencao(texto, origem)
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
    if not de.exists():
        print(f"  ATENÇÃO não há {de.relative_to(RAIZ)}")
        return
    para = DESTINO / de.name
    if para.exists():
        git("rm", "-q", str(para))
        print(f"  {para.relative_to(RAIZ)} (mínimo) substituído pelo completo")
    git("mv", str(de), str(para))
    texto = para.read_text(encoding="utf-8")
    texto, n_nome = re.subn(
        rf"^  - name: bronze_{re.escape(origem)}$", "  - name: salic", texto, flags=re.M
    )
    # Depois do #110 as sources legadas já declaram schema salic.
    texto, n_schema = re.subn(
        r"^    schema: salic(_bronze)?$", "    schema: salic", texto, flags=re.M
    )
    texto, n_ds = RE_DESC_SOURCE.subn(DESC_SOURCE, texto, count=1)
    if n_ds != 1:
        print("  ATENÇÃO descrição da source não encontrada para padronizar")

    # O #110 pôs, por tabela, identifier: bronze_<banco>_<tabela>. Com o nome
    # da tabela virando esse mesmo nome, a linha sai; fica só quando difere,
    # como nos nomes que o Postgres cortou em 63 caracteres.
    def trocar_tabela(m: re.Match[str]) -> str:
        nome = nome_bronze(origem, m.group(1))
        linha = f"      - name: {nome}"
        comentario, ident = m.group(2), m.group(3)
        if ident is not None and ident != nome:
            if comentario:
                linha += f"\n{comentario}"
            linha += f"\n        identifier: {ident}"
        return linha

    texto, n_tab = re.subn(
        rf"^      - name: {re.escape(origem)}__(\S+)$"
        r"(?:\n(?:(        #[^\n]*)\n)?        identifier: (\S+)$)?",
        trocar_tabela,
        texto,
        flags=re.M,
    )
    para.write_text(texto, encoding="utf-8")
    print(
        f"sources_{origem}.yml movido: source salic, schema salic, "
        f"{n_tab} tabelas renomeadas"
    )
    if n_nome != 1 or n_schema != 1:
        print(
            f"  ATENÇÃO name ({n_nome}) ou schema ({n_schema}) não trocados como esperado"
        )


def tirar_blocos(texto: str, cabeca: str, nomes: set[str]) -> tuple[str, list[str]]:
    """Separa os blocos ``<cabeca><nome>`` (até o próximo do mesmo recuo)."""
    linhas = texto.split("\n")
    fica: list[str] = []
    sai: list[str] = []
    atual: list[str] | None = None
    recuo = len(cabeca) - len(cabeca.lstrip())
    for linha in linhas:
        if linha.startswith(cabeca):
            atual = sai if linha[len(cabeca) :].strip() in nomes else fica
        elif (
            atual is sai and linha.strip() and (len(linha) - len(linha.lstrip())) <= recuo
        ):
            atual = fica
        (atual if atual is not None else fica).append(linha)
    return "\n".join(fica), sai


def migrar_schema_parcial(origem: str, assunto: str, renomes: dict[str, str]) -> None:
    de = ORIGEM_DIR / origem / "schema.yml"
    para = DESTINO / f"schema_{origem}_{assunto}.yml"
    fica, sai = tirar_blocos(de.read_text(encoding="utf-8"), "  - name: ", set(renomes))
    de.write_text(fica, encoding="utf-8")
    bloco = "\n".join(sai).rstrip() + "\n"
    for velho, novo in renomes.items():
        bloco = re.sub(
            rf"^(  - name: ){re.escape(velho)}$", rf"\g<1>{novo}", bloco, flags=re.M
        )
    bloco, n_desc = RE_DESC.subn(DESC_NOVA, bloco)
    bloco = trocar_mencao(bloco, origem)
    cabecalho = (
        "version: 2\n\nmodels:\n"
        if not para.exists()
        else para.read_text(encoding="utf-8").rstrip() + "\n\n"
    )
    para.write_text(cabecalho + bloco, encoding="utf-8")
    achados = bloco.count("\n  - name: ") + bloco.startswith("  - name: ")
    print(f"schema: {achados} blocos para {para.name}, {n_desc} descrições ajustadas")
    if achados != len(renomes):
        print(f"  ATENÇÃO esperava {len(renomes)} blocos no schema.yml legado")


def migrar_sources_parcial(origem: str, tabelas: set[str]) -> None:
    de = ORIGEM_DIR / f"sources_{origem}.yml"
    para = DESTINO / de.name
    nomes_velhos = {f"{origem}__{t}" for t in tabelas}
    fica, sai = tirar_blocos(
        de.read_text(encoding="utf-8"), "      - name: ", nomes_velhos
    )
    de.write_text(fica, encoding="utf-8")
    bloco = "\n".join(sai).rstrip() + "\n"

    # identifier igual ao nome novo sai; diferente (nome cortado em 63) fica.
    def trocar(m: re.Match[str]) -> str:
        nome = nome_bronze(origem, m.group(1))
        ident = m.group(2)
        return f"      - name: {nome}" + (
            f"\n        identifier: {ident}" if ident and ident != nome else ""
        )

    bloco, n_tab = re.subn(
        rf"^      - name: {re.escape(origem)}__(\S+)$(?:\n        identifier: (\S+)$)?",
        trocar,
        bloco,
        flags=re.M,
    )
    destino = para.read_text(encoding="utf-8")
    novos = {nome_bronze(origem, t) for t in tabelas}
    destino, substituidos = tirar_blocos(destino, "      - name: ", novos)
    if substituidos:
        n_sub = sum(1 for linha in substituidos if linha.startswith("      - name: "))
        print(f"  {n_sub} tabela(s) de {para.name} trocadas pela versão do legado")
    para.write_text(destino.rstrip() + "\n" + bloco, encoding="utf-8")
    print(f"sources: {n_tab} tabelas de {de.name} para {para.relative_to(RAIZ)}")
    if n_tab != len(tabelas):
        print(f"  ATENÇÃO esperava {len(tabelas)} tabelas no {de.name} legado")


def reescrever_leitores(
    origem: str, renomes: dict[str, str], tabelas: set[str] | None = None
) -> None:
    # .yml entra por causa dos relationships: to: ref('<origem>__x').
    alvos = (
        list((PROJETO / "models").rglob("*.sql"))
        + list((PROJETO / "models").rglob("*.yml"))
        + list((PROJETO / "tests").glob("*.sql"))
        + list((PROJETO / "macros").rglob("*.sql"))
    )
    nomes = "|".join(map(re.escape, renomes))
    padrao_ref = re.compile(r"""ref\(\s*(['"])(""" + nomes + r""")\1\s*\)""")
    sobra = re.compile(r"""source\(\s*['"]bronze_""" + re.escape(origem) + r"""['"]""")
    for arq in alvos:
        texto = arq.read_text(encoding="utf-8")
        novo, n_ref = padrao_ref.subn(
            lambda m: f"ref({m.group(1)}{renomes[m.group(2)]}{m.group(1)})", texto
        )
        novo, n_src = trocar_source(novo, origem, tabelas)
        if n_ref or n_src:
            arq.write_text(novo, encoding="utf-8")
            print(
                f"  {n_ref} ref() e {n_src} source() reescrito(s) em "
                f"{arq.relative_to(RAIZ)}"
            )
        if tabelas is None and sobra.search(novo):
            print(
                f"  ATENÇÃO sobrou source('bronze_{origem}', ...) em "
                f"{arq.relative_to(RAIZ)}"
            )
        velho = re.search(rf"""ref\(\s*['"]{re.escape(origem)}__""", novo)
        if velho and (tabelas is None or arq.is_relative_to(DESTINO)):
            print(f"  ATENÇÃO sobrou ref('{origem}__...') em {arq.relative_to(RAIZ)}")


def main(argv: list[str]) -> int:
    origem = argv[1] if len(argv) > 1 else ""
    if origem not in ORIGENS:
        print(f"uso: {argv[0]} <{'|'.join(ORIGENS)}> [--assunto X --tabelas a,b]")
        return 2
    opcoes = dict(zip(argv[2::2], argv[3::2]))
    tabelas = set(filter(None, opcoes.get("--tabelas", "").split(","))) or None
    assunto = opcoes.get("--assunto")
    if (tabelas is None) != (assunto is None):
        print("--assunto e --tabelas vão juntos")
        return 2
    longos = nomes_longos(origem, tabelas)
    if longos:
        print(
            f"ERRO: {len(longos)} modelo(s) passariam de {LIMITE_NOME} caracteres. "
            "Dê nome à mão (alias ou nome mais curto) e rode de novo:"
        )
        for velho, novo in longos:
            print(f"  {velho} -> {novo} ({len(novo)})")
        return 1
    if tabelas is not None:
        existem = {p.stem[len(origem) + 2 :] for p in (ORIGEM_DIR / origem).glob("*.sql")}
        if tabelas - existem:
            print(f"ERRO: sem modelo legado para {sorted(tabelas - existem)}")
            return 1
        renomes = migrar_sql(origem, tabelas)
        migrar_schema_parcial(origem, str(assunto), renomes)
        migrar_sources_parcial(origem, tabelas)
        reescrever_leitores(origem, renomes, tabelas)
        return 0
    renomes = migrar_sql(origem)
    migrar_schema(origem, renomes)
    migrar_sources(origem)
    reescrever_leitores(origem, renomes)
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
