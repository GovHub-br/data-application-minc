"""Gera os modelos SQL da bronze do SALIC a partir do perfil medido no banco.

Le /tmp/salic_perfil.json (colunas reais + amostra de 200 linhas por tabela) e
os tipos originais do SQL Server que ja estao nas descricoes das sources, e
escreve um modelo por tabela em dbt/minc/models/salic_dbt/bronze/<origem>/.

A REGRA CENTRAL: o dicionario diz a INTENCAO da origem, a amostra diz a
REALIDADE do texto gravado. Quando divergem, a amostra veta -- a coluna fica
TEXT. E todo cast e guardado por regex, entao valor fora do padrao vira NULL
em vez de derrubar o modelo em execucao.

Uso:
    /tmp/dbtvenv/bin/python .claude/skills/bronze-salic-dbt/scripts/gerar_modelos.py
    /tmp/dbtvenv/bin/python .../gerar_modelos.py --so sac__tbprojetos  # uma tabela
    /tmp/dbtvenv/bin/python .../gerar_modelos.py --relatorio   # so mede, nao escreve
"""

from __future__ import annotations

import argparse
import glob
import json
import re
import unicodedata

import yaml
from collections import Counter
from pathlib import Path

PERFIL = Path("/tmp/salic_perfil.json")
INVENTARIO = Path("/tmp/salic_inventario.json")
DESTINO = Path("dbt/minc/models/salic_dbt/bronze")
SOURCES = "dbt/minc/models/salic_dbt/bronze/sources_*.yml"

# Quantos valores nao-nulos a amostra precisa ter para que o dado possa,
# sozinho, decidir um tipo. Abaixo disso a amostra so serve para vetar.
MIN_PARA_INFERIR = 3

# --------------------------------------------------------------------------
# Dicionario: "Tipo original (SQL Server): int identity(4)." -> grupo
# --------------------------------------------------------------------------
TIPO_NA_DESCRICAO = re.compile(r"Tipo original \(SQL Server\):\s*([^.(]+)")

GRUPO_POR_TIPO_SQLSERVER = {
    "bit": "boolean",
    "tinyint": "integer",
    "smallint": "integer",
    "int": "integer",
    "int identity": "integer",
    "smallint identity": "integer",
    "tinyint identity": "integer",
    "bigint": "bigint",
    "bigint identity": "bigint",
    "decimal": "numeric",
    "numeric": "numeric",
    "money": "numeric",
    "smallmoney": "numeric",
    "float": "numeric",
    "real": "numeric",
    "decimal identity": "numeric",
    "numeric identity": "numeric",
    "datetime": "timestamp",
    "smalldatetime": "timestamp",
    "datetime2": "timestamp",
    "timestamp": "text",
    "date": "date",
    "time": "text",
}

# --------------------------------------------------------------------------
# Validacao da amostra. Mesmos padroes que vao para o guard em SQL.
# --------------------------------------------------------------------------
RE_INTEIRO = re.compile(r"^-?[0-9]+$")
RE_NUMERICO = re.compile(r"^-?[0-9]+(\.[0-9]+)?$")
RE_DATA = re.compile(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}$")
RE_DATAHORA = re.compile(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}[ T][0-9]{2}:[0-9]{2}")
BOOLEANOS = {"0", "1", "true", "false", "t", "f"}
BOOLEANOS_LITERAIS = {"true", "false", "t", "f"}

VALIDA = {
    "integer": lambda v: bool(RE_INTEIRO.match(v)),
    "bigint": lambda v: bool(RE_INTEIRO.match(v)),
    "numeric": lambda v: bool(RE_NUMERICO.match(v)),
    "date": lambda v: bool(RE_DATA.match(v)),
    "timestamp": lambda v: bool(RE_DATA.match(v) or RE_DATAHORA.match(v)),
    "boolean": lambda v: v.lower() in BOOLEANOS,
    "text": lambda v: True,
}


def dicionario() -> dict[str, dict[str, str]]:
    """Tipo do SQL Server por (tabela, coluna), lido das descricoes das sources.

    Le com YAML de verdade, nao linha a linha: a descricao vem dobrada em bloco
    `>` e o padrao quebra no meio ("Tipo original (SQL / Server): int(4)."),
    entao varredura por linha perde a maioria.
    """
    fora: dict[str, dict[str, str]] = {}
    for caminho in glob.glob(SOURCES):
        doc = yaml.safe_load(open(caminho, encoding="utf-8"))
        for fonte in doc.get("sources", []):
            for tab in fonte.get("tables", []):
                for col in tab.get("columns", []) or []:
                    m = TIPO_NA_DESCRICAO.search(col.get("description") or "")
                    if m:
                        fora.setdefault(tab["name"], {})[col["name"]] = (
                            m.group(1).strip().lower()
                        )
    return fora


def grupo_do_dicionario(tipo_sqlserver: str) -> str:
    """char/varchar/text/binary caem no default 'text' -- e isso e proposital:
    coluna declarada como texto na origem guarda zero a esquerda (PRONAC,
    sequencial, CPF). Converter para inteiro destruiria o valor."""
    return GRUPO_POR_TIPO_SQLSERVER.get(tipo_sqlserver, "text")


def parece_documento(valores: list[str]) -> bool:
    """CPF (11) e CNPJ (14) sao digitos de largura fixa. Sao identificadores,
    nao numeros: viram texto."""
    larguras = {len(v) for v in valores}
    return larguras <= {11} or larguras <= {14}


def tem_zero_a_esquerda(valores: list[str]) -> bool:
    return any(len(v) > 1 and v.startswith("0") for v in valores)


def inferir_da_amostra(valores: list[str]) -> str:
    """Tipo que o proprio dado sustenta. Conservador de proposito."""
    if len(valores) < MIN_PARA_INFERIR:
        return "text"
    if all(v.lower() in BOOLEANOS_LITERAIS for v in valores):
        return "boolean"
    if all(RE_INTEIRO.match(v) for v in valores):
        # zero a esquerda e largura de documento denunciam codigo, nao numero
        if tem_zero_a_esquerda(valores) or parece_documento(valores):
            return "text"
        largura = max(len(v.lstrip("-")) for v in valores)
        if largura > 18:
            return "text"
        return "bigint" if largura > 9 else "integer"
    if all(RE_NUMERICO.match(v) for v in valores):
        return "numeric"
    if all(RE_DATAHORA.match(v) for v in valores):
        return "timestamp"
    if all(RE_DATA.match(v) for v in valores):
        return "date"
    return "text"


def decidir(tipo_dic: str | None, valores: list[str]) -> tuple[str, str]:
    """Devolve (grupo, motivo). A amostra sempre pode vetar o dicionario."""
    do_dado = inferir_da_amostra(valores)

    if tipo_dic is None:
        return do_dado, "amostra"

    grupo_dic = grupo_do_dicionario(tipo_dic)
    if grupo_dic == "text":
        return "text", "dicionario-texto"
    if not valores:
        # nada na amostra para confirmar nem desmentir; o cast guardado e inocuo
        return grupo_dic, "dicionario-sem-amostra"
    if not all(VALIDA[grupo_dic](v) for v in valores):
        return "text", "amostra-vetou"
    # o dicionario passou no teste do dado. Ele ainda pode ser mais largo que a
    # amostra (numeric onde so vieram inteiros), e o mais largo e o certo.
    if grupo_dic == "integer" and do_dado == "bigint":
        return "bigint", "amostra-alargou"
    if grupo_dic in ("integer", "bigint") and (
        tem_zero_a_esquerda(valores) or parece_documento(valores)
    ):
        return "text", "amostra-vetou"
    return grupo_dic, "dicionario"


# --------------------------------------------------------------------------
# SQL
# --------------------------------------------------------------------------
RESERVADAS = {
    "all",
    "analyse",
    "analyze",
    "and",
    "any",
    "array",
    "as",
    "asc",
    "authorization",
    "between",
    "binary",
    "both",
    "case",
    "cast",
    "check",
    "collate",
    "column",
    "constraint",
    "create",
    "cross",
    "current_date",
    "current_role",
    "current_time",
    "current_timestamp",
    "current_user",
    "default",
    "deferrable",
    "desc",
    "distinct",
    "do",
    "else",
    "end",
    "except",
    "false",
    "for",
    "foreign",
    "freeze",
    "from",
    "full",
    "grant",
    "group",
    "having",
    "ilike",
    "in",
    "initially",
    "inner",
    "intersect",
    "into",
    "is",
    "isnull",
    "join",
    "leading",
    "left",
    "like",
    "limit",
    "localtime",
    "localtimestamp",
    "natural",
    "new",
    "not",
    "notnull",
    "null",
    "offset",
    "old",
    "on",
    "only",
    "or",
    "order",
    "outer",
    "overlaps",
    "placing",
    "primary",
    "references",
    "returning",
    "right",
    "select",
    "session_user",
    "similar",
    "some",
    "symmetric",
    "table",
    "then",
    "to",
    "trailing",
    "true",
    "union",
    "unique",
    "user",
    "using",
    "verbose",
    "when",
    "where",
    "window",
    "with",
}


def sem_acento(texto: str) -> str:
    return "".join(
        c for c in unicodedata.normalize("NFD", texto) if unicodedata.category(c) != "Mn"
    )


def apelido(coluna: str) -> str:
    """Nome de saida em snake_case ASCII. 194 colunas da origem tem espaco,
    acento ou barra -- consumir isso adiante exigiria aspas para sempre."""
    a = sem_acento(coluna.lower())
    a = re.sub(r"[^a-z0-9_]+", "_", a).rstrip("_")
    # o underscore da FRENTE fica: `_fatia` e o nome da coluna tecnica de
    # controle de carga da ingestao por fatias (ADR 0005), e renomea-la
    # quebraria a auditoria de carga repetida.
    if not a.strip("_"):
        return "coluna"
    return "c_" + a if a[0].isdigit() else a


def cita(nome: str) -> str:
    if re.match(r"^[a-z_][a-z0-9_]*$", nome) and nome not in RESERVADAS:
        return nome
    return '"' + nome.replace('"', '""') + '"'


def expressao(coluna: str, grupo: str, ja_tipada: bool) -> str:
    """Delega para as macros de macros/bronze_salic/casts_bronze.sql. A regra do
    cast vive la, num lugar so, e nao repetida em 571 arquivos."""
    ref = cita(coluna)
    if ja_tipada:
        return ref
    macro = {
        "text": f"bronze_texto('{coluna}')",
        "integer": f"bronze_inteiro('{coluna}')",
        "bigint": f"bronze_inteiro('{coluna}', tipo='bigint')",
        "numeric": f"bronze_numerico('{coluna}')",
        "date": f"bronze_data('{coluna}')",
        "timestamp": f"bronze_timestamp('{coluna}')",
        "boolean": f"bronze_booleano('{coluna}')",
    }[grupo]
    # a macro recebe o nome ja citado, para coluna com espaco/acento funcionar
    macro = macro.replace(f"'{coluna}'", "'" + ref.replace("'", "''") + "'")
    return "{{ " + macro + " }}"


FONTE_POR_ORIGEM = {
    "sac": "bronze_sac",
    "tabelas": "bronze_tabelas",
    "agentes": "bronze_agentes",
    "controledeacesso": "bronze_controledeacesso",
    "bdcorporativo": "bronze_bdcorporativo",
}


def montar_modelo(
    tabela: str, colunas: list[str], decisoes: dict, tipos_reais: dict, eh_view: bool
) -> str:
    origem = tabela.split("__")[0]
    convertidas = sum(1 for c in colunas if decisoes[c][0] != "text" and c != "_fatia")
    vetadas = sum(1 for c in colunas if decisoes[c][1] == "amostra-vetou")

    linhas = []
    for c in colunas:
        grupo, _ = decisoes[c]
        ja_tipada = tipos_reais.get(c) != "character varying"
        expr = expressao(c, grupo, ja_tipada)
        nome_saida = cita(apelido(c))
        # `_fatia as _fatia` seria ruido; coluna ja tipada sai como esta
        linhas.append(
            f"    {expr}" if expr == nome_saida else f"    {expr} as {nome_saida}"
        )

    cabecalho = [
        f"-- Bronze SALIC — {tabela}{' (VIEW na origem)' if eh_view else ''}.",
        f"-- Origem: salic_bronze.{tabela}, onde tudo chega como texto da",
        "-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.",
        f"-- {len(colunas)} colunas: {convertidas} tipadas, "
        f"{len(colunas) - convertidas - 1} mantidas como texto.",
    ]
    if vetadas:
        cabecalho.append(
            f"-- {vetadas} coluna(s) ficaram texto porque a amostra do banco"
        )
        cabecalho.append("-- contradiz o tipo declarado no dicionário do SALIC.")
    cabecalho.append(
        "-- Os casts são guardados por regex (macros bronze_*): valor fora do"
    )
    cabecalho.append("-- padrão vira NULL em vez de derrubar o modelo.")

    return (
        "\n".join(cabecalho)
        + "\nselect\n"
        + ",\n".join(linhas)
        + f"\nfrom {{{{ source('{FONTE_POR_ORIGEM[origem]}', '{tabela}') }}}}\n"
    )


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--so", default=None)
    ap.add_argument("--relatorio", action="store_true")
    args = ap.parse_args()

    perfil = json.loads(PERFIL.read_text())
    escopo = json.loads(INVENTARIO.read_text())["escopo"]
    dic = dicionario()
    print(
        f"dicionario: {len(dic)} tabelas, "
        f"{sum(len(v) for v in dic.values())} colunas com tipo declarado"
    )

    alvos = [args.so] if args.so else escopo
    contagem, motivos, escritos = Counter(), Counter(), 0

    for tabela in alvos:
        colunas = perfil["colunas"][tabela]
        tipos_reais = perfil["tipo_coluna"][tabela]
        amostras = perfil["perfil"].get(tabela, {})
        dic_tab = dic.get(tabela, {})

        decisoes = {}
        for c in colunas:
            if tipos_reais.get(c) != "character varying":
                decisoes[c] = ("nativo", "ja-tipada")
                continue
            info = amostras.get(c, {})
            # valores distintos quando cabem; senao a amostra curta de 5
            vals = info.get("valores") or info.get("amostra") or []
            vals = [v.strip() for v in vals if v and v.strip()]
            decisoes[c] = decidir(dic_tab.get(c), vals)
            contagem[decisoes[c][0]] += 1
            motivos[decisoes[c][1]] += 1

        if args.relatorio:
            continue

        origem = tabela.split("__")[0]
        pasta = DESTINO / origem
        pasta.mkdir(parents=True, exist_ok=True)
        # `$` e valido em nome de tabela do SQL Server, nao em nome de modelo dbt
        nome = tabela.replace("$", "_")
        eh_view = perfil["tipo_objeto"].get(tabela) == "VIEW"
        (pasta / f"{nome}.sql").write_text(
            montar_modelo(tabela, colunas, decisoes, tipos_reais, eh_view),
            encoding="utf-8",
        )
        escritos += 1

    print(f"\n{escritos} modelos escritos em {DESTINO}")
    print("\ntipo decidido por coluna:")
    for k, v in contagem.most_common():
        print(f"  {k:12} {v:6}")
    print("\nde onde veio a decisao:")
    for k, v in motivos.most_common():
        print(f"  {k:24} {v:6}")


if __name__ == "__main__":
    main()
