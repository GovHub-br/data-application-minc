"""Carga de fonte estática no schema ``seeds`` (ADR 0012), para qualquer fonte.

O INEP tem a versão dele em ``cliente_inep``, com o ``jsonb`` do CENSOESC e o
CSV lido em streaming. Esta é a das fontes pequenas, que cabem num DataFrame:
as planilhas do IBGE, a execução do SIOP por ano, os CSVs do Tesouro
Transparente. O caminho é o mesmo do ADR::

    Parquet de staging (toda coluna texto) ─▶ seeds.<tabela>

Recarregar um período é ``DELETE`` daquele período e ``COPY`` na mesma
transação: uma falha no meio deixa o período anterior intacto, e reexecutar
não duplica. Coluna nova num período entra como ``TEXT``.

A conferência contra a origem vai para ``seeds.<fonte>_controle_carga``, uma
linha por entidade e período: linhas na origem, no Parquet e no banco. A task
falha se as três não baterem.
"""

import csv
import io
import logging
import re
import unicodedata
from collections.abc import Iterable, Sequence
from datetime import date, datetime
from typing import Any

import pandas as pd

# Postgres corta identificador acima disto sem avisar, e duas tabelas cortadas
# no mesmo prefixo virariam uma só.
LIMITE_IDENTIFICADOR = 63
# Postgres recusa tabela com mais colunas que isto.
LIMITE_COLUNAS = 1600

_NAO_ALFANUMERICO = re.compile(r"[^a-z0-9]+")


def slug(texto: Any) -> str:
    """Nome de coluna ou tabela: sem acento, minúsculo, ``_`` no lugar do resto."""
    sem_acento = (
        unicodedata.normalize("NFKD", str(texto))
        .encode("ascii", "ignore")
        .decode("ascii")
    )
    return _NAO_ALFANUMERICO.sub("_", sem_acento.lower()).strip("_")


def normalizar_colunas(cabecalho: Sequence[Any]) -> list[str]:
    """Nomes de coluna únicos a partir do cabeçalho como veio.

    Célula vazia vira ``coluna_<n>`` (a posição, a partir de 1), e nome que se
    repete ganha ``_2``, ``_3``: a aba "Cultura" da MUNIC 2018 tem um cabeçalho
    repetido, e o dicionário de toda planilha do IBGE tem células vazias.
    """
    nomes: list[str] = []
    usados: set[str] = set()
    for posicao, celula in enumerate(cabecalho, start=1):
        texto = texto_da_celula(celula)
        base = (slug(texto) if texto is not None else "") or f"coluna_{posicao}"
        nome, n = base, 1
        while nome in usados:
            n += 1
            nome = f"{base}_{n}"
        usados.add(nome)
        nomes.append(nome)
    return nomes


def texto_da_celula(valor: Any) -> str | None:
    """O valor de uma célula como texto, sem mudar o que ele diz.

    O leitor de planilha devolve número como ``float``: ``1100015.0`` vira
    ``"1100015"``, e ``0.5`` fica ``"0.5"``. Data vira ISO. Vazio é nulo. A
    tipagem é do dbt.
    """
    if valor is None:
        return None
    if isinstance(valor, bool):
        return "true" if valor else "false"
    if isinstance(valor, float):
        if valor != valor:  # NaN
            return None
        return str(int(valor)) if valor.is_integer() else repr(valor)
    if isinstance(valor, (datetime, date)):
        return valor.isoformat()
    texto = str(valor)
    return texto if texto.strip() else None


def linhas_para_registros(
    colunas: Sequence[str], linhas: Iterable[Sequence[Any]]
) -> list[dict[str, str | None]]:
    """Uma linha por registro, sem as linhas inteiramente vazias."""
    registros = []
    for linha in linhas:
        valores = [texto_da_celula(v) for v in linha]
        if not any(v is not None for v in valores):
            continue
        valores += [None] * (len(colunas) - len(valores))
        registros.append(dict(zip(colunas, valores[: len(colunas)])))
    return registros


def validar_identificador(nome: str) -> str:
    if len(nome) > LIMITE_IDENTIFICADOR:
        raise ValueError(
            f"{nome!r} tem {len(nome)} caracteres; o Postgres corta em "
            f"{LIMITE_IDENTIFICADOR}"
        )
    return nome


def _ident(nome: str) -> str:
    return '"' + nome.replace('"', '""') + '"'


def _copy_csv(df: pd.DataFrame) -> io.StringIO:
    """CSV para ``COPY ... (FORMAT csv)``: nulo é campo vazio sem aspas.

    Valor vazio não chega aqui (já virou nulo), então não há como confundir
    os dois.
    """
    buffer = io.StringIO()
    escritor = csv.writer(buffer, lineterminator="\n")
    for linha in df.itertuples(index=False, name=None):
        escritor.writerow(["" if pd.isna(v) else v for v in linha])
    buffer.seek(0)
    return buffer


def substituir_periodo(
    conn_str: str,
    df: pd.DataFrame,
    schema: str,
    tabela: str,
    coluna_periodo: str,
    periodo: str,
) -> int:
    """Substitui ``periodo`` em ``schema.tabela`` pelas linhas de ``df``.

    ``df`` precisa ter ``coluna_periodo`` com ``periodo`` em toda linha.
    Devolve as linhas do período no banco, contadas depois do ``COPY``.
    """
    import psycopg2

    from cliente_postgres import ClientPostgresDB

    validar_identificador(tabela)
    colunas = [str(c) for c in df.columns]
    if len(colunas) > LIMITE_COLUNAS:
        raise ValueError(
            f"{tabela}: {len(colunas)} colunas, acima do limite do Postgres "
            f"({LIMITE_COLUNAS})"
        )
    if coluna_periodo not in colunas:
        raise ValueError(f"{tabela}: falta a coluna de período {coluna_periodo!r}")
    if not (df[coluna_periodo] == periodo).all():
        raise ValueError(f"{tabela}: há linhas fora do período {periodo!r}")

    db = ClientPostgresDB(conn_str)
    db.create_table_if_not_exists(dict.fromkeys(colunas), tabela, schema=schema)
    db._evolve_schema(colunas, tabela, schema=schema)

    alvo = f"{_ident(schema)}.{_ident(tabela)}"
    lista = ", ".join(_ident(c) for c in colunas)
    with psycopg2.connect(conn_str) as conn, conn.cursor() as cursor:
        cursor.execute(
            f"DELETE FROM {alvo} WHERE {_ident(coluna_periodo)} = %s", (periodo,)
        )
        cursor.copy_expert(
            f"COPY {alvo} ({lista}) FROM STDIN WITH (FORMAT csv)", _copy_csv(df)
        )
        cursor.execute(
            f"SELECT count(*) FROM {alvo} WHERE {_ident(coluna_periodo)} = %s",
            (periodo,),
        )
        contagem = cursor.fetchone()
        carregadas = int(contagem[0]) if contagem else 0
    logging.info(
        "[seeds_carga] %s.%s %s=%s: %d linhas",
        schema,
        tabela,
        coluna_periodo,
        periodo,
        carregadas,
    )
    return carregadas


def tabela_controle(fonte: str) -> str:
    return validar_identificador(f"{fonte}_controle_carga")


def registrar_carga(
    conn_str: str, schema: str, fonte: str, cargas: list[dict[str, Any]]
) -> None:
    """Grava em ``<fonte>_controle_carga`` uma linha por entidade e período.

    ``linhas_origem`` é o que a origem diz ter (a contagem da planilha ou da
    resposta), ``linhas_parquet`` o que foi gravado no staging e
    ``linhas_carregadas`` o que o banco tem depois do COPY.
    """
    import psycopg2
    from psycopg2.extras import execute_values

    alvo = f"{_ident(schema)}.{_ident(tabela_controle(fonte))}"
    with psycopg2.connect(conn_str) as conn, conn.cursor() as cursor:
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {_ident(schema)}")
        cursor.execute(
            f"CREATE TABLE IF NOT EXISTS {alvo} ("
            "entidade TEXT, periodo TEXT, tabela TEXT, arquivo TEXT, "
            "linhas_origem BIGINT, linhas_parquet BIGINT, linhas_carregadas BIGINT, "
            "key_raw TEXT, key_staging TEXT, dt_ingest TIMESTAMPTZ, "
            "PRIMARY KEY (entidade, periodo))"
        )
        execute_values(
            cursor,
            f"INSERT INTO {alvo} VALUES %s ON CONFLICT (entidade, periodo) DO UPDATE "
            "SET tabela = EXCLUDED.tabela, arquivo = EXCLUDED.arquivo, "
            "linhas_origem = EXCLUDED.linhas_origem, "
            "linhas_parquet = EXCLUDED.linhas_parquet, "
            "linhas_carregadas = EXCLUDED.linhas_carregadas, "
            "key_raw = EXCLUDED.key_raw, key_staging = EXCLUDED.key_staging, "
            "dt_ingest = EXCLUDED.dt_ingest",
            [
                (
                    c["entidade"],
                    c["periodo"],
                    c["tabela"],
                    c["arquivo"],
                    c["linhas_origem"],
                    c["linhas_parquet"],
                    c["linhas_carregadas"],
                    c["key_raw"],
                    c["key_staging"],
                    c["dt_ingest"],
                )
                for c in cargas
            ],
        )


def conferir_contagens(cargas: list[dict[str, Any]]) -> None:
    """Falha se origem, Parquet e banco não tiverem as mesmas linhas.

    Chamada depois de ``registrar_carga``: a divergência fica registrada na
    tabela de controle mesmo quando a task falha.
    """
    divergentes = [
        c
        for c in cargas
        if not c["linhas_origem"] == c["linhas_parquet"] == c["linhas_carregadas"]
    ]
    if divergentes:
        raise ValueError(
            "contagem diverge da origem (origem/parquet/banco): "
            + "; ".join(
                f"{c['periodo']} {c['entidade']}: {c['linhas_origem']}/"
                f"{c['linhas_parquet']}/{c['linhas_carregadas']}"
                for c in divergentes
            )
        )
