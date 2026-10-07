"""Microdados do Censo Escolar do INEP: descoberta, leitura e carga.

O INEP publica um ZIP por ano em
https://www.gov.br/inep/pt-br/acesso-a-informacao/dados-abertos/microdados/censo-escolar,
de 1995 em diante. O conteúdo muda ao longo da série, e é isso que este
módulo absorve:

- **1995-2006**: ``CENSOESC_AAAA.CSV`` e tabelas auxiliares (``DADOSCURSO``,
  ``EDUCPROF``...), separadas por ``|``. O ``CENSOESC`` chega a 3.808 colunas
  -- acima do limite de 1.600 do Postgres --, então vai para o banco com as
  colunas de identificação próprias e o resto num ``jsonb``.
- **2007-2024**: ``microdados_ed_basica_AAAA.csv`` (uma linha por escola), e
  de 2023 em diante o ``suplemento_cursos_tecnicos_AAAA.csv``. Separador ``;``.
- **2025**: seis ``Tabela_<X>_2025_V2.csv`` (escola, docente, matrícula,
  turma, gestor, curso técnico). O ``ed_basica`` deixou de existir.

Tudo em latin-1. O nome da pasta interna do ZIP também muda de ano para ano
(``microdados_ed_basica_2015/``, ``microdados_censo_escolar_2024_defeso/``,
``..._2025_v2/``), por isso cada CSV é reconhecido pelo nome do arquivo.

As funções de nome, separador e conversão são puras para serem testáveis sem
rede, MinIO ou Postgres.
"""

from __future__ import annotations

import csv
import functools
import io
import json
import logging
import re
import tempfile
import zipfile
from datetime import datetime
from pathlib import Path
from typing import IO, Any, Iterator

import pyarrow as pa
import pyarrow.csv as pa_csv
import pyarrow.parquet as pq

URL_PAGINA = (
    "https://www.gov.br/inep/pt-br/acesso-a-informacao/dados-abertos/"
    "microdados/censo-escolar"
)

ENCODING = "latin-1"

# O de 2025 está publicado como microdados_censo_escolar_2025_.zip: o
# sublinhado final é opcional para a DAG não depender de um padrão só.
_URL_ZIP = re.compile(
    r"https?://[^\"'\s<>]+/microdados_censo_escolar_(\d{4})_?\.zip", re.IGNORECASE
)

# Entidade de cada arquivo de dados, pelo nome sem ano e sem versão. A lista
# é fechada de propósito: arquivo novo num ZIP falha a task com o nome dele,
# em vez de virar uma tabela com nome inventado.
_ENTIDADE_POR_NOME = {
    # 1995-2006: o CENSOESC e tabelas auxiliares que entram e saem da série
    # (inventário dos 31 ZIPs em outubro de 2026). Os EMn não são a mesma
    # tabela com outro nome -- cada um tem colunas próprias --, então ficam
    # separados.
    "censoesc": "censoesc",
    "dadoscurso": "dadoscurso",  # 1995
    "dados_desp": "dados_desp",  # 1995
    "em8": "em8",  # 1996
    "em11": "em11",  # 1996
    "es6": "es6",  # 1996
    "em12": "em12",  # 1997-1998
    "educprof": "educprof",  # 1998-2006
    "indicesc": "indicesc",  # 2000-2003
    "indicreg": "indicreg",  # 2000-2003
    "medprof": "medprof",  # 2000-2003
    "em22": "em22",  # 2005-2006
    # 2007-2024
    "microdados_ed_basica": "ed_basica",
    "suplemento_cursos_tecnicos": "suplemento_cursos_tecnicos",
    # 2025 em diante
    "tabela_escola": "escola",
    "tabela_docente": "docente",
    "tabela_matricula": "matricula",
    "tabela_turma": "turma",
    "tabela_gestor_escolar": "gestor_escolar",
    "tabela_curso_tecnico": "curso_tecnico",
}
ENTIDADES = sorted(set(_ENTIDADE_POR_NOME.values()))

# _AAAA e _vN no fim do nome: microdados_ed_basica_2015, Tabela_Escola_2025_V2.
_SUFIXO_ANO_VERSAO = re.compile(r"(_\d{4})?(_v\d+)?$")

# O CENSOESC tem mais colunas do que o Postgres aceita. Estas ficam como
# coluna própria -- são as que identificam a escola e o território --, o resto
# vai para ``dados``. Os nomes variam entre 1995 (CO_IBGE, NU_ANO) e 2005
# (CODMUNIC, ANO); cada ano usa as que tiver.
ENTIDADE_JSONB = "censoesc"
COLUNAS_PROPRIAS_CENSOESC = (
    "mascara",
    "co_ibge",
    "codmunic",
    "nu_ano",
    "ano",
    "uf",
    "sigla",
    "munic",
    "dep",
    "loc",
    "codfunc",
)
COLUNA_JSONB = "dados"

COLUNA_ANO = "ano_censo"
COLUNA_DT_INGESTAO = "dt_ingestao"

# A intermediária de *.inep.gov.br, que o servidor não envia (ver o .pem).
_CERTIFICADO_INEP = (
    Path(__file__).parent / "certificados" / "rnp_icpedu_gr46_ov_tls_ca_2025.pem"
)

# Bloco de leitura do CSV, lido sem threads. Medido no CENSOESC de 2006
# (3.808 colunas): com 16 MB e threads o leitor sozinho chega a 2 GB de RSS;
# com 4 MB e uma thread, a 560 MB. O writer do Parquet ainda soma ~1,5 GB
# nesse arquivo -- é o buffer de cada coluna, e não cai com página menor,
# sem dicionário ou sem estatísticas --, por isso a task roda um ano por vez.
_BLOCO_BYTES = 4 * 1024 * 1024


# ── nomes e descoberta ───────────────────────────────────────────────────


def extrair_urls(html: str) -> dict[int, str]:
    """Ano -> URL do ZIP, a partir do HTML da página do INEP."""
    return {int(m.group(1)): m.group(0) for m in _URL_ZIP.finditer(html)}


def entidade_do_arquivo(nome: str) -> str:
    """Entidade de um CSV de dados pelo nome do arquivo.

    Raises:
        ValueError: o arquivo não é nenhum dos conhecidos.
    """
    base = Path(nome).name.lower()
    if base.endswith(".csv"):
        base = base[: -len(".csv")]
    base = _SUFIXO_ANO_VERSAO.sub("", base)
    if base not in _ENTIDADE_POR_NOME:
        raise ValueError(
            f"arquivo de dados desconhecido no ZIP do INEP: {nome} -- "
            "acrescente-o a _ENTIDADE_POR_NOME em cliente_inep.py"
        )
    return _ENTIDADE_POR_NOME[base]


def tabela_seeds(entidade: str) -> str:
    return f"inep_{entidade}"


def csvs_de_dados(arquivo: zipfile.ZipFile) -> list[zipfile.ZipInfo]:
    """Os CSVs de dados do ZIP: os de ``dados/``, sem o ``md5_*``.

    Anexos (dicionário, questionários) e leia-me ficam no raw, mas não viram
    tabela.
    """
    return [
        info
        for info in arquivo.infolist()
        if not info.is_dir()
        and info.filename.lower().endswith(".csv")
        and "/dados/" in f"/{info.filename.lower()}"
        and not Path(info.filename).name.lower().startswith("md5_")
    ]


def detectar_separador(cabecalho: str) -> str:
    """``|`` no layout antigo (até 2006), ``;`` daí em diante."""
    return "|" if cabecalho.count("|") > cabecalho.count(";") else ";"


def ler_cabecalho(abrir: Any) -> tuple[list[str], str]:
    """Colunas (em minúsculas) e separador do CSV."""
    with abrir() as bruto:
        linha = bruto.readline().decode(ENCODING).rstrip("\r\n")
    separador = detectar_separador(linha)
    return [coluna.strip().lower() for coluna in linha.split(separador)], separador


def contar_linhas(abrir: Any) -> int:
    """Linhas de dados do CSV (quebras de linha menos o cabeçalho).

    É a contagem independente do parser: se ela não bate com o que o
    Parquet recebeu, alguma linha foi fundida ou perdida na leitura.
    """
    quebras, ultimo = 0, b"\n"
    with abrir() as bruto:
        for bloco in iter(functools.partial(bruto.read, 1024 * 1024), b""):
            quebras += bloco.count(b"\n")
            ultimo = bloco[-1:]
    # Última linha sem \n também é linha.
    if ultimo != b"\n":
        quebras += 1
    return max(quebras - 1, 0)


# ── CSV -> Parquet ───────────────────────────────────────────────────────


def csv_para_parquet(
    abrir: Any,
    destino: str,
    ano_censo: int,
    dt_ingestao: datetime,
) -> int:
    """Converte o CSV que ``abrir()`` devolve em Parquet, em streaming.

    ``abrir`` é chamado mais de uma vez (cabeçalho e dados) e deve devolver
    um arquivo binário novo a cada chamada -- ``lambda: zip.open(info)``.
    Toda coluna é texto, com nome em minúsculas; vazio vira nulo. Acrescenta
    ``ano_censo`` e ``dt_ingestao``. Devolve o número de linhas gravadas.
    """
    colunas, separador = ler_cabecalho(abrir)
    if len(set(colunas)) != len(colunas):
        repetidas = sorted({c for c in colunas if colunas.count(c) > 1})
        raise ValueError(f"colunas repetidas no cabeçalho: {repetidas}")

    extras = [
        (COLUNA_ANO, pa.string(), str(ano_censo)),
        (COLUNA_DT_INGESTAO, pa.string(), dt_ingestao.isoformat()),
    ]
    schema = pa.schema(
        [pa.field(c, pa.string()) for c in colunas]
        + [pa.field(nome, tipo) for nome, tipo, _ in extras]
    )

    linhas = 0
    with abrir() as bruto, pq.ParquetWriter(destino, schema) as escritor:
        leitor = pa_csv.open_csv(
            bruto,
            read_options=pa_csv.ReadOptions(
                encoding=ENCODING,
                column_names=colunas,
                skip_rows=1,
                block_size=_BLOCO_BYTES,
                use_threads=False,
            ),
            parse_options=pa_csv.ParseOptions(delimiter=separador),
            convert_options=pa_csv.ConvertOptions(
                column_types={c: pa.string() for c in colunas},
                strings_can_be_null=True,
                null_values=[""],
            ),
        )
        for lote in leitor:
            for nome, tipo, valor in extras:
                lote = lote.append_column(
                    pa.field(nome, tipo), pa.array([valor] * lote.num_rows, tipo)
                )
            # Um row group por lote: sem isso o writer acumula até 1 milhão
            # de linhas antes de gravar, e o CENSOESC de 2006 passa de 2 GB.
            escritor.write_batch(lote, row_group_size=lote.num_rows)
            linhas += lote.num_rows
    return linhas


# ── Parquet -> Postgres ──────────────────────────────────────────────────


def colunas_destino(entidade: str, colunas_parquet: list[str]) -> list[str]:
    """Colunas da tabela em ``seeds`` para um Parquet da ``entidade``."""
    if entidade != ENTIDADE_JSONB:
        return colunas_parquet
    proprias = [c for c in colunas_parquet if c in COLUNAS_PROPRIAS_CENSOESC]
    return proprias + [COLUNA_ANO, COLUNA_DT_INGESTAO, COLUNA_JSONB]


def linhas_para_destino(
    entidade: str, lote: pa.RecordBatch, colunas: list[str]
) -> Iterator[list[str | None]]:
    """Linhas de ``lote`` na ordem de ``colunas``.

    No CENSOESC, o que não é coluna própria vai para ``dados`` como JSON,
    **sem os nulos**: são milhares de colunas por linha, a maioria vazia, e
    ``{"x": null}`` só ocuparia espaço -- ausente e nulo dizem o mesmo.
    """
    registros = lote.to_pylist()
    if entidade != ENTIDADE_JSONB:
        for registro in registros:
            yield [registro.get(c) for c in colunas]
        return
    fixas = set(colunas) - {COLUNA_JSONB}
    for registro in registros:
        dados = {k: v for k, v in registro.items() if k not in fixas and v is not None}
        yield [registro.get(c) for c in colunas if c != COLUNA_JSONB] + [
            json.dumps(dados, ensure_ascii=False)
        ]


def _copy_csv(linhas: Iterator[list[str | None]]) -> io.StringIO:
    """CSV para ``COPY ... (FORMAT csv)``: nulo é campo vazio sem aspas.

    Valor vazio não chega aqui (o Parquet já o converteu em nulo), então
    não há como confundir os dois.
    """
    buffer = io.StringIO()
    escritor = csv.writer(buffer, lineterminator="\n")
    for linha in linhas:
        escritor.writerow(["" if v is None else v for v in linha])
    buffer.seek(0)
    return buffer


def _ident(nome: str) -> str:
    return '"' + nome.replace('"', '""') + '"'


def carregar_parquet(
    conn_str: str,
    caminho_parquet: str,
    entidade: str,
    ano_censo: int,
    schema: str,
) -> int:
    """Substitui o ``ano_censo`` de ``schema.inep_<entidade>`` pelo Parquet.

    ``DELETE`` do ano e ``COPY`` na mesma transação: uma falha no meio
    deixa o ano anterior intacto, e reexecutar não duplica. Colunas novas de
    um ano entram como ``TEXT`` antes da transação. Devolve as linhas
    carregadas, contadas no banco depois do ``COPY``.
    """
    import psycopg2

    from cliente_postgres import ClientPostgresDB

    arquivo = pq.ParquetFile(caminho_parquet)
    colunas = colunas_destino(entidade, arquivo.schema_arrow.names)
    tabela = tabela_seeds(entidade)

    db = ClientPostgresDB(conn_str)
    if entidade == ENTIDADE_JSONB:
        _criar_tabela_censoesc(conn_str, schema, tabela)
    else:
        db.create_table_if_not_exists(dict.fromkeys(colunas), tabela, schema=schema)
    db._evolve_schema([c for c in colunas if c != COLUNA_JSONB], tabela, schema=schema)

    alvo = f"{_ident(schema)}.{_ident(tabela)}"
    lista = ", ".join(_ident(c) for c in colunas)
    with psycopg2.connect(conn_str) as conn, conn.cursor() as cursor:
        cursor.execute(
            f"DELETE FROM {alvo} WHERE {_ident(COLUNA_ANO)} = %s", (str(ano_censo),)
        )
        for lote in arquivo.iter_batches(batch_size=tamanho_do_lote(arquivo)):
            cursor.copy_expert(
                f"COPY {alvo} ({lista}) FROM STDIN WITH (FORMAT csv)",
                _copy_csv(linhas_para_destino(entidade, lote, colunas)),
            )
        cursor.execute(
            f"SELECT count(*) FROM {alvo} WHERE {_ident(COLUNA_ANO)} = %s",
            (str(ano_censo),),
        )
        contagem = cursor.fetchone()
        carregadas = int(contagem[0]) if contagem else 0
    logging.info(
        "[cliente_inep] %s.%s ano %s: %d linhas", schema, tabela, ano_censo, carregadas
    )
    return carregadas


def _linhas_por_lote(num_colunas: int) -> int:
    # ~2 milhões de células por COPY, nunca menos de 500 linhas.
    return max(500, 2_000_000 // max(num_colunas, 1))


def tamanho_do_lote(arquivo: pq.ParquetFile) -> int:
    """Linhas por lote lido do Parquet, pelas colunas **do Parquet**.

    Não pelas da tabela de destino: no CENSOESC o destino tem ~12 colunas
    (as próprias e o ``jsonb``), mas cada linha lida tem até 3.808. Contar
    pelo destino dava lotes de 166 mil linhas e matava o worker por falta de
    memória.
    """
    return _linhas_por_lote(arquivo.metadata.num_columns)


def _criar_tabela_censoesc(conn_str: str, schema: str, tabela: str) -> None:
    import psycopg2

    with psycopg2.connect(conn_str) as conn, conn.cursor() as cursor:
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {_ident(schema)}")
        cursor.execute(
            f"CREATE TABLE IF NOT EXISTS {_ident(schema)}.{_ident(tabela)} ("
            f"{_ident(COLUNA_ANO)} TEXT, {_ident(COLUNA_DT_INGESTAO)} TEXT, "
            f"{_ident(COLUNA_JSONB)} JSONB)"
        )


TABELA_CONTROLE = "inep_controle_carga"


def registrar_carga(conn_str: str, schema: str, cargas: list[dict[str, Any]]) -> None:
    """Grava em ``inep_controle_carga`` uma linha por ano e entidade carregados.

    É a conferência contra a origem que a issue pede: ``linhas_csv`` é a
    contagem de quebras de linha do arquivo, ``linhas_parquet`` o que o
    parser leu e ``linhas_carregadas`` o que o banco tem depois do COPY.
    """
    import psycopg2
    from psycopg2.extras import execute_values

    alvo = f"{_ident(schema)}.{_ident(TABELA_CONTROLE)}"
    with psycopg2.connect(conn_str) as conn, conn.cursor() as cursor:
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {_ident(schema)}")
        cursor.execute(
            f"CREATE TABLE IF NOT EXISTS {alvo} ("
            "ano_censo INTEGER, entidade TEXT, arquivo TEXT, linhas_csv BIGINT, "
            "linhas_parquet BIGINT, linhas_carregadas BIGINT, key_raw TEXT, "
            "key_staging TEXT, dt_ingestao TIMESTAMPTZ, "
            "PRIMARY KEY (ano_censo, entidade))"
        )
        execute_values(
            cursor,
            f"INSERT INTO {alvo} VALUES %s ON CONFLICT (ano_censo, entidade) DO UPDATE "
            "SET arquivo = EXCLUDED.arquivo, linhas_csv = EXCLUDED.linhas_csv, "
            "linhas_parquet = EXCLUDED.linhas_parquet, "
            "linhas_carregadas = EXCLUDED.linhas_carregadas, "
            "key_raw = EXCLUDED.key_raw, key_staging = EXCLUDED.key_staging, "
            "dt_ingestao = EXCLUDED.dt_ingestao",
            [
                (
                    c["ano"],
                    c["entidade"],
                    c["arquivo"],
                    c["linhas_csv"],
                    c["linhas_parquet"],
                    c["linhas_carregadas"],
                    c["key_raw"],
                    c["key_staging"],
                    c["dt_ingestao"],
                )
                for c in cargas
            ],
        )


def conferir_contagens(cargas: list[dict[str, Any]]) -> None:
    """Falha se CSV, Parquet e banco não tiverem as mesmas linhas.

    Chamada depois de ``registrar_carga``: a divergência fica registrada
    na tabela de controle mesmo quando a task falha.
    """
    divergentes = [
        c
        for c in cargas
        if not c["linhas_csv"] == c["linhas_parquet"] == c["linhas_carregadas"]
    ]
    if divergentes:
        raise ValueError(
            "contagem diverge da origem (csv/parquet/banco): "
            + "; ".join(
                f"{c['ano']} {c['entidade']}: {c['linhas_csv']}/"
                f"{c['linhas_parquet']}/{c['linhas_carregadas']}"
                for c in divergentes
            )
        )


# ── rede ─────────────────────────────────────────────────────────────────


@functools.cache
def bundle_ca() -> str:
    """Bundle do certifi mais a intermediária do INEP, num arquivo temporário.

    O ``download.inep.gov.br`` envia só o certificado folha. Navegador e curl
    do macOS buscam a intermediária pelo AIA; o OpenSSL do ``requests``, não.
    A cadeia ICP-Brasil que a imagem do Airflow instala não ajuda: a
    emissora é da RNP, sob a GlobalSign Root R46.
    """
    import certifi

    destino = Path(tempfile.gettempdir()) / "inep_ca_bundle.pem"
    destino.write_text(
        Path(certifi.where()).read_text() + "\n" + _CERTIFICADO_INEP.read_text()
    )
    return str(destino)


def obter_pagina(timeout: int = 60) -> str:
    import requests

    resposta = requests.get(URL_PAGINA, timeout=timeout, verify=bundle_ca())
    resposta.raise_for_status()
    return resposta.text


def baixar(url: str, destino: str, timeout: int = 120) -> int:
    """Baixa ``url`` para ``destino`` em blocos e devolve os bytes gravados."""
    import requests

    total = 0
    with requests.get(url, stream=True, timeout=timeout, verify=bundle_ca()) as r:
        r.raise_for_status()
        with open(destino, "wb") as saida:
            for bloco in r.iter_content(chunk_size=8 * 1024 * 1024):
                saida.write(bloco)
                total += len(bloco)
    esperado = r.headers.get("Content-Length")
    if esperado is not None and int(esperado) != total:
        raise IOError(f"download incompleto de {url}: {total} de {esperado} bytes")
    logging.info("[cliente_inep] %s: %.1f MB", url, total / 1e6)
    return total


def abrir_membro(arquivo: zipfile.ZipFile, info: zipfile.ZipInfo) -> Any:
    """Fábrica para ``csv_para_parquet``/``contar_linhas``: um handle novo por chamada."""

    def abrir() -> IO[bytes]:
        return arquivo.open(info)

    return abrir
