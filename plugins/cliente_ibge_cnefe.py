"""CNEFE do Censo 2022: os endereços de estabelecimento, um ZIP por município.

O Cadastro Nacional de Endereços para Fins Estatísticos (CNEFE) lista cada
endereço que o recenseador percorreu no Censo Demográfico 2022, com a espécie
(domicílio, estabelecimento de ensino, de saúde...) e, quando é
estabelecimento, o nome dele. O IBGE publica um ZIP por município, numa pasta
por UF::

    .../Arquivos_CNEFE/CSV/Municipio/<cod_uf>_<sigla>/<cod_municipio>_<NOME>.zip

São 5.570 ZIPs e 2,45 GB compactados (inventário de 16/11/2026; São Paulo
capital sozinho tem 186 MB). Cada ZIP tem um CSV separado por ``;``, com
quebra de linha CRLF, cabeçalho de 34 colunas, sem aspas e em ASCII: o IBGE
tira os acentos. O nome do ZIP às vezes tem acento (``2907400_CATOLÂNDIA``) e
o do CSV dentro dele vem em cp437, por isso o município é identificado pelos
sete dígitos do código, nunca pelo nome.

O eixo 5 do Cultura em Números usa duas espécies de endereço (``ESPECIES``) e
16 colunas (``COLUNAS``). O raw guarda o ZIP inteiro; o staging e o ``seeds``
guardam só essas linhas e colunas, todas como texto (ADR 0012). A varredura
dos nomes de estabelecimento atrás de equipamento cultural é regra de negócio
e fica no dbt.

Conferência contra a origem, em três níveis:

- a listagem de cada UF tem que ter os municípios de ``UFS``;
- em cada CSV, as quebras de linha têm que bater com as linhas que o parser
  leu (``converter_municipio`` falha se não baterem);
- as linhas das ``ESPECIES`` contadas nos bytes, sem o parser, vão para o
  controle de carga ao lado das do Parquet e das do banco.

As funções de listagem, contagem e conversão são puras para serem testáveis
sem rede, MinIO ou Postgres.
"""

from __future__ import annotations

import functools
import io
import logging
import re
import zipfile
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from datetime import datetime
from typing import IO, Callable
from urllib.parse import unquote

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pa_csv
import pyarrow.parquet as pq
import requests

from byte_nulo import tabela_sem_byte_nulo
from retry_helpers import retry_on_exception

URL_BASE = (
    "https://ftp.ibge.gov.br/Cadastro_Nacional_de_Enderecos_para_Fins_Estatisticos/"
    "Censo_Demografico_2022/Arquivos_CNEFE/CSV"
)
URL_MUNICIPIOS = f"{URL_BASE}/Municipio"
URL_DICIONARIO = f"{URL_BASE}/Dicionario_CNEFE_Censo_2022.xls"

ENTIDADE = "cnefe_2022"
TABELA_SEEDS = f"ibge_{ENTIDADE}"
COLUNA_UF = "uf"
COLUNA_DT_INGEST = "dt_ingest"


@dataclass(frozen=True)
class Uf:
    """Uma UF do catálogo: o código IBGE e quantos ZIPs a pasta dela tem."""

    codigo: str
    municipios: int


# Municípios por UF no Censo 2022: é o que a listagem de cada pasta tem que
# trazer. Somam 5.570 (o DF é um ZIP só).
UFS: dict[str, Uf] = {
    "RO": Uf("11", 52),
    "AC": Uf("12", 22),
    "AM": Uf("13", 62),
    "RR": Uf("14", 15),
    "PA": Uf("15", 144),
    "AP": Uf("16", 16),
    "TO": Uf("17", 139),
    "MA": Uf("21", 217),
    "PI": Uf("22", 224),
    "CE": Uf("23", 184),
    "RN": Uf("24", 167),
    "PB": Uf("25", 223),
    "PE": Uf("26", 185),
    "AL": Uf("27", 102),
    "SE": Uf("28", 75),
    "BA": Uf("29", 417),
    "MG": Uf("31", 853),
    "ES": Uf("32", 78),
    "RJ": Uf("33", 92),
    "SP": Uf("35", 645),
    "PR": Uf("41", 399),
    "SC": Uf("42", 295),
    "RS": Uf("43", 497),
    "MS": Uf("50", 79),
    "MT": Uf("51", 141),
    "GO": Uf("52", 246),
    "DF": Uf("53", 1),
}

# Espécies de endereço que o eixo 5 lê (``especies = c(4L, 6L)`` no script R).
# Rótulos do dicionário do IBGE (URL_DICIONARIO), conferidos em 16/11/2026:
#   4 = Estabelecimento de ensino
#   6 = Estabelecimento de outras finalidades
# As outras: 1 domicílio particular, 2 domicílio coletivo, 3 estabelecimento
# agropecuário, 5 de saúde, 7 edificação em construção ou reforma, 8
# estabelecimento religioso.
ESPECIES = ("4", "6")
COLUNA_ESPECIE = "COD_ESPECIE"

# As colunas que o eixo 5 lê, com o nome do cabeçalho do CSV. As outras 18
# (distrito, quadra, face, complementos, indicadores) ficam só no raw.
COLUNAS = (
    "COD_UNICO_ENDERECO",
    "COD_UF",
    "COD_MUNICIPIO",
    "COD_SETOR",
    "CEP",
    "DSC_LOCALIDADE",
    "NOM_TIPO_SEGLOGR",
    "NOM_TITULO_SEGLOGR",
    "NOM_SEGLOGR",
    "NUM_ENDERECO",
    "DSC_MODIFICADOR",
    "LATITUDE",
    "LONGITUDE",
    "NV_GEO_COORD",
    COLUNA_ESPECIE,
    "DSC_ESTABELECIMENTO",
)
COLUNAS_DESTINO = [c.lower() for c in COLUNAS] + [COLUNA_UF, COLUNA_DT_INGEST]
SCHEMA_ARROW = pa.schema([pa.field(c, pa.string()) for c in COLUNAS_DESTINO])

SEPARADOR = ";"

# Bloco de leitura do CSV. Lido em blocos, a memória fica no tamanho das linhas
# que passam no filtro, e não no do CSV: só o ZIP de São Paulo capital tem 186 MB.
_BLOCO_BYTES = 4 * 1024 * 1024

_HREF_ZIP = re.compile(r'href="((\d{7})_[^"/?]+\.zip)"', re.IGNORECASE)


# ── listagem ─────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class ArquivoMunicipio:
    """Um ZIP da listagem: o código do município, o nome publicado e a URL."""

    codigo: str
    nome: str
    url: str


def url_da_pasta(uf: str) -> str:
    return f"{URL_MUNICIPIOS}/{UFS[uf].codigo}_{uf}/"


def extrair_zips(html: str, url_pasta: str) -> list[ArquivoMunicipio]:
    """Os ZIPs da listagem de uma pasta do FTP (o índice HTML do Apache).

    O ``href`` vem percent-encoded (``2907400_CATOL%c3%82NDIA.zip``): a URL o
    usa como veio, e o nome é o decodificado.
    """
    achados: dict[str, ArquivoMunicipio] = {}
    for achado in _HREF_ZIP.finditer(html):
        href, codigo = achado.group(1), achado.group(2)
        achados.setdefault(
            href, ArquivoMunicipio(codigo, unquote(href), f"{url_pasta}{href}")
        )
    return sorted(achados.values(), key=lambda a: (a.codigo, a.nome))


def conferir_listagem(uf: str, arquivos: list[ArquivoMunicipio]) -> None:
    """Falha se a pasta da UF não tem exatamente os municípios esperados."""
    info = UFS[uf]
    codigos = [a.codigo for a in arquivos]
    fora = sorted(c for c in codigos if not c.startswith(info.codigo))
    repetidos = sorted({c for c in codigos if codigos.count(c) > 1})
    if len(arquivos) != info.municipios or fora or repetidos:
        raise ValueError(
            f"{uf}: a listagem tem {len(arquivos)} ZIPs, esperava {info.municipios}; "
            f"códigos de outra UF: {fora}; códigos repetidos: {repetidos}. "
            "Se o IBGE republicou a pasta, confira antes de mudar UFS."
        )


# ── CSV ──────────────────────────────────────────────────────────────────


def membro_csv(arquivo: zipfile.ZipFile, codigo: str) -> zipfile.ZipInfo:
    """O único CSV do ZIP, que tem que começar pelo código do município."""
    csvs = [
        info
        for info in arquivo.infolist()
        if not info.is_dir() and info.filename.lower().endswith(".csv")
    ]
    if len(csvs) != 1:
        raise ValueError(
            f"{codigo}: esperava um CSV no ZIP, achei {[i.filename for i in csvs]}"
        )
    if not csvs[0].filename.startswith(f"{codigo}_"):
        raise ValueError(
            f"{codigo}: o CSV do ZIP é de outro município: {csvs[0].filename}"
        )
    return csvs[0]


def _linhas(bruto: IO[bytes]) -> Iterator[bytes]:
    """As linhas do arquivo, sem o ``\\n`` (o ``\\r`` do CRLF fica)."""
    resto = b""
    for bloco in iter(functools.partial(bruto.read, 1024 * 1024), b""):
        pedacos = (resto + bloco).split(b"\n")
        resto = pedacos.pop()
        yield from pedacos
    if resto:
        yield resto


def colunas_do_cabecalho(linha: bytes) -> list[str]:
    return linha.decode("utf-8").rstrip("\r\n").split(SEPARADOR)


def conferir_cabecalho(colunas: list[str]) -> None:
    faltando = [c for c in COLUNAS if c not in colunas]
    repetidas = sorted({c for c in colunas if colunas.count(c) > 1})
    if faltando or repetidas:
        raise ValueError(
            f"o cabeçalho do CSV mudou: faltam {faltando}; repetidas {repetidas}"
        )


def contar_linhas(abrir: Callable[[], IO[bytes]]) -> tuple[int, int]:
    """``(linhas de dados, linhas das ESPECIES)`` contadas nos bytes.

    É a contagem independente do parser: cada linha é separada por ``\\n`` e a
    espécie lida pela posição no cabeçalho. Linha vazia não conta, como no
    parser. Se o parser fundir ou perder uma linha, a primeira contagem não
    bate com a dele; se errar a coluna, a segunda não bate com o Parquet.
    """
    alvo = {e.encode() for e in ESPECIES}
    total = especies = 0
    with abrir() as bruto:
        linhas = _linhas(bruto)
        posicao = colunas_do_cabecalho(next(linhas, b"")).index(COLUNA_ESPECIE)
        for linha in linhas:
            if not linha.rstrip(b"\r"):
                continue
            total += 1
            campos = linha.split(SEPARADOR.encode(), posicao + 1)
            if len(campos) > posicao and campos[posicao].rstrip(b"\r") in alvo:
                especies += 1
    return total, especies


def csv_para_tabela(
    abrir: Callable[[], IO[bytes]], uf: str, dt_ingest: datetime
) -> tuple[pa.Table, int]:
    """O CSV de ``abrir()`` filtrado nas ``ESPECIES``, e as linhas que o parser leu.

    ``abrir`` é chamado duas vezes (cabeçalho e dados) e deve devolver um
    arquivo binário novo a cada chamada. Toda coluna é texto, com nome em
    minúsculas; vazio vira nulo, e nada mais vira (``NA`` continua ``NA``).
    Aspas não delimitam campo: o IBGE não as usa, e com elas uma aspa solta
    num nome de estabelecimento engoliria a linha seguinte. Linha com coluna a
    mais ou a menos, ou byte fora do UTF-8, falha a leitura.

    Acrescenta ``uf`` (a sigla da pasta, não o dado) e ``dt_ingest``.
    """
    with abrir() as bruto:
        colunas = colunas_do_cabecalho(bruto.readline())
    conferir_cabecalho(colunas)

    alvo = pa.array(ESPECIES, pa.string())
    lidas = 0
    lotes: list[pa.RecordBatch] = []
    with abrir() as bruto:
        leitor = pa_csv.open_csv(
            bruto,
            read_options=pa_csv.ReadOptions(
                column_names=colunas,
                skip_rows=1,
                block_size=_BLOCO_BYTES,
                use_threads=False,
                encoding="utf8",
            ),
            parse_options=pa_csv.ParseOptions(delimiter=SEPARADOR, quote_char=False),
            convert_options=pa_csv.ConvertOptions(
                include_columns=list(COLUNAS),
                column_types={c: pa.string() for c in COLUNAS},
                strings_can_be_null=True,
                null_values=[""],
            ),
        )
        for lote in leitor:
            lidas += lote.num_rows
            filtrado = lote.filter(pc.is_in(lote.column(COLUNA_ESPECIE), value_set=alvo))
            n = filtrado.num_rows
            lotes.append(
                pa.RecordBatch.from_arrays(
                    [filtrado.column(c) for c in COLUNAS]
                    + [
                        pa.array([uf] * n, pa.string()),
                        pa.array([dt_ingest.isoformat()] * n, pa.string()),
                    ],
                    schema=SCHEMA_ARROW,
                )
            )
    return pa.Table.from_batches(lotes, schema=SCHEMA_ARROW), lidas


@dataclass(frozen=True)
class Conversao:
    """O CSV de um município filtrado nas espécies, com as contagens dos bytes."""

    tabela: pa.Table
    arquivo: str
    linhas_arquivo: int
    linhas_especies: int


def converter_municipio(
    conteudo: bytes, codigo: str, uf: str, dt_ingest: datetime
) -> Conversao:
    """O ZIP de um município como tabela das ``ESPECIES``, conferida contra o CSV.

    Falha se as quebras de linha do CSV não baterem com as linhas que o parser
    leu. O ZIP confere o próprio CRC ao fim da leitura: raw corrompido falha
    aqui também.
    """
    with zipfile.ZipFile(io.BytesIO(conteudo)) as arquivo:
        info = membro_csv(arquivo, codigo)

        def abrir() -> IO[bytes]:
            return arquivo.open(info)

        linhas_arquivo, linhas_especies = contar_linhas(abrir)
        tabela, lidas = csv_para_tabela(abrir, uf, dt_ingest)
    if lidas != linhas_arquivo:
        raise ValueError(
            f"{codigo}: o CSV tem {linhas_arquivo} linhas e o parser leu {lidas}"
        )
    return Conversao(tabela, info.filename, linhas_arquivo, linhas_especies)


# ── Parquet -> Postgres ──────────────────────────────────────────────────


def _ident(nome: str) -> str:
    return '"' + nome.replace('"', '""') + '"'


def csv_para_copy(tabela: pa.Table) -> io.BytesIO:
    """CSV para ``COPY ... (FORMAT csv)``: nulo é campo vazio sem aspas.

    O pyarrow põe toda string entre aspas e deixa o nulo sem nada, que é como
    o ``COPY`` distingue os dois. Valor vazio não chega aqui (a leitura já o
    converteu em nulo).

    O byte nulo sai do texto (ver ``byte_nulo``).
    """
    buffer = io.BytesIO()
    pa_csv.write_csv(
        tabela_sem_byte_nulo(tabela),
        buffer,
        write_options=pa_csv.WriteOptions(include_header=False, quoting_style="needed"),
    )
    buffer.seek(0)
    return buffer


def conferir_parte(tabela: pa.Table, uf: str) -> None:
    """Uma parte de staging tem as colunas do destino e só linhas da ``uf``."""
    if tabela.column_names != COLUNAS_DESTINO:
        raise ValueError(f"parte com colunas {tabela.column_names}")
    coluna = tabela.column(COLUNA_UF)
    if tabela.num_rows and (
        coluna.null_count or not pc.all(pc.equal(coluna, uf)).as_py()
    ):
        raise ValueError(f"parte de staging com linhas fora da UF {uf}")


def carregar_uf(conn_str: str, schema: str, uf: str, partes: Iterable[bytes]) -> int:
    """Substitui a ``uf`` em ``schema.ibge_cnefe_2022`` pelas ``partes`` (Parquet).

    ``DELETE`` da UF e ``COPY`` de cada parte na mesma transação: uma falha no
    meio deixa a UF anterior intacta, reexecutar não duplica e as outras UFs
    não são tocadas. Devolve as linhas da UF no banco depois do ``COPY``.
    """
    import psycopg2

    from cliente_postgres import ClientPostgresDB

    db = ClientPostgresDB(conn_str)
    db.create_table_if_not_exists(
        dict.fromkeys(COLUNAS_DESTINO), TABELA_SEEDS, schema=schema
    )
    db._evolve_schema(COLUNAS_DESTINO, TABELA_SEEDS, schema=schema)

    alvo = f"{_ident(schema)}.{_ident(TABELA_SEEDS)}"
    lista = ", ".join(_ident(c) for c in COLUNAS_DESTINO)
    conn = psycopg2.connect(conn_str)
    try:
        with conn, conn.cursor() as cursor:
            cursor.execute(f"DELETE FROM {alvo} WHERE {_ident(COLUNA_UF)} = %s", (uf,))
            for parte in partes:
                tabela = pq.read_table(pa.BufferReader(parte))
                conferir_parte(tabela, uf)
                cursor.copy_expert(
                    f"COPY {alvo} ({lista}) FROM STDIN WITH (FORMAT csv)",
                    csv_para_copy(tabela),
                )
            cursor.execute(
                f"SELECT count(*) FROM {alvo} WHERE {_ident(COLUNA_UF)} = %s", (uf,)
            )
            contagem = cursor.fetchone()
            carregadas = int(contagem[0]) if contagem else 0
    finally:
        conn.close()
    logging.info(
        "[cliente_ibge_cnefe] %s.%s uf=%s: %d linhas",
        schema,
        TABELA_SEEDS,
        uf,
        carregadas,
    )
    return carregadas


# ── rede ─────────────────────────────────────────────────────────────────


class DownloadIncompleto(IOError):
    """O corpo recebido tem menos (ou mais) bytes que o ``Content-Length``."""


# O FTP do IBGE oscila: em 16/11/2026 uma listagem passou de 60 s sem
# resposta, ZIPs de 100 KB levaram até 36 s e uma conexão caiu (SSLEOFError)
# depois de minutos parada. Tempo de conexão curto, de leitura de 2 minutos, e
# cinco tentativas por arquivo (15 s, 30 s, 1 min e 2 min de espera) antes de a
# task falhar; se falhar, o retry da task retoma do município em que parou.
_TIMEOUT = (30, 120)
_TENTATIVAS = retry_on_exception(
    max_attempts=5,
    initial_delay=15.0,
    exceptions_to_retry=(requests.RequestException, IOError),
)


@_TENTATIVAS
def _pagina(url: str) -> str:
    resposta = requests.get(url, timeout=_TIMEOUT)
    resposta.raise_for_status()
    return resposta.text


def obter_listagem(uf: str) -> list[ArquivoMunicipio]:
    """Os ZIPs da pasta da UF, conferidos contra ``UFS``."""
    url = url_da_pasta(uf)
    arquivos = extrair_zips(_pagina(url), url)
    conferir_listagem(uf, arquivos)
    return arquivos


@_TENTATIVAS
def baixar(url: str) -> bytes:
    """O ZIP como o IBGE publica, conferido pelo ``Content-Length``."""
    with requests.get(url, stream=True, timeout=_TIMEOUT) as resposta:
        resposta.raise_for_status()
        corpo = io.BytesIO()
        for bloco in resposta.iter_content(chunk_size=1024 * 1024):
            corpo.write(bloco)
        esperado = resposta.headers.get("Content-Length")
    conteudo = corpo.getvalue()
    if esperado is not None and int(esperado) != len(conteudo):
        raise DownloadIncompleto(
            f"download incompleto de {url}: {len(conteudo)} de {esperado} bytes"
        )
    return conteudo
