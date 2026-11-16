"""Cadastro de salas de exibição e complexos da ANCINE, nos dados abertos.

O Eixo 5 do Cultura em Números conta as salas de cinema em funcionamento por
município. O script do eixo lia um ``ANCINE.xlsx`` guardado no repositório
dele, sem origem registrada. A ANCINE publica o mesmo cadastro no Portal
Brasileiro de Dados Abertos, no conjunto "Salas de Exibição e Complexos
Registrados na Ancine" (``CONJUNTO``), com atualização mensal.

O recurso CSV do conjunto aponta para o servidor da própria ANCINE
(``URL_CSV``), num endereço fixo que não muda quando o arquivo é republicado.
A API do dados.gov.br pede chave de acesso, então a carga não passa por ela.

**A versão é o ``Last-Modified`` do arquivo**, no fuso de Brasília. O CSV não
traz data de referência própria. O servidor da ANCINE manda ``Last-Modified`` e
``ETag``, e o portal mostra a mesma data como "Última alteração em um arquivo".
Sem o cabeçalho, ou com data no futuro, a carga falha em vez de adivinhar;
nesses casos a versão vem por parâmetro.

Conferido em 16/11/2026, versão 2026-11-06:

- 6.433 linhas × 32 colunas, UTF-8 com BOM, separador ``;``, nenhuma quebra de
  linha dentro de campo;
- 3.717 salas ``EM FUNCIONAMENTO``;
- não há código IBGE: o município vem por nome (``MUNICIPIO_COMPLEXO``) e UF
  (``UF_COMPLEXO``), vazios em 25 linhas;
- ``REGISTRO_SALA`` vem vazio nas 37 salas ``NÃO ENCONTRADA``, então dentro de
  uma versão quem identifica a sala é a linha do arquivo.

Os dados vão como vieram, em texto (ADR 0012). O filtro de situação e a
contagem por município são regra de negócio e ficam no dbt.
"""

import csv
import io
import logging
import re
from dataclasses import dataclass
from datetime import date, datetime, timezone
from email.utils import parsedate_to_datetime
from zoneinfo import ZoneInfo

import requests

from seeds_carga import linhas_para_registros, normalizar_colunas

CONJUNTO = (
    "https://dados.gov.br/dados/conjuntos-dados/"
    "salas-de-exibicao-e-complexos-registrados-na-ancine"
)
URL_CSV = "https://dados.ancine.gov.br/dados-abertos/salas-de-exibicao-e-complexos.csv"

ENTIDADE = "salas_exibicao"
COLUNA_VERSAO = "versao"
# Número do registro no arquivo; o cabeçalho é o 1. Sem quebra de linha dentro
# de campo, é o número da linha.
COLUNA_LINHA = "linha"
SEPARADOR = ";"
# 6.433 linhas em 16/11/2026; bem menos que isso é arquivo cortado.
MINIMO_DE_LINHAS = 5_000
COLUNAS_ESPERADAS = (
    "registro_sala",
    "situacao_sala",
    "registro_complexo",
    "situacao_complexo",
    "municipio_complexo",
    "uf_complexo",
)
FUSO = ZoneInfo("America/Sao_Paulo")

_VERSAO = re.compile(r"^\d{4}-\d{2}-\d{2}$")


@dataclass(frozen=True)
class Arquivo:
    conteudo: bytes
    last_modified: str | None
    etag: str | None


def tabela_seeds() -> str:
    return f"ancine_{ENTIDADE}"


def baixar(url: str = URL_CSV, timeout: int = 120) -> Arquivo:
    """O CSV inteiro, com os cabeçalhos que dizem a versão.

    Se o endereço passar a responder uma página em vez do arquivo, falha aqui:
    o status 200 sozinho não garante que veio o CSV.
    """
    resposta = requests.get(url, timeout=timeout)
    resposta.raise_for_status()
    conteudo = resposta.content
    if not conteudo.strip() or conteudo.lstrip().startswith(b"<"):
        raise ValueError(
            f"a resposta de {url} não é o CSV "
            f"({resposta.headers.get('content-type')}): o arquivo mudou de "
            f"endereço? Confira o recurso CSV em {CONJUNTO}"
        )
    arquivo = Arquivo(
        conteudo=conteudo,
        last_modified=resposta.headers.get("Last-Modified"),
        etag=resposta.headers.get("ETag"),
    )
    logging.info(
        "[cliente_ancine] %s: %d bytes, Last-Modified %s, ETag %s",
        url,
        len(conteudo),
        arquivo.last_modified,
        arquivo.etag,
    )
    return arquivo


def hoje_em_brasilia() -> date:
    return datetime.now(FUSO).date()


def versao_do_arquivo(
    last_modified: str | None, informada: str | None = None, hoje: date | None = None
) -> str:
    """A versão, ``AAAA-MM-DD``: o dia do ``Last-Modified`` em Brasília, ou ``informada``.

    Falha sem o cabeçalho, com cabeçalho que não é data ou com data depois de
    ``hoje``. ``informada`` substitui o cabeçalho e passa pela mesma conferência
    de data.
    """
    hoje = hoje or hoje_em_brasilia()
    if informada:
        if not _VERSAO.match(informada):
            raise ValueError(f"versão informada {informada!r} não é AAAA-MM-DD")
        try:
            versao = date.fromisoformat(informada)
        except ValueError as erro:
            raise ValueError(f"versão informada {informada!r} inválida") from erro
        origem = "informada"
    else:
        if not last_modified:
            raise ValueError(
                "o servidor não mandou Last-Modified: sem ele não há como saber a "
                f"versão. Confira a data do arquivo em {CONJUNTO} e dispare com "
                '{"versao": "AAAA-MM-DD"}'
            )
        try:
            momento = parsedate_to_datetime(last_modified)
        except (TypeError, ValueError) as erro:
            raise ValueError(
                f"Last-Modified {last_modified!r} não é uma data HTTP"
            ) from erro
        if momento.tzinfo is None:
            # RFC 9110: a data HTTP é sempre em GMT.
            momento = momento.replace(tzinfo=timezone.utc)
        versao = momento.astimezone(FUSO).date()
        origem = f"Last-Modified {last_modified}"
    if versao > hoje:
        raise ValueError(f"versão ({origem}) {versao} depois de hoje ({hoje})")
    logging.info("[cliente_ancine] versão %s (%s)", versao, origem)
    return versao.isoformat()


def _texto(conteudo: bytes) -> str:
    try:
        return conteudo.decode("utf-8-sig")
    except UnicodeDecodeError as erro:
        raise ValueError(
            "o CSV deixou de vir em UTF-8; confira o arquivo antes de mudar o "
            f"encoding da carga ({erro})"
        ) from erro


def registros(
    conteudo: bytes, versao: str, minimo: int = MINIMO_DE_LINHAS
) -> list[dict[str, str | None]]:
    """Uma linha por sala, com a linha do arquivo e a versão.

    Falha se faltar uma coluna esperada ou se vier menos linha que o esperado:
    nesses casos o arquivo publicado não é mais o que a carga conhece. Não
    confere unicidade: o registro da sala vem vazio nas salas não encontradas.
    """
    linhas = list(
        csv.reader(io.StringIO(_texto(conteudo), newline=""), delimiter=SEPARADOR)
    )
    if not linhas:
        raise ValueError("CSV vazio")
    colunas = normalizar_colunas(linhas[0])
    for extra in (COLUNA_VERSAO, COLUNA_LINHA):
        if extra in colunas:
            raise ValueError(f"o CSV já tem a coluna {extra}")
    faltando = [c for c in COLUNAS_ESPERADAS if c not in colunas]
    if faltando:
        raise ValueError(f"o CSV não tem as colunas {faltando}: {colunas}")
    saida: list[dict[str, str | None]] = []
    # A linha inteiramente vazia é pulada sem renumerar as seguintes.
    for numero, linha in enumerate(linhas[1:], start=2):
        for registro in linhas_para_registros(colunas, [linha]):
            saida.append({**registro, COLUNA_LINHA: str(numero), COLUNA_VERSAO: versao})
    if len(saida) < minimo:
        raise ValueError(f"{len(saida)} linhas, esperava ao menos {minimo}")
    return saida


def contar_linhas(conteudo: bytes) -> int:
    """Linhas de dados por quebra de linha, sem o parser: a conferência independente.

    Se um campo passar a trazer quebra de linha, esta contagem fica maior que a
    do parser e a carga falha na conferência, de propósito: é preciso olhar o
    arquivo.
    """
    return sum(
        1 for linha in _texto(conteudo).splitlines()[1:] if linha.strip(SEPARADOR + " ")
    )
