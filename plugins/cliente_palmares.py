"""Planilha de comunidades quilombolas certificadas da Fundação Cultural Palmares.

A página "Certificação Quilombola" da Palmares (``PAGINA``) diz que a lista é
atualizada mensalmente e aponta para um Google Sheets público, que exporta em
xlsx sem login (``URL_EXPORT``). O arquivo se chama
``TABELA_DE_CRQ_CERTIFICADAS.xlsx`` e tem quatro abas: ``CERTIFICADAS`` (uma
linha por certidão de autodefinição), ``DADOS`` (resumos por estado e por ano,
em fórmulas sobre ``CERTIFICADAS``) e ``INDICADORES CGE 2022`` e ``2021``.

Conferido em 12/11/2026: 3.578 certidões × 21 colunas, nenhuma fórmula em
``CERTIFICADAS``, portarias no DOU de 04/06/2004 a 05/11/2026 e 4.783
comunidades (a soma de ``Nº DE CRQ``: uma certidão pode cobrir várias).

**A versão é o carimbo da aba DADOS.** A célula ``A3`` traz, em texto fixo,
``*ÚLTIMA ATUALIZAÇÃO: 11/11/2026``. Nem o HTTP (sem ``Last-Modified`` nem
``ETag``) nem o xlsx (sem ``docProps``) dizem quando a planilha mudou, e o
carimbo é o que a própria Palmares declara: faz o papel do ``last_modified``
do CKAN no ``cliente_dados_cultura``. A carga falha, em vez de adivinhar, se o
carimbo faltar, vier no futuro ou for anterior à portaria mais recente da
planilha (aí ele está velho). Nesses casos a versão vem por parâmetro.

**A planilha é corrigida, não só acrescida.** Entre a versão que o Eixo 3 usou
(até 23/07/2026) e a de 11/11/2026, 2026 ganhou 104 certidões e o total subiu
100. Por isso a recarga substitui a versão inteira.

**Descartados.** O export em CSV traz uma aba só (perde o carimbo) e as datas
como texto formatado. A página de Dados Abertos da Palmares tem xlsx e CSV,
mas parados na "Posição 14.04.2025"; a pasta ``comunidades-certificadas``,
arquivos de 2022 e 2023.

Os dados vão como vieram, em texto (ADR 0012). O que quem usa precisa saber
(certidão × comunidade, registro repetido, portaria que o Excel virou data,
municípios e códigos IBGE com erro) está no domínio ``palmares`` do
``docs-pages/src/dominios.yml``.
"""

import io
import logging
import re
from datetime import date
from typing import Any

import requests

from seeds_carga import linhas_para_registros, normalizar_colunas, slug

PAGINA = (
    "https://www.gov.br/palmares/pt-br/departamentos/"
    "protecao-preservacao-e-articulacao/certificacao-quilombola"
)
PLANILHA_ID = "1WBjixnnjJWrDXsA2WvElj65rrZ4nkNM-u5LclRV0lGs"
URL_EXPORT = f"https://docs.google.com/spreadsheets/d/{PLANILHA_ID}/export?format=xlsx"

ENTIDADE = "quilombos_certificados"
ABA = "CERTIFICADAS"
COLUNA_VERSAO = "versao"
# Número da linha na planilha (o cabeçalho é a 1). O número do registro
# repete (6 números em 14 linhas em 12/11/2026), então a linha é o que
# identifica a certidão dentro de uma versão.
COLUNA_LINHA = "linha"
COLUNA_DOU = "data_da_portaria_no_dou"
# 3.578 linhas em 12/11/2026; bem menos que isso é arquivo cortado.
MINIMO_DE_LINHAS = 3_000
COLUNAS_ESPERADAS = (
    "uf",
    "municipio",
    "codigo_do_ibge",
    "comunidade",
    "no_do_registro",
    COLUNA_DOU,
    "no_de_crq",
)

# Sobre o texto já passado pelo ``slug``: "*ÚLTIMA ATUALIZAÇÃO: 11/11/2026"
# vira "ultima_atualizacao_11_11_2026".
_CARIMBO = re.compile(r"atualizacao_(\d{1,2})_(\d{1,2})_(\d{4})(?!\d)")
_VERSAO = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_ASSINATURA_XLSX = b"PK\x03\x04"


def tabela_seeds() -> str:
    return f"palmares_{ENTIDADE}"


def baixar(url: str = URL_EXPORT, timeout: int = 120) -> bytes:
    """O xlsx da planilha, inteiro.

    Se a planilha deixar de ser pública, o Google responde 200 com a página
    de login; por isso confere a assinatura do zip em vez de só o status.
    """
    resposta = requests.get(url, timeout=timeout)
    resposta.raise_for_status()
    conteudo = resposta.content
    if not conteudo.startswith(_ASSINATURA_XLSX):
        raise ValueError(
            f"a resposta de {url} não é xlsx "
            f"({resposta.headers.get('content-type')}): a planilha deixou de ser "
            f"pública ou mudou de endereço? Confira o link em {PAGINA}"
        )
    logging.info("[cliente_palmares] %s: %d bytes", url, len(conteudo))
    return conteudo


def _livro(conteudo: bytes) -> Any:
    from python_calamine import CalamineWorkbook

    return CalamineWorkbook.from_filelike(io.BytesIO(conteudo))


def _linhas(livro: Any, aba: str) -> list[list[Any]]:
    linhas: list[list[Any]] = livro.get_sheet_by_name(aba).to_python(
        skip_empty_area=False
    )
    return linhas


def _linhas_certificadas(livro: Any) -> list[list[Any]]:
    if ABA not in livro.sheet_names:
        raise ValueError(f"a planilha não tem a aba {ABA}: {livro.sheet_names}")
    linhas = _linhas(livro, ABA)
    if not linhas:
        raise ValueError(f"aba {ABA} vazia")
    return linhas


def carimbos(livro: Any) -> set[date]:
    """As datas de "ÚLTIMA ATUALIZAÇÃO" escritas nas abas que não são de dados."""
    datas: set[date] = set()
    for aba in livro.sheet_names:
        if aba == ABA:
            continue
        for linha in _linhas(livro, aba):
            for celula in linha:
                if not isinstance(celula, str):
                    continue
                for dia, mes, ano in _CARIMBO.findall(slug(celula)):
                    try:
                        datas.add(date(int(ano), int(mes), int(dia)))
                    except ValueError as erro:
                        raise ValueError(
                            f"carimbo de atualização com data inválida: {celula!r}"
                        ) from erro
    return datas


def maior_data_dou(livro: Any, hoje: date) -> date | None:
    """A portaria mais recente da planilha, sem as datas depois de ``hoje``."""
    linhas = _linhas_certificadas(livro)
    colunas = normalizar_colunas(linhas[0])
    if COLUNA_DOU not in colunas:
        raise ValueError(f"a planilha não tem a coluna {COLUNA_DOU}: {colunas[:8]}")
    indice = colunas.index(COLUNA_DOU)
    datas = [
        linha[indice]
        for linha in linhas[1:]
        if indice < len(linha) and isinstance(linha[indice], date)
    ]
    # ``datetime`` é subclasse de ``date``; a comparação é pelo dia.
    dias = [d.date() if hasattr(d, "date") else d for d in datas]
    futuras = sorted({d for d in dias if d > hoje})
    if futuras:
        logging.warning(
            "[cliente_palmares] portarias no DOU depois de %s, ignoradas na "
            "conferência da versão: %s",
            hoje,
            [d.isoformat() for d in futuras],
        )
    validas = [d for d in dias if d <= hoje]
    return max(validas) if validas else None


def versao_da_planilha(
    conteudo: bytes, informada: str | None = None, hoje: date | None = None
) -> str:
    """A versão da planilha, ``AAAA-MM-DD``: o carimbo da aba DADOS ou ``informada``.

    Falha sem carimbo, com carimbos diferentes, com data no futuro ou anterior
    à portaria mais recente da planilha. ``informada`` substitui o carimbo e
    passa pelas mesmas conferências.
    """
    hoje = hoje or date.today()
    livro = _livro(conteudo)
    if informada:
        if not _VERSAO.match(informada):
            raise ValueError(f"versão informada {informada!r} não é AAAA-MM-DD")
        try:
            versao = date.fromisoformat(informada)
        except ValueError as erro:
            raise ValueError(f"versão informada {informada!r} inválida") from erro
        origem = "informada"
    else:
        datas = carimbos(livro)
        if not datas:
            raise ValueError(
                "a planilha não traz o carimbo 'ÚLTIMA ATUALIZAÇÃO: dd/mm/aaaa' "
                "fora da aba de dados: sem ele não há como saber a versão. "
                'Confira a planilha e dispare com {"versao": "AAAA-MM-DD"}'
            )
        if len(datas) > 1:
            raise ValueError(
                "a planilha traz mais de um carimbo de atualização: "
                f"{sorted(d.isoformat() for d in datas)}. "
                'Dispare com {"versao": "AAAA-MM-DD"}'
            )
        (versao,) = datas
        origem = "carimbo"
    if versao > hoje:
        raise ValueError(f"versão ({origem}) {versao} depois de hoje ({hoje})")
    ultima = maior_data_dou(livro, hoje)
    if ultima and versao < ultima:
        conserto = (
            "informe uma data a partir dela"
            if informada
            else 'o carimbo está velho. Confira a planilha e dispare com {"versao": '
            '"AAAA-MM-DD"}'
        )
        raise ValueError(
            f"versão ({origem}) {versao} anterior à portaria mais recente da "
            f"planilha ({ultima}): {conserto}"
        )
    logging.info(
        "[cliente_palmares] versão %s (%s); portaria mais recente %s",
        versao,
        origem,
        ultima,
    )
    return versao.isoformat()


def registros(
    conteudo: bytes, versao: str, minimo: int = MINIMO_DE_LINHAS
) -> list[dict[str, str | None]]:
    """Uma linha por certidão da aba CERTIFICADAS, com a linha da planilha e a versão.

    Falha se faltar a aba ou uma coluna esperada, ou se vier menos linha que
    o esperado: nesses casos a planilha publicada não é mais a que a carga
    conhece. Não confere unicidade: o número do registro repete.
    """
    linhas = _linhas_certificadas(_livro(conteudo))
    colunas = normalizar_colunas(linhas[0])
    for extra in (COLUNA_VERSAO, COLUNA_LINHA):
        if extra in colunas:
            raise ValueError(f"a planilha já tem coluna {extra}")
    faltando = [c for c in COLUNAS_ESPERADAS if c not in colunas]
    if faltando:
        raise ValueError(f"a planilha não tem as colunas {faltando}: {colunas}")
    saida: list[dict[str, str | None]] = []
    # O cabeçalho é a linha 1; a linha inteiramente vazia é pulada sem
    # renumerar as seguintes.
    for numero, linha in enumerate(linhas[1:], start=2):
        for registro in linhas_para_registros(colunas, [linha]):
            saida.append({**registro, COLUNA_LINHA: str(numero), COLUNA_VERSAO: versao})
    if len(saida) < minimo:
        raise ValueError(f"{len(saida)} linhas, esperava ao menos {minimo}")
    return saida
