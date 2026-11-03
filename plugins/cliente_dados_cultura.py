"""Planilhas do portal de dados abertos do MinC (dados.cultura.gov.br, CKAN).

O primeiro recurso é a "Lista de municípios que compõem o SNC", o export da
Plataforma do Sistema Nacional de Cultura que os scripts do Eixo 1 do Cultura
em Números leem (``SNC.R`` e ``snc_figuras_tabelas.R``). Ela traz estados e
municípios na mesma aba, uma linha por ente: adesão, lei do sistema, órgão
gestor, conselho, fundo e plano de cultura, com PIB, IDH e população.

Conferido em 03/11/2026: 5.595 entes (27 UFs e 5.568 municípios), um
``Cod.IBGE`` por linha, 56 colunas, as mesmas da planilha interna dos
scripts. A versão publicada estava atrasada: a última atualização de ente
era de 24/02/2026, com 3.749 adesões publicadas, contra 3.945 na planilha
interna de outubro.

A planilha tem nome e endereço do prefeito e do gestor de cultura. São dados
públicos de agentes públicos, mas não entram em log.

``RECURSOS`` lista cada planilha pelo id do recurso no CKAN. A URL e a data de
modificação vêm do ``package_show``: o CKAN troca a URL quando o arquivo é
republicado.
"""

import io
import logging
import time
from dataclasses import dataclass
from typing import Any

import requests

from seeds_carga import linhas_para_registros, normalizar_colunas

URL_CKAN = "https://dados.cultura.gov.br/api/3/action"
COLUNA_VERSAO = "versao"


@dataclass(frozen=True)
class Recurso:
    conjunto: str
    recurso_id: str
    # Coluna que identifica a linha; a conversão falha se ela vier vazia ou
    # repetida, porque aí a planilha publicada mudou de forma.
    chave: str
    minimo_de_linhas: int


RECURSOS: dict[str, Recurso] = {
    "snc_entes": Recurso(
        "sistema-nacional-de-cultura",
        "7b9f3c2a-03ed-4e6c-ac9a-d63f5ed47472",
        chave="cod_ibge",
        # 27 UFs e cerca de 5,6 mil municípios; bem menos que isso é arquivo
        # cortado.
        minimo_de_linhas=5_000,
    ),
}


def tabela_seeds(entidade: str) -> str:
    return f"dados_cultura_{entidade}"


# O portal devolve 500 de vez em quando e responde na tentativa seguinte
# (conferido em 03/11/2026: um 500 e sete 200 seguidos no mesmo arquivo).
_TENTATIVAS = 3
_ESPERA_S = 5.0


def _get(url: str, timeout: int, **kwargs: Any) -> requests.Response:
    for tentativa in range(1, _TENTATIVAS + 1):
        resposta = requests.get(url, timeout=timeout, **kwargs)
        if resposta.status_code < 500 or tentativa == _TENTATIVAS:
            resposta.raise_for_status()
            return resposta
        logging.warning(
            "[cliente_dados_cultura] %s: %d, tentativa %d de %d",
            url,
            resposta.status_code,
            tentativa,
            _TENTATIVAS,
        )
        time.sleep(_ESPERA_S * tentativa)
    raise AssertionError("inalcançável")


def descrever(recurso: Recurso, timeout: int = 60) -> dict[str, Any]:
    """URL e data de modificação do recurso, pelo ``package_show`` do conjunto."""
    resposta = _get(f"{URL_CKAN}/package_show", timeout, params={"id": recurso.conjunto})
    pacote = resposta.json()["result"]
    for item in pacote["resources"]:
        if item["id"] == recurso.recurso_id:
            modificado = item.get("last_modified") or item.get("created") or ""
            return {
                "url": item["url"],
                "versao": modificado[:10] or pacote["metadata_modified"][:10],
            }
    raise ValueError(f"recurso {recurso.recurso_id} não está mais em {recurso.conjunto}")


def baixar(url: str, timeout: int = 300) -> bytes:
    resposta = _get(url, timeout)
    logging.info("[cliente_dados_cultura] %s: %d bytes", url, len(resposta.content))
    return resposta.content


def linhas_da_planilha(conteudo: bytes) -> list[list[Any]]:
    """As linhas da primeira aba. O recurso diz XLS, mas o arquivo é xlsx: o
    ``calamine`` reconhece pelo conteúdo, não pela extensão."""
    from python_calamine import CalamineWorkbook

    livro = CalamineWorkbook.from_filelike(io.BytesIO(conteudo))
    linhas: list[list[Any]] = livro.get_sheet_by_name(livro.sheet_names[0]).to_python(
        skip_empty_area=False
    )
    return linhas


def registros(
    recurso: Recurso, linhas: list[list[Any]], versao: str
) -> list[dict[str, str | None]]:
    """Uma linha por ente, com o cabeçalho normalizado e a versão do arquivo.

    Falha se faltar a coluna-chave, se ela vier vazia ou repetida, ou se vier
    menos linha que o esperado: em qualquer desses casos o arquivo publicado
    não é mais o que a carga conhece.
    """
    if not linhas:
        raise ValueError("planilha vazia")
    colunas = normalizar_colunas(linhas[0])
    if COLUNA_VERSAO in colunas:
        raise ValueError(f"a planilha já tem coluna {COLUNA_VERSAO}")
    if recurso.chave not in colunas:
        raise ValueError(f"a planilha não tem a coluna {recurso.chave}: {colunas[:8]}")
    saida = [
        {**r, COLUNA_VERSAO: versao} for r in linhas_para_registros(colunas, linhas[1:])
    ]
    chaves = [r[recurso.chave] for r in saida]
    if any(c is None for c in chaves):
        raise ValueError(f"{recurso.chave} vazio em alguma linha")
    if len(set(chaves)) != len(chaves):
        raise ValueError(f"{recurso.chave} repetido na planilha")
    if len(saida) < recurso.minimo_de_linhas:
        raise ValueError(
            f"{len(saida)} linhas, esperava ao menos {recurso.minimo_de_linhas}"
        )
    return saida
