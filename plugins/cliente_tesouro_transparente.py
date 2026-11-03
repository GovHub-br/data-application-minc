"""CSVs do portal de dados abertos do Tesouro Transparente (CKAN).

O Eixo 1 do Cultura em Números calcula a participação da cultura na Receita
Corrente Líquida da União de 2003 em diante. O RREO do SICONFI cobre a União
de 2015 para a frente; antes disso, a RCL anual está no conjunto "Limites da
LRF para a União: Dívida Consolidada Líquida", que traz a RCL de cada
quadrimestre desde 2000 (o "Até o 3º Quadrimestre" é o ano fechado). O
conjunto parou de ser atualizado em 2021 e vai até o 2º quadrimestre de 2018.

``RECURSOS`` lista cada CSV pelo id do recurso no CKAN. A URL de download e a
data da última modificação vêm do ``package_show``, não ficam no código: o
CKAN troca a URL quando o arquivo é republicado.
"""

import csv
import io
import logging
from dataclasses import dataclass
from typing import Any

import requests

from seeds_carga import linhas_para_registros, normalizar_colunas

URL_CKAN = "https://www.tesourotransparente.gov.br/ckan/api/3/action"
COLUNA_VERSAO = "versao"
# O período da carga é o recurso inteiro: cada versão do CSV traz a série toda,
# então a nova substitui a anterior, em vez de somar a ela.
COLUNA_RECURSO = "recurso_id"


@dataclass(frozen=True)
class Recurso:
    conjunto: str
    recurso_id: str
    encoding: str = "latin-1"
    separador: str = ";"


RECURSOS: dict[str, Recurso] = {
    "lrf_uniao_dcl": Recurso(
        "limites-da-lrf-para-a-uniao-divida-consolidada-liquida-da-uniao",
        "90adbb1e-2d22-4398-ac09-29145b4cc89d",
    ),
}


def tabela_seeds(entidade: str) -> str:
    return f"tesouro_transparente_{entidade}"


def descrever(recurso: Recurso, timeout: int = 60) -> dict[str, Any]:
    """URL e data de modificação do recurso, pelo ``package_show`` do conjunto."""
    resposta = requests.get(
        f"{URL_CKAN}/package_show", params={"id": recurso.conjunto}, timeout=timeout
    )
    resposta.raise_for_status()
    pacote = resposta.json()["result"]
    for item in pacote["resources"]:
        if item["id"] == recurso.recurso_id:
            modificado = item.get("last_modified") or item.get("created") or ""
            return {
                "url": item["url"],
                "versao": modificado[:10] or pacote["metadata_modified"][:10],
            }
    raise ValueError(f"recurso {recurso.recurso_id} não está mais em {recurso.conjunto}")


def baixar(url: str, timeout: int = 120) -> bytes:
    resposta = requests.get(url, timeout=timeout)
    resposta.raise_for_status()
    logging.info(
        "[cliente_tesouro_transparente] %s: %d bytes", url, len(resposta.content)
    )
    return resposta.content


def registros(
    recurso: Recurso, conteudo: bytes, versao: str
) -> list[dict[str, str | None]]:
    """As linhas do CSV, com o cabeçalho normalizado, a versão e o recurso."""
    texto = conteudo.decode(recurso.encoding)
    linhas = list(csv.reader(io.StringIO(texto, newline=""), delimiter=recurso.separador))
    if not linhas:
        raise ValueError("CSV vazio")
    colunas = normalizar_colunas(linhas[0])
    repetidas = {COLUNA_VERSAO, COLUNA_RECURSO} & set(colunas)
    if repetidas:
        raise ValueError(f"o CSV já tem as colunas {sorted(repetidas)}")
    return [
        {**r, COLUNA_VERSAO: versao, COLUNA_RECURSO: recurso.recurso_id}
        for r in linhas_para_registros(colunas, linhas[1:])
    ]


def contar_linhas(recurso: Recurso, conteudo: bytes) -> int:
    """Linhas de dados por quebra de linha, sem o parser: a conferência independente."""
    texto = conteudo.decode(recurso.encoding)
    return sum(
        1 for linha in texto.splitlines()[1:] if linha.strip(recurso.separador + " ")
    )
