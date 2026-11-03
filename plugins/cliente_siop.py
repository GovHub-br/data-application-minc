"""Execução orçamentária federal pelo endpoint SPARQL público do SIOP.

O SIOP publica a execução da LOA como dados ligados, um grafo por exercício
(``http://orcamento.dados.gov.br/<ano>/``), consultável em SPARQL sem
autenticação. É a mesma via do pacote ``orcamentoBR``, do MPO.

A consulta agrega cada exercício por órgão, unidade orçamentária, função e
resultado primário, com os valores do projeto de lei à execução. É o recorte
da planilha ``funcoes_orgaos_unidades_rp`` que os scripts do Eixo 1 do
Cultura em Números leem, e que hoje está carregada à mão em
``siop.execucao_orcamentaria``.

Duas coisas que a consulta precisa respeitar, conferidas em 02/11/2026:

- **A soma volta em centavos.** O Virtuoso arredonda ``SUM`` de decimal para
  inteiro (R$ 102.434.450,95 vira 102434451). ``SUM(?valor * 100)`` sai
  exato, e a divisão por 100 é feita aqui, em ``Decimal``.
- **Linha zerada também vem.** Em 2023 são 2.562 combinações contra 2.559 da
  planilha; as 3 a mais têm empenho zero. A soma do empenhado bate com a
  planilha até o centavo (R$ 4.593.770.423.086,37).
"""

import json
import logging
from decimal import Decimal
from typing import Any

import requests

URL_SPARQL = "https://www1.siop.planejamento.gov.br/sparql/"

# Coluna do resultado -> predicado do vocabulário da LOA. A ordem é a da
# tabela ``siop.execucao_orcamentaria``.
VALORES: dict[str, str] = {
    "projeto_lei": "valorProjetoLei",
    "dotacao_inicial": "valorDotacaoInicial",
    "dotacao_atual": "valorLeiMaisCredito",
    "empenhado": "valorEmpenhado",
    "liquidado": "valorLiquidado",
    "pago": "valorPago",
}
DIMENSOES = (
    "orgao_cod",
    "orgao_nome",
    "funcao_cod",
    "funcao_nome",
    "uo_cod",
    "uo_nome",
    "rp_cod",
    "rp_nome",
)
COLUNA_ANO = "ano"
COLUNAS = (COLUNA_ANO, *DIMENSOES, *VALORES)


def consulta(ano: int) -> str:
    """O SPARQL de um exercício, agregado nas dimensões de ``DIMENSOES``."""
    somas = " ".join(f"(SUM(?{c} * 100) AS ?{c}_centavos)" for c in VALORES)
    valores = " ".join(f"?i loa:{p} ?{c} ." for c, p in VALORES.items())
    return f"""
PREFIX loa: <http://vocab.e.gov.br/2013/09/loa#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
SELECT ?orgao_cod ?orgao_nome ?funcao_cod ?funcao_nome ?uo_cod ?uo_nome
       ?rp_cod ?rp_nome {somas}
WHERE {{
  GRAPH <http://orcamento.dados.gov.br/{int(ano)}/> {{
    {valores}
    OPTIONAL {{
      ?i loa:temUnidadeOrcamentaria ?u . ?u loa:codigo ?uo_cod .
      OPTIONAL {{ ?u rdfs:label ?uo_nome }}
      OPTIONAL {{
        ?u loa:temOrgao ?o . ?o loa:codigo ?orgao_cod .
        OPTIONAL {{ ?o rdfs:label ?orgao_nome }}
      }}
    }}
    OPTIONAL {{
      ?i loa:temFuncao ?f . ?f loa:codigo ?funcao_cod .
      OPTIONAL {{ ?f rdfs:label ?funcao_nome }}
    }}
    OPTIONAL {{
      ?i loa:temResultadoPrimario ?r . ?r loa:codigo ?rp_cod .
      OPTIONAL {{ ?r rdfs:label ?rp_nome }}
    }}
  }}
}}
GROUP BY ?orgao_cod ?orgao_nome ?funcao_cod ?funcao_nome ?uo_cod ?uo_nome
         ?rp_cod ?rp_nome
"""


def baixar(ano: int, timeout: int = 600) -> bytes:
    """A resposta da consulta do exercício, em JSON de resultados SPARQL."""
    resposta = requests.get(
        URL_SPARQL,
        params={"query": consulta(ano)},
        headers={"Accept": "application/sparql-results+json"},
        timeout=timeout,
    )
    resposta.raise_for_status()
    logging.info("[cliente_siop] %d: %d bytes", ano, len(resposta.content))
    return resposta.content


def _reais(centavos: str) -> str:
    """``"10243445095"`` centavos -> ``"102434450.95"``, sem passar por float."""
    return str((Decimal(centavos) / 100).quantize(Decimal("0.01")))


def registros(ano: int, resposta: bytes) -> list[dict[str, str | None]]:
    """Uma linha por combinação de dimensões, com os valores em reais (texto)."""
    corpo: dict[str, Any] = json.loads(resposta)
    linhas = []
    for item in corpo["results"]["bindings"]:
        linha: dict[str, str | None] = {COLUNA_ANO: str(int(ano))}
        for dimensao in DIMENSOES:
            celula = item.get(dimensao)
            linha[dimensao] = celula["value"] if celula else None
        for coluna in VALORES:
            celula = item.get(f"{coluna}_centavos")
            linha[coluna] = _reais(celula["value"]) if celula else None
        linhas.append(linha)
    return linhas


def total(linhas: list[dict[str, str | None]], coluna: str = "empenhado") -> Decimal:
    return sum((Decimal(linha[coluna] or 0) for linha in linhas), Decimal(0))
