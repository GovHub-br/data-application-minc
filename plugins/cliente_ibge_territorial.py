"""Planilhas territoriais do IBGE: áreas, REGIC e composição das RMs.

O Eixo 3 do Cultura em Números monta o contexto territorial de cada município
com quatro publicações do IBGE que saem uma vez por versão e não têm API:

- **Áreas territoriais** (2025): área em km² por município e por UF.
- **REGIC 2018** (Regiões de Influência das Cidades): a hierarquia urbana de
  cada município, a base por cidade (VAR01 a VAR102) e os arranjos
  populacionais.
- **Composição dos recortes metropolitanos** (2025): que municípios formam
  cada região metropolitana e cada aglomeração urbana.

Todas ficam no geoftp do IBGE (``URL_GEOFTP``). ``ARQUIVOS`` lista cada uma
com o caminho, a versão, as abas de dados que vão para ``seeds`` e quantas
linhas cada aba deve ter, e as abas que ficam só no raw (dicionários e
notas). Essas listas são a conferência contra a origem: se o IBGE republicar
o arquivo com outra aba ou outro número de linhas, a conversão falha em vez
de carregar pela metade.

As planilhas de área terminam com uma nota de rodapé ("OBS: Estes valores de
área..."), separada dos dados por uma linha vazia. Por isso a tabela de uma
aba termina na primeira linha vazia: o que vem depois é nota, e continua no
arquivo do raw.

O que vem da planilha não é corrigido: todo valor vira texto, número inteiro
sem o ``.0`` que o leitor acrescenta. A tipagem é do dbt.
"""

import logging
from dataclasses import dataclass
from typing import Any

import requests

from cliente_ibge_perfil import abrir_livro, extensao, linhas_da_aba
from seeds_carga import linhas_para_registros, normalizar_colunas

URL_GEOFTP = "https://geoftp.ibge.gov.br/organizacao_do_territorio"
COLUNA_VERSAO = "versao"

_REGIC = (
    "divisao_regional/regioes_de_influencia_das_cidades/"
    "Regioes_de_influencia_das_cidades_2018_Resultados_definitivos/base_tabular"
)
_RM = "estrutura_territorial/municipios_por_regioes_metropolitanas/Situacao_2020a2029"


@dataclass(frozen=True)
class Aba:
    """Uma aba de dados: vira ``staging/ibge/<entidade>/`` e ``seeds.ibge_<entidade>``."""

    nome: str
    entidade: str
    linhas: int


@dataclass(frozen=True)
class Arquivo:
    caminho: str
    versao: str
    abas: tuple[Aba, ...]
    # Dicionários e notas: descrevem as variáveis, não têm uma linha por ente.
    # Ficam no raw, que guarda o arquivo inteiro.
    abas_ignoradas: tuple[str, ...] = ()

    @property
    def url(self) -> str:
        return f"{URL_GEOFTP}/{self.caminho}"


ARQUIVOS: dict[str, Arquivo] = {
    "areas_territoriais": Arquivo(
        "estrutura_territorial/areas_territoriais/2025/"
        "AR_BR_RG_UF_RGINT_RGI_MUN_2025.xls",
        "2025",
        (
            # 5.571 municípios (Boa Esperança do Norte, MT, instalado depois
            # da REGIC 2018) e as duas áreas operacionais das lagoas do RS,
            # que não são município: Lagoa Mirim (4300001) e Lagoa dos Patos
            # (4300002). Quem conta municípios precisa tirá-las.
            Aba("AR_BR_MUN_2025", "areas_territoriais_mun", 5573),
            Aba("AR_BR_UF_2025", "areas_territoriais_uf", 27),
        ),
        # Regiões intermediárias, imediatas, grandes regiões e o país não são
        # usados pelo Eixo 3; ficam no raw se alguém precisar.
        (
            "AR_BR_RGINT_2025",
            "AR_BR_RGI_2025",
            "AR_BR_RG_2025",
            "AR_BR_2025",
            "Dicionário_e_Notas",
        ),
    ),
    "regic_hierarquia": Arquivo(
        f"{_REGIC}/REGIC2018_Municipios_Hierarquia_e_regiao.xlsx",
        "2018",
        (Aba("Hierarquia e região", "regic_hierarquia", 5570),),
        ("Notas, fonte e dicionário",),
    ),
    "regic_cidades": Arquivo(
        f"{_REGIC}/REGIC2018_Cidades_v2.xlsx",
        "2018",
        (Aba("Base de dados por Cidades", "regic_cidades", 4899),),
        ("Descrição das variáveis",),
    ),
    "regic_arranjos_populacionais": Arquivo(
        f"{_REGIC}/REGIC2018_Arranjos_Populacionais_v2.xlsx",
        "2018",
        (Aba("Arranjos Populacionais", "regic_arranjos_populacionais", 960),),
        ("Descrição",),
    ),
    # O IBGE publica a mesma composição como .xlsx e, com o nome _v2, como
    # .xls e .ods; o conteúdo é igual. O .xlsx é o que o Eixo 3 usa.
    "composicao_rm": Arquivo(
        f"{_RM}/Composicao_RM_2025.xlsx",
        "2025",
        (
            # O IBGE corta o nome da aba em 31 caracteres, o limite do Excel.
            Aba("Composição - Recortes Metropoli", "composicao_rm", 1468),
            Aba("Composição - Aglomerações Urban", "composicao_au", 44),
        ),
    ),
}


def tabela_seeds(entidade: str) -> str:
    return f"ibge_{entidade}"


def tabelas_seeds() -> list[str]:
    """Todas as tabelas que os arquivos do catálogo carregam em ``seeds``."""
    return sorted(
        tabela_seeds(aba.entidade) for arq in ARQUIVOS.values() for aba in arq.abas
    )


def baixar(url: str, timeout: int = 300) -> bytes:
    """O arquivo como o IBGE publica."""
    resposta = requests.get(url, timeout=timeout)
    resposta.raise_for_status()
    logging.info("[cliente_ibge_territorial] %s: %d bytes", url, len(resposta.content))
    return resposta.content


def conferir_abas(chave: str, abas: list[str]) -> None:
    """Falha se a planilha tem aba nova ou perdeu alguma do catálogo."""
    arquivo = ARQUIVOS[chave]
    esperadas = [a.nome for a in arquivo.abas] + list(arquivo.abas_ignoradas)
    if sorted(abas) != sorted(esperadas):
        raise ValueError(
            f"{chave}: a planilha mudou. Abas novas: "
            f"{sorted(set(abas) - set(esperadas))}; abas que sumiram: "
            f"{sorted(set(esperadas) - set(abas))}. Atualize ARQUIVOS em "
            "cliente_ibge_territorial depois de conferir a planilha nova."
        )


def _ate_a_primeira_linha_vazia(linhas: list[list[Any]]) -> list[list[Any]]:
    """As linhas de dados, sem a nota de rodapé que vem depois de uma linha vazia."""
    dados = []
    for linha in linhas:
        if not any(str(c).strip() for c in linha if c is not None):
            break
        dados.append(linha)
    return dados


def registros_da_aba(
    chave: str, aba: Aba, linhas: list[list[Any]]
) -> list[dict[str, str | None]]:
    """Uma aba como registros: a primeira linha é o cabeçalho.

    A tabela termina na primeira linha vazia, e precisa ter o número de linhas
    do catálogo.
    """
    if not linhas:
        raise ValueError(f"{chave} / {aba.nome}: aba vazia")
    colunas = normalizar_colunas(linhas[0])
    if COLUNA_VERSAO in colunas:
        raise ValueError(
            f"{chave} / {aba.nome}: a planilha já tem coluna {COLUNA_VERSAO}"
        )
    versao = ARQUIVOS[chave].versao
    registros = [
        {**r, COLUNA_VERSAO: versao}
        for r in linhas_para_registros(colunas, _ate_a_primeira_linha_vazia(linhas[1:]))
    ]
    if len(registros) != aba.linhas:
        raise ValueError(
            f"{chave} / {aba.nome}: {len(registros)} linhas, esperava {aba.linhas}"
        )
    return registros


__all__ = [
    "ARQUIVOS",
    "COLUNA_VERSAO",
    "Aba",
    "Arquivo",
    "abrir_livro",
    "baixar",
    "conferir_abas",
    "extensao",
    "linhas_da_aba",
    "registros_da_aba",
    "tabela_seeds",
    "tabelas_seeds",
]
