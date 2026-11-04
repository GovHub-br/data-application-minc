"""MUNIC e ESTADIC do IBGE: as bases de dados de cada edição, como planilha.

As Pesquisas de Informações Básicas Municipais (MUNIC) e Estaduais (ESTADIC)
saem uma vez por edição, como uma planilha com uma aba por tema e uma linha
por município (ou UF). O Eixo 1 do Cultura em Números usa as edições que têm
cultura: os suplementos de 2006 e 2014 e as abas de cultura das edições
gerais. ``EDICOES`` lista cada uma com a URL no FTP do IBGE, as abas que a
planilha tem e quantos entes cada aba de dados deve ter.

Essas duas listas são a conferência contra a origem. Se o IBGE republicar
uma edição com outra aba, ou uma aba de dados vier com outro número de
linhas, a conversão falha em vez de carregar pela metade.

O que vem da planilha não é corrigido: todo valor vira texto, número inteiro
sem o ``.0`` que o leitor acrescenta. A tipagem é do dbt.
"""

import io
import logging
import zipfile
from dataclasses import dataclass
from typing import Any

import requests

from seeds_carga import linhas_para_registros, normalizar_colunas, slug

URL_FTP = "https://ftp.ibge.gov.br"
# Uma aba de dicionário em toda planilha: descreve as variáveis, não tem uma
# linha por ente.
ABA_DICIONARIO = "Dicionário"
COLUNA_EDICAO = "edicao"

_MUNICIPIOS_2006 = 5564
_MUNICIPIOS = 5570
_UFS = 27


@dataclass(frozen=True)
class Edicao:
    pesquisa: str
    ano: int
    caminho: str
    entes: int
    abas: tuple[str, ...]

    @property
    def url(self) -> str:
        return f"{URL_FTP}/{self.caminho}"


EDICOES: dict[str, Edicao] = {
    "munic_cultura_2006": Edicao(
        "munic",
        2006,
        "Perfil_Municipios/Cultura_2006/base_suplemento_cultura_2006.zip",
        _MUNICIPIOS_2006,
        (
            ABA_DICIONARIO,
            "Órgão gestor",
            "Infra-estrutura",
            "Recursos humanos",
            "Instrumentos de gestão",
            "Legislação",
            "Conselhos municipais",
            "Fundo municipal",
            "Recursos financeiros",
            "Fundação",
            "Ações projetos",
            "Meios de comunicação",
            "Equipamentos",
            "Variáveis externas",
        ),
    ),
    "munic_cultura_2014": Edicao(
        "munic",
        2014,
        "Perfil_Municipios/Cultura_2014/base_cultura_MUNIC_xls_2014.zip",
        _MUNICIPIOS,
        (
            ABA_DICIONARIO,
            "Órgão gestor",
            "Infraestrutura do órgão gestor",
            "Recursos humanos",
            "Políticas culturais",
            "Legislação",
            "Instâncias participativas",
            "Fundo cultura",
            "Meios de comunicação",
            "Equipamentos",
            "Variáveis externas",
        ),
    ),
    "munic_2018": Edicao(
        "munic",
        2018,
        "Perfil_Municipios/2018/Base_de_Dados/Base_MUNIC_2018_xlsx_20201103.zip",
        _MUNICIPIOS,
        (
            ABA_DICIONARIO,
            "Recursos humanos",
            "Legislação inst de planejamento",
            "Educação",
            "Cultura",
            "Saúde",
            "Assistência social",
            "Segurança alimentar",
            "Trabalho e inclusão produtiva",
            "Política para mulheres",
            "Instrumentos gestão migratória",
            "Enfrentamento sub-registro",
            "Variáveis externas",
        ),
    ),
    "munic_2021": Edicao(
        "munic",
        2021,
        "Perfil_Municipios/2021/Base_de_Dados/Base_MUNIC_2021_20240425.xlsx",
        _MUNICIPIOS,
        (
            ABA_DICIONARIO,
            "Informações atual prefeito",
            "Recursos humanos",
            "Legislação e instr de planej",
            "Educação",
            "Cultura",
            "Esporte",
            "Saúde",
            "COVID",
            "Variáveis externas",
        ),
    ),
    "estadic_2012": Edicao(
        "estadic",
        2012,
        "Perfil_Estados/2012/Base_ESTADIC_2012_xls.zip",
        _UFS,
        (
            ABA_DICIONARIO,
            "Informação governador",
            "Recursos humanos",
            "Conselhos e fundos",
            "Direitos humanos",
            "Gênero",
            "Segurança alimentar",
            "Inclusão produtiva",
            "Variáveis externas",
        ),
    ),
    "estadic_cultura_2014": Edicao(
        "estadic",
        2014,
        "Perfil_Estados/Cultura_2014/base_cultura_ESTADIC_xls_2014.zip",
        _UFS,
        (
            ABA_DICIONARIO,
            "Órgão gestor",
            "Infraestrutura do órgão gestor",
            "Recursos humanos",
            "Políticas culturais",
            "Legislação",
            "Instâncias participativas",
            "Fundo Cultura",
            "Equipamentos",
            "Variáveis externas",
        ),
    ),
    "estadic_2018": Edicao(
        "estadic",
        2018,
        "Perfil_Estados/2018/Base_ESTADIC_2018_20201215.xlsx",
        _UFS,
        (
            ABA_DICIONARIO,
            "Recursos humanos",
            "Educação",
            "Cultura",
            "Saúde",
            "Assistência social",
            "Segurança alimentar",
            "Trabalho e inclusão produtiva",
            "Política para mulheres",
            "Variáveis externas",
        ),
    ),
    "estadic_2021": Edicao(
        "estadic",
        2021,
        "Perfil_Estados/2021/Base_Estadic_2021.xlsx",
        _UFS,
        (
            ABA_DICIONARIO,
            "Recursos humanos",
            "Educação",
            "Cultura",
            "Esporte",
            "Saúde",
            "Variáveis externas",
        ),
    ),
    "estadic_2023": Edicao(
        "estadic",
        2023,
        "Perfil_Estados/2023/Base_Estadic_2023.xlsx",
        _UFS,
        (
            ABA_DICIONARIO,
            "Recursos Humanos",
            "Assistência Social",
            "Trabalho e Inclusão Produtiva",
            "Segurança Alimentar",
            "Política para Mulheres",
            "Segurança Pública",
            "Direitos Humanos",
            "Primeira Infância",
        ),
    ),
    "estadic_2024": Edicao(
        "estadic",
        2024,
        "Perfil_Estados/2024/Base_Estadic_2024.xlsx",
        _UFS,
        (
            ABA_DICIONARIO,
            "Recursos humanos",
            "Informática e comunicação",
            "Governança",
            "Habitação",
            "Transporte",
            "Agropecuária",
            "Igualdade Racial",
        ),
    ),
}


def entidade(edicao: str, aba: str) -> str:
    """``munic_2021`` + ``Cultura`` -> ``munic_2021_cultura``."""
    return f"{edicao}_{slug(aba)}"


def tabela_seeds(edicao: str, aba: str) -> str:
    return f"ibge_{entidade(edicao, aba)}"


def tabelas_seeds() -> list[str]:
    """Todas as tabelas que as edições do catálogo carregam em ``seeds``."""
    return sorted(
        tabela_seeds(chave, aba) for chave, ed in EDICOES.items() for aba in ed.abas
    )


def extensao(caminho: str) -> str:
    return caminho.rsplit(".", 1)[-1].lower()


def baixar(url: str, timeout: int = 300) -> bytes:
    """O arquivo da edição como o IBGE publica (ZIP ou planilha)."""
    resposta = requests.get(url, timeout=timeout)
    resposta.raise_for_status()
    logging.info("[cliente_ibge_perfil] %s: %d bytes", url, len(resposta.content))
    return resposta.content


def planilha_do_arquivo(conteudo: bytes, nome: str) -> tuple[str, bytes]:
    """A planilha dentro do arquivo baixado: o próprio, ou o único membro do ZIP."""
    if extensao(nome) != "zip":
        return nome, conteudo
    with zipfile.ZipFile(io.BytesIO(conteudo)) as arquivo:
        planilhas = [
            n for n in arquivo.namelist() if extensao(n) in {"xls", "xlsx", "ods"}
        ]
        if len(planilhas) != 1:
            raise ValueError(f"{nome}: esperava uma planilha no ZIP, achei {planilhas}")
        return planilhas[0], arquivo.read(planilhas[0])


def abrir_livro(planilha: bytes) -> Any:
    """A planilha aberta com o ``calamine``, que lê xls, xlsx e ods."""
    from python_calamine import CalamineWorkbook

    return CalamineWorkbook.from_filelike(io.BytesIO(planilha))


def linhas_da_aba(livro: Any, aba: str) -> list[list[Any]]:
    """As linhas de uma aba, com a primeira célula em A1.

    Uma aba por vez: a MUNIC tem milhões de células, e ler todas as abas de uma
    vez multiplicaria a memória da task.
    """
    linhas: list[list[Any]] = livro.get_sheet_by_name(aba).to_python(
        skip_empty_area=False
    )
    return linhas


def conferir_abas(edicao: str, abas: list[str]) -> None:
    esperadas = list(EDICOES[edicao].abas)
    if sorted(abas) != sorted(esperadas):
        raise ValueError(
            f"{edicao}: a planilha mudou. Abas novas: "
            f"{sorted(set(abas) - set(esperadas))}; abas que sumiram: "
            f"{sorted(set(esperadas) - set(abas))}. Atualize EDICOES em "
            "cliente_ibge_perfil depois de conferir a planilha nova."
        )


def registros_da_aba(
    edicao: str, aba: str, linhas: list[list[Any]]
) -> list[dict[str, str | None]]:
    """Uma aba como registros: a primeira linha é o cabeçalho.

    Aba de dados precisa ter uma linha por ente da edição.
    """
    if not linhas:
        raise ValueError(f"{edicao} / {aba}: aba vazia")
    colunas = normalizar_colunas(linhas[0])
    if COLUNA_EDICAO in colunas:
        raise ValueError(f"{edicao} / {aba}: a planilha já tem coluna {COLUNA_EDICAO}")
    registros = [
        {**r, COLUNA_EDICAO: edicao} for r in linhas_para_registros(colunas, linhas[1:])
    ]
    esperado = EDICOES[edicao].entes
    if aba != ABA_DICIONARIO and len(registros) != esperado:
        raise ValueError(
            f"{edicao} / {aba}: {len(registros)} linhas, esperava {esperado} "
            "(uma por ente)"
        )
    return registros
