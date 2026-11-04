"""Testes das partes puras da carga genérica em ``seeds`` (ADR 0012).

O que importa aqui: o cabeçalho de planilha vira nomes de coluna únicos e
válidos no Postgres (vazio, repetido e acentuado), o valor da célula vira
texto sem mudar o que diz, linha vazia não vira registro, e a conferência
falha quando origem, Parquet e banco divergem.
"""

from datetime import date, datetime

import pandas as pd
import pytest

import seeds_carga


def test_slug_tira_acento_e_pontuacao() -> None:
    assert seeds_carga.slug("Órgão gestor") == "orgao_gestor"
    assert seeds_carga.slug("Legislação inst de planejamento") == (
        "legislacao_inst_de_planejamento"
    )
    assert seeds_carga.slug("Enfrentamento sub-registro") == "enfrentamento_sub_registro"
    assert seeds_carga.slug("RECEITA CORRENTE LÍQUIDA - RCL") == (
        "receita_corrente_liquida_rcl"
    )


def test_cabecalho_vazio_e_repetido_vira_coluna_unica() -> None:
    # A aba "Cultura" da MUNIC 2018 repete um cabeçalho; o dicionário de toda
    # planilha do IBGE tem células vazias.
    assert seeds_carga.normalizar_colunas(
        ["Cod Municipio", "", "MCUL01", "mcul01", None, "  "]
    ) == ["cod_municipio", "coluna_2", "mcul01", "mcul01_2", "coluna_5", "coluna_6"]


def test_cabecalho_numerico_vira_texto() -> None:
    assert seeds_carga.normalizar_colunas([2016.0, "PIB [2016]"]) == ["2016", "pib_2016"]


@pytest.mark.parametrize(
    ("valor", "texto"),
    [
        (None, None),
        ("", None),
        ("   ", None),
        (1100015.0, "1100015"),
        (0.5, "0.5"),
        (float("nan"), None),
        (True, "true"),
        ("Sim", "Sim"),
        (" texto com espaço ", " texto com espaço "),
        (datetime(2024, 1, 2, 3, 4), "2024-01-02T03:04:00"),
        (date(2024, 1, 2), "2024-01-02"),
        (7, "7"),
    ],
)
def test_texto_da_celula(valor: object, texto: str | None) -> None:
    assert seeds_carga.texto_da_celula(valor) == texto


def test_linha_vazia_nao_vira_registro_e_linha_curta_completa_com_nulo() -> None:
    registros = seeds_carga.linhas_para_registros(
        ["a", "b", "c"], [["1", 2.0, None], ["", None, ""], ["x"]]
    )
    assert registros == [
        {"a": "1", "b": "2", "c": None},
        {"a": "x", "b": None, "c": None},
    ]


def test_identificador_longo_demais_falha() -> None:
    seeds_carga.validar_identificador("a" * 63)
    with pytest.raises(ValueError, match="corta em 63"):
        seeds_carga.validar_identificador("a" * 64)


def test_tabela_controle_por_fonte() -> None:
    assert seeds_carga.tabela_controle("ibge") == "ibge_controle_carga"


def test_copy_csv_escreve_nulo_como_campo_vazio() -> None:
    df = pd.DataFrame({"a": ["x", None], "b": ['com "aspas"', "y"]}).astype("string")
    assert seeds_carga._copy_csv(df).getvalue() == 'x,"com ""aspas"""\n,y\n'


def _carga(origem: int, parquet: int, banco: int) -> dict[str, object]:
    return {
        "entidade": "munic_2021_cultura",
        "periodo": "munic_2021",
        "linhas_origem": origem,
        "linhas_parquet": parquet,
        "linhas_carregadas": banco,
    }


def test_conferencia_passa_quando_as_tres_contagens_batem() -> None:
    seeds_carga.conferir_contagens([_carga(5570, 5570, 5570)])


def test_conferencia_falha_e_diz_onde_divergiu() -> None:
    with pytest.raises(ValueError, match="munic_2021 munic_2021_cultura: 5570/5570/5569"):
        seeds_carga.conferir_contagens([_carga(5570, 5570, 5569)])
