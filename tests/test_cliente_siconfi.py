"""Planejamento SICONFI a partir de linhas reais do extrato de entregas.

As linhas abaixo vieram de ``/extrato_entregas`` (São Paulo, 2025). O extrato
usa o nome por extenso do demonstrativo; procurar só a sigla fazia RREO e RGF
nunca entrarem na fila.
"""

import pytest

from cliente_siconfi import classify_delivery, work_units

_BASE = {"exercicio": 2025, "cod_ibge": 3550308, "periodo": 1}
_PODERES = ["E", "L"]


@pytest.mark.parametrize(
    ("entregavel", "expected"),
    [
        ("Relatório Resumido de Execução Orçamentária", "rreo"),
        ("Relatório de Gestão Fiscal", "rgf"),
        ("Balanço Anual (DCA)", "dca"),
        ("MSC Agregada", "msc"),
        ("MSC Encerramento", "msc"),
        ("RREO", "rreo"),
        ("Algo novo", None),
    ],
)
def test_classifica_nome_por_extenso(entregavel: str, expected: str | None) -> None:
    assert classify_delivery(entregavel) == expected


def test_rreo_gera_uma_unidade() -> None:
    item = {
        **_BASE,
        "entregavel": "Relatório Resumido de Execução Orçamentária",
        "periodicidade": "B",
        "tipo_relatorio": "P",
    }
    assert work_units(item, _PODERES) == [
        (
            "rreo",
            {
                "id_ente": 3550308,
                "an_exercicio": 2025,
                "nr_periodo": 1,
                "co_tipo_demonstrativo": "RREO",
            },
            "||",
        )
    ]


def test_rgf_gera_uma_unidade_por_poder() -> None:
    item = {
        **_BASE,
        "entregavel": "Relatório de Gestão Fiscal",
        "periodicidade": "Q",
        "tipo_relatorio": "P",
    }
    units = work_units(item, _PODERES)
    assert [u[1]["co_poder"] for u in units] == ["E", "L"]
    assert all(u[0] == "rgf" and u[1]["in_periodicidade"] == "Q" for u in units)


def test_dca() -> None:
    item = {**_BASE, "entregavel": "Balanço Anual (DCA)", "periodicidade": "A"}
    assert work_units(item, _PODERES) == [
        ("dca", {"id_ente": 3550308, "an_exercicio": 2025}, "||")
    ]


def test_msc_agregada_usa_mes_do_extrato() -> None:
    item = {**_BASE, "entregavel": "MSC Agregada", "periodicidade": "M", "periodo": 4}
    units = work_units(item, _PODERES)
    assert len(units) == 24
    assert {u[1]["co_tipo_matriz"] for u in units} == {"MSCC"}
    assert {u[1]["me_referencia"] for u in units} == {4}


def test_msc_encerramento_consulta_dezembro() -> None:
    # O extrato traz periodo=1, mas a API só devolve a MSCE com me_referencia=12.
    item = {**_BASE, "entregavel": "MSC Encerramento", "periodicidade": "A"}
    units = work_units(item, _PODERES)
    assert {u[1]["co_tipo_matriz"] for u in units} == {"MSCE"}
    assert {u[1]["me_referencia"] for u in units} == {12}


def test_linha_sem_ente_e_ignorada() -> None:
    assert work_units({"entregavel": "Relatório de Gestão Fiscal"}, _PODERES) == []
