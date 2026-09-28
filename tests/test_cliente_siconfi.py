"""Planejamento SICONFI a partir de linhas reais do extrato de entregas.

As linhas abaixo vieram de ``/extrato_entregas`` (São Paulo, 2025). O extrato
usa o nome por extenso do demonstrativo; procurar só a sigla fazia RREO e RGF
nunca entrarem na fila.
"""

import pytest

from cliente_siconfi import PlanScope, classify_delivery, poder_da_instituicao, work_units

_BASE = {"exercicio": 2025, "cod_ibge": 3550308, "periodo": 1}


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
    assert work_units(item) == [
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


_RGF = {
    **_BASE,
    "entregavel": "Relatório de Gestão Fiscal",
    "periodicidade": "Q",
    "tipo_relatorio": "P",
}


@pytest.mark.parametrize(
    ("instituicao", "poder"),
    [
        ("Prefeitura Municipal de São Paulo - SP", "E"),
        ("Governo do Estado do Espírito Santo", "E"),
        ("Governo do Distrito Federal", "E"),
        ("Câmara de Vereadores de São Paulo - SP", "L"),
        ("Câmara Legislativa do Distrito Federal", "L"),
        ("Assembleia Legislativa do Estado do Espírito Santo", "L"),
        # Conferido na API: o RGF do TC sai em co_poder=L, não em J.
        ("Tribunal de Contas do Município de São Paulo", "L"),
        ("Tribunal de Justiça do Estado do Espírito Santo", "J"),
        ("Ministério Público do Estado do Espírito Santo", "M"),
        ("Defensoria Pública do Estado do Espírito Santo", "D"),
        ("Consórcio Intermunicipal", None),
    ],
)
def test_poder_da_instituicao(instituicao: str, poder: str | None) -> None:
    assert poder_da_instituicao(instituicao) == poder


def test_rgf_consulta_so_o_poder_de_quem_entregou() -> None:
    item = {**_RGF, "instituicao": "Câmara de Vereadores de São Paulo - SP"}
    units = work_units(item)
    assert [u[1]["co_poder"] for u in units] == ["L"]
    assert units[0][0] == "rgf" and units[0][1]["in_periodicidade"] == "Q"


def test_rgf_de_instituicao_desconhecida_consulta_os_cinco_poderes() -> None:
    units = work_units({**_RGF, "instituicao": "Consórcio Intermunicipal"})
    assert [u[1]["co_poder"] for u in units] == ["E", "L", "J", "M", "D"]


def test_dca() -> None:
    item = {**_BASE, "entregavel": "Balanço Anual (DCA)", "periodicidade": "A"}
    assert work_units(item) == [("dca", {"id_ente": 3550308, "an_exercicio": 2025}, "||")]


def test_msc_agregada_usa_mes_do_extrato() -> None:
    item = {**_BASE, "entregavel": "MSC Agregada", "periodicidade": "M", "periodo": 4}
    units = work_units(item)
    assert len(units) == 24
    assert {u[1]["co_tipo_matriz"] for u in units} == {"MSCC"}
    assert {u[1]["me_referencia"] for u in units} == {4}


def test_msc_encerramento_consulta_dezembro() -> None:
    # O extrato traz periodo=1, mas a API só devolve a MSCE com me_referencia=12.
    item = {**_BASE, "entregavel": "MSC Encerramento", "periodicidade": "A"}
    units = work_units(item)
    assert {u[1]["co_tipo_matriz"] for u in units} == {"MSCE"}
    assert {u[1]["me_referencia"] for u in units} == {12}


def test_linha_sem_ente_e_ignorada() -> None:
    assert work_units({"entregavel": "Relatório de Gestão Fiscal"}) == []


# --- Recorte configurável ---------------------------------------------------

_ANOS = {"start_year": 2019, "end_year": 2025}
_MSC_DEZ = {
    "id_ente": 32,
    "an_referencia": 2024,
    "me_referencia": 12,
    "co_tipo_matriz": "MSCC",
    "classe_conta": 6,
    "id_tv": "ending_balance",
}


def test_recorte_padrao_so_filtra_os_anos() -> None:
    scope = PlanScope.from_config(_ANOS)
    assert scope.allows("msc_patrimonial", {**_MSC_DEZ, "classe_conta": 1})
    assert not scope.allows("msc_orcamentaria", {**_MSC_DEZ, "an_referencia": 2026})
    assert not scope.allows("dca", {"id_ente": 32, "an_exercicio": 2018})


def test_recorte_do_outro_repositorio() -> None:
    scope = PlanScope.from_config(
        {
            **_ANOS,
            "fact_endpoints": ["msc_orcamentaria", "dca"],
            "msc_months": [12],
            "msc_classes": [6],
            "msc_value_types": ["ending_balance"],
            "msc_matrix_types": ["mscc"],
        }
    )
    assert scope.allows("msc_orcamentaria", _MSC_DEZ)
    assert not scope.allows("msc_orcamentaria", {**_MSC_DEZ, "me_referencia": 11})
    assert not scope.allows("msc_orcamentaria", {**_MSC_DEZ, "classe_conta": 5})
    assert not scope.allows("msc_orcamentaria", {**_MSC_DEZ, "id_tv": "period_change"})
    assert not scope.allows("msc_orcamentaria", {**_MSC_DEZ, "co_tipo_matriz": "MSCE"})
    assert not scope.allows("msc_patrimonial", {**_MSC_DEZ, "classe_conta": 1})
    assert scope.allows("dca", {"id_ente": 32, "an_exercicio": 2024})
    assert not scope.enabled("rreo")
    # O extrato não é demonstrativo de fatos: nunca fica de fora.
    assert scope.enabled("extrato_entregas")


def test_uma_linha_de_msc_do_extrato_vira_uma_unidade_no_recorte_estreito() -> None:
    scope = PlanScope.from_config(
        {
            **_ANOS,
            "fact_endpoints": ["msc_orcamentaria"],
            "msc_months": [12],
            "msc_classes": [6],
            "msc_value_types": ["ending_balance"],
            "msc_matrix_types": ["MSCC"],
        }
    )
    item = {
        "exercicio": 2024,
        "cod_ibge": 32,
        "entregavel": "MSC Agregada",
        "periodicidade": "M",
    }
    kept = [
        u
        for periodo in range(1, 13)
        for u in work_units({**item, "periodo": periodo})
        if scope.allows(u[0], u[1])
    ]
    assert [(u[0], u[1]) for u in kept] == [("msc_orcamentaria", _MSC_DEZ)]


def test_constraints_vao_para_o_claim() -> None:
    scope = PlanScope.from_config({**_ANOS, "rgf_poderes": ["e", "L"]})
    assert scope.constraints("rgf") == {
        "an_exercicio": frozenset(range(2019, 2026)),
        "co_poder": frozenset({"E", "L"}),
    }
    assert set(scope.constraints("extrato_entregas")) == {"an_referencia"}


def test_fingerprint_muda_com_o_recorte() -> None:
    base = PlanScope.from_config(_ANOS)
    assert base.fingerprint() == PlanScope.from_config(dict(_ANOS)).fingerprint()
    narrowed = PlanScope.from_config({**_ANOS, "msc_months": [12]})
    assert base.fingerprint() != narrowed.fingerprint()


@pytest.mark.parametrize(
    "config",
    [
        {"fact_endpoints": ["msc_orcamentario"]},
        {"rgf_poderes": ["X"]},
        {"msc_value_types": ["saldo_final"]},
        {"msc_matrix_types": ["MSCX"]},
    ],
)
def test_recorte_invalido_falha_cedo(config: dict) -> None:
    with pytest.raises(ValueError, match="siconfi_config"):
        PlanScope.from_config({**_ANOS, **config})
