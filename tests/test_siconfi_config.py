"""Configuração da extração SICONFI: padrões, precedência e validação rígida.

A API responde HTTP 200 com lista vazia a parâmetro inválido, então um erro de
configuração que passasse daqui viraria milhares de "vazios" silenciosos.
"""

import json
from pathlib import Path
from typing import Any

import pytest

from siconfi_config import DEFAULT_CONFIG, carregar, mesclar, validar_anexos

ANO = 2026
_JSON_PADRAO = (
    Path(__file__).resolve().parents[1]
    / "dags/data_ingest/siconfi/siconfi_extracao_config.json"
)


def _endpoint(config: dict[str, Any], nome: str) -> dict[str, Any]:
    return dict(config["endpoints"][nome])


# --- Padrões ---------------------------------------------------------------------


def test_json_do_repositorio_e_igual_aos_padroes_do_codigo() -> None:
    assert json.loads(_JSON_PADRAO.read_text()) == DEFAULT_CONFIG


def test_sem_variable_usa_os_padroes() -> None:
    config = carregar(None, None, ANO)

    assert config["global"]["esferas"] == ["M", "E", "D", "U"]
    assert _endpoint(config, "msc_orcamentaria")["classes"] == [5, 6]
    assert _endpoint(config, "msc_orcamentaria")["meses"] == [12]
    assert not _endpoint(config, "msc_patrimonial")["ativo"]
    assert not _endpoint(config, "msc_controle")["ativo"]


def test_corrente_e_resolvido_na_execucao() -> None:
    config = carregar(None, None, ANO)
    assert {
        cfg["ano_fim"] for cfg in config["endpoints"].values() if "ano_fim" in cfg
    } == {ANO}
    assert carregar(None, None, 2030)["endpoints"]["dca"]["ano_fim"] == 2030


def test_padroes_nao_sao_alterados_pela_carga() -> None:
    antes = json.dumps(DEFAULT_CONFIG, sort_keys=True)
    carregar({"endpoints": {"dca": {"no_anexo": ["DCA-Anexo I-E"]}}}, None, ANO)
    assert json.dumps(DEFAULT_CONFIG, sort_keys=True) == antes


# --- Precedência -------------------------------------------------------------------


def test_dag_run_conf_vence_a_variable_que_vence_o_padrao() -> None:
    variable = {
        "global": {"max_paralelismo": 3},
        "endpoints": {"dca": {"ano_inicio": 2015}, "rreo": {"ano_inicio": 2016}},
    }
    conf = {"endpoints": {"dca": {"ano_inicio": 2020}}}

    config = carregar(variable, conf, ANO)

    assert _endpoint(config, "dca")["ano_inicio"] == 2020  # conf
    assert _endpoint(config, "rreo")["ano_inicio"] == 2016  # Variable
    assert config["global"]["max_paralelismo"] == 3  # Variable
    assert config["global"]["retentativas"] == 4  # padrão
    assert _endpoint(config, "dca")["ativo"] is True  # padrão, no mesmo bloco


def test_lista_e_null_substituem_em_vez_de_somar() -> None:
    variable = {"endpoints": {"dca": {"no_anexo": ["DCA-Anexo I-E"]}}}
    assert _endpoint(carregar(variable, None, ANO), "dca")["no_anexo"] == [
        "DCA-Anexo I-E"
    ]
    conf = {"endpoints": {"dca": {"no_anexo": None}}}
    assert _endpoint(carregar(variable, conf, ANO), "dca")["no_anexo"] is None

    fase_2 = {"endpoints": {"msc_orcamentaria": {"meses": list(range(1, 13))}}}
    so_junho = {"endpoints": {"msc_orcamentaria": {"meses": [6]}}}
    assert _endpoint(carregar(fase_2, so_junho, ANO), "msc_orcamentaria")["meses"] == [6]


def test_fase_2_da_msc_e_so_trocar_os_meses() -> None:
    variable = {"endpoints": {"msc_orcamentaria": {"meses": list(range(1, 13))}}}
    assert _endpoint(carregar(variable, None, ANO), "msc_orcamentaria")["meses"] == list(
        range(1, 13)
    )


def test_poderes_por_esfera_mesclam_por_esfera() -> None:
    variable = {"endpoints": {"rgf": {"poderes_por_esfera": {"M": ["E"]}}}}
    poderes = _endpoint(carregar(variable, None, ANO), "rgf")["poderes_por_esfera"]
    assert poderes["M"] == ["E"]
    assert poderes["E"] == ["E", "L", "J", "M", "D"]


def test_mesclar_nao_compartilha_referencias() -> None:
    base = {"a": {"lista": [1]}}
    resultado = mesclar(base, {"b": {"lista": [2]}})
    resultado["a"]["lista"].append(9)
    resultado["b"]["lista"].append(9)
    assert base == {"a": {"lista": [1]}}


# --- Validação -----------------------------------------------------------------------


@pytest.mark.parametrize(
    ("variable", "trecho"),
    [
        ({"endpoints": {"dca": {"ano_inicio": 2012}}}, "endpoints.dca.ano_inicio"),
        (
            {"endpoints": {"rreo": {"ano_inicio": 2014}}},
            "primeiro exercício com dados (2015)",
        ),
        ({"endpoints": {"rgf": {"ano_inicio": 2014}}}, "endpoints.rgf.ano_inicio"),
        (
            {"endpoints": {"msc_orcamentaria": {"ano_inicio": 2018}}},
            "primeiro exercício com dados (2019)",
        ),
        (
            {"endpoints": {"dca": {"ano_inicio": 2024, "ano_fim": 2020}}},
            "ano_inicio (2024) maior que ano_fim (2020)",
        ),
        ({"endpoints": {"dca": {"ano_fim": 2027}}}, "está no futuro"),
        ({"endpoints": {"dca": {"ano_fim": "atual"}}}, "endpoints.dca.ano_fim"),
        (
            {"endpoints": {"msc_orcamentaria": {"classes": [5, 7]}}},
            "só tem as classes [5, 6]",
        ),
        ({"endpoints": {"msc_orcamentaria": {"meses": [0, 12]}}}, "[0] fora de 1–12"),
        ({"endpoints": {"msc_orcamentaria": {"meses": [13]}}}, "fora de 1–12"),
        (
            {"endpoints": {"msc_orcamentaria": {"tipos_matriz": ["MSCE"], "meses": [6]}}},
            "MSCE só existe com me_referencia=12",
        ),
        (
            {"endpoints": {"msc_orcamentaria": {"tipos_matriz": ["MSCX"]}}},
            "msc_orcamentaria.tipos_matriz",
        ),
        (
            {"endpoints": {"msc_orcamentaria": {"id_tv": ["saldo_final"]}}},
            "msc_orcamentaria.id_tv",
        ),
        (
            {"endpoints": {"rgf": {"poderes_por_esfera": {"M": ["E", "X"]}}}},
            "poderes_por_esfera.M",
        ),
        (
            {"endpoints": {"rgf": {"poderes_por_esfera": {"Z": ["E"]}}}},
            "poderes_por_esfera.Z",
        ),
        ({"endpoints": {"rreo": {"periodos": [1, 7]}}}, "[7] fora dos bimestres 1–6"),
        ({"global": {"esferas": ["M", "X"]}}, "global.esferas"),
        ({"global": {"esferas": []}}, "global.esferas"),
        ({"global": {"max_paralelismo": 0}}, "global.max_paralelismo"),
        ({"global": {"max_minutos_por_execucao": 60}}, "global.max_minutos_por_execucao"),
        ({"endpoints": {"dca": {"no_anexo": []}}}, "endpoints.dca.no_anexo"),
        ({"endpoints": {"dca": {"ativo": "sim"}}}, "endpoints.dca.ativo"),
        # Chave com erro de digitação não pode ser ignorada em silêncio.
        ({"endpoints": {"dca": {"ano_inico": 2014}}}, "endpoints.dca.ano_inico"),
        ({"endpoints": {"siconfi_novo": {"ativo": True}}}, "endpoints.siconfi_novo"),
        (
            {"global": {"incluir_cod_ibge": [32], "excluir_cod_ibge": [32]}},
            "incluir_cod_ibge e em excluir_cod_ibge",
        ),
        (
            {"endpoints": {"extrato_entregas": {"ano_inicio": 2020}}},
            "fora do extrato_entregas (2020–2026)",
        ),
        (
            {
                "global": {"esferas": ["M", "E", "D", "U"]},
                "endpoints": {"rgf": {"poderes_por_esfera": {"M": ["E", "L"]}}},
            },
            None,  # válido: a mescla mantém as outras esferas
        ),
    ],
)
def test_configuracao_invalida_falha_com_mensagem_clara(
    variable: dict[str, Any], trecho: str | None
) -> None:
    if trecho is None:
        carregar(variable, None, ANO)
        return
    with pytest.raises(ValueError, match="Configuração SICONFI inválida") as excinfo:
        carregar(variable, None, ANO)
    assert trecho in str(excinfo.value)


def test_rgf_sem_poderes_para_uma_esfera_ativa_falha() -> None:
    config = json.loads(json.dumps(DEFAULT_CONFIG))
    del config["endpoints"]["rgf"]["poderes_por_esfera"]["D"]
    # A mescla com o padrão recolocaria "D"; a checagem vale para o valor final.
    from siconfi_config import _regras, _resolver

    erros = _regras(_resolver(config, ANO), ANO)
    assert any("faltam as esferas ['D']" in erro for erro in erros)


def test_todos_os_erros_aparecem_juntos() -> None:
    variable = {
        "global": {"esferas": ["X"]},
        "endpoints": {"rreo": {"periodos": [9]}, "dca": {"ano_inicio": "2013"}},
    }
    with pytest.raises(ValueError) as excinfo:
        carregar(variable, None, ANO)
    mensagem = str(excinfo.value)
    assert "global.esferas" in mensagem and "endpoints.dca.ano_inicio" in mensagem


def test_reprocessar_na_variable_e_recusado() -> None:
    with pytest.raises(ValueError, match="use só no dag_run.conf"):
        carregar({"global": {"reprocessar": ["erro"]}}, None, ANO)
    conf = {"global": {"reprocessar": ["vazio_inesperado", "erro"]}}
    assert carregar(None, conf, ANO)["global"]["reprocessar"] == [
        "vazio_inesperado",
        "erro",
    ]


@pytest.mark.parametrize("variable", [[1, 2], "texto", 3])
def test_variable_que_nao_e_objeto_falha(variable: Any) -> None:
    with pytest.raises(ValueError, match="precisa ser um objeto JSON"):
        carregar(variable, None, ANO)


def test_repeticoes_sao_removidas_mantendo_a_ordem() -> None:
    variable = {"endpoints": {"msc_orcamentaria": {"classes": [6, 5, 6]}}}
    assert _endpoint(carregar(variable, None, ANO), "msc_orcamentaria")["classes"] == [
        6,
        5,
    ]


# --- Anexos contra /anexos-relatorios ---------------------------------------------

_ANEXOS = [
    {"esfera": "E", "demonstrativo": "DCA", "anexo": "DCA-Anexo I-E"},
    {"esfera": "E", "demonstrativo": "RREO", "anexo": "RREO-Anexo 01"},
    {"esfera": "U", "demonstrativo": "RREO", "anexo": "RREO-Anexo 04.3"},
    {"esfera": "M", "demonstrativo": "RGF", "anexo": "RGF-Anexo 01"},
]


def test_anexo_conhecido_passa() -> None:
    config = carregar(
        {
            "endpoints": {
                "dca": {"no_anexo": ["DCA-Anexo I-E", "Anexo I-E"]},
                "rreo": {"no_anexo": ["RREO-Anexo 04.3"]},
            }
        },
        None,
        ANO,
    )
    assert validar_anexos(config, _ANEXOS) == []


def test_anexo_desconhecido_ou_de_outro_demonstrativo_falha() -> None:
    config = carregar(
        {
            "endpoints": {
                "dca": {"no_anexo": ["DCA-Anexo I-Z"]},
                "rgf": {"no_anexo": ["RREO-Anexo 01"]},
            }
        },
        None,
        ANO,
    )
    erros = "\n".join(validar_anexos(config, _ANEXOS))
    assert "endpoints.dca.no_anexo: ['DCA-Anexo I-Z']" in erros
    assert "endpoints.rgf.no_anexo: ['RREO-Anexo 01']" in erros


def test_anexo_sem_anexos_relatorios_carregado_falha() -> None:
    config = carregar({"endpoints": {"dca": {"no_anexo": ["DCA-Anexo I-E"]}}}, None, ANO)
    assert "não carregado" in validar_anexos(config, [])[0]


def test_null_dispensa_anexos_relatorios() -> None:
    assert validar_anexos(carregar(None, None, ANO), []) == []
