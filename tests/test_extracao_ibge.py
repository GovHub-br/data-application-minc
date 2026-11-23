"""Testes dos resultados do SIDRA (``ClienteIBGE.get_resultados_achatar``).

O que importa aqui: cada linha achatada tem uma chave única, e a coluna da
chave não fica nula quando o agregado não tem classificação, como o Censo
2022 na tabela 4709. Sem isso, o upsert da ``ibge_agregados_dag`` não tem
como evitar a duplicação a cada run semanal.

Também a configuração da Variable ``ibge_config`` (``especificacoes_resultados``)
e o envio da classificação ao SIDRA.

Com ``IBGE_SIDRA_REDE=1`` os últimos testes consultam a API de verdade: a 4709
(a população do Censo 2022, uma linha por município e o total do Brasil) e os
10 indicadores dos eixos 3 e 5 por UF.
"""

import http
import json
import os
from typing import Any

import pytest

import extracao_ibge
from extracao_ibge import ClienteIBGE, chave_classificacoes, especificacoes_resultados

_CHAVE = [
    "agregado_id",
    "variavel_id",
    "nivel",
    "localidade_id",
    "periodo",
    "classificacoes_chave",
]

_RESPOSTA = [
    {
        "id": "93",
        "variavel": "População residente",
        "unidade": "Pessoas",
        "resultados": [
            {
                "classificacoes": [],
                "series": [
                    {
                        "localidade": {"id": "1100205", "nome": "Porto Velho (RO)"},
                        "serie": {"2022": "460434"},
                    },
                    {
                        "localidade": {
                            "id": "2605459",
                            "nome": "Fernando de Noronha (PE)",
                        },
                        "serie": {"2022": "3167"},
                    },
                ],
            }
        ],
    },
    {
        "id": "10605",
        "variavel": "Taxa de crescimento geométrico",
        "unidade": "%",
        "resultados": [
            {
                "classificacoes": [{"id": "1", "categoria": {"1": "Total"}}],
                "series": [
                    {
                        "localidade": {"id": "1100205", "nome": "Porto Velho (RO)"},
                        "serie": {"2022": "0.45"},
                    }
                ],
            }
        ],
    },
]


def _achatar(monkeypatch: pytest.MonkeyPatch, resposta: Any) -> list[dict[str, Any]]:
    monkeypatch.setattr(ClienteIBGE, "get_resultados", lambda self, **kw: resposta)
    return ClienteIBGE().get_resultados_achatar("4709", periodos="2022", nivel="N6")


def test_uma_linha_por_variavel_localidade_e_periodo(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    linhas = _achatar(monkeypatch, _RESPOSTA)
    assert len(linhas) == 3
    chaves = [tuple(linha[c] for c in _CHAVE) for linha in linhas]
    assert len(set(chaves)) == len(chaves)
    porto_velho = linhas[0]
    assert porto_velho["valor"] == "460434"
    assert porto_velho["nivel"] == "N6"
    assert porto_velho["periodo"] == "2022"


def test_chave_nao_fica_nula_sem_classificacao(monkeypatch: pytest.MonkeyPatch) -> None:
    linhas = _achatar(monkeypatch, _RESPOSTA)
    assert linhas[0]["classificacoes"] is None
    assert linhas[0]["classificacoes_chave"] == ""
    assert all(linha[c] is not None for linha in linhas for c in _CHAVE)


def test_classificacoes_em_json_e_a_chave_so_com_ids(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """O str() do Python (aspas simples) não era lido pelo SQL, e o nome da
    categoria na chave duplicava a linha quando o IBGE o mudasse."""
    linhas = _achatar(monkeypatch, _RESPOSTA)
    assert json.loads(linhas[2]["classificacoes"]) == [
        {"id": "1", "categoria": {"1": "Total"}}
    ]
    assert linhas[2]["classificacoes_chave"] == "1[1]"


def test_chave_de_classificacoes_em_ordem_de_id() -> None:
    classificacoes = [
        {"id": "2661", "nome": "Localização", "categoria": {"12869": "Em terra"}},
        {"id": "1714", "nome": "Quesito", "categoria": {"60024": "Total"}},
    ]
    assert chave_classificacoes(classificacoes) == "1714[60024]|2661[12869]"
    assert chave_classificacoes([]) == ""


class _RequestQueRegistra:
    def __init__(self) -> None:
        self.chamadas: list[dict[str, Any]] = []

    def __call__(self, cliente: Any, metodo: str, path: str, **kwargs: Any) -> Any:
        self.chamadas.append({"path": path, **kwargs})
        return http.HTTPStatus.OK, []


def test_classificacao_e_timeout_vao_na_requisicao(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registra = _RequestQueRegistra()
    monkeypatch.setattr(extracao_ibge, "request_safe", registra)
    cliente = ClienteIBGE()

    cliente.get_resultados("9605", "2022", "1000093", "N6", classificacao="86[2776,2777]")
    cliente.get_resultados("4714", "2022", "93", "N3")

    com, sem = registra.chamadas
    assert com["path"] == "/agregados/9605/periodos/2022/variaveis/1000093"
    assert com["params"] == {"localidades": "N6[all]", "classificacao": "86[2776,2777]"}
    assert com["timeout"] == extracao_ibge.TIMEOUT_RESULTADOS_S > 10
    assert sem["params"] == {"localidades": "N3[all]"}


# ---------------------------------------------------------------------------
# A configuração da Variable ibge_config
# ---------------------------------------------------------------------------

# Os indicadores dos eixos 3 e 5 do Cultura em Números (issue #170): os
# parâmetros do SIDRA_INDICADORES de analytics/src/pipelines/aux_ibge.py do
# eixo 3, um item por agregado. É o que vai na Variable de cada ambiente.
_CONFIG_EIXOS_3_E_5: dict[str, Any] = {
    "resultados": [
        {
            "agregado": "4714",
            "variaveis": "93",
            "periodos": "2022",
            "niveis": ["N6", "N3"],
        },
        {
            "agregado": "6579",
            "variaveis": "9324",
            "periodos": "2025|2026",
            "niveis": ["N6", "N3"],
        },
        {
            "agregado": "9923",
            "variaveis": "1000093",
            "periodos": "2022",
            "classificacao": "1[1,2]",
            "niveis": ["N6", "N3"],
        },
        {
            "agregado": "9605",
            "variaveis": "1000093",
            "periodos": "2022",
            "classificacao": "86[2776,2777,2778,2779,2780]",
            "niveis": ["N6", "N3"],
        },
        {
            "agregado": "9578",
            "variaveis": "4709|4728",
            "periodos": "2022",
            "classificacao": "2661[32776,60027]",
            "niveis": ["N6", "N3"],
        },
        {
            "agregado": "9718",
            "variaveis": "350|4727",
            "periodos": "2022",
            "classificacao": "1714[60024]|2661[32776,12869]",
            "niveis": ["N6", "N3"],
        },
        {
            "agregado": "9756",
            "variaveis": "9175|10613",
            "periodos": "2022",
            "classificacao": "86[95251]",
            "niveis": ["N6", "N3"],
        },
        {
            "agregado": "9543",
            "variaveis": "2513",
            "periodos": "2022",
            "classificacao": "2[6794]|86[95251]|287[100362]",
            "niveis": ["N6", "N3"],
        },
        {
            "agregado": "5938",
            "variaveis": "37",
            "periodos": "2023",
            "niveis": ["N6", "N3"],
        },
        {
            "agregado": "9537",
            "variaveis": "140|1000140",
            "periodos": "2022",
            "classificacao": "133[2827]|2[6794]|58[95253]",
            "niveis": ["N6", "N3"],
        },
    ]
}


def test_a_configuracao_dos_eixos_3_e_5_e_valida() -> None:
    especificacoes = especificacoes_resultados(_CONFIG_EIXOS_3_E_5)
    assert len(especificacoes) == 10
    assert all(e["niveis"] == ["N6", "N3"] for e in especificacoes)
    cor_ou_raca = next(e for e in especificacoes if e["agregado"] == "9605")
    assert cor_ou_raca["classificacao"] == "86[2776,2777,2778,2779,2780]"


def test_o_formato_antigo_continua_valendo() -> None:
    """O Eixo 1 lê o 4709 que a Variable do MinC configura no formato antigo."""
    antigo = {
        "resultados_agregados_ids": ["4709"],
        "resultados_periodos": "2022",
        "resultados_nivel": "N6",
    }
    assert especificacoes_resultados(antigo) == [
        {
            "agregado": "4709",
            "variaveis": "allxp",
            "periodos": "2022",
            "classificacao": None,
            "niveis": ["N6"],
        }
    ]
    assert especificacoes_resultados({}) == []


def test_padroes_e_listas_unidas() -> None:
    (espec,) = especificacoes_resultados(
        {"resultados": [{"agregado": 6579, "periodos": ["2025", "2026"]}]}
    )
    assert espec == {
        "agregado": "6579",
        "variaveis": "allxp",
        "periodos": "2025|2026",
        "classificacao": None,
        "niveis": ["N3"],
    }


@pytest.mark.parametrize(
    "config, mensagem",
    [
        ({"resultados": [{"variaveis": "93"}]}, "código numérico"),
        ({"resultados": [{"agregado": "4714", "niveis": ["N99"]}]}, "nível desconhecido"),
        (
            {"resultados": [{"agregado": "9605", "classificacao": "86:2776"}]},
            "sintaxe do SIDRA",
        ),
        ({"resultados": [{"agregado": "4714", "periodos": ""}]}, "'periodos' vazio"),
        ({"resultados": [{"agregado": "4714"}, {"agregado": "4714"}]}, "duas vezes"),
        (
            {"resultados": [{"agregado": "4709"}], "resultados_agregados_ids": ["4709"]},
            "Tire-o de resultados_agregados_ids",
        ),
        # dict() de [4714] ou de 4714 dava TypeError, que ia para o retry.
        ({"resultados": [[4714]]}, "não é um objeto"),
        ({"resultados": [4714]}, "não é um objeto"),
        ({"resultados": "4714"}, "precisa ser uma lista"),
        ({"resultados_agregados_ids": "4714"}, "precisa ser uma lista"),
    ],
)
def test_configuracao_invalida(config: dict[str, Any], mensagem: str) -> None:
    with pytest.raises(ValueError, match=mensagem):
        especificacoes_resultados(config)


def test_a_dag_usa_a_mesma_chave() -> None:
    pytest.importorskip("airflow.sdk", reason="DAG do Airflow 3")
    import importlib.util
    from pathlib import Path

    arquivo = (
        Path(__file__).resolve().parents[1]
        / "dags"
        / "data_ingest"
        / "ibge"
        / "ibge_agregados_dag.py"
    )
    spec = importlib.util.spec_from_file_location("ibge_agregados_dag", arquivo)
    assert spec and spec.loader
    modulo = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(modulo)
    assert modulo.CHAVE_RESULTADOS == _CHAVE


@pytest.mark.skipif(not os.environ.get("IBGE_SIDRA_REDE"), reason="sem rede")
def test_censo_2022_uma_linha_por_municipio_e_o_total_do_brasil() -> None:
    linhas = ClienteIBGE().get_resultados_achatar(
        "4709", periodos="2022", variaveis="93", nivel="N6"
    )
    assert len(linhas) == 5570
    assert len({linha["localidade_id"] for linha in linhas}) == 5570
    # População residente do Brasil no Censo 2022.
    assert sum(int(linha["valor"]) for linha in linhas) == 203_080_756


@pytest.mark.skipif(not os.environ.get("IBGE_SIDRA_REDE"), reason="sem rede")
@pytest.mark.parametrize(
    "espec",
    especificacoes_resultados(_CONFIG_EIXOS_3_E_5),
    ids=lambda e: e["agregado"],
)
def test_indicadores_dos_eixos_3_e_5_por_uf(espec: dict[str, Any]) -> None:
    """Cada indicador volta completo: um valor por UF em cada combinação de
    variável, período e categorias pedida, sem chave repetida."""
    linhas = ClienteIBGE(sleep_entre_requests=0).get_resultados_achatar(
        espec["agregado"],
        periodos=espec["periodos"],
        variaveis=espec["variaveis"],
        nivel="N3",
        classificacao=espec["classificacao"],
    )
    chaves = {tuple(linha[c] for c in _CHAVE) for linha in linhas}
    assert len(chaves) == len(linhas) > 0
    combinacoes = {
        (linha["variavel_id"], linha["periodo"], linha["classificacoes_chave"])
        for linha in linhas
    }
    assert len({linha["localidade_id"] for linha in linhas}) == 27
    assert len(linhas) == len(combinacoes) * 27
