"""Testes dos resultados do SIDRA (``ClienteIBGE.get_resultados_achatar``).

O que importa aqui: cada linha achatada tem uma chave única, e a coluna da
chave não fica nula quando o agregado não tem classificação, como o Censo
2022 na tabela 4709. Sem isso, o upsert da ``ibge_agregados_dag`` não tem
como evitar a duplicação a cada run semanal.

Com ``IBGE_SIDRA_REDE=1`` o último teste consulta a 4709 de verdade e confere
a população do Censo 2022: uma linha por município e o total do Brasil.
"""

import os
from typing import Any

import pytest

from extracao_ibge import ClienteIBGE

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
    assert linhas[2]["classificacoes_chave"] == linhas[2]["classificacoes"] != ""
    assert all(linha[c] is not None for linha in linhas for c in _CHAVE)


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
