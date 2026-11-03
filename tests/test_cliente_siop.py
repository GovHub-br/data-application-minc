"""Testes do cliente SPARQL do SIOP.

O que importa aqui: a consulta soma em centavos (o Virtuoso arredonda a soma
de decimal para inteiro), os valores voltam em reais sem passar por float, e
a linha sem alguma dimensão chega como nulo em vez de sumir.

Com ``SIOP_SPARQL_REDE=1`` o último teste consulta o endpoint de verdade e
confere 2023 contra a planilha do Painel do Orçamento carregada em
``siop.execucao_orcamentaria`` (conferido em 02/11/2026). Sem a variável, é
pulado.
"""

import json
import os
from decimal import Decimal
from typing import Any, cast

import pytest

import cliente_siop as siop


def test_consulta_soma_em_centavos_todos_os_valores() -> None:
    texto = siop.consulta(2023)
    assert "GRAPH <http://orcamento.dados.gov.br/2023/>" in texto
    for coluna, predicado in siop.VALORES.items():
        assert f"(SUM(?{coluna} * 100) AS ?{coluna}_centavos)" in texto
        assert f"?i loa:{predicado} ?{coluna} ." in texto
    assert "GROUP BY ?orgao_cod" in texto


def test_consulta_recusa_ano_que_nao_e_numero() -> None:
    with pytest.raises(ValueError):
        siop.consulta(cast(Any, "2023/> } DROP"))


def _resposta(*itens: dict[str, str]) -> bytes:
    bindings = [
        {chave: {"type": "literal", "value": valor} for chave, valor in item.items()}
        for item in itens
    ]
    return json.dumps({"head": {"vars": []}, "results": {"bindings": bindings}}).encode()


def test_registros_em_reais_com_centavos_exatos() -> None:
    resposta = _resposta(
        {
            "orgao_cod": "42000",
            "orgao_nome": "Ministério da Cultura",
            "funcao_cod": "13",
            "funcao_nome": "Cultura",
            "uo_cod": "42207",
            "uo_nome": "Instituto Brasileiro de Museus",
            "rp_cod": "2",
            "rp_nome": "Primária discricionária",
            "projeto_lei_centavos": "0",
            "dotacao_inicial_centavos": "10263096000",
            "dotacao_atual_centavos": "10263096000",
            "empenhado_centavos": "10243445095",
            "liquidado_centavos": "7533618646",
            "pago_centavos": "7397908579",
        }
    )
    (linha,) = siop.registros(2023, resposta)
    assert linha["ano"] == "2023"
    assert linha["uo_cod"] == "42207"
    assert linha["empenhado"] == "102434450.95"
    assert linha["pago"] == "73979085.79"
    assert linha["projeto_lei"] == "0.00"
    assert list(linha) == list(siop.COLUNAS)


def test_dimensao_ausente_vira_nulo() -> None:
    (linha,) = siop.registros(2003, _resposta({"empenhado_centavos": "100"}))
    assert linha["uo_cod"] is None
    assert linha["rp_nome"] is None
    assert linha["empenhado"] == "1.00"
    assert linha["pago"] is None


def test_total_soma_em_decimal() -> None:
    linhas = siop.registros(
        2023,
        _resposta({"empenhado_centavos": "1"}, {"empenhado_centavos": "2"}),
    )
    assert siop.total(linhas) == Decimal("0.03")


@pytest.mark.skipif(not os.environ.get("SIOP_SPARQL_REDE"), reason="sem rede")
def test_2023_bate_com_a_planilha_do_painel() -> None:
    linhas = siop.registros(2023, siop.baixar(2023))
    assert siop.total(linhas) == Decimal("4593770423086.37")
    # A planilha tem 2.559 combinações; a consulta traz 3 a mais, todas zeradas.
    assert len(linhas) == 2562
