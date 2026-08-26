"""Testes do cliente do SGS/BACEN: leitura da Variable de séries,
normalização do payload e as duas peculiaridades da API (``ultimos`` no
caminho, 404 em série vazia)."""

from typing import Any

import pytest

from cliente_bacen import (
    ClienteBacen,
    configuracoes_de_series,
    data_sgs_para_iso,
    normalizar_registros,
)


def test_data_do_sgs_vira_iso() -> None:
    # O ponto do teste: 01/07/2026 é 1º de julho. Gravado como veio, o cast
    # para date no Postgres (DateStyle MDY) leria 7 de janeiro.
    assert data_sgs_para_iso("01/07/2026") == "2026-07-01"
    assert data_sgs_para_iso("31/12/1991") == "1991-12-31"


def test_data_fora_do_formato_nao_vira_data_errada() -> None:
    assert data_sgs_para_iso("2026-07-01") is None
    assert data_sgs_para_iso("") is None


def test_normalizacao_monta_a_linha_da_tabela() -> None:
    linhas = normalizar_registros(
        "ipca_servicos",
        10844,
        [{"data": "01/07/2026", "valor": "0.54"}],
        dt_ingest="2026-08-26T10:00:00",
    )

    assert linhas == [
        {
            "serie": "ipca_servicos",
            "codigo_serie": "10844",
            "data": "2026-07-01",
            "valor": "0.54",
            "dt_ingest": "2026-08-26T10:00:00",
        }
    ]


def test_ponto_sem_apuracao_fica_nulo_e_nao_string_vazia() -> None:
    linhas = normalizar_registros(
        "ipca_servicos", 10844, [{"data": "01/07/2026", "valor": ""}]
    )

    assert linhas[0]["valor"] is None


def test_registro_sem_data_valida_e_descartado() -> None:
    # Sem data não há chave primária: a linha não teria como ser atualizada
    # no UPSERT.
    linhas = normalizar_registros(
        "ipca_servicos",
        10844,
        [{"data": "??", "valor": "1.0"}, {"data": "01/07/2026", "valor": "0.54"}],
    )

    assert len(linhas) == 1
    assert linhas[0]["data"] == "2026-07-01"


def test_variable_no_formato_nome_codigo() -> None:
    # A forma que alguém escreve na Variable no dia a dia: puxar mais um
    # código do BACEN é acrescentar uma chave aqui, não criar outra DAG.
    configuracoes = configuracoes_de_series({"ipca_servicos": 10844, "ipca": 433})

    assert configuracoes == [
        {
            "serie": "ipca_servicos",
            "codigo": 10844,
            "data_inicial": None,
            "data_final": None,
        },
        {"serie": "ipca", "codigo": 433, "data_inicial": None, "data_final": None},
    ]


def test_forma_longa_carrega_o_recorte_de_datas() -> None:
    configuracoes = configuracoes_de_series(
        [{"serie": "selic", "codigo": "11", "data_inicial": "01/01/2020"}]
    )

    assert configuracoes == [
        {
            "serie": "selic",
            "codigo": 11,
            "data_inicial": "01/01/2020",
            "data_final": None,
        }
    ]


def test_serie_sem_nome_e_identificada_pelo_codigo() -> None:
    assert configuracoes_de_series([{"codigo": 10844}])[0]["serie"] == "10844"


def test_codigo_invalido_falha_em_vez_de_sumir() -> None:
    # Ignorar em silêncio deixaria a tabela sem os pontos da série e ninguém
    # olhando.
    with pytest.raises(ValueError, match="Codigo SGS invalido"):
        configuracoes_de_series({"ipca_servicos": "dez mil"})

    with pytest.raises(ValueError, match="Configuracao de serie invalida"):
        configuracoes_de_series([{"serie": "sem_codigo"}])

    with pytest.raises(ValueError, match="deve ser objeto"):
        configuracoes_de_series("10844")


@pytest.fixture
def chamadas(monkeypatch: pytest.MonkeyPatch) -> list[dict[str, Any]]:
    """Substitui o ``request`` do ClienteBase e registra o que foi chamado."""
    registradas: list[dict[str, Any]] = []

    def falso_request(
        self: ClienteBacen, method: str, path: str, **kwargs: Any
    ) -> tuple[Any, Any]:
        registradas.append({"path": path, "params": kwargs.get("params", {})})
        return 200, [{"data": "01/07/2026", "valor": "0.54"}]

    monkeypatch.setattr(ClienteBacen, "request", falso_request)
    return registradas


def test_ultimos_vai_no_caminho_e_nao_na_query_string(
    chamadas: list[dict[str, Any]],
) -> None:
    # `?ultimos=13` é ignorado em silêncio pelo SGS: a API responde 200 com a
    # série inteira. Só o caminho limita o retorno.
    ClienteBacen().get_ultimos(10844, 13)

    assert chamadas[0]["path"] == "/bcdata.sgs.10844/dados/ultimos/13"
    assert "ultimos" not in chamadas[0]["params"]


def test_intervalo_vira_data_inicial_e_final(chamadas: list[dict[str, Any]]) -> None:
    ClienteBacen().get_serie(10844, data_inicial="01/01/2022", data_final="31/12/2022")

    assert chamadas[0]["path"] == "/bcdata.sgs.10844/dados"
    assert chamadas[0]["params"] == {
        "formato": "json",
        "dataInicial": "01/01/2022",
        "dataFinal": "31/12/2022",
    }


def test_serie_inteira_nao_manda_recorte(chamadas: list[dict[str, Any]]) -> None:
    ClienteBacen().get_serie(10844)

    assert chamadas[0]["params"] == {"formato": "json"}


def test_falha_da_api_devolve_lista_vazia(monkeypatch: pytest.MonkeyPatch) -> None:
    # O SGS responde 404 tanto para intervalo sem ponto quanto para série
    # inexistente, e o ClienteBase devolve (500, None) depois dos retries.
    # Quem decide se isso é erro é a DAG, não o cliente.
    monkeypatch.setattr(
        ClienteBacen, "request", lambda self, method, path, **kwargs: (500, None)
    )

    assert ClienteBacen().get_serie(999999) == []
