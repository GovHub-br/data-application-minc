"""Testes do cliente da legislação do Senado: a normalização do XML virado
JSON (``""``, objeto ou lista) e o que cada chamada pede à API."""

from http import HTTPStatus
from typing import Any

import pytest

from cliente_senadores import ClienteSenadores


@pytest.mark.parametrize(
    ("valor", "esperado"),
    [
        ("", []),
        (None, []),
        ({"id": "1"}, [{"id": "1"}]),
        ([{"id": "1"}, {"id": "2"}], [{"id": "1"}, {"id": "2"}]),
        (
            "Classificação Temática Unificada / Cultura",
            ["Classificação Temática Unificada / Cultura"],
        ),
    ],
)
def test_como_lista_normaliza_as_tres_formas(valor: Any, esperado: list[Any]) -> None:
    assert ClienteSenadores.como_lista(valor) == esperado


def _lista(documentos: Any) -> dict[str, Any]:
    return {"ListaDocumento": {"Metadados": {"Versao": "x"}, "documentos": documentos}}


def test_documentos_da_lista_vazia_um_e_varios() -> None:
    doc = {"id": "540762", "tipo": "LCP"}
    assert ClienteSenadores.documentos_da_lista(_lista("")) == []
    assert ClienteSenadores.documentos_da_lista(_lista({"documento": doc})) == [doc]
    assert ClienteSenadores.documentos_da_lista(_lista({"documento": [doc, doc]})) == [
        doc,
        doc,
    ]


def test_documentos_de_resposta_inesperada_nao_quebram() -> None:
    assert ClienteSenadores.documentos_da_lista(None) == []
    assert ClienteSenadores.documentos_da_lista({"outra": 1}) == []
    assert ClienteSenadores.documentos_do_detalhe([]) == []


def test_documentos_do_detalhe() -> None:
    doc = {"id": "550189", "ementa": "Restabelece princípios..."}
    resposta = {"DetalheDocumento": {"documentos": {"documento": [doc]}}}
    assert ClienteSenadores.documentos_do_detalhe(resposta) == [doc]


@pytest.fixture
def chamadas(monkeypatch: pytest.MonkeyPatch) -> list[dict[str, Any]]:
    """Substitui o ``request`` do ClienteBase e registra o que foi chamado."""
    registro: list[dict[str, Any]] = []

    def falso_request(
        self: ClienteSenadores, method: str, path: str, **kwargs: Any
    ) -> tuple[HTTPStatus, dict[str, Any]]:
        registro.append({"method": method, "path": path, **kwargs})
        return HTTPStatus.OK, {"ok": True}

    monkeypatch.setattr(ClienteSenadores, "request", falso_request)
    return registro


def test_lista_pede_tipo_e_ano_com_timeout_de_60s(chamadas: list[dict[str, Any]]) -> None:
    status, resposta = ClienteSenadores().get_legislacao_lista("DEC", 1998)

    assert status == HTTPStatus.OK and resposta == {"ok": True}
    (chamada,) = chamadas
    assert chamada["path"] == "/legislacao/lista"
    assert chamada["params"] == {"tipo": "DEC", "ano": 1998}
    assert chamada["timeout"] == 60
    assert chamada["headers"] == {"accept": "application/json"}


def test_detalhe_pede_o_codigo_com_timeout_de_180s(
    chamadas: list[dict[str, Any]],
) -> None:
    ClienteSenadores().get_legislacao_detalhe("549988")

    (chamada,) = chamadas
    assert chamada["path"] == "/legislacao/549988"
    assert chamada["timeout"] == 180


def test_falha_da_api_volta_como_status_sem_excecao(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # O ClienteBase devolve (500, None) depois de esgotar as tentativas.
    monkeypatch.setattr(
        ClienteSenadores, "request", lambda self, method, path, **kwargs: (500, None)
    )
    status, resposta = ClienteSenadores().get_legislacao_detalhe("1")
    assert status == HTTPStatus.INTERNAL_SERVER_ERROR
    assert resposta is None
