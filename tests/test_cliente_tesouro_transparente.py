"""Testes do cliente do CKAN do Tesouro Transparente.

O que importa aqui: o CSV em latin-1 com CRLF vira registros com acento e
com o recurso de origem, a contagem por quebra de linha não depende do
parser, e a URL de download vem do ``package_show`` pelo id do recurso.
"""

from typing import Any

import pytest

import cliente_tesouro_transparente as tesouro

_RECURSO = tesouro.RECURSOS["lrf_uniao_dcl"]
_CSV = (
    "PERÍODO;DÍVIDA CONSOLIDADA - DC (I);RECEITA CORRENTE LÍQUIDA - RCL\r\n"
    "2000 - Até o 3º Quadrimestre;829736251;145110650\r\n"
    "2018 - Até o 1º Quadrimestre;5461391644;743632071,9\r\n"
    ";;\r\n"
).encode("latin-1")


def test_registros_do_csv_em_latin1() -> None:
    registros = tesouro.registros(_RECURSO, _CSV, "2021-12-08")
    assert registros == [
        {
            "periodo": "2000 - Até o 3º Quadrimestre",
            "divida_consolidada_dc_i": "829736251",
            "receita_corrente_liquida_rcl": "145110650",
            "versao": "2021-12-08",
            "recurso_id": _RECURSO.recurso_id,
        },
        {
            "periodo": "2018 - Até o 1º Quadrimestre",
            "divida_consolidada_dc_i": "5461391644",
            "receita_corrente_liquida_rcl": "743632071,9",
            "versao": "2021-12-08",
            "recurso_id": _RECURSO.recurso_id,
        },
    ]


def test_contagem_por_quebra_de_linha_ignora_linha_vazia() -> None:
    assert tesouro.contar_linhas(_RECURSO, _CSV) == 2


class _Resposta:
    def __init__(self, corpo: dict[str, Any]) -> None:
        self.corpo = corpo

    def raise_for_status(self) -> None:
        pass

    def json(self) -> dict[str, Any]:
        return self.corpo


def test_descrever_acha_o_recurso_pelo_id(monkeypatch: pytest.MonkeyPatch) -> None:
    pacote = {
        "metadata_modified": "2021-12-08T10:00:00",
        "resources": [
            {"id": "outro", "url": "https://x/metadados.pdf"},
            {
                "id": _RECURSO.recurso_id,
                "url": "https://x/dcl.csv",
                "last_modified": "2021-12-07T09:00:00",
            },
        ],
    }
    chamadas: list[dict[str, Any]] = []

    def falso_get(url: str, **kwargs: Any) -> _Resposta:
        chamadas.append({"url": url, **kwargs})
        return _Resposta({"result": pacote})

    monkeypatch.setattr(tesouro.requests, "get", falso_get)
    assert tesouro.descrever(_RECURSO) == {
        "url": "https://x/dcl.csv",
        "versao": "2021-12-07",
    }
    assert chamadas[0]["params"] == {"id": _RECURSO.conjunto}


def test_recurso_que_sumiu_do_conjunto_falha(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        tesouro.requests,
        "get",
        lambda url, **kw: _Resposta(
            {"result": {"metadata_modified": "2021-12-08", "resources": []}}
        ),
    )
    with pytest.raises(ValueError, match="não está mais"):
        tesouro.descrever(_RECURSO)
