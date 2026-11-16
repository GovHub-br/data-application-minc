"""Testes do cliente do cadastro de salas de exibição da ANCINE.

O que importa aqui:

- O CSV em UTF-8 com BOM vira registros com acento, a linha do arquivo e a
  versão, e o vazio vira nulo.
- A contagem por quebra de linha não depende do parser e diverge dele quando
  um campo traz quebra de linha.
- A versão é o dia do ``Last-Modified`` em Brasília, não em GMT.
- A carga falha, em vez de adivinhar, quando o arquivo muda de forma.
"""

from datetime import date
from typing import Any

import pytest

import cliente_ancine as ancine

_CABECALHO = (
    "NOME_SALA;REGISTRO_SALA;SITUACAO_SALA;REGISTRO_COMPLEXO;SITUACAO_COMPLEXO;"
    "MUNICIPIO_COMPLEXO;UF_COMPLEXO"
)
_CSV = (
    "﻿" + _CABECALHO + "\n"
    "SALA 1;5001;EM FUNCIONAMENTO;2001;EM FUNCIONAMENTO;JAGUARIÚNA;SP\n"
    ";;;;;;\n"
    "SALA ANTIGA;;NÃO ENCONTRADA;2002;FECHADO;SÃO CAETANO DO SUL;SP\n"
).encode("utf-8")


def test_registros_do_csv_em_utf8_com_bom() -> None:
    registros = ancine.registros(_CSV, "2026-11-06", minimo=1)
    assert registros == [
        {
            "nome_sala": "SALA 1",
            "registro_sala": "5001",
            "situacao_sala": "EM FUNCIONAMENTO",
            "registro_complexo": "2001",
            "situacao_complexo": "EM FUNCIONAMENTO",
            "municipio_complexo": "JAGUARIÚNA",
            "uf_complexo": "SP",
            "linha": "2",
            "versao": "2026-11-06",
        },
        {
            "nome_sala": "SALA ANTIGA",
            "registro_sala": None,
            "situacao_sala": "NÃO ENCONTRADA",
            "registro_complexo": "2002",
            "situacao_complexo": "FECHADO",
            "municipio_complexo": "SÃO CAETANO DO SUL",
            "uf_complexo": "SP",
            # A linha vazia (a 3) é pulada sem renumerar a seguinte.
            "linha": "4",
            "versao": "2026-11-06",
        },
    ]


def test_contagem_por_quebra_de_linha_bate_com_o_parser() -> None:
    assert ancine.contar_linhas(_CSV) == 2
    assert len(ancine.registros(_CSV, "2026-11-06", minimo=1)) == 2


def test_quebra_de_linha_dentro_de_campo_faz_as_contagens_divergirem() -> None:
    com_quebra = (
        _CABECALHO + "\n"
        '"SALA\nDUPLA";5001;EM FUNCIONAMENTO;2001;EM FUNCIONAMENTO;RECIFE;PE\n'
    ).encode("utf-8")
    assert len(ancine.registros(com_quebra, "2026-11-06", minimo=1)) == 1
    assert ancine.contar_linhas(com_quebra) == 2


def test_falta_coluna_esperada_falha() -> None:
    sem_uf = _CSV.decode("utf-8").replace(";UF_COMPLEXO", "").encode("utf-8")
    with pytest.raises(ValueError, match="uf_complexo"):
        ancine.registros(sem_uf, "2026-11-06", minimo=1)


def test_coluna_que_a_carga_acrescenta_nao_pode_vir_no_csv() -> None:
    com_versao = _CSV.decode("utf-8").replace("UF_COMPLEXO", "VERSAO").encode("utf-8")
    with pytest.raises(ValueError, match="já tem a coluna versao"):
        ancine.registros(com_versao, "2026-11-06", minimo=1)


def test_arquivo_cortado_falha() -> None:
    with pytest.raises(ValueError, match="esperava ao menos"):
        ancine.registros(_CSV, "2026-11-06")


def test_csv_que_deixou_de_ser_utf8_falha() -> None:
    em_latin1 = _CSV.decode("utf-8-sig").encode("latin-1")
    with pytest.raises(ValueError, match="UTF-8"):
        ancine.registros(em_latin1, "2026-11-06", minimo=1)


@pytest.mark.parametrize(
    ("last_modified", "versao"),
    [
        ("Fri, 06 Nov 2026 17:36:25 GMT", "2026-11-06"),
        # 02:00 em GMT ainda é 23:00 do dia anterior em Brasília.
        ("Sat, 07 Nov 2026 02:00:00 GMT", "2026-11-06"),
    ],
)
def test_versao_e_o_dia_do_last_modified_em_brasilia(
    last_modified: str, versao: str
) -> None:
    assert ancine.versao_do_arquivo(last_modified, hoje=date(2026, 11, 16)) == versao


def test_sem_last_modified_falha_e_pede_a_versao() -> None:
    with pytest.raises(ValueError, match='"versao"'):
        ancine.versao_do_arquivo(None, hoje=date(2026, 11, 16))


def test_last_modified_que_nao_e_data_falha() -> None:
    with pytest.raises(ValueError, match="não é uma data HTTP"):
        ancine.versao_do_arquivo("ontem", hoje=date(2026, 11, 16))


def test_versao_informada_substitui_o_cabecalho() -> None:
    assert (
        ancine.versao_do_arquivo(
            "Fri, 06 Nov 2026 17:36:25 GMT",
            informada="2026-11-01",
            hoje=date(2026, 11, 16),
        )
        == "2026-11-01"
    )


@pytest.mark.parametrize("informada", ["06/11/2026", "2026-02-30"])
def test_versao_informada_invalida_falha(informada: str) -> None:
    with pytest.raises(ValueError, match="versão informada"):
        ancine.versao_do_arquivo(None, informada=informada, hoje=date(2026, 11, 16))


def test_versao_no_futuro_falha() -> None:
    with pytest.raises(ValueError, match="depois de hoje"):
        ancine.versao_do_arquivo("Fri, 06 Nov 2026 17:36:25 GMT", hoje=date(2026, 11, 5))


class _Resposta:
    def __init__(self, conteudo: bytes, headers: dict[str, str]) -> None:
        self.content = conteudo
        self.headers = headers

    def raise_for_status(self) -> None:
        pass


def test_baixar_devolve_o_arquivo_e_os_cabecalhos(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    chamadas: list[dict[str, Any]] = []

    def falso_get(url: str, **kwargs: Any) -> _Resposta:
        chamadas.append({"url": url, **kwargs})
        return _Resposta(
            _CSV,
            {"Last-Modified": "Fri, 06 Nov 2026 17:36:25 GMT", "ETag": '"6aee1019"'},
        )

    monkeypatch.setattr(ancine.requests, "get", falso_get)
    arquivo = ancine.baixar()
    assert arquivo == ancine.Arquivo(_CSV, "Fri, 06 Nov 2026 17:36:25 GMT", '"6aee1019"')
    assert chamadas[0]["url"] == ancine.URL_CSV


def test_baixar_recusa_pagina_no_lugar_do_csv(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        ancine.requests,
        "get",
        lambda url, **kw: _Resposta(
            b"<!DOCTYPE html><html>...</html>", {"content-type": "text/html"}
        ),
    )
    with pytest.raises(ValueError, match="não é o CSV"):
        ancine.baixar()
