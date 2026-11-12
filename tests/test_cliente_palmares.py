"""Testes do cliente da planilha de comunidades quilombolas certificadas (Palmares).

O que importa aqui: cada certidão vira um registro com a linha da planilha e
a versão, sem mudar o que a célula diz; a versão sai do carimbo da aba DADOS
e a carga falha, em vez de adivinhar, quando o carimbo falta ou não bate com
as portarias; e uma planilha com outra forma falha em vez de carregar.

Com ``PALMARES_REDE=1`` o último teste baixa a planilha de verdade e confere
a forma dela. Sem a variável, é pulado.
"""

import io
import os
from datetime import date
from typing import Any

import pytest

import cliente_palmares as palmares

openpyxl = pytest.importorskip("openpyxl")
pytest.importorskip("python_calamine")

_CABECALHO = [
    "REGIÃO",
    " UF",
    "MUNICÍPIO",
    "CÓDIGO DO IBGE",
    "COMUNIDADE",
    "Nº DO REGISTRO",
    "Nº DA PORTARIA",
    "DATA DA PORTARIA NO DOU",
    "Nº PROCESSO INCRA",
    "Nº DE CRQ",
]
_HOJE = date(2026, 11, 12)


def _linha(registro: int, dou: Any, portaria: Any = "1/2026") -> list[Any]:
    return [
        "NORDESTE",
        "BA",
        "BARRA",
        2903201,
        f"COMUNIDADE {registro}",
        registro,
        portaria,
        dou,
        "\xa0",
        1,
    ]


def _xlsx(
    certificadas: list[list[Any]] | None,
    dados: list[list[Any]] | None = None,
) -> bytes:
    livro = openpyxl.Workbook()
    livro.remove(livro.active)
    if certificadas is not None:
        aba = livro.create_sheet("CERTIFICADAS")
        for linha in certificadas:
            aba.append(linha)
    if dados is not None:
        aba = livro.create_sheet("DADOS")
        for linha in dados:
            aba.append(linha)
    buffer = io.BytesIO()
    livro.save(buffer)
    return buffer.getvalue()


_DADOS = [
    ["CERTIFICADAS POR ESTADO", None, "NÚMERO DE COMUNIDADES CERTIFICADAS POR ANO"],
    ["IMPORTANTE: faça o download da planilha", "ANO", "Quantidade"],
    ["*ÚLTIMA ATUALIZAÇÃO: 11/11/2026", 2004, 270],
]
_PLANILHA = [
    _CABECALHO,
    _linha(1, date(2004, 6, 4), "nov./06"),
    [None] * len(_CABECALHO),
    _linha(2, date(2026, 11, 5), 254.0),
]


def test_cada_certidao_vira_um_registro_com_linha_e_versao() -> None:
    saida = palmares.registros(_xlsx(_PLANILHA, _DADOS), "2026-11-11", minimo=2)
    assert saida == [
        {
            "regiao": "NORDESTE",
            "uf": "BA",
            "municipio": "BARRA",
            "codigo_do_ibge": "2903201",
            "comunidade": "COMUNIDADE 1",
            "no_do_registro": "1",
            # O Excel virou a Portaria 11/2006 em data; a célula é texto e fica.
            "no_da_portaria": "nov./06",
            "data_da_portaria_no_dou": "2004-06-04",
            # Vazio vem como espaço não separável e vira nulo.
            "no_processo_incra": None,
            "no_de_crq": "1",
            "linha": "2",
            "versao": "2026-11-11",
        },
        {
            "regiao": "NORDESTE",
            "uf": "BA",
            "municipio": "BARRA",
            "codigo_do_ibge": "2903201",
            "comunidade": "COMUNIDADE 2",
            "no_do_registro": "2",
            "no_da_portaria": "254",
            "data_da_portaria_no_dou": "2026-11-05",
            "no_processo_incra": None,
            "no_de_crq": "1",
            # A linha vazia é pulada sem renumerar a seguinte.
            "linha": "4",
            "versao": "2026-11-11",
        },
    ]


def test_registro_repetido_nao_falha() -> None:
    planilha = [_CABECALHO, _linha(7, date(2010, 1, 1)), _linha(7, date(2010, 1, 1))]
    assert len(palmares.registros(_xlsx(planilha), "v", minimo=2)) == 2


@pytest.mark.parametrize(
    ("planilha", "erro"),
    [
        (None, "não tem a aba CERTIFICADAS"),
        ([_CABECALHO[:3], ["NORDESTE", "BA", "BARRA"]], "não tem as colunas"),
        ([_CABECALHO + ["Versão"], _linha(1, date(2010, 1, 1)) + ["x"]], "coluna versao"),
        ([_CABECALHO + ["Linha"], _linha(1, date(2010, 1, 1)) + ["x"]], "coluna linha"),
        ([_CABECALHO, _linha(1, date(2010, 1, 1))], "esperava ao menos 2"),
    ],
)
def test_planilha_com_outra_forma_falha(
    planilha: list[list[Any]] | None, erro: str
) -> None:
    with pytest.raises(ValueError, match=erro):
        palmares.registros(_xlsx(planilha, _DADOS), "v", minimo=2)


def test_versao_e_o_carimbo_da_aba_dados() -> None:
    conteudo = _xlsx(_PLANILHA, _DADOS)
    assert palmares.versao_da_planilha(conteudo, hoje=_HOJE) == "2026-11-11"


def test_sem_carimbo_falha_e_manda_informar_a_versao() -> None:
    conteudo = _xlsx(_PLANILHA, [["CERTIFICADAS POR ESTADO"]])
    with pytest.raises(ValueError, match='"versao"'):
        palmares.versao_da_planilha(conteudo, hoje=_HOJE)


@pytest.mark.parametrize(
    ("dados", "erro"),
    [
        (_DADOS + [["ÚLTIMA ATUALIZAÇÃO: 01/11/2026"]], "mais de um carimbo"),
        ([["*ÚLTIMA ATUALIZAÇÃO: 31/02/2026"]], "data inválida"),
        ([["*ÚLTIMA ATUALIZAÇÃO: 13/11/2026"]], "depois de hoje"),
        # A planilha tem portaria de 05/11: o carimbo de 01/11 está velho.
        ([["*Última atualização: 01/11/2026"]], "anterior à portaria"),
    ],
)
def test_carimbo_que_nao_bate_falha(dados: list[list[Any]], erro: str) -> None:
    with pytest.raises(ValueError, match=erro):
        palmares.versao_da_planilha(_xlsx(_PLANILHA, dados), hoje=_HOJE)


def test_carimbo_repetido_com_a_mesma_data_vale() -> None:
    conteudo = _xlsx(_PLANILHA, _DADOS + [["ÚLTIMA ATUALIZAÇÃO: 11/11/2026"]])
    assert palmares.versao_da_planilha(conteudo, hoje=_HOJE) == "2026-11-11"


def test_portaria_no_futuro_nao_entra_na_conferencia() -> None:
    planilha = _PLANILHA + [_linha(3, date(2062, 7, 23))]
    conteudo = _xlsx(planilha, _DADOS)
    assert palmares.versao_da_planilha(conteudo, hoje=_HOJE) == "2026-11-11"


def test_versao_informada_vence_o_carimbo_e_e_conferida() -> None:
    conteudo = _xlsx(_PLANILHA, [["sem carimbo"]])
    assert (
        palmares.versao_da_planilha(conteudo, informada="2026-11-12", hoje=_HOJE)
        == "2026-11-12"
    )
    with pytest.raises(ValueError, match="anterior à portaria"):
        palmares.versao_da_planilha(conteudo, informada="2026-11-01", hoje=_HOJE)
    with pytest.raises(ValueError, match="não é AAAA-MM-DD"):
        palmares.versao_da_planilha(conteudo, informada="11/11/2026", hoje=_HOJE)
    with pytest.raises(ValueError, match="inválida"):
        palmares.versao_da_planilha(conteudo, informada="2026-02-31", hoje=_HOJE)


def test_tabela_no_schema_seeds() -> None:
    assert palmares.tabela_seeds() == "palmares_quilombos_certificados"


class _Resposta:
    def __init__(self, conteudo: bytes, status_code: int = 200) -> None:
        self.content = conteudo
        self.status_code = status_code
        self.headers = {"content-type": "text/html; charset=utf-8"}

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


def test_baixar_recusa_a_pagina_de_login(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        palmares.requests, "get", lambda url, **kw: _Resposta(b"<!doctype html>")
    )
    with pytest.raises(ValueError, match="não é xlsx"):
        palmares.baixar()


def test_baixar_devolve_o_xlsx(monkeypatch: pytest.MonkeyPatch) -> None:
    conteudo = _xlsx(_PLANILHA, _DADOS)
    monkeypatch.setattr(palmares.requests, "get", lambda url, **kw: _Resposta(conteudo))
    assert palmares.baixar() == conteudo


def test_baixar_404_falha(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(palmares.requests, "get", lambda url, **kw: _Resposta(b"", 404))
    with pytest.raises(RuntimeError, match="HTTP 404"):
        palmares.baixar()


@pytest.mark.skipif(not os.environ.get("PALMARES_REDE"), reason="sem rede")
def test_planilha_publicada_tem_a_forma_que_a_carga_conhece() -> None:
    conteudo = palmares.baixar()
    versao = palmares.versao_da_planilha(conteudo)
    assert versao >= "2026-11-11"
    saida = palmares.registros(conteudo, versao)
    assert len(saida) >= 3_578
    assert set(palmares.COLUNAS_ESPERADAS) <= set(saida[0])
    # Toda certidão cobre ao menos uma comunidade (4.783 em 12/11/2026).
    comunidades = sum(int(r["no_de_crq"] or 0) for r in saida)
    assert comunidades >= len(saida)
    assert len({r["linha"] for r in saida}) == len(saida)
