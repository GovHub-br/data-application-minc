"""Testes dos períodos do extrato do BB Ágil, com os anos extras por programa.

O que importa aqui: só as contas do programa listado na Variable
``bsc_pnab_anos_extras_por_programa`` ganham os meses a mais (a LAB 1 em
2020-2022), as outras continuam com os de ``bsc_pnab_lista_anos``, e um mês
que já está na lista não é pedido duas vezes.

E o mês em andamento nunca é pedido: o checkpoint grava conta × mês como feito
na primeira consulta, então pedir o mês corrente o congelava no dia da consulta
(o extrato de set/2026 parou em 04/09 e o de out/2026 veio vazio).
"""

from datetime import date
from typing import Any, cast

import pytest

pytest.importorskip("airflow.sdk", reason="config_bsc_pnab lê Variables do Airflow 3")

import agencias_bbagil  # noqa: E402

_LAB1_MUNICIPIOS = "30882120200002"
_LPG_MUNICIPIOS = "30882120230006"


def test_ano_gera_doze_meses_com_o_ultimo_dia() -> None:
    periodos = agencias_bbagil.gerar_periodos_mensais([2020])
    assert len(periodos) == 12
    assert periodos[1] == ("2020-02-01", "2020-02-29")
    assert periodos[-1] == ("2020-12-01", "2020-12-31")


def test_mes_em_andamento_nao_entra() -> None:
    periodos = agencias_bbagil.gerar_periodos_mensais([2026], hoje=date(2026, 11, 4))
    assert len(periodos) == 10
    assert periodos[-1] == ("2026-10-01", "2026-10-31")


def test_mes_recem_encerrado_espera_um_dia_de_carencia() -> None:
    no_dia_1 = agencias_bbagil.gerar_periodos_mensais([2026], hoje=date(2026, 11, 1))
    no_dia_2 = agencias_bbagil.gerar_periodos_mensais([2026], hoje=date(2026, 11, 2))
    assert no_dia_1[-1] == ("2026-09-01", "2026-09-30")
    assert no_dia_2[-1] == ("2026-10-01", "2026-10-31")


def test_virada_do_ano_fecha_dezembro() -> None:
    periodos = agencias_bbagil.gerar_periodos_mensais([2026, 2027], hoje=date(2027, 1, 2))
    assert len(periodos) == 12
    assert periodos[-1] == ("2026-12-01", "2026-12-31")


def test_padrao_usa_o_dia_de_brasilia_e_deixa_o_mes_corrente_de_fora() -> None:
    hoje = agencias_bbagil.hoje_em_brasilia()
    periodos = agencias_bbagil.gerar_periodos_mensais([hoje.year])
    assert all(not inicio.startswith(f"{hoje:%Y-%m}") for inicio, _ in periodos)


def test_lista_recebida_perde_o_mes_em_andamento() -> None:
    recebidos = [
        ("2026-09-01", "2026-09-30"),
        ("2026-10-01", "2026-10-31"),
        ("2026-11-01", "2026-11-30"),
    ]
    encerrados = agencias_bbagil.apenas_meses_encerrados(
        recebidos, hoje=date(2026, 11, 4)
    )
    assert encerrados == recebidos[:2]


def test_so_o_programa_listado_ganha_os_anos_extras() -> None:
    base = agencias_bbagil.gerar_periodos_mensais([2023])
    extras = agencias_bbagil.periodos_extras_por_programa(
        {_LAB1_MUNICIPIOS: [2020, 2021]}
    )
    lab1 = agencias_bbagil.periodos_da_conta(_LAB1_MUNICIPIOS, base, extras)
    lpg = agencias_bbagil.periodos_da_conta(_LPG_MUNICIPIOS, base, extras)
    assert len(lab1) == 36
    assert lab1[:12] == base
    assert ("2020-01-01", "2020-01-31") in lab1
    assert lpg == base


def test_mes_repetido_nao_e_pedido_duas_vezes() -> None:
    base = agencias_bbagil.gerar_periodos_mensais([2023])
    extras = agencias_bbagil.periodos_extras_por_programa({_LAB1_MUNICIPIOS: [2023]})
    assert agencias_bbagil.periodos_da_conta(_LAB1_MUNICIPIOS, base, extras) == base


def test_codigo_numerico_e_sem_programa() -> None:
    base = agencias_bbagil.gerar_periodos_mensais([2023])
    extras = agencias_bbagil.periodos_extras_por_programa(
        cast(Any, {30882120200002: ["2020"]})
    )
    assert len(agencias_bbagil.periodos_da_conta(" 30882120200002 ", base, extras)) == 24
    assert agencias_bbagil.periodos_da_conta(None, base, extras) == base


def test_variable_fora_do_formato_falha() -> None:
    with pytest.raises(ValueError, match="deve ser um objeto"):
        agencias_bbagil.periodos_extras_por_programa(cast(Any, [2020]))


def test_padrao_nao_pede_anos_extras() -> None:
    assert agencias_bbagil.periodos_extras_por_programa() == {}
