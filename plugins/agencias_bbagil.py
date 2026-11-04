"""Geracao dos periodos mensais a extrair do BB Agil.

A descoberta de agencia/conta dos entes deixou de vir de planilha Excel --
ela agora vem da API oficial do Transferegov (``agencias_transferegov.py``).
Este modulo ficou com a unica responsabilidade que nao dependia da
planilha: gerar os periodos mensais (ente x periodo e o produto cartesiano
que ``extracao_bbagil_dag.py`` consome).
"""

import logging
from calendar import monthrange
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

import config_bsc_pnab as settings

logger = logging.getLogger(__name__)

# So se pede mes encerrado. O checkpoint da extracao grava a combinacao
# conta x mes como feita na primeira consulta e nunca mais a repete, entao um
# mes pedido em andamento fica congelado no dia da consulta: foi assim que o
# extrato de set/2026 parou em 04/09 e o de out/2026 veio vazio. A carencia
# cobre o lancamento do ultimo dia que so aparece no dia seguinte, e o fuso
# evita que a virada do mes em UTC (21h em Brasilia) feche o mes antes da hora.
FUSO_BRASILIA = ZoneInfo("America/Sao_Paulo")
DIAS_CARENCIA_MES_FECHADO = 1


def hoje_em_brasilia() -> date:
    return datetime.now(FUSO_BRASILIA).date()


def mes_encerrado(ultimo_dia: date, hoje: date) -> bool:
    """Se o mes que termina em ``ultimo_dia`` ja pode ser pedido em ``hoje``:
    com a carencia de um dia, o mes M entra a partir do dia 2 de M+1."""
    return ultimo_dia < hoje - timedelta(days=DIAS_CARENCIA_MES_FECHADO)


def gerar_periodos_mensais(
    anos: list[int] = settings.LISTA_ANOS,
    hoje: date | None = None,
) -> list[tuple[str, str]]:
    """Gera (primeiro_dia, ultimo_dia) de cada mes encerrado dos anos
    informados, no formato YYYY-MM-DD. O mes em andamento nunca entra -- ver
    ``mes_encerrado``."""
    hoje = hoje or hoje_em_brasilia()
    periodos = []

    for ano in sorted(anos):
        for mes in range(1, 13):
            ultimo_dia = monthrange(ano, mes)[1]
            if not mes_encerrado(date(ano, mes, ultimo_dia), hoje):
                continue
            periodos.append(
                (f"{ano:04d}-{mes:02d}-01", f"{ano:04d}-{mes:02d}-{ultimo_dia:02d}")
            )

    logger.info("[agencias_bbagil] %d periodos mensais gerados", len(periodos))
    return periodos


def apenas_meses_encerrados(
    periodos: list[tuple[str, str]], hoje: date | None = None
) -> list[tuple[str, str]]:
    """Descarta o periodo cujo mes ainda nao encerrou. Existe para quem recebe
    a lista pronta -- o extrato a recebe por XCom de outra task: uma lista
    calculada antes da virada do mes, ou com a regra antiga, nao pode fazer a
    extracao pedir mes em andamento."""
    hoje = hoje or hoje_em_brasilia()
    return [
        periodo
        for periodo in periodos
        if mes_encerrado(date.fromisoformat(periodo[1]), hoje)
    ]


def periodos_extras_por_programa(
    anos_por_programa: dict[str, list[int]] = settings.ANOS_EXTRAS_POR_PROGRAMA,
) -> dict[str, list[tuple[str, str]]]:
    """Periodos mensais a mais por codigo de programa (Variable
    ``bsc_pnab_anos_extras_por_programa``)."""
    if not isinstance(anos_por_programa, dict):
        raise ValueError(
            "bsc_pnab_anos_extras_por_programa deve ser um objeto "
            '{"<codigo_programa>": [anos]}'
        )
    return {
        str(codigo).strip(): gerar_periodos_mensais([int(ano) for ano in anos])
        for codigo, anos in anos_por_programa.items()
    }


def periodos_da_conta(
    codigo_programa: str | None,
    periodos: list[tuple[str, str]],
    extras: dict[str, list[tuple[str, str]]],
) -> list[tuple[str, str]]:
    """Periodos de uma conta: os de LISTA_ANOS mais os extras do programa dela,
    sem repetir mes."""
    adicionais = extras.get(str(codigo_programa or "").strip(), [])
    vistos = set(periodos)
    return list(periodos) + [p for p in adicionais if p not in vistos]
