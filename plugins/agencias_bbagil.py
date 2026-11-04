"""Geracao dos periodos mensais a extrair do BB Agil.

A descoberta de agencia/conta dos entes deixou de vir de planilha Excel --
ela agora vem da API oficial do Transferegov (``agencias_transferegov.py``).
Este modulo ficou com a unica responsabilidade que nao dependia da
planilha: gerar os periodos mensais (ente x periodo e o produto cartesiano
que ``extracao_bbagil_dag.py`` consome).
"""

import logging
from calendar import monthrange
from datetime import date

import config_bsc_pnab as settings

logger = logging.getLogger(__name__)


def gerar_periodos_mensais(
    anos: list[int] = settings.LISTA_ANOS,
) -> list[tuple[str, str]]:
    """Gera (primeiro_dia, ultimo_dia) de cada mes para os anos informados,
    no formato YYYY-MM-DD, sem ultrapassar o mes corrente."""
    hoje = date.today()
    periodos = []

    for ano in sorted(anos):
        for mes in range(1, 13):
            if (ano, mes) > (hoje.year, hoje.month):
                continue
            ultimo_dia = monthrange(ano, mes)[1]
            periodos.append(
                (f"{ano:04d}-{mes:02d}-01", f"{ano:04d}-{mes:02d}-{ultimo_dia:02d}")
            )

    logger.info("[agencias_bbagil] %d periodos mensais gerados", len(periodos))
    return periodos


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
