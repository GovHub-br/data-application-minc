"""Tira o byte nulo (``\\x00``) do texto que vai para o Postgres.

O Postgres não guarda esse caractere em ``text`` nem em ``jsonb`` (no JSON ele
vira ``\\u0000``, que o ``jsonb`` também recusa) e recusa o ``COPY`` inteiro
quando encontra um. A regra é a mesma em toda carga: o byte sai, e um valor que
era só ele vira nulo. O raw e o Parquet seguem com o valor original.

Fica fora de ``postgres_helpers`` porque aquele módulo importa o Airflow, e os
clientes que usam este são testados sem ele.
"""

from __future__ import annotations

from typing import Any

import pyarrow as pa
import pyarrow.compute as pc

BYTE_NULO = "\x00"


def sem_byte_nulo(valor: Any) -> Any:
    """``valor`` sem o byte nulo; ``None`` se só havia ele. Não-texto passa igual."""
    if not isinstance(valor, str) or BYTE_NULO not in valor:
        return valor
    return valor.replace(BYTE_NULO, "") or None


def tabela_sem_byte_nulo(tabela: pa.Table) -> pa.Table:
    """``tabela`` com :func:`sem_byte_nulo` aplicado às colunas de texto.

    Coluna sem byte nulo, o caso normal, não é copiada.
    """
    colunas = []
    for coluna in tabela.columns:
        texto = pa.types.is_string(coluna.type) or pa.types.is_large_string(coluna.type)
        if texto and pc.any(pc.match_substring(coluna, BYTE_NULO)).as_py():
            limpa = pc.replace_substring(coluna, BYTE_NULO, "")
            coluna = pc.if_else(pc.equal(limpa, ""), None, limpa)
        colunas.append(coluna)
    return pa.Table.from_arrays(colunas, schema=tabela.schema)
