"""A carga da bronze do SICONFI contra um Trino de verdade.

``test_siconfi_bronze.py`` confere o SQL que a carga **escreve**. Aqui ele é
**executado**, no mesmo Trino descartável de ``test_siconfi_controle_trino.py``
(catálogo PostgreSQL, sem ``system.query``). O que só o Trino diz: o
``json_parse`` com ``UNNEST`` põe cada valor na coluna certa, o número chega
como o texto do JSON, o apóstrofo e o nulo sobrevivem, a coluna nova da
segunda página entra sem apagar as linhas da primeira, e o item acha o
envelope da página em ``paginas`` pelo ``_pagina_id``.

Roda quando ``SICONFI_TEST_TRINO`` aponta para esse Trino (``host:porta``);
a receita para subi-lo está no topo de ``test_siconfi_controle_trino.py``.
Sem a variável, é pulado.
"""

import json
import os
import uuid
from datetime import datetime, timezone
from typing import Any, Iterator, Sequence

import pytest

import siconfi_bronze as bronze

_TRINO = os.environ.get("SICONFI_TEST_TRINO")
_CATALOGO = os.environ.get("SICONFI_TEST_TRINO_CATALOG", "hml_analytics")

pytestmark = pytest.mark.skipif(
    not _TRINO, reason="SICONFI_TEST_TRINO não definida: precisa de um Trino"
)


class _Trino:
    def __init__(self, endereco: str) -> None:
        import trino

        host, porta = endereco.rsplit(":", 1)
        self.conn = trino.dbapi.connect(host=host, port=int(porta), user="airflow")

    def run(self, sql: str) -> None:
        cur = self.conn.cursor()
        cur.execute(sql)
        cur.fetchall()

    def records(self, sql: str) -> list[Sequence[Any]]:
        cur = self.conn.cursor()
        cur.execute(sql)
        return list(cur.fetchall())


@pytest.fixture
def destino() -> Iterator[bronze.Destino]:
    executor = _Trino(str(_TRINO))
    schema = f"siconfi_bronze_teste_{uuid.uuid4().hex[:8]}"
    alvo = bronze.Destino(executor, _CATALOGO, schema, "msc_orcamentaria")
    try:
        alvo.garantir()
        executor.run(bronze.ddl_paginas(_CATALOGO, schema))
        yield alvo
    finally:
        for nome in ("msc_orcamentaria", bronze.TABELA_PAGINAS):
            executor.run(f'DROP TABLE IF EXISTS "{_CATALOGO}"."{schema}"."{nome}"')
        executor.run(f'DROP SCHEMA IF EXISTS "{_CATALOGO}"."{schema}"')


def _texto(itens: list[str]) -> str:
    """Um arquivo do raw escrito à mão, para controlar o texto dos números."""
    return (
        '{"endpoint": "msc_orcamentaria", "request_hash": "h", '
        '"request_params": {"id_ente": 17}, "page_offset": 0, '
        '"fetched_at": "2026-10-27T10:00:00+00:00", "run_id": "r", '
        '"payload": {"items": [' + ", ".join(itens) + "]}}"
    )


def test_carga_fiel_com_partes_apostrofo_nulo_e_coluna_nova(
    destino: bronze.Destino,
) -> None:
    parte1 = _texto(['{"conta": "622920103", "valor": 18449.34, "funcao": null}'])
    parte2 = _texto(
        [
            '{"conta": "6221301", "valor": 1e-05, "funcao": "13"}',
            '{"conta": "d\'água", "valor": 7, "funcao": "13"}',
        ]
    )
    lida = bronze.juntar_partes("raw/p.json", ["p1", "p2"], [parte1, parte2])
    assert bronze.carregar_pagina(destino, lida) == 3
    paginas = bronze.tabela(_CATALOGO, destino.schema, bronze.TABELA_PAGINAS)
    agora = datetime(2026, 11, 5, tzinfo=timezone.utc)
    for comando in bronze.inserts_paginas(
        _CATALOGO, destino.schema, [bronze.registro_da_pagina(lida, "run-1", agora)]
    ):
        destino.executor.run(comando)

    linhas = destino.executor.records(
        f"SELECT i._item, i.conta, i.valor, i.funcao, p.request_params, p.itens "
        f"FROM {destino.nome} i JOIN {paginas} p ON p.pagina_id = i._pagina_id "
        "ORDER BY CAST(i._item AS integer)"
    )
    assert [tuple(linha) for linha in linhas] == [
        ("1", "622920103", "18449.34", None, '{"id_ente":17}', "3"),
        ("2", "6221301", "1e-05", "13", '{"id_ente":17}', "3"),
        ("3", "d'água", "7", "13", '{"id_ente":17}', "3"),
    ]
    feitas = destino.executor.records(
        bronze.sql_paginas_carregadas(_CATALOGO, destino.schema, "msc_orcamentaria")
    )
    assert [tuple(f) for f in feitas] == [("raw/p.json",)]

    # Segunda página com um campo que a tabela ainda não tem.
    outra = bronze.juntar_partes(
        "raw/q.json", ["q"], [_texto(['{"conta": "1", "valor": 2, "uf": "TO"}'])]
    )
    assert bronze.carregar_pagina(destino, outra) == 1
    contagem = destino.executor.records(
        f"SELECT count(*), count(uf), count(DISTINCT _pagina_id) FROM {destino.nome}"
    )
    assert tuple(contagem[0]) == (4, 1, 2)


def test_pagina_grande_vira_varios_inserts_sem_perder_item(
    destino: bronze.Destino,
) -> None:
    itens = [json.dumps({"conta": f"{i:09d}", "valor": i / 100}) for i in range(3_000)]
    lida = bronze.juntar_partes("raw/g.json", ["g"], [_texto(itens)])
    comandos = bronze.inserts_da_pagina(
        destino.nome,
        lida,
        ["conta", "valor"],
        limite=60_000,
    )
    assert len(comandos) > 1
    destino.acrescentar(["conta", "valor"])
    for comando in comandos:
        destino.executor.run(comando)
    resultado = destino.executor.records(
        f"SELECT count(*), count(DISTINCT _item), min(CAST(_item AS integer)), "
        f"max(CAST(_item AS integer)) FROM {destino.nome}"
    )
    assert tuple(resultado[0]) == (3_000, 3_000, 1, 3_000)
