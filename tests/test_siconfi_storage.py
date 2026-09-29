"""Bronze e fila do SICONFI contra um Postgres de verdade.

O que se testa aqui é semântica de SQL — qual busca vale, o que volta para a
fila, o que a migração faz com uma Bronze antiga — e um mock não pegaria erro
nela. Cada teste roda num banco novo, criado e apagado pela fixture.

Sem ``SICONFI_TEST_DSN`` os testes são pulados: a CI não tem Postgres. Para
rodar localmente, com um Postgres descartável::

    docker run -d --rm --name pg-siconfi-test -p 55432:5432 \\
        -e POSTGRES_PASSWORD=test postgres:15-alpine
    SICONFI_TEST_DSN="host=localhost port=55432 user=postgres password=test" \\
        pytest tests/test_siconfi_storage.py
"""

import os
import threading
import uuid
from collections.abc import Iterator
from typing import Any

import pytest

psycopg2 = pytest.importorskip("psycopg2")
from psycopg2 import sql  # noqa: E402
from psycopg2.extensions import make_dsn, parse_dsn  # noqa: E402

from cliente_siconfi import (  # noqa: E402
    PlanScope,
    SiconfiPage,
    SiconfiRetryableError,
    plan_units,
    request_hash,
)
from siconfi_config import carregar  # noqa: E402
from siconfi_storage import SiconfiStorage  # noqa: E402

_DSN = os.environ.get("SICONFI_TEST_DSN")
pytestmark = pytest.mark.skipif(
    not _DSN, reason="SICONFI_TEST_DSN não definido; estes testes precisam de Postgres"
)

_DCA = {"id_ente": 35, "an_exercicio": 2025}
_EXTRATO_DCA = {
    "exercicio": 2025,
    "cod_ibge": 35,
    "periodo": 1,
    "periodicidade": "A",
    "entregavel": "Balanço Anual (DCA)",
    "data_status": "2026-04-30T22:35:52Z",
    "status_relatorio": "HO",
    "forma_envio": "P",
}


@pytest.fixture
def conn_str() -> Iterator[str]:
    assert _DSN is not None
    name = f"siconfi_test_{uuid.uuid4().hex[:12]}"
    admin = psycopg2.connect(_DSN)
    admin.autocommit = True
    with admin.cursor() as cur:
        cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
    try:
        yield make_dsn(**{**parse_dsn(_DSN), "dbname": name})
    finally:
        with admin.cursor() as cur:
            cur.execute(
                sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(name))
            )
        admin.close()


@pytest.fixture
def storage(conn_str: str) -> SiconfiStorage:
    storage = SiconfiStorage(conn_str)
    storage.ensure_tables()
    return storage


def _query(conn_str: str, query: str, params: tuple = ()) -> list[tuple]:
    conn = psycopg2.connect(conn_str)
    try:
        with conn, conn.cursor() as cur:
            cur.execute(query, params)
            return cur.fetchall() if cur.description else []
    finally:
        conn.close()


def _pages(
    endpoint: str,
    params: dict[str, Any],
    batches: list[list[dict[str, Any]]],
    fail_at: int | None = None,
) -> Iterator[SiconfiPage]:
    """Imita ``SiconfiClient.iter_pages``; ``fail_at`` interrompe naquela página."""
    offset = 0
    for index, items in enumerate(batches):
        if index == fail_at:
            raise SiconfiRetryableError(503, "indisponível")
        yield SiconfiPage(
            endpoint=endpoint,
            params=params,
            offset=offset,
            items=items,
            has_more=index < len(batches) - 1,
            headers={},
            payload={"items": items},
        )
        offset += len(items)


def _current_items(conn_str: str, endpoint: str) -> list[dict[str, Any]]:
    """O contrato do dbt: só a busca completa mais recente de cada consulta."""
    table = SiconfiStorage._table(endpoint)
    rows = _query(
        conn_str,
        f"""
        WITH latest AS (
            SELECT DISTINCT ON (endpoint, request_hash) fetch_id
            FROM siconfi_bronze.fetches
            WHERE completed_at IS NOT NULL AND endpoint = %s
            ORDER BY endpoint, request_hash, completed_at DESC
        )
        SELECT i.payload FROM siconfi_bronze.{table} i
        JOIN latest USING (fetch_id)
        ORDER BY i.bronze_item_id
        """,
        (endpoint,),
    )
    return [row[0] for row in rows]


def _anexo(n: int) -> dict[str, Any]:
    return {"anexo": "DCA-Anexo I-E", "conta": f"conta {n}", "valor": n}


# --- Busca completa e busca interrompida --------------------------------------


def test_busca_completa_fica_marcada_e_os_itens_carregam_o_fetch_id(
    storage: SiconfiStorage, conn_str: str
) -> None:
    batches = [[_anexo(1), _anexo(2)], [_anexo(3)]]
    rows = storage.persist_fetch("dca", _DCA, _pages("dca", _DCA, batches), "run-1")

    assert rows == 3
    [(fetch_id, hash_, completed, count)] = _query(
        conn_str,
        "SELECT fetch_id, request_hash, completed_at IS NOT NULL, item_count "
        "FROM siconfi_bronze.fetches",
    )
    assert (hash_, completed, count) == (request_hash("dca", _DCA), True, 3)
    assert _query(
        conn_str,
        "SELECT DISTINCT fetch_id FROM siconfi_bronze.dca_items "
        "UNION SELECT DISTINCT fetch_id FROM siconfi_bronze.raw_pages",
    ) == [(fetch_id,)]
    assert _current_items(conn_str, "dca") == batches[0] + batches[1]


def test_busca_interrompida_nao_conta_e_a_refeita_nao_duplica(
    storage: SiconfiStorage, conn_str: str
) -> None:
    batches = [[_anexo(1), _anexo(2)], [_anexo(3)]]
    with pytest.raises(SiconfiRetryableError):
        storage.persist_fetch(
            "dca", _DCA, _pages("dca", _DCA, batches, fail_at=1), "run-1"
        )
    # A página 1 da busca interrompida está gravada, mas não vale.
    assert _query(conn_str, "SELECT count(*) FROM siconfi_bronze.dca_items") == [(2,)]
    assert _current_items(conn_str, "dca") == []

    storage.persist_fetch("dca", _DCA, _pages("dca", _DCA, batches), "run-1")

    assert _query(conn_str, "SELECT count(*) FROM siconfi_bronze.dca_items") == [(5,)]
    assert _current_items(conn_str, "dca") == batches[0] + batches[1]
    assert _query(
        conn_str,
        "SELECT completed_at IS NOT NULL, count(*) FROM siconfi_bronze.fetches "
        "GROUP BY 1 ORDER BY 1",
    ) == [(False, 1), (True, 1)]


def test_busca_completa_vazia_substitui_a_anterior(
    storage: SiconfiStorage, conn_str: str
) -> None:
    storage.persist_fetch("dca", _DCA, _pages("dca", _DCA, [[_anexo(1)]]), "run-1")
    storage.persist_fetch("dca", _DCA, _pages("dca", _DCA, [[]]), "run-2")

    assert _current_items(conn_str, "dca") == []


def test_consultas_diferentes_nao_se_substituem(
    storage: SiconfiStorage, conn_str: str
) -> None:
    outro_ano = {**_DCA, "an_exercicio": 2024}
    storage.persist_fetch("dca", _DCA, _pages("dca", _DCA, [[_anexo(1)]]), "run-1")
    storage.persist_fetch(
        "dca", outro_ano, _pages("dca", outro_ano, [[_anexo(2)]]), "run-1"
    )

    assert _current_items(conn_str, "dca") == [_anexo(1), _anexo(2)]


# --- Migração ------------------------------------------------------------------

_BRONZE_ANTERIOR = """
CREATE SCHEMA siconfi_bronze;
CREATE TABLE siconfi_bronze.raw_pages (
    raw_page_id BIGSERIAL PRIMARY KEY,
    endpoint TEXT NOT NULL,
    request_hash TEXT NOT NULL,
    request_params JSONB NOT NULL,
    page_offset INTEGER NOT NULL,
    fetched_at TIMESTAMPTZ NOT NULL,
    run_id TEXT NOT NULL,
    response_headers JSONB NOT NULL,
    payload JSONB NOT NULL,
    item_count INTEGER NOT NULL
);
CREATE TABLE siconfi_bronze.dca_items (
    bronze_item_id BIGSERIAL PRIMARY KEY,
    raw_page_id BIGINT NOT NULL REFERENCES siconfi_bronze.raw_pages(raw_page_id),
    dt_ingest TIMESTAMPTZ NOT NULL,
    run_id TEXT NOT NULL,
    request_hash TEXT NOT NULL,
    source_page_offset INTEGER NOT NULL,
    payload JSONB NOT NULL
);
INSERT INTO siconfi_bronze.raw_pages VALUES
    (1, 'dca', 'h', '{}', 0, now(), 'antigo', '{}', '{}', 1);
INSERT INTO siconfi_bronze.dca_items VALUES
    (1, 1, now(), 'antigo', 'h', 0, '{"valor": 1}');
"""


def test_migra_bronze_anterior_ao_fetch_id(conn_str: str) -> None:
    _query(conn_str, _BRONZE_ANTERIOR)
    storage = SiconfiStorage(conn_str)

    storage.ensure_tables()
    storage.ensure_tables()  # a segunda vez não tem nada a fazer

    sem_coluna = _query(
        conn_str,
        """
        SELECT t.table_name FROM information_schema.tables t
        WHERE t.table_schema = 'siconfi_bronze' AND t.table_name <> 'fetches'
          AND NOT EXISTS (
            SELECT 1 FROM information_schema.columns c
            WHERE c.table_schema = t.table_schema AND c.table_name = t.table_name
              AND c.column_name = 'fetch_id')
        """,
    )
    assert sem_coluna == []
    indexes = {
        row[0]
        for row in _query(
            conn_str,
            "SELECT indexname FROM pg_indexes WHERE schemaname = 'siconfi_bronze'",
        )
    }
    assert {
        "fetches_request_idx",
        "raw_pages_request_idx",
        "dca_items_fetch_id_idx",
        "extrato_entregas_items_fetch_id_idx",
    } <= indexes
    # A linha antiga continua lá, mas fica fora do contrato.
    assert _query(conn_str, "SELECT payload, fetch_id FROM siconfi_bronze.dca_items") == [
        ({"valor": 1}, None)
    ]
    assert _current_items(conn_str, "dca") == []


def test_ensure_tables_em_paralelo_num_banco_vazio(conn_str: str) -> None:
    errors: list[BaseException] = []

    def run() -> None:
        try:
            SiconfiStorage(conn_str).ensure_tables()
        except BaseException as exc:  # noqa: BLE001 — o teste inspeciona depois
            errors.append(exc)

    threads = [threading.Thread(target=run) for _ in range(4)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert errors == []


# --- Extrato buscado de novo e retificação -------------------------------------


def _complete_all(storage: SiconfiStorage, endpoint: str, status: str) -> None:
    for unit in storage.claim(endpoint, 1000, "teste"):
        storage.complete(endpoint, unit["work_key"], status)


def _statuses(conn_str: str, endpoint: str) -> list[str]:
    rows = _query(
        conn_str,
        "SELECT status FROM siconfi_control.work_queue WHERE endpoint = %s",
        (endpoint,),
    )
    return [row[0] for row in rows]


def _extrato(ente: int, ano: int) -> dict[str, Any]:
    return {"id_ente": ente, "an_referencia": ano}


def test_requeue_stale_devolve_so_o_extrato_recente_e_velho(
    storage: SiconfiStorage, conn_str: str
) -> None:
    endpoint = "extrato_entregas"
    storage.enqueue_many(
        endpoint, [(_extrato(35, ano), None, None) for ano in (2020, 2025, 2026)]
    )
    _complete_all(storage, endpoint, "success")
    storage.enqueue_many(endpoint, [(_extrato(33, 2026), None, None)])
    _complete_all(storage, endpoint, "permanent_error")
    # Município de uma execução anterior, com o recorte hoje só nas UFs.
    storage.enqueue_many(
        endpoint,
        [(_extrato(43, 2026), None, None), (_extrato(3550308, 2026), None, None)],
    )
    _complete_all(storage, endpoint, "success")
    # Tudo "buscado há 30 dias", menos o RS (43), buscado agora.
    _query(
        conn_str,
        "UPDATE siconfi_control.work_queue "
        "SET last_success_at = now() - interval '30 days' "
        "WHERE params->>'id_ente' <> '43'",
    )

    requeued = storage.requeue_stale(
        endpoint,
        "an_referencia",
        min_year=2025,
        max_year=2026,
        older_than_days=7,
        entity_ids=[33, 35, 43],
    )

    assert requeued == 2
    status = {
        (int(ente), int(ano)): s
        for ente, ano, s in _query(
            conn_str,
            "SELECT params->>'id_ente', params->>'an_referencia', status "
            "FROM siconfi_control.work_queue WHERE endpoint = %s",
            (endpoint,),
        )
    }
    assert status == {
        (35, 2020): "success",  # ano fora da janela
        (35, 2025): "pending",
        (35, 2026): "pending",
        (33, 2026): "permanent_error",  # repetir não muda o resultado
        (43, 2026): "success",  # buscado há pouco
        (3550308, 2026): "success",  # ente fora do recorte
    }


@pytest.mark.parametrize(
    ("status_relatorio", "expected"),
    [("HO", "success"), ("RE", "pending")],
)
def test_so_a_retificacao_poe_o_dca_de_volta_na_fila(
    storage: SiconfiStorage, conn_str: str, status_relatorio: str, expected: str
) -> None:
    scope = PlanScope(carregar(None, None, 2026))

    def enqueue(item: dict[str, Any]) -> None:
        for endpoint, params, revision, esperado in plan_units([item], "E", scope):
            storage.enqueue_many(endpoint, [(params, revision, esperado)])

    enqueue(_EXTRATO_DCA)
    _complete_all(storage, "dca", "success")

    # O extrato buscado de novo, igual ou retificado.
    enqueue({**_EXTRATO_DCA, "status_relatorio": status_relatorio})

    assert _statuses(conn_str, "dca") == [expected]


# --- Status da partição e log --------------------------------------------------------

_RREO = {
    "id_ente": 32,
    "an_exercicio": 2024,
    "nr_periodo": 6,
    "co_tipo_demonstrativo": "RREO",
}


def test_partition_log_distingue_vazio_esperado_de_inesperado(
    storage: SiconfiStorage, conn_str: str
) -> None:
    storage.enqueue_many(
        "rgf",
        [
            ({**_RREO, "co_poder": "E"}, "m", True),
            ({**_RREO, "co_poder": "L"}, "m", False),
            ({**_RREO, "co_poder": "J"}, "m", True),
        ],
    )
    for unit in storage.claim("rgf", 10, "run-1"):
        poder = unit["params"]["co_poder"]
        status = {"E": "unexpected_empty", "L": "no_data", "J": "success"}[poder]
        storage.complete(
            "rgf", unit["work_key"], status, run_id="run-1", item_count=0, page_count=1
        )

    log = {
        params["co_poder"]: (status, esperado, pages)
        for params, status, esperado, pages in _query(
            conn_str,
            "SELECT params, status, esperado, page_count "
            "FROM siconfi_control.partition_log WHERE run_id = 'run-1'",
        )
    }
    assert log == {
        "E": ("vazio_inesperado", True, 1),
        "L": ("vazio_esperado", False, 1),
        "J": ("sucesso_com_dados", True, 1),
    }
    # Vazio inesperado não volta sozinho para a fila.
    assert storage.claim("rgf", 10, "run-2") == []


def test_complete_sem_run_id_nao_registra_log(
    storage: SiconfiStorage, conn_str: str
) -> None:
    storage.enqueue_many("dca", [(_DCA, "m", True)])
    _complete_all(storage, "dca", "success")
    assert _query(conn_str, "SELECT count(*) FROM siconfi_control.partition_log") == [
        (0,)
    ]


def test_log_partition_das_referencias(storage: SiconfiStorage, conn_str: str) -> None:
    storage.log_partition("run-1", "entes", {}, "sucesso_com_dados", item_count=5598)
    with pytest.raises(ValueError):
        storage.log_partition("run-1", "entes", {}, "success")
    assert _query(
        conn_str, "SELECT endpoint, status, item_count FROM siconfi_control.partition_log"
    ) == [("entes", "sucesso_com_dados", 5598)]


def test_release_desfaz_a_tentativa_contada_no_claim(
    storage: SiconfiStorage, conn_str: str
) -> None:
    storage.enqueue_many("dca", [(_DCA, "m", True)])
    [unit] = storage.claim("dca", 10, "run-1")
    assert unit["attempts"] == 1 and unit["esperado"] is True
    storage.release("dca", [unit["work_key"]], "run-1")

    assert _query(
        conn_str, "SELECT status, attempts FROM siconfi_control.work_queue"
    ) == [("pending", 0)]


def test_requeue_status_so_devolve_o_status_e_o_recorte_pedidos(
    storage: SiconfiStorage, conn_str: str
) -> None:
    storage.enqueue_many(
        "dca",
        [({"id_ente": ente, "an_exercicio": 2024}, "m", True) for ente in (32, 33, 35)],
    )
    for unit in storage.claim("dca", 10, "run-1"):
        ente = unit["params"]["id_ente"]
        status = {32: "unexpected_empty", 33: "unexpected_empty", 35: "success"}[ente]
        storage.complete("dca", unit["work_key"], status)

    devolvidas = storage.requeue_status(
        "dca", ["vazio_inesperado"], {"id_ente": frozenset({32, 35})}
    )

    assert devolvidas == 1
    por_ente = dict(
        _query(
            conn_str,
            "SELECT (params->>'id_ente')::int, status FROM siconfi_control.work_queue",
        )
    )
    assert por_ente == {32: "pending", 33: "unexpected_empty", 35: "success"}


def test_replanejar_igual_nao_reescreve_a_fila(
    storage: SiconfiStorage, conn_str: str
) -> None:
    storage.enqueue_many("dca", [(_DCA, "m", True)])
    _complete_all(storage, "dca", "success")
    [(antes,)] = _query(conn_str, "SELECT updated_at FROM siconfi_control.work_queue")

    assert storage.enqueue_many("dca", [(_DCA, "m", True)]) == 0

    assert _query(
        conn_str, "SELECT updated_at, status FROM siconfi_control.work_queue"
    ) == [(antes, "success")]


def test_retificacao_zera_as_tentativas(storage: SiconfiStorage, conn_str: str) -> None:
    storage.enqueue_many("dca", [(_DCA, "m1", True)])
    _complete_all(storage, "dca", "permanent_error")
    storage.enqueue_many("dca", [(_DCA, "m2", True)])
    assert _query(
        conn_str, "SELECT status, attempts FROM siconfi_control.work_queue"
    ) == [("pending", 0)]


# --- Referências, extrato por ente e ano, configuração da execução ------------------


def test_latest_items_e_ente_esferas_usam_so_a_carga_mais_recente(
    storage: SiconfiStorage,
) -> None:
    antiga = [{"cod_ibge": 32, "esfera": "E"}, {"cod_ibge": 99, "esfera": "E"}]
    atual = [{"cod_ibge": 32, "esfera": "E"}, {"cod_ibge": 53, "esfera": "D"}]
    storage.persist_fetch("entes", {}, _pages("entes", {}, [antiga]), "run-1")
    storage.persist_fetch("entes", {}, _pages("entes", {}, [atual]), "run-2")
    # Uma recarga interrompida não conta.
    with pytest.raises(SiconfiRetryableError):
        storage.persist_fetch(
            "entes", {}, _pages("entes", {}, [[{"cod_ibge": 1}], []], fail_at=1), "run-3"
        )

    assert storage.latest_items("entes") == atual
    assert storage.ente_esferas() == {32: "E", 53: "D"}


def test_extrato_fetches_since_le_um_ente_e_ano_por_vez_so_a_busca_mais_recente(
    storage: SiconfiStorage,
) -> None:
    es, df = _extrato(32, 2024), _extrato(53, 2024)
    linha = {"entregavel": "Balanço Anual (DCA)", "status_relatorio": "HO"}
    storage.persist_fetch(
        "extrato_entregas", es, _pages("extrato_entregas", es, [[linha]]), "run-1"
    )
    storage.persist_fetch(
        "extrato_entregas", df, _pages("extrato_entregas", df, [[]]), "run-1"
    )
    retificado = {**linha, "status_relatorio": "RE"}
    storage.persist_fetch(
        "extrato_entregas",
        es,
        _pages("extrato_entregas", es, [[retificado], [linha]]),
        "run-2",
    )

    primeira = storage.extrato_fetches_since(None, 10)
    assert [(params, items) for _, _, params, items in primeira] == [
        (df, []),
        (es, [retificado, linha]),
    ]

    storage.save_plan_cursor("extrato_entregas", *primeira[0][:2], "escopo")
    cursor = storage.plan_cursor("extrato_entregas", "escopo")
    assert cursor == primeira[0][:2]
    assert [p for _, _, p, _ in storage.extrato_fetches_since(cursor, 10)] == [es]
    # Recorte diferente: o cursor não vale e o extrato é relido do começo.
    assert storage.plan_cursor("extrato_entregas", "outro") is None


def test_save_run_config_guarda_a_configuracao_efetiva(
    storage: SiconfiStorage, conn_str: str
) -> None:
    config = carregar(None, {"global": {"incluir_cod_ibge": [32]}}, 2026)
    storage.save_run_config("run-1", config, None, {"global": {"incluir_cod_ibge": [32]}})
    storage.save_run_config("run-1", config, None, None)  # nova tentativa da task

    [(salva, variable, conf)] = _query(
        conn_str, "SELECT config, variable, dag_run_conf FROM siconfi_control.run_config"
    )
    assert salva == config and salva["endpoints"]["dca"]["ano_fim"] == 2026
    assert (variable, conf) == (None, None)


# --- Limite de chamadas simultâneas --------------------------------------------------


@pytest.mark.parametrize(("vagas", "bloqueia"), [(1, True), (2, False)])
def test_rate_limiter_limita_chamadas_simultaneas(
    conn_str: str, vagas: int, bloqueia: bool
) -> None:
    from cliente_siconfi import SiconfiRateLimiter

    # Dois limiters com conexões próprias, como duas tasks em workers diferentes.
    primeiro = SiconfiRateLimiter(conn_str, interval_s=0, max_concurrency=vagas)
    segundo = SiconfiRateLimiter(conn_str, interval_s=0, max_concurrency=vagas)
    entrou = threading.Event()

    def outra_task() -> None:
        with segundo.slot():
            entrou.set()

    try:
        with primeiro.slot():
            thread = threading.Thread(target=outra_task)
            thread.start()
            assert entrou.wait(1.0) is not bloqueia
        assert entrou.wait(5.0)
        thread.join()
    finally:
        primeiro.close()
        segundo.close()
