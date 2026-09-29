"""Persistência Bronze e controle retomável da ingestão SICONFI.

A Bronze só recebe linhas, nunca apaga: a mesma consulta aparece mais de uma
vez quando uma busca é interrompida e refeita, quando uma retificação a põe de
volta na fila e a cada recarga de ``entes``/``anexos-relatorios``. Cada busca
tem um ``fetch_id``, gravado nas páginas e nos itens, e só ganha
``completed_at`` em ``siconfi_bronze.fetches`` depois da última página. Quem lê
a Bronze (o dbt) usa só a busca completa mais recente de cada consulta::

    SELECT DISTINCT ON (endpoint, request_hash) fetch_id
    FROM siconfi_bronze.fetches
    WHERE completed_at IS NOT NULL
    ORDER BY endpoint, request_hash, completed_at DESC

e junta os itens por ``fetch_id``. Linhas com ``fetch_id`` nulo são de antes
desse controle e ficam de fora. Uma busca completa sem itens também vale: ela
substitui a versão anterior da consulta.
"""

from __future__ import annotations

import json
import uuid
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable, Iterator, Mapping

import psycopg2
from psycopg2 import sql
from psycopg2.extensions import cursor as Cursor
from psycopg2.extras import execute_values

from cliente_siconfi import (
    CONTROL_SCHEMA,
    FACT_ENDPOINTS,
    SiconfiPage,
    SiconfiPermanentError,
    SiconfiRetryableError,
    request_hash,
)

BRONZE_SCHEMA = "siconfi_bronze"
# Depois disso, uma unidade em ``running`` volta a poder ser reservada. O
# orçamento de tempo das tasks (``max_run_minutes``) precisa ficar abaixo.
CLAIM_LEASE_MINUTES = 60
VALID_ENDPOINTS = frozenset(
    {"anexos-relatorios", "entes", "extrato_entregas", *FACT_ENDPOINTS}
)
_BRONZE = sql.Identifier(BRONZE_SCHEMA)
_CONTROL = sql.Identifier(CONTROL_SCHEMA)

_DDL = [
    sql.SQL("CREATE SCHEMA IF NOT EXISTS {bronze}"),
    sql.SQL("CREATE SCHEMA IF NOT EXISTS {control}"),
    sql.SQL("""
        CREATE TABLE IF NOT EXISTS {bronze}.raw_pages (
            raw_page_id BIGSERIAL PRIMARY KEY,
            endpoint TEXT NOT NULL,
            request_hash TEXT NOT NULL,
            request_params JSONB NOT NULL,
            page_offset INTEGER NOT NULL,
            fetched_at TIMESTAMPTZ NOT NULL,
            run_id TEXT NOT NULL,
            response_headers JSONB NOT NULL,
            payload JSONB NOT NULL,
            item_count INTEGER NOT NULL,
            fetch_id TEXT
        )
        """),
    sql.SQL("""
        CREATE TABLE IF NOT EXISTS {bronze}.fetches (
            fetch_id TEXT PRIMARY KEY,
            endpoint TEXT NOT NULL,
            request_hash TEXT NOT NULL,
            request_params JSONB NOT NULL,
            run_id TEXT NOT NULL,
            started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            completed_at TIMESTAMPTZ,
            item_count INTEGER
        )
        """),
    sql.SQL("""
        CREATE TABLE IF NOT EXISTS {control}.work_queue (
            endpoint TEXT NOT NULL,
            work_key TEXT NOT NULL,
            params JSONB NOT NULL,
            revision_marker TEXT,
            esperado BOOLEAN,
            status TEXT NOT NULL DEFAULT 'pending',
            attempts INTEGER NOT NULL DEFAULT 0,
            claimed_at TIMESTAMPTZ,
            claimed_by TEXT,
            next_attempt_at TIMESTAMPTZ,
            last_error TEXT,
            last_success_at TIMESTAMPTZ,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            PRIMARY KEY (endpoint, work_key)
        )
        """),
    sql.SQL(
        "CREATE INDEX IF NOT EXISTS work_queue_claim_idx "
        "ON {control}.work_queue (endpoint, status, updated_at)"
    ),
    sql.SQL("""
        CREATE TABLE IF NOT EXISTS {control}.plan_cursor (
            source TEXT PRIMARY KEY,
            completed_at TIMESTAMPTZ NOT NULL,
            fetch_id TEXT NOT NULL,
            scope_hash TEXT NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        """),
    sql.SQL("""
        CREATE TABLE IF NOT EXISTS {control}.run_config (
            run_id TEXT PRIMARY KEY,
            config JSONB NOT NULL,
            variable JSONB,
            dag_run_conf JSONB,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        """),
    sql.SQL("""
        CREATE TABLE IF NOT EXISTS {control}.partition_log (
            partition_log_id BIGSERIAL PRIMARY KEY,
            run_id TEXT NOT NULL,
            endpoint TEXT NOT NULL,
            work_key TEXT NOT NULL,
            params JSONB NOT NULL,
            esperado BOOLEAN,
            status TEXT NOT NULL,
            item_count INTEGER,
            page_count INTEGER,
            error TEXT,
            logged_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        """),
    sql.SQL(
        "CREATE INDEX IF NOT EXISTS partition_log_run_idx "
        "ON {control}.partition_log (run_id, endpoint, status)"
    ),
    sql.SQL("""
        CREATE TABLE IF NOT EXISTS {control}.reference_state (
            endpoint TEXT PRIMARY KEY,
            refreshed_at TIMESTAMPTZ NOT NULL
        )
        """),
]
_ITEMS_DDL = sql.SQL("""
    CREATE TABLE IF NOT EXISTS {bronze}.{table} (
        bronze_item_id BIGSERIAL PRIMARY KEY,
        raw_page_id BIGINT NOT NULL REFERENCES {bronze}.raw_pages(raw_page_id),
        dt_ingest TIMESTAMPTZ NOT NULL,
        run_id TEXT NOT NULL,
        request_hash TEXT NOT NULL,
        source_page_offset INTEGER NOT NULL,
        payload JSONB NOT NULL,
        fetch_id TEXT
    )
    """)
# Serializa o ensure_tables entre tasks paralelas: sem isso, duas tasks que
# encontram o mesmo índice faltando criam os dois ao mesmo tempo, e a segunda
# falha com violação de unicidade no catálogo.
_MIGRATION_LOCK = "siconfi_storage.ensure_tables"
_UPSERT_WORK = sql.SQL("""
    INSERT INTO {control}.work_queue
        (endpoint, work_key, params, revision_marker, esperado)
    VALUES %s
    ON CONFLICT (endpoint, work_key) DO UPDATE SET
        params = EXCLUDED.params,
        status = CASE
            WHEN {control}.work_queue.revision_marker
                 IS DISTINCT FROM EXCLUDED.revision_marker
            THEN 'pending' ELSE {control}.work_queue.status END,
        attempts = CASE
            WHEN {control}.work_queue.revision_marker
                 IS DISTINCT FROM EXCLUDED.revision_marker
            THEN 0 ELSE {control}.work_queue.attempts END,
        revision_marker = EXCLUDED.revision_marker,
        esperado = EXCLUDED.esperado,
        updated_at = now()
    -- O planejamento reenvia a fila inteira a cada execução; sem este filtro,
    -- dezenas de milhares de linhas iguais seriam reescritas toda hora.
    WHERE {control}.work_queue.revision_marker IS DISTINCT FROM EXCLUDED.revision_marker
       OR {control}.work_queue.esperado IS DISTINCT FROM EXCLUDED.esperado
    RETURNING (xmax = 0) AS inserted
    """).format(control=_CONTROL)
# Status da fila → status com que a partição aparece no ``partition_log``.
QUEUE_STATUS = {
    "success": "sucesso_com_dados",
    "no_data": "vazio_esperado",
    "unexpected_empty": "vazio_inesperado",
    "permanent_error": "erro",
    "retry": "erro",
}


def classify_outcome(
    rows: int,
    esperado: bool | None,
    exc: BaseException | None = None,
    attempts: int = 1,
    max_attempts: int = 5,
) -> str:
    """Status da fila para o resultado de uma busca.

    A API responde HTTP 200 com lista vazia a parâmetro errado, então vazio só
    é normal quando o extrato não mostra a entrega (``esperado`` falso ou
    nulo). Vazio numa partição que o extrato mostra como entregue é
    ``unexpected_empty``: é ali que aparece bug de parâmetro, e ele não volta
    sozinho para a fila.
    """
    empty = "unexpected_empty" if esperado else "no_data"
    if exc is None:
        return "success" if rows else empty
    if isinstance(exc, SiconfiRetryableError):
        return "retry" if attempts < max_attempts else "permanent_error"
    if isinstance(exc, SiconfiPermanentError) and exc.status_code == 404:
        return empty
    return "permanent_error"


class SiconfiStorage:
    def __init__(self, conn_str: str) -> None:
        self.conn_str = conn_str

    @contextmanager
    def _cursor(self) -> Iterator[Cursor]:
        """Cursor numa conexão própria: commit ao sair sem erro, e sempre fecha.

        ``with psycopg2.connect()`` sozinho só encerra a transação — não fecha
        a conexão.
        """
        conn = psycopg2.connect(self.conn_str)
        try:
            with conn, conn.cursor() as cur:
                yield cur
        finally:
            conn.close()

    @staticmethod
    def _table(endpoint: str) -> str:
        if endpoint not in VALID_ENDPOINTS:
            raise ValueError(f"endpoint SICONFI inválido: {endpoint}")
        return f"{endpoint.replace('-', '_')}_items"

    def ensure_tables(self) -> None:
        with self._cursor() as cur:
            cur.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (_MIGRATION_LOCK,))
            for statement in _DDL:
                cur.execute(statement.format(bronze=_BRONZE, control=_CONTROL))
            for endpoint in sorted(VALID_ENDPOINTS):
                cur.execute(
                    _ITEMS_DDL.format(
                        bronze=_BRONZE, table=sql.Identifier(self._table(endpoint))
                    )
                )
            self._migrate(cur)

    def _migrate(self, cur: Cursor) -> None:
        """Leva uma Bronze anterior ao ``fetch_id`` ao formato atual.

        ``ALTER TABLE`` e ``CREATE INDEX`` pegam lock na tabela mesmo com ``IF
        NOT EXISTS``, e esta função roda no começo de toda task — várias em
        paralelo, gravando. Por isso o catálogo é consultado antes, e só o que
        falta é executado: depois da primeira vez, nada aqui trava a tabela.
        """
        cur.execute(
            "SELECT table_name FROM information_schema.columns "
            "WHERE table_schema = %s AND column_name = 'fetch_id'",
            (BRONZE_SCHEMA,),
        )
        with_fetch_id = {row[0] for row in cur.fetchall()}
        cur.execute(
            "SELECT indexname FROM pg_indexes WHERE schemaname = %s", (BRONZE_SCHEMA,)
        )
        indexes = {row[0] for row in cur.fetchall()}

        tables = ["raw_pages", *(self._table(e) for e in sorted(VALID_ENDPOINTS))]
        for table in tables:
            if table not in with_fetch_id:
                cur.execute(
                    sql.SQL(
                        "ALTER TABLE {}.{} ADD COLUMN IF NOT EXISTS fetch_id TEXT"
                    ).format(_BRONZE, sql.Identifier(table))
                )
        wanted = {
            "fetches_request_idx": (
                "fetches",
                sql.SQL("(endpoint, request_hash, completed_at DESC)"),
            ),
            "raw_pages_request_idx": (
                "raw_pages",
                sql.SQL("(endpoint, request_hash)"),
            ),
            **{
                f"{table}_fetch_id_idx": (table, sql.SQL("(fetch_id)"))
                for table in tables[1:]
            },
        }
        for name, (table, columns) in wanted.items():
            if name not in indexes:
                cur.execute(
                    sql.SQL("CREATE INDEX IF NOT EXISTS {} ON {}.{} {}").format(
                        sql.Identifier(name),
                        _BRONZE,
                        sql.Identifier(table),
                        columns,
                    )
                )
        # Fila anterior à distinção entre vazio esperado e inesperado.
        cur.execute(
            "SELECT 1 FROM information_schema.columns WHERE table_schema = %s "
            "AND table_name = 'work_queue' AND column_name = 'esperado'",
            (CONTROL_SCHEMA,),
        )
        if cur.fetchone() is None:
            cur.execute(
                sql.SQL(
                    "ALTER TABLE {}.work_queue ADD COLUMN IF NOT EXISTS esperado BOOLEAN"
                ).format(_CONTROL)
            )

    def reference_due(self, endpoint: str, refresh_hours: int) -> bool:
        with self._cursor() as cur:
            cur.execute(
                sql.SQL(
                    "SELECT refreshed_at FROM {}.reference_state WHERE endpoint = %s"
                ).format(_CONTROL),
                (endpoint,),
            )
            row = cur.fetchone()
        return row is None or row[0] <= datetime.now(timezone.utc) - timedelta(
            hours=refresh_hours
        )

    def mark_reference_refreshed(self, endpoint: str) -> None:
        with self._cursor() as cur:
            cur.execute(
                sql.SQL(
                    "INSERT INTO {}.reference_state (endpoint, refreshed_at) "
                    "VALUES (%s, now()) ON CONFLICT (endpoint) DO UPDATE "
                    "SET refreshed_at = EXCLUDED.refreshed_at"
                ).format(_CONTROL),
                (endpoint,),
            )

    def persist_fetch(
        self,
        endpoint: str,
        params: Mapping[str, Any],
        pages: Iterable[SiconfiPage],
        run_id: str,
    ) -> int:
        """Grava uma busca inteira e só a marca completa depois da última página.

        Cada página continua numa transação própria, para uma busca longa não
        segurar uma transação aberta. Se ``pages`` levantar exceção no meio, a
        busca fica sem ``completed_at`` e quem lê a Bronze a ignora.
        """
        # Mesma normalização do SiconfiClient.iter_pages, para o hash casar com
        # o das páginas.
        request_params = {k: v for k, v in params.items() if v is not None}
        fetch_id = uuid.uuid4().hex
        with self._cursor() as cur:
            cur.execute(
                sql.SQL(
                    "INSERT INTO {}.fetches "
                    "(fetch_id, endpoint, request_hash, request_params, run_id) "
                    "VALUES (%s, %s, %s, %s::jsonb, %s)"
                ).format(_BRONZE),
                (
                    fetch_id,
                    endpoint,
                    request_hash(endpoint, request_params),
                    json.dumps(request_params, default=str),
                    run_id,
                ),
            )
        rows = sum(self.persist_page(page, run_id, fetch_id) for page in pages)
        with self._cursor() as cur:
            cur.execute(
                sql.SQL(
                    "UPDATE {}.fetches SET completed_at = now(), item_count = %s "
                    "WHERE fetch_id = %s"
                ).format(_BRONZE),
                (rows, fetch_id),
            )
        return rows

    def persist_page(self, page: SiconfiPage, run_id: str, fetch_id: str) -> int:
        now = datetime.now(timezone.utc)
        query_hash = request_hash(page.endpoint, page.params)
        with self._cursor() as cur:
            cur.execute(
                sql.SQL("""
                    INSERT INTO {}.raw_pages
                      (endpoint, request_hash, request_params, page_offset, fetched_at,
                       run_id, response_headers, payload, item_count, fetch_id)
                    VALUES (%s, %s, %s::jsonb, %s, %s, %s, %s::jsonb, %s::jsonb, %s, %s)
                    RETURNING raw_page_id
                    """).format(_BRONZE),
                (
                    page.endpoint,
                    query_hash,
                    json.dumps(page.params, default=str),
                    page.offset,
                    now,
                    run_id,
                    json.dumps(page.headers),
                    json.dumps(page.payload, default=str),
                    len(page.items),
                    fetch_id,
                ),
            )
            row = cur.fetchone()
            assert row is not None  # RETURNING sempre devolve a linha inserida
            raw_page_id = row[0]
            if page.items:
                # execute_values manda a página em poucos comandos; o
                # executemany do psycopg2 faz uma ida ao banco por item.
                execute_values(
                    cur,
                    sql.SQL(
                        "INSERT INTO {}.{} (raw_page_id, dt_ingest, run_id, "
                        "request_hash, source_page_offset, payload, fetch_id) "
                        "VALUES %s"
                    )
                    .format(_BRONZE, sql.Identifier(self._table(page.endpoint)))
                    .as_string(cur),
                    [
                        (
                            raw_page_id,
                            now,
                            run_id,
                            query_hash,
                            page.offset,
                            json.dumps(item, default=str),
                            fetch_id,
                        )
                        for item in page.items
                    ],
                    template="(%s, %s, %s, %s, %s, %s::jsonb, %s)",
                    page_size=1000,
                )
        return len(page.items)

    def enqueue_many(
        self,
        endpoint: str,
        units: Iterable[tuple[dict[str, Any], str | None, bool | None]],
        chunk_size: int = 1000,
    ) -> int:
        """Insere/atualiza várias unidades ``(params, revision_marker, esperado)``.

        O planejamento produz dezenas de milhares de unidades por execução; uma
        conexão por unidade tornava o planejamento impraticável contra um
        Postgres remoto. As chaves são deduplicadas porque ``ON CONFLICT DO
        UPDATE`` recusa afetar a mesma linha duas vezes no mesmo comando.

        ``esperado`` diz se o extrato mostra a entrega: é o que separa, depois,
        o vazio esperado do inesperado. ``None`` para o próprio extrato.
        """
        self._table(endpoint)
        deduped: dict[str, tuple[str, str, str | None, bool | None]] = {}
        for params, revision_marker, esperado in units:
            key = request_hash(endpoint, params)
            deduped[key] = (
                key,
                json.dumps(params, default=str),
                revision_marker,
                esperado,
            )
        if not deduped:
            return 0

        rows = [(endpoint, *unit) for unit in deduped.values()]
        inserted = 0
        with self._cursor() as cur:
            query = _UPSERT_WORK.as_string(cur)
            for start in range(0, len(rows), chunk_size):
                returned = execute_values(
                    cur,
                    query,
                    rows[start : start + chunk_size],
                    template="(%s, %s, %s::jsonb, %s, %s)",
                    page_size=chunk_size,
                    fetch=True,
                )
                inserted += sum(1 for (is_new,) in returned if is_new)
        return inserted

    @staticmethod
    def _param_filter_sql(
        param_filter: Mapping[str, Iterable[Any]] | None,
    ) -> tuple[sql.Composable, list[list[str]]]:
        filters = sorted((param_filter or {}).items())
        filter_sql = sql.SQL("").join(
            sql.SQL(" AND (params->>{}) = ANY(%s)").format(sql.Literal(key))
            for key, _ in filters
        )
        return filter_sql, [sorted(str(v) for v in values) for _, values in filters]

    def claim(
        self,
        endpoint: str,
        limit: int,
        worker_id: str,
        lease_minutes: int = CLAIM_LEASE_MINUTES,
        param_filter: Mapping[str, Iterable[Any]] | None = None,
    ) -> list[dict[str, Any]]:
        """Reserva até ``limit`` unidades cujos ``params`` caibam em ``param_filter``."""
        self._table(endpoint)
        filter_sql, filter_values = self._param_filter_sql(param_filter)
        with self._cursor() as cur:
            cur.execute(
                sql.SQL("""
                    WITH candidates AS (
                        SELECT endpoint, work_key
                        FROM {control}.work_queue
                        WHERE endpoint = %s AND (
                            status IN ('pending', 'retry')
                            OR (status = 'running'
                                AND claimed_at < now() - (%s * interval '1 minute'))
                        )
                        AND (next_attempt_at IS NULL OR next_attempt_at <= now())
                        {filters}
                        ORDER BY updated_at, work_key
                        FOR UPDATE SKIP LOCKED
                        LIMIT %s
                    )
                    UPDATE {control}.work_queue q
                    SET status = 'running', attempts = q.attempts + 1,
                        claimed_at = now(), claimed_by = %s, updated_at = now()
                    FROM candidates c
                    WHERE q.endpoint = c.endpoint AND q.work_key = c.work_key
                    RETURNING q.work_key, q.params, q.attempts, q.esperado
                    """).format(control=_CONTROL, filters=filter_sql),
                (endpoint, lease_minutes, *filter_values, limit, worker_id),
            )
            return [
                {
                    "work_key": key,
                    "params": params,
                    "attempts": attempts,
                    "esperado": esperado,
                }
                for key, params, attempts, esperado in cur.fetchall()
            ]

    def release(self, endpoint: str, work_keys: list[str], worker_id: str) -> None:
        """Devolve à fila unidades reservadas e não processadas.

        Sem isso, uma task que para no meio — por tempo ou por erro — deixa as
        unidades em ``running`` até o lease vencer, e a nova tentativa do
        Airflow não encontra nada para fazer. A tentativa contada no claim é
        desfeita: a unidade não chegou a ser tentada.
        """
        if not work_keys:
            return
        with self._cursor() as cur:
            cur.execute(
                sql.SQL("""
                    UPDATE {}.work_queue
                    SET status = 'pending', claimed_at = NULL, claimed_by = NULL,
                        attempts = GREATEST(attempts - 1, 0)
                    WHERE endpoint = %s AND work_key = ANY(%s)
                      AND status = 'running' AND claimed_by = %s
                    """).format(_CONTROL),
                (endpoint, work_keys, worker_id),
            )

    def complete(
        self,
        endpoint: str,
        work_key: str,
        status: str,
        error: str | None = None,
        retry_delay_s: int = 300,
        *,
        run_id: str | None = None,
        item_count: int | None = None,
        page_count: int | None = None,
    ) -> None:
        """Fecha a unidade e, com ``run_id``, registra a partição no ``partition_log``."""
        if status not in QUEUE_STATUS:
            raise ValueError(f"status inválido: {status}")
        values = {
            "status": status,
            "error": error[:2000] if error else None,
            "retry_delay_s": retry_delay_s,
            "endpoint": endpoint,
            "work_key": work_key,
            "run_id": run_id,
            "log_status": QUEUE_STATUS[status],
            "item_count": item_count,
            "page_count": page_count,
        }
        update = sql.SQL("""
            UPDATE {control}.work_queue
            SET status = %(status)s, last_error = %(error)s,
                next_attempt_at = CASE WHEN %(status)s = 'retry'
                    THEN now() + (%(retry_delay_s)s * interval '1 second')
                    ELSE NULL END,
                last_success_at = CASE
                    WHEN %(status)s IN ('success', 'no_data', 'unexpected_empty')
                    THEN now() ELSE last_success_at END,
                updated_at = now()
            WHERE endpoint = %(endpoint)s AND work_key = %(work_key)s
            RETURNING endpoint, work_key, params, esperado
            """).format(control=_CONTROL)
        query = update if run_id is None else sql.SQL("""
                WITH updated AS ({update})
                INSERT INTO {control}.partition_log
                    (run_id, endpoint, work_key, params, esperado, status,
                     item_count, page_count, error)
                SELECT %(run_id)s, endpoint, work_key, params, esperado,
                       %(log_status)s, %(item_count)s, %(page_count)s, %(error)s
                FROM updated
                """).format(update=update, control=_CONTROL)
        with self._cursor() as cur:
            cur.execute(query, values)

    def log_partition(
        self,
        run_id: str,
        endpoint: str,
        params: Mapping[str, Any],
        status: str,
        *,
        esperado: bool | None = None,
        item_count: int | None = None,
        page_count: int | None = None,
        error: str | None = None,
    ) -> None:
        """Registra uma partição que não passa pela fila (``entes``, anexos)."""
        if status not in QUEUE_STATUS.values():
            raise ValueError(f"status de partição inválido: {status}")
        with self._cursor() as cur:
            cur.execute(
                sql.SQL(
                    "INSERT INTO {}.partition_log (run_id, endpoint, work_key, params, "
                    "esperado, status, item_count, page_count, error) "
                    "VALUES (%s, %s, %s, %s::jsonb, %s, %s, %s, %s, %s)"
                ).format(_CONTROL),
                (
                    run_id,
                    endpoint,
                    request_hash(endpoint, params),
                    json.dumps(dict(params), default=str),
                    esperado,
                    status,
                    item_count,
                    page_count,
                    error[:2000] if error else None,
                ),
            )

    def requeue_stale(
        self,
        endpoint: str,
        year_key: str,
        min_year: int,
        max_year: int,
        older_than_days: int,
        entity_ids: Iterable[int] | None,
    ) -> int:
        """Devolve à fila as unidades concluídas há mais de ``older_than_days``.

        Serve ao extrato: uma unidade concluída nunca mais seria buscada, e uma
        retificação posterior do ente não chegaria. Só a janela de anos
        ``[min_year, max_year]`` dos ``entity_ids`` do recorte volta (``None``
        = todos os entes) — é onde entrega e retificação ainda acontecem.
        ``permanent_error`` fica como está: repetir não muda nada.
        """
        self._table(endpoint)
        entes = None if entity_ids is None else [int(e) for e in entity_ids]
        with self._cursor() as cur:
            cur.execute(
                sql.SQL("""
                    UPDATE {}.work_queue
                    SET status = 'pending', attempts = 0, updated_at = now()
                    WHERE endpoint = %s
                      AND status IN ('success', 'no_data', 'unexpected_empty')
                      AND (params->>%s)::int BETWEEN %s AND %s
                      AND (%s::bigint[] IS NULL
                           OR (params->>'id_ente')::bigint = ANY(%s::bigint[]))
                      AND last_success_at < now() - (%s * interval '1 day')
                    """).format(_CONTROL),
                (endpoint, year_key, min_year, max_year, entes, entes, older_than_days),
            )
            return cur.rowcount

    def requeue_status(
        self,
        endpoint: str,
        statuses: Iterable[str],
        param_filter: Mapping[str, Iterable[Any]] | None = None,
    ) -> int:
        """Reprocessamento pontual: devolve à fila as partições nesses status.

        ``statuses`` são os do ``partition_log`` (``vazio_inesperado``,
        ``erro``...). Só o que cabe em ``param_filter`` — o recorte da execução
        — volta.
        """
        self._table(endpoint)
        queue_statuses = sorted(
            q for q, log in QUEUE_STATUS.items() if log in set(statuses)
        )
        if not queue_statuses:
            return 0
        filter_sql, filter_values = self._param_filter_sql(param_filter)
        with self._cursor() as cur:
            cur.execute(
                sql.SQL("""
                    UPDATE {control}.work_queue
                    SET status = 'pending', attempts = 0, next_attempt_at = NULL,
                        updated_at = now()
                    WHERE endpoint = %s AND status = ANY(%s) {filters}
                    """).format(control=_CONTROL, filters=filter_sql),
                (endpoint, queue_statuses, *filter_values),
            )
            return cur.rowcount

    def latest_items(self, endpoint: str) -> list[dict[str, Any]]:
        """Itens da busca completa mais recente de ``endpoint`` sem parâmetros."""
        with self._cursor() as cur:
            cur.execute(
                sql.SQL("""
                    SELECT i.payload FROM {bronze}.{table} i
                    WHERE i.fetch_id = (
                        SELECT fetch_id FROM {bronze}.fetches
                        WHERE endpoint = %s AND completed_at IS NOT NULL
                        ORDER BY completed_at DESC LIMIT 1)
                    ORDER BY i.bronze_item_id
                    """).format(
                    bronze=_BRONZE, table=sql.Identifier(self._table(endpoint))
                ),
                (endpoint,),
            )
            return [row[0] for row in cur.fetchall() if isinstance(row[0], dict)]

    def ente_esferas(self) -> dict[int, str]:
        """``cod_ibge → esfera`` da carga mais recente de ``/entes``."""
        return {
            int(item["cod_ibge"]): str(item.get("esfera", "")).upper()
            for item in self.latest_items("entes")
            if item.get("cod_ibge") is not None
        }

    def extrato_fetches_since(
        self, cursor: tuple[datetime, str] | None, limit: int
    ) -> list[tuple[datetime, str, dict[str, Any], list[dict[str, Any]]]]:
        """Extratos (um por ente e ano) completados depois de ``cursor``.

        Devolve ``(completed_at, fetch_id, request_params, itens)`` em ordem.
        Só a busca mais recente de cada consulta entra: reler uma busca antiga
        depois da nova trocaria o ``revision_marker`` duas vezes e rebuscaria
        o demonstrativo sem ele ter mudado.
        """
        after_ts, after_id = cursor or (datetime.min.replace(tzinfo=timezone.utc), "")
        with self._cursor() as cur:
            cur.execute(
                sql.SQL("""
                    WITH latest AS (
                        SELECT DISTINCT ON (request_hash)
                               fetch_id, completed_at, request_params
                        FROM {bronze}.fetches
                        WHERE endpoint = 'extrato_entregas'
                          AND completed_at IS NOT NULL
                        ORDER BY request_hash, completed_at DESC, fetch_id DESC
                    ), pending AS (
                        SELECT * FROM latest
                        WHERE (completed_at, fetch_id) > (%s, %s)
                        ORDER BY completed_at, fetch_id
                        LIMIT %s
                    )
                    SELECT p.completed_at, p.fetch_id, p.request_params,
                           COALESCE(
                               jsonb_agg(i.payload ORDER BY i.bronze_item_id)
                                   FILTER (WHERE i.bronze_item_id IS NOT NULL),
                               '[]'::jsonb)
                    FROM pending p
                    LEFT JOIN {bronze}.extrato_entregas_items i
                           ON i.fetch_id = p.fetch_id
                    GROUP BY p.completed_at, p.fetch_id, p.request_params
                    ORDER BY p.completed_at, p.fetch_id
                    """).format(bronze=_BRONZE),
                (after_ts, after_id, limit),
            )
            return [
                (
                    completed_at,
                    fetch_id,
                    params,
                    [i for i in items if isinstance(i, dict)],
                )
                for completed_at, fetch_id, params, items in cur.fetchall()
            ]

    def plan_cursor(self, source: str, scope_hash: str) -> tuple[datetime, str] | None:
        """Última busca de ``source`` já planejada com este recorte; ``None`` se mudou."""
        with self._cursor() as cur:
            cur.execute(
                sql.SQL(
                    "SELECT completed_at, fetch_id, scope_hash FROM {}.plan_cursor "
                    "WHERE source = %s"
                ).format(_CONTROL),
                (source,),
            )
            row = cur.fetchone()
        return (row[0], row[1]) if row and row[2] == scope_hash else None

    def save_plan_cursor(
        self, source: str, completed_at: datetime, fetch_id: str, scope_hash: str
    ) -> None:
        with self._cursor() as cur:
            cur.execute(
                sql.SQL(
                    "INSERT INTO {}.plan_cursor (source, completed_at, fetch_id, "
                    "scope_hash) VALUES (%s, %s, %s, %s) ON CONFLICT (source) DO UPDATE "
                    "SET completed_at = EXCLUDED.completed_at, "
                    "fetch_id = EXCLUDED.fetch_id, scope_hash = EXCLUDED.scope_hash, "
                    "updated_at = now()"
                ).format(_CONTROL),
                (source, completed_at, fetch_id, scope_hash),
            )

    def save_run_config(
        self,
        run_id: str,
        config: Mapping[str, Any],
        variable: Any = None,
        dag_run_conf: Any = None,
    ) -> None:
        """Guarda a configuração efetiva da execução, junto do que a produziu."""
        with self._cursor() as cur:
            cur.execute(
                sql.SQL(
                    "INSERT INTO {}.run_config (run_id, config, variable, dag_run_conf) "
                    "VALUES (%s, %s::jsonb, %s::jsonb, %s::jsonb) "
                    "ON CONFLICT (run_id) DO UPDATE SET config = EXCLUDED.config, "
                    "variable = EXCLUDED.variable, "
                    "dag_run_conf = EXCLUDED.dag_run_conf, created_at = now()"
                ).format(_CONTROL),
                (
                    run_id,
                    json.dumps(dict(config), default=str),
                    None if variable is None else json.dumps(variable, default=str),
                    (
                        None
                        if dag_run_conf is None
                        else json.dumps(dag_run_conf, default=str)
                    ),
                ),
            )
