"""Persistência Bronze e controle retomável da ingestão SICONFI.

Uma tabela de páginas cruas por endpoint (``raw_pages_<endpoint>``). Os itens são
expandidos em ``<endpoint>_items`` só onde ainda não há estruturação no dbt — ver
``PAGES_ONLY_ENDPOINTS``."""

from __future__ import annotations

import json
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable, Iterator, Mapping

import psycopg2
from psycopg2 import sql
from psycopg2.extensions import cursor as Cursor
from psycopg2.extras import execute_values

from cliente_siconfi import CONTROL_SCHEMA, FACT_ENDPOINTS, SiconfiPage, request_hash

BRONZE_SCHEMA = "siconfi_bronze"
# Depois disso, uma unidade em ``running`` volta a poder ser reservada. O
# orçamento de tempo das tasks (``max_run_minutes``) precisa ficar abaixo.
CLAIM_LEASE_MINUTES = 60
VALID_ENDPOINTS = frozenset(
    {"anexos-relatorios", "entes", "extrato_entregas", *FACT_ENDPOINTS}
)
# Endpoints sem tabela de itens: o banco guarda só a página crua em
# ``raw_pages_<endpoint>`` e a estruturação em colunas é feita no dbt. Uma linha
# de item em JSONB não comprime (fica abaixo do limite do TOAST) e repete os
# nomes de campo: a MSC orçamentária chegou a ~830 bytes por linha e 45 GB. As três
# MSC têm as mesmas colunas e filas grandes, e a DCA (um ente por exercício, de 2013
# em diante) também; rreo e rgf ficam com itens porque as filas deles já esvaziaram.
PAGES_ONLY_ENDPOINTS = frozenset(
    {"dca", "msc_patrimonial", "msc_orcamentaria", "msc_controle"}
)
_BRONZE = sql.Identifier(BRONZE_SCHEMA)
_CONTROL = sql.Identifier(CONTROL_SCHEMA)

_DDL = [
    sql.SQL("CREATE SCHEMA IF NOT EXISTS {bronze}"),
    sql.SQL("CREATE SCHEMA IF NOT EXISTS {control}"),
    sql.SQL("""
        CREATE TABLE IF NOT EXISTS {control}.work_queue (
            endpoint TEXT NOT NULL,
            work_key TEXT NOT NULL,
            params JSONB NOT NULL,
            revision_marker TEXT,
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
        CREATE TABLE IF NOT EXISTS {control}.plan_state (
            source TEXT PRIMARY KEY,
            last_item_id BIGINT NOT NULL,
            scope_hash TEXT NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        """),
    sql.SQL("""
        CREATE TABLE IF NOT EXISTS {control}.reference_state (
            endpoint TEXT PRIMARY KEY,
            refreshed_at TIMESTAMPTZ NOT NULL
        )
        """),
]
_PAGES_DDL = sql.SQL("""
    CREATE TABLE IF NOT EXISTS {bronze}.{table} (
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
    )
    """)
_ITEMS_DDL = sql.SQL("""
    CREATE TABLE IF NOT EXISTS {bronze}.{table} (
        bronze_item_id BIGSERIAL PRIMARY KEY,
        raw_page_id BIGINT NOT NULL REFERENCES {bronze}.{pages}(raw_page_id),
        dt_ingest TIMESTAMPTZ NOT NULL,
        run_id TEXT NOT NULL,
        request_hash TEXT NOT NULL,
        source_page_offset INTEGER NOT NULL,
        payload JSONB NOT NULL
    )
    """)
_UPSERT_WORK = sql.SQL("""
    INSERT INTO {control}.work_queue (endpoint, work_key, params, revision_marker)
    VALUES %s
    ON CONFLICT (endpoint, work_key) DO UPDATE SET
        params = EXCLUDED.params,
        status = CASE
            WHEN {control}.work_queue.revision_marker
                 IS DISTINCT FROM EXCLUDED.revision_marker
            THEN 'pending' ELSE {control}.work_queue.status END,
        revision_marker = EXCLUDED.revision_marker,
        updated_at = now()
    RETURNING (xmax = 0) AS inserted
    """).format(control=_CONTROL)


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
    def _validate(endpoint: str) -> None:
        if endpoint not in VALID_ENDPOINTS:
            raise ValueError(f"endpoint SICONFI inválido: {endpoint}")

    @classmethod
    def _pages_table(cls, endpoint: str) -> str:
        cls._validate(endpoint)
        return f"raw_pages_{endpoint.replace('-', '_')}"

    @classmethod
    def _table(cls, endpoint: str) -> str:
        cls._validate(endpoint)
        if endpoint in PAGES_ONLY_ENDPOINTS:
            raise ValueError(f"{endpoint} não tem tabela de itens; leia as páginas")
        return f"{endpoint.replace('-', '_')}_items"

    def ensure_tables(self) -> None:
        with self._cursor() as cur:
            for statement in _DDL:
                cur.execute(statement.format(bronze=_BRONZE, control=_CONTROL))
            for endpoint in sorted(VALID_ENDPOINTS):
                pages = sql.Identifier(self._pages_table(endpoint))
                cur.execute(_PAGES_DDL.format(bronze=_BRONZE, table=pages))
                if endpoint in PAGES_ONLY_ENDPOINTS:
                    continue
                cur.execute(
                    _ITEMS_DDL.format(
                        bronze=_BRONZE,
                        table=sql.Identifier(self._table(endpoint)),
                        pages=pages,
                    )
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

    def persist_page(self, page: SiconfiPage, run_id: str) -> int:
        now = datetime.now(timezone.utc)
        query_hash = request_hash(page.endpoint, page.params)
        with self._cursor() as cur:
            cur.execute(
                sql.SQL("""
                    INSERT INTO {}.{}
                      (endpoint, request_hash, request_params, page_offset, fetched_at,
                       run_id, response_headers, payload, item_count)
                    VALUES (%s, %s, %s::jsonb, %s, %s, %s, %s::jsonb, %s::jsonb, %s)
                    RETURNING raw_page_id
                    """).format(
                    _BRONZE, sql.Identifier(self._pages_table(page.endpoint))
                ),
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
                ),
            )
            row = cur.fetchone()
            assert row is not None  # RETURNING sempre devolve a linha inserida
            raw_page_id = row[0]
            if page.items and page.endpoint not in PAGES_ONLY_ENDPOINTS:
                # execute_values manda a página em poucos comandos; o
                # executemany do psycopg2 faz uma ida ao banco por item.
                execute_values(
                    cur,
                    sql.SQL(
                        "INSERT INTO {}.{} (raw_page_id, dt_ingest, run_id, "
                        "request_hash, source_page_offset, payload) VALUES %s"
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
                        )
                        for item in page.items
                    ],
                    template="(%s, %s, %s, %s, %s, %s::jsonb)",
                    page_size=1000,
                )
        return len(page.items)

    def enqueue_many(
        self,
        endpoint: str,
        units: Iterable[tuple[dict[str, Any], str | None]],
        chunk_size: int = 1000,
    ) -> int:
        """Insere/atualiza várias unidades de trabalho numa única conexão.

        O planejamento produz dezenas de milhares de unidades por execução; uma
        conexão por unidade tornava ``plan_facts`` impraticável contra um
        Postgres remoto. As chaves são deduplicadas porque ``ON CONFLICT DO
        UPDATE`` recusa afetar a mesma linha duas vezes no mesmo comando.
        """
        self._validate(endpoint)
        deduped: dict[str, tuple[str, str, str | None]] = {}
        for params, revision_marker in units:
            key = request_hash(endpoint, params)
            deduped[key] = (key, json.dumps(params, default=str), revision_marker)
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
                    template="(%s, %s, %s::jsonb, %s)",
                    page_size=chunk_size,
                    fetch=True,
                )
                inserted += sum(1 for (is_new,) in returned if is_new)
        return inserted

    def claim(
        self,
        endpoint: str,
        limit: int,
        worker_id: str,
        lease_minutes: int = CLAIM_LEASE_MINUTES,
        param_filter: Mapping[str, Iterable[Any]] | None = None,
    ) -> list[dict[str, Any]]:
        """Reserva até ``limit`` unidades cujos ``params`` caibam em ``param_filter``."""
        self._validate(endpoint)
        filters = sorted((param_filter or {}).items())
        filter_sql = sql.SQL("").join(
            sql.SQL(" AND (params->>{}) = ANY(%s)").format(sql.Literal(key))
            for key, _ in filters
        )
        filter_values = [sorted(str(v) for v in values) for _, values in filters]
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
                    RETURNING q.work_key, q.params, q.attempts
                    """).format(control=_CONTROL, filters=filter_sql),
                (endpoint, lease_minutes, *filter_values, limit, worker_id),
            )
            return [
                {"work_key": key, "params": params, "attempts": attempts}
                for key, params, attempts in cur.fetchall()
            ]

    def release(self, endpoint: str, work_keys: list[str], worker_id: str) -> None:
        """Devolve à fila unidades reservadas e não processadas.

        Sem isso, uma task que para no meio — por tempo ou por erro — deixa as
        unidades em ``running`` até o lease vencer, e a nova tentativa do
        Airflow não encontra nada para fazer.
        """
        if not work_keys:
            return
        with self._cursor() as cur:
            cur.execute(
                sql.SQL("""
                    UPDATE {}.work_queue
                    SET status = 'pending', claimed_at = NULL, claimed_by = NULL
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
    ) -> None:
        if status not in {"success", "no_data", "permanent_error", "retry"}:
            raise ValueError(f"status inválido: {status}")
        with self._cursor() as cur:
            cur.execute(
                sql.SQL("""
                    UPDATE {}.work_queue
                    SET status = %(status)s, last_error = %(error)s,
                        next_attempt_at = CASE WHEN %(status)s = 'retry'
                            THEN now() + (%(retry_delay_s)s * interval '1 second')
                            ELSE NULL END,
                        last_success_at = CASE
                            WHEN %(status)s IN ('success', 'no_data') THEN now()
                            ELSE last_success_at END,
                        updated_at = now()
                    WHERE endpoint = %(endpoint)s AND work_key = %(work_key)s
                    """).format(_CONTROL),
                {
                    "status": status,
                    "error": error[:2000] if error else None,
                    "retry_delay_s": retry_delay_s,
                    "endpoint": endpoint,
                    "work_key": work_key,
                },
            )

    def entity_ids(self) -> list[int]:
        with self._cursor() as cur:
            cur.execute(
                sql.SQL(
                    "SELECT DISTINCT (payload->>'cod_ibge')::bigint FROM {}.{} "
                    "WHERE payload ? 'cod_ibge'"
                ).format(_BRONZE, sql.Identifier(self._table("entes")))
            )
            return [int(row[0]) for row in cur.fetchall()]

    def payloads_since(
        self, endpoint: str, after_id: int, limit: int
    ) -> list[tuple[int, dict[str, Any]]]:
        """Itens de ``endpoint`` com ``bronze_item_id > after_id``, em ordem."""
        with self._cursor() as cur:
            cur.execute(
                sql.SQL(
                    "SELECT bronze_item_id, payload FROM {}.{} "
                    "WHERE bronze_item_id > %s ORDER BY bronze_item_id LIMIT %s"
                ).format(_BRONZE, sql.Identifier(self._table(endpoint))),
                (after_id, limit),
            )
            return [
                (item_id, payload)
                for item_id, payload in cur.fetchall()
                if isinstance(payload, dict)
            ]

    def plan_watermark(self, source: str, scope_hash: str) -> int:
        """Último item de ``source`` já planejado com este recorte; 0 se mudou."""
        with self._cursor() as cur:
            cur.execute(
                sql.SQL(
                    "SELECT last_item_id, scope_hash FROM {}.plan_state "
                    "WHERE source = %s"
                ).format(_CONTROL),
                (source,),
            )
            row = cur.fetchone()
        return int(row[0]) if row and row[1] == scope_hash else 0

    def save_plan_watermark(
        self, source: str, last_item_id: int, scope_hash: str
    ) -> None:
        with self._cursor() as cur:
            cur.execute(
                sql.SQL(
                    "INSERT INTO {}.plan_state (source, last_item_id, scope_hash) "
                    "VALUES (%s, %s, %s) ON CONFLICT (source) DO UPDATE SET "
                    "last_item_id = EXCLUDED.last_item_id, "
                    "scope_hash = EXCLUDED.scope_hash, updated_at = now()"
                ).format(_CONTROL),
                (source, last_item_id, scope_hash),
            )
