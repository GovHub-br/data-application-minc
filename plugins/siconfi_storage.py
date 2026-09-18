"""Persistência Bronze e controle retomável da ingestão SICONFI."""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable

import psycopg2
from psycopg2 import sql

from cliente_siconfi import SiconfiPage, request_hash

BRONZE_SCHEMA = "siconfi_bronze"
CONTROL_SCHEMA = "siconfi_control"
VALID_ENDPOINTS = {
    "anexos-relatorios", "entes", "extrato_entregas", "rreo", "rgf", "dca",
    "msc_patrimonial", "msc_orcamentaria", "msc_controle",
}


class SiconfiStorage:
    def __init__(self, conn_str: str) -> None:
        self.conn_str = conn_str

    @staticmethod
    def _table(endpoint: str) -> str:
        if endpoint not in VALID_ENDPOINTS:
            raise ValueError(f"endpoint SICONFI inválido: {endpoint}")
        return f"{endpoint.replace('-', '_')}_items"

    def ensure_tables(self) -> None:
        with psycopg2.connect(self.conn_str) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(
                        sql.Identifier(BRONZE_SCHEMA)
                    )
                )
                cur.execute(
                    sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(
                        sql.Identifier(CONTROL_SCHEMA)
                    )
                )
                cur.execute(
                    sql.SQL(
                        """
                        CREATE TABLE IF NOT EXISTS {}.raw_pages (
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
                        """
                    ).format(sql.Identifier(BRONZE_SCHEMA))
                )
                cur.execute(
                    sql.SQL(
                        """
                        CREATE TABLE IF NOT EXISTS {}.work_queue (
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
                        """
                    ).format(sql.Identifier(CONTROL_SCHEMA))
                )
                cur.execute(
                    sql.SQL(
                        """
                        CREATE TABLE IF NOT EXISTS {}.reference_state (
                            endpoint TEXT PRIMARY KEY,
                            refreshed_at TIMESTAMPTZ NOT NULL
                        )
                        """
                    ).format(sql.Identifier(CONTROL_SCHEMA))
                )
                for endpoint in VALID_ENDPOINTS:
                    cur.execute(
                        sql.SQL(
                            """
                            CREATE TABLE IF NOT EXISTS {}.{} (
                                bronze_item_id BIGSERIAL PRIMARY KEY,
                                raw_page_id BIGINT NOT NULL REFERENCES {}.raw_pages(raw_page_id),
                                dt_ingest TIMESTAMPTZ NOT NULL,
                                run_id TEXT NOT NULL,
                                request_hash TEXT NOT NULL,
                                source_page_offset INTEGER NOT NULL,
                                payload JSONB NOT NULL
                            )
                            """
                        ).format(
                            sql.Identifier(BRONZE_SCHEMA),
                            sql.Identifier(self._table(endpoint)),
                            sql.Identifier(BRONZE_SCHEMA),
                        )
                    )

    def reference_due(self, endpoint: str, refresh_hours: int) -> bool:
        with psycopg2.connect(self.conn_str) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL(
                        "SELECT refreshed_at FROM {}.reference_state WHERE endpoint = %s"
                    ).format(sql.Identifier(CONTROL_SCHEMA)),
                    (endpoint,),
                )
                row = cur.fetchone()
        return row is None or row[0] <= datetime.now(timezone.utc) - timedelta(
            hours=refresh_hours
        )

    def mark_reference_refreshed(self, endpoint: str) -> None:
        with psycopg2.connect(self.conn_str) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL(
                        "INSERT INTO {}.reference_state (endpoint, refreshed_at) "
                        "VALUES (%s, now()) ON CONFLICT (endpoint) DO UPDATE "
                        "SET refreshed_at = EXCLUDED.refreshed_at"
                    ).format(sql.Identifier(CONTROL_SCHEMA)),
                    (endpoint,),
                )

    def persist_page(self, page: SiconfiPage, run_id: str) -> int:
        now = datetime.now(timezone.utc)
        query_hash = request_hash(page.endpoint, page.params)
        with psycopg2.connect(self.conn_str) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL(
                        """
                        INSERT INTO {}.raw_pages
                          (endpoint, request_hash, request_params, page_offset, fetched_at,
                           run_id, response_headers, payload, item_count)
                        VALUES (%s, %s, %s::jsonb, %s, %s, %s, %s::jsonb, %s::jsonb, %s)
                        RETURNING raw_page_id
                        """
                    ).format(sql.Identifier(BRONZE_SCHEMA)),
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
                raw_page_id = cur.fetchone()[0]
                if page.items:
                    cur.executemany(
                        sql.SQL(
                            "INSERT INTO {}.{} "
                            "(raw_page_id, dt_ingest, run_id, request_hash, "
                            "source_page_offset, payload) "
                            "VALUES (%s, %s, %s, %s, %s, %s::jsonb)"
                        ).format(
                            sql.Identifier(BRONZE_SCHEMA),
                            sql.Identifier(self._table(page.endpoint)),
                        ).as_string(cur),
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
                    )
        return len(page.items)

    def enqueue(
        self,
        endpoint: str,
        params: dict[str, Any],
        revision_marker: str | None = None,
    ) -> bool:
        self._table(endpoint)
        key = request_hash(endpoint, params)
        with psycopg2.connect(self.conn_str) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL(
                        """
                        INSERT INTO {}.work_queue (endpoint, work_key, params, revision_marker)
                        VALUES (%s, %s, %s::jsonb, %s)
                        ON CONFLICT (endpoint, work_key) DO UPDATE SET
                            params = EXCLUDED.params,
                            status = CASE
                                WHEN {}.work_queue.revision_marker IS DISTINCT FROM EXCLUDED.revision_marker
                                THEN 'pending' ELSE {}.work_queue.status END,
                            revision_marker = EXCLUDED.revision_marker,
                            updated_at = now()
                        RETURNING (xmax = 0) AS inserted
                        """
                    ).format(
                        sql.Identifier(CONTROL_SCHEMA),
                        sql.Identifier(CONTROL_SCHEMA),
                        sql.Identifier(CONTROL_SCHEMA),
                    ),
                    (endpoint, key, json.dumps(params, default=str), revision_marker),
                )
                return bool(cur.fetchone()[0])

    def claim(
        self,
        endpoint: str,
        limit: int,
        worker_id: str,
        lease_minutes: int = 60,
    ) -> list[dict[str, Any]]:
        self._table(endpoint)
        with psycopg2.connect(self.conn_str) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL(
                        """
                        WITH candidates AS (
                            SELECT endpoint, work_key
                            FROM {}.work_queue
                            WHERE endpoint = %s AND (
                                status IN ('pending', 'retry')
                                OR (status = 'running' AND claimed_at < now() - (%s * interval '1 minute'))
                            )
                            AND (next_attempt_at IS NULL OR next_attempt_at <= now())
                            ORDER BY updated_at, work_key
                            FOR UPDATE SKIP LOCKED
                            LIMIT %s
                        )
                        UPDATE {}.work_queue q
                        SET status = 'running', attempts = q.attempts + 1,
                            claimed_at = now(), claimed_by = %s, updated_at = now()
                        FROM candidates c
                        WHERE q.endpoint = c.endpoint AND q.work_key = c.work_key
                        RETURNING q.work_key, q.params, q.attempts
                        """
                    ).format(sql.Identifier(CONTROL_SCHEMA), sql.Identifier(CONTROL_SCHEMA)),
                    (endpoint, lease_minutes, limit, worker_id),
                )
                return [
                    {"work_key": key, "params": params, "attempts": attempts}
                    for key, params, attempts in cur.fetchall()
                ]

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
        with psycopg2.connect(self.conn_str) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL(
                        """
                        UPDATE {}.work_queue
                        SET status = %s, last_error = %s,
                            next_attempt_at = CASE WHEN %s = 'retry'
                                THEN now() + (%s * interval '1 second') ELSE NULL END,
                            last_success_at = CASE WHEN %s IN ('success', 'no_data') THEN now()
                                ELSE last_success_at END,
                            updated_at = now()
                        WHERE endpoint = %s AND work_key = %s
                        """
                    ).format(sql.Identifier(CONTROL_SCHEMA)),
                    (
                        status,
                        error[:2000] if error else None,
                        status,
                        retry_delay_s,
                        status,
                        endpoint,
                        work_key,
                    ),
                )

    def entity_ids(self) -> list[int]:
        table = self._table("entes")
        with psycopg2.connect(self.conn_str) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL(
                        "SELECT DISTINCT (payload->>'cod_ibge')::bigint FROM {}.{} "
                        "WHERE payload ? 'cod_ibge'"
                    ).format(sql.Identifier(BRONZE_SCHEMA), sql.Identifier(table))
                )
                return [int(row[0]) for row in cur.fetchall()]

    def payloads(self, endpoint: str, max_rows: int = 100000) -> Iterable[dict[str, Any]]:
        table = self._table(endpoint)
        with psycopg2.connect(self.conn_str) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL(
                        "SELECT payload FROM {}.{} ORDER BY bronze_item_id DESC LIMIT %s"
                    ).format(
                        sql.Identifier(BRONZE_SCHEMA), sql.Identifier(table)
                    ),
                    (max_rows,),
                )
                for (payload,) in cur.fetchall():
                    if isinstance(payload, dict):
                        yield payload
