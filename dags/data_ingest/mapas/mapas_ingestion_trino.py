"""DAG de ingestão ELT do Mapas Culturais via Trino (PostgreSQL → Bronze/PostgreSQL).

Lê todos os schemas configurados do banco Mapas Culturais e grava na camada
Bronze do data warehouse do MinC, sem que nenhum dado passe por Python: quem
lê da origem e escreve no destino é o Trino. O Airflow só emite SQL e registra
o resultado.

Por que Trino
-------------
O Airflow em produção roda na infra do Serpro; o banco do Mapas e o Postgres
de destino ficam na infra do MinC — redes separadas. A única Connection que
esta DAG usa é ``trino_default``. Nenhum byte do volume atravessa a fronteira:
o Airflow manda SQL e recebe contagem de linhas.

Diferenças em relação à ingestão SALIC
---------------------------------------
O SALIC usa SQL Server como origem, então precisa de T-SQL via passthrough do
conector para ler metadados de chave primária e contagem de linhas. O Mapas usa
PostgreSQL, portanto:

* PKs de coluna única e tipo inteiro são descobertas via
  ``information_schema.table_constraints`` — query padrão, sem passthrough.
* Contagem aproximada de linhas vem de ``pg_catalog.pg_stat_user_tables``
  (melhor esforço; zero se indisponível, o que desliga o fatiamento e carrega
  a tabela em uma vez só).
* Não há ``unsupported-type-handling`` nem ``case-insensitive-name-matching``
  porque PostgreSQL → PostgreSQL não tem os tipos exóticos nem o CamelCase do
  SQL Server.

A montagem do SQL de carga (fatias, predicados, DDL da bronze) reutiliza
``plugins/trino_bronze.py`` sem alteração.

Configuração
------------
Variable ``mapas_trino_data`` (JSON): lista de schemas a ingerir. Cada entrada
descreve um schema do banco Mapas::

    [
      {
        "schema": "public",
        "catalog": "mapas",
        "tables": [],
        "exclude_tables": [],
        "rows_per_slice": 500000,
        "slice_concurrency": 2
      }
    ]

``tables`` vazio significa "todas as tabelas base do schema". ``catalog`` pode
ser omitido: o padrão é ``mapas``, que é o nome do arquivo em
``infra/trino/etc/catalog/``.

Connections requeridas
----------------------
``trino_default`` — a única. Em desenvolvimento local, ``localhost:8090``
(o Airflow roda em ``network_mode: host``). Em produção, o host do Trino do
MinC.
"""

import logging
import os
import time
import traceback
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from datetime import datetime, timedelta, timezone
from typing import Any

from airflow.models import Variable
from airflow.providers.trino.hooks.trino import TrinoHook
from airflow.sdk import Param, dag, get_current_context, task

from trino_bronze import (
    DEFAULT_BRONZE_SCHEMA,
    DEFAULT_TARGET_CATALOG,
    SLICE_COLUMN,
    bronze_ddl,
    bronze_schema,
    bronze_table_name,
    build_statements,
    metadata_key,
    parse_only_tables,
    pick_key_columns,
    plan_slices,
    quote_ident,
    source_fqtn,
    sql_literal,
    target_catalog,
)

TRINO_CONN_ID = "trino_default"

_DEFAULT_CATALOG = "mapas"
_DEFAULT_CONTROL_SCHEMA = "control"
_BRONZE_SCHEMA_VAR = "mapas_trino_bronze_schema"
_CONTROL_SCHEMA_VAR = "mapas_trino_control_schema"
_TARGET_CATALOG_VAR = "mapas_trino_target_catalog"
_LOG_TABLE = "mapas_trino_ingestion_log"

_DEFAULT_ROWS_PER_SLICE = 500_000
_DEFAULT_SLICE_CONCURRENCY = 2
_SLICE_RETRIES = 2

_MAX_PARALLEL_TABLES = int(os.getenv("MAPAS_TRINO_MAX_PARALLEL_TABLES", "4"))
_MAX_MAPPED_TASKS = int(os.getenv("MAPAS_TRINO_MAX_MAPPED_TASKS", "64"))

default_args = {
    "owner": "Wallyson Souza",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


# ── Acesso a banco ────────────────────────────────────────────────────────────


def trino_run(sql: str) -> None:
    TrinoHook(trino_conn_id=TRINO_CONN_ID).run(sql)


def trino_records(sql: str) -> list[Any]:
    return TrinoHook(trino_conn_id=TRINO_CONN_ID).get_records(sql)


# ── Metadados da origem PostgreSQL ────────────────────────────────────────────


def _fetch_pg_keys(catalog: str, schema: str) -> dict[tuple[str, str], str]:
    """PKs de coluna única e tipo inteiro via information_schema do PostgreSQL.

    Diferente do SALIC (SQL Server), não usa passthrough: o information_schema
    do Postgres é consultado diretamente pelo Trino. Só colunas de tipo inteiro
    são elegíveis para fatiamento — são as que o predicado de faixa empurra
    com eficiência para a origem.
    """
    try:
        rows = trino_records(
            f"""
            SELECT kcu.table_schema, kcu.table_name, kcu.column_name, 1
            FROM {catalog}.information_schema.table_constraints tc
            JOIN {catalog}.information_schema.key_column_usage kcu
              ON tc.constraint_name = kcu.constraint_name
             AND tc.table_schema   = kcu.table_schema
             AND tc.table_name     = kcu.table_name
            JOIN {catalog}.information_schema.columns col
              ON col.table_schema = kcu.table_schema
             AND col.table_name   = kcu.table_name
             AND col.column_name  = kcu.column_name
            WHERE tc.constraint_type   = 'PRIMARY KEY'
              AND kcu.ordinal_position = 1
              AND lower(kcu.table_schema) = lower({sql_literal(schema)})
              AND col.data_type IN ('integer', 'bigint', 'smallint')
            """
        )
        return pick_key_columns(rows)
    except Exception as exc:
        logging.warning(
            "[mapas_trino] PKs de %s.%s indisponíveis (%s: %s). "
            "Tabelas carregadas sem fatiamento.",
            catalog,
            schema,
            type(exc).__name__,
            exc,
        )
        return {}


def _fetch_pg_row_counts(catalog: str, schema: str) -> dict[tuple[str, str], int]:
    """Contagem aproximada de linhas via pg_stat_user_tables (melhor esforço).

    n_live_tup é uma estimativa do autovacuum — pode divergir do COUNT(*) real,
    o que é aceitável para decidir se fatiar ou não. Se a view não estiver
    acessível pelo Trino, retorna dicionário vazio e todas as tabelas são
    carregadas em uma só query (sem fatiamento).
    """
    try:
        rows = trino_records(
            f"""
            SELECT schemaname, tablename, n_live_tup
            FROM {catalog}.pg_catalog.pg_stat_user_tables
            WHERE schemaname = {sql_literal(schema)}
            """
        )
        return {metadata_key(r[0], r[1]): int(r[2] or 0) for r in rows}
    except Exception as exc:
        logging.warning(
            "[mapas_trino] pg_stat_user_tables indisponível para %s.%s (%s: %s). "
            "Contagem de linhas zerada — sem fatiamento.",
            catalog,
            schema,
            type(exc).__name__,
            exc,
        )
        return {}


# ── Log de controle ───────────────────────────────────────────────────────────


def _target_catalog_name() -> str:
    return Variable.get(_TARGET_CATALOG_VAR, default_var=DEFAULT_TARGET_CATALOG)


def _bronze_schema_name() -> str:
    return Variable.get(_BRONZE_SCHEMA_VAR, default_var=DEFAULT_BRONZE_SCHEMA)


def _control_schema_name() -> str:
    return Variable.get(_CONTROL_SCHEMA_VAR, default_var=_DEFAULT_CONTROL_SCHEMA)


def _create_log_table_sql(catalogo: str, controle: str) -> str:
    return f"""
CREATE TABLE IF NOT EXISTS {catalogo}.{controle}.{_LOG_TABLE} (
    dag_id       varchar,
    run_id       varchar,
    "catalog"    varchar,
    "schema"     varchar,
    table_name   varchar,
    bronze_table varchar,
    status       varchar,
    key_column   varchar,
    slices       integer,
    rows_loaded  bigint,
    rows_source  bigint,
    error_msg    varchar,
    started_at   timestamp(6) with time zone,
    finished_at  timestamp(6) with time zone
)
"""


def write_log(target: dict, status: str, stats: dict) -> None:
    context = get_current_context()
    erro = stats.get("error_msg")
    if erro:
        erro = erro[:4000]
    valores = ", ".join(
        [
            sql_literal(context["dag"].dag_id),
            sql_literal(context["run_id"]),
            sql_literal(target["catalog"]),
            sql_literal(target["schema"]),
            sql_literal(target["table"]),
            sql_literal(target["bronze_table"]),
            sql_literal(status),
            _literal_or_null(target.get("key_column")),
            _numero_ou_null(stats.get("slices")),
            _numero_ou_null(stats.get("rows_loaded")),
            _numero_ou_null(target.get("row_count")),
            _literal_or_null(erro),
            _instante(stats["started_at"]),
            _instante(datetime.now(timezone.utc)),
        ]
    )
    trino_run(
        f"""
        INSERT INTO {target_catalog(target)}.{target["control_schema"]}.{_LOG_TABLE}
            (dag_id, run_id, "catalog", "schema", table_name,
             bronze_table, status, key_column, slices, rows_loaded,
             rows_source, error_msg, started_at, finished_at)
        VALUES ({valores})
        """
    )


def _literal_or_null(valor: str | None) -> str:
    return "NULL" if valor is None else sql_literal(valor)


def _numero_ou_null(valor: int | None) -> str:
    return "NULL" if valor is None else str(int(valor))


def _instante(quando: datetime) -> str:
    return (
        f"CAST(from_iso8601_timestamp({sql_literal(quando.isoformat())}) "
        f"AS timestamp(6) with time zone)"
    )


def tables_done_today(catalogo: str, controle: str) -> set[tuple[str, str]]:
    """Pares (schema, tabela) que já concluíram hoje, para retomada."""
    linhas = trino_records(
        f"""
        SELECT "schema", table_name
        FROM {catalogo}.{controle}.{_LOG_TABLE}
        WHERE status = 'success'
          AND started_at >= CAST(current_date AS timestamp(6) with time zone)
        """
    )
    return {(row[0], row[1]) for row in linhas}


# ── DAG ───────────────────────────────────────────────────────────────────────


@dag(
    dag_id="mapas_ingestion_trino",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["mapas", "bronze", "extraction", "trino"],
    doc_md=__doc__,
    params={
        "full_refresh": Param(
            False,
            type="boolean",
            title="Recarregar tabelas já concluídas hoje",
            description=(
                "Desligado, a DAG pula as tabelas que já terminaram com sucesso "
                "hoje — permite retomar uma carga interrompida sem refazer o que "
                "já passou. Ligue para forçar tudo de novo."
            ),
        ),
        "only_tables": Param(
            None,
            type=["null", "string"],
            title="Carregar apenas estas tabelas",
            description=(
                "Lista separada por vírgula no formato schema.tabela, por exemplo: "
                "public.occurrence, tiger.addr. Vazio carrega tudo o que a "
                "Variable mapas_trino_data descreve."
            ),
        ),
        "rows_per_slice": Param(
            0,
            type="integer",
            minimum=0,
            title="Linhas por fatia (0 = usar o valor da Variable)",
            description=(
                "Tamanho alvo de cada INSERT. Só afeta tabelas com PK inteira "
                "e estimativa de linhas acima do limite."
            ),
        ),
        "dry_run": Param(
            False,
            type="boolean",
            title="Só planejar",
            description=(
                "Monta o plano e registra o SQL no log da task, sem escrever "
                "nada na bronze."
            ),
        ),
    },
)
def mapas_ingestion_trino() -> None:
    """Carrega os schemas do Mapas Culturais na Bronze usando o Trino como motor de cópia."""

    @task
    def load_config() -> list[dict]:
        raw: list[dict] = Variable.get("mapas_trino_data", deserialize_json=True)
        override = int(get_current_context()["params"]["rows_per_slice"])

        configs = []
        for source in raw:
            schema = source["schema"]
            configs.append(
                {
                    "schema": schema,
                    "catalog": source.get("catalog", _DEFAULT_CATALOG),
                    "tables": source.get("tables", []),
                    "exclude_tables": [
                        t.lower() for t in source.get("exclude_tables", [])
                    ],
                    "rows_per_slice": override
                    or source.get("rows_per_slice", _DEFAULT_ROWS_PER_SLICE),
                    "slice_concurrency": source.get(
                        "slice_concurrency", _DEFAULT_SLICE_CONCURRENCY
                    ),
                }
            )
        logging.info(
            "[mapas_trino] load_config: %d schema(s): %s",
            len(configs),
            [f"{c['catalog']}.{c['schema']}" for c in configs],
        )
        return configs

    @task
    def ensure_schemas() -> None:
        """Cria bronze, control e a tabela de log — tudo pelo Trino."""
        catalogo = _target_catalog_name()
        bronze = _bronze_schema_name()
        controle = _control_schema_name()
        logging.info(
            "[mapas_trino] destino: %s.%s (bronze) e %s.%s (controle)",
            catalogo, bronze, catalogo, controle,
        )
        trino_run(f"CREATE SCHEMA IF NOT EXISTS {catalogo}.{bronze}")
        trino_run(f"CREATE SCHEMA IF NOT EXISTS {catalogo}.{controle}")
        trino_run(_create_log_table_sql(catalogo, controle))

    @task
    def plan_targets(configs: list[dict]) -> list[list[dict]]:
        """Monta a lista de tabelas a carregar, já com chave e contagem de linhas."""
        params = get_current_context()["params"]
        catalogo = _target_catalog_name()
        bronze = _bronze_schema_name()
        controle = _control_schema_name()
        done = (
            set() if params["full_refresh"] else tables_done_today(catalogo, controle)
        )
        destino = {
            "target_catalog": catalogo,
            "bronze_schema": bronze,
            "control_schema": controle,
        }
        only = parse_only_tables(params["only_tables"])

        targets: list[dict] = []
        for source in configs:
            targets.extend(_plan_source(source, done, only, destino))

        targets.sort(key=lambda t: t["row_count"], reverse=True)
        lotes = _distribuir_em_lotes(targets)
        logging.info(
            "[mapas_trino] plan_targets: %d tabela(s) em %d lote(s), ~%d linhas no total.",
            len(targets),
            len(lotes),
            sum(t["row_count"] for t in targets),
        )
        return lotes

    @task(max_active_tis_per_dagrun=_MAX_PARALLEL_TABLES)
    def load_batch(lote: list[dict]) -> int:
        params = get_current_context()["params"]
        feitas = (
            set()
            if params["full_refresh"]
            else tables_done_today(_target_catalog_name(), _control_schema_name())
        )
        total, falhas = _carregar_lote(lote, feitas)
        if falhas:
            raise RuntimeError(
                f"{len(falhas)} de {len(lote)} tabela(s) falharam neste lote: "
                + ", ".join(falhas[:10])
                + (" ..." if len(falhas) > 10 else "")
            )
        return total

    configs = load_config()
    schemas_ready = ensure_schemas()
    targets = plan_targets(configs)

    schemas_ready >> targets
    load_batch.expand(lote=targets)


# ── Carga de um lote ──────────────────────────────────────────────────────────


def _carregar_lote(
    lote: list[dict], feitas: set[tuple[str, str]]
) -> tuple[int, list[str]]:
    total = 0
    falhas = []
    for target in lote:
        if (target["schema"], target["table"]) in feitas:
            logging.info(
                "[mapas_trino] %s.%s já concluída hoje — pulando.",
                target["schema"],
                target["table"],
            )
            continue
        try:
            total += _load_one(target)
        except Exception:
            falhas.append(f"{target['schema']}.{target['table']}")
    return total, falhas


# ── Carga de uma tabela ───────────────────────────────────────────────────────


def _load_one(target: dict) -> int:
    started_at = datetime.now(timezone.utc)
    dry_run = get_current_context()["params"]["dry_run"]
    t0 = time.monotonic()

    try:
        columns = _fetch_columns(target)
        if not columns:
            logging.warning(
                "[mapas_trino] %s.%s sem colunas — pulando.",
                target["schema"],
                target["table"],
            )
            write_log(target, "skipped", _stats(started_at, error="sem colunas"))
            return 0

        statements = build_statements(target, columns, _slices_for(target))

        if dry_run:
            _log_dry_run(target, columns, statements)
            write_log(target, "dry_run", _stats(started_at, slices=len(statements)))
            return 0

        _recreate_bronze_table(target, columns)
        rows = _run_statements(target, statements)

        logging.info(
            "[mapas_trino] concluído %s.%s: %d linha(s) em %d fatia(s), %.0fs "
            "(origem estimava %d).",
            bronze_schema(target),
            target["bronze_table"],
            rows,
            len(statements),
            time.monotonic() - t0,
            target["row_count"],
        )
        write_log(
            target, "success", _stats(started_at, rows=rows, slices=len(statements))
        )
        return rows

    except Exception as exc:
        logging.error(
            "[mapas_trino] ERRO em %s.%s (%.0fs) %s: %s",
            target["schema"],
            target["table"],
            time.monotonic() - t0,
            type(exc).__name__,
            exc,
        )
        write_log(target, "error", _stats(started_at, error=traceback.format_exc()))
        raise


# ── Planejamento ──────────────────────────────────────────────────────────────


def _plan_source(
    source: dict,
    done: set[tuple[str, str]],
    only: set[tuple[str, str]],
    destino: dict,
) -> list[dict]:
    catalog = source["catalog"]
    schema = source["schema"]

    rows = trino_records(
        f"""
        SELECT table_schema, table_name
        FROM {catalog}.information_schema.tables
        WHERE lower(table_schema) = lower({sql_literal(schema)})
          AND table_type = 'BASE TABLE'
        ORDER BY table_name
        """
    )
    if source["tables"]:
        wanted = {t.lower() for t in source["tables"]}
        rows = [r for r in rows if r[1].lower() in wanted]

    keys = _fetch_pg_keys(catalog, schema)
    counts = _fetch_pg_row_counts(catalog, schema)

    targets = []
    for table_schema, table in rows:
        if not _wanted(source, table, done, only):
            continue
        key = metadata_key(table_schema, table)
        targets.append(
            {
                "catalog": catalog,
                **destino,
                "schema": table_schema,
                "table": table,
                # schema usado como discriminador no nome da bronze (público__tabela,
                # tiger__tabela…) para evitar colisão entre schemas distintos.
                "bronze_table": bronze_table_name(table_schema, table),
                "row_count": counts.get(key, 0),
                "key_column": keys.get(key),
                "rows_per_slice": source["rows_per_slice"],
                "slice_concurrency": source["slice_concurrency"],
            }
        )
    logging.info(
        "[mapas_trino] %s.%s: %d tabela(s) a carregar (%d com chave para fatiar).",
        catalog,
        schema,
        len(targets),
        sum(1 for t in targets if t["key_column"]),
    )
    return targets


def _distribuir_em_lotes(targets: list[dict]) -> list[list[dict]]:
    if not targets:
        return []
    n = min(_MAX_MAPPED_TASKS, len(targets))
    lotes: list[list[dict]] = [[] for _ in range(n)]
    for i, target in enumerate(targets):
        lotes[i % n].append(target)
    return lotes


def _wanted(
    source: dict, table: str, done: set[tuple[str, str]], only: set[tuple[str, str]]
) -> bool:
    if table.lower() in source["exclude_tables"]:
        return False
    if only and (source["schema"].lower(), table.lower()) not in only:
        return False
    return (source["schema"], table) not in done


def _fetch_columns(target: dict) -> list[tuple[str, str]]:
    rows = trino_records(
        f"""
        SELECT column_name, data_type
        FROM {target['catalog']}.information_schema.columns
        WHERE table_schema = {sql_literal(target['schema'])}
          AND table_name   = {sql_literal(target['table'])}
        ORDER BY ordinal_position
        """
    )
    return [(r[0], r[1]) for r in rows]


def _slices_for(target: dict) -> list[tuple[int | None, int | None]]:
    key = target["key_column"]
    if not key or target["row_count"] <= target["rows_per_slice"]:
        return [(None, None)]

    ident = quote_ident(key)
    rows = trino_records(
        f"SELECT min({ident}), max({ident}) FROM {source_fqtn(target)}"
    )
    key_min, key_max = (rows[0][0], rows[0][1]) if rows else (None, None)
    slices = plan_slices(
        None if key_min is None else int(key_min),
        None if key_max is None else int(key_max),
        target["row_count"],
        target["rows_per_slice"],
    )
    logging.info(
        "[mapas_trino] %s.%s: chave %s em [%s, %s], ~%d linhas → %d fatia(s).",
        target["schema"],
        target["table"],
        key,
        key_min,
        key_max,
        target["row_count"],
        len(slices),
    )
    return slices


# ── Execução ──────────────────────────────────────────────────────────────────


def _recreate_bronze_table(target: dict, columns: list[tuple[str, str]]) -> None:
    drop, create = bronze_ddl(target, columns)
    trino_run(drop)
    trino_run(create)
    logging.info(
        "[mapas_trino] %s.%s recriada com %d coluna(s) + %s.",
        bronze_schema(target),
        target["bronze_table"],
        len(columns),
        SLICE_COLUMN,
    )


def _run_statements(target: dict, statements: list[dict]) -> int:
    if len(statements) == 1:
        with closing(_trino_conn()) as conn:
            return _run_slice(conn, target, statements[0])

    workers = min(target["slice_concurrency"], len(statements))
    lotes = [statements[i::workers] for i in range(workers)]
    lotes = [lote for lote in lotes if lote]
    logging.info(
        "[mapas_trino] %s.%s: %d fatia(s) em %d thread(s) paralela(s).",
        target["schema"],
        target["table"],
        len(statements),
        len(lotes),
    )

    conexoes = [_trino_conn() for _ in lotes]
    try:
        with ThreadPoolExecutor(max_workers=len(lotes)) as pool:
            futuros = [
                pool.submit(_run_batch, conexoes[i], target, lote)
                for i, lote in enumerate(lotes)
            ]
            return sum(f.result() for f in futuros)
    finally:
        for conn in conexoes:
            try:
                conn.close()
            except Exception:
                pass


def _trino_conn() -> Any:
    """Abre uma conexão DBAPI com o Trino. Só pode ser chamada na thread principal."""
    return TrinoHook(trino_conn_id=TRINO_CONN_ID).get_conn()


def _run_batch(conn: Any, target: dict, statements: list[dict]) -> int:
    return sum(_run_slice(conn, target, st) for st in statements)


def _run_slice(conn: Any, target: dict, statement: dict) -> int:
    label = f"{target['schema']}.{target['table']} fatia {statement['index']}"
    for attempt in range(1, _SLICE_RETRIES + 2):
        t0 = time.monotonic()
        try:
            if attempt > 1:
                _delete_slice_range(conn, target, statement)
            rows = _insert_rows(conn, statement["insert"])
            logging.info(
                "[mapas_trino] %s %s: %d linha(s) em %.0fs.",
                label,
                statement["range"],
                rows,
                time.monotonic() - t0,
            )
            return rows
        except Exception as exc:
            if attempt > _SLICE_RETRIES:
                logging.error("[mapas_trino] %s esgotou as tentativas.", label)
                raise
            logging.warning(
                "[mapas_trino] %s falhou após %.0fs na tentativa %d/%d (%s: %s). "
                "Vai apagar a faixa e repetir.",
                label,
                time.monotonic() - t0,
                attempt,
                _SLICE_RETRIES + 1,
                type(exc).__name__,
                exc,
            )
    return 0


def _insert_rows(conn: Any, sql: str) -> int:
    cursor = conn.cursor()
    cursor.execute(sql)
    records = cursor.fetchall()
    if records and records[0] and records[0][0] is not None:
        return int(records[0][0])
    return 0


def _delete_slice_range(conn: Any, target: dict, statement: dict) -> None:
    cursor = conn.cursor()
    cursor.execute(statement["delete"])
    apagadas = cursor.fetchall()
    logging.info(
        "[mapas_trino] %s.%s fatia %d: %s linha(s) parciais apagadas.",
        target["schema"],
        target["table"],
        statement["index"],
        apagadas[0][0] if apagadas and apagadas[0] else "?",
    )


# ── Relato ────────────────────────────────────────────────────────────────────


def _stats(
    started_at: datetime,
    rows: int | None = None,
    slices: int | None = None,
    error: str | None = None,
) -> dict:
    return {
        "started_at": started_at,
        "rows_loaded": rows,
        "slices": slices,
        "error_msg": error,
    }


def _log_dry_run(
    target: dict, columns: list[tuple[str, str]], statements: list[dict]
) -> None:
    logging.info(
        "[mapas_trino] DRY RUN %s.%s → %s.%s\n"
        "  colunas : %d\n"
        "  chave   : %s\n"
        "  linhas  : %d (estimativa da origem)\n"
        "  fatias  : %d\n"
        "  primeira:\n%s",
        target["schema"],
        target["table"],
        bronze_schema(target),
        target["bronze_table"],
        len(columns),
        target["key_column"] or "(nenhuma — carga em uma vez só)",
        target["row_count"],
        len(statements),
        statements[0]["insert"],
    )


mapas_ingestion_trino()
