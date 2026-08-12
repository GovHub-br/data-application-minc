"""DAG de ingestão ELT do SALIC (SQL Server → Bronze/PostgreSQL).

Extrai dados dos servidores SALIC via SQL Server e carrega em bruto na camada
Bronze do PostgreSQL.  A transformação Bronze → Silver → Gold é responsabilidade
do DBT, disparado em outra DAG via Cosmos.

Esta DAG faz **apenas Extract + Load**: nenhum dado é transformado em Python.
Todos os valores são armazenados como TEXT (cast defensivo no load); a tipagem
correta fica com o DBT.

Configuração:
    Variable ``salic_data`` (JSON): lista de fontes com ``conn_id``, ``database``,
    ``schema``, ``tables`` e ``chunk_size``.  Se ``tables`` for vazio, todas as
    tabelas do schema são extraídas.

Connections requeridas:
    ``mssql_salic_<servidor>`` — SQL Server de cada servidor SALIC.
    ``postgres_default`` — PostgreSQL destino (camada Bronze).
"""

import logging
import traceback
import psycopg2
import psycopg2.extras
from datetime import datetime, timedelta
from datetime import timezone

from airflow.models import Variable
from airflow.sdk import dag, task
from airflow.sdk import get_current_context

from cliente_postgres import ClientPostgresDB
from extractor import Extractor
from postgres_helpers import get_postgres_conn
from sql_server_extractor import SQLServerExtractor

_BRONZE_SCHEMA = "bronze"
_CONTROL_SCHEMA = "control"
_LOG_TABLE = "salic_ingestion_log"
_DEFAULT_SCHEMA = "dbo"
_DEFAULT_CHUNK_SIZE = 50_000

default_args = {
    "owner": "Wallyson Souza",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="salic_ingestion",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["salic", "bronze", "extraction"],
)
def salic_ingestion() -> None:
    """Extrai todas as tabelas dos servidores SALIC e carrega na Bronze."""

    @task
    def load_config() -> list[dict]:
        raw: list[dict] = Variable.get("salic_data", deserialize_json=True)
        configs = []
        for source in raw:
            configs.append(
                {
                    "server": source["server"],
                    "conn_id": source["conn_id"],
                    "database": source["database"],
                    "schema": source.get("schema", _DEFAULT_SCHEMA),
                    "tables": source.get("tables", []),
                    "exclude_tables": [t.lower() for t in source.get("exclude_tables", [])],
                    "chunk_size": source.get("chunk_size", _DEFAULT_CHUNK_SIZE),
                }
            )
        logging.info(
            "[salic_ingestion] load_config: %d fonte(s) carregada(s): %s",
            len(configs),
            [c["server"] for c in configs],
        )
        return configs

    @task
    def expand_targets(configs: list[dict]) -> list[dict]:
        context = get_current_context()
        run_id = context["run_id"]

        targets: list[dict] = []
        for source in configs:
            tables = source["tables"]
            if not tables:
                extractor = SQLServerExtractor(conn_id=source["conn_id"])
                tables = extractor.list_tables(source["database"], source["schema"])
                logging.info(
                    "[salic_ingestion] expand_targets: %d tabelas descobertas "
                    "em %s.[%s] (conn=%s)",
                    len(tables),
                    source["database"],
                    source["schema"],
                    source["conn_id"],
                )

            exclude = source["exclude_tables"]
            for table in tables:
                if table.lower() in exclude:
                    logging.info(
                        "[salic_ingestion] expand_targets: tabela %s ignorada (exclude_tables).",
                        table,
                    )
                    continue
                targets.append(
                    {
                        "server": source["server"],
                        "conn_id": source["conn_id"],
                        "database": source["database"],
                        "schema": source["schema"],
                        "table": table,
                        "chunk_size": source["chunk_size"],
                    }
                )

        pg_conn_str = get_postgres_conn("postgres_default")
        with psycopg2.connect(pg_conn_str) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT server, database, table_name
                    FROM {_CONTROL_SCHEMA}.{_LOG_TABLE}
                    WHERE status = 'success'
                      AND started_at >= CURRENT_DATE
                    """,
                )
                done = {(r[0], r[1], r[2]) for r in cur.fetchall()}

        if done:
            before = len(targets)
            targets = [
                t for t in targets
                if (t["server"], t["database"], t["table"]) not in done
            ]
            logging.info(
                "[salic_ingestion] expand_targets: %d tabela(s) já concluídas neste run — pulando.",
                before - len(targets),
            )

        logging.info(
            "[salic_ingestion] expand_targets: %d target(s) restante(s)",
            len(targets),
        )
        return targets

    @task
    def ensure_schemas() -> None:
        """Garante que os schemas bronze e control existem antes das tasks paralelas."""
        pg_conn_str = get_postgres_conn("postgres_default")
        with psycopg2.connect(pg_conn_str) as conn:
            with conn.cursor() as cur:
                cur.execute(f"CREATE SCHEMA IF NOT EXISTS {_BRONZE_SCHEMA};")
                cur.execute(f"CREATE SCHEMA IF NOT EXISTS {_CONTROL_SCHEMA};")
                cur.execute(f"""
                    CREATE TABLE IF NOT EXISTS {_CONTROL_SCHEMA}.{_LOG_TABLE} (
                        id          SERIAL PRIMARY KEY,
                        dag_id      TEXT,
                        run_id      TEXT,
                        server      TEXT,
                        database    TEXT,
                        schema      TEXT,
                        table_name  TEXT,
                        bronze_table TEXT,
                        status      TEXT,
                        rows_loaded INTEGER,
                        error_msg   TEXT,
                        started_at  TIMESTAMP,
                        finished_at TIMESTAMP
                    );
                """)
            conn.commit()

    def _insert_log(
        pg_conn_str: str,
        dag_id: str,
        run_id: str,
        target: dict,
        bronze_table: str,
        status: str,
        rows_loaded: int,
        error_msg: str | None,
        started_at: datetime,
    ) -> None:
        with psycopg2.connect(pg_conn_str) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    INSERT INTO {_CONTROL_SCHEMA}.{_LOG_TABLE}
                        (dag_id, run_id, server, database, schema, table_name,
                         bronze_table, status, rows_loaded, error_msg,
                         started_at, finished_at)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    """,
                    (
                        dag_id,
                        run_id,
                        target["server"],
                        target["database"],
                        target["schema"],
                        target["table"],
                        bronze_table,
                        status,
                        rows_loaded,
                        error_msg,
                        started_at,
                        datetime.now(timezone.utc),
                    ),
                )
            conn.commit()

    @task
    def extract_and_load(target: dict) -> int:
        context = get_current_context()
        dag_id = context["dag"].dag_id
        run_id = context["run_id"]

        database = target["database"]
        schema = target["schema"]
        table = target["table"]
        chunk_size = target["chunk_size"]
        conn_id = target["conn_id"]
        bronze_table = f"{database.lower()}__{table.lower()}"
        started_at = datetime.now(timezone.utc)

        logging.info(
            "[salic_ingestion] extract_and_load: %s.[%s].[%s] → %s.\"%s\"",
            database, schema, table, _BRONZE_SCHEMA, bronze_table,
        )

        pg_conn_str = get_postgres_conn("postgres_default")

        try:
            # ── 1. Descobre colunas da origem ─────────────────────────────────
            mssql = SQLServerExtractor(conn_id=conn_id)
            columns = mssql.list_columns(database, schema, table)
            if not columns:
                logging.warning(
                    "[salic_ingestion] Tabela %s.[%s].[%s] sem colunas — pulando.",
                    database, schema, table,
                )
                _insert_log(pg_conn_str, dag_id, run_id, target, bronze_table,
                            "skipped", 0, "sem colunas", started_at)
                return 0

            # ── 2. Prepara tabela Bronze (DROP + CREATE all TEXT) ──────────────
            db = ClientPostgresDB(pg_conn_str)
            db.drop_table_if_exists(bronze_table, schema=_BRONZE_SCHEMA)

            col_names = [c.lower() for c in columns]
            cols_sql  = ", ".join(f'"{c}"' for c in col_names)
            cols_ddl  = ", ".join(f'"{c}" TEXT' for c in col_names)

            with psycopg2.connect(pg_conn_str) as prep_conn:
                with prep_conn.cursor() as cur:
                    cur.execute(
                        f'CREATE TABLE {_BRONZE_SCHEMA}."{bronze_table}" ({cols_ddl});'
                    )
                prep_conn.commit()

            logging.info(
                "[salic_ingestion] Tabela %s.\"%s\" criada com %d colunas TEXT.",
                _BRONZE_SCHEMA, bronze_table, len(columns),
            )

            # ── 3. Extract + Load ──────────────────────────────────────────────
            extractor = Extractor(mssql)

            total = 0
            load_conn = psycopg2.connect(pg_conn_str)
            try:
                cur = load_conn.cursor()
                insert_sql = (
                    f'INSERT INTO {_BRONZE_SCHEMA}."{bronze_table}" ({cols_sql}) VALUES %s'
                )
                for chunk in extractor.buildExtraction(
                    database, table, chunk_size, schema=schema
                ):
                    values = [
                        tuple(None if v is None else str(v) for v in row.values())
                        for row in chunk
                    ]
                    psycopg2.extras.execute_values(
                        cur,
                        insert_sql,
                        values,
                        page_size=3000,
                    )
                    total += len(chunk)
                    logging.info(
                        "[salic_ingestion] %s.[%s].[%s]: %d linhas (total: %d)",
                        database, schema, table, len(chunk), total,
                    )

                load_conn.commit()
            finally:
                cur.close()
                load_conn.close()

            logging.info(
                "[salic_ingestion] Concluído: %s.\"%s\" — %d linhas.",
                _BRONZE_SCHEMA, bronze_table, total,
            )

            _insert_log(pg_conn_str, dag_id, run_id, target, bronze_table,
                        "success", total, None, started_at)
            return total

        except Exception as exc:
            error_msg = traceback.format_exc()
            logging.error(
                "[salic_ingestion] ERRO em %s.[%s].[%s]: %s",
                database, schema, table, exc,
            )
            _insert_log(pg_conn_str, dag_id, run_id, target, bronze_table,
                        "error", 0, error_msg, started_at)
            return 0

    # ── Wiring das tasks ──────────────────────────────────────────────────────
    configs = load_config()
    schemas_ready = ensure_schemas()
    targets = expand_targets(configs)
    schemas_ready >> extract_and_load.override(max_active_tis_per_dagrun=16).expand(target=targets)


salic_ingestion()
