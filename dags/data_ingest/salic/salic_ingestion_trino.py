"""DAG de ingestão ELT do SALIC via Trino (SQL Server → Bronze/PostgreSQL).

Versão 2 da ``salic_ingestion``. Faz a mesma coisa — Extract + Load em bruto na
camada Bronze, tipagem por conta do dbt — mas o dado não passa mais por Python:
quem lê do SQL Server e escreve no Postgres é o Trino, e o worker do Airflow só
emite SQL e registra o resultado.

Por que trocar
--------------
Na v1 cada linha vira objeto Python três vezes (``pymssql`` → ``dict`` →
``str()`` → tupla → ``execute_values``). Com terabytes isso é lento o bastante
para que um ``SELECT *`` fique horas com o mesmo cursor aberto — e é aí que a
VPN cai e o ``remote query timeout`` do servidor derruba a sessão, o modo de
falha que a v1 trata no ``except`` de ``pymssql``.

Trocar o motor sozinho não resolveria isso. Um ``CREATE TABLE AS SELECT *`` no
Trino tem exatamente o mesmo problema: o conector JDBC lê a tabela por **uma
única conexão**, e a query continua aberta do começo ao fim. O que resolve é
fatiar:

1. Descobre a chave inteira da tabela (PK de uma coluna, ou coluna identity) e
   a contagem aproximada de linhas — em três queries por banco, lendo
   ``sys.partitions`` e ``sys.indexes`` pelo passthrough do conector. Nenhuma
   varredura de tabela, nem uma query por tabela.
2. Divide o domínio da chave em faixas de ``rows_per_slice`` linhas.
3. Cada faixa vira um ``INSERT ... SELECT ... WHERE k >= a AND k < b``. O Trino
   empurra esse predicado para o SQL Server, então cada query é curta e cabe
   folgada dentro do timeout do servidor.
4. As faixas de uma tabela rodam em paralelo entre si, e várias tabelas rodam
   em paralelo entre si.
5. Uma faixa que falha é repetida sozinha, sem recomeçar a tabela inteira.

O ganho está no item 5, não no 4. Medido contra o SALIC em 2026-08-24: um fluxo
único leu blob a 7,2 MB/s e três fluxos em paralelo somaram 7,3 MB/s — o link da
VPN satura por volta de 8 MB/s e **abrir mais conexões não aumenta a vazão**.
Subir `slice_concurrency` esperando ir mais rápido é perda de tempo; o que as
fatias compram é resiliência, que é justamente o que faltava.

Tabela sem chave inteira, ou menor que ``rows_per_slice``, é carregada de uma
vez só — fatiar não traria nada.

A montagem do SQL — fatias, predicados, conversão de tipo — mora em
``plugins/trino_bronze.py``, que não depende de Airflow e é testado em
``tests/test_trino_bronze.py``. Aqui fica só a orquestração.

Configuração
------------
Variable ``salic_trino_data`` (JSON): lista de fontes. Cada fonte descreve um
banco do SQL Server e o catálogo do Trino que aponta para ele::

    [
      {
        "database": "SAC",
        "catalog": "salic_sac",
        "schema": "dbo",
        "tables": [],
        "exclude_tables": [],
        "rows_per_slice": 5000000,
        "slice_concurrency": 4
      }
    ]

``tables`` vazio significa "todas as tabelas base do schema". ``catalog`` pode
ser omitido: o padrão é ``salic_<database em minúsculas>``, que é o nome dos
arquivos em ``infra/trino/etc/catalog/``.

Connections requeridas
----------------------
``trino_default`` — **a única**. Com o compose deste repositório,
``localhost:8090`` (o Airflow roda em ``network_mode: host``), usuário qualquer,
sem senha.

Isto é regra de arquitetura, não conveniência: em produção o Airflow fica na
infra do Serpro e o SQL Server, o Trino e o Postgres ficam na infra do MinC.
**O Airflow não tem rota até banco nenhum** — ele só conversa com o Trino, que
por sua vez está do lado dos dois bancos. Nenhum byte do volume atravessa a
fronteira entre as duas redes: o Airflow manda SQL e recebe contagem de linhas.

Daí vêm duas consequências que parecem detalhe e não são:

* o DDL da bronze sai pelo Trino, então as colunas nascem ``varchar`` e não
  ``TEXT`` (mesmo armazenamento no PostgreSQL);
* apagar uma fatia para repeti-la recorta pela coluna técnica ``_fatia``, não
  pela chave da tabela — ver ``trino_bronze.SLICE_COLUMN``.

Qualquer ``psycopg2``/``PostgresHook`` reintroduzido aqui quebra em produção,
mesmo passando no ambiente local, onde tudo divide a mesma máquina.

Diferenças de conteúdo em relação à v1
--------------------------------------
Nome da tabela bronze, nome das colunas e tipo ``TEXT`` são idênticos aos da
v1 — as 561 fontes declaradas em ``dbt/minc/models/salic_dbt/bronze/`` continuam
valendo sem alteração. O texto dentro das colunas muda em três tipos
(``bit``, ``char(n)`` e ``varbinary``); o porquê de cada um está no docstring
de ``trino_bronze.cast_to_text``.
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
    SLICE_COLUMN,
    DEFAULT_TARGET_CATALOG,
    TSQL_KEY_COLUMNS,
    TSQL_ROW_COUNTS,
    bronze_ddl,
    bronze_table_name,
    build_statements,
    index_row_counts,
    metadata_key,
    parse_only_tables,
    passthrough,
    pick_key_columns,
    plan_slices,
    quote_ident,
    source_fqtn,
    bronze_schema,
    sql_literal,
    target_catalog,
)

TRINO_CONN_ID = "trino_default"

# Schemas de destino. Padrões: os valores reais vêm das Variables
# `salic_trino_bronze_schema` e `salic_trino_control_schema`. Em banco
# compartilhado o usuário do Trino costuma ter permissão só num schema próprio —
# criar tabela fora dele dá "permission denied for schema".
_DEFAULT_CONTROL_SCHEMA = "control"
_BRONZE_SCHEMA_VAR = "salic_trino_bronze_schema"
_CONTROL_SCHEMA_VAR = "salic_trino_control_schema"
_LOG_TABLE = "salic_trino_ingestion_log"
_DEFAULT_SCHEMA = "dbo"
_DEFAULT_CATALOG_PREFIX = "salic_"

# Nome do catálogo do Trino que aponta para o data warehouse. Vem da Variable
# `salic_trino_target_catalog` porque quem batiza os catálogos é quem administra
# o Trino — em produção ele pode não se chamar "dw". Sem a Variable, usa o padrão.
_TARGET_CATALOG_VAR = "salic_trino_target_catalog"

# Uma fatia de 5M linhas leva minutos, não horas — é o que mantém cada query
# abaixo do `remote query timeout` de 20min do servidor do SALIC.
_DEFAULT_ROWS_PER_SLICE = 5_000_000

# Quantas faixas da MESMA tabela vão ao SQL Server ao mesmo tempo.
_DEFAULT_SLICE_CONCURRENCY = 4

# Quantas TABELAS carregam ao mesmo tempo. Lido do ambiente porque o Airflow
# precisa deste número ao parsear a DAG, e ler Variable no parse bate no banco
# de metadados a cada ciclo do dag-processor.
#
# ATENÇÃO: o número de conexões simultâneas ao SQL Server é este valor VEZES o
# `slice_concurrency` da fonte. O padrão 4 × 4 = 16 conexões. Subir os dois sem
# medir é o jeito mais fácil de saturar a VPN — que é justamente o problema que
# esta DAG existe para resolver.
_MAX_PARALLEL_TABLES = int(os.getenv("SALIC_TRINO_MAX_PARALLEL_TABLES", "4"))

# Teto de tasks mapeadas. O Airflow recusa expandir acima de `max_map_length`
# (padrão 1024) com "pushed value is too large to map as a downstream's
# dependency" — e são 1139 tabelas só no SALIC. Em vez de depender de uma config
# de infraestrutura que varia por ambiente, a DAG agrupa as tabelas em lotes e
# mapeia sobre os lotes. Cada lote carrega as suas tabelas em sequência.
_MAX_MAPPED_TASKS = int(os.getenv("SALIC_TRINO_MAX_MAPPED_TASKS", "256"))

# Repetições de uma faixa isolada antes de derrubar a tabela inteira.
_SLICE_RETRIES = 2

default_args = {
    "owner": "Wallyson Souza",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


# ── Acesso a banco ───────────────────────────────────────────────────────────


def trino_run(sql: str) -> None:
    """Executa um comando no Trino e descarta o resultado."""
    TrinoHook(trino_conn_id=TRINO_CONN_ID).run(sql)


def trino_records(sql: str) -> list[Any]:
    """Executa *sql* no Trino numa conexão nova e devolve todas as linhas.

    Conexão por chamada de propósito: as faixas de uma tabela rodam em threads
    e cada uma precisa da sua.
    """
    return TrinoHook(trino_conn_id=TRINO_CONN_ID).get_records(sql)


def source_metadata(catalog: str, schema: str, tsql: str) -> list[Any]:
    """Roda um T-SQL de metadados na origem, tolerando indisponibilidade.

    A ``query()`` depende do ``sp_describe_first_result_set`` do SQL Server para
    inferir o formato do resultado, e nem todo servidor ou permissão o expõe.
    Sem esses metadados a ingestão ainda funciona — só perde o fatiamento —,
    então a falha vira aviso e não erro.
    """
    try:
        tsql_rendered = tsql.format(schema=sql_literal(schema))
        return trino_records(passthrough(catalog, tsql_rendered))
    except Exception as exc:
        logging.warning(
            "[salic_trino] metadados de %s indisponíveis (%s: %s). As tabelas "
            "deste banco serão carregadas sem fatiamento.",
            catalog,
            type(exc).__name__,
            exc,
        )
        return []


# ── Log de controle ──────────────────────────────────────────────────────────

def _target_catalog() -> str:
    """Catálogo de destino configurado, ou o padrão."""
    return Variable.get(_TARGET_CATALOG_VAR, default_var=DEFAULT_TARGET_CATALOG)


def _bronze_schema() -> str:
    """Schema da bronze configurado, ou o padrão."""
    return Variable.get(_BRONZE_SCHEMA_VAR, default_var=DEFAULT_BRONZE_SCHEMA)


def _control_schema() -> str:
    """Schema do log de controle configurado, ou o padrão."""
    return Variable.get(_CONTROL_SCHEMA_VAR, default_var=_DEFAULT_CONTROL_SCHEMA)


def _create_log_table_sql(catalogo: str, controle: str) -> str:
    """DDL da tabela de controle.

    Sem SERIAL nem CREATE INDEX: o Trino não os emite. A tabela guarda uma linha
    por tabela por execução — algumas milhares por ano —, então varredura
    completa na consulta de retomada é irrelevante.
    """
    return f"""
CREATE TABLE IF NOT EXISTS {catalogo}.{controle}.{_LOG_TABLE} (
    dag_id       varchar,
    run_id       varchar,
    "catalog"    varchar,
    "database"   varchar,
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
    """Registra o desfecho de uma tabela, pelo Trino.

    Vai pelo catálogo ``dw`` como todo o resto: o Airflow não tem rota até o
    Postgres, só até o Trino.
    """
    context = get_current_context()
    erro = stats.get("error_msg")
    if erro:
        # Traceback inteiro não cabe nem ajuda; as primeiras linhas dizem o quê.
        erro = erro[:4000]
    valores = ", ".join(
        [
            sql_literal(context["dag"].dag_id),
            sql_literal(context["run_id"]),
            sql_literal(target["catalog"]),
            sql_literal(target["database"]),
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
            (dag_id, run_id, "catalog", "database", "schema", table_name,
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
    """Instante como literal que o Trino aceita em coluna timestamptz."""
    return (
        f"CAST(from_iso8601_timestamp({sql_literal(quando.isoformat())}) "
        f"AS timestamp(6) with time zone)"
    )


def tables_done_today(catalogo: str, controle: str) -> set[tuple[str, str]]:
    """Pares ``(database, tabela)`` que já concluíram hoje, para retomada."""
    linhas = trino_records(
        f"""
        SELECT "database", table_name
        FROM {catalogo}.{controle}.{_LOG_TABLE}
        WHERE status = 'success'
          AND started_at >= CAST(current_date AS timestamp(6) with time zone)
        """
    )
    return {(row[0], row[1]) for row in linhas}


@dag(
    dag_id="salic_ingestion_trino",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["salic", "bronze", "extraction", "trino"],
    doc_md=__doc__,
    params={
        "full_refresh": Param(
            False,
            type="boolean",
            title="Recarregar tabelas já concluídas hoje",
            description=(
                "Desligado, a DAG pula as tabelas que já terminaram com sucesso "
                "hoje — é o que permite retomar uma carga interrompida sem "
                "refazer o que já passou. Ligue para forçar tudo de novo."
            ),
        ),
        "only_tables": Param(
            None,
            # ["null", "string"] em vez de "string": sem o null a UI do Airflow
            # marca o campo como obrigatório e não deixa disparar com ele vazio —
            # que é justamente o caso mais comum (carregar tudo).
            type=["null", "string"],
            title="Carregar apenas estas tabelas",
            description=(
                "Lista separada por vírgula no formato banco.tabela, por "
                "exemplo: SAC.Projetos, Agentes.Agentes. Vazio carrega tudo o "
                "que a Variable salic_trino_data descreve."
            ),
        ),
        "rows_per_slice": Param(
            0,
            type="integer",
            minimum=0,
            title="Linhas por fatia (0 = usar o valor da Variable)",
            description=(
                "Tamanho alvo de cada INSERT. Menor deixa cada query mais curta "
                "e mais resistente a queda de VPN, ao custo de mais idas e "
                "vindas. Só afeta tabelas com chave inteira."
            ),
        ),
        "dry_run": Param(
            False,
            type="boolean",
            title="Só planejar",
            description=(
                "Monta o plano, registra no log da task o SQL de cada fatia e "
                "não escreve nada na bronze."
            ),
        ),
    },
)
def salic_ingestion_trino() -> None:
    """Carrega os bancos do SALIC na Bronze usando o Trino como motor de cópia."""

    @task
    def load_config() -> list[dict]:
        raw: list[dict] = Variable.get("salic_trino_data", deserialize_json=True)
        override = int(get_current_context()["params"]["rows_per_slice"])

        configs = []
        for source in raw:
            database = source["database"]
            configs.append(
                {
                    "database": database,
                    "catalog": source.get(
                        "catalog", _DEFAULT_CATALOG_PREFIX + database.lower()
                    ),
                    "schema": source.get("schema", _DEFAULT_SCHEMA),
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
            "[salic_trino] load_config: %d fonte(s): %s",
            len(configs),
            [f"{c['database']} → {c['catalog']}" for c in configs],
        )
        return configs

    @task
    def ensure_schemas() -> None:
        """Cria bronze, control e a tabela de log — tudo pelo Trino."""
        catalogo = _target_catalog()
        bronze = _bronze_schema()
        controle = _control_schema()
        logging.info(
            "[salic_trino] destino: %s.%s (bronze) e %s.%s (controle)",
            catalogo, bronze, catalogo, controle,
        )
        trino_run(f"CREATE SCHEMA IF NOT EXISTS {catalogo}.{bronze}")
        trino_run(f"CREATE SCHEMA IF NOT EXISTS {catalogo}.{controle}")
        trino_run(_create_log_table_sql(catalogo, controle))

    @task
    def plan_targets(configs: list[dict]) -> list[dict]:
        """Monta a lista de tabelas a carregar, já com estatística e chave.

        Todo o metadado sai em três queries por banco — nunca uma por tabela.
        Com 561 tabelas, a diferença entre as duas abordagens é o tempo de
        planejamento inteiro.
        """
        params = get_current_context()["params"]
        catalogo = _target_catalog()
        bronze = _bronze_schema()
        controle = _control_schema()
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

        # Maiores primeiro: com um pool de tarefas fixo, deixar a tabela de 200 GB
        # para o fim faz a DAG inteira esperar por ela sozinha no final.
        targets.sort(key=lambda t: t["row_count"], reverse=True)
        lotes = _distribuir_em_lotes(targets)
        logging.info(
            "[salic_trino] plan_targets: %d tabela(s) em %d lote(s), ~%d linhas no "
            "total. Maiores: %s",
            len(targets),
            len(lotes),
            sum(t["row_count"] for t in targets),
            [(t["table"], t["row_count"]) for t in targets[:5]],
        )
        return lotes

    @task(max_active_tis_per_dagrun=_MAX_PARALLEL_TABLES)
    def load_batch(lote: list[dict]) -> int:
        """Carrega, em sequência, as tabelas de um lote.

        Uma tabela que falha não derruba as outras do lote: o erro vai para o log
        de controle e a task só falha no fim, dizendo quantas quebraram. Assim uma
        tabela problemática no meio de vinte não custa as outras dezenove.
        """
        params = get_current_context()["params"]
        feitas = (
            set()
            if params["full_refresh"]
            else tables_done_today(_target_catalog(), _control_schema())
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

    # plan_targets lê control.salic_trino_ingestion_log para saber o que já
    # passou hoje, então depende do ensure_schemas ter criado a tabela.
    schemas_ready >> targets
    load_batch.expand(lote=targets)


# ── Carga de um lote ─────────────────────────────────────────────────────────


def _carregar_lote(
    lote: list[dict], feitas: set[tuple[str, str]]
) -> tuple[int, list[str]]:
    """Carrega as tabelas do lote em sequência, sem parar na primeira falha.

    Consultar ``feitas`` aqui dentro — e não só no planejamento — é o que torna a
    repetição da task barata: numa segunda tentativa, as tabelas que já
    concluíram no dia são puladas em vez de recarregadas.
    """
    total = 0
    falhas = []
    for target in lote:
        if (target["database"], target["table"]) in feitas:
            logging.info(
                "[salic_trino] %s.%s já concluída hoje — pulando.",
                target["database"],
                target["table"],
            )
            continue
        try:
            total += _load_one(target)
        except Exception:
            falhas.append(f"{target['database']}.{target['table']}")
    return total, falhas


# ── Carga de uma tabela ──────────────────────────────────────────────────────


def _load_one(target: dict) -> int:
    """Recria a tabela bronze e a preenche, fatia a fatia, pelo Trino."""
    started_at = datetime.now(timezone.utc)
    dry_run = get_current_context()["params"]["dry_run"]
    t0 = time.monotonic()

    try:
        columns = _fetch_columns(target)
        if not columns:
            logging.warning(
                "[salic_trino] %s.%s sem colunas — pulando.",
                target["database"],
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
            "[salic_trino] concluído %s.%s: %d linha(s) em %d fatia(s), %.0fs "
            "(origem estimava %d).",
            bronze_schema(target),
            target["bronze_table"],
            rows,
            len(statements),
            time.monotonic() - t0,
            target["row_count"],
        )
        _warn_on_divergence(target, rows)
        write_log(
            target, "success", _stats(started_at, rows=rows, slices=len(statements))
        )
        return rows

    except Exception as exc:
        logging.error(
            "[salic_trino] ERRO em %s.%s (catálogo=%s, %.0fs) %s: %s",
            target["database"],
            target["table"],
            target["catalog"],
            time.monotonic() - t0,
            type(exc).__name__,
            exc,
        )
        write_log(target, "error", _stats(started_at, error=traceback.format_exc()))
        raise


# ── Planejamento ─────────────────────────────────────────────────────────────


def _plan_source(
    source: dict,
    done: set[tuple[str, str]],
    only: set[tuple[str, str]],
    destino: dict,
) -> list[dict]:
    """Descobre as tabelas de um banco e anexa contagem e chave a cada uma."""
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

    counts = index_row_counts(source_metadata(catalog, schema, TSQL_ROW_COUNTS))
    keys = pick_key_columns(source_metadata(catalog, schema, TSQL_KEY_COLUMNS))

    targets = []
    for table_schema, table in rows:
        if not _wanted(source, table, done, only):
            continue
        targets.append(
            {
                "catalog": catalog,
                **destino,
                "database": source["database"],
                "schema": table_schema,
                "table": table,
                "bronze_table": bronze_table_name(source["database"], table),
                "row_count": counts.get(metadata_key(table_schema, table), 0),
                "key_column": keys.get(metadata_key(table_schema, table)),
                "rows_per_slice": source["rows_per_slice"],
                "slice_concurrency": source["slice_concurrency"],
            }
        )
    logging.info(
        "[salic_trino] %s: %d tabela(s) a carregar (%d com chave para fatiar).",
        catalog,
        len(targets),
        sum(1 for t in targets if t["key_column"]),
    )
    return targets


def _distribuir_em_lotes(targets: list[dict]) -> list[list[dict]]:
    """Agrupa as tabelas em no máximo ``_MAX_MAPPED_TASKS`` lotes.

    Distribui em round-robin, e não em fatias contíguas, porque `targets` chega
    ordenado da maior para a menor: em blocos contíguos o primeiro lote levaria
    todas as tabelas gigantes e os últimos ficariam ociosos.
    """
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
    """Aplica exclude_tables, o parâmetro only_tables e a retomada do dia."""
    if table.lower() in source["exclude_tables"]:
        return False
    if only and (source["database"].lower(), table.lower()) not in only:
        return False
    return (source["database"], table) not in done


def _fetch_columns(target: dict) -> list[tuple[str, str]]:
    """Colunas da tabela de origem, na ordem de definição, com o tipo do Trino."""
    rows = trino_records(
        f"""
        SELECT column_name, data_type
        FROM {target['catalog']}.information_schema.columns
        WHERE table_schema = {sql_literal(target['schema'])}
          AND table_name = {sql_literal(target['table'])}
        ORDER BY ordinal_position
        """
    )
    return [(r[0], r[1]) for r in rows]


def _slices_for(target: dict) -> list[tuple[int | None, int | None]]:
    """Faixas de chave desta tabela, consultando min/max só quando compensa."""
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
        "[salic_trino] %s.%s: chave %s em [%s, %s], ~%d linhas → %d fatia(s).",
        target["database"],
        target["table"],
        key,
        key_min,
        key_max,
        target["row_count"],
        len(slices),
    )
    return slices


# ── Execução ─────────────────────────────────────────────────────────────────


def _recreate_bronze_table(target: dict, columns: list[tuple[str, str]]) -> None:
    """DROP + CREATE da tabela bronze, pelo Trino."""
    drop, create = bronze_ddl(target, columns)
    trino_run(drop)
    trino_run(create)
    logging.info(
        "[salic_trino] %s.%s recriada com %d coluna(s) + %s.",
        bronze_schema(target),
        target["bronze_table"],
        len(columns),
        SLICE_COLUMN,
    )


def _run_statements(target: dict, statements: list[dict]) -> int:
    """Roda as faixas, até ``slice_concurrency`` de cada vez.

    As conexões são todas abertas **aqui, na thread principal**, e entregues
    prontas às threads. Isso não é estilo: no Airflow 3 a resolução de uma
    Connection passa pelo canal de comunicação da task, que só existe na thread
    principal — chamar ``TrinoHook(...)`` dentro de uma thread falha com
    ``The conn_id 'trino_default' isn't defined``, e o retry por fatia mascara
    isso como um aviso enquanto a carga vem pela metade.

    Cada thread recebe a sua própria conexão e um lote de faixas; nenhuma
    conexão é compartilhada entre threads.
    """
    if len(statements) == 1:
        with closing(_trino_conn()) as conn:
            return _run_slice(conn, target, statements[0])

    workers = min(target["slice_concurrency"], len(statements))
    lotes = [statements[i::workers] for i in range(workers)]
    lotes = [lote for lote in lotes if lote]
    logging.info(
        "[salic_trino] %s.%s: %d fatia(s) em %d thread(s) paralela(s).",
        target["database"],
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
    """Roda um lote de faixas em sequência, numa conexão só."""
    return sum(_run_slice(conn, target, st) for st in statements)


def _run_slice(conn: Any, target: dict, statement: dict) -> int:
    """Executa uma faixa, repetindo-a sozinha se a conexão cair.

    A repetição é o ponto: com a VPN instável, uma faixa perdida no meio de uma
    tabela de 200 GB não pode custar a recópia dos outros 199.
    """
    label = f"{target['database']}.{target['table']} fatia {statement['index']}"
    for attempt in range(1, _SLICE_RETRIES + 2):
        t0 = time.monotonic()
        try:
            if attempt > 1:
                _delete_slice_range(conn, target, statement)
            rows = _insert_rows(conn, statement["insert"])
            logging.info(
                "[salic_trino] %s %s: %d linha(s) em %.0fs.",
                label,
                statement["range"],
                rows,
                time.monotonic() - t0,
            )
            return rows
        except Exception as exc:
            if attempt > _SLICE_RETRIES:
                logging.error("[salic_trino] %s esgotou as tentativas.", label)
                raise
            logging.warning(
                "[salic_trino] %s falhou após %.0fs na tentativa %d/%d (%s: %s). "
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
    """Executa um INSERT no Trino e devolve quantas linhas ele escreveu.

    O Trino responde a um INSERT com uma linha única contendo a contagem — é de
    onde sai o número, sem custar um ``count(*)`` na bronze.
    """
    cursor = conn.cursor()
    cursor.execute(sql)
    records = cursor.fetchall()
    if records and records[0] and records[0][0] is not None:
        return int(records[0][0])
    return 0


def _delete_slice_range(conn: Any, target: dict, statement: dict) -> None:
    """Apaga o que a faixa possa ter escrito antes de falhar.

    Recorta pela coluna técnica ``_fatia``, não pela chave da tabela: é o único
    predicado que o conector empurra para o Postgres. Ver
    ``trino_bronze.SLICE_COLUMN``.
    """
    cursor = conn.cursor()
    cursor.execute(statement["delete"])
    apagadas = cursor.fetchall()
    logging.info(
        "[salic_trino] %s.%s fatia %d: %s linha(s) parciais apagadas.",
        target["database"],
        target["table"],
        statement["index"],
        apagadas[0][0] if apagadas and apagadas[0] else "?",
    )


# ── Relato ───────────────────────────────────────────────────────────────────


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


def _warn_on_divergence(target: dict, rows: int) -> None:
    """Compara o carregado com a estimativa da origem.

    ``sys.partitions`` é aproximado e a origem é um banco vivo, então diferença
    pequena é esperada. Acima de 1% vale olhar — costuma ser fatia que não
    cobriu o domínio inteiro da chave.
    """
    expected = target["row_count"]
    if expected <= 0:
        return
    drift = abs(rows - expected) / expected
    if drift > 0.01:
        logging.warning(
            "[salic_trino] %s.%s: carregou %d linha(s), origem estimava %d "
            "(%.1f%% de diferença).",
            target["database"],
            target["table"],
            rows,
            expected,
            drift * 100,
        )


def _log_dry_run(
    target: dict, columns: list[tuple[str, str]], statements: list[dict]
) -> None:
    logging.info(
        "[salic_trino] DRY RUN %s.%s → %s.%s\n"
        "  colunas : %d\n"
        "  chave   : %s\n"
        "  linhas  : %d (estimativa da origem)\n"
        "  fatias  : %d\n"
        "  primeira:\n%s",
        target["database"],
        target["table"],
        bronze_schema(target),
        target["bronze_table"],
        len(columns),
        target["key_column"] or "(nenhuma — carga em uma vez só)",
        target["row_count"],
        len(statements),
        statements[0]["insert"],
    )


salic_ingestion_trino()
