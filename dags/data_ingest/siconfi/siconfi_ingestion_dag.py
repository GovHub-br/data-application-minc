"""Ingestão retomável dos nove endpoints da API pública SICONFI.

O escopo é deliberadamente dirigido pela Variable ``siconfi_config``. O
padrão cobre só o exercício corrente e todos os demonstrativos. Para um
backfill, estreite o recorte antes de ampliar os anos: a API aceita 1 req/s e
cada combinação de parâmetros é uma requisição — a MSC completa de um ente
num ano são 312. Chaves de recorte (ausente ou ``null`` = sem filtro):

- ``fact_endpoints``: quais de ``rreo``, ``rgf``, ``dca`` e ``msc_*`` buscar;
- ``rgf_poderes``: E, L, J, M, D;
- ``msc_months``, ``msc_classes``, ``msc_value_types``, ``msc_matrix_types``.

Exemplo — o saldo final de dezembro da execução orçamentária, mais o DCA::

    {"start_year": 2019, "end_year": 2025,
     "fact_endpoints": ["msc_orcamentaria", "dca"],
     "msc_months": [12], "msc_classes": [6],
     "msc_value_types": ["ending_balance"], "msc_matrix_types": ["MSCC"]}

O recorte vale também para o que já está na fila: estreitá-lo para de buscar o
que ficou de fora, sem apagar nada. Cada task de ingestão trabalha até
``max_run_minutes`` e devolve à fila o que não processou.
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta
from typing import Any

from airflow.sdk import Variable, dag, get_current_context, task

from cliente_siconfi import (
    FACT_ENDPOINTS,
    PlanScope,
    SiconfiClient,
    SiconfiPermanentError,
    SiconfiRetryableError,
    work_units,
)
from postgres_helpers import get_postgres_conn
from schedule_loader import get_dynamic_schedule
from siconfi_storage import CLAIM_LEASE_MINUTES, SiconfiStorage

logger = logging.getLogger(__name__)

# Não há pool do Airflow aqui de propósito: o contrato de 1 req/s é garantido
# pelo SiconfiRateLimiter, que coordena via PostgreSQL e por isso vale entre
# tasks, workers e DAGs. Um pool seria redundante — e, se não existisse na
# instância, o scheduler deixaria as tasks em `scheduled` para sempre, sem log.
_DEFAULT_CONFIG: dict[str, Any] = {
    "start_year": datetime.now().year,
    "end_year": datetime.now().year,
    "entity_ids": [],
    "page_limit": 5000,
    # O limite real de cada task é o tempo; este teto só evita reservar demais.
    "max_work_units_per_run": 2000,
    "max_run_minutes": 45,
    "reference_refresh_hours": 168,
    # Linhas do extrato lidas por execução do planejamento, a partir de onde
    # a anterior parou.
    "max_manifest_rows_for_planning": 100000,
    "fact_endpoints": None,
    "rgf_poderes": None,
    "msc_months": None,
    "msc_classes": None,
    "msc_value_types": None,
    "msc_matrix_types": None,
}
_DEFAULT_ARGS = {"owner": "MinC", "retries": 2, "retry_delay": timedelta(minutes=10)}
# Unidades de trabalho acumuladas antes de cada gravação em lote no planejamento.
_PLAN_FLUSH_SIZE = 5000
# Linhas do extrato lidas do banco de cada vez no planejamento.
_PLAN_READ_BATCH = 10000


def _config() -> dict[str, Any]:
    configured = Variable.get("siconfi_config", default={}, deserialize_json=True)
    config = {**_DEFAULT_CONFIG, **configured}
    if int(config["start_year"]) > int(config["end_year"]):
        raise ValueError("siconfi_config: start_year não pode ser maior que end_year")
    if int(config["page_limit"]) < 1 or int(config["max_work_units_per_run"]) < 1:
        raise ValueError(
            "siconfi_config: page_limit e max_work_units_per_run devem ser positivos"
        )
    if not 0 < float(config["max_run_minutes"]) < CLAIM_LEASE_MINUTES:
        raise ValueError(
            f"siconfi_config: max_run_minutes deve ficar entre 0 e {CLAIM_LEASE_MINUTES}"
        )
    PlanScope.from_config(config)  # valida as chaves de recorte
    return config


def _run_id() -> str:
    return str(get_current_context()["run_id"])


def _storage(conn_str: str) -> SiconfiStorage:
    storage = SiconfiStorage(conn_str)
    storage.ensure_tables()
    return storage


def _refresh_reference(
    conn_str: str, config: dict[str, Any], run_id: str
) -> dict[str, int]:
    storage = _storage(conn_str)
    client = SiconfiClient(conn_str, page_limit=int(config["page_limit"]))
    result: dict[str, int] = {}
    try:
        for endpoint in ("anexos-relatorios", "entes"):
            if not storage.reference_due(
                endpoint, int(config["reference_refresh_hours"])
            ):
                result[endpoint] = 0
                continue
            result[endpoint] = sum(
                storage.persist_page(page, run_id)
                for page in client.iter_pages(endpoint, {})
            )
            storage.mark_reference_refreshed(endpoint)
    finally:
        client.close()
    return result


def _plan_extrato(conn_str: str, config: dict[str, Any]) -> int:
    storage = _storage(conn_str)
    entity_ids = [int(value) for value in config["entity_ids"]] or storage.entity_ids()
    if not entity_ids:
        raise ValueError(
            "Nenhum ente disponível; aguarde a carga de /entes " "ou configure entity_ids"
        )
    years = range(int(config["start_year"]), int(config["end_year"]) + 1)
    created = storage.enqueue_many(
        "extrato_entregas",
        (
            ({"id_ente": entity_id, "an_referencia": year}, None)
            for entity_id in entity_ids
            for year in years
        ),
    )
    logger.info(
        "[siconfi] %s unidades de extrato novas, %s entes", created, len(entity_ids)
    )
    return created


def _plan_facts(conn_str: str, config: dict[str, Any]) -> dict[str, int]:
    scope = PlanScope.from_config(config)
    storage = _storage(conn_str)
    created = {endpoint: 0 for endpoint in FACT_ENDPOINTS}
    # As unidades vão para o banco em lotes: um extrato nacional completo
    # planeja centenas de milhares delas, e uma conexão por unidade fazia
    # esta task nunca terminar contra um Postgres remoto.
    buffers: dict[str, list[tuple[dict[str, Any], str | None]]] = {
        endpoint: [] for endpoint in FACT_ENDPOINTS
    }

    def flush(endpoint: str) -> None:
        if buffers[endpoint]:
            created[endpoint] += storage.enqueue_many(endpoint, buffers[endpoint])
            buffers[endpoint].clear()

    # O extrato é lido a partir de onde a execução anterior parou. Antes, só
    # as 100 mil linhas mais recentes eram lidas, e num backfill nacional a
    # maior parte do extrato nunca era planejada. Mudar o recorte zera a marca
    # e relê tudo.
    fingerprint = scope.fingerprint()
    after = storage.plan_watermark("extrato_entregas", fingerprint)
    budget = int(config["max_manifest_rows_for_planning"])
    read = 0
    while read < budget:
        batch = storage.payloads_since(
            "extrato_entregas", after, min(_PLAN_READ_BATCH, budget - read)
        )
        if not batch:
            break
        for _, item in batch:
            for endpoint, params, revision in work_units(item):
                if scope.allows(endpoint, params):
                    buffers[endpoint].append((params, revision))
                    if len(buffers[endpoint]) >= _PLAN_FLUSH_SIZE:
                        flush(endpoint)
        for endpoint in FACT_ENDPOINTS:
            flush(endpoint)
        after = batch[-1][0]
        storage.save_plan_watermark("extrato_entregas", after, fingerprint)
        read += len(batch)
    logger.info("[siconfi] %s linhas do extrato lidas; unidades novas: %s", read, created)
    return created


def _ingest_work(
    endpoint: str, conn_str: str, config: dict[str, Any], run_id: str
) -> dict[str, int]:
    scope = PlanScope.from_config(config)
    summary = dict.fromkeys(
        ("claimed", "success", "no_data", "retry", "permanent_error", "released", "rows"),
        0,
    )
    if not scope.enabled(endpoint):
        logger.info("[siconfi] %s fora de fact_endpoints; nada a fazer", endpoint)
        return summary
    storage = _storage(conn_str)
    claimed = storage.claim(
        endpoint,
        int(config["max_work_units_per_run"]),
        run_id,
        param_filter=scope.constraints(endpoint),
    )
    summary["claimed"] = len(claimed)
    deadline = time.monotonic() + 60 * float(config["max_run_minutes"])
    done = 0
    client = SiconfiClient(conn_str, page_limit=int(config["page_limit"]))
    try:
        for unit in claimed:
            if time.monotonic() >= deadline:
                break
            try:
                rows = sum(
                    storage.persist_page(page, run_id)
                    for page in client.iter_pages(endpoint, unit["params"])
                )
                status, error = ("success" if rows else "no_data"), None
                summary["rows"] += rows
            except SiconfiRetryableError as exc:
                status, error = "retry", str(exc)
                logger.warning("[siconfi] %s será repetido: %s", endpoint, exc)
            except SiconfiPermanentError as exc:
                status = "no_data" if exc.status_code == 404 else "permanent_error"
                error = str(exc)
                logger.warning("[siconfi] %s não recuperável: %s", endpoint, exc)
            storage.complete(endpoint, unit["work_key"], status, error)
            summary[status] += 1
            done += 1
    finally:
        client.close()
        # Por tempo esgotado ou por erro inesperado: o que não foi processado
        # volta para a fila agora, não quando o lease vencer.
        remaining = [unit["work_key"] for unit in claimed[done:]]
        storage.release(endpoint, remaining, run_id)
        summary["released"] = len(remaining)
    logger.info("[siconfi] %s: %s", endpoint, summary)
    return summary


@dag(
    dag_id="siconfi_ingestion_dag",
    schedule=get_dynamic_schedule("siconfi_ingestion_dag", default="@hourly"),
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=_DEFAULT_ARGS,
    tags=["minc", "siconfi", "tesouro", "raw", "bronze"],
)
def siconfi_ingestion_dag() -> None:
    @task
    def refresh_reference_data() -> dict[str, int]:
        return _refresh_reference(get_postgres_conn(), _config(), _run_id())

    @task
    def plan_extrato() -> int:
        return _plan_extrato(get_postgres_conn(), _config())

    @task
    def plan_facts() -> dict[str, int]:
        return _plan_facts(get_postgres_conn(), _config())

    @task
    def ingest(endpoint: str) -> dict[str, int]:
        return _ingest_work(endpoint, get_postgres_conn(), _config(), _run_id())

    planned_facts = plan_facts()
    (
        refresh_reference_data()
        >> plan_extrato()
        >> ingest.override(task_id="ingest_extrato")("extrato_entregas")
        >> planned_facts
    )
    for endpoint in FACT_ENDPOINTS:
        planned_facts >> ingest.override(task_id=f"ingest_{endpoint}")(endpoint)


siconfi_ingestion_dag()
