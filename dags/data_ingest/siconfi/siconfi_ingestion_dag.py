"""Ingestão retomável dos nove endpoints da API pública SICONFI.

O escopo é deliberadamente dirigido por ``siconfi_config``. O padrão inicia
somente no exercício corrente e processa uma quantidade limitada de unidades
por execução; para backfill nacional, configure ``start_year`` e aumente a
capacidade gradualmente, sem violar 1 req/s.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any

from airflow.sdk import Variable, dag, get_current_context, task

from cliente_siconfi import (
    SiconfiClient,
    SiconfiPermanentError,
    SiconfiRetryableError,
    work_units,
)
from postgres_helpers import get_postgres_conn
from schedule_loader import get_dynamic_schedule
from siconfi_storage import SiconfiStorage

logger = logging.getLogger(__name__)

# Não há pool do Airflow aqui de propósito: o contrato de 1 req/s é garantido
# pelo SiconfiRateLimiter, que coordena via PostgreSQL e por isso vale entre
# tasks, workers e DAGs. Um pool seria redundante — e, se não existisse na
# instância, o scheduler deixaria as tasks em `scheduled` para sempre, sem log.
_FACT_ENDPOINTS = [
    "rreo",
    "rgf",
    "dca",
    "msc_patrimonial",
    "msc_orcamentaria",
    "msc_controle",
]
_DEFAULT_CONFIG: dict[str, Any] = {
    "start_year": datetime.now().year,
    "end_year": datetime.now().year,
    "entity_ids": [],
    "page_limit": 5000,
    "max_work_units_per_run": 50,
    "reference_refresh_hours": 168,
    "max_manifest_rows_for_planning": 100000,
    "rgf_poderes": ["E", "L", "J", "M", "D"],
}
_DEFAULT_ARGS = {"owner": "MinC", "retries": 2, "retry_delay": timedelta(minutes=10)}
# Unidades de trabalho acumuladas antes de cada gravação em lote no planejamento.
_PLAN_FLUSH_SIZE = 5000


def _config() -> dict[str, Any]:
    configured = Variable.get("siconfi_config", default={}, deserialize_json=True)
    config = {**_DEFAULT_CONFIG, **configured}
    if int(config["start_year"]) > int(config["end_year"]):
        raise ValueError("siconfi_config: start_year não pode ser maior que end_year")
    if int(config["page_limit"]) < 1 or int(config["max_work_units_per_run"]) < 1:
        raise ValueError(
            "siconfi_config: page_limit e max_work_units_per_run devem ser positivos"
        )
    return config


def _run_id() -> str:
    return str(get_current_context()["run_id"])


def _ingest_work(
    endpoint: str, conn_str: str, config: dict[str, Any], run_id: str
) -> dict[str, int]:
    storage = SiconfiStorage(conn_str)
    storage.ensure_tables()
    claimed = storage.claim(endpoint, int(config["max_work_units_per_run"]), run_id)
    summary = {
        "claimed": len(claimed),
        "success": 0,
        "no_data": 0,
        "retry": 0,
        "permanent_error": 0,
        "rows": 0,
    }
    client = SiconfiClient(conn_str, page_limit=int(config["page_limit"]))
    try:
        for unit in claimed:
            try:
                rows = 0
                for page in client.iter_pages(endpoint, unit["params"]):
                    rows += storage.persist_page(page, run_id)
                storage.complete(
                    endpoint, unit["work_key"], "success" if rows else "no_data"
                )
                summary["success" if rows else "no_data"] += 1
                summary["rows"] += rows
            except SiconfiRetryableError as exc:
                storage.complete(endpoint, unit["work_key"], "retry", str(exc))
                summary["retry"] += 1
                logger.warning("[siconfi] %s será repetido: %s", endpoint, exc)
            except SiconfiPermanentError as exc:
                status = "no_data" if exc.status_code == 404 else "permanent_error"
                storage.complete(endpoint, unit["work_key"], status, str(exc))
                summary[status] += 1
                logger.warning("[siconfi] %s não recuperável: %s", endpoint, exc)
    finally:
        client.close()
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
        conn_str, config, run_id = get_postgres_conn(), _config(), _run_id()
        storage = SiconfiStorage(conn_str)
        storage.ensure_tables()
        client = SiconfiClient(conn_str, page_limit=int(config["page_limit"]))
        result: dict[str, int] = {}
        try:
            for endpoint in ("anexos-relatorios", "entes"):
                if not storage.reference_due(
                    endpoint, int(config["reference_refresh_hours"])
                ):
                    result[endpoint] = 0
                    continue
                count = 0
                for page in client.iter_pages(endpoint, {}):
                    count += storage.persist_page(page, run_id)
                storage.mark_reference_refreshed(endpoint)
                result[endpoint] = count
        finally:
            client.close()
        return result

    @task
    def plan_extrato() -> int:
        conn_str, config = get_postgres_conn(), _config()
        storage = SiconfiStorage(conn_str)
        storage.ensure_tables()
        entity_ids = [int(value) for value in config["entity_ids"]] or storage.entity_ids()
        if not entity_ids:
            raise ValueError(
                "Nenhum ente disponível; aguarde a carga de /entes ou configure entity_ids"
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
            "[siconfi] %s unidades de extrato novas, %s entes",
            created,
            len(entity_ids),
        )
        return created

    @task
    def ingest_extrato() -> dict[str, int]:
        return _ingest_work("extrato_entregas", get_postgres_conn(), _config(), _run_id())

    @task
    def plan_facts(_: dict[str, int]) -> dict[str, int]:
        conn_str, config = get_postgres_conn(), _config()
        storage = SiconfiStorage(conn_str)
        storage.ensure_tables()
        created = {endpoint: 0 for endpoint in _FACT_ENDPOINTS}
        seen: set[tuple[str, str]] = set()
        # As unidades vão para o banco em lotes: um extrato nacional completo
        # planeja centenas de milhares delas, e uma conexão por unidade fazia
        # esta task nunca terminar contra um Postgres remoto.
        buffers: dict[str, list[tuple[dict[str, Any], str | None]]] = {
            endpoint: [] for endpoint in _FACT_ENDPOINTS
        }

        def flush(endpoint: str) -> None:
            if buffers[endpoint]:
                created[endpoint] += storage.enqueue_many(endpoint, buffers[endpoint])
                buffers[endpoint].clear()

        def enqueue(endpoint: str, params: dict[str, Any], revision: str | None) -> None:
            signature = (endpoint, str(sorted(params.items())))
            if signature in seen:
                return
            seen.add(signature)
            buffers[endpoint].append((params, revision))
            if len(buffers[endpoint]) >= _PLAN_FLUSH_SIZE:
                flush(endpoint)

        for item in storage.payloads(
            "extrato_entregas", int(config["max_manifest_rows_for_planning"])
        ):
            for endpoint, params, revision in work_units(item, config["rgf_poderes"]):
                enqueue(endpoint, params, revision)
        for endpoint in _FACT_ENDPOINTS:
            flush(endpoint)
        logger.info("[siconfi] unidades de fatos planejadas: %s", created)
        return created

    def fact_task(endpoint: str):
        @task(task_id=f"ingest_{endpoint}")
        def _ingest(_: dict[str, int]) -> dict[str, int]:
            return _ingest_work(endpoint, get_postgres_conn(), _config(), _run_id())
        return _ingest

    reference = refresh_reference_data()
    planned_extrato = plan_extrato()
    reference >> planned_extrato
    extrato = ingest_extrato()
    planned_extrato >> extrato
    planned_facts = plan_facts(extrato)
    for endpoint in _FACT_ENDPOINTS:
        fact_task(endpoint)(planned_facts)


siconfi_ingestion_dag()
