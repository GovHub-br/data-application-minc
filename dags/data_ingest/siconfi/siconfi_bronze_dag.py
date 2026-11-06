"""Carga da bronze do SICONFI: do raw no MinIO para o Postgres, pelo Trino.

A ingestão (``siconfi_ingestion_dag``) e a conversão das páginas antigas
(``siconfi_migracao_raw_pages_dag``) gravam uma página da API por arquivo em
``raw/siconfi/<entidade>/``. Esta DAG põe os itens em
``<catálogo>.siconfi.<entidade>`` e o envelope de cada página em
``<catálogo>.siconfi.paginas``, as tabelas que o dbt copia em
``siconfi/bronze_*``. O Airflow lê o raw e fala só com o Trino; a regra da
cópia fiel está em ``siconfi_bronze``.

Cada run compara o raw inteiro de cada entidade com o que já foi carregado
(as páginas em ``siconfi.paginas``) e carrega as que faltam, até
``max_run_minutes``. Por isso não depende do Asset da ingestão: as pastas da
conversão não emitem evento, e uma página que ficou para trás entra no run
seguinte. Reexecutar não duplica página já registrada.

Agendamento: ``@daily`` por padrão (Variable ``dynamic_schedules``). A carga
inicial da MSC orçamentária é grande (mais de 100 milhões de itens no recorte
do Eixo 1, a cerca de 4.500 linhas/s por fila no Trino local), então os
primeiros runs param no tempo e o seguinte continua.
"""

import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from typing import Any

from airflow.sdk import Param, Variable, dag, get_current_context, task

import datalakehouse
import siconfi_bronze as bronze
from siconfi_controle import (
    DEFAULT_CATALOG,
    TARGET_CATALOG_VAR,
    TRINO_CONN_ID,
    TrinoExecutor,
)
from siconfi_storage import VALID_ENDPOINTS, entidade, raw_keys
from schedule_loader import get_dynamic_schedule

logger = logging.getLogger(__name__)

# As de referência primeiro: são pequenas e o planejamento depende delas.
_ENDPOINTS = (
    "entes",
    "anexos-relatorios",
    "extrato_entregas",
    "rreo",
    "msc_orcamentaria",
    "dca",
    "rgf",
    "msc_controle",
    "msc_patrimonial",
)
assert set(_ENDPOINTS) == VALID_ENDPOINTS
# Páginas registradas em ``paginas`` por INSERT: menos comandos, e no máximo
# este tanto de páginas carregadas de novo se a task cair.
_REGISTRO_EM_LOTE = 50
_MAX_RUN_MINUTES = 110

default_args = {"owner": "MinC", "retries": 2, "retry_delay": timedelta(minutes=10)}


def _alvo() -> tuple[str, str]:
    """Catálogo e schema da bronze, das Variables."""
    return (
        Variable.get(TARGET_CATALOG_VAR, default=DEFAULT_CATALOG),
        Variable.get(bronze.VARIABLE_SCHEMA, default=bronze.SCHEMA_PADRAO),
    )


def _carregar_endpoint(
    endpoint: str, prazo_s: float, paralelo: int, carga_run_id: str
) -> dict[str, Any]:
    catalogo, schema = _alvo()
    executor = TrinoExecutor(TRINO_CONN_ID)
    destino = bronze.Destino(executor, catalogo, schema, entidade(endpoint))

    paginas = bronze.agrupar_paginas(raw_keys(endpoint))
    feitas = {
        str(linha[0])
        for linha in executor.records(
            bronze.sql_paginas_carregadas(catalogo, schema, endpoint)
        )
    }
    pendentes = [p for p in paginas.completas if p not in feitas]
    resumo: dict[str, Any] = {
        "endpoint": endpoint,
        "paginas_no_raw": len(paginas.completas),
        "ja_carregadas": len(feitas),
        "pendentes": len(pendentes),
        "incompletas": len(paginas.incompletas),
        "ambiguas": len(paginas.ambiguas),
        "carregadas": 0,
        "itens": 0,
    }
    if paginas.ambiguas:
        logger.warning(
            "[siconfi_bronze_dag] %s: %d páginas inteiras e divididas ao mesmo "
            "tempo ficaram de fora (ex.: %s)",
            endpoint,
            len(paginas.ambiguas),
            sorted(paginas.ambiguas)[0],
        )

    prazo = time.monotonic() + prazo_s
    trava = threading.Lock()
    registros: list[dict[str, str | None]] = []

    def registrar(forcar: bool = False) -> None:
        with trava:
            if not registros or (len(registros) < _REGISTRO_EM_LOTE and not forcar):
                return
            lote = list(registros)
            registros.clear()
        for comando in bronze.inserts_paginas(catalogo, schema, lote):
            executor.run(comando)

    def uma(pagina: str) -> int:
        if time.monotonic() >= prazo:
            return -1
        arquivos = paginas.completas[pagina]
        lida = bronze.juntar_partes(
            pagina, arquivos, [datalakehouse.ler_texto(k) for k in arquivos]
        )
        bronze.conferir_endpoint(lida, endpoint)
        with trava:
            # Coluna nova cria-se uma vez, sem duas threads no mesmo ALTER.
            destino.acrescentar(bronze.campos(lida.itens))
        itens = bronze.carregar_pagina(destino, lida)
        registro = bronze.registro_da_pagina(
            lida, carga_run_id, datetime.now(timezone.utc)
        )
        with trava:
            registros.append(registro)
        registrar()
        return itens

    try:
        with ThreadPoolExecutor(max_workers=paralelo) as pool:
            futuros = [pool.submit(uma, p) for p in pendentes]
            for futuro in as_completed(futuros):
                itens = futuro.result()
                if itens >= 0:
                    resumo["carregadas"] += 1
                    resumo["itens"] += itens
    finally:
        registrar(forcar=True)
    resumo["restantes"] = resumo["pendentes"] - resumo["carregadas"]
    logger.info("[siconfi_bronze_dag] %s", resumo)
    return resumo


@dag(
    dag_id="siconfi_bronze_dag",
    schedule=get_dynamic_schedule("siconfi_bronze_dag", default="@daily"),
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    # Duas entidades por vez, cada uma com ``paralelo`` INSERTs simultâneos.
    max_active_tasks=2,
    default_args=default_args,
    params={
        "endpoints": Param(
            list(_ENDPOINTS),
            type="array",
            items={"type": "string", "enum": list(_ENDPOINTS)},
            title="Entidades a carregar",
        ),
        "max_run_minutes": Param(
            50,
            type="integer",
            minimum=1,
            maximum=_MAX_RUN_MINUTES,
            title="Minutos por entidade antes de parar",
        ),
        "paralelo": Param(
            4, type="integer", minimum=1, maximum=8, title="INSERTs simultâneos"
        ),
    },
    tags=["minc", "siconfi", "tesouro", "datalakehouse", "bronze", "trino"],
)
def siconfi_bronze_dag() -> None:
    """Carrega o raw do SICONFI no MinIO para a bronze, pelo Trino."""

    @task
    def preparar() -> list[str]:
        """Cria as tabelas da bronze e a de páginas, uma vez por run."""
        catalogo, schema = _alvo()
        executor = TrinoExecutor(TRINO_CONN_ID)
        for endpoint in _ENDPOINTS:
            bronze.Destino(executor, catalogo, schema, entidade(endpoint)).garantir()
        executor.run(bronze.ddl_paginas(catalogo, schema))
        pedidos: list[str] = get_current_context()["params"]["endpoints"]
        return [e for e in _ENDPOINTS if e in pedidos]

    @task(execution_timeout=timedelta(hours=2))
    def carregar(endpoint: str) -> dict[str, Any]:
        contexto = get_current_context()
        params = contexto["params"]
        return _carregar_endpoint(
            endpoint,
            prazo_s=60 * min(float(params["max_run_minutes"]), _MAX_RUN_MINUTES),
            paralelo=int(params["paralelo"]),
            carga_run_id=str(contexto["dag_run"].run_id),
        )

    carregar.expand(endpoint=preparar())


siconfi_bronze_dag()
