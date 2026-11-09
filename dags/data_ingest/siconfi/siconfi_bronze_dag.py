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

Uma página só entra inteira e coerente: com todas as partes, e com as partes
vindas da mesma tentativa da ingestão (``siconfi_bronze.conferir_pagina``). A
que não passa fica de fora, com aviso, e é tentada de novo no run seguinte.

**Conexões e threads.** As páginas de uma entidade são repartidas em até
``paralelo`` lotes, um por thread. No Airflow 3 a Connection só se resolve na
thread principal da task, então as conexões com o Trino são abertas aqui, antes
das threads, uma para cada lote, e fechadas no fim; nenhuma thread cria
``TrinoHook``. É o mesmo arranjo da ``salic_ingestion_trino`` e da
``mapas_ingestion_trino``. O MinIO é lido pelo client do ``S3Hook`` em cache,
criado na thread principal pelo ``raw_keys``.

Agendamento: ``@daily`` por padrão (Variable ``dynamic_schedules``). A carga
inicial da MSC orçamentária é grande (mais de 170 milhões de itens no recorte
do Eixo 1, a cerca de 4.500 linhas/s por fila no Trino local), então os
primeiros runs param no tempo e o seguinte continua.
"""

import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
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
# Páginas registradas em ``paginas`` por INSERT, em cada thread: menos comandos,
# e no máximo este tanto de páginas por thread carregadas de novo se a task cair.
_REGISTRO_EM_LOTE = 50
_MAX_RUN_MINUTES = 110

default_args = {"owner": "MinC", "retries": 2, "retry_delay": timedelta(minutes=10)}


def _alvo() -> tuple[str, str]:
    """Catálogo e schema da bronze, das Variables."""
    return (
        Variable.get(TARGET_CATALOG_VAR, default=DEFAULT_CATALOG),
        Variable.get(bronze.VARIABLE_SCHEMA, default=bronze.SCHEMA_PADRAO),
    )


def _trino_conn() -> Any:
    """Abre uma conexão DBAPI com o Trino. Só pode ser chamada na thread principal."""
    from airflow.providers.trino.hooks.trino import TrinoHook

    return TrinoHook(trino_conn_id=TRINO_CONN_ID).get_conn()


def _carregar_endpoint(
    endpoint: str, prazo_s: float, paralelo: int, carga_run_id: str
) -> dict[str, Any]:
    catalogo, schema = _alvo()
    executor = TrinoExecutor(TRINO_CONN_ID)  # só na thread principal
    destino = bronze.Destino(executor, catalogo, schema, entidade(endpoint))

    # O ``raw_keys`` também cria, aqui na thread principal, o S3Hook em cache que
    # as threads usam para ler o raw.
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
        "inconsistentes": 0,
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
    if pendentes:
        # As colunas que já existem são lidas aqui; nas threads só se acrescenta.
        destino.colunas()
        carga = _Carga(
            endpoint, paginas, destino, carga_run_id, time.monotonic() + prazo_s
        )
        _carregar_pendentes(pendentes, carga, paralelo, resumo)
    resumo["restantes"] = resumo["pendentes"] - resumo["carregadas"]
    logger.info("[siconfi_bronze_dag] %s", resumo)
    return resumo


@dataclass
class _Carga:
    """O que as threads de uma entidade compartilham."""

    endpoint: str
    paginas: bronze.Paginas
    destino: bronze.Destino
    carga_run_id: str
    prazo: float  # time.monotonic() em que nenhuma página nova começa
    # Coluna nova: um ALTER só, sem duas threads nele.
    trava: threading.Lock = field(default_factory=threading.Lock)
    # Uma thread que falha avisa as outras, que param na página seguinte.
    parar: threading.Event = field(default_factory=threading.Event)


def _carregar_pendentes(
    pendentes: list[str], carga: _Carga, paralelo: int, resumo: dict[str, Any]
) -> None:
    """Carrega as páginas pendentes em até ``paralelo`` threads, uma conexão cada.

    As conexões são todas abertas **aqui, na thread principal**, e entregues
    prontas às threads (ver a docstring do módulo). Cada thread percorre o seu
    lote em sequência e registra em ``paginas``, pela própria conexão, o que
    carregou.
    """
    workers = max(1, min(paralelo, len(pendentes)))
    lotes = [pendentes[i::workers] for i in range(workers)]
    conexoes = [_trino_conn() for _ in lotes]
    try:
        with ThreadPoolExecutor(max_workers=len(lotes)) as pool:
            futuros = [
                pool.submit(_carregar_lote, carga, bronze.ConexaoTrino(conexoes[i]), lote)
                for i, lote in enumerate(lotes)
            ]
            for futuro in futuros:
                for chave, valor in futuro.result().items():
                    resumo[chave] += valor
    finally:
        for conexao in conexoes:
            try:
                conexao.close()
            except Exception:
                pass


def _carregar_lote(
    carga: _Carga, trino: bronze.ConexaoTrino, lote: list[str]
) -> dict[str, int]:
    """O trabalho de uma thread: as páginas do lote, em sequência, por ``trino``."""
    feito = {"inconsistentes": 0, "carregadas": 0, "itens": 0}
    registros: list[dict[str, str | None]] = []
    try:
        for pagina in lote:
            if carga.parar.is_set() or time.monotonic() >= carga.prazo:
                break
            registro = _carregar_pagina(carga, trino, pagina)
            if registro is None:
                feito["inconsistentes"] += 1
                continue
            feito["carregadas"] += 1
            feito["itens"] += int(registro["itens"] or 0)
            registros.append(registro)
            if len(registros) >= _REGISTRO_EM_LOTE:
                _registrar(carga, trino, registros)
    except BaseException:
        carga.parar.set()
        raise
    finally:
        # A página só entra em ``paginas`` depois dos itens; o que foi
        # carregado antes de uma falha fica registrado mesmo assim.
        if registros:
            _registrar(carga, trino, registros)
    return feito


def _carregar_pagina(
    carga: _Carga, trino: bronze.ConexaoTrino, pagina: str
) -> dict[str, str | None] | None:
    """Carrega os itens de uma página e devolve a linha dela em ``paginas``.

    Devolve ``None``, sem carregar nada, quando as partes não fecham
    (``bronze.conferir_pagina``): a página fica para o run seguinte.
    """
    arquivos = carga.paginas.completas[pagina]
    lida = bronze.juntar_partes(
        pagina, arquivos, [datalakehouse.ler_texto(k) for k in arquivos]
    )
    bronze.conferir_endpoint(lida, carga.endpoint)
    problemas = bronze.conferir_pagina(lida)
    if problemas:
        logger.warning(
            "[siconfi_bronze_dag] %s: página fica para o próximo run (%s): %s",
            carga.endpoint,
            "; ".join(problemas),
            pagina,
        )
        return None
    with carga.trava:
        carga.destino.acrescentar(bronze.campos(lida.itens), trino)
    bronze.carregar_pagina(carga.destino, lida, trino)
    return bronze.registro_da_pagina(lida, carga.carga_run_id, datetime.now(timezone.utc))


def _registrar(
    carga: _Carga, trino: bronze.ConexaoTrino, registros: list[dict[str, str | None]]
) -> None:
    for comando in bronze.inserts_paginas(
        carga.destino.catalogo, carga.destino.schema, registros
    ):
        trino.run(comando)
    registros.clear()


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
