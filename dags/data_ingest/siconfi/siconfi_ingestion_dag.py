"""Ingestão retomável da API de Dados Abertos do SICONFI para o datalake de análise.

Endpoints: ``/anexos-relatorios``, ``/entes``, ``/extrato_entregas``, ``/rreo``,
``/rgf``, ``/dca`` e ``/msc_orcamentaria`` (classes 5 e 6). ``/msc_patrimonial``
e ``/msc_controle`` continuam suportados, desligados por padrão.

O que se extrai não está no código: vem da Variable
``siconfi_extracao_config``, com precedência
``dag_run.conf > Variable > padrões`` — ver ``siconfi_config`` e o README desta
pasta. Exemplo de backfill pontual pelo ``dag_run.conf``::

    {"global": {"incluir_cod_ibge": [32]},
     "endpoints": {"dca": {"ano_inicio": 2017, "ano_fim": 2024}}}

O extrato de entregas decide o que se busca de cada ente e ano: RREO normal
ou simplificado, RGF quadrimestral ou semestral e de quais poderes, meses de
MSC entregues, DCA entregue ou não. Sem ele a DAG montaria combinações que a
API responde com lista vazia, sem erro.

O extrato dos últimos ``rebusca_anos`` exercícios volta à fila a cada
``rebusca_dias``: é assim que uma retificação chega, porque o demonstrativo só
é rebuscado quando a linha dele no extrato muda.
"""

import json
import logging
import time
from datetime import datetime, timedelta
from typing import Any, Iterable, Iterator

from airflow.sdk import Variable, dag, get_current_context, task

from cliente_siconfi import (
    ESFERAS,
    FACT_ENDPOINTS,
    REFERENCE_ENDPOINTS,
    PlanScope,
    SiconfiClient,
    SiconfiPage,
    SiconfiParametroInvalido,
    SiconfiRequestError,
    esfera_do_ente,
    plan_units,
    validate_params,
)
from postgres_helpers import get_postgres_conn
from schedule_loader import get_dynamic_schedule
from siconfi_config import (
    STATUS_PARTICAO,
    VARIABLE_NAME,
    carregar,
    config_da_chave,
    validar_anexos,
)
from siconfi_storage import QUEUE_STATUS, SiconfiStorage, classify_outcome

default_args = {
    "owner": "Wallyson Souza",
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
}

# Não há pool do Airflow aqui de propósito: o intervalo entre chamadas e o
# teto de chamadas simultâneas são garantidos pelo SiconfiRateLimiter, que
# coordena via PostgreSQL e por isso vale entre tasks, workers e DAGs. Um pool
# seria redundante — e, se não existisse na instância, o scheduler deixaria as
# tasks em `scheduled` para sempre, sem log.

# Unidades de trabalho acumuladas antes de cada gravação em lote no planejamento.
_PLAN_FLUSH_SIZE = 5000
# Extratos (um por ente e ano) lidos do banco de cada vez no planejamento.
_PLAN_READ_BATCH = 500


class _ContaPaginas:
    """Repassa as páginas de uma busca contando quantas vieram."""

    def __init__(self, pages: Iterable[SiconfiPage]) -> None:
        self._pages = pages
        self.total = 0

    def __iter__(self) -> Iterator[SiconfiPage]:
        for page in self._pages:
            self.total += 1
            yield page


def _run_id() -> str:
    return str(get_current_context()["run_id"])


def _storage(conn_str: str) -> SiconfiStorage:
    storage = SiconfiStorage(conn_str)
    storage.ensure_tables()
    return storage


def _carregar_configuracao(
    conn_str: str, variable: Any, conf: Any, run_id: str
) -> dict[str, Any]:
    config = carregar(variable, conf, ano_corrente=datetime.now().year)
    _storage(conn_str).save_run_config(run_id, config, variable, conf or None)
    logging.info(
        "[siconfi_ingestion_dag.py] configuração efetiva "
        "(Variable %s, dag_run.conf %s): %s",
        "presente" if variable is not None else "ausente, padrões do código",
        "presente" if conf else "ausente",
        json.dumps(config, ensure_ascii=False, sort_keys=True),
    )
    return config


def _entes_do_recorte(
    storage: SiconfiStorage, config: dict[str, Any]
) -> tuple[dict[int, str], frozenset[int] | None]:
    """Entes selecionados (``cod_ibge → esfera``) e o filtro que vai para o claim.

    O filtro é ``None`` quando o recorte pega todos os entes: não há por que
    mandar 5.598 códigos para cada claim.
    """
    entes = storage.ente_esferas()
    if not entes:
        raise ValueError(
            "/entes ainda não foi carregado; ative endpoints.entes em "
            f"{VARIABLE_NAME} e rode de novo"
        )
    glob = config["global"]
    incluir = set(glob["incluir_cod_ibge"])
    excluir = set(glob["excluir_cod_ibge"])
    desconhecidos = sorted(incluir - entes.keys())
    if desconhecidos:
        raise ValueError(
            f"{VARIABLE_NAME}.global.incluir_cod_ibge: {desconhecidos} "
            "não estão em /entes"
        )
    selecionados = {
        cod: esfera
        for cod, esfera in entes.items()
        if esfera in glob["esferas"]
        and (not incluir or cod in incluir)
        and cod not in excluir
    }
    if not selecionados:
        raise ValueError(
            f"{VARIABLE_NAME}.global: esferas, incluir_cod_ibge e excluir_cod_ibge "
            "não deixam nenhum ente"
        )
    restrito = bool(incluir or excluir) or set(glob["esferas"]) != set(ESFERAS)
    return selecionados, frozenset(selecionados) if restrito else None


def _reprocessar(
    storage: SiconfiStorage,
    config: dict[str, Any],
    scope: PlanScope,
    endpoints: Iterable[str],
) -> dict[str, int]:
    statuses = config["global"]["reprocessar"]
    if not statuses:
        return {}
    devolvidas = {
        endpoint: storage.requeue_status(endpoint, statuses, scope.constraints(endpoint))
        for endpoint in endpoints
        if scope.enabled(endpoint)
    }
    logging.info(
        "[siconfi_ingestion_dag.py] reprocessamento de %s: %s", statuses, devolvidas
    )
    return devolvidas


def _atualizar_referencias(
    conn_str: str, config: dict[str, Any], run_id: str
) -> dict[str, int]:
    storage = _storage(conn_str)
    client = SiconfiClient.from_config(conn_str, config)
    result: dict[str, int] = {}
    try:
        for endpoint in REFERENCE_ENDPOINTS:
            cfg = config_da_chave(config, endpoint)
            if not cfg["ativo"] or not storage.reference_due(
                endpoint, int(cfg["recarga_horas"])
            ):
                result[endpoint] = 0
                continue
            pages = _ContaPaginas(client.iter_pages(endpoint, {}))
            try:
                rows = storage.persist_fetch(endpoint, {}, pages, run_id)
            except SiconfiRequestError as exc:
                storage.log_partition(
                    run_id, endpoint, {}, "erro", page_count=pages.total, error=str(exc)
                )
                raise
            # Tabela de referência vazia nunca é normal: sem /entes não há
            # recorte, sem /anexos-relatorios não há validação de no_anexo.
            status = "sucesso_com_dados" if rows else "vazio_inesperado"
            storage.log_partition(
                run_id,
                endpoint,
                {},
                status,
                esperado=True,
                item_count=rows,
                page_count=pages.total,
            )
            if not rows:
                raise RuntimeError(f"/{endpoint} respondeu sem nenhum item")
            storage.mark_reference_refreshed(endpoint)
            result[endpoint] = rows
    finally:
        client.close()
    return result


def _montar_particoes_extrato(conn_str: str, config: dict[str, Any]) -> dict[str, int]:
    endpoint = "extrato_entregas"
    storage = _storage(conn_str)
    entes, filtro = _entes_do_recorte(storage, config)
    scope = PlanScope(config, filtro)
    if not scope.enabled(endpoint):
        logging.info("[siconfi_ingestion_dag.py] extrato_entregas desligado")
        return {}
    years = scope.years(endpoint)
    created = storage.enqueue_many(
        endpoint,
        (
            ({"id_ente": ente, "an_referencia": year}, None, None)
            for ente in sorted(entes)
            for year in years
        ),
    )
    # Uma retificação só chega se o extrato for buscado de novo. Volta para a
    # fila o extrato dos exercícios recentes, que é onde entrega e retificação
    # ainda acontecem; o planejamento dos fatos compara o revision_marker e só
    # põe de volta na fila o demonstrativo que de fato mudou.
    cfg = scope.cfg(endpoint)
    refresh_years = int(cfg["rebusca_anos"])
    min_year = max(years.start, datetime.now().year - refresh_years + 1)
    requeued = 0
    if refresh_years and min_year < years.stop:
        requeued = storage.requeue_stale(
            endpoint,
            "an_referencia",
            min_year=min_year,
            max_year=years.stop - 1,
            older_than_days=int(cfg["rebusca_dias"]),
            entity_ids=filtro,
        )
    reprocessed = _reprocessar(storage, config, scope, [endpoint]).get(endpoint, 0)
    logging.info(
        "[siconfi_ingestion_dag.py] extrato: %s partições novas, %s devolvidas para "
        "rebusca, %s reprocessadas; %s entes × %s–%s",
        created,
        requeued,
        reprocessed,
        len(entes),
        years.start,
        years.stop - 1,
    )
    return {"novas": created, "rebusca": requeued, "reprocessadas": reprocessed}


class _FilaEmLotes:
    """Acumula as partições planejadas e grava na fila em lotes.

    Um extrato nacional completo planeja centenas de milhares de partições, e
    uma conexão por partição fazia o planejamento nunca terminar contra um
    Postgres remoto.
    """

    def __init__(self, storage: SiconfiStorage) -> None:
        self.storage = storage
        self.created = dict.fromkeys(FACT_ENDPOINTS, 0)
        self._buffers: dict[str, list[tuple[dict[str, Any], str, bool]]] = {
            endpoint: [] for endpoint in FACT_ENDPOINTS
        }

    def add(self, endpoint: str, unit: tuple[dict[str, Any], str, bool]) -> None:
        self._buffers[endpoint].append(unit)
        if len(self._buffers[endpoint]) >= _PLAN_FLUSH_SIZE:
            self._flush(endpoint)

    def flush(self) -> None:
        for endpoint in FACT_ENDPOINTS:
            self._flush(endpoint)

    def _flush(self, endpoint: str) -> None:
        if self._buffers[endpoint]:
            self.created[endpoint] += self.storage.enqueue_many(
                endpoint, self._buffers[endpoint]
            )
            self._buffers[endpoint].clear()


def _planejar_do_extrato(
    storage: SiconfiStorage,
    scope: PlanScope,
    entes: dict[int, str],
    budget: int,
) -> tuple[dict[str, int], int]:
    """Lê o extrato de cada ente e ano de onde a execução anterior parou.

    Mudar o recorte zera o cursor e relê tudo — sem rebuscar nada, porque o
    que não mudou mantém o revision_marker.
    """
    fila = _FilaEmLotes(storage)
    fingerprint = scope.fingerprint()
    cursor = storage.plan_cursor("extrato_entregas", fingerprint)
    read = 0
    while read < budget:
        batch = storage.extrato_fetches_since(
            cursor, min(_PLAN_READ_BATCH, budget - read)
        )
        if not batch:
            break
        for _, _, params, items in batch:
            ente = int(params["id_ente"])
            if scope.entes is not None and ente not in scope.entes:
                continue
            esfera = entes.get(ente) or esfera_do_ente(ente)
            for endpoint, unit_params, revision, esperado in plan_units(
                items, esfera, scope
            ):
                fila.add(endpoint, (unit_params, revision, esperado))
        fila.flush()
        cursor = (batch[-1][0], batch[-1][1])
        storage.save_plan_cursor("extrato_entregas", *cursor, fingerprint)
        read += len(batch)
    return fila.created, read


def _montar_particoes_fatos(conn_str: str, config: dict[str, Any]) -> dict[str, int]:
    storage = _storage(conn_str)
    entes, filtro = _entes_do_recorte(storage, config)
    scope = PlanScope(config, filtro)
    if not any(scope.enabled(endpoint) for endpoint in FACT_ENDPOINTS):
        logging.info("[siconfi_ingestion_dag.py] nenhum demonstrativo ligado")
        return {}
    erros = validar_anexos(config, storage.latest_items("anexos-relatorios"))
    if erros:
        raise ValueError(
            f"Configuração SICONFI inválida ({VARIABLE_NAME} / dag_run.conf):\n- "
            + "\n- ".join(erros)
        )
    created, read = _planejar_do_extrato(
        storage, scope, entes, int(config["global"]["max_extratos_por_planejamento"])
    )
    reprocessed = _reprocessar(storage, config, scope, FACT_ENDPOINTS)
    logging.info(
        "[siconfi_ingestion_dag.py] %s extratos lidos; partições novas: %s; "
        "reprocessadas: %s",
        read,
        created,
        reprocessed,
    )
    return {**created, "extratos_lidos": read, "reprocessadas": sum(reprocessed.values())}


def _ingerir(
    endpoint: str, conn_str: str, config: dict[str, Any], run_id: str
) -> dict[str, int]:
    summary = dict.fromkeys(
        ("reservadas", *STATUS_PARTICAO, "devolvidas", "linhas", "paginas"), 0
    )
    storage = _storage(conn_str)
    _, filtro = _entes_do_recorte(storage, config)
    scope = PlanScope(config, filtro)
    if not scope.enabled(endpoint):
        logging.info("[siconfi_ingestion_dag.py] %s desligado; nada a fazer", endpoint)
        return summary
    glob = config["global"]
    claimed = storage.claim(
        endpoint,
        int(glob["max_particoes_por_execucao"]),
        run_id,
        param_filter=scope.constraints(endpoint),
    )
    summary["reservadas"] = len(claimed)
    max_tentativas = int(glob["max_tentativas_por_particao"])
    deadline = time.monotonic() + 60 * float(glob["max_minutos_por_execucao"])
    done = 0
    client = SiconfiClient.from_config(conn_str, config)
    try:
        for unit in claimed:
            if time.monotonic() >= deadline:
                break
            params = unit["params"]
            pages = _ContaPaginas(())
            rows, failure = 0, None
            try:
                # Antes do persist_fetch, para não abrir uma busca que nunca
                # terminaria.
                validate_params(endpoint, params)
                pages = _ContaPaginas(client.iter_pages(endpoint, params))
                rows = storage.persist_fetch(endpoint, params, pages, run_id)
            except (SiconfiParametroInvalido, SiconfiRequestError) as exc:
                failure = exc
            status = classify_outcome(
                rows, unit["esperado"], failure, unit["attempts"], max_tentativas
            )
            error = str(failure) if failure else None
            storage.complete(
                endpoint,
                unit["work_key"],
                status,
                error,
                run_id=run_id,
                item_count=rows,
                page_count=pages.total,
            )
            log_status = QUEUE_STATUS[status]
            logging.log(
                (
                    logging.INFO
                    if log_status in ("sucesso_com_dados", "vazio_esperado")
                    else logging.WARNING
                ),
                "[siconfi_ingestion_dag.py] %s %s: %s (%s linhas, %s páginas)%s",
                endpoint,
                json.dumps(params, ensure_ascii=False, sort_keys=True),
                log_status,
                rows,
                pages.total,
                f" — {error}" if error else "",
            )
            summary[log_status] += 1
            summary["linhas"] += rows
            summary["paginas"] += pages.total
            done += 1
    finally:
        client.close()
        # Por tempo esgotado ou por erro inesperado: o que não foi processado
        # volta para a fila agora, não quando o lease vencer.
        remaining = [unit["work_key"] for unit in claimed[done:]]
        storage.release(endpoint, remaining, run_id)
        summary["devolvidas"] = len(remaining)
    logging.info("[siconfi_ingestion_dag.py] %s: %s", endpoint, summary)
    return summary


@dag(
    dag_id="siconfi_ingestion_dag",
    schedule=get_dynamic_schedule("siconfi_ingestion_dag", default="@hourly"),
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["minc", "siconfi", "tesouro", "raw", "bronze"],
    doc_md=__doc__,
)
def siconfi_ingestion_dag() -> None:
    """DAG de ingestão da API pública SICONFI (Tesouro Nacional).

    Fluxo:

    1. ``carregar_configuracao`` -- lê a Variable e o ``dag_run.conf``,
       valida e grava a configuração efetiva em
       ``siconfi_control.run_config``. Nada é chamado antes disso.
    2. ``atualizar_referencias`` -- recarrega ``/anexos-relatorios`` e
       ``/entes`` quando a última carga tem mais de ``recarga_horas``.
    3. ``montar_particoes_extrato`` -- enfileira uma partição por ente x ano
       em ``siconfi_control.work_queue`` e devolve à fila o extrato recente.
    4. ``ingerir_extrato_entregas`` -- consome a fila do extrato.
    5. ``montar_particoes_fatos`` -- lê o extrato de cada ente e ano a partir
       de onde a execução anterior parou e enfileira o que foi entregue.
    6. ``ingerir_<endpoint>`` -- uma task por demonstrativo, em paralelo.

    Toda página recebida vai para ``siconfi_bronze``, marcada com o
    ``fetch_id`` da busca; quem lê a Bronze usa só a busca completa mais
    recente de cada consulta (ver ``siconfi_storage``). Cada partição
    processada fica em ``siconfi_control.partition_log`` com o status
    ``sucesso_com_dados``, ``vazio_esperado``, ``vazio_inesperado`` ou
    ``erro``.

    Toda a lógica de HTTP, de limite de requisições e de montagem das
    consultas fica em ``cliente_siconfi``; a de configuração em
    ``siconfi_config``; a de fila e persistência em ``siconfi_storage``.
    Esta DAG só orquestra.
    """

    @task
    def carregar_configuracao() -> dict[str, Any]:
        # A Variable é lida aqui, e não no topo do arquivo, para não virar uma
        # consulta ao banco a cada parse do scheduler.
        context = get_current_context()
        conf = dict(context["dag_run"].conf or {})
        variable = Variable.get(VARIABLE_NAME, default=None, deserialize_json=True)
        return _carregar_configuracao(get_postgres_conn(), variable, conf, _run_id())

    @task
    def atualizar_referencias(config: dict[str, Any]) -> dict[str, int]:
        return _atualizar_referencias(get_postgres_conn(), config, _run_id())

    @task
    def montar_particoes_extrato(config: dict[str, Any]) -> dict[str, int]:
        return _montar_particoes_extrato(get_postgres_conn(), config)

    @task
    def montar_particoes_fatos(config: dict[str, Any]) -> dict[str, int]:
        return _montar_particoes_fatos(get_postgres_conn(), config)

    @task
    def ingerir(endpoint: str, config: dict[str, Any]) -> dict[str, int]:
        return _ingerir(endpoint, get_postgres_conn(), config, _run_id())

    config = carregar_configuracao()
    planned_facts = montar_particoes_fatos(config)
    (
        atualizar_referencias(config)
        >> montar_particoes_extrato(config)
        >> ingerir.override(task_id="ingerir_extrato_entregas")(
            "extrato_entregas", config
        )
        >> planned_facts
    )
    for endpoint in FACT_ENDPOINTS:
        planned_facts >> ingerir.override(task_id=f"ingerir_{endpoint}")(endpoint, config)


siconfi_ingestion_dag()
