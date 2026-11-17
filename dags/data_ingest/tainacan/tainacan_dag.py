"""Repositórios Tainacan do Iphan e do Ibram (eixos 3 e 5 do Cultura em Números).

Uma DAG para as quatro instâncias de ``cliente_tainacan``, uma entidade por
coleção. Segue o caminho de seeds do ADR 0012, com a coleção como período::

    /collections  ─▶ raw/<fonte>/colecoes/instancia=<chave>/<dia>/<run>.json
    /collection/{id}/items
                  ─▶ raw/<fonte>/<entidade>/<dia>/<run>/pagina_NNNN.json
                     raw/<fonte>/<entidade>/<dia>/<run>/coleta.json
                  ─▶ staging/<fonte>/<entidade>/<dia>/<run>.parquet
                  ─▶ Postgres seeds.<fonte>_<entidade>

``<dia>`` é ``ano=AAAA/mes=MM/dia=DD``. ``<fonte>`` é ``iphan`` (Bem Brasileiro,
INRC, INDL) ou ``ibram`` (Museus e Pontos de Memória). ``<entidade>`` é
``<instância>_<slug da coleção>``, como ``inrc_05_identificacao_bem_cultural``,
fixada no catálogo do cliente.

O raw de cada página é o corpo da resposta como veio. O ``coleta.json`` é
gravado por último, depois da conferência, com o ``X-WP-Total`` e a key de cada
página: a pasta de um run vale pelo ``coleta.json`` dela, e uma pasta sem ele é
de uma tentativa que não fechou.

**Carga.** Completa por coleção: cada run substitui a coleção inteira em
``seeds`` (``DELETE`` + ``COPY`` na mesma transação), então reexecutar não
duplica, e item despublicado na origem sai da tabela. O histórico fica no raw.
A conferência (``X-WP-Total``, linhas do Parquet e do banco, por coleção) vai
para ``seeds.iphan_controle_carga`` e ``seeds.ibram_controle_carga``, e a task
falha se as três não baterem.

**Catálogo.** No Iphan, coleção que a instância lista e o catálogo não tem, ou
coleção do catálogo que sumiu da listagem, faz o run falhar em
``conferir_catalogo``, depois de as outras coleções terem sido carregadas. No
Ibram só as coleções 208 e 179824 interessam, e as outras são ignoradas.

**Parâmetros.** ``instancias`` (``bcr``, ``inrc``, ``indl``, ``cadastro``) e
``colecoes`` (ids) recortam o run; vazio é o catálogo inteiro. Mensal por padrão
(Variable ``dynamic_schedules``). Conexões ``minio_datalakehouse`` e a do
Postgres de ``get_postgres_conn``.
"""

import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Any

from airflow.sdk import Param, dag, get_current_context, task
from airflow.sdk.exceptions import AirflowFailException

import cliente_tainacan as tainacan
import datalakehouse
import schemas_minc as schemas
import seeds_carga
from openmetadata.lineage import publicar_linhagem, tabela
from postgres_helpers import get_postgres_conn
from schedule_loader import get_dynamic_schedule

_LOG = "[tainacan_dag.py]"
_FONTES = sorted({i.fonte for i in tainacan.INSTANCIAS.values()})
_ASSETS_STAGING = [
    datalakehouse.asset(f"{datalakehouse.CAMADA_STAGING}/{fonte}/") for fonte in _FONTES
]

default_args = {
    "owner": "MinC",
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
}


def _colecao(alvo: dict[str, Any]) -> tainacan.Colecao:
    (colecao,) = tainacan.colecoes_selecionadas([alvo["instancia"]], [alvo["colecao_id"]])
    return colecao


def _listar(params: dict[str, Any]) -> tuple[list[dict[str, Any]], list[str]]:
    """Os alvos da extração e os problemas de catálogo, com a listagem gravada no raw."""
    escolhidas = tainacan.colecoes_selecionadas(params["instancias"], params["colecoes"])
    run_id, data = datalakehouse.run_atual()
    alvos: list[dict[str, Any]] = []
    problemas: list[str] = []
    for chave in sorted({c.instancia for c in escolhidas}):
        instancia = tainacan.INSTANCIAS[chave]
        corpo, listadas = tainacan.ClienteTainacan(instancia).colecoes()
        particao = {"instancia": chave, "ano": f"{data:%Y}", "mes": f"{data:%m}"}
        key = datalakehouse.caminho_particionado(
            datalakehouse.CAMADA_RAW,
            instancia.fonte,
            "colecoes",
            {**particao, "dia": f"{data:%d}"},
            run_id,
            "json",
        )
        datalakehouse.gravar_bytes_em_partes(corpo, key)
        erros, avisos = tainacan.conferir_catalogo(instancia, listadas)
        for aviso in avisos:
            logging.warning("%s %s", _LOG, aviso)
        problemas.extend(erros)
        listadas_ids = {int(c["id"]) for c in listadas}
        alvos.extend(
            {"instancia": c.instancia, "colecao_id": c.id}
            for c in escolhidas
            if c.instancia == chave and c.id in listadas_ids
        )
    logging.info(
        "%s %d coleções a carregar, %d problemas de catálogo",
        _LOG,
        len(alvos),
        len(problemas),
    )
    return alvos, problemas


def _extrair(alvo: dict[str, Any]) -> dict[str, Any]:
    """Pagina a coleção, grava as páginas e a coleta no raw e o Parquet no staging."""
    colecao = _colecao(alvo)
    instancia = tainacan.INSTANCIAS[colecao.instancia]
    run_id, data = datalakehouse.run_atual()
    coleta = tainacan.Coleta(colecao)
    linhas: list[dict[str, str | None]] = []
    for pagina in tainacan.ClienteTainacan(instancia).paginas(colecao.id):
        key = datalakehouse.caminho_no_run(
            datalakehouse.CAMADA_RAW,
            colecao.fonte,
            colecao.entidade,
            run_id,
            data,
            f"pagina_{pagina.numero:04d}",
        )
        datalakehouse.gravar_bytes_em_partes(pagina.corpo, key)
        itens = pagina.itens()
        coleta.registrar(pagina, itens, key)
        linhas.extend(
            tainacan.linha(item, pagina.numero, posicao)
            for posicao, item in enumerate(itens, start=1)
        )
    coleta.conferir()
    key_coleta = datalakehouse.caminho_no_run(
        datalakehouse.CAMADA_RAW, colecao.fonte, colecao.entidade, run_id, data, "coleta"
    )
    resumo = json.dumps(coleta.resumo(), ensure_ascii=False, indent=1)
    datalakehouse.gravar_bytes_em_partes(resumo.encode(), key_coleta)
    dt_ingest = datetime.now(timezone.utc)
    key_staging = datalakehouse.caminho(
        datalakehouse.CAMADA_STAGING, colecao.fonte, colecao.entidade, run_id, data
    )
    datalakehouse.gravar_parquet(linhas, key_staging, dt_ingest=dt_ingest)
    logging.info(
        "%s %s: %d itens em %d páginas",
        _LOG,
        colecao.entidade,
        len(linhas),
        len(coleta.paginas),
    )
    return {
        "fonte": colecao.fonte,
        "entidade": colecao.entidade,
        "periodo": str(colecao.id),
        "tabela": colecao.tabela,
        "arquivo": colecao.url_itens,
        "linhas_origem": coleta.total_declarado,
        "key_raw": key_coleta,
        "key_staging": key_staging,
        "dt_ingest": dt_ingest.isoformat(),
    }


@dag(
    dag_id="tainacan_dag",
    schedule=get_dynamic_schedule("tainacan_dag", default="@monthly"),
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
    params={
        "instancias": Param(
            [],
            type="array",
            items={"type": "string", "enum": sorted(tainacan.INSTANCIAS)},
            description="Instâncias a carregar. Vazio é todas.",
        ),
        "colecoes": Param(
            [],
            type="array",
            items={"type": "integer"},
            description=(
                "Ids das coleções a carregar, dentro das instâncias escolhidas. "
                "Vazio é todas as do catálogo."
            ),
        ),
    },
    tags=["minc", "iphan", "ibram", "tainacan", "datalakehouse", "seeds"],
)
def tainacan_dag() -> None:
    """Carrega as coleções Tainacan do Iphan e do Ibram no schema seeds."""

    # Devolve os alvos, que a extração mapeia; os problemas de catálogo vão na
    # XCom ``problemas``, porque o Airflow não mapeia sobre XCom de chave própria
    # (``multiple_outputs``).
    @task
    def listar_colecoes() -> list[dict[str, Any]]:
        """Lista as coleções de cada instância, grava a listagem e confere o catálogo."""
        contexto = get_current_context()
        alvos, problemas = _listar(contexto["params"])
        contexto["ti"].xcom_push(key="problemas", value=problemas)
        return alvos

    # Três coleções ao mesmo tempo, no máximo, e cada uma pagina em sequência.
    @task(max_active_tis_per_dag=3, execution_timeout=timedelta(hours=2))
    def extrair_colecao(alvo: dict[str, Any]) -> dict[str, Any]:
        """Pagina a coleção, grava cada página no raw como veio e o Parquet no staging."""
        return _extrair(alvo)

    @task(
        max_active_tis_per_dag=2,
        outlets=[
            *_ASSETS_STAGING,
            *(tabela(schemas.SCHEMA_SEEDS, c.tabela) for c in tainacan.COLECOES),
        ],
    )
    def carregar_seeds(carga: dict[str, Any]) -> dict[str, Any]:
        """Substitui a coleção inteira em ``seeds`` pelo Parquet do run."""
        df = tainacan.garantir_colunas(datalakehouse.ler_parquet(carga["key_staging"]))
        carregadas = seeds_carga.substituir_periodo(
            get_postgres_conn(),
            df,
            schemas.SCHEMA_SEEDS,
            carga["tabela"],
            tainacan.COLUNA_COLECAO,
            carga["periodo"],
        )
        return {**carga, "linhas_parquet": len(df), "linhas_carregadas": carregadas}

    @task(
        outlets=[
            tabela(schemas.SCHEMA_SEEDS, seeds_carga.tabela_controle(fonte))
            for fonte in _FONTES
        ]
    )
    def registrar_conferencia(cargas: list[dict[str, Any]]) -> None:
        """Grava a conferência por coleção e falha se origem, Parquet e banco divergem."""
        cargas = list(cargas)
        conn = get_postgres_conn()
        for fonte in sorted({c["fonte"] for c in cargas}):
            da_fonte = [c for c in cargas if c["fonte"] == fonte]
            seeds_carga.registrar_carga(conn, schemas.SCHEMA_SEEDS, fonte, da_fonte)
        seeds_carga.conferir_contagens(cargas)

    # all_done: o problema de catálogo aparece também quando a carga falhou, ou
    # quando não sobrou coleção para carregar.
    @task(trigger_rule="all_done")
    def conferir_catalogo(problemas: list[str]) -> None:
        """Falha se a listagem das instâncias e o catálogo do cliente divergiram."""
        if problemas:
            raise AirflowFailException(
                "o catálogo de cliente_tainacan precisa de atualização: "
                + " | ".join(problemas)
            )

    listagem = listar_colecoes()
    cargas = carregar_seeds.expand(carga=extrair_colecao.expand(alvo=listagem))
    conferencia = registrar_conferencia(cargas)
    conferencia >> conferir_catalogo(listagem["problemas"])
    conferencia >> publicar_linhagem()


tainacan_dag()
