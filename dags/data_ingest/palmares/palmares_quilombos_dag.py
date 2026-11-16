"""Comunidades quilombolas certificadas pela Fundação Palmares, para ``seeds``.

A Palmares publica a lista de certidões de autodefinição num Google Sheets,
atualizado todo mês (ver ``cliente_palmares``). O Eixo 3 do Cultura em
Números usa a aba ``CERTIFICADAS`` no Resultado 1.

Fonte estática, no caminho de seeds do ADR 0012, com a versão da planilha (o
carimbo "ÚLTIMA ATUALIZAÇÃO" da aba DADOS) como período::

    Sheets ─▶ raw/palmares/quilombos_certificados/versao=AAAA-MM-DD/<run>.xlsx
           ─▶ staging/palmares/quilombos_certificados/versao=AAAA-MM-DD/<run>.parquet
           ─▶ Postgres seeds.palmares_quilombos_certificados

O raw guarda o xlsx inteiro, com as quatro abas; staging e ``seeds``, só a
aba CERTIFICADAS, com o número da linha na planilha e a versão. Cada versão
que um run encontra fica ao lado das anteriores, e recarregar a mesma versão
a substitui, então reexecutar não duplica. A conferência (linhas da planilha,
do Parquet e do banco) vai para ``seeds.palmares_controle_carga``.

Sem carimbo, ou com um carimbo que não bate com as portarias da planilha, a
carga falha em ``baixar_para_raw``; confira a planilha e dispare com
``{"versao": "AAAA-MM-DD"}``.

Mensal por padrão (Variable ``dynamic_schedules``). Um run sem versão nova só
recarrega a mesma.
"""

import logging
from datetime import datetime, timedelta, timezone
from typing import Any

from airflow.sdk import Param, dag, get_current_context, task

import cliente_palmares as palmares
import datalakehouse
import schemas_minc as schemas
import seeds_carga
from openmetadata.lineage import publicar_linhagem, tabela
from postgres_helpers import get_postgres_conn
from schedule_loader import get_dynamic_schedule

ASSET_STAGING_PALMARES = datalakehouse.asset(
    f"{datalakehouse.CAMADA_STAGING}/{schemas.FONTE_PALMARES}/"
)

default_args = {
    "owner": "MinC",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def _run_id() -> str:
    return str(get_current_context()["dag_run"].run_id)


@dag(
    dag_id="palmares_quilombos_dag",
    schedule=get_dynamic_schedule("palmares_quilombos_dag", default="@monthly"),
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
    params={
        "versao": Param(
            "",
            type="string",
            pattern=r"^(\d{4}-\d{2}-\d{2})?$",
            description=(
                "Versão da planilha (AAAA-MM-DD). Vazio lê o carimbo 'ÚLTIMA "
                "ATUALIZAÇÃO' da aba DADOS; informe só quando ele faltar ou "
                "estiver velho."
            ),
        )
    },
    tags=["minc", "palmares", "quilombolas", "datalakehouse", "seeds"],
)
def palmares_quilombos_dag() -> None:
    """Carrega a planilha de comunidades quilombolas certificadas da Fundação
    Palmares no schema seeds."""

    @task
    def baixar_para_raw() -> dict[str, Any]:
        """Baixa a planilha, descobre a versão e grava o xlsx em ``raw/`` como veio."""
        conteudo = palmares.baixar()
        informada = get_current_context()["params"]["versao"] or None
        versao = palmares.versao_da_planilha(conteudo, informada=informada)
        key = datalakehouse.caminho_particionado(
            datalakehouse.CAMADA_RAW,
            schemas.FONTE_PALMARES,
            palmares.ENTIDADE,
            {palmares.COLUNA_VERSAO: versao},
            _run_id(),
            "xlsx",
        )
        datalakehouse.gravar_bytes_em_partes(conteudo, key)
        logging.info("[palmares_quilombos_dag.py] versão %s em %s", versao, key)
        return {"key_raw": key, "versao": versao, "url": palmares.URL_EXPORT}

    @task
    def raw_para_parquet(item: dict[str, Any]) -> dict[str, Any]:
        linhas = palmares.registros(
            datalakehouse.ler_bytes(item["key_raw"]), item["versao"]
        )
        dt_ingest = datetime.now(timezone.utc)
        key = datalakehouse.caminho_particionado(
            datalakehouse.CAMADA_STAGING,
            schemas.FONTE_PALMARES,
            palmares.ENTIDADE,
            {palmares.COLUNA_VERSAO: item["versao"]},
            _run_id(),
            "parquet",
        )
        datalakehouse.gravar_parquet(linhas, key, dt_ingest=dt_ingest)
        return {
            "entidade": palmares.ENTIDADE,
            "periodo": item["versao"],
            "tabela": palmares.tabela_seeds(),
            "arquivo": item["url"],
            "linhas_origem": len(linhas),
            "key_raw": item["key_raw"],
            "key_staging": key,
            "dt_ingest": dt_ingest.isoformat(),
        }

    @task(
        outlets=[
            ASSET_STAGING_PALMARES,
            tabela(schemas.SCHEMA_SEEDS, palmares.tabela_seeds()),
            tabela(
                schemas.SCHEMA_SEEDS,
                seeds_carga.tabela_controle(schemas.FONTE_PALMARES),
            ),
        ]
    )
    def parquet_para_seeds(item: dict[str, Any]) -> None:
        conn = get_postgres_conn()
        df = datalakehouse.ler_parquet(item["key_staging"])
        carregadas = seeds_carga.substituir_periodo(
            conn,
            df,
            schemas.SCHEMA_SEEDS,
            item["tabela"],
            palmares.COLUNA_VERSAO,
            item["periodo"],
        )
        cargas = [{**item, "linhas_parquet": len(df), "linhas_carregadas": carregadas}]
        seeds_carga.registrar_carga(
            conn, schemas.SCHEMA_SEEDS, schemas.FONTE_PALMARES, cargas
        )
        seeds_carga.conferir_contagens(cargas)

    convertido = raw_para_parquet(baixar_para_raw())
    parquet_para_seeds(convertido) >> publicar_linhagem()


palmares_quilombos_dag()
