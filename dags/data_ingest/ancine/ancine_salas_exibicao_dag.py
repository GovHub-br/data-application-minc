"""Salas de exibição e complexos registrados na ANCINE, para ``seeds``.

A ANCINE publica nos dados abertos o cadastro de salas de cinema e dos
complexos onde elas ficam, atualizado todo mês (ver ``cliente_ancine``). O
Eixo 5 do Cultura em Números usa o cadastro para contar as salas em
funcionamento por município.

Fonte estática, no caminho de seeds do ADR 0012, com a versão do arquivo (o
``Last-Modified`` do servidor da ANCINE, no fuso de Brasília) como período::

    ANCINE ─▶ raw/ancine/salas_exibicao/versao=AAAA-MM-DD/<run>.csv
           ─▶ staging/ancine/salas_exibicao/versao=AAAA-MM-DD/<run>.parquet
           ─▶ Postgres seeds.ancine_salas_exibicao

O raw guarda o CSV como veio. Staging e ``seeds`` trazem as 32 colunas em
texto, mais o número da linha no arquivo e a versão. Cada versão que um run
encontra fica ao lado das anteriores, e recarregar a mesma versão a substitui,
então reexecutar não duplica. A conferência (linhas do arquivo por quebra de
linha, do Parquet e do banco) vai para ``seeds.ancine_controle_carga``.

O cadastro traz o CNPJ da sala e do exibidor, o nome do exibidor (em ME,
EIRELI e MEI, muitas vezes o de uma pessoa) e o endereço do complexo, que vão
para o raw, o Parquet e ``seeds`` como vieram. As colunas estão em
``cliente_ancine.DADOS_SENSIVEIS``; nenhuma delas entra em log.

Sem ``Last-Modified``, ou com uma data no futuro, a carga falha em
``baixar_para_raw``; confira a data do arquivo no portal e dispare com
``{"versao": "AAAA-MM-DD"}``.

Mensal por padrão (Variable ``dynamic_schedules``). Um run sem versão nova só
recarrega a mesma.
"""

import logging
from datetime import datetime, timedelta, timezone
from typing import Any

from airflow.sdk import Param, dag, get_current_context, task

import cliente_ancine as ancine
import datalakehouse
import schemas_minc as schemas
import seeds_carga
from openmetadata.lineage import publicar_linhagem, tabela
from postgres_helpers import get_postgres_conn
from schedule_loader import get_dynamic_schedule

ASSET_STAGING_ANCINE_SALAS = datalakehouse.asset_staging(
    schemas.FONTE_ANCINE, ancine.ENTIDADE
)

default_args = {
    "owner": "MinC",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def _run_id() -> str:
    return str(get_current_context()["dag_run"].run_id)


@dag(
    dag_id="ancine_salas_exibicao_dag",
    schedule=get_dynamic_schedule("ancine_salas_exibicao_dag", default="@monthly"),
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
                "Versão do arquivo (AAAA-MM-DD). Vazio usa o Last-Modified do "
                "servidor da ANCINE; informe só quando ele faltar ou estiver "
                "errado."
            ),
        )
    },
    tags=["minc", "ancine", "salas_exibicao", "datalakehouse", "seeds"],
)
def ancine_salas_exibicao_dag() -> None:
    """Carrega o cadastro de salas de exibição e complexos da ANCINE no schema
    seeds."""

    @task
    def baixar_para_raw() -> dict[str, Any]:
        """Baixa o CSV, descobre a versão e grava o arquivo em ``raw/`` como veio."""
        arquivo = ancine.baixar()
        informada = get_current_context()["params"]["versao"] or None
        versao = ancine.versao_do_arquivo(arquivo.last_modified, informada=informada)
        key = datalakehouse.caminho_particionado(
            datalakehouse.CAMADA_RAW,
            schemas.FONTE_ANCINE,
            ancine.ENTIDADE,
            {ancine.COLUNA_VERSAO: versao},
            _run_id(),
            "csv",
        )
        datalakehouse.gravar_bytes_em_partes(arquivo.conteudo, key)
        logging.info(
            "[ancine_salas_exibicao_dag.py] versão %s (ETag %s) em %s",
            versao,
            arquivo.etag,
            key,
        )
        return {"key_raw": key, "versao": versao, "url": ancine.URL_CSV}

    @task
    def raw_para_parquet(item: dict[str, Any]) -> dict[str, Any]:
        conteudo = datalakehouse.ler_bytes(item["key_raw"])
        linhas = ancine.registros(conteudo, item["versao"])
        dt_ingest = datetime.now(timezone.utc)
        key = datalakehouse.caminho_particionado(
            datalakehouse.CAMADA_STAGING,
            schemas.FONTE_ANCINE,
            ancine.ENTIDADE,
            {ancine.COLUNA_VERSAO: item["versao"]},
            _run_id(),
            "parquet",
        )
        datalakehouse.gravar_parquet(linhas, key, dt_ingest=dt_ingest)
        return {
            "entidade": ancine.ENTIDADE,
            "periodo": item["versao"],
            "tabela": ancine.tabela_seeds(),
            "arquivo": item["url"],
            "linhas_origem": ancine.contar_linhas(conteudo),
            "key_raw": item["key_raw"],
            "key_staging": key,
            "dt_ingest": dt_ingest.isoformat(),
        }

    @task(
        outlets=[
            ASSET_STAGING_ANCINE_SALAS,
            tabela(schemas.SCHEMA_SEEDS, ancine.tabela_seeds()),
            tabela(
                schemas.SCHEMA_SEEDS,
                seeds_carga.tabela_controle(schemas.FONTE_ANCINE),
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
            ancine.COLUNA_VERSAO,
            item["periodo"],
        )
        cargas = [{**item, "linhas_parquet": len(df), "linhas_carregadas": carregadas}]
        seeds_carga.registrar_carga(
            conn, schemas.SCHEMA_SEEDS, schemas.FONTE_ANCINE, cargas
        )
        seeds_carga.conferir_contagens(cargas)

    convertido = raw_para_parquet(baixar_para_raw())
    parquet_para_seeds(convertido) >> publicar_linhagem()


ancine_salas_exibicao_dag()
