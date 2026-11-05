"""Planilhas do portal de dados abertos do MinC (dados.cultura.gov.br) para ``seeds``.

O primeiro recurso é a lista de estados e municípios do Sistema Nacional de
Cultura, que o Eixo 1 do Cultura em Números usa para adesão, conselho, fundo
e plano de cultura (ver ``cliente_dados_cultura``). Recurso novo é uma
entrada em ``RECURSOS``.

Fonte estática, no caminho de seeds do ADR 0012, com a versão do arquivo
(o dia do ``last_modified`` do recurso no CKAN) como período::

    CKAN ─▶ raw/dados_cultura/<entidade>/versao=AAAA-MM-DD/<run>.xlsx
         ─▶ staging/dados_cultura/<entidade>/versao=AAAA-MM-DD/<run>.parquet
         ─▶ Postgres seeds.dados_cultura_<entidade>

Cada versão que um run encontra fica na tabela, ao lado das anteriores. O
portal só mostra a versão atual, então o histórico começa na primeira carga
e tem só as versões que estavam publicadas no dia de algum run: uma versão
publicada e substituída entre dois runs não fica registrada, e duas
publicações no mesmo dia têm a mesma versão (a carga da segunda substitui a
da primeira). Recarregar a mesma versão a substitui, então reexecutar não
duplica. A conferência (linhas da planilha, do Parquet e do banco) vai para
``seeds.dados_cultura_controle_carga``.

A lista do SNC traz dados bancários (agência e conta), CNPJ, nome do
prefeito e do gestor de cultura e endereço, que vão para o raw, o Parquet e
``seeds`` como vieram. As colunas estão em
``cliente_dados_cultura.DADOS_SENSIVEIS``; nenhuma delas entra em log.

Mensal por padrão (Variable ``dynamic_schedules``). O portal republica sem
data fixa; um run sem versão nova só recarrega a mesma.
"""

from datetime import datetime, timedelta, timezone
from typing import Any

from airflow.sdk import Param, dag, get_current_context, task

import cliente_dados_cultura as portal
import datalakehouse
import schemas_minc as schemas
import seeds_carga
from openmetadata.lineage import publicar_linhagem, tabela
from postgres_helpers import get_postgres_conn
from schedule_loader import get_dynamic_schedule

ASSET_STAGING_DADOS_CULTURA = datalakehouse.asset(
    f"{datalakehouse.CAMADA_STAGING}/{schemas.FONTE_DADOS_CULTURA}/"
)

default_args = {
    "owner": "MinC",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def _run_id() -> str:
    return str(get_current_context()["dag_run"].run_id)


@dag(
    dag_id="dados_cultura_dag",
    schedule=get_dynamic_schedule("dados_cultura_dag", default="@monthly"),
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
    params={
        "recursos": Param(
            [],
            type="array",
            items={"type": "string", "enum": sorted(portal.RECURSOS)},
            description="Recursos a carregar. Vazio carrega todos do catálogo.",
        )
    },
    tags=["minc", "dados_cultura", "snc", "ckan", "datalakehouse", "seeds"],
)
def dados_cultura_dag() -> None:
    """Carrega planilhas do portal de dados abertos do MinC no schema seeds,
    como a lista de entes do Sistema Nacional de Cultura."""

    @task
    def listar_recursos() -> list[str]:
        return sorted(get_current_context()["params"]["recursos"] or portal.RECURSOS)

    @task
    def baixar_para_raw(entidade: str) -> dict[str, Any]:
        """Baixa a versão publicada e grava em ``raw/`` como veio."""
        descricao = portal.descrever(portal.RECURSOS[entidade])
        key = datalakehouse.caminho_particionado(
            datalakehouse.CAMADA_RAW,
            schemas.FONTE_DADOS_CULTURA,
            entidade,
            {"versao": descricao["versao"]},
            _run_id(),
            "xlsx",
        )
        datalakehouse.gravar_bytes_em_partes(portal.baixar(descricao["url"]), key)
        return {"entidade": entidade, "key_raw": key, **descricao}

    @task
    def raw_para_parquet(item: dict[str, Any]) -> dict[str, Any]:
        entidade = item["entidade"]
        linhas = portal.registros(
            portal.RECURSOS[entidade],
            portal.linhas_da_planilha(datalakehouse.ler_bytes(item["key_raw"])),
            item["versao"],
        )
        dt_ingest = datetime.now(timezone.utc)
        key = datalakehouse.caminho_particionado(
            datalakehouse.CAMADA_STAGING,
            schemas.FONTE_DADOS_CULTURA,
            entidade,
            {"versao": item["versao"]},
            _run_id(),
            "parquet",
        )
        datalakehouse.gravar_parquet(linhas, key, dt_ingest=dt_ingest)
        return {
            "entidade": entidade,
            "periodo": item["versao"],
            "tabela": portal.tabela_seeds(entidade),
            "arquivo": item["url"],
            "linhas_origem": len(linhas),
            "key_raw": item["key_raw"],
            "key_staging": key,
            "dt_ingest": dt_ingest.isoformat(),
        }

    @task(
        outlets=[ASSET_STAGING_DADOS_CULTURA]
        + [tabela(schemas.SCHEMA_SEEDS, portal.tabela_seeds(e)) for e in portal.RECURSOS]
        + [
            tabela(
                schemas.SCHEMA_SEEDS,
                seeds_carga.tabela_controle(schemas.FONTE_DADOS_CULTURA),
            )
        ]
    )
    def parquet_para_seeds(itens: list[dict[str, Any]]) -> None:
        conn = get_postgres_conn()
        cargas = []
        for item in itens:
            df = datalakehouse.ler_parquet(item["key_staging"])
            carregadas = seeds_carga.substituir_periodo(
                conn,
                df,
                schemas.SCHEMA_SEEDS,
                item["tabela"],
                portal.COLUNA_VERSAO,
                item["periodo"],
            )
            cargas.append(
                {**item, "linhas_parquet": len(df), "linhas_carregadas": carregadas}
            )
        seeds_carga.registrar_carga(
            conn, schemas.SCHEMA_SEEDS, schemas.FONTE_DADOS_CULTURA, cargas
        )
        seeds_carga.conferir_contagens(cargas)

    brutos = baixar_para_raw.expand(entidade=listar_recursos())
    convertidos = raw_para_parquet.expand(item=brutos)
    parquet_para_seeds(convertidos) >> publicar_linhagem()


dados_cultura_dag()
