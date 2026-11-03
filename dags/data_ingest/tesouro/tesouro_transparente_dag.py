"""CSVs do Tesouro Transparente (CKAN) para o schema ``seeds``.

O primeiro recurso é a RCL anual da União de 2000 a 2018, que o Eixo 1 do
Cultura em Números usa na participação da cultura no orçamento federal antes
de o RREO do SICONFI cobrir a União (ver ``cliente_tesouro_transparente``).
Recurso novo é uma entrada em ``RECURSOS``.

Fonte estática, no caminho de seeds do ADR 0012, com a versão do arquivo
(a data de modificação no CKAN) como partição::

    CKAN ─▶ raw/tesouro_transparente/<entidade>/versao=AAAA-MM-DD/<run>.csv
         ─▶ staging/tesouro_transparente/<entidade>/versao=AAAA-MM-DD/<run>.parquet
         ─▶ Postgres seeds.tesouro_transparente_<entidade>

Cada versão do CSV traz a série inteira, então a carga substitui todas as
linhas do recurso; reexecutar não duplica. A conferência (linhas do arquivo
por quebra de linha, do Parquet e do banco) vai para
``seeds.tesouro_transparente_controle_carga``.

Manual: o conjunto da RCL não é atualizado desde 2021.
"""

from datetime import datetime, timedelta, timezone
from typing import Any

from airflow.sdk import Param, dag, get_current_context, task

import cliente_tesouro_transparente as tesouro
import datalakehouse
import schemas_minc as schemas
import seeds_carga
from openmetadata.lineage import publicar_linhagem, tabela
from postgres_helpers import get_postgres_conn

ASSET_STAGING_TESOURO = datalakehouse.asset(
    f"{datalakehouse.CAMADA_STAGING}/{schemas.FONTE_TESOURO_TRANSPARENTE}/"
)

default_args = {
    "owner": "MinC",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def _run_id() -> str:
    return str(get_current_context()["dag_run"].run_id)


@dag(
    dag_id="tesouro_transparente_dag",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
    params={
        "recursos": Param(
            [],
            type="array",
            items={"type": "string", "enum": sorted(tesouro.RECURSOS)},
            description="Recursos a carregar. Vazio carrega todos do catálogo.",
        )
    },
    tags=["minc", "tesouro", "ckan", "datalakehouse", "seeds"],
)
def tesouro_transparente_dag() -> None:
    """Carrega CSVs do Tesouro Transparente no schema seeds, como a RCL
    anual da União desde 2000."""

    @task
    def listar_recursos() -> list[str]:
        return sorted(get_current_context()["params"]["recursos"] or tesouro.RECURSOS)

    @task
    def baixar_para_raw(entidade: str) -> dict[str, Any]:
        """Baixa o CSV da versão publicada e grava em ``raw/`` como veio."""
        recurso = tesouro.RECURSOS[entidade]
        descricao = tesouro.descrever(recurso)
        key = datalakehouse.caminho_particionado(
            datalakehouse.CAMADA_RAW,
            schemas.FONTE_TESOURO_TRANSPARENTE,
            entidade,
            {"versao": descricao["versao"]},
            _run_id(),
            "csv",
        )
        datalakehouse.gravar_bytes_em_partes(tesouro.baixar(descricao["url"]), key)
        return {"entidade": entidade, "key_raw": key, **descricao}

    @task
    def raw_para_parquet(item: dict[str, Any]) -> dict[str, Any]:
        entidade = item["entidade"]
        recurso = tesouro.RECURSOS[entidade]
        conteudo = datalakehouse.ler_bytes(item["key_raw"])
        linhas = tesouro.registros(recurso, conteudo, item["versao"])
        dt_ingest = datetime.now(timezone.utc)
        key = datalakehouse.caminho_particionado(
            datalakehouse.CAMADA_STAGING,
            schemas.FONTE_TESOURO_TRANSPARENTE,
            entidade,
            {"versao": item["versao"]},
            _run_id(),
            "parquet",
        )
        datalakehouse.gravar_parquet(linhas, key, dt_ingest=dt_ingest)
        return {
            "entidade": entidade,
            "periodo": recurso.recurso_id,
            "tabela": tesouro.tabela_seeds(entidade),
            "arquivo": item["url"],
            "linhas_origem": tesouro.contar_linhas(recurso, conteudo),
            "key_raw": item["key_raw"],
            "key_staging": key,
            "dt_ingest": dt_ingest.isoformat(),
        }

    @task(
        outlets=[ASSET_STAGING_TESOURO]
        + [
            tabela(schemas.SCHEMA_SEEDS, tesouro.tabela_seeds(e))
            for e in tesouro.RECURSOS
        ]
        + [
            tabela(
                schemas.SCHEMA_SEEDS,
                seeds_carga.tabela_controle(schemas.FONTE_TESOURO_TRANSPARENTE),
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
                tesouro.COLUNA_RECURSO,
                item["periodo"],
            )
            cargas.append(
                {**item, "linhas_parquet": len(df), "linhas_carregadas": carregadas}
            )
        seeds_carga.registrar_carga(
            conn, schemas.SCHEMA_SEEDS, schemas.FONTE_TESOURO_TRANSPARENTE, cargas
        )
        seeds_carga.conferir_contagens(cargas)

    brutos = baixar_para_raw.expand(entidade=listar_recursos())
    convertidos = raw_para_parquet.expand(item=brutos)
    parquet_para_seeds(convertidos) >> publicar_linhagem()


tesouro_transparente_dag()
