"""Execução orçamentária federal do SIOP, por exercício (Eixo 1 do Cultura em Números).

Substitui a planilha ``funcoes_orgaos_unidades_rp`` do Painel do Orçamento,
carregada à mão em ``siop.execucao_orcamentaria``, pela consulta SPARQL
pública do SIOP (ver ``cliente_siop``). Segue o caminho de seeds do ADR 0012,
com o exercício como período::

    SPARQL do SIOP ─▶ raw/siop/execucao_orcamentaria/ano=AAAA/<run>.json
                   ─▶ staging/siop/execucao_orcamentaria/ano=AAAA/<run>.parquet
                   ─▶ Postgres seeds.siop_execucao_orcamentaria

O raw é a resposta do endpoint como veio. Cada exercício substitui só a si
mesmo (``DELETE`` + ``COPY`` por ``ano``), então reexecutar não duplica, e a
revisão de um exercício antigo entra quando ele é pedido de novo.

Sem ``params.anos``, o run pede o exercício corrente e o anterior, que são os
que ainda mudam. A carga da série inteira (2003 em diante) é um run manual
com a lista de anos.

A conferência vai para ``seeds.siop_controle_carga``: linhas da resposta, do
Parquet e do banco, por exercício.
"""

from datetime import datetime, timedelta, timezone
from typing import Any

from airflow.sdk import Param, dag, get_current_context, task

import cliente_siop as siop
import datalakehouse
import schemas_minc as schemas
import seeds_carga
from openmetadata.lineage import publicar_linhagem, tabela
from postgres_helpers import get_postgres_conn
from schedule_loader import get_dynamic_schedule

_ENTIDADE = "execucao_orcamentaria"
_TABELA = f"{schemas.FONTE_SIOP}_{_ENTIDADE}"
# Primeiro exercício que os scripts do Eixo 1 usam.
_PRIMEIRO_ANO = 2003

ASSET_STAGING_SIOP = datalakehouse.asset_staging(schemas.FONTE_SIOP, _ENTIDADE)

default_args = {
    "owner": "MinC",
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
}


def _run_id() -> str:
    return str(get_current_context()["dag_run"].run_id)


@dag(
    dag_id="siop_execucao_dag",
    schedule=get_dynamic_schedule("siop_execucao_dag", default="@monthly"),
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
    params={
        "anos": Param(
            [],
            type="array",
            items={"type": "integer", "minimum": 2000},
            description=(
                "Exercícios a carregar. Vazio pede o corrente e o anterior; "
                f"a série inteira começa em {_PRIMEIRO_ANO}."
            ),
        )
    },
    tags=["minc", "siop", "orcamento", "datalakehouse", "seeds"],
)
def siop_execucao_dag() -> None:
    """Carrega a execução orçamentária federal do SIOP, por exercício, no schema seeds."""

    @task
    def listar_anos() -> list[int]:
        pedidos = get_current_context()["params"]["anos"]
        if pedidos:
            return sorted({int(ano) for ano in pedidos})
        corrente = datetime.now(timezone.utc).year
        return [corrente - 1, corrente]

    # Uma consulta por vez: cada exercício agrega o orçamento federal inteiro
    # no endpoint público (cerca de 20 s por ano).
    @task(max_active_tis_per_dag=1, execution_timeout=timedelta(minutes=30))
    def baixar_para_raw(ano: int) -> dict[str, Any]:
        """Consulta o exercício e grava a resposta em ``raw/`` como veio."""
        key = datalakehouse.caminho_particionado(
            datalakehouse.CAMADA_RAW,
            schemas.FONTE_SIOP,
            _ENTIDADE,
            {"ano": ano},
            _run_id(),
            "json",
        )
        datalakehouse.gravar_bytes_em_partes(siop.baixar(ano), key)
        return {"ano": ano, "key_raw": key}

    @task(max_active_tis_per_dag=2)
    def raw_para_parquet(item: dict[str, Any]) -> dict[str, Any]:
        """A resposta vira uma linha por combinação, com os valores em reais."""
        ano, key_raw = item["ano"], item["key_raw"]
        dt_ingest = datetime.now(timezone.utc)
        linhas = siop.registros(ano, datalakehouse.ler_bytes(key_raw))
        if not linhas:
            raise ValueError(f"o SIOP não devolveu linhas para {ano}")
        key = datalakehouse.caminho_particionado(
            datalakehouse.CAMADA_STAGING,
            schemas.FONTE_SIOP,
            _ENTIDADE,
            {"ano": ano},
            _run_id(),
            "parquet",
        )
        datalakehouse.gravar_parquet(linhas, key, dt_ingest=dt_ingest)
        return {
            "entidade": _ENTIDADE,
            "periodo": str(ano),
            "tabela": _TABELA,
            "arquivo": siop.URL_SPARQL,
            "linhas_origem": len(linhas),
            "key_raw": key_raw,
            "key_staging": key,
            "dt_ingest": dt_ingest.isoformat(),
        }

    @task(
        outlets=[
            ASSET_STAGING_SIOP,
            tabela(schemas.SCHEMA_SEEDS, _TABELA),
            tabela(schemas.SCHEMA_SEEDS, seeds_carga.tabela_controle(schemas.FONTE_SIOP)),
        ]
    )
    def parquet_para_seeds(itens: list[dict[str, Any]]) -> None:
        """Substitui cada exercício em ``seeds`` e registra a conferência."""
        conn = get_postgres_conn()
        cargas = []
        for item in itens:
            df = datalakehouse.ler_parquet(item["key_staging"])
            carregadas = seeds_carga.substituir_periodo(
                conn,
                df,
                schemas.SCHEMA_SEEDS,
                _TABELA,
                siop.COLUNA_ANO,
                item["periodo"],
            )
            cargas.append(
                {**item, "linhas_parquet": len(df), "linhas_carregadas": carregadas}
            )
        seeds_carga.registrar_carga(
            conn, schemas.SCHEMA_SEEDS, schemas.FONTE_SIOP, cargas
        )
        seeds_carga.conferir_contagens(cargas)

    brutos = baixar_para_raw.expand(ano=listar_anos())
    convertidos = raw_para_parquet.expand(item=brutos)
    parquet_para_seeds(convertidos) >> publicar_linhagem()


siop_execucao_dag()
