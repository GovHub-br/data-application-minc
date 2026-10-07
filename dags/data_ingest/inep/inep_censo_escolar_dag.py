"""Ingestão dos microdados do Censo Escolar do INEP (issue #76).

Fonte estática, publicada uma vez por ano, e por isso segue o caminho de
seeds::

    download.inep.gov.br ─▶ raw/inep/censo_escolar/ano_censo=AAAA/<run>.zip
                         ─▶ staging/inep/<entidade>/ano_censo=AAAA/<run>.parquet
                         ─▶ Postgres seeds.inep_<entidade>

Um run carrega os anos de ``params.anos`` (vazio = todos os que a página do
INEP lista, 1995 em diante), um mapeamento por ano. Cada ano substitui só a
si mesmo no Postgres (``DELETE`` + ``COPY`` por ``ano_censo`` na mesma
transação), então reexecutar não duplica. O raw nunca é sobrescrito: cada
run grava a própria key.

O que muda ao longo da série -- nomes de arquivo, separador, as 3.808
colunas do CENSOESC que vão para ``jsonb`` -- está em ``cliente_inep``.

A conferência contra a origem fica em ``seeds.inep_controle_carga``: linhas
do CSV (contadas por quebra de linha), do Parquet e do banco, por ano e
entidade. A task falha se as três não baterem.
"""

import logging
import tempfile
import zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from airflow.sdk import Param, dag, get_current_context, task

import cliente_inep as inep
import datalakehouse
import schemas_minc as schemas
from openmetadata.lineage import publicar_linhagem, tabela
from postgres_helpers import get_postgres_conn

_ENTIDADE_RAW = "censo_escolar"

ASSET_STAGING_INEP = datalakehouse.asset(
    f"{datalakehouse.CAMADA_STAGING}/{schemas.FONTE_INEP}/"
)

default_args = {
    "owner": "Caio Borges",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def _run_id() -> str:
    return str(get_current_context()["dag_run"].run_id)


def _converter_csv(
    arquivo: zipfile.ZipFile,
    info: zipfile.ZipInfo,
    ano: int,
    key_raw: str,
    dt_ingestao: datetime,
    tmp: str,
) -> dict[str, Any]:
    """Um CSV do ZIP vira um Parquet no staging; devolve o que a carga precisa."""
    entidade = inep.entidade_do_arquivo(info.filename)
    abrir = inep.abrir_membro(arquivo, info)
    local = str(Path(tmp) / f"{entidade}.parquet")
    linhas_csv = inep.contar_linhas(abrir)
    linhas_parquet = inep.csv_para_parquet(abrir, local, ano, dt_ingestao)
    key = datalakehouse.caminho_particionado(
        datalakehouse.CAMADA_STAGING,
        schemas.FONTE_INEP,
        entidade,
        {"ano_censo": ano},
        _run_id(),
        "parquet",
    )
    datalakehouse.gravar_arquivo(local, key)
    Path(local).unlink()
    logging.info(
        "[inep] %s %s: csv=%d parquet=%d", ano, entidade, linhas_csv, linhas_parquet
    )
    return {
        "ano": ano,
        "entidade": entidade,
        "arquivo": Path(info.filename).name,
        "linhas_csv": linhas_csv,
        "linhas_parquet": linhas_parquet,
        "key_raw": key_raw,
        "key_staging": key,
        "dt_ingestao": dt_ingestao.isoformat(),
    }


@dag(
    dag_id="inep_censo_escolar_dag",
    # Manual: o INEP publica um ano por vez, sem data fixa (2025 saiu em
    # 31/07/2026). Dispare com {"anos": [AAAA]} quando sair um ano novo.
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
    params={
        "anos": Param(
            [],
            type="array",
            items={"type": "integer"},
            description="Anos do censo a carregar. Vazio carrega todos os da página.",
        )
    },
    tags=["minc", "inep", "datalakehouse", "seeds"],
)
def inep_censo_escolar_dag() -> None:
    @task
    def descobrir_anos() -> list[dict[str, Any]]:
        """Anos e URLs da página do INEP, filtrados por ``params.anos``."""
        urls = inep.extrair_urls(inep.obter_pagina())
        if not urls:
            raise ValueError(f"nenhum ZIP encontrado em {inep.URL_PAGINA}")
        pedidos = set(get_current_context()["params"]["anos"] or urls)
        faltando = pedidos - urls.keys()
        if faltando:
            raise ValueError(f"anos sem ZIP na página do INEP: {sorted(faltando)}")
        return [{"ano": ano, "url": urls[ano]} for ano in sorted(pedidos)]

    @task(max_active_tis_per_dag=2, execution_timeout=timedelta(hours=1))
    def baixar_para_raw(item: dict[str, Any]) -> dict[str, Any]:
        """Baixa o ZIP do ano e grava em ``raw/`` como veio."""
        key = datalakehouse.caminho_particionado(
            datalakehouse.CAMADA_RAW,
            schemas.FONTE_INEP,
            _ENTIDADE_RAW,
            {"ano_censo": item["ano"]},
            _run_id(),
            "zip",
        )
        with tempfile.TemporaryDirectory() as tmp:
            local = str(Path(tmp) / "censo.zip")
            inep.baixar(item["url"], local)
            datalakehouse.gravar_arquivo(local, key)
        return {"ano": item["ano"], "key_raw": key}

    # Uma por vez: o CENSOESC de 2003-2006 passa de 2 GB de memória na
    # conversão (ver _BLOCO_BYTES em cliente_inep).
    @task(max_active_tis_per_dag=1, execution_timeout=timedelta(hours=2))
    def raw_para_parquet(item: dict[str, Any]) -> list[dict[str, Any]]:
        """Converte cada CSV de dados do ZIP em um Parquet no staging."""
        ano, key_raw = item["ano"], item["key_raw"]
        dt_ingestao = datetime.now(timezone.utc)
        arquivos: list[dict[str, Any]] = []
        with tempfile.TemporaryDirectory() as tmp:
            local_zip = datalakehouse.baixar_arquivo(
                key_raw, str(Path(tmp) / "censo.zip")
            )
            with zipfile.ZipFile(local_zip) as arquivo:
                membros = inep.csvs_de_dados(arquivo)
                if not membros:
                    raise ValueError(f"{key_raw}: nenhum CSV em dados/")
                for info in membros:
                    arquivos.append(
                        _converter_csv(arquivo, info, ano, key_raw, dt_ingestao, tmp)
                    )
        return arquivos

    # Uma por vez: dois anos em paralelo disputariam o CREATE TABLE e o
    # ALTER TABLE ADD COLUMN da mesma tabela.
    @task(
        max_active_tis_per_dag=1,
        execution_timeout=timedelta(hours=2),
        outlets=[ASSET_STAGING_INEP]
        + [tabela(schemas.SCHEMA_SEEDS, inep.tabela_seeds(e)) for e in inep.ENTIDADES]
        + [tabela(schemas.SCHEMA_SEEDS, inep.TABELA_CONTROLE)],
    )
    def parquet_para_seeds(arquivos: list[dict[str, Any]]) -> None:
        """Substitui o ano de cada entidade em ``seeds`` e registra a conferência."""
        conn = get_postgres_conn()
        cargas: list[dict[str, Any]] = []
        with tempfile.TemporaryDirectory() as tmp:
            for item in arquivos:
                local = datalakehouse.baixar_arquivo(
                    item["key_staging"], str(Path(tmp) / f"{item['entidade']}.parquet")
                )
                carregadas = inep.carregar_parquet(
                    conn, local, item["entidade"], item["ano"], schemas.SCHEMA_SEEDS
                )
                Path(local).unlink()
                cargas.append({**item, "linhas_carregadas": carregadas})
        inep.registrar_carga(conn, schemas.SCHEMA_SEEDS, cargas)

        inep.conferir_contagens(cargas)

    brutos = baixar_para_raw.expand(item=descobrir_anos())
    convertidos = raw_para_parquet.expand(item=brutos)
    parquet_para_seeds.expand(arquivos=convertidos) >> publicar_linhagem()


inep_censo_escolar_dag()
