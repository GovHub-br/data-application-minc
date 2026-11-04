"""Ingestão das bases da MUNIC e da ESTADIC do IBGE (Eixo 1 do Cultura em Números).

Fonte estática, publicada uma vez por edição, e por isso segue o caminho de
seeds do ADR 0012::

    ftp.ibge.gov.br ─▶ raw/ibge/perfil/edicao=<edicao>/<run>.<zip|xls|xlsx>
                    ─▶ staging/ibge/<edicao>_<aba>/edicao=<edicao>/<run>.parquet
                    ─▶ Postgres seeds.ibge_<edicao>_<aba>

Uma tabela por aba e edição, porque os códigos de variável (``A2``,
``MCUL01``) mudam de sentido de uma edição para outra: juntar 2006 e 2014
na mesma coluna misturaria perguntas diferentes. Cada edição substitui só a
si mesma (``DELETE`` + ``COPY`` por ``edicao``), então reexecutar não
duplica. O raw nunca é sobrescrito: cada run grava a própria key, em partes
se passar do limite do proxy do MinIO.

As edições, as abas esperadas e quantos entes cada aba de dados deve ter
estão em ``cliente_ibge_perfil.EDICOES``. A conferência contra a origem vai
para ``seeds.ibge_controle_carga``.

Manual: o IBGE publica edição nova sem data fixa. Dispare com
``{"edicoes": ["munic_2021"]}`` para uma edição, ou vazio para todas.
"""

import logging
from datetime import datetime, timedelta, timezone
from typing import Any

from airflow.sdk import Param, dag, get_current_context, task

import cliente_ibge_perfil as ibge
import datalakehouse
import schemas_minc as schemas
import seeds_carga
from openmetadata.lineage import publicar_linhagem, tabela
from postgres_helpers import get_postgres_conn

_ENTIDADE_RAW = "perfil"

ASSET_STAGING_IBGE = datalakehouse.asset(
    f"{datalakehouse.CAMADA_STAGING}/{schemas.FONTE_IBGE}/"
)

default_args = {
    "owner": "MinC",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def _run_id() -> str:
    return str(get_current_context()["dag_run"].run_id)


@dag(
    dag_id="ibge_perfil_dag",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
    params={
        "edicoes": Param(
            [],
            type="array",
            items={"type": "string", "enum": sorted(ibge.EDICOES)},
            description="Edições a carregar. Vazio carrega todas do catálogo.",
        )
    },
    tags=["minc", "ibge", "munic", "estadic", "datalakehouse", "seeds"],
)
def ibge_perfil_dag() -> None:
    """Carrega as bases da MUNIC e da ESTADIC do IBGE no schema seeds,
    uma tabela por aba e edição."""

    @task
    def listar_edicoes() -> list[str]:
        pedidas = get_current_context()["params"]["edicoes"] or sorted(ibge.EDICOES)
        desconhecidas = set(pedidas) - set(ibge.EDICOES)
        if desconhecidas:
            raise ValueError(f"edições fora do catálogo: {sorted(desconhecidas)}")
        return sorted(pedidas)

    @task(max_active_tis_per_dag=2, execution_timeout=timedelta(minutes=30))
    def baixar_para_raw(edicao: str) -> dict[str, Any]:
        """Baixa o arquivo da edição e grava em ``raw/`` como veio."""
        info = ibge.EDICOES[edicao]
        conteudo = ibge.baixar(info.url)
        key = datalakehouse.caminho_particionado(
            datalakehouse.CAMADA_RAW,
            schemas.FONTE_IBGE,
            _ENTIDADE_RAW,
            {"edicao": edicao},
            _run_id(),
            ibge.extensao(info.caminho),
        )
        datalakehouse.gravar_bytes_em_partes(conteudo, key)
        return {"edicao": edicao, "key_raw": key, "arquivo": info.caminho}

    @task(max_active_tis_per_dag=1, execution_timeout=timedelta(hours=1))
    def raw_para_parquet(item: dict[str, Any]) -> list[dict[str, Any]]:
        """Uma aba vira um Parquet no staging; confere abas e linhas por ente."""
        edicao, key_raw = item["edicao"], item["key_raw"]
        dt_ingest = datetime.now(timezone.utc)
        nome, planilha = ibge.planilha_do_arquivo(
            datalakehouse.ler_bytes(key_raw), item["arquivo"]
        )
        livro = ibge.abrir_livro(planilha)
        ibge.conferir_abas(edicao, list(livro.sheet_names))

        arquivos = []
        for aba in livro.sheet_names:
            registros = ibge.registros_da_aba(edicao, aba, ibge.linhas_da_aba(livro, aba))
            entidade = ibge.entidade(edicao, aba)
            key = datalakehouse.caminho_particionado(
                datalakehouse.CAMADA_STAGING,
                schemas.FONTE_IBGE,
                entidade,
                {"edicao": edicao},
                _run_id(),
                "parquet",
            )
            datalakehouse.gravar_parquet(registros, key, dt_ingest=dt_ingest)
            logging.info(
                "[ibge_perfil_dag] %s / %s: %d linhas", edicao, aba, len(registros)
            )
            arquivos.append(
                {
                    "entidade": entidade,
                    "periodo": edicao,
                    "tabela": ibge.tabela_seeds(edicao, aba),
                    "arquivo": f"{nome} / {aba}",
                    "linhas_origem": len(registros),
                    "key_raw": key_raw,
                    "key_staging": key,
                    "dt_ingest": dt_ingest.isoformat(),
                }
            )
        return arquivos

    @task(
        max_active_tis_per_dag=1,
        execution_timeout=timedelta(hours=1),
        outlets=[ASSET_STAGING_IBGE]
        + [tabela(schemas.SCHEMA_SEEDS, t) for t in ibge.tabelas_seeds()]
        + [tabela(schemas.SCHEMA_SEEDS, seeds_carga.tabela_controle(schemas.FONTE_IBGE))],
    )
    def parquet_para_seeds(arquivos: list[dict[str, Any]]) -> None:
        """Substitui a edição em cada tabela de ``seeds`` e registra a conferência."""
        conn = get_postgres_conn()
        cargas = []
        for item in arquivos:
            df = datalakehouse.ler_parquet(item["key_staging"])
            carregadas = seeds_carga.substituir_periodo(
                conn,
                df,
                schemas.SCHEMA_SEEDS,
                item["tabela"],
                ibge.COLUNA_EDICAO,
                item["periodo"],
            )
            cargas.append(
                {**item, "linhas_parquet": len(df), "linhas_carregadas": carregadas}
            )
        seeds_carga.registrar_carga(
            conn, schemas.SCHEMA_SEEDS, schemas.FONTE_IBGE, cargas
        )
        seeds_carga.conferir_contagens(cargas)

    brutos = baixar_para_raw.expand(edicao=listar_edicoes())
    convertidos = raw_para_parquet.expand(item=brutos)
    parquet_para_seeds.expand(arquivos=convertidos) >> publicar_linhagem()


ibge_perfil_dag()
