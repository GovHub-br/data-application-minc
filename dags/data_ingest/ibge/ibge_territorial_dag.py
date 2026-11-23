"""Ingestão das planilhas territoriais do IBGE (Eixo 3 do Cultura em Números).

Áreas territoriais 2025, REGIC 2018 (hierarquia, base por cidade e arranjos
populacionais) e composição dos recortes metropolitanos 2025. Fonte estática,
publicada uma vez por versão, e por isso segue o caminho de seeds do
ADR 0012::

    geoftp.ibge.gov.br ─▶ raw/ibge/<arquivo>/versao=<ano>/<run>.<xls|xlsx>
                       ─▶ staging/ibge/<entidade>/versao=<ano>/<run>.parquet
                       ─▶ Postgres seeds.ibge_<entidade>

O raw guarda o arquivo inteiro, com dicionários e notas; staging e ``seeds``,
uma tabela por aba de dados. Cada versão substitui só a si mesma
(``DELETE`` + ``COPY`` por ``versao``), então reexecutar não duplica.

Os arquivos, as abas e quantas linhas cada aba deve ter estão em
``cliente_ibge_territorial.ARQUIVOS``. A conferência contra a origem vai para
``seeds.ibge_controle_carga``, a mesma tabela da MUNIC. Ela acontece antes de
trocar a versão: Parquet que não bate com a origem falha sem tocar em
``seeds``, e a contagem no banco é conferida antes do commit.

Manual: são publicações únicas. Dispare com ``{"arquivos": ["regic_cidades"]}``
para um arquivo, ou vazio para todos.
"""

import logging
from datetime import datetime, timedelta, timezone
from typing import Any

from airflow.sdk import Param, dag, get_current_context, task

import cliente_ibge_territorial as ibge
import datalakehouse
import schemas_minc as schemas
import seeds_carga
from openmetadata.lineage import publicar_linhagem, tabela
from postgres_helpers import get_postgres_conn

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
    dag_id="ibge_territorial_dag",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
    params={
        "arquivos": Param(
            [],
            type="array",
            items={"type": "string", "enum": sorted(ibge.ARQUIVOS)},
            description="Arquivos a carregar. Vazio carrega todos do catálogo.",
        )
    },
    tags=["minc", "ibge", "regic", "territorio", "datalakehouse", "seeds"],
)
def ibge_territorial_dag() -> None:
    """Carrega as áreas territoriais, a REGIC e a composição das RMs do IBGE
    no schema seeds, uma tabela por aba de dados."""

    @task
    def listar_arquivos() -> list[str]:
        pedidos = get_current_context()["params"]["arquivos"] or sorted(ibge.ARQUIVOS)
        desconhecidos = set(pedidos) - set(ibge.ARQUIVOS)
        if desconhecidos:
            raise ValueError(f"arquivos fora do catálogo: {sorted(desconhecidos)}")
        return sorted(pedidos)

    @task(max_active_tis_per_dag=2, execution_timeout=timedelta(minutes=30))
    def baixar_para_raw(chave: str) -> dict[str, Any]:
        """Baixa o arquivo e grava em ``raw/`` como veio."""
        arquivo = ibge.ARQUIVOS[chave]
        conteudo = ibge.baixar(arquivo.url)
        key = datalakehouse.caminho_particionado(
            datalakehouse.CAMADA_RAW,
            schemas.FONTE_IBGE,
            chave,
            {ibge.COLUNA_VERSAO: arquivo.versao},
            _run_id(),
            ibge.extensao(arquivo.caminho),
        )
        datalakehouse.gravar_bytes_em_partes(conteudo, key)
        return {"chave": chave, "key_raw": key}

    @task(max_active_tis_per_dag=1, execution_timeout=timedelta(minutes=30))
    def raw_para_parquet(item: dict[str, Any]) -> list[dict[str, Any]]:
        """Cada aba de dados vira um Parquet no staging; confere abas e linhas."""
        chave, key_raw = item["chave"], item["key_raw"]
        arquivo = ibge.ARQUIVOS[chave]
        dt_ingest = datetime.now(timezone.utc)
        livro = ibge.abrir_livro(datalakehouse.ler_bytes(key_raw))
        ibge.conferir_abas(chave, list(livro.sheet_names))

        convertidos = []
        for aba in arquivo.abas:
            registros = ibge.registros_da_aba(
                chave, aba, ibge.linhas_da_aba(livro, aba.nome)
            )
            key = datalakehouse.caminho_particionado(
                datalakehouse.CAMADA_STAGING,
                schemas.FONTE_IBGE,
                aba.entidade,
                {ibge.COLUNA_VERSAO: arquivo.versao},
                _run_id(),
                "parquet",
            )
            datalakehouse.gravar_parquet(registros, key, dt_ingest=dt_ingest)
            logging.info(
                "[ibge_territorial_dag] %s / %s: %d linhas",
                chave,
                aba.nome,
                len(registros),
            )
            convertidos.append(
                {
                    "entidade": aba.entidade,
                    "periodo": arquivo.versao,
                    "tabela": ibge.tabela_seeds(aba.entidade),
                    "arquivo": f"{arquivo.url} / {aba.nome}",
                    "linhas_origem": len(registros),
                    "key_raw": key_raw,
                    "key_staging": key,
                    "dt_ingest": dt_ingest.isoformat(),
                }
            )
        return convertidos

    @task(
        max_active_tis_per_dag=1,
        execution_timeout=timedelta(minutes=30),
        outlets=[ASSET_STAGING_IBGE]
        + [tabela(schemas.SCHEMA_SEEDS, t) for t in ibge.tabelas_seeds()]
        + [tabela(schemas.SCHEMA_SEEDS, seeds_carga.tabela_controle(schemas.FONTE_IBGE))],
    )
    def parquet_para_seeds(convertidos: list[dict[str, Any]]) -> None:
        """Substitui a versão em cada tabela de ``seeds`` e registra a conferência.

        Origem × Parquet é conferido para todas as abas antes de trocar qualquer
        tabela: se uma diverge nessa etapa, nenhuma é tocada. A contagem no banco
        é conferida depois, tabela por tabela, cada uma na sua transação. Se a
        de uma aba falha, essa aba volta à versão anterior, mas as que já foram
        carregadas antes dela ficam na nova, e ``registrar_carga`` não roda, então
        a divergência não chega a ``ibge_controle_carga`` — fica no log da task.
        """
        conn = get_postgres_conn()
        lidos: list[tuple[dict[str, Any], Any]] = []
        for item in convertidos:
            df = datalakehouse.ler_parquet(item["key_staging"])
            lidos.append(({**item, "linhas_parquet": len(df)}, df))
        seeds_carga.conferir_staging([item for item, _ in lidos])
        cargas = []
        for item, df in lidos:
            carregadas = seeds_carga.substituir_periodo(
                conn,
                df,
                schemas.SCHEMA_SEEDS,
                item["tabela"],
                ibge.COLUNA_VERSAO,
                item["periodo"],
                linhas_esperadas=len(df),
            )
            cargas.append({**item, "linhas_carregadas": carregadas})
        seeds_carga.registrar_carga(
            conn, schemas.SCHEMA_SEEDS, schemas.FONTE_IBGE, cargas
        )
        seeds_carga.conferir_contagens(cargas)

    brutos = baixar_para_raw.expand(chave=listar_arquivos())
    convertidos = raw_para_parquet.expand(item=brutos)
    parquet_para_seeds.expand(convertidos=convertidos) >> publicar_linhagem()


ibge_territorial_dag()
