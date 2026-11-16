"""Ingestão do CNEFE do Censo 2022 do IBGE (issue #168, eixo 5 do Cultura em Números).

Fonte estática, publicada uma vez com o Censo, e por isso segue o caminho de
seeds do ADR 0012, com a UF no lugar do período::

    ftp.ibge.gov.br ─▶ raw/ibge/cnefe_2022/uf=XX/municipio=NNNNNNN/<run>.zip
                    ─▶ staging/ibge/cnefe_2022/uf=XX/municipio=NNNNNNN/<run>.parquet
                    ─▶ Postgres seeds.ibge_cnefe_2022

O raw guarda o ZIP de cada município como o IBGE publica, em partes se passar
do limite do proxy do MinIO, e nunca é sobrescrito: cada run grava a própria
key. O staging e o ``seeds`` guardam só as espécies de endereço 4 e 6 e as
16 colunas que o eixo 5 lê (``cliente_ibge_cnefe``).

Uma UF por mapeamento. Download e conversão rodam em paralelo; a carga, uma
UF por vez, porque todas escrevem na mesma tabela. Se o download de uma UF
cair no meio, a nova tentativa da task retoma do município em que parou.
Cada UF substitui só a si mesma no Postgres (``DELETE`` + ``COPY`` por ``uf``
na mesma transação), então reexecutar não duplica.

A conferência contra a origem vai para ``seeds.ibge_controle_carga``, uma
linha por UF: as linhas das espécies 4 e 6 contadas nos bytes dos CSVs, no
Parquet e no banco. A task falha se as três não baterem. Antes disso, a
listagem de cada UF tem que ter os municípios esperados, e cada CSV tem que
ter tantas quebras de linha quanto linhas lidas.

Manual: a fonte não muda. Dispare com ``{"ufs": ["RR"]}`` para recarregar
uma UF, ou vazio para todas.
"""

import logging
from collections.abc import Iterator
from datetime import datetime, timedelta, timezone
from typing import Any

from airflow.sdk import Param, dag, get_current_context, task

import cliente_ibge_cnefe as cnefe
import datalakehouse
import schemas_minc as schemas
import seeds_carga
from openmetadata.lineage import publicar_linhagem, tabela
from postgres_helpers import get_postgres_conn

ASSET_STAGING_CNEFE = datalakehouse.asset_staging(schemas.FONTE_IBGE, cnefe.ENTIDADE)

default_args = {
    "owner": "MinC",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def _run_id() -> str:
    return str(get_current_context()["dag_run"].run_id)


def _key(camada: str, uf: str, codigo: str, run_id: str) -> str:
    extensao = "zip" if camada == datalakehouse.CAMADA_RAW else "parquet"
    return datalakehouse.caminho_particionado(
        camada,
        schemas.FONTE_IBGE,
        cnefe.ENTIDADE,
        {"uf": uf, "municipio": codigo},
        run_id,
        extensao,
    )


def _padrao(camada: str, uf: str, run_id: str) -> str:
    """As keys de uma UF num run, para o controle de carga."""
    return _key(camada, uf, "*", run_id)


def _completo_no_raw(key: str) -> bool:
    """Se ``key`` já está no raw, inteira ou em partes completas.

    Partes de uma gravação que caiu no meio contam como ausente: a próxima
    gravação as substitui (``datalakehouse._gravar_partes``).
    """
    try:
        datalakehouse.partes_do_objeto(key)
    except (FileNotFoundError, datalakehouse.PartesInconsistentes):
        return False
    return True


def _baixar_uf(uf: str, run_id: str) -> list[str]:
    """Grava em ``raw/`` o ZIP de cada município da UF; devolve os códigos.

    Na nova tentativa de uma task que caiu, pula o município que este run já
    gravou inteiro: MG tem 853 ZIPs, e o FTP do IBGE oscila.
    """
    municipios = []
    for arquivo in cnefe.obter_listagem(uf):
        key = _key(datalakehouse.CAMADA_RAW, uf, arquivo.codigo, run_id)
        municipios.append(arquivo.codigo)
        if _completo_no_raw(key):
            logging.info(
                "[ibge_cnefe_2022_dag] %s %s: já no raw deste run", uf, arquivo.nome
            )
            continue
        conteudo = cnefe.baixar(arquivo.url)
        datalakehouse.gravar_bytes_em_partes(conteudo, key)
        logging.info(
            "[ibge_cnefe_2022_dag] %s %s: %d bytes", uf, arquivo.nome, len(conteudo)
        )
    return municipios


def _converter_uf(uf: str, municipios: list[str], run_id: str) -> dict[str, Any]:
    """Converte o raw de cada município em Parquet no staging.

    Devolve a linha da UF no controle de carga, sem ``linhas_carregadas``.
    """
    dt_ingest = datetime.now(timezone.utc)
    linhas_arquivo = linhas_origem = linhas_parquet = 0
    for codigo in municipios:
        conversao = cnefe.converter_municipio(
            datalakehouse.ler_bytes(_key(datalakehouse.CAMADA_RAW, uf, codigo, run_id)),
            codigo,
            uf,
            dt_ingest,
        )
        datalakehouse.gravar_tabela(
            conversao.tabela, _key(datalakehouse.CAMADA_STAGING, uf, codigo, run_id)
        )
        logging.info(
            "[ibge_cnefe_2022_dag] %s %s: %d endereços, %d das espécies nos bytes, "
            "%d no Parquet",
            uf,
            conversao.arquivo,
            conversao.linhas_arquivo,
            conversao.linhas_especies,
            conversao.tabela.num_rows,
        )
        linhas_arquivo += conversao.linhas_arquivo
        linhas_origem += conversao.linhas_especies
        linhas_parquet += conversao.tabela.num_rows
    return {
        "entidade": cnefe.ENTIDADE,
        "periodo": uf,
        "tabela": cnefe.TABELA_SEEDS,
        "arquivo": (
            f"CSV/Municipio/{cnefe.UFS[uf].codigo}_{uf}/: {len(municipios)} ZIPs, "
            f"{linhas_arquivo} endereços"
        ),
        "linhas_origem": linhas_origem,
        "linhas_parquet": linhas_parquet,
        "key_raw": _padrao(datalakehouse.CAMADA_RAW, uf, run_id),
        "key_staging": _padrao(datalakehouse.CAMADA_STAGING, uf, run_id),
        "dt_ingest": dt_ingest.isoformat(),
    }


def _partes_da_uf(uf: str, municipios: list[str], run_id: str) -> Iterator[bytes]:
    """Cada parte de staging dos municípios da UF, na ordem, uma de cada vez."""
    for codigo in municipios:
        key = _key(datalakehouse.CAMADA_STAGING, uf, codigo, run_id)
        for parte in datalakehouse.partes_do_objeto(key):
            yield datalakehouse.ler_bytes_das_partes([parte])


@dag(
    dag_id="ibge_cnefe_2022_dag",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
    params={
        "ufs": Param(
            [],
            type="array",
            items={"type": "string", "enum": list(cnefe.UFS)},
            description="Siglas das UFs a carregar. Vazio carrega as 27.",
        )
    },
    tags=["minc", "ibge", "cnefe", "datalakehouse", "seeds"],
)
def ibge_cnefe_2022_dag() -> None:
    """Carrega no schema seeds os endereços de estabelecimento de ensino e de
    outras finalidades do CNEFE 2022, uma UF por vez."""

    @task
    def listar_ufs() -> list[str]:
        pedidas = get_current_context()["params"]["ufs"] or list(cnefe.UFS)
        desconhecidas = set(pedidas) - set(cnefe.UFS)
        if desconhecidas:
            raise ValueError(f"UFs fora do catálogo: {sorted(desconhecidas)}")
        return [uf for uf in cnefe.UFS if uf in set(pedidas)]

    # Três UFs por vez no FTP do IBGE. Seis horas: em 16/11/2026 um ZIP levou de
    # 1 a 36 s, e MG tem 853.
    @task(max_active_tis_per_dag=3, execution_timeout=timedelta(hours=6))
    def baixar_para_raw(uf: str) -> dict[str, Any]:
        """Baixa o ZIP de cada município da UF e grava em ``raw/`` como veio."""
        return {"uf": uf, "municipios": _baixar_uf(uf, _run_id())}

    @task(max_active_tis_per_dag=2, execution_timeout=timedelta(hours=3))
    def raw_para_parquet(item: dict[str, Any]) -> dict[str, Any]:
        """Converte o CSV de cada município em Parquet, só as espécies 4 e 6."""
        carga = _converter_uf(item["uf"], item["municipios"], _run_id())
        return {**item, "carga": carga}

    # Uma por vez: todas as UFs escrevem na mesma tabela, e duas disputariam o
    # CREATE TABLE e o controle de carga.
    @task(
        max_active_tis_per_dag=1,
        execution_timeout=timedelta(hours=1),
        outlets=[
            ASSET_STAGING_CNEFE,
            tabela(schemas.SCHEMA_SEEDS, cnefe.TABELA_SEEDS),
            tabela(schemas.SCHEMA_SEEDS, seeds_carga.tabela_controle(schemas.FONTE_IBGE)),
        ],
    )
    def parquet_para_seeds(item: dict[str, Any]) -> None:
        """Substitui a UF em ``seeds.ibge_cnefe_2022`` e registra a conferência."""
        uf = item["uf"]
        conn = get_postgres_conn()
        carregadas = cnefe.carregar_uf(
            conn,
            schemas.SCHEMA_SEEDS,
            uf,
            _partes_da_uf(uf, item["municipios"], _run_id()),
        )
        carga = {**item["carga"], "linhas_carregadas": carregadas}
        seeds_carga.registrar_carga(
            conn, schemas.SCHEMA_SEEDS, schemas.FONTE_IBGE, [carga]
        )
        seeds_carga.conferir_contagens([carga])

    brutos = baixar_para_raw.expand(uf=listar_ufs())
    convertidos = raw_para_parquet.expand(item=brutos)
    parquet_para_seeds.expand(item=convertidos) >> publicar_linhagem()


ibge_cnefe_2022_dag()
