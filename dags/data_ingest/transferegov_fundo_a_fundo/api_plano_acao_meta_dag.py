import logging
from datetime import datetime, timedelta

from airflow.sdk import dag, task

import datalakehouse
import schemas_minc as schemas
from cliente_transferegov_fundo_a_fundo import ClienteTransfereGov
from extracao_por_plano_acao import carregar_planos_acao, extrair_por_plano_acao


ASSET_PLANO_ACAO = datalakehouse.asset_staging(
    schemas.FONTE_TRANSFEREGOV, schemas.TABELA_PLANO_ACAO
)
ASSET_META = datalakehouse.asset_staging(
    schemas.FONTE_TRANSFEREGOV, schemas.TABELA_PLANO_ACAO_META
)

default_args = {
    "owner": "Caio Borges",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="api_plano_acao_meta_dag",
    schedule=[ASSET_PLANO_ACAO],
    start_date=datetime(2023, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["minc", "transferegov", "metas", "raw"],
)
def api_plano_acao_meta_dag() -> None:
    """Passo 3A da secao 6 da especificacao: metas de cada plano de acao.

    Disparada por ``api_planos_acao_dag`` (as metas so podem ser buscadas
    depois que os planos existem, porque o endpoint so filtra por
    ``id_plano_acao``). Le os planos do staging de ``plano_acao_minc`` e grava
    ``plano_acao_meta_minc`` em raw e staging, com ``id_programa`` e
    ``cod_ibge`` propagados do plano-pai. O identificador da meta na origem
    (secao 9.1) e ``id_meta_plano_acao``, nao ``id_meta`` como sugere o ER.
    """

    @task
    def extrair_metas() -> str:
        cadeia = datalakehouse.ler_cadeia(ASSET_PLANO_ACAO)
        planos = carregar_planos_acao(
            datalakehouse.ler_parquet(
                datalakehouse.key_da_cadeia(cadeia, schemas.TABELA_PLANO_ACAO)
            )
        )

        if not planos:
            raise ValueError(
                "[api_plano_acao_meta_dag.py] Nenhum plano de ação no staging de "
                f"{schemas.TABELA_PLANO_ACAO}"
            )

        logging.info(
            "[api_plano_acao_meta_dag.py] Buscando metas de %d planos de ação",
            len(planos),
        )

        api = ClienteTransfereGov()
        metas = extrair_por_plano_acao(
            planos,
            buscar=api.get_metas_by_plano_acao,
            rotulo="metas",
        )

        if not metas:
            raise ValueError("[api_plano_acao_meta_dag.py] Nenhuma meta foi extraída")

        return datalakehouse.gravar_raw(
            metas, schemas.FONTE_TRANSFEREGOV, schemas.TABELA_PLANO_ACAO_META
        )

    @task(outlets=[ASSET_META])
    def converter_metas_para_staging(key_raw: str) -> str:
        key = datalakehouse.raw_para_staging(key_raw)
        cadeia = datalakehouse.ler_cadeia(ASSET_PLANO_ACAO)
        datalakehouse.publicar_cadeia(
            ASSET_META, {**cadeia, schemas.TABELA_PLANO_ACAO_META: key}
        )
        return key

    converter_metas_para_staging(extrair_metas())


api_plano_acao_meta_dag()
