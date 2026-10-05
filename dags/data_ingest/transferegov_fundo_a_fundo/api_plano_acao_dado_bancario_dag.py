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
ASSET_DADO_BANCARIO = datalakehouse.asset_staging(
    schemas.FONTE_TRANSFEREGOV, schemas.TABELA_PLANO_ACAO_DADO_BANCARIO
)

default_args = {
    "owner": "Caio Borges",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="api_plano_acao_dado_bancario_dag",
    schedule=[ASSET_PLANO_ACAO],
    start_date=datetime(2023, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["minc", "transferegov", "dado_bancario", "raw"],
)
def api_plano_acao_dado_bancario_dag() -> None:
    """Passo 3B da secao 6: contas bancarias de cada plano de acao.

    Grava **todas** as contas de cada plano, com todas as colunas da origem,
    em ``plano_acao_dado_bancario_minc`` (raw e staging do datalakehouse).
    Antes essa informacao era efeito colateral da DAG do BB Agil, que
    guardava uma unica conta por plano e apenas quatro campos -- perdendo
    tanto a granularidade ("uma linha por registro de conta bancaria", secao
    7.1) quanto colunas da origem.

    Escolher qual conta consultar no BB Agil continua existindo, mas como
    regra de consumo, dentro de ``extracao_bbagil_dag`` -- nao mais como
    filtro na ingestao.
    """

    @task
    def extrair_dados_bancarios() -> str:
        cadeia = datalakehouse.ler_cadeia(ASSET_PLANO_ACAO)
        planos = carregar_planos_acao(
            datalakehouse.ler_parquet(
                datalakehouse.key_da_cadeia(cadeia, schemas.TABELA_PLANO_ACAO)
            )
        )

        if not planos:
            raise ValueError(
                "[api_plano_acao_dado_bancario_dag.py] Nenhum plano de ação no "
                f"staging de {schemas.TABELA_PLANO_ACAO}"
            )

        logging.info(
            "[api_plano_acao_dado_bancario_dag.py] Buscando contas de %d planos "
            "de ação",
            len(planos),
        )

        api = ClienteTransfereGov()
        contas = extrair_por_plano_acao(
            planos,
            buscar=api.get_dados_bancarios_by_plano_acao,
            rotulo="dados bancários",
        )

        if not contas:
            raise ValueError(
                "[api_plano_acao_dado_bancario_dag.py] Nenhuma conta bancária "
                "foi extraída"
            )

        # Validacao 12.5: conta sem agencia/numero nao serve para consultar o
        # extrato. Continua sendo gravada (e dado da origem), mas contada.
        inutilizaveis = sum(
            1
            for conta in contas
            if not conta.get("numero_agencia_plano_acao_dado_bancario")
            or not conta.get("numero_conta_plano_acao_dado_bancario")
        )
        if inutilizaveis:
            logging.warning(
                "[api_plano_acao_dado_bancario_dag.py] %d contas sem agência ou "
                "número utilizáveis para consulta no BB Ágil",
                inutilizaveis,
            )

        return datalakehouse.gravar_raw(
            contas, schemas.FONTE_TRANSFEREGOV, schemas.TABELA_PLANO_ACAO_DADO_BANCARIO
        )

    @task(outlets=[ASSET_DADO_BANCARIO])
    def converter_dados_bancarios_para_staging(key_raw: str) -> str:
        key = datalakehouse.raw_para_staging(key_raw)
        cadeia = datalakehouse.ler_cadeia(ASSET_PLANO_ACAO)
        datalakehouse.publicar_cadeia(
            ASSET_DADO_BANCARIO, {**cadeia, schemas.TABELA_PLANO_ACAO_DADO_BANCARIO: key}
        )
        return key

    converter_dados_bancarios_para_staging(extrair_dados_bancarios())


api_plano_acao_dado_bancario_dag()
