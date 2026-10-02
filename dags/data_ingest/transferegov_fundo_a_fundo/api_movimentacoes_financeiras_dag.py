import logging
from datetime import datetime, timedelta
from typing import Any

from airflow.sdk import dag, task

import datalakehouse
import schemas_minc as schemas
from cliente_transferegov_fundo_a_fundo import ClienteTransfereGov
from schedule_loader import get_dynamic_schedule


default_args = {
    "owner": "Caio Borges",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="api_movimentacoes_financeiras_dag",
    schedule=get_dynamic_schedule("api_movimentacoes_financeiras_dag"),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["minc", "transferegov", "gestao_financeira", "raw"],
)
def api_movimentacoes_financeiras_dag() -> None:
    """DAG de ingestao dos endpoints ``/gestao_financeira_lancamentos`` e
    ``/gestao_financeira_subtransacoes`` do Transferegov Fundo a Fundo.

    Fluxo API -> raw (JSON) -> staging (Parquet) no datalakehouse, em dois
    pares sequenciais:

    1. ``extrair_lancamentos`` -> ``converter_lancamentos_para_staging``
       Busca TODOS os lancamentos financeiros em bloco (paginacao simples,
       sem filtro por FK -- o endpoint nao possui ``id_plano_acao`` no
       payload, confirmado via Swagger oficial). O cruzamento com plano de
       acao (via ``cnpj_ente_solicitante_gestao_financeira`` ou pelo
       endpoint-ponte ``/plano_acao_dado_bancario``) fica para a
       transformacao, fora do escopo desta DAG.
    2. ``extrair_subtransacoes`` -> ``converter_subtransacoes_para_staging``
       Le os ``id_lancamento_gestao_financeira`` do staging de lancamentos
       gerado **neste mesmo run** (key recebida por XCom) e busca as
       subtransacoes de cada um.

    Toda a logica de request/paginacao vive em
    ``cliente_transferegov_fundo_a_fundo.ClienteTransfereGov`` -- esta DAG
    contem apenas orquestracao e movimentacao de dados.
    """

    @task
    def extrair_lancamentos() -> str:
        api = ClienteTransfereGov()
        lancamentos_data = api.get_lancamentos_financeiros()

        if not lancamentos_data:
            raise ValueError(
                "[api_movimentacoes_financeiras_dag.py] Nenhum lancamento "
                "financeiro foi extraido"
            )

        logging.info(
            "[api_movimentacoes_financeiras_dag.py] %d lancamentos "
            "financeiros extraidos",
            len(lancamentos_data),
        )
        return datalakehouse.gravar_raw(
            lancamentos_data,
            schemas.FONTE_TRANSFEREGOV,
            schemas.ENTIDADE_LANCAMENTOS,
        )

    @task
    def converter_lancamentos_para_staging(key_raw: str) -> str:
        return datalakehouse.raw_para_staging(key_raw)

    @task
    def extrair_subtransacoes(key_staging_lancamentos: str) -> str:
        """Busca as subtransacoes dos lancamentos extraidos neste run."""
        lancamentos = datalakehouse.ler_parquet(key_staging_lancamentos)
        ids_lancamentos = (
            lancamentos["id_lancamento_gestao_financeira"].dropna().unique().tolist()
        )

        if not ids_lancamentos:
            raise ValueError(
                "[api_movimentacoes_financeiras_dag.py] Nenhum lancamento "
                f"encontrado em {key_staging_lancamentos}"
            )

        api = ClienteTransfereGov()
        subtransacoes_data: list[dict[str, Any]] = []

        for id_lancamento in ids_lancamentos:
            logging.info(
                "[api_movimentacoes_financeiras_dag.py] Buscando subtransacoes "
                "para lancamento ID: %s",
                id_lancamento,
            )
            subtransacoes = api.get_subtransacoes_by_lancamento(int(id_lancamento))

            if subtransacoes:
                subtransacoes_data.extend(subtransacoes)
                logging.info(
                    "[api_movimentacoes_financeiras_dag.py] Lancamento %s: %d "
                    "subtransacoes encontradas",
                    id_lancamento,
                    len(subtransacoes),
                )
            else:
                logging.warning(
                    "[api_movimentacoes_financeiras_dag.py] Nenhuma subtransacao "
                    "encontrada para lancamento ID: %s",
                    id_lancamento,
                )

        if not subtransacoes_data:
            raise ValueError(
                "[api_movimentacoes_financeiras_dag.py] Nenhuma subtransacao "
                "foi extraida"
            )

        return datalakehouse.gravar_raw(
            subtransacoes_data,
            schemas.FONTE_TRANSFEREGOV,
            schemas.ENTIDADE_SUBTRANSACOES,
        )

    @task
    def converter_subtransacoes_para_staging(key_raw: str) -> str:
        return datalakehouse.raw_para_staging(key_raw)

    staging_lancamentos = converter_lancamentos_para_staging(extrair_lancamentos())
    converter_subtransacoes_para_staging(extrair_subtransacoes(staging_lancamentos))


api_movimentacoes_financeiras_dag()
