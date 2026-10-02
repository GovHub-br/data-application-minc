import logging
from datetime import datetime, timedelta
from typing import Any

from airflow.sdk import dag, task
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

import datalakehouse
import schemas_minc as schemas
from cliente_transferegov_fundo_a_fundo import ClienteTransfereGov


default_args = {
    "owner": "Caio Borges",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="api_relatorios_gestao_dag",
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["minc", "transferegov", "relatorios", "raw"],
)
def api_relatorios_gestao_dag() -> None:
    @task
    def extrair_relatorios_gestao() -> str:
        logging.info(
            "[api_relatorios_gestao_dag.py] Iniciando extração de relatórios de gestão"
        )

        planos = datalakehouse.ler_staging_recente(
            schemas.FONTE_TRANSFEREGOV, schemas.TABELA_PLANO_ACAO
        )
        ids_planos = planos["id_plano_acao"].dropna().unique().tolist()

        if not ids_planos:
            raise ValueError(
                "[api_relatorios_gestao_dag.py] Nenhum plano de ação encontrado"
            )

        api = ClienteTransfereGov()
        relatorios_data: list[dict[str, Any]] = []

        for id_plano in ids_planos:
            logging.info(
                "[api_relatorios_gestao_dag.py] Buscando relatórios para plano ID: %s",
                id_plano,
            )

            relatorios_raw = api.get_relatorios_by_plano_acao(int(id_plano))

            if relatorios_raw:
                relatorios_finais = [
                    r for r in relatorios_raw if r.get("tipo_relatorio_gestao") == "FINAL"
                ]
                relatorios_data.extend(relatorios_finais)

                logging.info(
                    "[api_relatorios_gestao_dag.py] Plano %s: %d relatórios FINAL encontrados",
                    id_plano,
                    len(relatorios_finais),
                )
            else:
                logging.warning(
                    "[api_relatorios_gestao_dag.py] Nenhum relatório encontrado para plano ID: %s",
                    id_plano,
                )

        if not relatorios_data:
            raise ValueError(
                "[api_relatorios_gestao_dag.py] Nenhum relatório foi extraído"
            )

        logging.info(
            "[api_relatorios_gestao_dag.py] Extração concluída com %s registros",
            len(relatorios_data),
        )
        return datalakehouse.gravar_raw(
            relatorios_data, schemas.FONTE_TRANSFEREGOV, schemas.TABELA_RELATORIO_GESTAO
        )

    @task
    def converter_relatorios_para_staging(key_raw: str) -> str:
        return datalakehouse.raw_para_staging(key_raw)

    trigger_anexos = TriggerDagRunOperator(
        task_id="trigger_anexos",
        trigger_dag_id="api_anexos_relatorios_dag",
        wait_for_completion=False,
    )

    converter_relatorios_para_staging(extrair_relatorios_gestao()) >> trigger_anexos


api_relatorios_gestao_dag()
