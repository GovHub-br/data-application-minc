import logging
from datetime import datetime, timedelta

from airflow.sdk import dag, task
from airflow.sdk import Variable
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

import datalakehouse
import schemas_minc as schemas
from cliente_transferegov_fundo_a_fundo import ClienteTransfereGov
from schedule_loader import get_dynamic_schedule
from territorio_ibge import derivar_territorio


default_args = {
    "owner": "Wallyson Souza",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="api_planos_acao_dag",
    schedule=get_dynamic_schedule("api_planos_acao_dag"),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["minc", "transferegov", "planos_acao", "raw"],
)
def api_planos_acao_dag() -> None:
    @task
    def extrair_planos_acao() -> str:
        """Busca os planos de ação de cada programa e grava em ``raw/``.

        Só a key do arquivo volta por XCom: a lista de milhares de planos
        estoura o IPC do Airflow 3.x.
        """
        logging.info("[api_planos_acao_dag.py] Iniciando extração de planos de ação")

        ids_alvo = Variable.get(
            "transferegov_programas_ids",
            default=schemas.PROGRAMAS_IDS_PADRAO,
            deserialize_json=True,
        )

        api = ClienteTransfereGov()
        planos_data: list[dict] = []

        for id_programa in ids_alvo:
            logging.info(
                "[api_planos_acao_dag.py] Buscando planos de ação para programa ID: %s",
                id_programa,
            )
            planos = api.get_planos_acao_by_programa(int(id_programa))

            if planos:
                planos_data.extend(planos)
                logging.info(
                    "[api_planos_acao_dag.py] Programa %s: %d planos extraídos",
                    id_programa,
                    len(planos),
                )
            else:
                logging.warning(
                    "[api_planos_acao_dag.py] Nenhum plano encontrado para programa ID: %s",
                    id_programa,
                )

        if not planos_data:
            raise ValueError("[api_planos_acao_dag.py] Nenhum plano de ação foi extraído")

        return datalakehouse.gravar_raw(
            planos_data, schemas.FONTE_TRANSFEREGOV, schemas.TABELA_PLANO_ACAO
        )

    @task
    def converter_planos_acao_para_staging(key_raw: str) -> str:
        # Campos territoriais da secao 7.1. Sem isso o plano ESTADUAL fica com
        # o codigo IBGE do municipio da capital, que e o que a validacao 12.7
        # proibe. Entram so no staging: o raw e a resposta da API intocada.
        return datalakehouse.raw_para_staging(key_raw, enriquecer=derivar_territorio)

    trigger_relatorios = TriggerDagRunOperator(
        task_id="trigger_relatorios",
        trigger_dag_id="api_relatorios_gestao_dag",
        wait_for_completion=False,
    )

    # Metas e dados bancarios sao os dois ramos que dependem so do plano de
    # acao (passos 3A e 3B da secao 6) — disparados em paralelo com os
    # relatorios de gestao, que seguem o proprio ramo (passo 6).
    trigger_metas = TriggerDagRunOperator(
        task_id="trigger_metas",
        trigger_dag_id="api_plano_acao_meta_dag",
        wait_for_completion=False,
    )

    trigger_dado_bancario = TriggerDagRunOperator(
        task_id="trigger_dado_bancario",
        trigger_dag_id="api_plano_acao_dado_bancario_dag",
        wait_for_completion=False,
    )

    staging = converter_planos_acao_para_staging(extrair_planos_acao())
    staging >> [trigger_relatorios, trigger_metas, trigger_dado_bancario]


api_planos_acao_dag()
