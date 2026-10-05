import logging
from datetime import datetime, timedelta

from airflow.sdk import dag, task
from airflow.sdk import Variable

import datalakehouse
import schemas_minc as schemas
from cliente_transferegov_fundo_a_fundo import ClienteTransfereGov
from territorio_ibge import derivar_territorio


ASSET_PROGRAMA = datalakehouse.asset_staging(
    schemas.FONTE_TRANSFEREGOV, schemas.TABELA_PROGRAMA
)
# Evento que dispara metas, dado bancario e relatorios de gestao (passos 3A,
# 3B e 6 da secao 6), com a key deste run no extra.
ASSET_PLANO_ACAO = datalakehouse.asset_staging(
    schemas.FONTE_TRANSFEREGOV, schemas.TABELA_PLANO_ACAO
)

default_args = {
    "owner": "Wallyson Souza",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="api_planos_acao_dag",
    # Roda depois de cada carga de programas, e nao por cron proprio: o
    # agendamento e o da api_programas_dag.
    schedule=[ASSET_PROGRAMA],
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

    @task(outlets=[ASSET_PLANO_ACAO])
    def converter_planos_acao_para_staging(key_raw: str) -> str:
        # Campos territoriais da secao 7.1. Sem isso o plano ESTADUAL fica com
        # o codigo IBGE do municipio da capital, que e o que a validacao 12.7
        # proibe. Entram so no staging: o raw e a resposta da API intocada.
        key = datalakehouse.raw_para_staging(key_raw, enriquecer=derivar_territorio)
        cadeia = datalakehouse.ler_cadeia(ASSET_PROGRAMA)
        datalakehouse.publicar_cadeia(
            ASSET_PLANO_ACAO, {**cadeia, schemas.TABELA_PLANO_ACAO: key}
        )
        return key

    converter_planos_acao_para_staging(extrair_planos_acao())


api_planos_acao_dag()
