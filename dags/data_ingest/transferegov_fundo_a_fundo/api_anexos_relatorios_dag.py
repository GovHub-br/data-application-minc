import logging
from datetime import datetime, timedelta
from airflow.sdk import dag, task
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

import schemas_minc as schemas
from openmetadata.lineage import publicar_linhagem, tabela
from cliente_postgres import ClientPostgresDB
from cliente_transferegov_fundo_a_fundo import ClienteTransfereGovBackend
from postgres_helpers import get_postgres_conn


default_args = {
    "owner": "Caio Borges",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="api_anexos_relatorios_dag",
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["minc", "transferegov", "anexos", "raw"],
)
def api_anexos_relatorios_dag() -> None:
    @task(
        inlets=[tabela(schemas.SCHEMA_TRANSFEREGOV, schemas.TABELA_RELATORIO_GESTAO)],
        outlets=[tabela(schemas.SCHEMA_TRANSFEREGOV, schemas.TABELA_ANEXO_RELATORIO)],
    )
    def fetch_and_load_anexos_relatorios() -> None:
        logging.info("[api_anexos_relatorios_dag.py] Iniciando extração de anexos de relatórios")

        db = ClientPostgresDB(get_postgres_conn())
        ids_relatorios = db.get_id_relatorios_gestao(
            schema=schemas.SCHEMA_TRANSFEREGOV,
            table_name=schemas.TABELA_RELATORIO_GESTAO,
        )

        if not ids_relatorios:
            raise ValueError(
                "[api_anexos_relatorios_dag.py] Nenhum relatório de gestão encontrado"
            )

        api = ClienteTransfereGovBackend()
        total_inseridos = 0

        for id_relatorio in ids_relatorios:
            logging.info(
                "[api_anexos_relatorios_dag.py] Buscando anexos para relatório ID: %s",
                id_relatorio,
            )

            anexos_raw = api.get_anexos_relatorio(int(id_relatorio))

            if not anexos_raw:
                logging.warning(
                    "[api_anexos_relatorios_dag.py] Nenhum anexo encontrado para relatório ID: %s",
                    id_relatorio,
                )
                continue

            for anexo in anexos_raw:
                anexo["id_relatorio_gestao"] = id_relatorio
                anexo["dt_ingest"] = datetime.now().isoformat()

            db.insert_data(
                anexos_raw,
                table_name=schemas.TABELA_ANEXO_RELATORIO,
                primary_key=["id"],
                conflict_fields=["id"],
                schema=schemas.SCHEMA_TRANSFEREGOV,
            )

            total_inseridos += len(anexos_raw)
            logging.info(
                "[api_anexos_relatorios_dag.py] Relatório %s: %d anexos inseridos",
                id_relatorio,
                len(anexos_raw),
            )

        if total_inseridos == 0:
            raise ValueError("[api_anexos_relatorios_dag.py] Nenhum anexo foi extraído")

        logging.info(
            "[api_anexos_relatorios_dag.py] Extração e carga concluídas com %s registros no total",
            total_inseridos,
        )

    trigger_download = TriggerDagRunOperator(
        task_id="trigger_download_anexos",
        trigger_dag_id="download_anexos_transferegov_dag",
        wait_for_completion=False,
    )

    fetch_and_load_anexos_relatorios() >> [trigger_download, publicar_linhagem()]


api_anexos_relatorios_dag()
