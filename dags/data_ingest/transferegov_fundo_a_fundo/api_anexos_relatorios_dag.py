import logging
from datetime import datetime, timedelta
from airflow.sdk import dag, task

import datalakehouse
import schemas_minc as schemas
from cliente_transferegov_fundo_a_fundo import ClienteTransfereGovBackend


ASSET_RELATORIOS = datalakehouse.asset_staging(
    schemas.FONTE_TRANSFEREGOV, schemas.TABELA_RELATORIO_GESTAO
)
# Evento que dispara a download_anexos_transferegov_dag.
ASSET_ANEXOS = datalakehouse.asset_staging(
    schemas.FONTE_TRANSFEREGOV, schemas.TABELA_ANEXO_RELATORIO
)

default_args = {
    "owner": "Caio Borges",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="api_anexos_relatorios_dag",
    schedule=[ASSET_RELATORIOS],
    start_date=datetime(2023, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["minc", "transferegov", "anexos", "raw"],
)
def api_anexos_relatorios_dag() -> None:
    @task
    def extrair_anexos_relatorios() -> str:
        logging.info(
            "[api_anexos_relatorios_dag.py] Iniciando extração de anexos de relatórios"
        )

        cadeia = datalakehouse.ler_cadeia(ASSET_RELATORIOS)
        relatorios = datalakehouse.ler_parquet(
            datalakehouse.key_da_cadeia(cadeia, schemas.TABELA_RELATORIO_GESTAO)
        )
        ids_relatorios = relatorios["id_relatorio_gestao"].dropna().unique().tolist()

        if not ids_relatorios:
            raise ValueError(
                "[api_anexos_relatorios_dag.py] Nenhum relatório de gestão encontrado"
            )

        api = ClienteTransfereGovBackend()
        anexos_data: list[dict] = []

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

            # O payload do anexo nao traz o relatorio de onde veio; sem isso o
            # raw nao se liga de volta ao plano de acao.
            for anexo in anexos_raw:
                anexo["id_relatorio_gestao"] = id_relatorio

            anexos_data.extend(anexos_raw)
            logging.info(
                "[api_anexos_relatorios_dag.py] Relatório %s: %d anexos encontrados",
                id_relatorio,
                len(anexos_raw),
            )

        if not anexos_data:
            raise ValueError("[api_anexos_relatorios_dag.py] Nenhum anexo foi extraído")

        logging.info(
            "[api_anexos_relatorios_dag.py] Extração concluída com %s registros no total",
            len(anexos_data),
        )
        return datalakehouse.gravar_raw(
            anexos_data, schemas.FONTE_TRANSFEREGOV, schemas.TABELA_ANEXO_RELATORIO
        )

    @task(outlets=[ASSET_ANEXOS])
    def converter_anexos_para_staging(key_raw: str) -> str:
        key = datalakehouse.raw_para_staging(key_raw)
        cadeia = datalakehouse.ler_cadeia(ASSET_RELATORIOS)
        datalakehouse.publicar_cadeia(
            ASSET_ANEXOS, {**cadeia, schemas.TABELA_ANEXO_RELATORIO: key}
        )
        return key

    converter_anexos_para_staging(extrair_anexos_relatorios())


api_anexos_relatorios_dag()
