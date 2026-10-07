import logging
from datetime import datetime, timedelta
from typing import Any

from airflow.sdk import dag, task

import datalakehouse
import schemas_minc as schemas
from cliente_transferegov_fundo_a_fundo import ClienteTransfereGov


ASSET_PLANO_ACAO = datalakehouse.asset_staging(
    schemas.FONTE_TRANSFEREGOV, schemas.TABELA_PLANO_ACAO
)
# Evento que dispara a api_anexos_relatorios_dag.
ASSET_RELATORIOS = datalakehouse.asset_staging(
    schemas.FONTE_TRANSFEREGOV, schemas.TABELA_RELATORIO_GESTAO
)

default_args = {
    "owner": "Caio Borges",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="api_relatorios_gestao_dag",
    schedule=[ASSET_PLANO_ACAO],
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

        cadeia = datalakehouse.ler_cadeia(ASSET_PLANO_ACAO)
        planos = datalakehouse.ler_parquet(
            datalakehouse.key_da_cadeia(cadeia, schemas.TABELA_PLANO_ACAO)
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

    @task(outlets=[ASSET_RELATORIOS])
    def converter_relatorios_para_staging(key_raw: str) -> str:
        key = datalakehouse.raw_para_staging(key_raw)
        cadeia = datalakehouse.ler_cadeia(ASSET_PLANO_ACAO)
        datalakehouse.publicar_cadeia(
            ASSET_RELATORIOS, {**cadeia, schemas.TABELA_RELATORIO_GESTAO: key}
        )
        return key

    converter_relatorios_para_staging(extrair_relatorios_gestao())


api_relatorios_gestao_dag()
