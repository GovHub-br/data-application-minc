import logging
from datetime import datetime, timedelta
from typing import Any

from airflow.sdk import dag, task
from airflow.sdk import Variable
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

import datalakehouse
import schemas_minc as schemas
from cliente_transferegov_fundo_a_fundo import ClienteTransfereGov
from schedule_loader import get_dynamic_schedule


default_args = {
    "owner": "Caio Borges",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

# Filtra por id_programa e nao por codigo_programa: a API publica devolve 500
# ao filtrar /programas por codigo (bug do lado do servidor), e o id ja e a
# identificador do programa no staging.
_URL_CONSULTA_PROGRAMA = (
    "https://api-publica.transferegov.gestao.gov.br/fundoafundo/programas?id_programa={}"
)


def _politica_por_id_programa() -> dict[int, dict[str, str]]:
    """Mapeia ``id_programa`` -> ``sigla``/``politica_publica`` a partir da
    Variable ``transferegov_politicas_publicas``.

    A secao 7.2 exige ``sigla`` e ``politica_publica`` em ``programa_minc``,
    mas o endpoint ``/programa`` nao devolve nenhum dos dois -- essa relacao
    e decisao do MinC, nao dado da origem. Como o repositorio nao versiona
    catalogo, a Variable e a unica fonte possivel. Formato esperado::

        [{"sigla": "LPG",
          "politica_publica": "LEI PAULO GUSTAVO (2022)",
          "id_programas": [46, 47]}, ...]
    """
    politicas = Variable.get(
        "transferegov_politicas_publicas",
        default=[],
        deserialize_json=True,
    )

    if not politicas:
        logging.warning(
            "[api_programas_dag.py] Variable 'transferegov_politicas_publicas' "
            "nao configurada — 'sigla' e 'politica_publica' ficarao nulas no "
            "staging de %s",
            schemas.TABELA_PROGRAMA,
        )

    return {
        int(id_programa): {
            "sigla": politica["sigla"],
            "politica_publica": politica["politica_publica"],
        }
        for politica in politicas
        for id_programa in politica.get("id_programas", [])
    }


@dag(
    dag_id="api_programas_dag",
    schedule=get_dynamic_schedule("api_programas_dag"),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["minc", "transferegov", "programas", "raw"],
)
def api_programas_dag() -> None:
    @task
    def extrair_programas() -> str:
        """Busca os programas do escopo e grava a resposta da API em ``raw/``."""
        logging.info("[api_programas_dag.py] Iniciando extração de programas")
        ids_alvo = Variable.get(
            "transferegov_programas_ids",
            default=schemas.PROGRAMAS_IDS_PADRAO,
            deserialize_json=True,
        )

        api = ClienteTransfereGov()
        programas_data: list[dict[str, Any]] = []

        for id_programa in ids_alvo:
            logging.info("[api_programas_dag.py] Buscando programa ID: %s", id_programa)
            programa = api.get_programa_by_id(int(id_programa))

            if programa:
                programas_data.append(programa)
            else:
                logging.warning(
                    "[api_programas_dag.py] Programa não encontrado para ID: %s",
                    id_programa,
                )

        if not programas_data:
            raise ValueError("[api_programas_dag.py] Nenhum programa foi extraído")

        # Validacao 12.1: programa no escopo que a API nao devolveu.
        if len(programas_data) < len(ids_alvo):
            logging.warning(
                "[api_programas_dag.py] %d de %d programas do escopo não foram "
                "encontrados na API",
                len(ids_alvo) - len(programas_data),
                len(ids_alvo),
            )

        return datalakehouse.gravar_raw(
            programas_data, schemas.FONTE_TRANSFEREGOV, schemas.TABELA_PROGRAMA
        )

    @task
    def converter_programas_para_staging(key_raw: str) -> str:
        """Gera o Parquet de staging, com os campos que não vêm da API.

        ``sigla``, ``politica_publica`` e ``url_consulta`` são obrigatórios na
        seção 7.2, mas são decisão do MinC, não dado da origem -- por isso
        entram aqui e não no raw. O ``codigo_programa`` vem no payload e é
        mantido como texto (é identificador de negócio, não número).
        """
        politicas = _politica_por_id_programa()

        def enriquecer(programa: dict[str, Any]) -> dict[str, Any]:
            id_programa = int(programa["id_programa"])
            politica = politicas.get(id_programa, {})
            if not politica:
                logging.warning(
                    "[api_programas_dag.py] Programa %s sem política pública "
                    "mapeada na Variable 'transferegov_politicas_publicas'",
                    id_programa,
                )
            return {
                "sigla": politica.get("sigla"),
                "politica_publica": politica.get("politica_publica"),
                "url_consulta": _URL_CONSULTA_PROGRAMA.format(id_programa),
            }

        return datalakehouse.raw_para_staging(key_raw, enriquecer=enriquecer)

    staging = converter_programas_para_staging(extrair_programas())

    trigger_planos_acao = TriggerDagRunOperator(
        task_id="trigger_planos_acao",
        trigger_dag_id="api_planos_acao_dag",
        wait_for_completion=False,
    )

    staging >> trigger_planos_acao


api_programas_dag()
