"""DAG de ingestao de metadados do MinC no OpenMetadata.

O codigo de apoio vive em `helpers/openmetadata/`
"""

from datetime import datetime, timedelta

from airflow.sdk import dag, task
from schedule_loader import get_dynamic_schedule

from openmetadata.config import (
    ALL_RECIPES,
    GLOSSARY_DEFINITION_PATH,
    GLOSSARY_TASK_ID,
    INGEST_GLOSSARY,
    RECIPE_PIPELINE,
)


@dag(
    dag_id="openmetadata_ingestion_dag",
    schedule=get_dynamic_schedule("openmetadata_ingestion_dag"),
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=8),
    default_args={
        "owner": "@arthrok",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "execution_timeout": timedelta(hours=3),
    },
    tags=["openmetadata", "dbt", "postgres", "metadata", "minc"],
)
def openmetadata_ingestion_dag() -> None:
    """Executa as recipes do OpenMetadata para o escopo do MinC."""

    @task(task_id="run_openmetadata_recipe_base")
    def run_openmetadata_recipe(
        recipe_path: str,
        command: str,
        replacements: dict,
        variaveis: dict,
        dbt_project_dir: str = "",
    ) -> None:
        from airflow.sdk import Variable

        from openmetadata.runner import (
            run_openmetadata_recipe as execute_openmetadata_recipe,
        )

        resolvidos = dict(replacements)
        for marcador, nome_da_variable in variaveis.items():
            resolvidos[marcador] = Variable.get(nome_da_variable)

        execute_openmetadata_recipe(
            recipe_path=recipe_path,
            command=command,
            replacements=resolvidos,
            dbt_project_dir=dbt_project_dir,
        )

    @task(task_id=GLOSSARY_TASK_ID)
    def sync_glossary(glossary_definition_path: str) -> dict:
        """Cria ou atualiza o glossario MinC antes de qualquer recipe rodar.

        Precisa vir ANTES: `meta.openmetadata.glossary` nos schema.yml apenas
        referencia termo por FQN. Quem cria o termo e este sync, e uma
        referencia a FQN que nao existe no servidor nao derruba a ingestao --
        ela e descartada em silencio, e o ativo chega ao catalogo sem o
        vinculo. Ate hoje os 26 termos foram aplicados a mao em algum momento,
        e editar `glossaries/minc.csv` nao chegava ao servidor.

        E idempotente e nao remove termo remoto: sincroniza a hierarquia
        primeiro e as relacoes depois, quando todos os FQNs ja existem.
        """
        from airflow.sdk import Variable

        from openmetadata.glossary import sync_glossary as aplicar_glossario

        return aplicar_glossario(
            glossary_definition_path=glossary_definition_path,
            host_port=Variable.get("OM_HOST"),
            jwt_token=Variable.get("INGESTION_TOKEN"),
        )

    recipe_tasks = {
        recipe.task_id: run_openmetadata_recipe.override(task_id=recipe.task_id)(
            recipe_path=recipe.recipe_path,
            command=recipe.command,
            replacements=dict(recipe.replacements),
            variaveis=dict(recipe.variaveis),
            dbt_project_dir=recipe.dbt_project_dir,
        )
        for recipe in ALL_RECIPES
    }

    previous_task = (
        sync_glossary(glossary_definition_path=GLOSSARY_DEFINITION_PATH)
        if INGEST_GLOSSARY
        else None
    )
    for task_id in RECIPE_PIPELINE:
        current_task = recipe_tasks.get(task_id)
        if current_task is None:
            continue
        if previous_task is not None:
            previous_task >> current_task
        previous_task = current_task


openmetadata_ingestion_dag()
