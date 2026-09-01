import os
from datetime import datetime

from cosmos import DbtDag, ExecutionConfig, ProfileConfig, ProjectConfig, RenderConfig
from cosmos.constants import DBT_LOG_PATH_ENVVAR, LoadMode


dbt_log_path = "/tmp/dbt_logs"
os.makedirs(dbt_log_path, exist_ok=True)
os.environ[DBT_LOG_PATH_ENVVAR] = dbt_log_path

profile_config = ProfileConfig(
    profiles_yml_filepath=f"{os.environ['AIRFLOW_REPO_BASE']}/dbt/minc/profiles.yml",
    profile_name="minc",
    target_name="prod",
)

dbt_project_path = f"{os.environ['AIRFLOW_REPO_BASE']}/dbt/minc"

minc_cosmos_dag = DbtDag(
    project_config=ProjectConfig(
        dbt_project_path,
        # Manifest versionado, gerado por scripts/gerar_manifest_dbt.sh.
        manifest_path=f"{dbt_project_path}/manifest.json",
    ),
    # Sem isto o Cosmos monta a DAG rodando `dbt ls` no parse. Com o tamanho
    # atual do projeto isso leva ~35s e estoura o timeout do dag processor --
    # a DAG desaparece da UI. Lendo o manifest o parse e imediato.
    #
    # O preco: o manifest precisa ser regerado e commitado a cada mudanca no
    # projeto dbt. tests/test_dbt_manifest.py falha quando os dois divergem.
    render_config=RenderConfig(load_method=LoadMode.DBT_MANIFEST),
    profile_config=profile_config,
    execution_config=ExecutionConfig(
        dbt_executable_path=f"{os.environ['AIRFLOW_REPO_BASE']}/.local/bin/dbt",
    ),
    schedule="0 1 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    dag_id="minc_cosmos_dag",
    default_args={"retries": 2},
)
