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
    project_config=ProjectConfig(dbt_project_path),
    # O Cosmos monta a DAG rodando `dbt ls` no parse. Medido em 06/09/2026 nos
    # 647 modelos ativos: 102s sem partial parse, 34s com ele. Os dois estouram
    # o core.dagbag_import_timeout default de 30s, entao infra/docker-compose.yml
    # sobe os dois timeouts de parse -- sem eles a DAG desaparece da UI.
    #
    # Nao paga isso a cada ciclo: o Cosmos 1.14 tem enable_cache_dbt_ls ligado
    # por padrao, hasheia o conteudo do projeto (was_project_modified) e so
    # re-executa o `dbt ls` quando algum arquivo do dbt muda.
    #
    # Confirmado que os dois modos montam a MESMA DAG: 1212 tasks, mesmos
    # task_ids, comparado contra o manifest de 55fefcc.
    #
    # A alternativa era LoadMode.DBT_MANIFEST lendo um manifest.json
    # versionado. Saiu em 06/09/2026 -- ver ADR 0007.
    render_config=RenderConfig(load_method=LoadMode.DBT_LS),
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
