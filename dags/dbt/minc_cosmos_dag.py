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
    # install_dbt_deps vem True por padrao. Com isso o Cosmos chama
    # has_non_empty_dependencies_file() uma vez por operator -- 1212 vezes a cada
    # parse -- e cada chamada emite um INFO "Project ... does not have
    # {'dependencies.yml', 'packages.yml'}". Medido em 06/10/2026: 1213 dessas
    # linhas por log de task, 538KB de 572KB, ou 94% do arquivo. Somado as 30
    # execucoes retidas deu 25,3GB e lotou o disco da VM (100%, 0 byte livre),
    # derrubando o Airflow.
    #
    # O projeto nao tem packages.yml nem dependencies.yml: nao ha o que instalar.
    # Criar um packages.yml silenciaria o aviso pelo caminho errado -- a funcao
    # passaria a retornar True e o Cosmos executaria `dbt deps` de verdade em
    # cada uma das 1212 tasks. Desligar e o correto.
    #
    # Nao usar operator_args['install_deps'] nem RenderConfig.dbt_deps: ambos
    # estao deprecados desde o Cosmos 1.9. Este campo sincroniza os dois sozinho
    # (converter.py:250 e :260), evitando o erro de consistencia que o
    # LoadMode.DBT_LS exige entre eles.
    project_config=ProjectConfig(dbt_project_path, install_dbt_deps=False),
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
