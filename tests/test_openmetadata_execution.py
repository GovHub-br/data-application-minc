import importlib.util
import inspect
import shutil
import sys
from types import ModuleType
from pathlib import Path

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]
EXECUTION_PATH = REPO_ROOT / "helpers/openmetadata/workflows.py"


def _openmetadata_real_instalado() -> bool:
    """O pacote de verdade, nao o `metadata` falso que os testes injetam.

    A checagem roda na importacao do modulo, antes de qualquer monkeypatch em
    `sys.modules`, entao nao confunde um com o outro.
    """
    return (
        shutil.which("metadata") is not None
        and importlib.util.find_spec("metadata") is not None
    )


# Tres testes deste arquivo sao guardas de AMBIENTE: so dizem alguma coisa com o
# openmetadata-ingestion instalado de verdade. Ele mora apenas na imagem do
# Airflow (infra/docker/airflow/requirements.lock.txt) e de proposito fica fora
# do Poetry -- resolve-lo ali rebaixaria pacotes e nao e preciso para o resto da
# suite. Sem esta marca eles falhavam no CI por ausencia do pacote, que e
# exatamente a situacao esperada ali.
#
# Para que nao virem decoracao, o job `docker_build` roda a suite DENTRO da
# imagem construida, que e onde estas guardas tem o que verificar.
requer_openmetadata_real = pytest.mark.skipif(
    not _openmetadata_real_instalado(),
    reason=(
        "openmetadata-ingestion ausente: guarda de ambiente, roda dentro da "
        "imagem do Airflow (job docker_build), nao no ambiente do Poetry"
    ),
)


def _load_execution_module():
    spec = importlib.util.spec_from_file_location(
        "openmetadata_workflows_under_test", EXECUTION_PATH
    )
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class FakeTable:
    pass


class FakeDatabase:
    pass


class FakeMetadataClient:
    def __init__(self) -> None:
        self.calls: list[dict] = []

    def list_all_entities(
        self,
        entity: type,
        fields: list[str] | None = None,
        limit: int = 100,
        params: dict | None = None,
    ) -> list:
        self.calls.append(
            {
                "entity": entity,
                "fields": fields,
                "limit": limit,
                "params": params,
            }
        )
        return []


def test_table_list_calls_use_bounded_default_page_size() -> None:
    execution = _load_execution_module()
    client = FakeMetadataClient()

    execution.set_entity_list_page_size(client, FakeTable, page_size=20)
    client.list_all_entities(entity=FakeTable, fields=["columns"])

    assert client.calls == [
        {
            "entity": FakeTable,
            "fields": ["columns"],
            "limit": 20,
            "params": None,
        }
    ]


def test_pagination_preserves_other_entities_and_explicit_limits() -> None:
    execution = _load_execution_module()
    client = FakeMetadataClient()

    execution.set_entity_list_page_size(client, FakeTable, page_size=20)
    client.list_all_entities(entity=FakeDatabase)
    client.list_all_entities(entity=FakeTable, limit=5)

    assert [call["limit"] for call in client.calls] == [100, 5]


def test_pagination_rejects_invalid_page_size() -> None:
    execution = _load_execution_module()

    with pytest.raises(ValueError, match="maior que zero"):
        execution.set_entity_list_page_size(FakeMetadataClient(), FakeTable, page_size=0)


def test_profile_command_runs_in_process(monkeypatch, tmp_path: Path) -> None:
    execution = _load_execution_module()
    recipe = {
        "source": {"type": "postgres"},
        "workflowConfig": {"raiseOnError": True},
    }
    recipe_path = tmp_path / "profiler.yaml"
    recipe_path.write_text(yaml.safe_dump(recipe), encoding="utf-8")
    received_configs: list[dict] = []

    monkeypatch.setattr(
        execution,
        "execute_profiler_workflow_in_process",
        received_configs.append,
    )

    execution.execute_metadata("profile", recipe_path)

    assert received_configs == [recipe]


def test_classifier_command_runs_in_process(monkeypatch, tmp_path: Path) -> None:
    execution = _load_execution_module()
    recipe = {
        "source": {"type": "postgres"},
        "workflowConfig": {"raiseOnError": True},
    }
    recipe_path = tmp_path / "classifier.yaml"
    recipe_path.write_text(yaml.safe_dump(recipe), encoding="utf-8")
    received_configs: list[dict] = []

    monkeypatch.setattr(
        execution,
        "execute_classifier_workflow_in_process",
        received_configs.append,
    )

    execution.execute_metadata("classify", recipe_path)

    assert received_configs == [recipe]


def test_table_workflow_installs_pagination_before_execution(monkeypatch) -> None:
    execution = _load_execution_module()
    client = FakeMetadataClient()
    workflow_config = type("FakeWorkflowConfig", (), {"successThreshold": 80})()
    workflow = type(
        "FakeWorkflowInstance",
        (),
        {"metadata": client, "workflow_config": workflow_config},
    )()

    class FakeWorkflowClass:
        @classmethod
        def create(cls, workflow_config: dict):
            assert workflow_config == {"source": {"type": "postgres"}}
            return workflow

    class FakeSdkTable:
        pass

    execute_calls: list[tuple[object, dict]] = []

    def fake_execute_workflow(*, workflow: object, config_dict: dict) -> None:
        workflow.metadata.list_all_entities(entity=FakeSdkTable, fields=["columns"])
        execute_calls.append((workflow, config_dict))

    fake_modules = {
        "metadata": ModuleType("metadata"),
        "metadata.cli": ModuleType("metadata.cli"),
        "metadata.cli.common": ModuleType("metadata.cli.common"),
        "metadata.generated": ModuleType("metadata.generated"),
        "metadata.generated.schema": ModuleType("metadata.generated.schema"),
        "metadata.generated.schema.entity": ModuleType(
            "metadata.generated.schema.entity"
        ),
        "metadata.generated.schema.entity.data": ModuleType(
            "metadata.generated.schema.entity.data"
        ),
        "metadata.generated.schema.entity.data.table": ModuleType(
            "metadata.generated.schema.entity.data.table"
        ),
    }
    fake_modules["metadata.cli.common"].execute_workflow = fake_execute_workflow
    fake_modules["metadata.generated.schema.entity.data.table"].Table = FakeSdkTable

    for module_name, module in fake_modules.items():
        monkeypatch.setitem(sys.modules, module_name, module)

    config = {"source": {"type": "postgres"}}
    execution.execute_table_workflow_in_process(
        workflow_config=config,
        workflow_class=FakeWorkflowClass,
        workflow_name="AutoClassificationWorkflow",
    )

    assert execute_calls == [(workflow, config)]
    assert client.calls[0]["entity"] is FakeSdkTable
    assert client.calls[0]["limit"] == 20
    assert workflow.workflow_config.successThreshold == 100


@requer_openmetadata_real
def test_regular_ingestion_keeps_cli_execution(monkeypatch, tmp_path: Path) -> None:
    execution = _load_execution_module()
    recipe = {"source": {"type": "postgres"}}
    recipe_path = tmp_path / "metadata.yaml"
    recipe_path.write_text(yaml.safe_dump(recipe), encoding="utf-8")
    subprocess_calls: list[tuple[list[str], dict[str, object]]] = []

    def record_subprocess_call(command: list[str], **kwargs: object) -> None:
        subprocess_calls.append((command, kwargs))

    monkeypatch.setattr(execution.subprocess, "run", record_subprocess_call)

    execution.execute_metadata("ingest", recipe_path)

    assert len(subprocess_calls) == 1
    command, kwargs = subprocess_calls[0]
    assert command[1:] == ["ingest", "-c", str(recipe_path)]
    assert kwargs["check"] is True


def test_render_fails_loudly_when_a_placeholder_has_no_value(tmp_path: Path) -> None:
    """Marcador sem valor precisa quebrar aqui, nao no OpenMetadata.

    Antes a substituicao era silenciosa: a recipe seguia com o literal
    `${INGESTION_TOKEN}` no lugar do token e o servidor respondia erro de
    autenticacao, apontando para credencial invalida em vez de para a chave
    ausente nos replacements.
    """
    from openmetadata.rendering import render_recipe

    recipe = tmp_path / "r.yaml"
    recipe.write_text("token: ${INGESTION_TOKEN}\nhost: ${OM_HOST}\n", encoding="utf-8")
    saida = tmp_path / "out"
    saida.mkdir()

    with pytest.raises(KeyError, match="INGESTION_TOKEN"):
        render_recipe(str(recipe), {"OM_HOST": "https://x/api"}, saida)


def test_render_succeeds_when_every_placeholder_has_a_value(tmp_path: Path) -> None:
    from openmetadata.rendering import render_recipe

    recipe = tmp_path / "r.yaml"
    recipe.write_text("token: ${INGESTION_TOKEN}\nhost: ${OM_HOST}\n", encoding="utf-8")
    saida = tmp_path / "out"
    saida.mkdir()

    destino = render_recipe(
        str(recipe),
        {"INGESTION_TOKEN": "jwt", "OM_HOST": "https://x/api"},
        saida,
    )

    assert destino.read_text(encoding="utf-8") == "token: jwt\nhost: https://x/api\n"


@requer_openmetadata_real
def test_pagination_patch_still_binds_to_the_real_client() -> None:
    """O patch precisa continuar pegando na biblioteca de verdade.

    `set_entity_list_page_size` sobrescreve `list_all_entities` na instancia do
    cliente, e o profiler chama justamente esse metodo. Entre 1.12 e 1.13 o call
    site ja mudou de modulo. Se um dia mudar de metodo, o patch vira no-op
    silencioso e o timeout de 60s volta parecendo problema de rede -- os outros
    testes nao pegam isso porque usam um cliente falso.
    """
    from metadata.generated.schema.entity.data.table import Table
    from metadata.ingestion.ometa.ometa_api import OpenMetadata

    assert hasattr(OpenMetadata, "list_all_entities"), (
        "OpenMetadata.list_all_entities sumiu: o patch de paginacao em "
        "helpers/openmetadata/workflows.py virou no-op."
    )

    assinatura = inspect.signature(OpenMetadata.list_all_entities)
    assert (
        "limit" in assinatura.parameters
    ), "list_all_entities nao aceita mais `limit`; a paginacao precisa de outra via."
    assert (
        assinatura.parameters["limit"].default == 100
    ), "o default do cliente mudou; reveja se o patch ainda e necessario."

    # E o fetcher do profiler continua chamando sem passar limit?
    from metadata.profiler.source.fetcher import fetcher_strategy

    fonte = inspect.getsource(fetcher_strategy)
    trecho = fonte[fonte.index("tables = self.metadata.list_all_entities(") :][:400]
    assert "entity=Table" in trecho
    assert (
        "limit=" not in trecho.split(")")[0]
    ), "o fetcher passou a definir `limit`; o patch pode ter ficado redundante."


@requer_openmetadata_real
def test_metadata_cli_is_resolvable_in_this_environment() -> None:
    """O CLI `metadata` precisa existir no PATH de verdade.

    Os outros testes de execucao monkeypatcham subprocess.run, entao o caminho
    do binario nunca era resolvido -- e a primeira execucao real falhou com
    FileNotFoundError em /usr/python/bin/metadata, porque o codigo derivava o
    caminho de sys.executable, herdado de quando a task rodava num virtualenv.
    """
    import shutil

    caminho = shutil.which("metadata")

    assert caminho is not None, (
        "CLI `metadata` ausente do PATH. Ele vem do openmetadata-ingestion, "
        "que precisa estar em infra/docker/airflow/requirements.lock.txt."
    )


def test_cli_path_does_not_depend_on_the_interpreter_location() -> None:
    """O task runner do Airflow roda com outro python que nao o do pacote.

    Derivar o caminho do binario de sys.executable so funciona quando os dois
    moram na mesma pasta -- verdade dentro de um virtualenv, falso na imagem.
    """
    import ast

    fonte = (
        Path(__file__).resolve().parents[1] / "helpers/openmetadata/workflows.py"
    ).read_text(encoding="utf-8")

    # AST, e nao busca textual: o docstring da funcao cita `sys.executable` de
    # proposito, para registrar o que mudou e por que.
    usa_sys_executable = any(
        isinstance(no, ast.Attribute)
        and no.attr == "executable"
        and isinstance(no.value, ast.Name)
        and no.value.id == "sys"
        for no in ast.walk(ast.parse(fonte))
    )

    assert not usa_sys_executable, (
        "workflows.py voltou a derivar o caminho do CLI de sys.executable; "
        "use shutil.which para achar o binario no PATH."
    )


def test_metadata_task_does_not_materialize_models() -> None:
    """Catalogar nao pode rodar `dbt build`.

    O acoplamento custou caro numa execucao real: 16 modelos que leem um schema
    inexistente derrubaram a task, e a documentacao dos outros 865 nunca chegou
    ao OpenMetadata. Materializar e trabalho do minc_cosmos_dag.
    """
    from openmetadata.dbt_artifacts import DBT_COMMANDS

    comandos = {" ".join(c) for c in DBT_COMMANDS}

    assert "build" not in comandos, (
        "a task de metadados voltou a rodar `dbt build`; um modelo quebrado "
        "passa a bloquear a documentacao de todo o projeto."
    )
    assert "run" not in comandos and "seed" not in comandos
    assert "docs generate" in comandos


def test_only_the_manifest_is_required() -> None:
    """catalog.json e run_results.json enriquecem, mas nao podem bloquear.

    No schema do OpenMetadata so dbtManifestFilePath e obrigatorio.
    """
    from openmetadata.dbt_artifacts import (
        OPTIONAL_DBT_ARTIFACTS,
        REQUIRED_DBT_ARTIFACTS,
    )

    assert REQUIRED_DBT_ARTIFACTS == ("manifest.json",)
    assert set(OPTIONAL_DBT_ARTIFACTS) == {"catalog.json", "run_results.json"}
