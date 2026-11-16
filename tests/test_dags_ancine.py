"""Fiação da DAG do cadastro de salas de exibição da ANCINE (Eixo 5).

Carrega a DAG de verdade e confere o que um refactor pode quebrar sem que
nenhum teste de função perceba: a ordem raw → staging → seeds, as tabelas
declaradas para a linhagem, o agendamento mensal e o parâmetro da versão.

Como em ``test_dags_fontes_eixo1.py``: sem ``airflow.sdk`` o arquivo é pulado,
e sem o OpenMetadata (que só existe na imagem do Airflow) os testes que
carregam a DAG também; eles rodam no job ``docker_build``.
"""

import importlib.util
from pathlib import Path
from typing import Any

import pytest

pytest.importorskip("airflow.sdk", reason="DAGs do Airflow 3: rodam dentro da imagem")

from airflow.dag_processing.dagbag import DagBag  # noqa: E402

_ARQUIVO = (
    Path(__file__).resolve().parents[1]
    / "dags"
    / "data_ingest"
    / "ancine"
    / "ancine_salas_exibicao_dag.py"
)
_DAG_ID = "ancine_salas_exibicao_dag"

pytestmark = pytest.mark.skipif(
    importlib.util.find_spec("metadata") is None,
    reason=(
        "openmetadata-ingestion ausente: a DAG importa a linhagem; roda dentro "
        "da imagem do Airflow (job docker_build)"
    ),
)


@pytest.fixture(scope="module")
def dag() -> Any:
    bag = DagBag(str(_ARQUIVO), include_examples=False)
    assert not bag.import_errors, bag.import_errors
    return bag.dags[_DAG_ID]


def test_caminho_raw_staging_seeds(dag: Any) -> None:
    caminho = ["baixar_para_raw", "raw_para_parquet", "parquet_para_seeds"]
    for anterior, seguinte in zip(caminho, caminho[1:]):
        assert seguinte in dag.get_task(anterior).downstream_task_ids
    assert "publicar_linhagem" in dag.get_task("parquet_para_seeds").downstream_task_ids
    assert dag.max_active_runs == 1


def test_e_mensal_e_le_a_versao_do_arquivo_por_padrao(dag: Any) -> None:
    # Sem a Variable dynamic_schedules, vale o padrão da DAG.
    assert dag.schedule == "@monthly"
    assert dag.params["versao"] == ""


def test_declara_as_tabelas_de_seeds(dag: Any) -> None:
    nomes = " ".join(
        getattr(o, "name", str(o)) for o in dag.get_task("parquet_para_seeds").outlets
    )
    assert "ancine_salas_exibicao" in nomes
    assert "ancine_controle_carga" in nomes
