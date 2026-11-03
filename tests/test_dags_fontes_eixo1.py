"""Fiação das DAGs que trazem as fontes do Eixo 1 do Cultura em Números.

Carrega as DAGs de verdade e confere o que um refactor pode quebrar sem que
nenhum teste de função perceba: a ordem raw → staging → seeds, as tabelas
declaradas para a linhagem, os parâmetros e o default do IPCA no BACEN.

As DAGs usam ``airflow.sdk`` (Airflow 3): sem ele o arquivo é pulado, como em
``test_siconfi_dags.py``. As três DAGs de seeds também importam a linhagem do
OpenMetadata, que só existe na imagem do Airflow e fica fora do Poetry de
propósito (ver ``test_openmetadata_execution.py``). Os testes que as carregam
são pulados no job ``test`` e rodam no ``docker_build``, que executa a suíte
dentro da imagem. Os do BACEN e do BB Ágil rodam nos dois.
"""

import importlib.util
from pathlib import Path
from typing import Any

import pytest

pytest.importorskip("airflow.sdk", reason="DAGs do Airflow 3: rodam dentro da imagem")

from airflow.dag_processing.dagbag import DagBag  # noqa: E402

import cliente_ibge_perfil as ibge  # noqa: E402

_DAGS = Path(__file__).resolve().parents[1] / "dags" / "data_ingest"
_ARQUIVOS = {
    "ibge_perfil_dag": _DAGS / "ibge" / "ibge_perfil_dag.py",
    "siop_execucao_dag": _DAGS / "siop" / "siop_execucao_dag.py",
    "tesouro_transparente_dag": _DAGS / "tesouro" / "tesouro_transparente_dag.py",
    "bacen_sgs_ingest_dag": _DAGS / "bacen" / "bacen_sgs_ingest_dag.py",
    "extracao_bbagil_dag": _DAGS / "bsc_pnab" / "extracao_bbagil_dag.py",
}
_DAGS_DE_SEEDS = ("ibge_perfil_dag", "siop_execucao_dag", "tesouro_transparente_dag")
_CAMINHO = ["baixar_para_raw", "raw_para_parquet", "parquet_para_seeds"]

requer_openmetadata = pytest.mark.skipif(
    importlib.util.find_spec("metadata") is None,
    reason=(
        "openmetadata-ingestion ausente: as DAGs de seeds importam a linhagem; "
        "rodam dentro da imagem do Airflow (job docker_build)"
    ),
)


@pytest.fixture(scope="module")
def dags() -> dict[str, Any]:
    carregadas: dict[str, Any] = {}
    for dag_id in _DAGS_DE_SEEDS:
        bag = DagBag(str(_ARQUIVOS[dag_id]), include_examples=False)
        assert not bag.import_errors, bag.import_errors
        carregadas[dag_id] = bag.dags[dag_id]
    return carregadas


@requer_openmetadata
@pytest.mark.parametrize(
    "dag_id", ["ibge_perfil_dag", "siop_execucao_dag", "tesouro_transparente_dag"]
)
def test_caminho_raw_staging_seeds(dags: dict[str, Any], dag_id: str) -> None:
    dag = dags[dag_id]
    for anterior, seguinte in zip(_CAMINHO, _CAMINHO[1:]):
        assert seguinte in dag.get_task(anterior).downstream_task_ids, dag_id
    assert "publicar_linhagem" in dag.get_task("parquet_para_seeds").downstream_task_ids
    assert dag.max_active_runs == 1


def _outlets(dag: Any) -> set[str]:
    return {
        getattr(o, "name", str(o)) for o in dag.get_task("parquet_para_seeds").outlets
    }


@requer_openmetadata
def test_ibge_declara_todas_as_tabelas_do_catalogo(dags: dict[str, Any]) -> None:
    nomes = " ".join(_outlets(dags["ibge_perfil_dag"]))
    for tabela in ibge.tabelas_seeds():
        assert tabela in nomes
    assert "ibge_controle_carga" in nomes


@requer_openmetadata
def test_ibge_e_tesouro_sao_manuais(dags: dict[str, Any]) -> None:
    for dag_id in ("ibge_perfil_dag", "tesouro_transparente_dag"):
        assert type(dags[dag_id].timetable).__name__ == "NullTimetable", dag_id
    assert dags["ibge_perfil_dag"].params["edicoes"] == []
    assert dags["siop_execucao_dag"].params["anos"] == []


def test_bacen_traz_o_ipca_cheio_por_padrao() -> None:
    modulo = _modulo("bacen_sgs_ingest_dag")
    assert modulo._SERIES_PADRAO == {"ipca_servicos": 10844, "ipca": 433}


def _modulo(dag_id: str) -> Any:
    import importlib.util

    spec = importlib.util.spec_from_file_location(dag_id, _ARQUIVOS[dag_id])
    assert spec and spec.loader
    modulo = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(modulo)
    return modulo


class _BancoFalso:
    """Responde a consulta do checkpoint com as combinações já feitas."""

    def __init__(self, feitas: list[tuple[str, str, str, str]]) -> None:
        self.feitas = feitas

    def execute_query(self, query: str) -> list[tuple[str, str, str, str]]:
        assert "controle_extracao_bbagil_extrato" in query
        return self.feitas


def test_bbagil_so_a_conta_da_lab1_ganha_os_meses_extras() -> None:
    modulo = _modulo("extracao_bbagil_dag")
    import agencias_bbagil

    base = agencias_bbagil.gerar_periodos_mensais([2023])
    extras = agencias_bbagil.periodos_extras_por_programa({"30882120200002": [2020]})
    lab1 = {
        "id_plano_acao": "1",
        "id_plano_acao_dado_bancario": "10",
        "codigo_programa": "30882120200002",
    }
    lpg = {
        "id_plano_acao": "2",
        "id_plano_acao_dado_bancario": "20",
        "codigo_programa": "30882120230006",
    }
    feitas = [("1", "10", "2020-01-01", "2020-01-31")]

    pendentes = modulo._combinacoes_extrato_pendentes(
        _BancoFalso(feitas), [lab1, lpg], base, extras
    )

    por_conta: dict[str, list[Any]] = {}
    for ente, periodo in pendentes:
        por_conta.setdefault(ente["id_plano_acao"], []).append(periodo)
    # LAB 1: 12 meses de 2023 + 11 de 2020 (janeiro já foi feito).
    assert len(por_conta["1"]) == 23
    assert ("2020-01-01", "2020-01-31") not in por_conta["1"]
    assert por_conta["2"] == base


def test_bbagil_sem_extras_e_como_antes() -> None:
    modulo = _modulo("extracao_bbagil_dag")
    import agencias_bbagil

    base = agencias_bbagil.gerar_periodos_mensais([2023])
    ente = {
        "id_plano_acao": "1",
        "id_plano_acao_dado_bancario": "10",
        "codigo_programa": "30882120200002",
    }
    pendentes = modulo._combinacoes_extrato_pendentes(_BancoFalso([]), [ente], base)
    assert [p for _, p in pendentes] == base
