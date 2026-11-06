"""Fiação das DAGs do SICONFI: o que deixa o run vermelho e o que emite Asset.

Um refactor das trigger rules não quebra nenhum teste de função, mas pode fazer o
run voltar a ficar verde com uma cópia falhando (era o ponto 5 do Copilot na
revisão do PR #86), ou um ``anunciar_*`` parar de rodar depois de uma falha.
Estes testes carregam as DAGs de verdade e conferem essas ligações, e chamam as
tasks que decidem a cor do run.

As DAGs usam ``airflow.sdk`` (Airflow 3), que só existe na imagem do Airflow:
no ambiente do Poetry o arquivo é pulado, e o job ``docker_build`` do CI roda a
suíte dentro da imagem, como em ``test_openmetadata_execution.py``.
"""

import importlib.util
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator
from unittest.mock import patch

import pytest

pytest.importorskip("airflow.sdk", reason="DAGs do Airflow 3: rodam dentro da imagem")

from airflow.dag_processing.dagbag import DagBag  # noqa: E402
from airflow.sdk import TriggerRule  # noqa: E402
from siconfi_storage import VALID_ENDPOINTS, ContagemDePaginas  # noqa: E402
from airflow.sdk.exceptions import (  # noqa: E402
    AirflowFailException,
    AirflowSkipException,
)

_PASTA = Path(__file__).resolve().parents[1] / "dags" / "data_ingest" / "siconfi"
_ENTIDADES = {
    "anexos_relatorios",
    "entes",
    "extrato_entregas",
    "rreo",
    "rgf",
    "dca",
    "msc_patrimonial",
    "msc_orcamentaria",
    "msc_controle",
}


@pytest.fixture(scope="module")
def dags() -> dict[str, Any]:
    bag = DagBag(str(_PASTA), include_examples=False)
    assert not bag.import_errors, bag.import_errors
    carregadas: dict[str, Any] = bag.dags
    return carregadas


def _modulo(arquivo: str) -> Any:
    spec = importlib.util.spec_from_file_location(arquivo, _PASTA / f"{arquivo}.py")
    assert spec and spec.loader
    modulo = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(modulo)
    return modulo


# ── ingestão ─────────────────────────────────────────────────────────────


def test_ingestao_roda_uma_task_e_um_run_por_vez(dags: dict[str, Any]) -> None:
    """Sem trava de linha no controle, é o que impede duas tasks na mesma fila."""
    dag = dags["siconfi_ingestion_dag"]
    assert dag.max_active_tasks == 1
    assert dag.max_active_runs == 1


def test_ingestao_falha_vermelha_o_run(dags: dict[str, Any]) -> None:
    dag = dags["siconfi_ingestion_dag"]
    vigia = dag.get_task("falhar_se_algo_falhou")
    assert vigia.trigger_rule == TriggerRule.ONE_FAILED
    assert vigia.upstream_task_ids == {t for t in dag.task_ids if t != vigia.task_id}
    with pytest.raises(AirflowFailException):
        vigia.python_callable()


def test_so_anunciar_emite_asset_e_roda_mesmo_com_falha(dags: dict[str, Any]) -> None:
    dag = dags["siconfi_ingestion_dag"]
    anuncios = {t.task_id: t for t in dag.tasks if t.task_id.startswith("anunciar_")}
    assert set(anuncios) == {f"anunciar_{e}" for e in _ENTIDADES}
    for task_id, task in anuncios.items():
        entidade = task_id.removeprefix("anunciar_")
        assert task.trigger_rule == TriggerRule.ALL_DONE
        assert [o.uri for o in task.outlets] == [
            f"s3://minc-datalakehouse/raw/siconfi/{entidade}"
        ]
    outros = [t for t in dag.tasks if not t.task_id.startswith("anunciar_")]
    assert all(not t.outlets for t in outros)


def test_controle_e_preparado_uma_vez_antes_do_refresh(dags: dict[str, Any]) -> None:
    dag = dags["siconfi_ingestion_dag"]
    assert "preparar_controle" in dag.get_task("refresh_reference_data").upstream_task_ids


# ── conversão ────────────────────────────────────────────────────────────


def test_conversao_limita_a_concorrencia(dags: dict[str, Any]) -> None:
    dag = dags["siconfi_migracao_raw_pages_dag"]
    assert dag.max_active_tasks == 3
    assert dag.max_active_runs == 1


def test_conversao_falha_vermelha_o_run(dags: dict[str, Any]) -> None:
    dag = dags["siconfi_migracao_raw_pages_dag"]
    copias = {t for t in dag.task_ids if t.startswith("migrar_")}
    assert len(copias) == 10  # 9 tabelas e a fila
    conferir = dag.get_task("conferir")
    assert conferir.trigger_rule == TriggerRule.ALL_DONE
    assert conferir.upstream_task_ids == copias
    vigia = dag.get_task("falhar_se_algo_falhou")
    assert vigia.trigger_rule == TriggerRule.ONE_FAILED
    assert vigia.upstream_task_ids == copias | {"conferir", "conversao_completa"}


def test_conversao_completa_e_o_sinal_de_fim(dags: dict[str, Any]) -> None:
    task = dags["siconfi_migracao_raw_pages_dag"].get_task("conversao_completa")
    assert task.upstream_task_ids == {"conferir"}
    completo = {
        "raw_pages_rreo": {"situacao": "completo"},
        "fila": {"situacao": "completo"},
    }
    assert task.python_callable(completo) == "conversão completa"
    with pytest.raises(AirflowSkipException, match="raw_pages_rgf"):
        task.python_callable({**completo, "raw_pages_rgf": {"situacao": "em_andamento"}})


class _ProgressoFalso:
    def ler(self, tabela: str) -> tuple[int, int]:
        return (10, 10)

    def ler_fila(self) -> tuple[str, str, int]:
        return ("", "", 0)


@contextmanager
def _origem_falsa() -> Iterator[None]:
    yield None


def test_conferir_falha_quando_falta_arquivo_no_raw() -> None:
    dag = _modulo("siconfi_migracao_raw_pages_dag")
    instante = datetime(2026, 9, 28, tzinfo=timezone.utc)
    with (
        patch.object(dag, "_progresso", return_value=_ProgressoFalso()),
        patch.object(dag, "_origem", _origem_falsa),
        patch.object(dag, "tabela_existe", return_value=True),
        patch.object(dag, "total_na_origem", return_value=(10, 10)),
        patch.object(dag, "runs_na_origem", return_value=[("run", instante, 10)]),
        patch.object(dag, "paginas_dos_runs", return_value=ContagemDePaginas(9, 0)),
        patch.object(dag, "fila_existe", return_value=False),
    ):
        with pytest.raises(AirflowFailException, match="inconsistente"):
            dag._conferir()


# ── bronze ───────────────────────────────────────────────────────────────


def test_bronze_prepara_antes_de_carregar_e_limita_a_concorrencia(
    dags: dict[str, Any],
) -> None:
    dag = dags["siconfi_bronze_dag"]
    assert "carregar" in dag.get_task("preparar").downstream_task_ids
    assert dag.max_active_runs == 1
    assert dag.max_active_tasks == 2
    assert set(dag.params["endpoints"]) == set(VALID_ENDPOINTS)


class _TrinoDaBronze:
    """Responde as páginas já registradas e guarda o SQL executado."""

    def __init__(self, feitas: list[str]) -> None:
        self.feitas = feitas
        self.executados: list[str] = []

    def run(self, sql: str) -> None:
        self.executados.append(sql)

    def records(self, sql: str) -> list[tuple[str, ...]]:
        if '"siconfi"."paginas"' in sql:
            assert "WHERE endpoint = 'rreo'" in sql
            return [(p,) for p in self.feitas]
        return [("_pagina_id",), ("_item",)]


def _pagina_raw(itens: int) -> str:
    return (
        '{"endpoint": "rreo", "request_hash": "h", "request_params": {}, '
        '"page_offset": 0, "fetched_at": "2026-10-27T10:00:00+00:00", '
        '"run_id": "r", "payload": {"items": ['
        + ", ".join('{"conta": "' + str(i) + '"}' for i in range(itens))
        + "]}}"
    )


def test_bronze_carrega_so_o_que_falta_e_registra_cada_pagina() -> None:
    dag = _modulo("siconfi_bronze_dag")
    pasta = "raw/siconfi/rreo/ano=2026/mes=10/dia=27/r"
    keys = [f"{pasta}/a_0.json", f"{pasta}/b_0_p01de02.json", f"{pasta}/b_0_p02de02.json"]
    trino = _TrinoDaBronze(feitas=[f"{pasta}/a_0.json"])
    with (
        patch.object(dag, "_alvo", return_value=("hml_analytics", "siconfi")),
        patch.object(dag, "TrinoExecutor", return_value=trino),
        patch.object(dag, "raw_keys", return_value=keys),
        patch.object(
            dag.datalakehouse, "ler_texto", side_effect=lambda k: _pagina_raw(2)
        ),
    ):
        resumo = dag._carregar_endpoint(
            "rreo", prazo_s=60, paralelo=2, carga_run_id="run-b"
        )

    assert resumo["pendentes"] == 1
    assert resumo["carregadas"] == 1
    assert resumo["itens"] == 4  # duas partes de 2 itens
    inserts = [
        s
        for s in trino.executados
        if s.startswith('INSERT INTO "hml_analytics"."siconfi"."rreo"')
    ]
    assert len(inserts) == 1
    (registro,) = [
        s
        for s in trino.executados
        if s.startswith('INSERT INTO "hml_analytics"."siconfi"."paginas"')
    ]
    # Os itens entram antes da linha da página: se a task cair entre os dois,
    # a página é carregada de novo, e não registrada sem os itens.
    assert trino.executados.index(inserts[0]) < trino.executados.index(registro)
    assert f"'{pasta}/b_0.json', '2', '4', 'rreo'" in registro
    assert "'run-b'" in registro


def test_bronze_para_no_prazo_e_nao_registra_o_que_nao_carregou() -> None:
    dag = _modulo("siconfi_bronze_dag")
    keys = [f"raw/siconfi/rreo/x/p{i}_0.json" for i in range(5)]
    trino = _TrinoDaBronze(feitas=[])
    with (
        patch.object(dag, "_alvo", return_value=("hml_analytics", "siconfi")),
        patch.object(dag, "TrinoExecutor", return_value=trino),
        patch.object(dag, "raw_keys", return_value=keys),
        patch.object(
            dag.datalakehouse, "ler_texto", side_effect=lambda k: _pagina_raw(1)
        ),
    ):
        resumo = dag._carregar_endpoint("rreo", prazo_s=0, paralelo=1, carga_run_id="r")
    assert resumo["carregadas"] == 0
    assert resumo["restantes"] == 5
    assert not [s for s in trino.executados if s.startswith("INSERT")]
