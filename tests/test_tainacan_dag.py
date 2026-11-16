"""Fiação da ``tainacan_dag``, com a API, o MinIO e o Postgres trocados por falsos.

Carrega a DAG de verdade (``airflow.sdk``, Airflow 3; sem ele o arquivo é
pulado, como em ``test_senado_dags.py``).
"""

import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from unittest.mock import patch

import httpx
import pandas as pd
import pytest

pytest.importorskip("airflow.sdk", reason="DAGs do Airflow 3: rodam dentro da imagem")

from airflow.dag_processing.dagbag import DagBag  # noqa: E402

import cliente_tainacan as tainacan  # noqa: E402
import datalakehouse  # noqa: E402

_ARQUIVO = (
    Path(__file__).resolve().parents[1]
    / "dags"
    / "data_ingest"
    / "tainacan"
    / "tainacan_dag.py"
)
_RUN = ("manual__2026-11-16T12:00:00+00:00", datetime(2026, 11, 16, tzinfo=timezone.utc))


@pytest.fixture(scope="module")
def dag() -> Any:
    bag = DagBag(str(_ARQUIVO), include_examples=False)
    assert not bag.import_errors, bag.import_errors
    return bag.dags["tainacan_dag"]


def test_ordem_das_tasks(dag: Any) -> None:
    ordem = [
        "listar_colecoes",
        "extrair_colecao",
        "carregar_seeds",
        "registrar_conferencia",
    ]
    for anterior, seguinte in zip(ordem, ordem[1:]):
        assert seguinte in dag.get_task(anterior).downstream_task_ids
    assert {"conferir_catalogo", "publicar_linhagem"} <= dag.get_task(
        "registrar_conferencia"
    ).downstream_task_ids
    assert dag.get_task("conferir_catalogo").trigger_rule == "all_done"


def test_parametros_e_agendamento(dag: Any) -> None:
    assert dict(dag.params)["instancias"] == []
    assert dict(dag.params)["colecoes"] == []
    assert dag.max_active_runs == 1
    assert dag.get_task("extrair_colecao").max_active_tis_per_dag == 3


class _MinioFalso:
    def __init__(self) -> None:
        self.objetos: dict[str, bytes] = {}
        self.parquets: dict[str, list[dict[str, Any]]] = {}

    def gravar_bytes_em_partes(self, conteudo: bytes, key: str) -> str:
        self.objetos[key] = conteudo
        return key

    def gravar_parquet(
        self, registros: list[dict[str, Any]], key: str, dt_ingest: Any = None
    ) -> list[str]:
        self.parquets[key] = registros
        return [key]

    def ler_parquet(self, key: str) -> pd.DataFrame:
        return datalakehouse._registros_para_df(self.parquets[key], None, None)


def _api(itens: list[dict[str, Any]], por_pagina: int) -> httpx.MockTransport:
    def responder(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/collections"):
            listadas = [
                {"id": c.id, "slug": c.slug}
                for c in tainacan.COLECOES
                if c.instancia == "cadastro"
            ]
            return httpx.Response(
                200, json=listadas, headers={"X-WP-Total": "2", "X-WP-TotalPages": "1"}
            )
        pagina = int(request.url.params["paged"])
        return httpx.Response(
            200,
            json={"items": itens[(pagina - 1) * por_pagina : pagina * por_pagina]},
            headers={
                "X-WP-Total": str(len(itens)),
                "X-WP-TotalPages": str(math.ceil(len(itens) / por_pagina)),
            },
        )

    return httpx.MockTransport(responder)


def test_extrair_grava_cada_pagina_no_raw_e_a_coleta_por_ultimo(dag: Any) -> None:
    itens = [
        {
            "id": i,
            "collection_id": "208",
            "title": f"museu {i}",
            "metadata": {"a": {"value": ""}},
        }
        for i in range(1, 101)
    ]
    minio = _MinioFalso()
    transporte = _api(itens, 96)
    cliente_real = tainacan.ClienteTainacan
    with (
        patch.object(datalakehouse, "run_atual", return_value=_RUN),
        patch.object(
            datalakehouse, "gravar_bytes_em_partes", minio.gravar_bytes_em_partes
        ),
        patch.object(datalakehouse, "gravar_parquet", minio.gravar_parquet),
        patch.object(
            tainacan, "ClienteTainacan", lambda inst: cliente_real(inst, transporte)
        ),
    ):
        extrair = dag.get_task("extrair_colecao").python_callable
        resultado = extrair({"instancia": "cadastro", "colecao_id": 208})

    keys = list(minio.objetos)
    assert keys[-1] == resultado["key_raw"] and keys[-1].endswith("/coleta.json")
    assert [k.rsplit("/", 1)[1] for k in keys[:-1]] == [
        "pagina_0001.json",
        "pagina_0002.json",
    ]
    assert keys[0].startswith(
        "raw/ibram/cadastro_museus/ano=2026/mes=11/dia=16/manual__2026-11-16T12_00_00_00_00/"
    )
    coleta = json.loads(minio.objetos[keys[-1]])
    assert coleta["x_wp_total"] == coleta["itens"] == 100
    assert resultado["linhas_origem"] == 100
    assert (
        resultado["tabela"] == "ibram_cadastro_museus" and resultado["periodo"] == "208"
    )
    linhas = minio.parquets[resultado["key_staging"]]
    assert len(linhas) == 100 and linhas[0]["metadata"] == '{"a":{"value":""}}'


def test_listar_tira_a_colecao_que_sumiu_e_a_registra_como_problema(dag: Any) -> None:
    from unittest.mock import MagicMock

    listadas = [
        {"id": 208, "slug": "museus", "name": "Museus"},
        {"id": 1, "slug": "outra"},
    ]
    transporte = httpx.MockTransport(
        lambda r: httpx.Response(
            200, json=listadas, headers={"X-WP-Total": "2", "X-WP-TotalPages": "1"}
        )
    )
    minio = _MinioFalso()
    contexto = {"params": {"instancias": ["cadastro"], "colecoes": []}, "ti": MagicMock()}
    listar = dag.get_task("listar_colecoes").python_callable
    cliente_real = tainacan.ClienteTainacan
    with (
        patch.dict(listar.__globals__, {"get_current_context": lambda: contexto}),
        patch.object(datalakehouse, "run_atual", return_value=_RUN),
        patch.object(
            datalakehouse, "gravar_bytes_em_partes", minio.gravar_bytes_em_partes
        ),
        patch.object(
            tainacan, "ClienteTainacan", lambda inst: cliente_real(inst, transporte)
        ),
    ):
        alvos = listar()

    assert alvos == [{"instancia": "cadastro", "colecao_id": 208}]
    (chamada,) = contexto["ti"].xcom_push.call_args_list
    assert chamada.kwargs["key"] == "problemas"
    (problema,) = chamada.kwargs["value"]
    assert "179824" in problema and "não está mais na listagem" in problema
    (key,) = minio.objetos
    assert key.startswith("raw/ibram/colecoes/instancia=cadastro/ano=2026/mes=11/dia=16/")
