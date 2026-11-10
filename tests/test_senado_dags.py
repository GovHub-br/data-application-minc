"""Fiação da ``senado_legislacao_dag`` e as decisões que pintam o run.

Carrega a DAG de verdade (``airflow.sdk``, Airflow 3; sem ele o arquivo é
pulado, como em ``test_siconfi_dags.py``) e chama as funções do módulo com o
cliente da API e o MinIO trocados por falsos.
"""

import importlib.util
import json
import time
from datetime import datetime, timezone
from http import HTTPStatus
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest

pytest.importorskip("airflow.sdk", reason="DAGs do Airflow 3: rodam dentro da imagem")

from airflow.dag_processing.dagbag import DagBag  # noqa: E402
from airflow.sdk.exceptions import AirflowFailException  # noqa: E402

import senado_legislacao as sl  # noqa: E402

_ARQUIVO = (
    Path(__file__).resolve().parents[1]
    / "dags"
    / "data_ingest"
    / "senado"
    / "senado_legislacao_dag.py"
)


@pytest.fixture(scope="module")
def dag() -> Any:
    bag = DagBag(str(_ARQUIVO), include_examples=False)
    assert not bag.import_errors, bag.import_errors
    return bag.dags["senado_legislacao_dag"]


@pytest.fixture(scope="module")
def modulo() -> Any:
    spec = importlib.util.spec_from_file_location("senado_legislacao_dag", _ARQUIVO)
    assert spec and spec.loader
    carregado = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(carregado)
    return carregado


def test_ordem_das_tasks(dag: Any) -> None:
    ordem = ["preparar", "listar", "detalhar", "carregar_bronze", "conferir"]
    for anterior, seguinte in zip(ordem, ordem[1:]):
        assert seguinte in dag.get_task(anterior).downstream_task_ids
    assert dag.get_task("conferir").upstream_task_ids == {
        "listar",
        "detalhar",
        "carregar_bronze",
    }


def test_mensal_um_run_por_vez_e_params(dag: Any) -> None:
    assert dag.max_active_runs == 1
    assert dag.catchup is False
    # Sem a Variable dynamic_schedules, vale o padrão do schedule_loader.
    assert dag.schedule == "@monthly"
    assert dag.params["anos"] == []
    assert dag.params["max_run_minutes"] == 110
    assert dag.params["paralelo"] == 4


class _ClienteFalso:
    """Responde ao detalhe na hora, salvo os ids marcados para falhar."""

    def __init__(self, falham: set[str] | None = None, demora: float = 0.0) -> None:
        self.falham = falham or set()
        self.demora = demora

    def get_legislacao_detalhe(self, codigo: str) -> tuple[HTTPStatus, Any]:
        time.sleep(self.demora)
        if codigo in self.falham:
            return HTTPStatus.INTERNAL_SERVER_ERROR, None
        return HTTPStatus.OK, {
            "DetalheDocumento": {"documentos": {"documento": {"id": codigo}}}
        }

    def get_legislacao_lista(self, tipo: str, ano: int) -> tuple[HTTPStatus, Any]:
        if (tipo, ano) == ("DEC", 1990):
            return HTTPStatus.INTERNAL_SERVER_ERROR, None
        return HTTPStatus.OK, {
            "ListaDocumento": {"documentos": {"documento": [{"id": "1"}, {"id": "2"}]}}
        }

    documentos_da_lista = staticmethod(sl.ClienteSenadores.documentos_da_lista)
    documentos_do_detalhe = staticmethod(sl.ClienteSenadores.documentos_do_detalhe)


def test_detalhar_grava_um_arquivo_por_id_e_conta_as_falhas(modulo: Any) -> None:
    gravados: dict[str, bytes] = {}
    pendentes = [(str(i), sl.Norma(2026, "LEI")) for i in range(1, 31)]
    with patch.object(
        modulo.datalakehouse,
        "gravar_bytes_em_partes",
        side_effect=lambda corpo, key: gravados.setdefault(key, corpo) and key,
    ):
        resumo = modulo._detalhar(
            _ClienteFalso(falham={"7"}), pendentes, "run", time.monotonic() + 60, 4
        )

    assert resumo["tentadas"] == 30 and resumo["baixados"] == 29 and resumo["falhas"] == 1
    assert resumo["restantes"] == 1 and resumo["exemplos_de_falha"] == ["7: HTTP 500"]
    key = sl.key_detalhe(2026, "1", "run")
    envelope = json.loads(gravados[key])
    assert envelope["entidade"] == "legislacao_detalhe" and envelope["itens"] == 1
    assert envelope["requisicao"] == {
        "path": "/legislacao/1",
        "id": "1",
        "ano": 2026,
        "tipo": "LEI",
    }


def test_detalhar_para_de_pegar_id_novo_no_prazo(modulo: Any) -> None:
    pendentes = [(str(i), sl.Norma(2026, "LEI")) for i in range(1, 1_000)]
    with patch.object(modulo.datalakehouse, "gravar_bytes_em_partes", return_value="k"):
        resumo = modulo._detalhar(
            _ClienteFalso(demora=0.01), pendentes, "run", time.monotonic() + 0.2, 2
        )
    assert 0 < resumo["baixados"] < len(pendentes)
    assert resumo["restantes"] == len(pendentes) - resumo["baixados"]


def test_listar_segue_depois_de_uma_falha(modulo: Any) -> None:
    gravados: list[str] = []

    def gravar(corpo: bytes, key: str) -> str:
        gravados.append(key)
        return key

    with patch.object(modulo.datalakehouse, "gravar_bytes_em_partes", side_effect=gravar):
        resumo = modulo._listar(
            _ClienteFalso(), [("LEI", 1990), ("DEC", 1990), ("LCP", 1990)], "run"
        )
    assert resumo["gravadas"] == 2 and resumo["normas"] == 4
    assert resumo["falhas"] == ["DEC 1990: HTTP 500"]
    assert gravados[0] == sl.key_lista("LEI", 1990, "run")


def _resumos(
    falhas_lista: list[str] | None = None, tentadas: int = 40_000, falhas: int = 0
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    lista = {"falhas": falhas_lista or []}
    detalhe = {
        "tentadas": tentadas,
        "falhas": falhas,
        "exemplos_de_falha": [],
        "restantes": 0,
        "por_tipo": {"LEI": {"listadas": 1, "com_detalhe": 1, "pendentes": 0}},
    }
    return lista, detalhe, {"restantes": 0}


def test_um_detalhe_que_nunca_baixa_nao_pinta_o_run(modulo: Any) -> None:
    resumo = modulo._veredito(*_resumos(falhas=1), [], [])
    assert resumo["detalhes_restantes"] == 0


@pytest.mark.parametrize(
    "resumos, problemas",
    [
        (_resumos(falhas_lista=["DEC 1990: HTTP 500"]), []),
        (_resumos(tentadas=100, falhas=60), []),
        (_resumos(), ["legislacao_lista: x tem 1 de 2 linhas"]),
    ],
)
def test_falha_de_listagem_excesso_de_falhas_e_linha_perdida_pintam_o_run(
    modulo: Any, resumos: tuple[dict[str, Any], ...], problemas: list[str]
) -> None:
    with pytest.raises(AirflowFailException):
        modulo._veredito(*resumos, problemas, [])


def test_carregar_pula_o_que_ja_entrou_e_registra_o_resto(modulo: Any) -> None:
    agora = datetime(2026, 11, 10, tzinfo=timezone.utc)
    ja = sl.Arquivo("raw/senado/legislacao_lista/ano=1990/tipo=LEI/a.json", ("a",), agora)
    novo = sl.Arquivo(
        "raw/senado/legislacao_lista/ano=1990/tipo=LCP/b.json", ("b",), agora
    )
    envelope = {
        "entidade": "legislacao_lista",
        "resposta": {"ListaDocumento": {"documentos": {"documento": {"id": "9"}}}},
    }
    gravados: list[list[str]] = []

    def gravar(
        alvos: Any, convertidos: list[Any], carga: str, quando: datetime
    ) -> dict[str, int]:
        gravados.append([c.arquivo for c in convertidos])
        return {"legislacao_lista": 1, "arquivos": len(convertidos)}

    def arquivos(entidade: str) -> tuple[list[sl.Arquivo], set[str]]:
        return ([ja, novo], set()) if entidade == "legislacao_lista" else ([], set())

    with (
        patch.object(modulo, "_arquivos_do_raw", side_effect=arquivos),
        patch.object(modulo.sl, "ler_envelope", return_value=envelope),
        patch.object(modulo.bronze, "gravar", side_effect=gravar),
    ):
        resumo = modulo._carregar({}, {ja.key}, time.monotonic() + 60, "carga")

    assert gravados == [[novo.key]]
    assert resumo["arquivos"] == 1 and resumo["restantes"] == 0
