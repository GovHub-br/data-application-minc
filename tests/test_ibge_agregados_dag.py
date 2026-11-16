"""Recarga da ``ibge_agregados_dag`` sem derrubar as tabelas de ``ibge_sidra``.

O dbt cria views em cima dessas tabelas (``ibge_sidra.bronze_*``). Com elas no
lugar, o ``DROP TABLE`` que ``extrair_catalogo`` fazia era recusado pelo
Postgres (``DependentObjectsStillExist``) e a DAG falhava todo domingo. As
tasks mapeadas não derrubavam nada, mas acrescentavam uma cópia de
``periodos``, ``variaveis`` e ``localidades`` a cada run (issue #100).

Os testes chamam o código das tasks de verdade, com uma API do IBGE falsa. Os
que gravam rodam num Postgres real, apontado por ``TEST_POSTGRES_DSN`` como em
``test_cliente_postgres.py``, e são pulados sem ele. Os outros rodam sempre.
"""

import importlib.util
import os
import uuid
from pathlib import Path
from typing import Any, Iterator

import pytest

pytest.importorskip("airflow.sdk", reason="DAGs do Airflow 3: rodam dentro da imagem")

import psycopg2  # noqa: E402
from airflow.dag_processing.dagbag import DagBag  # noqa: E402
from airflow.sdk.exceptions import AirflowFailException  # noqa: E402

from extracao_ibge import ClienteIBGE  # noqa: E402

DSN = os.environ.get("TEST_POSTGRES_DSN")
_ARQUIVO = (
    Path(__file__).resolve().parents[1]
    / "dags"
    / "data_ingest"
    / "ibge"
    / "ibge_agregados_dag.py"
)
# Task mapeada → tabela que ela grava.
_TASKS_DO_LOTE = {
    "extrair_metadados": "metadados",
    "extrair_periodos": "periodos",
    "extrair_variaveis": "variaveis",
    "extrair_localidades": "localidades",
}


class _IbgeFalso(ClienteIBGE):
    """A API de Agregados em miniatura: duas pesquisas, três agregados.

    ``periodos`` muda o que ``get_periodos`` devolve entre um run e outro, e
    um agregado em ``falham`` responde como a API fora do ar (lista vazia).
    """

    pesquisas: list[dict[str, Any]] = []
    periodos: list[str] = []
    falham: set[str] = set()

    def __init__(self) -> None:
        super().__init__(sleep_entre_requests=0)

    def get_pesquisas(self) -> list[dict[str, Any]]:
        return self.pesquisas

    def get_metadados(self, agregado_id: int | str) -> dict[str, Any] | None:
        if str(agregado_id) in self.falham:
            return None
        return {
            "id": int(agregado_id),
            "nome": f"Agregado {agregado_id}",
            "periodicidade": {"frequencia": "anual"},
        }

    def get_periodos(self, agregado_id: int | str) -> list[dict[str, Any]]:
        if str(agregado_id) in self.falham:
            return []
        return [
            {"id": p, "literals": [p], "modificacao": "01/01/2025"} for p in self.periodos
        ]

    def get_variaveis(self, agregado_id: int | str) -> list[dict[str, Any]]:
        if str(agregado_id) in self.falham:
            return []
        return [{"id": 93, "nome": "População residente", "unidade": "Pessoas"}]

    def get_localidades(
        self, agregado_id: int | str, nivel: str = "N3"
    ) -> list[dict[str, Any]]:
        if str(agregado_id) in self.falham:
            return []
        return [
            {"id": uf, "nome": nome, "nivel": {"id": nivel, "nome": "UF"}}
            for uf, nome in (("11", "Rondônia"), ("53", "Distrito Federal"))
        ]


_PESQUISAS = [
    {
        "id": "CD",
        "nome": "Censo Demográfico",
        "agregados": [{"id": 4709, "nome": "População"}, {"id": 4714, "nome": "Área"}],
    },
    {"id": "PA", "nome": "PNAD", "agregados": [{"id": 6579, "nome": "Estimativa"}]},
]


@pytest.fixture(scope="module")
def modulo() -> Any:
    spec = importlib.util.spec_from_file_location("ibge_agregados_dag_teste", _ARQUIVO)
    assert spec and spec.loader
    carregado = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(carregado)
    return carregado


@pytest.fixture
def dag(modulo: Any, monkeypatch: pytest.MonkeyPatch) -> Any:
    """A DAG montada do módulo carregado, com a API falsa e sem a Variable."""
    monkeypatch.setattr(_IbgeFalso, "pesquisas", [dict(p) for p in _PESQUISAS])
    monkeypatch.setattr(_IbgeFalso, "periodos", ["2010", "2022"])
    monkeypatch.setattr(_IbgeFalso, "falham", set())
    monkeypatch.setattr(modulo, "ClienteIBGE", _IbgeFalso)
    monkeypatch.setattr(modulo, "_get_config", lambda: dict(modulo._IBGE_CONFIG_DEFAULTS))
    return modulo.ibge_agregados_dag()


def _rodar(dag: Any, task_id: str, **kwargs: Any) -> Any:
    return dag.get_task(task_id).python_callable(**kwargs)


# ---------------------------------------------------------------------------
# Sem banco
# ---------------------------------------------------------------------------


def test_dag_carrega_e_o_catalogo_vem_antes_de_tudo() -> None:
    bag = DagBag(str(_ARQUIVO), include_examples=False)
    assert not bag.import_errors, bag.import_errors
    dag = bag.dags["ibge_agregados_dag"]

    assert dag.get_task("extrair_catalogo").downstream_task_ids == {
        "preparar_batches",
        "extrair_resultados",
    }
    assert dag.get_task("preparar_batches").downstream_task_ids == set(_TASKS_DO_LOTE)


def test_preparar_batches_sem_o_xcom_do_catalogo_falha_sem_retry(
    dag: Any, modulo: Any
) -> None:
    """O ``TypeError: 'NoneType' object is not iterable`` do run manual de
    09/11/2026: o Airflow entrega ``None`` quando ``extrair_catalogo`` termina
    em sucesso sem ter rodado, e repetir a task não traz o XCom."""
    with pytest.raises(AirflowFailException, match="marcada como sucesso"):
        _rodar(dag, "preparar_batches", catalogo=None)


def test_preparar_batches_divide_os_ids_do_catalogo(dag: Any) -> None:
    # O 7 repetido: cada agregado num lote só, ou dois lotes paralelos o
    # substituiriam ao mesmo tempo.
    catalogo = [{"id": i} for i in range(1, 121)] + [{"id": 7}]

    batches = _rodar(dag, "preparar_batches", catalogo=catalogo)

    assert [len(b) for b in batches] == [50, 50, 20]
    assert batches[0][0] == "1" and batches[-1][-1] == "120"


class _BancoQueRegistra:
    """Guarda as chamadas de escrita; ``DROP`` faz o teste falhar."""

    def __init__(self, conn_str: str) -> None:
        self.chamadas: list[tuple[str, str, dict[str, Any]]] = []
        _BancoQueRegistra.ultimo = self

    def substituir_dados(self, data: list, table_name: str, **kwargs: Any) -> None:
        self.chamadas.append(("substituir_dados", table_name, kwargs))

    def insert_data(self, data: list, table_name: str, **kwargs: Any) -> None:
        self.chamadas.append(("insert_data", table_name, kwargs))

    def drop_table_if_exists(self, *args: Any, **kwargs: Any) -> None:
        raise AssertionError("DROP derruba as views do dbt em cima da tabela")

    ultimo: "_BancoQueRegistra"


def test_nenhuma_task_derruba_tabela_e_cada_uma_substitui_o_seu_recorte(
    dag: Any, modulo: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(modulo, "ClientPostgresDB", _BancoQueRegistra)
    monkeypatch.setattr(modulo, "get_postgres_conn", lambda: "")

    _rodar(dag, "extrair_catalogo")
    chamadas = _BancoQueRegistra.ultimo.chamadas
    for task_id in _TASKS_DO_LOTE:
        _rodar(dag, task_id, batch=["4709"])
        chamadas += _BancoQueRegistra.ultimo.chamadas

    por_tabela = {tabela: (metodo, kwargs) for metodo, tabela, kwargs in chamadas}
    assert por_tabela["pesquisas"] == ("substituir_dados", {"schema": "ibge_sidra"})
    assert por_tabela["agregados_catalogo"] == (
        "substituir_dados",
        {"schema": "ibge_sidra"},
    )
    for tabela in ("periodos", "variaveis", "localidades"):
        metodo, kwargs = por_tabela[tabela]
        assert metodo == "substituir_dados", tabela
        assert kwargs["coluna_recorte"] == "agregado_id", tabela
    # metadados já tinha chave e upsert: não duplicava.
    metodo, kwargs = por_tabela["metadados"]
    assert metodo == "insert_data" and kwargs["conflict_fields"] == ["agregado_id"]


# ---------------------------------------------------------------------------
# Num Postgres de verdade
# ---------------------------------------------------------------------------


@pytest.fixture
def schema(modulo: Any, monkeypatch: pytest.MonkeyPatch) -> Iterator[str]:
    """Schema próprio no lugar de ``ibge_sidra``, apagado no fim."""
    if not DSN:
        pytest.skip("TEST_POSTGRES_DSN não definido — sem Postgres para testar")
    nome = f"teste_{uuid.uuid4().hex[:10]}"
    monkeypatch.setattr(modulo, "SCHEMA", nome)
    monkeypatch.setattr(modulo, "get_postgres_conn", lambda: DSN)
    yield nome
    with psycopg2.connect(DSN) as conn, conn.cursor() as cursor:
        cursor.execute(f'DROP SCHEMA IF EXISTS "{nome}" CASCADE')
    conn.close()


def _sql(sql: str) -> list[Any]:
    with psycopg2.connect(DSN) as conn, conn.cursor() as cursor:
        cursor.execute(sql)
        linhas = cursor.fetchall() if cursor.description else []
    conn.close()
    return linhas


def _view_do_dbt(schema: str, tabela: str) -> None:
    """Como a ``bronze_<tabela>`` que o dbt materializa como view."""
    _sql(f'CREATE VIEW "{schema}".bronze_{tabela} AS SELECT * FROM "{schema}".{tabela}')


def test_catalogo_recarrega_por_baixo_das_views_do_dbt(dag: Any, schema: str) -> None:
    _rodar(dag, "extrair_catalogo")
    _view_do_dbt(schema, "pesquisas")
    _view_do_dbt(schema, "agregados_catalogo")
    _IbgeFalso.pesquisas[1]["agregados"].append({"id": 7060, "nome": "IPCA"})

    agregados = _rodar(dag, "extrair_catalogo")

    assert [a["id"] for a in agregados] == [4709, 4714, 6579, 7060]
    assert _sql(f'SELECT id FROM "{schema}".bronze_pesquisas ORDER BY id') == [
        ("CD",),
        ("PA",),
    ]
    assert _sql(f'SELECT id FROM "{schema}".bronze_agregados_catalogo ORDER BY id') == [
        ("4709",),
        ("4714",),
        ("6579",),
        ("7060",),
    ]


@pytest.mark.parametrize("task_id", list(_TASKS_DO_LOTE))
def test_lote_rodado_de_novo_nao_duplica_nem_apaga_o_outro_lote(
    dag: Any, schema: str, task_id: str
) -> None:
    tabela = _TASKS_DO_LOTE[task_id]
    _rodar(dag, task_id, batch=["4709", "4714"])
    _rodar(dag, task_id, batch=["6579"])
    contagem = (
        f'SELECT agregado_id, count(*) FROM "{schema}".{tabela} GROUP BY 1 ORDER BY 1'
    )
    antes = _sql(contagem)
    _view_do_dbt(schema, tabela)

    _rodar(dag, task_id, batch=["4709", "4714"])

    assert _sql(contagem) == antes
    assert [agregado for agregado, _ in antes] == ["4709", "4714", "6579"]


def test_periodo_que_saiu_da_api_sai_da_tabela(dag: Any, schema: str) -> None:
    _rodar(dag, "extrair_periodos", batch=["4709"])
    _IbgeFalso.periodos = ["2022", "2023"]

    _rodar(dag, "extrair_periodos", batch=["4709"])

    assert _sql(f'SELECT id FROM "{schema}".periodos ORDER BY id') == [
        ("2022",),
        ("2023",),
    ]


def test_agregado_em_que_a_api_falhou_fica_com_as_linhas_anteriores(
    dag: Any, schema: str
) -> None:
    _rodar(dag, "extrair_periodos", batch=["4709", "4714"])
    _IbgeFalso.periodos = ["2023"]
    _IbgeFalso.falham = {"4714"}

    _rodar(dag, "extrair_periodos", batch=["4709", "4714"])

    assert _sql(
        f'SELECT agregado_id, id FROM "{schema}".periodos ORDER BY agregado_id, id'
    ) == [("4709", "2023"), ("4714", "2010"), ("4714", "2022")]
