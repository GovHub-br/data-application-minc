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

    # Resultados: duas localidades por nível, um valor por período pedido e
    # uma linha por categoria da classificação (só a primeira classificação).
    niveis_vazios: set[str] = set()
    pedidos: list[dict[str, Any]] = []

    def get_resultados(
        self,
        agregado_id: int | str,
        periodos: str = "ultimo",
        variaveis: str = "allxp",
        nivel: str = "N3",
        localidades: str = "all",
        classificacao: str | None = None,
        timeout: float = 0,
    ) -> list[dict[str, Any]]:
        self.pedidos.append(
            {"agregado": str(agregado_id), "nivel": nivel, "classificacao": classificacao}
        )
        if nivel in self.niveis_vazios or str(agregado_id) in self.falham:
            return []
        locais = {"N3": ("11", "53"), "N6": ("1100205", "5300108")}[nivel]
        if classificacao:
            cls, cats = classificacao.split("|")[0].rstrip("]").split("[")
            combinacoes = [
                [{"id": cls, "nome": "C", "categoria": {cat: f"cat {cat}"}}]
                for cat in cats.split(",")
            ]
        else:
            combinacoes = [[]]
        return [
            {
                "id": var,
                "variavel": f"Variável {var}",
                "unidade": "Pessoas",
                "resultados": [
                    {
                        "classificacoes": combinacao,
                        "series": [
                            {
                                "localidade": {"id": local, "nome": local},
                                "serie": {p: "10" for p in periodos.split("|")},
                            }
                            for local in locais
                        ],
                    }
                    for combinacao in combinacoes
                ],
            }
            for var in variaveis.split("|")
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
    monkeypatch.setattr(_IbgeFalso, "niveis_vazios", set())
    monkeypatch.setattr(_IbgeFalso, "pedidos", [])
    monkeypatch.setattr(modulo, "ClienteIBGE", _IbgeFalso)
    monkeypatch.setattr(modulo, "_get_config", lambda: dict(modulo._IBGE_CONFIG_DEFAULTS))
    return modulo.ibge_agregados_dag()


def _configurar(modulo: Any, monkeypatch: pytest.MonkeyPatch, **config: Any) -> None:
    """A Variable ibge_config com ``config`` por cima dos defaults."""
    monkeypatch.setattr(
        modulo, "_get_config", lambda: {**modulo._IBGE_CONFIG_DEFAULTS, **config}
    )


# Cor ou raça (9605) em dois níveis e a população estimada (6579) em um.
_COR_OU_RACA = {
    "agregado": "9605",
    "variaveis": "1000093",
    "periodos": "2022",
    "classificacao": "86[2776,2777]",
    "niveis": ["N6", "N3"],
}
_ESTIMATIVA = {"agregado": "6579", "variaveis": "9324", "periodos": "2025|2026"}


def _rodar(dag: Any, task_id: str, **kwargs: Any) -> Any:
    return dag.get_task(task_id).python_callable(**kwargs)


# ---------------------------------------------------------------------------
# Sem banco
# ---------------------------------------------------------------------------


def test_dag_carrega_e_o_catalogo_vem_antes_de_tudo() -> None:
    bag = DagBag(str(_ARQUIVO), include_examples=False)
    assert not bag.import_errors, bag.import_errors
    dag = bag.dags["ibge_agregados_dag"]
    # Dois runs juntos gravariam os mesmos agregados ao mesmo tempo.
    assert dag.max_active_runs == 1

    assert dag.get_task("extrair_catalogo").downstream_task_ids == {
        "preparar_batches",
        "preparar_resultados",
    }
    assert dag.get_task("preparar_batches").downstream_task_ids == set(_TASKS_DO_LOTE)
    assert dag.get_task("preparar_resultados").downstream_task_ids == {
        "extrair_resultados",
        "apagar_resultados_fora_da_config",
    }


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
        type(self).ultimo = self

    def substituir_dados(self, data: list, table_name: str, **kwargs: Any) -> None:
        self.chamadas.append(("substituir_dados", table_name, kwargs))

    def insert_data(self, data: list, table_name: str, **kwargs: Any) -> None:
        self.chamadas.append(("insert_data", table_name, kwargs))

    def apagar_fora_do_recorte(
        self, table_name: str, schema: str, coluna_recorte: str, manter: list
    ) -> int:
        self.chamadas.append(
            (
                "apagar_fora_do_recorte",
                table_name,
                {"coluna_recorte": coluna_recorte, "manter": manter},
            )
        )
        return 0

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
# Resultados
# ---------------------------------------------------------------------------


def test_preparar_resultados_com_configuracao_invalida_falha_sem_retry(
    dag: Any, modulo: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    _configurar(
        modulo,
        monkeypatch,
        resultados=[{"agregado": "4709"}],
        resultados_agregados_ids=["4709"],
    )
    with pytest.raises(AirflowFailException, match="duas vezes"):
        _rodar(dag, "preparar_resultados")


def test_preparar_resultados_junta_o_formato_novo_e_o_antigo(
    dag: Any, modulo: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    _configurar(
        modulo,
        monkeypatch,
        resultados=[_COR_OU_RACA],
        resultados_agregados_ids=["4709"],
        resultados_periodos="2022",
        resultados_nivel="N6",
    )
    especificacoes = _rodar(dag, "preparar_resultados")
    assert [(e["agregado"], e["niveis"]) for e in especificacoes] == [
        ("9605", ["N6", "N3"]),
        ("4709", ["N6"]),
    ]


class _BancoComContexto(_BancoQueRegistra):
    def __enter__(self) -> "_BancoComContexto":
        return self

    def __exit__(self, *args: Any) -> None:
        pass


def test_resultados_do_agregado_gravados_juntos_em_todos_os_niveis(
    dag: Any, modulo: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(modulo, "ClientPostgresDB", _BancoComContexto)
    monkeypatch.setattr(modulo, "get_postgres_conn", lambda: "")
    (espec,) = _especificacoes(modulo, [_COR_OU_RACA])

    _rodar(dag, "extrair_resultados", spec=espec)

    assert [p["classificacao"] for p in _IbgeFalso.pedidos] == ["86[2776,2777]"] * 2
    ((metodo, tabela, kwargs),) = _BancoComContexto.ultimo.chamadas
    assert (metodo, tabela) == ("substituir_dados", "resultados")
    assert kwargs["coluna_recorte"] == "agregado_id"
    assert kwargs["primary_key"] == modulo.CHAVE_RESULTADOS


def test_nivel_vazio_falha_sem_gravar_nada(
    dag: Any, modulo: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A API devolve ``[]`` quando responde com erro: gravar o N6 sem o N3
    apagaria as linhas de UF do run anterior."""
    monkeypatch.setattr(modulo, "ClientPostgresDB", _BancoComContexto)
    monkeypatch.setattr(modulo, "get_postgres_conn", lambda: "")
    _BancoComContexto.ultimo = _BancoComContexto("")
    _IbgeFalso.niveis_vazios = {"N3"}
    (espec,) = _especificacoes(modulo, [_COR_OU_RACA])

    # O nível pode não existir para o agregado: a mensagem diz onde conferir.
    with pytest.raises(
        RuntimeError, match=r"nivel=N3 sem dados.*/agregados/9605/localidades/N3"
    ):
        _rodar(dag, "extrair_resultados", spec=espec)

    assert _BancoComContexto.ultimo.chamadas == []


def test_agregados_fora_da_config_sao_apagados_de_resultados(
    dag: Any, modulo: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(modulo, "ClientPostgresDB", _BancoComContexto)
    monkeypatch.setattr(modulo, "get_postgres_conn", lambda: "")

    _rodar(
        dag,
        "apagar_resultados_fora_da_config",
        especificacoes=_especificacoes(modulo, [_COR_OU_RACA, _ESTIMATIVA]),
    )

    assert _BancoComContexto.ultimo.chamadas == [
        (
            "apagar_fora_do_recorte",
            "resultados",
            {"coluna_recorte": "agregado_id", "manter": ["9605", "6579"]},
        )
    ]


def _especificacoes(modulo: Any, resultados: list[dict[str, Any]]) -> list[Any]:
    return modulo.especificacoes_resultados(
        {**modulo._IBGE_CONFIG_DEFAULTS, "resultados": resultados}
    )


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


def _contagem_resultados(schema: str) -> list[Any]:
    return _sql(
        f"SELECT agregado_id, nivel, periodo, classificacoes_chave, count(*) "
        f'FROM "{schema}".resultados GROUP BY 1, 2, 3, 4 ORDER BY 1, 2, 3, 4'
    )


def test_resultados_rodados_de_novo_nao_duplicam_e_seguem_a_variable(
    dag: Any, modulo: Any, schema: str
) -> None:
    cor, estimativa = _especificacoes(modulo, [_COR_OU_RACA, _ESTIMATIVA])
    _rodar(dag, "extrair_resultados", spec=cor)
    _rodar(dag, "extrair_resultados", spec=estimativa)
    antes = _contagem_resultados(schema)
    _view_do_dbt(schema, "resultados")

    _rodar(dag, "extrair_resultados", spec=cor)
    _rodar(dag, "extrair_resultados", spec=estimativa)
    assert _contagem_resultados(schema) == antes
    # 2 localidades × (N6 + N3) × 2 categorias; 2 localidades × N3 × 2 períodos.
    assert sum(n for agregado, *_, n in antes if agregado == "9605") == 8
    assert sum(n for agregado, *_, n in antes if agregado == "6579") == 4

    # Período que sai da Variable sai do banco; o outro agregado fica.
    (so_2026,) = _especificacoes(modulo, [{**_ESTIMATIVA, "periodos": "2026"}])
    _rodar(dag, "extrair_resultados", spec=so_2026)
    depois = _contagem_resultados(schema)
    assert {(a, p) for a, _, p, _, _ in depois if a == "6579"} == {("6579", "2026")}
    assert [linha for linha in depois if linha[0] == "9605"] == [
        linha for linha in antes if linha[0] == "9605"
    ]


def test_agregado_que_sai_da_variable_sai_do_banco(
    dag: Any, modulo: Any, schema: str
) -> None:
    cor, estimativa = _especificacoes(modulo, [_COR_OU_RACA, _ESTIMATIVA])
    _rodar(dag, "extrair_resultados", spec=cor)
    _rodar(dag, "extrair_resultados", spec=estimativa)

    # Variable vazia (ou sumida) não apaga nada.
    _rodar(dag, "apagar_resultados_fora_da_config", especificacoes=[])
    assert {linha[0] for linha in _contagem_resultados(schema)} == {"6579", "9605"}

    _rodar(dag, "apagar_resultados_fora_da_config", especificacoes=[cor])
    assert {linha[0] for linha in _contagem_resultados(schema)} == {"9605"}


def test_linha_com_a_chave_no_formato_antigo_e_trocada_e_nao_duplicada(
    dag: Any, modulo: Any, schema: str
) -> None:
    """Antes, a chave era o str() das classificações, com os nomes. A carga
    substitui o agregado inteiro, então a linha antiga sai."""
    (cor,) = _especificacoes(modulo, [{**_COR_OU_RACA, "niveis": ["N3"]}])
    _rodar(dag, "extrair_resultados", spec=cor)
    antigo = "[{'id': '86', 'nome': 'C', 'categoria': {'2776': 'cat 2776'}}]"
    _sql(
        f'UPDATE "{schema}".resultados SET classificacoes_chave = $${antigo}$$ '
        "WHERE classificacoes_chave = '86[2776]'"
    )

    _rodar(dag, "extrair_resultados", spec=cor)

    assert _sql(
        f'SELECT DISTINCT classificacoes_chave FROM "{schema}".resultados ORDER BY 1'
    ) == [("86[2776]",), ("86[2777]",)]
    assert _sql(f'SELECT count(*) FROM "{schema}".resultados') == [(4,)]
