"""Testes do ``ClientPostgresDB`` contra um Postgres de verdade.

O cliente é quase todo DDL dinâmico, transação e quoting de identificador —
o tipo de coisa que um mock de ``psycopg2`` aprova sem testar. Por isso os
testes que tocam o banco rodam num Postgres real, apontado por
``TEST_POSTGRES_DSN``, e são pulados quando a variável não existe. Um banco
descartável basta:

    docker run -d --rm --name pg-teste -e POSTGRES_PASSWORD=teste \\
        -e POSTGRES_DB=teste -p 55432:5432 postgres:16-alpine
    export TEST_POSTGRES_DSN="host=localhost port=55432 dbname=teste \\
        user=postgres password=teste"

Cada teste recebe um schema próprio, apagado no fim.
"""

import logging
import os
import uuid
from typing import Any, Iterator, List

import psycopg2
import psycopg2.extensions
import psycopg2.extras
import pytest

from cliente_postgres import ClientPostgresDB

DSN = os.environ.get("TEST_POSTGRES_DSN")


def _exigir_banco() -> None:
    if not DSN:
        pytest.skip("TEST_POSTGRES_DSN não definido — sem Postgres para testar")


@pytest.fixture
def schema() -> Iterator[str]:
    _exigir_banco()
    nome = f"teste_{uuid.uuid4().hex[:10]}"
    yield nome
    with psycopg2.connect(DSN) as conn, conn.cursor() as cursor:
        cursor.execute(f'DROP SCHEMA IF EXISTS "{nome}" CASCADE')
    conn.close()


@pytest.fixture
def conexoes_abertas(monkeypatch: pytest.MonkeyPatch) -> List[Any]:
    """Espiona ``psycopg2.connect``: cada item é uma conexão aberta pelo cliente."""
    abertas: List[Any] = []
    connect_real = psycopg2.connect

    def espiao(*args: Any, **kwargs: Any) -> Any:
        conn = connect_real(*args, **kwargs)
        abertas.append(conn)
        return conn

    monkeypatch.setattr(psycopg2, "connect", espiao)
    return abertas


@pytest.fixture
def db() -> Iterator[ClientPostgresDB]:
    _exigir_banco()
    assert DSN
    cliente = ClientPostgresDB(DSN)
    yield cliente
    cliente.close()


def consultar(sql: str) -> List[Any]:
    with psycopg2.connect(DSN) as conn, conn.cursor() as cursor:
        cursor.execute(sql)
        linhas = cursor.fetchall()
    conn.close()
    return linhas


def colunas(schema: str, tabela: str) -> List[str]:
    return [
        linha[0]
        for linha in consultar(
            "SELECT column_name FROM information_schema.columns "
            f"WHERE table_schema = '{schema}' AND table_name = '{tabela}' "
            "ORDER BY ordinal_position"
        )
    ]


# ---------------------------------------------------------------------------
# Conexão
# ---------------------------------------------------------------------------


def test_insert_data_abre_uma_conexao_so(
    db: ClientPostgresDB, schema: str, conexoes_abertas: List[Any]
) -> None:
    db.insert_data([{"id": 1, "nome": "a"}], "t", schema=schema)

    assert len(conexoes_abertas) == 1


def test_chamadas_seguidas_reaproveitam_a_conexao(
    db: ClientPostgresDB, schema: str, conexoes_abertas: List[Any]
) -> None:
    for i in range(5):
        db.insert_data([{"id": i}], "t", schema=schema)
    db.execute_query(f'SELECT count(*) FROM "{schema}".t')
    db.drop_table_if_exists("t", schema=schema)

    assert len(conexoes_abertas) == 1


def test_context_manager_fecha_a_conexao(
    schema: str, conexoes_abertas: List[Any]
) -> None:
    assert DSN
    with ClientPostgresDB(DSN) as db:
        db.insert_data([{"id": 1}], "t", schema=schema)

    assert len(conexoes_abertas) == 1
    assert conexoes_abertas[0].closed


def test_conexao_perdida_e_reaberta(
    db: ClientPostgresDB, schema: str, conexoes_abertas: List[Any]
) -> None:
    db.insert_data([{"id": 1}], "t", schema=schema)
    conexoes_abertas[0].close()

    db.insert_data([{"id": 2}], "t", schema=schema)

    assert len(conexoes_abertas) == 2
    assert consultar(f'SELECT count(*) FROM "{schema}".t') == [(2,)]


def test_falha_nao_envenena_a_conexao(db: ClientPostgresDB, schema: str) -> None:
    """Quem pega a exceção e segue no laço (``extracao_anexos_dag``) precisa
    de uma conexão utilizável, não de ``InFailedSqlTransaction``."""
    db.insert_data([{"id": 1}], "t", schema=schema, primary_key=["id"])
    with pytest.raises(RuntimeError):
        db.insert_data([{"id": 1}], "t", schema=schema)

    db.insert_data([{"id": 2}], "t", schema=schema)

    assert consultar(f'SELECT id FROM "{schema}".t ORDER BY id') == [("1",), ("2",)]


def test_conexao_ociosa_nao_segura_transacao(
    db: ClientPostgresDB, schema: str, conexoes_abertas: List[Any]
) -> None:
    """Conexão longa parada em ``idle in transaction`` segura lock e trava o
    ``DROP``/``ALTER`` de quem vier depois — o dbt, por exemplo."""
    db.insert_data([{"id": 1}], "t", schema=schema)
    db.execute_query(f'SELECT * FROM "{schema}".t')

    status = conexoes_abertas[0].info.transaction_status

    assert status == psycopg2.extensions.TRANSACTION_STATUS_IDLE


def test_senha_nao_vai_para_o_log(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.DEBUG)

    ClientPostgresDB("host=h dbname=d user=u password=segredo-xyz")

    assert "segredo-xyz" not in caplog.text


# ---------------------------------------------------------------------------
# insert_data — comportamento que não pode mudar
# ---------------------------------------------------------------------------


def test_insert_data_cria_schema_tabela_e_colunas_em_minusculo(
    db: ClientPostgresDB, schema: str
) -> None:
    db.insert_data([{"tipoAnexo": "x", "Id": 1}], "anexos", schema=schema)

    assert colunas(schema, "anexos") == ["tipoanexo", "id"]
    assert consultar(f'SELECT tipoanexo, id FROM "{schema}".anexos') == [("x", "1")]


def test_insert_data_achata_json_aninhado_com_separador(
    db: ClientPostgresDB, schema: str
) -> None:
    db.insert_data([{"a": {"B": 1}, "lista": [1, 2]}], "t", schema=schema)

    assert sorted(colunas(schema, "t")) == ["a__b", "lista"]
    assert consultar(f'SELECT lista FROM "{schema}".t') == [("[1, 2]",)]


def test_insert_data_evolui_schema_com_chave_nova(
    db: ClientPostgresDB, schema: str
) -> None:
    db.insert_data([{"id": 1}], "t", schema=schema)
    db.insert_data([{"id": 2, "nova": "n"}], "t", schema=schema)

    assert colunas(schema, "t") == ["id", "nova"]
    assert consultar(f'SELECT id, nova FROM "{schema}".t ORDER BY id') == [
        ("1", None),
        ("2", "n"),
    ]


def test_insert_data_ve_coluna_criada_por_fora(db: ClientPostgresDB, schema: str) -> None:
    """Outra task pode ter evoluído a tabela entre duas chamadas."""
    db.insert_data([{"id": 1}], "t", schema=schema)
    with psycopg2.connect(DSN) as conn, conn.cursor() as cursor:
        cursor.execute(f'ALTER TABLE "{schema}".t ADD COLUMN extra TEXT')
    conn.close()

    db.insert_data([{"id": 2, "extra": "e"}], "t", schema=schema)

    assert consultar(f"SELECT extra FROM \"{schema}\".t WHERE id = '2'") == [("e",)]


def test_insert_data_vazio_nao_toca_no_banco(
    db: ClientPostgresDB, schema: str, conexoes_abertas: List[Any]
) -> None:
    db.insert_data([], "t", schema=schema)

    assert conexoes_abertas == []


def test_upsert_atualiza_pela_chave(db: ClientPostgresDB, schema: str) -> None:
    kwargs: Any = {"schema": schema, "primary_key": ["id"], "conflict_fields": ["id"]}
    db.insert_data([{"id": 1, "v": "velho"}], "t", **kwargs)
    db.insert_data([{"id": 1, "v": "novo"}], "t", **kwargs)

    assert consultar(f'SELECT id, v FROM "{schema}".t') == [("1", "novo")]


def test_upsert_com_chave_repetida_no_mesmo_lote_fica_com_a_ultima(
    db: ClientPostgresDB, schema: str
) -> None:
    """Sem deduplicar, o Postgres recusa o lote inteiro: ``ON CONFLICT DO
    UPDATE command cannot affect row a second time``."""
    db.insert_data(
        [{"id": 1, "v": "a"}, {"id": 2, "v": "b"}, {"id": 1, "v": "c"}],
        "t",
        schema=schema,
        primary_key=["id"],
        conflict_fields=["id"],
    )

    assert consultar(f'SELECT id, v FROM "{schema}".t ORDER BY id') == [
        ("1", "c"),
        ("2", "b"),
    ]


def test_chave_em_camelcase_casa_com_a_coluna_minuscula(
    db: ClientPostgresDB, schema: str
) -> None:
    db.insert_data(
        [{"idPlano": 1, "v": "a"}],
        "t",
        schema=schema,
        primary_key=["idPlano"],
        conflict_fields=["idPlano"],
    )
    db.insert_data(
        [{"idPlano": 1, "v": "b"}],
        "t",
        schema=schema,
        primary_key=["idPlano"],
        conflict_fields=["idPlano"],
    )

    assert consultar(f'SELECT idplano, v FROM "{schema}".t') == [("1", "b")]


def test_insert_data_aguenta_lote_maior_que_uma_pagina(
    db: ClientPostgresDB, schema: str
) -> None:
    linhas = [{"id": i} for i in range(2_500)]

    db.insert_data(linhas, "t", schema=schema)

    assert consultar(f'SELECT count(*) FROM "{schema}".t') == [(2_500,)]


def test_limite_de_colunas_manda_excedente_para_payload_origem(
    db: ClientPostgresDB, schema: str
) -> None:
    db.insert_data([{"a": 1, "b": 2}], "t", schema=schema, limite_colunas=2)
    db.insert_data([{"a": 3, "c": 4}], "t", schema=schema, limite_colunas=2)

    assert colunas(schema, "t") == ["a", "b", "payload_origem"]
    assert consultar(f"SELECT a, payload_origem FROM \"{schema}\".t WHERE a = '3'") == [
        ("3", '{"c": 4}')
    ]


def test_identificador_com_aspas_nao_quebra_o_sql(
    db: ClientPostgresDB, schema: str
) -> None:
    db.insert_data([{'col"estranha': 1}], 'tab"ela', schema=schema)

    assert colunas(schema, 'tab"ela') == ['col"estranha']


# ---------------------------------------------------------------------------
# _evolve_schema e create_table_if_not_exists (usados por cliente_inep)
# ---------------------------------------------------------------------------


def test_evolve_schema_ignora_coluna_que_ja_existe(
    db: ClientPostgresDB, schema: str
) -> None:
    db.create_table_if_not_exists({"a": None}, "t", schema=schema)

    excedentes = db._evolve_schema(["a", "b", "c"], "t", schema=schema)

    assert excedentes == set()
    assert colunas(schema, "t") == ["a", "b", "c"]


def test_evolve_schema_devolve_o_que_passou_do_limite(
    db: ClientPostgresDB, schema: str
) -> None:
    db.create_table_if_not_exists({"a": None}, "t", schema=schema)

    excedentes = db._evolve_schema(["a", "b", "c"], "t", schema=schema, limite_colunas=2)

    assert excedentes == {"c"}
    assert colunas(schema, "t") == ["a", "b"]


# ---------------------------------------------------------------------------
# drop_table_if_exists e insert_csv_data
# ---------------------------------------------------------------------------


def test_drop_seguido_de_insert_recria_a_tabela(
    db: ClientPostgresDB, schema: str
) -> None:
    db.insert_data([{"velha": 1}], "t", schema=schema)
    db.drop_table_if_exists("t", schema=schema)
    db.insert_data([{"nova": 1}], "t", schema=schema)

    assert colunas(schema, "t") == ["nova"]


def test_drop_propaga_o_erro(db: ClientPostgresDB, schema: str) -> None:
    """Engolir o erro deixava o ``insert_csv_data`` gravar em cima da tabela
    antiga."""
    db.insert_data([{"id": 1}], "t", schema=schema)
    db.execute_statement(f'CREATE VIEW "{schema}".v AS SELECT * FROM "{schema}".t')

    with pytest.raises(psycopg2.Error):
        db.drop_table_if_exists("t", schema=schema)


def test_insert_csv_data_substitui_a_tabela(db: ClientPostgresDB, schema: str) -> None:
    db.insert_csv_data("a,b\n1,x\n", "t", schema=schema)
    db.insert_csv_data("c\n9\n", "t", schema=schema)

    assert colunas(schema, "t") == ["c"]
    assert consultar(f'SELECT c FROM "{schema}".t') == [("9",)]


def test_insert_csv_data_que_falha_preserva_a_tabela_antiga(
    db: ClientPostgresDB, schema: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    db.insert_csv_data("a\n1\n", "t", schema=schema)

    def explode(*args: Any, **kwargs: Any) -> None:
        raise psycopg2.OperationalError("caiu no meio")

    monkeypatch.setattr(psycopg2.extras, "execute_values", explode)
    with pytest.raises(RuntimeError):
        db.insert_csv_data("b\n2\n", "t", schema=schema)

    assert colunas(schema, "t") == ["a"]
    assert consultar(f'SELECT a FROM "{schema}".t') == [("1",)]


# ---------------------------------------------------------------------------
# execute_query e execute_statement
# ---------------------------------------------------------------------------


def test_execute_statement_e_execute_query(db: ClientPostgresDB, schema: str) -> None:
    db.execute_statement(f'CREATE SCHEMA "{schema}"')
    db.execute_statement(f'CREATE TABLE "{schema}".t (x int)')
    db.execute_statement(f'INSERT INTO "{schema}".t VALUES (1), (2)')

    assert db.execute_query(f'SELECT x FROM "{schema}".t ORDER BY x') == [(1,), (2,)]
