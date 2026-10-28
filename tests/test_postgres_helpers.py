from typing import Any

import psycopg2
import psycopg2.extensions
import pytest
from airflow.models.connection import Connection

import postgres_helpers
from postgres_helpers import get_postgres_conn


@pytest.fixture
def conexao_airflow(monkeypatch: pytest.MonkeyPatch) -> Connection:
    conexao = Connection(
        conn_id="postgres_default",
        conn_type="postgres",
        host="pg.interno",
        port=5433,
        login="airflow",
        password="se nha'com espaço",
        schema="dbanalytics",
    )
    monkeypatch.setattr(
        postgres_helpers.PostgresHook,
        "get_connection",
        classmethod(lambda cls, conn_id: conexao),
    )
    return conexao


def test_monta_o_dsn_sem_abrir_conexao(
    conexao_airflow: Connection, monkeypatch: pytest.MonkeyPatch
) -> None:
    def proibido(*args: Any, **kwargs: Any) -> None:
        raise AssertionError("get_postgres_conn não deveria abrir conexão")

    monkeypatch.setattr(psycopg2, "connect", proibido)

    dsn = get_postgres_conn()

    assert psycopg2.extensions.parse_dsn(dsn) == {
        "host": "pg.interno",
        "port": "5433",
        "user": "airflow",
        "password": "se nha'com espaço",
        "dbname": "dbanalytics",
    }


def test_senha_nao_vai_para_o_log(
    conexao_airflow: Connection, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level("DEBUG")

    get_postgres_conn()

    assert "se nha" not in caplog.text


def test_campos_vazios_ficam_de_fora_do_dsn(
    conexao_airflow: Connection,
) -> None:
    conexao_airflow.port = None
    conexao_airflow.schema = ""

    dsn = get_postgres_conn()

    assert set(psycopg2.extensions.parse_dsn(dsn)) == {"host", "user", "password"}
