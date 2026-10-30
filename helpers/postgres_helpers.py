import logging

from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extensions import make_dsn


def get_postgres_conn(data_base_name: str = "postgres_default") -> str:
    """Monta o DSN da conexão ``data_base_name`` do Airflow.

    Lê só o cadastro da conexão — não abre conexão com o banco. A versão
    anterior chamava ``hook.get_conn()`` para descobrir os parâmetros, o que
    custava um handshake e deixava uma conexão aberta a cada cliente criado.
    O ``make_dsn`` cuida do quoting: senha com espaço ou aspas não quebra o
    DSN.
    """
    try:
        conexao = PostgresHook.get_connection(data_base_name)
    except Exception as e:
        logging.error(f"Failed to obtain PostgreSQL connection: {e}")
        raise

    parametros = {
        "dbname": conexao.schema,
        "user": conexao.login,
        "password": conexao.password,
        "host": conexao.host,
        "port": conexao.port,
    }
    logging.info(
        f"[postgres_helpers] Obtained PostgreSQL connection: "
        f"dbname={conexao.schema}, user={conexao.login}, "
        f"host={conexao.host}, port={conexao.port}"
    )
    dsn: str = make_dsn(**{k: v for k, v in parametros.items() if v not in (None, "")})
    return dsn
