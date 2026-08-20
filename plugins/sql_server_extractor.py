import logging

import pymssql
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook

from relational_database_extractor import RelationalDatabaseExtractor


class SQLServerExtractor(RelationalDatabaseExtractor):
    """Extrator concreto para Microsoft SQL Server.

    Usa :class:`~airflow.providers.microsoft.mssql.hooks.mssql.MsSqlHook`
    (provider ``apache-airflow-providers-microsoft-mssql``) para obter a
    conexão via Airflow Connection, sem hardcodar host ou credenciais.

    O provider usa ``pymssql`` por padrão.  Identificadores são quotados com
    colchetes (``[schema].[tabela]``), conforme dialeto T-SQL.

    Exemplo::

        extractor = SQLServerExtractor(conn_id="mssql_salic_agentes")
        for chunk in extractor.buildExtraction("Agentes", "Projetos"):
            process(chunk)
    """

    _default_schema: str = "dbo"

    def __init__(self, conn_id: str) -> None:
        """Inicializa o extrator com o Airflow Connection ID do SQL Server.

        Args:
            conn_id: ID da Airflow Connection do tipo Microsoft SQL Server.
        """
        super().__init__(rde=conn_id)
        self.conn_id = conn_id

    # ── Quoting (dialeto SQL Server) ──────────────────────────────────────────

    def _quote_identifier(self, name: str) -> str:
        return f"[{name}]"

    # ── Conexão ───────────────────────────────────────────────────────────────

    def _get_conn(self, database: str):
        """Conecta ao SQL Server com timeout=0 para evitar remote query timeout.

        MsSqlHook.get_conn() não expõe o parâmetro timeout do pymssql, então
        extraímos os parâmetros da Connection do Airflow e abrimos a conexão
        diretamente.  timeout=0 desabilita o query-level timeout no driver
        pymssql (distinto do remote query timeout do servidor, mas ajuda a
        manter o cursor ativo durante fetchmany de tabelas grandes).
        """
        hook = MsSqlHook(mssql_conn_id=self.conn_id, schema=database)
        airflow_conn = hook.get_connection(self.conn_id)

        host = airflow_conn.host
        port = int(airflow_conn.port or 1433)
        login = airflow_conn.login
        password = airflow_conn.password

        logging.info(
            "[SQLServerExtractor] _get_conn: conectando em %s:%d db=%s (conn_id=%s, timeout=0)",
            host, port, database, self.conn_id,
        )
        return pymssql.connect(
            server=host,
            port=port,
            user=login,
            password=password,
            database=database,
            timeout=0,       # sem query-level timeout no driver
            login_timeout=30,
        )

    # ── Metadados ─────────────────────────────────────────────────────────────

    def list_tables(self, database: str, schema: str = "dbo") -> list[str]:
        """Retorna todas as tabelas base do *schema* via INFORMATION_SCHEMA.

        Args:
            database: Nome do banco de dados de origem.
            schema:   Schema do SQL Server (default ``"dbo"``).

        Returns:
            Lista de nomes de tabela ordenada alfabeticamente.
        """
        query = (
            "SELECT TABLE_NAME "
            "FROM INFORMATION_SCHEMA.TABLES "
            "WHERE TABLE_TYPE = 'BASE TABLE' "
            "  AND TABLE_SCHEMA = %s "
            "ORDER BY TABLE_NAME"
        )
        conn = self._get_conn(database)
        try:
            cursor = conn.cursor()
            cursor.execute(query, (schema,))
            tables = [row[0] for row in cursor.fetchall()]
            logging.info(
                "[SQLServerExtractor] list_tables: %d tabelas em %s.[%s] (conn=%s)",
                len(tables),
                database,
                schema,
                self.conn_id,
            )
            return tables
        finally:
            conn.close()

    def list_columns(self, database: str, schema: str, table: str) -> list[str]:
        """Retorna as colunas de *table* ordenadas por ORDINAL_POSITION.

        Args:
            database: Nome do banco de dados de origem.
            schema:   Schema da tabela.
            table:    Nome da tabela.

        Returns:
            Lista de nomes de coluna na ordem de definição.
        """
        query = (
            "SELECT COLUMN_NAME "
            "FROM INFORMATION_SCHEMA.COLUMNS "
            "WHERE TABLE_SCHEMA = %s "
            "  AND TABLE_NAME = %s "
            "ORDER BY ORDINAL_POSITION"
        )
        conn = self._get_conn(database)
        try:
            cursor = conn.cursor()
            cursor.execute(query, (schema, table))
            columns = [row[0] for row in cursor.fetchall()]
            logging.info(
                "[SQLServerExtractor] list_columns: %d colunas em %s.[%s].[%s]",
                len(columns),
                database,
                schema,
                table,
            )
            return columns
        finally:
            conn.close()

