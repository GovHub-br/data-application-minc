import logging

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
        hook = MsSqlHook(mssql_conn_id=self.conn_id, schema=database)
        return hook.get_conn()

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

