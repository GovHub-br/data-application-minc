import logging
import time
from abc import ABC, abstractmethod
from typing import Iterator


class RelationalDatabaseExtractor(ABC):
    """Interface para extratores de bancos relacionais.

    Cada dialeto SQL (SQL Server, MySQL, Postgres) implementa esta ABC numa
    subclasse concreta.  Os métodos concretos ``_build_fqtn`` e ``_select_star``
    usam o padrão Template Method: delegam para ``_quote_identifier`` (abstrato),
    garantindo quoting correto por dialeto sem duplicar a estrutura da query.
    """

    _default_schema: str = ""

    def __init__(self, rde: object = None) -> None:
        self.rde = rde

    # ── Contrato obrigatório ──────────────────────────────────────────────────

    def buildExtraction(
        self,
        database: str,
        table: str,
        chunk_size: int = 50_000,
        schema: str | None = None,
    ) -> Iterator[list[dict]]:
        """Itera sobre *table* em chunks via DB-API 2.0 ``fetchmany``.

        Template Method: delega quoting a ``_quote_identifier``, conexão a
        ``_get_conn`` e metadados a ``list_columns`` — todos abstratos nas
        subclasses.  Subclasses só precisam sobrescrever se precisarem de
        comportamento diferente (ex.: streaming nativo, cursor server-side).

        Args:
            database:   Nome do banco de dados de origem.
            table:      Nome da tabela a extrair.
            chunk_size: Número máximo de linhas por chunk (default 50 000).
            schema:     Schema da tabela (default ``"dbo"``).

        Yields:
            Lista de dicionários ``{coluna: valor}`` com até *chunk_size*
            registros (último chunk pode ter menos).
        """
        schema = schema or self._default_schema
        fqtn = self._build_fqtn(schema, table)
        query = self._select_star(fqtn)
        columns = self.list_columns(database, schema, table)

        logging.info(
            "[%s] buildExtraction: SELECT * FROM %s "
            "(database=%s, chunk_size=%d)",
            type(self).__name__,
            fqtn,
            database,
            chunk_size,
        )

        conn = self._get_conn(database)
        chunk_n = 0
        try:
            cursor = conn.cursor()

            t_exec = time.monotonic()
            logging.info(
                "[%s] buildExtraction: executando query em %s.[%s].[%s]",
                type(self).__name__, database, schema, table,
            )
            cursor.execute(query)
            logging.info(
                "[%s] buildExtraction: cursor aberto em %.1fs — iniciando fetchmany "
                "(chunk_size=%d) em %s.[%s].[%s]",
                type(self).__name__,
                time.monotonic() - t_exec,
                chunk_size,
                database, schema, table,
            )

            while True:
                t_fetch = time.monotonic()
                try:
                    rows = cursor.fetchmany(chunk_size)
                except Exception as fetch_err:
                    elapsed = time.monotonic() - t_exec
                    logging.error(
                        "[%s] ERRO no fetchmany — chunk=%d, tabela=%s.[%s].[%s], "
                        "elapsed_total=%.1fs, tipo_erro=%s: %s",
                        type(self).__name__,
                        chunk_n + 1,
                        database, schema, table,
                        elapsed,
                        type(fetch_err).__name__,
                        fetch_err,
                    )
                    raise
                if not rows:
                    break
                chunk_n += 1
                fetch_ms = (time.monotonic() - t_fetch) * 1000
                logging.debug(
                    "[%s] chunk %d: %d linhas de %s.[%s].[%s] em %.0fms",
                    type(self).__name__,
                    chunk_n,
                    len(rows),
                    database, schema, table,
                    fetch_ms,
                )
                yield [dict(zip(columns, row)) for row in rows]
        finally:
            cursor.close()
            conn.close()

    @abstractmethod
    def list_tables(self, database: str, schema: str) -> list[str]:
        """Retorna os nomes de todas as tabelas base do *schema*.

        Args:
            database: Nome do banco de dados de origem.
            schema:   Nome do schema a inspecionar.

        Returns:
            Lista de nomes de tabela ordenada alfabeticamente.
        """
        ...

    @abstractmethod
    def list_columns(self, database: str, schema: str, table: str) -> list[str]:
        """Retorna os nomes das colunas de *table* na ordem de ORDINAL_POSITION.

        Args:
            database: Nome do banco de dados de origem.
            schema:   Schema da tabela.
            table:    Nome da tabela.

        Returns:
            Lista de nomes de coluna na ordem de definição da tabela.
        """
        ...

    @abstractmethod
    def _get_conn(self, database: str):
        """Retorna uma conexão ativa para *database*.

        Cada dialeto usa o hook/driver apropriado (MsSqlHook, MySqlHook, etc.).

        Args:
            database: Nome do banco de dados de origem.

        Returns:
            Objeto de conexão compatível com DB-API 2.0.
        """
        ...

    @abstractmethod
    def _quote_identifier(self, name: str) -> str:
        """Aplica o quoting de identificadores do dialeto (ex.: ``[name]``).

        Args:
            name: Identificador SQL (schema, tabela, coluna).

        Returns:
            Identificador com quoting do dialeto.
        """
        ...

    # ── Helpers concretos (Template Method) ──────────────────────────────────

    def _build_fqtn(self, schema: str, table: str) -> str:
        """Monta o fully-qualified table name com quoting do dialeto.

        Args:
            schema: Nome do schema.
            table:  Nome da tabela.

        Returns:
            String no formato ``[schema].[table]`` (ou equivalente do dialeto).
        """
        return f"{self._quote_identifier(schema)}.{self._quote_identifier(table)}"

    def _select_star(self, quoted_fqtn: str) -> str:
        """Gera um ``SELECT * FROM <fqtn>`` pronto para executar.

        Args:
            quoted_fqtn: FQTN já com quoting do dialeto.

        Returns:
            String SQL de seleção completa.
        """
        return f"SELECT * FROM {quoted_fqtn}"
