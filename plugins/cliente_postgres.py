import json
import logging
from typing import Any, Dict, List, Optional, Tuple
import psycopg2
import psycopg2.extras  # Added import for execute_values
from pandas import json_normalize
import pandas as pd
import io


class ClientPostgresDB:
    """Client for interacting with PostgreSQL database."""

    SEPARATOR = "__"

    def _flatten_data(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Flatten nested JSON data and normalize keys to lowercase.

        Normalizing to lowercase avoids PostgreSQL case-folding conflicts:
        unquoted identifiers are folded to lowercase by Postgres, so sending
        CamelCase keys (e.g. ``tipoAnexo``) would create columns named
        ``tipoanexo`` while the INSERT would reference ``tipoAnexo`` — causing
        ``UndefinedColumn``.  By lowercasing on the Python side we guarantee
        consistency between column creation and column reference.

        Args:
            data (List[Dict[str, Any]]): List of nested dictionaries.

        Returns:
            List[Dict[str, Any]]: List of flat dictionaries with lowercase keys.
        """
        return list(
            map(
                lambda d: {
                    str(k).lower(): v if type(v) is not list else str(v)
                    for k, v in d.items()
                },
                json_normalize(data, sep=ClientPostgresDB.SEPARATOR).to_dict(
                    orient="records"
                ),
            )
        )

    def __init__(self, conn_str: str) -> None:
        self.conn_str = conn_str
        logging.info(
            f"[cliente_postgres.py] Initialized ClientPostgresDB with conn_str: "
            f"{conn_str}"
        )

    def create_table_if_not_exists(
        self,
        sample_data: Dict[str, Any],
        table_name: str,
        primary_key: Optional[List[str]] = None,
        schema: str = "raw",
    ) -> None:
        """Create table dynamically based on data structure.

        Args:
            sample_data: Sample data to determine schema
            table_name: Name of table to create
            primary_key: List of primary key column names
            schema: Database schema name
        """
        with psycopg2.connect(self.conn_str) as conn:
            with conn.cursor() as cursor:
                cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")
                logging.info(f"[cliente_postgres.py] Schema {schema} ensured to exist")

                flattened_sample = self._flatten_data([sample_data])[0]
                column_definitions: List[str] = []

                for column in flattened_sample.keys():
                    column_definitions.append(f'"{column}" TEXT')

                if primary_key:
                    pk_str = ", ".join(primary_key)
                    column_definitions.append(f"PRIMARY KEY ({pk_str})")

                create_table_query = (
                    f"CREATE TABLE IF NOT EXISTS {schema}.{table_name} ("
                    f"{', '.join(column_definitions)});"
                )

                try:
                    cursor.execute(create_table_query)
                    logging.info(
                        f"[cliente_postgres.py] Table {schema}.{table_name} created "
                        f"or already exists"
                    )
                except psycopg2.Error as err:
                    logging.error(
                        f"[cliente_postgres.py] Failed to create table {schema}."
                        f"{table_name}. Error: {str(err)}"
                    )
                    raise RuntimeError(
                        f"Failed to create table {schema}.{table_name}"
                    ) from err

    def _evolve_schema(
        self,
        columns: List[str],
        table_name: str,
        schema: str = "raw",
        limite_colunas: Optional[int] = None,
    ) -> set:
        """Adiciona colunas novas à tabela que ainda não existem no banco.

        Consulta ``information_schema.columns`` para descobrir as colunas
        existentes e, para cada chave em *columns* que não esteja presente,
        executa ``ALTER TABLE … ADD COLUMN`` com tipo ``TEXT``.

        Isso resolve o problema de *Schema Drift* quando APIs governamentais
        retornam chaves inéditas nos JSONs — camada RAW aceita qualquer
        coluna nova sem quebrar o pipeline.

        Parameters
        ----------
        columns : List[str]
            Lista de nomes de colunas extraídas dos dados de entrada.
        table_name : str
            Nome da tabela destino.
        schema : str
            Schema do banco (default: ``"raw"``).
        limite_colunas : Optional[int]
            Teto de colunas da tabela. Ao ser atingido, as colunas novas
            deixam de ser criadas e são devolvidas ao chamador em vez de
            adicionadas. Serve para tabelas que consolidam planilhas de
            milhares de entes, onde cada layout novo traz cabeçalhos
            inéditos e o crescimento é ilimitado — o Postgres para de aceitar
            colunas em 1600 e o INSERT passa a falhar para todo mundo.

        Returns
        -------
        set
            Colunas que **não** foram criadas por causa do limite.
        """
        with psycopg2.connect(self.conn_str) as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_schema = %s AND table_name = %s
                    """,
                    (schema, table_name),
                )
                existing = {row[0] for row in cursor.fetchall()}

                novas = [c for c in columns if c not in existing]
                if not novas:
                    return set()

                if limite_colunas is not None:
                    vagas = max(0, limite_colunas - len(existing))
                    excedentes = set(novas[vagas:])
                    novas = novas[:vagas]

                    if excedentes:
                        logging.warning(
                            f"[cliente_postgres.py] {schema}.{table_name} atingiu "
                            f"o limite de {limite_colunas} colunas — "
                            f"{len(excedentes)} colunas novas serão gravadas em "
                            f"payload_origem"
                        )
                else:
                    excedentes = set()

                for col in novas:
                    try:
                        cursor.execute(
                            f"ALTER TABLE {schema}.{table_name} "
                            f'ADD COLUMN "{col}" TEXT;'
                        )
                        logging.info(
                            f"[cliente_postgres.py] Schema evolution: "
                            f'adicionada coluna "{col}" (TEXT) em '
                            f"{schema}.{table_name}"
                        )
                    except psycopg2.errors.DuplicateColumn:
                        conn.rollback()
                        logging.debug(
                            f'[cliente_postgres.py] Coluna "{col}" '
                            f"já existe em {schema}.{table_name} — ignorando"
                        )
                    except psycopg2.Error as err:
                        conn.rollback()
                        logging.warning(
                            f"[cliente_postgres.py] Falha ao adicionar coluna "
                            f'"{col}" em {schema}.{table_name}: {err}'
                        )

                conn.commit()
                return excedentes

    def insert_data(
        self,
        data: List[Dict[str, Any]],
        table_name: str,
        conflict_fields: Optional[List[str]] = None,
        primary_key: Optional[List[str]] = None,
        schema: str = "raw",
        limite_colunas: Optional[int] = None,
    ) -> None:
        """Insert data into database table.

        Automatically normalizes all keys to **lowercase** and evolves
        the table schema (``_evolve_schema``) before the INSERT, so new
        or CamelCase keys returned by upstream APIs never cause an
        ``UndefinedColumn`` error.

        Args:
            data: List of dictionaries to insert
            table_name: Target table name
            conflict_fields: List of column names for conflict resolution
            primary_key: List of primary key column names
            schema: Database schema name
            limite_colunas: Teto de colunas da tabela. As colunas que não
                couberem vão para ``payload_origem`` (JSON) em vez de virarem
                coluna — ver ``_evolve_schema``. Sem valor, o comportamento é
                o de sempre: toda chave vira coluna.
        """
        if not data:
            logging.warning(
                f"[cliente_postgres.py] No data to insert into {schema}.{table_name}"
            )
            return

        # ── Normalização: todas as chaves para minúsculo ──
        data_norm = [{str(k).lower(): v for k, v in row.items()} for row in data]

        self.create_table_if_not_exists(
            data_norm[0], table_name, primary_key=primary_key, schema=schema
        )

        flattened_data = self._flatten_data(data_norm)
        columns = list(flattened_data[0].keys())

        excedentes = self._evolve_schema(
            columns, table_name, schema=schema, limite_colunas=limite_colunas
        )
        if excedentes:
            flattened_data = self._mover_para_payload_origem(flattened_data, excedentes)
            columns = list(flattened_data[0].keys())
            # payload_origem só passa a existir quando alguma coluna transborda
            # — sem este segundo passo (agora sem limite, para a própria coluna
            # não ser barrada), o INSERT quebraria com UndefinedColumn.
            self._evolve_schema(["payload_origem"], table_name, schema=schema)

        values = [tuple(item.values()) for item in flattened_data]

        cols_sql = ", ".join(f'"{c}"' for c in columns)
        sql = f"INSERT INTO {schema}.{table_name} ({cols_sql}) VALUES %s"

        if conflict_fields:
            conflict_str = ", ".join(conflict_fields)
            update_str = ", ".join([f'"{col}" = EXCLUDED."{col}"' for col in columns])
            sql += f" ON CONFLICT ({conflict_str}) DO UPDATE SET {update_str}"

        with psycopg2.connect(self.conn_str) as conn:
            with conn.cursor() as cursor:
                try:
                    psycopg2.extras.execute_values(cursor, sql, values)
                    conn.commit()
                    logging.info(
                        f"[cliente_postgres.py] Inserted data into {schema}."
                        f"{table_name}"
                    )
                except psycopg2.Error as err:
                    logging.error(
                        f"[cliente_postgres.py] Failed to insert data into {schema}."
                        f"{table_name}. Error: {str(err)}"
                    )
                    raise RuntimeError(
                        f"Failed to insert data into {schema}.{table_name}"
                    ) from err

    @staticmethod
    def _mover_para_payload_origem(
        linhas: List[Dict[str, Any]], excedentes: set
    ) -> List[Dict[str, Any]]:
        """Tira das linhas as colunas que não couberam na tabela e guarda o
        conteúdo delas em ``payload_origem`` (JSON serializado).

        Nenhum dado da origem é descartado: a coluna deixa de existir como
        coluna, mas o par nome/valor continua consultável via
        ``payload_origem``.
        """
        movidas: List[Dict[str, Any]] = []

        for linha in linhas:
            extras = {
                coluna: valor for coluna, valor in linha.items() if coluna in excedentes
            }
            nova = {
                coluna: valor
                for coluna, valor in linha.items()
                if coluna not in excedentes
            }
            payload_atual = nova.get("payload_origem")
            if payload_atual:
                extras = {**json.loads(payload_atual), **extras}
            nova["payload_origem"] = json.dumps(extras, ensure_ascii=False, default=str)
            movidas.append(nova)

        return movidas

    def insert_data_por_tabela(
        self,
        data: List[Dict[str, Any]],
        table_name: str,
        schema: str = "raw",
        conflict_fields: Optional[List[str]] = None,
        primary_key: Optional[List[str]] = None,
        limite_colunas: Optional[int] = None,
    ) -> None:
        """Insere dados em tabela destino determinada dinamicamente.

        Wrapper de conveniência sobre ``insert_data`` para o fluxo de
        planilhas, onde a tabela destino sai do roteamento por aba/template
        (``extracao_planilhas.resolver_tabela_planilha``).

        Args:
            data: Lista de dicionários a inserir.
            table_name: Nome da tabela destino.
            schema: Schema do banco (default: ``"raw"``).
            conflict_fields: Chave natural para o UPSERT — sem ela, cada
                execução duplicaria as linhas da planilha inteira.
            primary_key: Chave primária a criar junto com a tabela.
            limite_colunas: Ver ``insert_data``.
        """
        self.insert_data(
            data,
            table_name=table_name,
            conflict_fields=conflict_fields,
            primary_key=primary_key,
            schema=schema,
            limite_colunas=limite_colunas,
        )

    def execute_statement(self, statement: str) -> None:
        """Executa um comando que não retorna linhas (DDL, UPDATE, DELETE).

        ``execute_query`` sempre chama ``fetchall`` e por isso quebra em
        comandos sem resultado.
        """
        logging.info(f"[cliente_postgres.py] Executing statement: {statement}")
        with psycopg2.connect(self.conn_str) as conn:
            with conn.cursor() as cursor:
                cursor.execute(statement)
                conn.commit()

    def execute_query(self, query: str) -> List[Tuple[Any, ...]]:
        """Execute a query and return the results.

        Args:
            query: SQL query to execute

        Returns:
            List of tuples containing the query results
        """
        logging.info(f"[cliente_postgres.py] Executing query: {query}")
        with psycopg2.connect(self.conn_str) as conn:
            with conn.cursor() as cursor:
                cursor.execute(query)
                results = cursor.fetchall()
                logging.info(
                    f"[cliente_postgres.py] Query executed successfully, fetched "
                    f"{len(results)} rows"
                )
                return results

    def drop_table_if_exists(self, table_name: str, schema: str = "raw") -> None:
        """Remove a tabela se ela existir."""
        conn = psycopg2.connect(self.conn_str)
        cursor = conn.cursor()
        drop_table_query = f"DROP TABLE IF EXISTS {schema}.{table_name};"
        try:
            cursor.execute(drop_table_query)
            conn.commit()
            print(f"Tabela {schema}.{table_name} removida com sucesso.")
        except Exception as e:
            print(f"Erro ao remover a tabela {schema}.{table_name}: {e}")
        finally:
            cursor.close()
            conn.close()

    def insert_csv_data(
        self, csv_data: str, table_name: str, schema: str = "raw"
    ) -> None:
        """
        Insere dados de um CSV no banco de dados, garantindo que a tabela seja criada.
        Se a tabela existir, ela será removida antes da inserção dos novos dados.

        Args:
            csv_data (str): Dados do CSV como string.
            table_name (str): Nome da tabela de destino.
            schema (str): Nome do schema do banco (padrão: "raw").
        """
        # Converte o CSV para DataFrame
        df = pd.read_csv(io.StringIO(csv_data))

        # Converte o DataFrame para lista de dicionários
        data = df.to_dict(orient="records")

        # Remove a tabela existente
        self.drop_table_if_exists(table_name, schema)

        # Insere os novos dados
        self.insert_data(data, table_name, primary_key=None, schema=schema)
