import io
import json
import logging
from contextlib import contextmanager
from typing import Any, Dict, Iterator, List, Optional, Sequence, Set, Tuple

import pandas as pd
import psycopg2
import psycopg2.extensions
import psycopg2.extras


def _ident(nome: str) -> str:
    return '"' + nome.replace('"', '""') + '"'


def _tabela(schema: str, table_name: str) -> str:
    return f"{_ident(schema)}.{_ident(table_name)}"


def _achatar(
    objeto: Dict[Any, Any], prefixo: str = "", plano: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """Uma linha aninhada em ``{"pai__filho": valor}``, chaves em minúsculo."""
    plano = {} if plano is None else plano
    for chave, valor in objeto.items():
        nome = f"{prefixo}{str(chave).lower()}"
        if isinstance(valor, dict):
            # dict vazio não vira coluna, como no json_normalize
            _achatar(valor, f"{nome}{ClientPostgresDB.SEPARATOR}", plano)
        else:
            plano[nome] = str(valor) if isinstance(valor, list) else valor
    return plano


class ClientPostgresDB:
    """Client for interacting with PostgreSQL database.

    Cada instância abre **uma** conexão, na primeira operação, e a reaproveita
    até ``close()`` — ou até o fim do bloco ``with``. Cada método público roda
    na sua própria transação e termina com ``commit`` ou ``rollback``: a
    conexão nunca fica parada em ``idle in transaction`` segurando lock, e uma
    falha não impede a próxima chamada. Se o servidor derrubar a conexão, a
    próxima operação abre outra.

    As colunas de cada tabela já vista ficam em cache na instância. Um laço de
    ``insert_data`` na mesma tabela só consulta o catálogo quando aparece
    chave nova, e só executa DDL quando há o que criar.
    """

    SEPARATOR = "__"
    # Linhas por INSERT no execute_values. O padrão do psycopg2 (100) faz dez
    # idas e voltas ao banco a cada mil linhas.
    TAMANHO_PAGINA = 1000

    @staticmethod
    def _flatten_data(data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Flatten nested JSON data and normalize keys to lowercase.

        Normalizing to lowercase avoids PostgreSQL case-folding conflicts:
        unquoted identifiers are folded to lowercase by Postgres, so sending
        CamelCase keys (e.g. ``tipoAnexo``) would create columns named
        ``tipoanexo`` while the INSERT would reference ``tipoAnexo`` — causing
        ``UndefinedColumn``.  By lowercasing on the Python side we guarantee
        consistency between column creation and column reference.

        Objeto aninhado vira ``pai__filho``; lista vira texto. Toda linha sai
        com as mesmas colunas, na mesma ordem, e a chave que falta numa linha
        vale ``None``. Substitui o ``json_normalize``, que preenchia a lacuna
        com NaN — gravado como o texto ``'NaN'`` — e promovia a float a coluna
        de inteiros com lacuna (``3550308.0``).

        Args:
            data (List[Dict[str, Any]]): List of nested dictionaries.

        Returns:
            List[Dict[str, Any]]: List of flat dictionaries with lowercase keys.
        """
        planas = [_achatar(linha) for linha in data]
        colunas = dict.fromkeys(coluna for linha in planas for coluna in linha)
        return [{coluna: linha.get(coluna) for coluna in colunas} for linha in planas]

    def __init__(self, conn_str: str) -> None:
        self.conn_str = conn_str
        self._conn: Optional[psycopg2.extensions.connection] = None
        self._schemas: Set[str] = set()
        self._colunas: Dict[Tuple[str, str], Set[str]] = {}
        try:
            parametros = psycopg2.extensions.parse_dsn(conn_str)
        except psycopg2.ProgrammingError:
            parametros = {}
        logging.info(
            f"[cliente_postgres.py] Initialized ClientPostgresDB for "
            f"host={parametros.get('host')} dbname={parametros.get('dbname')}"
        )

    def __enter__(self) -> "ClientPostgresDB":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    def close(self) -> None:
        """Fecha a conexão, se houver uma aberta."""
        if self._conn is not None and not self._conn.closed:
            self._conn.close()
        self._conn = None

    def _conexao(self) -> psycopg2.extensions.connection:
        if self._conn is None or self._conn.closed:
            self._conn = psycopg2.connect(self.conn_str)
        return self._conn

    @contextmanager
    def _transacao(self) -> Iterator[psycopg2.extensions.cursor]:
        """Cursor numa transação que termina em ``commit`` ou ``rollback``.

        No rollback o cache de schemas e colunas é descartado: ele pode ter
        registrado DDL que acabou de ser desfeito.
        """
        conn = self._conexao()
        try:
            with conn.cursor() as cursor:
                yield cursor
            conn.commit()
        except BaseException:
            self._schemas.clear()
            self._colunas.clear()
            try:
                if not conn.closed:
                    conn.rollback()
            except psycopg2.Error:
                # Conexão quebrada no meio: fecha para a próxima operação
                # abrir outra, e deixa subir o erro original.
                self.close()
            raise

    def _consultar(self, query: str) -> List[Tuple[Any, ...]]:
        with self._transacao() as cursor:
            cursor.execute(query)
            return cursor.fetchall()

    # ------------------------------------------------------------------
    # DDL — cada passo recebe o cursor da transação de quem chama
    # ------------------------------------------------------------------

    def _garantir_schema(self, cursor: psycopg2.extensions.cursor, schema: str) -> None:
        if schema in self._schemas:
            return
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {_ident(schema)}")
        self._schemas.add(schema)
        logging.info(f"[cliente_postgres.py] Schema {schema} ensured to exist")

    def _garantir_tabela(
        self,
        cursor: psycopg2.extensions.cursor,
        columns: Sequence[str],
        table_name: str,
        primary_key: Optional[List[str]],
        schema: str,
    ) -> None:
        if (schema, table_name) in self._colunas:
            return
        self._garantir_schema(cursor, schema)

        column_definitions = [f"{_ident(c)} TEXT" for c in columns]
        if primary_key:
            # As colunas chegam em minúsculo (_flatten_data); a chave também,
            # para casar com elas agora que vai entre aspas.
            pk_str = ", ".join(_ident(c.lower()) for c in primary_key)
            column_definitions.append(f"PRIMARY KEY ({pk_str})")

        try:
            cursor.execute(
                f"CREATE TABLE IF NOT EXISTS {_tabela(schema, table_name)} "
                f"({', '.join(column_definitions)})"
            )
        except psycopg2.Error as err:
            logging.error(
                f"[cliente_postgres.py] Failed to create table {schema}."
                f"{table_name}. Error: {str(err)}"
            )
            raise RuntimeError(f"Failed to create table {schema}.{table_name}") from err
        logging.info(
            f"[cliente_postgres.py] Table {schema}.{table_name} created "
            f"or already exists"
        )

    def _ler_colunas(
        self, cursor: psycopg2.extensions.cursor, table_name: str, schema: str
    ) -> Set[str]:
        cursor.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            """,
            (schema, table_name),
        )
        existentes = {row[0] for row in cursor.fetchall()}
        self._colunas[(schema, table_name)] = existentes
        return existentes

    def _evoluir(
        self,
        cursor: psycopg2.extensions.cursor,
        columns: Sequence[str],
        table_name: str,
        schema: str,
        limite_colunas: Optional[int] = None,
    ) -> Set[str]:
        existentes = self._colunas.get((schema, table_name))
        if existentes is not None and all(c in existentes for c in columns):
            return set()
        # Chave nova (ou tabela ainda fora do cache): relê o catálogo, porque
        # outra task pode ter evoluído a tabela desde a última leitura.
        existentes = self._ler_colunas(cursor, table_name, schema)

        novas = [c for c in dict.fromkeys(columns) if c not in existentes]
        if not novas:
            return set()

        excedentes: Set[str] = set()
        if limite_colunas is not None:
            vagas = max(0, limite_colunas - len(existentes))
            excedentes = set(novas[vagas:])
            novas = novas[:vagas]

            if excedentes:
                logging.warning(
                    f"[cliente_postgres.py] {schema}.{table_name} atingiu "
                    f"o limite de {limite_colunas} colunas — "
                    f"{len(excedentes)} colunas novas serão gravadas em "
                    f"payload_origem"
                )

        if novas:
            # Um ALTER só, com IF NOT EXISTS: duas tasks evoluindo a mesma
            # tabela ao mesmo tempo não se atropelam.
            cursor.execute(
                f"ALTER TABLE {_tabela(schema, table_name)} "
                + ", ".join(f"ADD COLUMN IF NOT EXISTS {_ident(c)} TEXT" for c in novas)
            )
            existentes.update(novas)
            logging.info(
                f"[cliente_postgres.py] Schema evolution: adicionadas "
                f"{len(novas)} colunas (TEXT) em {schema}.{table_name}: "
                f"{', '.join(novas)}"
            )

        return excedentes

    def _preparar_tabela(
        self,
        cursor: psycopg2.extensions.cursor,
        linhas: List[Dict[str, Any]],
        table_name: str,
        primary_key: Optional[List[str]],
        schema: str,
        limite_colunas: Optional[int],
    ) -> List[Dict[str, Any]]:
        """Garante schema, tabela e colunas; devolve as linhas prontas para o
        INSERT (com ``payload_origem`` quando alguma coluna transbordou)."""
        columns = list(linhas[0].keys())
        self._garantir_tabela(cursor, columns, table_name, primary_key, schema)

        excedentes = self._evoluir(
            cursor, columns, table_name, schema, limite_colunas=limite_colunas
        )
        if excedentes:
            linhas = self._mover_para_payload_origem(linhas, excedentes)
            # payload_origem só passa a existir quando alguma coluna transborda
            # — sem este segundo passo (agora sem limite, para a própria coluna
            # não ser barrada), o INSERT quebraria com UndefinedColumn.
            self._evoluir(cursor, ["payload_origem"], table_name, schema)
        return linhas

    def _inserir(
        self,
        cursor: psycopg2.extensions.cursor,
        linhas: List[Dict[str, Any]],
        table_name: str,
        conflict_fields: Optional[List[str]],
        schema: str,
    ) -> None:
        columns = list(linhas[0].keys())
        values = [tuple(item.values()) for item in linhas]

        cols_sql = ", ".join(_ident(c) for c in columns)
        sql = f"INSERT INTO {_tabela(schema, table_name)} ({cols_sql}) VALUES %s"

        if conflict_fields:
            chave = [c.lower() for c in conflict_fields]
            values = self._ultima_por_chave(columns, values, chave)
            conflict_str = ", ".join(_ident(c) for c in chave)
            update_str = ", ".join(
                f"{_ident(col)} = EXCLUDED.{_ident(col)}" for col in columns
            )
            sql += f" ON CONFLICT ({conflict_str}) DO UPDATE SET {update_str}"

        try:
            psycopg2.extras.execute_values(
                cursor, sql, values, page_size=self.TAMANHO_PAGINA
            )
        except psycopg2.Error as err:
            logging.error(
                f"[cliente_postgres.py] Failed to insert data into {schema}."
                f"{table_name}. Error: {str(err)}"
            )
            raise RuntimeError(
                f"Failed to insert data into {schema}.{table_name}"
            ) from err
        logging.info(
            f"[cliente_postgres.py] Inserted {len(values)} rows into {schema}."
            f"{table_name}"
        )

    @staticmethod
    def _ultima_por_chave(
        columns: List[str], values: List[Tuple[Any, ...]], chave: List[str]
    ) -> List[Tuple[Any, ...]]:
        """Deixa uma linha por chave de conflito — a última, como se o upsert
        fosse linha a linha. O Postgres recusa o lote inteiro quando a mesma
        chave aparece duas vezes num ``ON CONFLICT DO UPDATE``."""
        posicoes = [columns.index(c) for c in chave]
        unicas = {tuple(v[i] for i in posicoes): v for v in values}
        return list(unicas.values())

    def _drop(
        self, cursor: psycopg2.extensions.cursor, table_name: str, schema: str
    ) -> None:
        cursor.execute(f"DROP TABLE IF EXISTS {_tabela(schema, table_name)}")
        self._colunas.pop((schema, table_name), None)

    # ------------------------------------------------------------------
    # Escrita
    # ------------------------------------------------------------------

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
        columns = list(self._flatten_data([sample_data])[0].keys())
        with self._transacao() as cursor:
            self._garantir_tabela(cursor, columns, table_name, primary_key, schema)

    def _evolve_schema(
        self,
        columns: List[str],
        table_name: str,
        schema: str = "raw",
        limite_colunas: Optional[int] = None,
    ) -> set:
        """Adiciona colunas novas à tabela que ainda não existem no banco.

        Consulta ``information_schema.columns`` para descobrir as colunas
        existentes e cria, num ``ALTER TABLE … ADD COLUMN IF NOT EXISTS`` só,
        as chaves de *columns* que faltam, todas como ``TEXT``.

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
        with self._transacao() as cursor:
            return self._evoluir(
                cursor, columns, table_name, schema, limite_colunas=limite_colunas
            )

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

        O DDL vai numa transação curta e o INSERT em outra: o lock exclusivo
        de um ``ALTER TABLE`` não fica preso enquanto o lote é gravado, e
        tasks paralelas na mesma tabela não esperam umas pelas outras.

        Args:
            data: List of dictionaries to insert
            table_name: Target table name
            conflict_fields: List of column names for conflict resolution.
                Linhas repetidas na mesma chave ficam só com a última.
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

        linhas = self._flatten_data(data)
        with self._transacao() as cursor:
            linhas = self._preparar_tabela(
                cursor, linhas, table_name, primary_key, schema, limite_colunas
            )
        with self._transacao() as cursor:
            self._inserir(cursor, linhas, table_name, conflict_fields, schema)

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

    def drop_table_if_exists(self, table_name: str, schema: str = "raw") -> None:
        """Remove a tabela se ela existir. Falha sobe — quem dropa para
        recarregar não pode seguir gravando em cima da tabela antiga."""
        try:
            with self._transacao() as cursor:
                self._drop(cursor, table_name, schema)
        except psycopg2.Error as err:
            logging.error(
                f"[cliente_postgres.py] Erro ao remover a tabela "
                f"{schema}.{table_name}: {err}"
            )
            raise
        logging.info(f"[cliente_postgres.py] Tabela {schema}.{table_name} removida")

    def insert_csv_data(
        self, csv_data: str, table_name: str, schema: str = "raw"
    ) -> None:
        """
        Insere dados de um CSV no banco de dados, garantindo que a tabela seja criada.
        Se a tabela existir, ela será removida antes da inserção dos novos dados.

        Remoção e carga vão na mesma transação: se a carga falhar, a tabela
        antiga continua lá, e quem lê nunca encontra a tabela faltando.

        Args:
            csv_data (str): Dados do CSV como string.
            table_name (str): Nome da tabela de destino.
            schema (str): Nome do schema do banco (padrão: "raw").
        """
        data = pd.read_csv(io.StringIO(csv_data)).to_dict(orient="records")
        linhas = self._flatten_data(data) if data else []

        with self._transacao() as cursor:
            self._drop(cursor, table_name, schema)
            if not linhas:
                logging.warning(
                    f"[cliente_postgres.py] CSV vazio: {schema}.{table_name} "
                    f"removida e não recriada"
                )
                return
            linhas = self._preparar_tabela(cursor, linhas, table_name, None, schema, None)
            self._inserir(cursor, linhas, table_name, None, schema)

    # ------------------------------------------------------------------
    # SQL livre
    # ------------------------------------------------------------------

    def execute_statement(self, statement: str) -> None:
        """Executa um comando que não retorna linhas (DDL, UPDATE, DELETE).

        ``execute_query`` sempre chama ``fetchall`` e por isso quebra em
        comandos sem resultado.
        """
        logging.info(f"[cliente_postgres.py] Executing statement: {statement}")
        with self._transacao() as cursor:
            cursor.execute(statement)

    def execute_query(self, query: str) -> List[Tuple[Any, ...]]:
        """Execute a query and return the results.

        Args:
            query: SQL query to execute

        Returns:
            List of tuples containing the query results
        """
        logging.info(f"[cliente_postgres.py] Executing query: {query}")
        results = self._consultar(query)
        logging.info(
            f"[cliente_postgres.py] Query executed successfully, fetched "
            f"{len(results)} rows"
        )
        return results
