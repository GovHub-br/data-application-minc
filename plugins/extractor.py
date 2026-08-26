import logging
from typing import Iterator

from relational_database_extractor import RelationalDatabaseExtractor

"""
Classe Extractor: Gerencia a extração de dados de bancos relacionais.
Permite registrar diferentes tipos de extratores de bancos de dados e criar instâncias
com base no tipo de banco de dados. Fornece um método para construir a extração
de dados de uma tabela específica em um banco de dados, retornando um iterador de chunks de dados.

"""

class Extractor:

    _REGISTRY: dict[str, type[RelationalDatabaseExtractor]] = {}

    def __init__(self, rde: RelationalDatabaseExtractor | None = None) -> None:
        self.rde: list[RelationalDatabaseExtractor] = []
        if rde is not None:
            self.rde.append(rde)

    @classmethod
    def register(cls, db_type: str, extractor_class: type[RelationalDatabaseExtractor]) -> None:
        cls._REGISTRY[db_type] = extractor_class
        logging.debug("[Extractor] Registrado '%s' → %s", db_type, extractor_class.__name__)

    @classmethod
    def from_type(cls, db_type: str, conn_id: str) -> "Extractor":
        
        if db_type not in cls._REGISTRY:
            raise ValueError(
                f"[Extractor] Tipo de banco '{db_type}' não registrado. "
                f"Disponíveis: {sorted(cls._REGISTRY)}"
            )
        extractor_class = cls._REGISTRY[db_type]
        logging.info(
            "[Extractor] from_type: instanciando %s (conn_id=%s)",
            extractor_class.__name__,
            conn_id,
        )
        return cls(extractor_class(conn_id))

    def buildExtraction(
        self,
        database: str,
        table: str,
        chunk_size: int = 50_000,
        **kwargs: object,
    ) -> Iterator[list[dict]]:

        if not self.rde:
            raise RuntimeError(
                "[Extractor] Nenhum RelationalDatabaseExtractor foi registrado. "
                "Passe um RDE no __init__ ou use Extractor.from_type()."
            )
        rde = self.rde[0]
        logging.info(
            "[Extractor] buildExtraction: %s.%s via %s (chunk_size=%d)",
            database,
            table,
            type(rde).__name__,
            chunk_size,
        )
        return rde.buildExtraction(database, table, chunk_size, **kwargs)
