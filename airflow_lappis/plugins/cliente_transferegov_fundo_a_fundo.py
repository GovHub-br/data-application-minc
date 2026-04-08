import logging
import http
from cliente_base import ClienteBase

class ClienteTransfereGov(ClienteBase):
    BASE_URL = "https://api.transferegov.gestao.gov.br/fundoafundo"
    BASE_HEADER = {"accept": "application/json"}
    DEFAULT_PAGE_LIMIT = 1000

    def __init__(self) -> None:
        super().__init__(base_url=ClienteTransfereGov.BASE_URL)
        logging.info(
            "[cliente_transferegov.py] Initialized ClienteTransfereGov with base_url: "
            f"{ClienteTransfereGov.BASE_URL}"
        )

    def get_programa_by_id(self, id_programa: int) -> dict | None:
        """
        Obtem metadados do programa filtrando via PostgREST (eq.)
        """
        endpoint = f"/programa?id_programa=eq.{id_programa}"
        logging.info(f"[cliente_transferegov.py] Fetching programa ID: {id_programa}")
        
        status, data = self.request(
            http.HTTPMethod.GET, endpoint, headers=self.BASE_HEADER
        )
        
        if status == http.HTTPStatus.OK and data:
            logging.info(f"[cliente_transferegov.py] Successfully fetched programa: {id_programa}")
            # APIs PostgREST sempre retornam listas. Pegamos o primeiro item.
            return data[0] if isinstance(data, list) else data
            
        logging.warning(
            f"[cliente_transferegov.py] Failed to fetch programa {id_programa}. Status: {status}"
        )
        return None

    def get_planos_acao_by_programa(
        self, id_programa: int, limit: int = DEFAULT_PAGE_LIMIT
    ) -> list | None:
        """
        Obtem todos os planos de acao vinculados a um programa via FK,
        paginando com os parametros limit/offset ate o fim dos registros.
        """
        if limit <= 0:
            raise ValueError("limit must be greater than 0")

        all_planos = []
        offset = 0

        logging.info(
            f"[cliente_transferegov.py] Fetching planos for programa: {id_programa} "
            f"with pagination limit={limit}"
        )

        while True:
            endpoint = (
                f"/plano_acao?id_programa=eq.{id_programa}"
                f"&limit={limit}&offset={offset}"
            )

            status, data = self.request(
                http.HTTPMethod.GET, endpoint, headers=self.BASE_HEADER
            )

            if status != http.HTTPStatus.OK or not isinstance(data, list):
                logging.warning(
                    f"[cliente_transferegov.py] Failed to fetch planos for programa "
                    f"{id_programa} at offset {offset}. Status: {status}"
                )
                return None

            if not data:
                break

            all_planos.extend(data)
            logging.info(
                f"[cliente_transferegov.py] Retrieved {len(data)} registros "
                f"(offset={offset}). Total acumulado: {len(all_planos)}"
            )

            if len(data) < limit:
                break

            offset += limit

        logging.info(
            f"[cliente_transferegov.py] Successfully fetched {len(all_planos)} "
            f"planos for programa: {id_programa}"
        )
        return all_planos