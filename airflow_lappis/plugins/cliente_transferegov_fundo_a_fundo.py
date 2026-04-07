import logging
import http
from cliente_base import ClienteBase

class ClienteTransfereGov(ClienteBase):
    BASE_URL = "https://api.transferegov.gestao.gov.br/fundoafundo"
    BASE_HEADER = {"accept": "application/json"}

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

    def get_planos_acao_by_programa(self, id_programa: int) -> list | None:
        """
        Obtem todos os planos de acao vinculados a um programa via FK.
        """
        endpoint = f"/plano_acao?id_programa=eq.{id_programa}"
        logging.info(f"[cliente_transferegov.py] Fetching planos for programa: {id_programa}")
        
        status, data = self.request(
            http.HTTPMethod.GET, endpoint, headers=self.BASE_HEADER
        )
        
        if status == http.HTTPStatus.OK and isinstance(data, list):
            logging.info(
                f"[cliente_transferegov.py] Successfully fetched {len(data)} planos for programa: {id_programa}"
            )
            return data
            
        logging.warning(
            f"[cliente_transferegov.py] Failed to fetch planos for programa {id_programa}. Status: {status}"
        )
        return None