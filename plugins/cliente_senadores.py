"""Cliente da API de Dados Abertos do Senado Federal.

Além dos senadores em exercício, traz a legislação federal que o Resultado 5
do Eixo 3 do Cultura em Números levanta (``senado_legislacao_dag``):

- ``/legislacao/lista?tipo=<sigla>&ano=<ano>``: as normas de um tipo assinadas
  num ano, já com a ementa;
- ``/legislacao/{codigo}``: o detalhe de uma norma, com publicações, alterações
  (``vides``), dispositivos, indexação e classe temática.

Medido em 10/11/2026: ``tipo=LEI`` traz só as leis numeradas (``LEI-n``), mas
``tipo=DEC`` traz os decretos numerados (``DEC-n``) **e** os sem número
(``DEC-sn``). O detalhe médio tem 4 KiB e sai em meio segundo; o das normas
muito alteradas passa de 700 KiB e leva até 20 s, daí os timeouts do Eixo 3
(60 s na listagem, 180 s no detalhe).

A API converte XML em JSON: um conjunto sem itens vem como ``""``, com um item
vem como o objeto e com vários vem como lista. ``como_lista`` normaliza.
Documentação: https://legis.senado.leg.br/dadosabertos/api-docs/swagger-ui/index.html
"""

import http
import logging
from typing import Any

from cliente_base import ClienteBase


class ClienteSenadores(ClienteBase):
    """
    Cliente para consumir a API de Dados Abertos do Senado Federal.
    """

    BASE_URL = "https://legis.senado.leg.br/dadosabertos"
    BASE_HEADER = {"accept": "application/json"}
    TIMEOUT_LISTA = 60
    TIMEOUT_DETALHE = 180

    def __init__(self) -> None:
        super().__init__(base_url=ClienteSenadores.BASE_URL)
        logging.info(
            f"[cliente_senadores.py] Initialized ClienteSenadores em: {ClienteSenadores.BASE_URL}"
        )

    def get_senadores_atuais(self) -> list:
        """
        Obtém a lista de senadores em exercício.
        O Senado geralmente retorna tudo em uma única chamada, sem paginação complexa como a Câmara.
        """
        endpoint = "/senador/lista/atual"
        logging.info("[cliente_senadores.py] Fetching senadores atuais")

        status, data = self.request(
            http.HTTPMethod.GET, endpoint, headers=self.BASE_HEADER
        )

        if status == http.HTTPStatus.OK and isinstance(data, dict):
            # A estrutura do JSON do Senado é: ListaParlamentarEmExercicio -> Parlamentares -> Parlamentar
            try:
                lista_root = data.get("ListaParlamentarEmExercicio", {})
                parlamentares = lista_root.get("Parlamentares", {}).get("Parlamentar", [])

                if isinstance(parlamentares, dict):
                    parlamentares = [parlamentares]

                logging.info(
                    f"[cliente_senadores.py] Successfully fetched {len(parlamentares)} senadores"
                )
                return parlamentares
            except Exception as e:
                logging.error(
                    f"[cliente_senadores.py] Erro ao parsear JSON do Senado: {e}"
                )
                return []
        else:
            logging.warning(f"[cliente_senadores.py] Failed with status: {status}")
            return []

    # ── legislação ───────────────────────────────────────────────────────

    @staticmethod
    def como_lista(valor: Any) -> list[Any]:
        """``""`` ou ``None`` viram lista vazia, um objeto vira lista de um."""
        if valor is None or valor == "":
            return []
        if isinstance(valor, list):
            return list(valor)
        return [valor]

    def get_legislacao_lista(
        self, tipo: str, ano: int
    ) -> tuple[http.HTTPStatus, dict | list | None]:
        """A resposta inteira de ``/legislacao/lista`` para um tipo e um ano.

        Devolve a resposta como veio, para a DAG gravar no raw; os documentos
        saem dela por ``documentos_da_lista``.
        """
        status, data = self.request(
            http.HTTPMethod.GET,
            "/legislacao/lista",
            params={"tipo": tipo, "ano": ano},
            headers=self.BASE_HEADER,
            timeout=self.TIMEOUT_LISTA,
        )
        return http.HTTPStatus(status), data

    def get_legislacao_detalhe(
        self, codigo: str
    ) -> tuple[http.HTTPStatus, dict | list | None]:
        """A resposta inteira de ``/legislacao/{codigo}``, como veio."""
        status, data = self.request(
            http.HTTPMethod.GET,
            f"/legislacao/{codigo}",
            headers=self.BASE_HEADER,
            timeout=self.TIMEOUT_DETALHE,
        )
        return http.HTTPStatus(status), data

    @classmethod
    def documentos_da_lista(cls, resposta: Any) -> list[dict[str, Any]]:
        """Os documentos de uma resposta de ``/legislacao/lista``."""
        if not isinstance(resposta, dict):
            return []
        raiz = resposta.get("ListaDocumento") or {}
        documentos = raiz.get("documentos") if isinstance(raiz, dict) else None
        if isinstance(documentos, dict):
            documentos = documentos.get("documento")
        return [d for d in cls.como_lista(documentos) if isinstance(d, dict)]

    @classmethod
    def documentos_do_detalhe(cls, resposta: Any) -> list[dict[str, Any]]:
        """O documento (em lista, normalmente de um) de ``/legislacao/{codigo}``."""
        if not isinstance(resposta, dict):
            return []
        raiz = resposta.get("DetalheDocumento") or {}
        documentos = raiz.get("documentos") if isinstance(raiz, dict) else None
        if isinstance(documentos, dict):
            documentos = documentos.get("documento")
        return [d for d in cls.como_lista(documentos) if isinstance(d, dict)]
