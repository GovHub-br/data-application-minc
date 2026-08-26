import http
import logging
from typing import Any, Iterable, Iterator

from cliente_base import ClienteBase


class ClienteTransfereGov(ClienteBase):
    """Cliente da API publica de dados abertos do Transferegov Fundo a Fundo.

    A API antiga (``api.transferegov.gestao.gov.br``, PostgREST) esta
    defasada e nao recebe mais atualizacao. Esta classe fala com a nova
    (``api-publica.``, FastAPI), cujo contrato difere em quatro pontos:

    1. os endpoints sao no plural, com traco: ``/plano_acao`` virou
       ``/planos-acao``;
    2. os filtros vao direto na URL, sem os conectores do PostgREST:
       ``?id_programa=46`` no lugar de ``?id_programa=eq.46``;
    3. a paginacao usa ``pagina`` (base 1) e ``tamanho_da_pagina``
       (maximo 1000) no lugar de ``limit``/``offset``;
    4. a resposta vem envelopada em ``{"data": [...], "total_pages", ...}``
       e nao mais como lista direta.
    """

    BASE_URL = "https://api-publica.transferegov.gestao.gov.br/fundoafundo"
    BASE_HEADER = {"accept": "application/json"}

    # Teto imposto pela propria API; pedir mais devolve 400.
    DEFAULT_PAGE_LIMIT = 1000

    # A API aceita varios valores num filtro so, no formato `campo=(a,b,c)`,
    # ate 200 por requisicao (limite que ela informa num 400 ao estourar).
    # E o que permite buscar metas e dados bancarios de milhares de planos
    # em dezenas de chamadas em vez de uma por plano.
    LOTE_MAX_VALORES = 200

    def __init__(self) -> None:
        super().__init__(base_url=ClienteTransfereGov.BASE_URL)
        logging.info(
            "[cliente_transferegov.py] Initialized ClienteTransfereGov with base_url: "
            f"{ClienteTransfereGov.BASE_URL}"
        )

    # ── infraestrutura ────────────────────────────────────────────────

    def _get_paginado(
        self,
        endpoint_base: str,
        contexto: str,
        limit: int = DEFAULT_PAGE_LIMIT,
    ) -> list | None:
        """Percorre um endpoint da API nova ate a ultima pagina.

        O criterio de parada usa ``total_pages`` do envelope, com a pagina
        vazia como salvaguarda: sem isso, qualquer consulta acima de 1000
        registros trunca em silencio.

        Args:
            endpoint_base: caminho ja com os filtros aplicados.
            contexto: texto usado nas mensagens de log.
            limit: registros por pagina (teto de ``DEFAULT_PAGE_LIMIT``).

        Returns:
            Lista com todos os registros, ou ``None`` se alguma pagina falhar.
            Lista vazia significa consulta bem-sucedida sem resultados — é
            diferente de ``None``, e as DAGs dependem dessa distincao.
        """
        if limit <= 0:
            raise ValueError("limit must be greater than 0")
        if limit > self.DEFAULT_PAGE_LIMIT:
            raise ValueError(f"limit must be <= {self.DEFAULT_PAGE_LIMIT}")

        registros: list = []
        pagina = 1

        while True:
            separador = "&" if "?" in endpoint_base else "?"
            endpoint = (
                f"{endpoint_base}{separador}pagina={pagina}" f"&tamanho_da_pagina={limit}"
            )

            status, corpo = self.request(
                http.HTTPMethod.GET, endpoint, headers=self.BASE_HEADER
            )

            if status != http.HTTPStatus.OK or not isinstance(corpo, dict):
                logging.warning(
                    f"[cliente_transferegov.py] Failed to fetch {contexto} at "
                    f"pagina {pagina}. Status: {status}"
                )
                return None

            dados = corpo.get("data") or []
            registros.extend(dados)

            total_paginas = corpo.get("total_pages") or pagina
            if not dados or pagina >= total_paginas:
                break

            pagina += 1

        logging.info(
            f"[cliente_transferegov.py] Fetched {len(registros)} registros "
            f"({contexto})"
        )
        return registros

    @classmethod
    def _lotes(cls, valores: Iterable[Any]) -> Iterator[list]:
        """Fatia uma colecao de ids em blocos aceitos pelo filtro em lote."""
        bloco: list = []
        for valor in valores:
            bloco.append(valor)
            if len(bloco) == cls.LOTE_MAX_VALORES:
                yield bloco
                bloco = []
        if bloco:
            yield bloco

    @staticmethod
    def _filtro_lote(campo: str, valores: Iterable[Any]) -> str:
        """Monta o filtro multivalor da API: ``campo=(v1,v2,v3)``."""
        return f"{campo}=({','.join(str(v) for v in valores)})"

    # ── programas ─────────────────────────────────────────────────────

    def get_programa_by_id(self, id_programa: int) -> dict | None:
        """Obtem os metadados de um programa.

        Filtra por ``id_programa`` e nao por ``codigo_programa``: filtrar por
        codigo devolve 500 nesta API (bug do lado do servidor), e o id ja e a
        chave primaria da tabela de destino.
        """
        registros = self._get_paginado(
            f"/programas?id_programa={id_programa}",
            f"programa {id_programa}",
        )

        if registros:
            programa: dict = registros[0]
            return programa

        logging.warning(
            f"[cliente_transferegov.py] Failed to fetch programa {id_programa}"
        )
        return None

    # ── planos de acao ────────────────────────────────────────────────

    def get_planos_acao_by_programa(
        self, id_programa: int, limit: int = DEFAULT_PAGE_LIMIT
    ) -> list | None:
        """Obtem todos os planos de acao vinculados a um programa."""
        return self._get_paginado(
            f"/planos-acao?id_programa={id_programa}",
            f"planos_acao for programa {id_programa}",
            limit=limit,
        )

    # ── filhos do plano de acao ───────────────────────────────────────

    def get_metas_by_plano_acao(
        self, id_plano_acao: int, limit: int = DEFAULT_PAGE_LIMIT
    ) -> list | None:
        """Obtem todas as metas vinculadas a um plano de acao."""
        return self._get_paginado(
            f"/planos-acao-metas?id_plano_acao={id_plano_acao}",
            f"metas for plano_acao {id_plano_acao}",
            limit=limit,
        )

    def get_metas_by_planos_acao(
        self, ids_plano_acao: Iterable[int], limit: int = DEFAULT_PAGE_LIMIT
    ) -> list | None:
        """Versao em lote de ``get_metas_by_plano_acao``.

        Uma requisicao a cada ``LOTE_MAX_VALORES`` planos em vez de uma por
        plano. Devolve os registros de todos os planos misturados — quem
        chama separa por ``id_plano_acao``, que vem no proprio payload.
        """
        return self._get_lote(
            "/planos-acao-metas", "id_plano_acao", ids_plano_acao, "metas", limit
        )

    def get_dados_bancarios_by_plano_acao(
        self, id_plano_acao: int, limit: int = DEFAULT_PAGE_LIMIT
    ) -> list | None:
        """Obtem **todas** as contas bancarias vinculadas a um plano de acao.

        Devolve todos os registros com todas as colunas de proposito: a
        granularidade da tabela e uma linha por conta, e escolher uma so
        conta e regra de consumo (qual conta consultar no BB Agil), nao de
        ingestao.
        """
        return self._get_paginado(
            f"/planos-acao-dados-bancarios?id_plano_acao={id_plano_acao}",
            f"dados bancarios for plano_acao {id_plano_acao}",
            limit=limit,
        )

    def get_dados_bancarios_by_planos_acao(
        self, ids_plano_acao: Iterable[int], limit: int = DEFAULT_PAGE_LIMIT
    ) -> list | None:
        """Versao em lote de ``get_dados_bancarios_by_plano_acao``."""
        return self._get_lote(
            "/planos-acao-dados-bancarios",
            "id_plano_acao",
            ids_plano_acao,
            "dados bancarios",
            limit,
        )

    def _get_lote(
        self,
        endpoint: str,
        campo: str,
        valores: Iterable[Any],
        rotulo: str,
        limit: int = DEFAULT_PAGE_LIMIT,
    ) -> list | None:
        """Consulta um endpoint com filtro multivalor, em blocos.

        Falha de um bloco derruba a chamada inteira (``None``): devolver o
        resultado parcial faria a carga gravar menos registros do que existe
        sem que ninguem percebesse.
        """
        ids = list(valores)
        if not ids:
            return []

        registros: list = []
        for indice, bloco in enumerate(self._lotes(ids), start=1):
            filtro = self._filtro_lote(campo, bloco)
            parcial = self._get_paginado(
                f"{endpoint}?{filtro}",
                f"{rotulo} (lote {indice}, {len(bloco)} ids)",
                limit=limit,
            )
            if parcial is None:
                logging.warning(
                    f"[cliente_transferegov.py] Lote {indice} de {rotulo} falhou; "
                    f"abortando para nao gravar carga incompleta"
                )
                return None
            registros.extend(parcial)

        requisicoes = (len(ids) - 1) // self.LOTE_MAX_VALORES + 1
        logging.info(
            f"[cliente_transferegov.py] Fetched {len(registros)} {rotulo} para "
            f"{len(ids)} planos de acao em {requisicoes} requisicao(oes)"
        )
        return registros

    # ── relatorios de gestao ──────────────────────────────────────────

    def get_relatorios_by_plano_acao(
        self, id_plano_acao: int, limit: int = DEFAULT_PAGE_LIMIT
    ) -> list | None:
        """Obtem todos os relatorios de gestao vinculados a um plano de acao."""
        return self._get_paginado(
            f"/relatorios-gestao?id_plano_acao={id_plano_acao}",
            f"relatorios for plano_acao {id_plano_acao}",
            limit=limit,
        )

    # ── gestao financeira ─────────────────────────────────────────────

    def get_lancamentos_financeiros(self, limit: int = DEFAULT_PAGE_LIMIT) -> list | None:
        """Obtem todos os lancamentos de gestao financeira.

        NOTA: este endpoint nao possui campo id_plano_acao no payload — por
        isso a busca e feita em bloco, sem filtro por FK, ao contrario de
        get_planos_acao_by_programa/get_relatorios_by_plano_acao. O
        cruzamento com plano de acao deve ser feito posteriormente via
        cnpj_ente_solicitante_gestao_financeira ou pelo endpoint-ponte
        /planos-acao-dados-bancarios.
        """
        return self._get_paginado(
            "/gestao-financeira-lancamentos",
            "lancamentos financeiros",
            limit=limit,
        )

    def get_subtransacoes_by_lancamento(
        self, id_lancamento_gestao_financeira: int, limit: int = DEFAULT_PAGE_LIMIT
    ) -> list | None:
        """Obtem todas as subtransacoes vinculadas a um lancamento."""
        return self._get_paginado(
            "/gestao-financeira-subtransacoes"
            f"?id_lancamento_gestao_financeira={id_lancamento_gestao_financeira}",
            f"subtransacoes for lancamento {id_lancamento_gestao_financeira}",
            limit=limit,
        )

    # ── controle de carga ─────────────────────────────────────────────

    def get_data_ultima_atualizacao(self) -> str | None:
        """Devolve quando a origem atualizou os dados pela ultima vez.

        Endpoint novo, sem equivalente na API antiga. Permite pular a carga
        quando a origem nao mudou desde a execucao anterior.
        """
        status, corpo = self.request(
            http.HTTPMethod.GET, "/data-atualizacao", headers=self.BASE_HEADER
        )

        if status == http.HTTPStatus.OK and isinstance(corpo, dict):
            return corpo.get("data_ultima_atualizacao")

        logging.warning(
            "[cliente_transferegov.py] Failed to fetch data-atualizacao. "
            f"Status: {status}"
        )
        return None


class ClienteTransfereGovBackend(ClienteBase):
    BASE_URL = "https://fundos.transferegov.sistema.gov.br/maisbrasil-transferencia-backend/api/public"
    BASE_HEADER = {"accept": "application/json"}

    def __init__(self) -> None:
        super().__init__(base_url=ClienteTransfereGovBackend.BASE_URL)
        logging.info(
            "[cliente_transferegov_backend.py] Initialized ClienteTransfereGovBackend with base_url: "
            f"{ClienteTransfereGovBackend.BASE_URL}"
        )

    def get_anexos_relatorio(self, id_relatorio_gestao: int) -> list | None:
        """
        Obtem todos os anexos vinculados a um relatorio de gestao.
        """
        endpoint = f"/anexos/relatorio-gestao/{id_relatorio_gestao}"
        logging.info(
            f"[cliente_transferegov_backend.py] Fetching anexos for relatorio: {id_relatorio_gestao}"
        )

        status, data = self.request(
            http.HTTPMethod.GET, endpoint, headers=self.BASE_HEADER
        )

        if status == http.HTTPStatus.OK and data:
            logging.info(
                f"[cliente_transferegov_backend.py] Successfully fetched anexos for relatorio: {id_relatorio_gestao}"
            )
            return data if isinstance(data, list) else [data]

        logging.warning(
            f"[cliente_transferegov_backend.py] Failed to fetch anexos for relatorio {id_relatorio_gestao}. Status: {status}"
        )
        return None
