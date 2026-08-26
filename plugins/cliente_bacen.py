"""Cliente da API SGS (Sistema Gerenciador de Series Temporais) do BACEN.

Base: ``https://api.bcb.gov.br/dados/serie`` -- catalogo em
https://dadosabertos.bcb.gov.br.

A API devolve sempre a mesma forma para qualquer serie::

    [{"data": "01/07/2026", "valor": "0.54"}, ...]

Por isso o cliente e generico: quem escolhe a serie e a DAG. Dois detalhes da
API que o codigo precisa respeitar:

* ``ultimos`` **nao** funciona como query string. So o formato de caminho
  (``/dados/ultimos/13``) limita o retorno -- ``?ultimos=13`` e ignorado em
  silencio e a API responde 200 com a serie inteira. Dai ``get_ultimos`` ser
  um metodo separado, e nao um parametro de ``get_serie``.
* Series de periodicidade **diaria** aceitam no maximo 10 anos por
  requisicao (HTTP 406 acima disso). Series mensais nao tem esse limite --
  por isso ``get_serie`` nao quebra o intervalo em janelas. Uma serie diaria
  com historico longo precisaria disso.
"""

import http
import logging
from datetime import datetime
from typing import Any, Optional

from cliente_base import ClienteBase

FORMATO_DATA_SGS = "%d/%m/%Y"


def data_sgs_para_iso(data: str) -> Optional[str]:
    """Converte ``dd/mm/aaaa`` (formato do SGS) para ``aaaa-mm-dd``.

    Nao e cosmetica: ``ClientPostgresDB`` cria toda coluna como TEXT, e quem
    faz o cast para ``date`` e o dbt, la na frente, usando o ``DateStyle`` do
    Postgres -- que por padrao e ``MDY``. Gravando ``01/07/2026`` como veio,
    o cast devolveria 7 de janeiro em vez de 1 de julho, sem erro nenhum.
    ISO nao tem essa ambiguidade.

    Devolve ``None`` quando a data nao esta no formato esperado, para o
    chamador decidir o que fazer com a linha.
    """
    try:
        return datetime.strptime(str(data).strip(), FORMATO_DATA_SGS).date().isoformat()
    except (ValueError, AttributeError):
        logging.warning("[cliente_bacen.py] Data fora do formato esperado: %r", data)
        return None


def normalizar_registros(
    serie: str,
    codigo: int,
    registros: list[dict[str, str]],
    dt_ingest: Optional[str] = None,
) -> list[dict[str, Any]]:
    """Converte o payload cru do SGS nas linhas da tabela raw.

    ``serie`` e ``codigo`` vao em toda linha porque a tabela guarda todas as
    series configuradas: sem eles nao daria para saber de qual serie o ponto
    veio, e a chave primaria (codigo_serie, data) nao existiria.

    Registro sem data reconhecivel e descartado -- ``data`` compoe a chave
    primaria, entao a linha nao teria como ser identificada nem atualizada.
    Valor vazio (a API manda ``""`` em ponto sem apuracao) vira ``None``, e
    nao a string vazia, para o ponto faltante ser nulo no banco.
    """
    dt_ingest = dt_ingest or datetime.now().isoformat()
    linhas: list[dict[str, Any]] = []

    for registro in registros:
        data_iso = data_sgs_para_iso(registro.get("data", ""))

        if data_iso is None:
            logging.warning(
                "[cliente_bacen.py] Registro sem data valida descartado: %r", registro
            )
            continue

        valor = str(registro.get("valor", "")).strip()
        linhas.append(
            {
                "serie": serie,
                "codigo_serie": str(codigo),
                "data": data_iso,
                "valor": valor or None,
                "dt_ingest": dt_ingest,
            }
        )

    return linhas


def configuracoes_de_series(bruto: Any) -> list[dict[str, Any]]:
    """Normaliza o conteudo da Variable ``bacen_series_sgs`` em configuracoes.

    Aceita as duas formas que alguem escreveria na Variable::

        {"ipca_servicos": 10844, "ipca": 433}

        [{"serie": "ipca_servicos", "codigo": 10844,
          "data_inicial": "01/01/2022"}]

    A primeira e o caso comum (nome -> codigo, serie inteira). A segunda
    existe para quando uma serie precisa de recorte de datas -- serie diaria
    com historico longo, por exemplo, que o SGS nao entrega de uma vez.

    Erra alto de proposito: codigo nao numerico vira ``ValueError`` em vez de
    serie silenciosamente ignorada, porque o sintoma seria uma tabela sem os
    pontos daquela serie e ninguem olhando.
    """
    if isinstance(bruto, dict):
        itens = [{"serie": nome, "codigo": codigo} for nome, codigo in bruto.items()]
    elif isinstance(bruto, list):
        itens = list(bruto)
    else:
        raise ValueError(
            "[cliente_bacen.py] 'bacen_series_sgs' deve ser objeto "
            f"{{nome: codigo}} ou lista de objetos, e nao {type(bruto).__name__}"
        )

    configuracoes: list[dict[str, Any]] = []

    for item in itens:
        if not isinstance(item, dict) or "codigo" not in item:
            raise ValueError(
                f"[cliente_bacen.py] Configuracao de serie invalida: {item!r} "
                "-- esperado objeto com 'serie' e 'codigo'"
            )

        try:
            codigo = int(item["codigo"])
        except (TypeError, ValueError) as erro:
            raise ValueError(
                f"[cliente_bacen.py] Codigo SGS invalido em {item!r}: "
                "o codigo da serie e um numero inteiro"
            ) from erro

        configuracoes.append(
            {
                # Sem nome, o codigo identifica a serie -- e melhor do que
                # recusar a configuracao por falta de rotulo.
                "serie": str(item.get("serie") or codigo),
                "codigo": codigo,
                "data_inicial": item.get("data_inicial"),
                "data_final": item.get("data_final"),
            }
        )

    return configuracoes


class ClienteBacen(ClienteBase):
    """Consome series temporais do SGS/BACEN."""

    BASE_URL = "https://api.bcb.gov.br/dados/serie"
    TIMEOUT_SEGUNDOS = 60

    def __init__(self) -> None:
        super().__init__(base_url=ClienteBacen.BASE_URL)
        logging.info(
            "[cliente_bacen.py] Initialized ClienteBacen with base_url: %s",
            ClienteBacen.BASE_URL,
        )

    def get_serie(
        self,
        codigo: int,
        data_inicial: Optional[str] = None,
        data_final: Optional[str] = None,
    ) -> list[dict[str, str]]:
        """Busca uma serie do SGS, inteira ou recortada por intervalo.

        Args:
            codigo: Codigo da serie no SGS (ex.: 10844).
            data_inicial: Inicio do recorte em ``dd/mm/aaaa``. Sem ele, a API
                devolve desde o primeiro ponto da serie.
            data_final: Fim do recorte em ``dd/mm/aaaa``.

        Returns:
            Payload cru da API (``data``/``valor``), ou lista vazia se a
            requisicao falhar ou o intervalo nao tiver ponto nenhum -- o SGS
            responde 404 nos dois casos, sem distinguir um do outro.
        """
        params = {"formato": "json"}

        if data_inicial:
            params["dataInicial"] = data_inicial
        if data_final:
            params["dataFinal"] = data_final

        return self._buscar(f"/bcdata.sgs.{codigo}/dados", params, codigo)

    def get_ultimos(self, codigo: int, quantidade: int = 12) -> list[dict[str, str]]:
        """Busca os ultimos N pontos de uma serie.

        ``quantidade`` vai no caminho, e nao na query string -- ver o modulo.
        """
        return self._buscar(
            f"/bcdata.sgs.{codigo}/dados/ultimos/{quantidade}",
            {"formato": "json"},
            codigo,
        )

    def _buscar(
        self, path: str, params: dict[str, str], codigo: int
    ) -> list[dict[str, str]]:
        logging.info("[cliente_bacen.py] Buscando serie %s em %s", codigo, path)

        status, dados = self.request(
            http.HTTPMethod.GET,
            path,
            params=params,
            timeout=ClienteBacen.TIMEOUT_SEGUNDOS,
        )

        if status == http.HTTPStatus.OK and isinstance(dados, list):
            logging.info(
                "[cliente_bacen.py] Serie %s: %d registros recebidos",
                codigo,
                len(dados),
            )
            return dados

        logging.warning(
            "[cliente_bacen.py] Serie %s nao retornou dados (status %s)", codigo, status
        )
        return []
