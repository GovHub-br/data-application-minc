"""Cliente para a API IBGE Agregados v3.

Cobre todos os endpoints documentados em:
https://servicodados.ibge.gov.br/api/docs/agregados?versao=3

Endpoints suportados:
  GET /agregados                                           → lista pesquisas e agregados
  GET /agregados/{id}/metadados                           → metadados de um agregado
  GET /agregados/{id}/periodos                            → períodos disponíveis
  GET /agregados/{id}/variaveis                           → variáveis disponíveis
  GET /agregados/{id}/localidades/{nivel}                 → localidades por nível geográfico
  GET /agregados/{id}/periodos/{per}/variaveis/{var}     → séries de resultados
"""

import http
import json
import logging
import re
import time
from typing import Any, Optional, TypedDict

from cliente_base import ClienteBase
from safe_request import request_safe

logger = logging.getLogger(__name__)

# Mapeamento dos códigos de nível geográfico para descrições
NIVEIS_GEOGRAFICOS: dict[str, str] = {
    "N1": "Brasil",
    "N2": "Grande Região",
    "N3": "Unidade da Federação",
    "N6": "Município",
    "N7": "Região Metropolitana",
    "N8": "Mesorregião Geográfica",
    "N9": "Microrregião Geográfica",
    "N101": "País",
    "N102": "Região Imediata",
    "N103": "Região Intermediária",
}

_RETRY_SLEEP_S = 1.0

# Uma consulta no nível municipal (N6) leva de 1 a 6 s e passa de 3 MB (medido
# em 19/11/2026): os 10 s do ClienteBase não dão folga.
TIMEOUT_RESULTADOS_S = 120.0

# Sintaxe do parâmetro ``classificacao`` do SIDRA: ``86[2776,2777]`` ou, com
# mais de uma, ``1714[60024]|2661[32776,12869]``.
_CLASSIFICACAO = re.compile(r"^\d+\[[^\[\]]+\](\|\d+\[[^\[\]]+\])*$")


class EspecificacaoResultados(TypedDict):
    """O que pedir ao SIDRA para um agregado (uma task de resultados)."""

    agregado: str
    variaveis: str
    periodos: str
    classificacao: Optional[str]
    niveis: list[str]


def chave_classificacoes(classificacoes: list[dict[str, Any]]) -> str:
    """As categorias de uma linha só pelos IDs, na sintaxe do SIDRA.

    ``[{"id": "86", "nome": "Cor ou raça", "categoria": {"2776": "Branca"}}]``
    vira ``86[2776]``; com mais de uma classificação, em ordem de ID e unidas
    por ``|``. Sem os nomes, a chave não muda quando o IBGE renomeia uma
    categoria. Sem classificação, ``""``: a coluna entra na chave da tabela, e
    o Postgres recusa nulo em chave primária.
    """
    partes = []
    for classificacao in sorted(classificacoes, key=lambda c: int(c["id"])):
        categorias = sorted(classificacao.get("categoria", {}), key=int)
        partes.append(f"{classificacao['id']}[{','.join(categorias)}]")
    return "|".join(partes)


def _juntar(valor: Any, campo: str, agregado: str) -> str:
    """Texto ou lista de textos (unida com ``|``), nunca vazio."""
    if isinstance(valor, (list, tuple)):
        valor = "|".join(str(v).strip() for v in valor)
    texto = str(valor).strip() if valor is not None else ""
    if not texto:
        raise ValueError(f"agregado {agregado}: '{campo}' vazio")
    return texto


def especificacoes_resultados(
    config: dict[str, Any],
) -> list[EspecificacaoResultados]:
    """Os agregados cujos valores a DAG grava, a partir da Variable ``ibge_config``.

    Dois formatos convivem:

    - ``resultados``: lista de ``{"agregado", "variaveis", "periodos",
      "classificacao", "niveis"}``, um item por agregado. Só ``agregado`` é
      obrigatório; ``variaveis`` e ``periodos`` aceitam texto (``"2025|2026"``)
      ou lista;
    - o antigo: ``resultados_agregados_ids`` com ``resultados_periodos`` e
      ``resultados_nivel`` valendo para todos.

    Um agregado aparece uma vez só: a carga substitui as linhas do agregado
    inteiro, e duas especificações dele apagariam uma a outra.

    Raises:
        ValueError: configuração que a API recusaria ou que a carga não
            consegue gravar sem perda.
    """
    nivel_padrao = str(config.get("resultados_nivel", "N3"))
    periodos_padrao = config.get("resultados_periodos", "ultimo")
    itens: list[tuple[str, dict[str, Any]]] = [
        ("resultados", dict(item)) for item in config.get("resultados") or []
    ]
    itens += [
        (
            "resultados_agregados_ids",
            {"agregado": agregado, "periodos": periodos_padrao},
        )
        for agregado in config.get("resultados_agregados_ids") or []
    ]

    especificacoes: list[EspecificacaoResultados] = []
    origem_de: dict[str, str] = {}
    for origem, item in itens:
        agregado = str(item.get("agregado", "")).strip()
        if not agregado.isdigit():
            raise ValueError(f"{origem}: agregado {agregado!r} não é um código numérico")
        if agregado in origem_de:
            dica = (
                " Tire-o de resultados_agregados_ids: a lista 'resultados' já o tem."
                if "resultados_agregados_ids" in (origem, origem_de[agregado])
                and origem != origem_de[agregado]
                else ""
            )
            raise ValueError(f"agregado {agregado} aparece duas vezes.{dica}")
        origem_de[agregado] = origem

        niveis = item.get("niveis") or [nivel_padrao]
        if isinstance(niveis, str):
            niveis = [niveis]
        niveis = [str(n).strip() for n in niveis]
        desconhecidos = [n for n in niveis if n not in NIVEIS_GEOGRAFICOS]
        if desconhecidos:
            raise ValueError(f"agregado {agregado}: nível desconhecido {desconhecidos}")

        classificacao = item.get("classificacao")
        if classificacao is not None:
            classificacao = str(classificacao).replace(" ", "")
            if not _CLASSIFICACAO.match(classificacao):
                raise ValueError(
                    f"agregado {agregado}: classificação {classificacao!r} fora da "
                    "sintaxe do SIDRA, como 86[2776,2777] ou 1714[60024]|2661[32776]"
                )

        especificacoes.append(
            {
                "agregado": agregado,
                "variaveis": _juntar(
                    item.get("variaveis", "allxp"), "variaveis", agregado
                ),
                "periodos": _juntar(item.get("periodos", "ultimo"), "periodos", agregado),
                "classificacao": classificacao or None,
                "niveis": list(dict.fromkeys(niveis)),
            }
        )
    return especificacoes


class ClienteIBGE(ClienteBase):
    """Cliente HTTP para a API IBGE Agregados v3."""

    BASE_URL = "https://servicodados.ibge.gov.br/api/v3"

    def __init__(self, sleep_entre_requests: float = _RETRY_SLEEP_S) -> None:
        super().__init__(base_url=self.BASE_URL)
        self.sleep_entre_requests = sleep_entre_requests

    # ------------------------------------------------------------------
    # Endpoint 1: /agregados
    # ------------------------------------------------------------------

    def get_pesquisas(self) -> list[dict[str, Any]]:
        """GET /agregados — retorna todas as pesquisas com seus agregados."""
        status, data = request_safe(self, http.HTTPMethod.GET, "/agregados")
        if status != http.HTTPStatus.OK or not isinstance(data, list):
            logger.warning(
                "[extracao_ibge] get_pesquisas: status=%s tipo=%s",
                status,
                type(data).__name__,
            )
            return []
        logger.info("[extracao_ibge] get_pesquisas: %s pesquisas retornadas", len(data))
        return data

    def listar_agregados(
        self, pesquisas: list[dict[str, Any]] | None = None
    ) -> list[dict[str, Any]]:
        """Achata a resposta de get_pesquisas em uma lista plana de agregados.

        Aceita ``pesquisas`` já buscados para evitar uma segunda chamada à API
        quando o chamador já possui os dados.

        Cada item recebe ``pesquisa_id`` e ``pesquisa_nome`` para rastreabilidade.
        """
        if pesquisas is None:
            pesquisas = self.get_pesquisas()
        agregados: list[dict[str, Any]] = []
        for pesquisa in pesquisas:
            pid = pesquisa.get("id", "")
            pnome = pesquisa.get("nome", "")
            for agr in pesquisa.get("agregados", []):
                agregados.append(
                    {
                        "id": agr.get("id"),
                        "nome": agr.get("nome"),
                        "pesquisa_id": pid,
                        "pesquisa_nome": pnome,
                    }
                )
        logger.info(
            "[extracao_ibge] listar_agregados: %s agregados em %s pesquisas",
            len(agregados),
            len(pesquisas),
        )
        return agregados

    # ------------------------------------------------------------------
    # Endpoint 2: /agregados/{id}/metadados
    # ------------------------------------------------------------------

    def get_metadados(self, agregado_id: int | str) -> Optional[dict[str, Any]]:
        """GET /agregados/{id}/metadados."""
        status, data = request_safe(
            self, http.HTTPMethod.GET, f"/agregados/{agregado_id}/metadados"
        )
        if status != http.HTTPStatus.OK or not isinstance(data, dict):
            logger.warning(
                "[extracao_ibge] get_metadados: agregado=%s status=%s",
                agregado_id,
                status,
            )
            return None
        return data

    def get_metadados_batch(self, ids: list[int | str]) -> list[dict[str, Any]]:
        """Chama get_metadados para cada ID e achata o resultado.

        Campos aninhados complexos (listas) são convertidos para string JSON
        para compatibilidade com insert_data do ClientPostgresDB.
        """
        import json

        resultados: list[dict[str, Any]] = []
        for agregado_id in ids:
            meta = self.get_metadados(agregado_id)
            if meta:
                plano: dict[str, Any] = {"agregado_id": str(agregado_id)}
                for k, v in meta.items():
                    plano[k] = (
                        json.dumps(v, ensure_ascii=False)
                        if isinstance(v, (list, dict))
                        else v
                    )
                resultados.append(plano)
            if self.sleep_entre_requests:
                time.sleep(self.sleep_entre_requests)
        return resultados

    # ------------------------------------------------------------------
    # Endpoint 3: /agregados/{id}/periodos
    # ------------------------------------------------------------------

    def get_periodos(self, agregado_id: int | str) -> list[dict[str, Any]]:
        """GET /agregados/{id}/periodos."""
        status, data = request_safe(
            self, http.HTTPMethod.GET, f"/agregados/{agregado_id}/periodos"
        )
        if status != http.HTTPStatus.OK or not isinstance(data, list):
            logger.warning(
                "[extracao_ibge] get_periodos: agregado=%s status=%s",
                agregado_id,
                status,
            )
            return []
        return data

    def get_periodos_batch(self, ids: list[int | str]) -> list[dict[str, Any]]:
        """Busca períodos para cada agregado ID e adiciona ``agregado_id`` em cada registro."""
        import json

        resultados: list[dict[str, Any]] = []
        for agregado_id in ids:
            periodos = self.get_periodos(agregado_id)
            for p in periodos:
                row: dict[str, Any] = {"agregado_id": str(agregado_id)}
                for k, v in p.items():
                    row[k] = (
                        json.dumps(v, ensure_ascii=False)
                        if isinstance(v, (list, dict))
                        else v
                    )
                resultados.append(row)
            if self.sleep_entre_requests:
                time.sleep(self.sleep_entre_requests)
        return resultados

    # ------------------------------------------------------------------
    # Endpoint 4: /agregados/{id}/variaveis
    # ------------------------------------------------------------------

    def get_variaveis(self, agregado_id: int | str) -> list[dict[str, Any]]:
        """GET /agregados/{id}/variaveis."""
        status, data = request_safe(
            self, http.HTTPMethod.GET, f"/agregados/{agregado_id}/variaveis"
        )
        if status != http.HTTPStatus.OK or not isinstance(data, list):
            logger.warning(
                "[extracao_ibge] get_variaveis: agregado=%s status=%s",
                agregado_id,
                status,
            )
            return []
        return data

    def get_variaveis_batch(self, ids: list[int | str]) -> list[dict[str, Any]]:
        """Busca variáveis para cada agregado ID."""
        import json

        resultados: list[dict[str, Any]] = []
        for agregado_id in ids:
            variaveis = self.get_variaveis(agregado_id)
            for v in variaveis:
                row: dict[str, Any] = {"agregado_id": str(agregado_id)}
                for k, val in v.items():
                    row[k] = (
                        json.dumps(val, ensure_ascii=False)
                        if isinstance(val, (list, dict))
                        else val
                    )
                resultados.append(row)
            if self.sleep_entre_requests:
                time.sleep(self.sleep_entre_requests)
        return resultados

    # ------------------------------------------------------------------
    # Endpoint 5: /agregados/{id}/localidades/{nivel}
    # ------------------------------------------------------------------

    def get_localidades(
        self, agregado_id: int | str, nivel: str = "N3"
    ) -> list[dict[str, Any]]:
        """GET /agregados/{id}/localidades/{nivel}."""
        status, data = request_safe(
            self, http.HTTPMethod.GET, f"/agregados/{agregado_id}/localidades/{nivel}"
        )
        if status != http.HTTPStatus.OK or not isinstance(data, list):
            logger.warning(
                "[extracao_ibge] get_localidades: agregado=%s nivel=%s status=%s",
                agregado_id,
                nivel,
                status,
            )
            return []
        return data

    def get_localidades_batch(
        self, ids: list[int | str], nivel: str = "N3"
    ) -> list[dict[str, Any]]:
        """Busca localidades para cada agregado ID no nível geográfico indicado."""
        import json

        resultados: list[dict[str, Any]] = []
        for agregado_id in ids:
            localidades = self.get_localidades(agregado_id, nivel)
            for loc in localidades:
                row: dict[str, Any] = {"agregado_id": str(agregado_id), "nivel": nivel}
                for k, v in loc.items():
                    row[k] = (
                        json.dumps(v, ensure_ascii=False)
                        if isinstance(v, (list, dict))
                        else v
                    )
                resultados.append(row)
            if self.sleep_entre_requests:
                time.sleep(self.sleep_entre_requests)
        return resultados

    # ------------------------------------------------------------------
    # Endpoint 6: /agregados/{id}/periodos/{per}/variaveis/{var}
    # ------------------------------------------------------------------

    def get_resultados(
        self,
        agregado_id: int | str,
        periodos: str = "ultimo",
        variaveis: str = "allxp",
        nivel: str = "N3",
        localidades: str = "all",
        classificacao: Optional[str] = None,
        timeout: float = TIMEOUT_RESULTADOS_S,
    ) -> list[dict[str, Any]]:
        """GET /agregados/{id}/periodos/{per}/variaveis/{var}

        Args:
            agregado_id: ID numérico do agregado IBGE.
            periodos: Período(s) separados por ``|``, ou ``"ultimo"`` para o
                mais recente, ou ``"all"`` para todos.
            variaveis: ID(s) de variável separados por ``|``, ou ``"allxp"``
                para todas.
            nivel: Código de nível geográfico (ex.: ``"N3"`` para UF).
            localidades: Código de localidade dentro do nível, ou ``"all"``.
            classificacao: Categorias pedidas, na sintaxe do SIDRA
                (``86[2776,2777]|2[6794]``). Sem ela, a API devolve só o total
                de cada classificação.
            timeout: Segundos de espera pela resposta.

        Returns:
            Lista de registros de séries de resultados.
        """
        path = f"/agregados/{agregado_id}/periodos/{periodos}/variaveis/{variaveis}"
        params = {"localidades": f"{nivel}[{localidades}]"}
        if classificacao:
            params["classificacao"] = classificacao
        status, data = request_safe(
            self, http.HTTPMethod.GET, path, params=params, timeout=timeout
        )
        if status != http.HTTPStatus.OK or not isinstance(data, list):
            # A mensagem de erro da API (classificação inválida, consulta grande
            # demais) vem no corpo: sem ela, o log só diz que falhou.
            logger.warning(
                "[extracao_ibge] get_resultados: agregado=%s periodos=%s nivel=%s "
                "status=%s resposta=%r",
                agregado_id,
                periodos,
                nivel,
                status,
                str(data)[:300],
            )
            return []
        return data

    def get_resultados_achatar(
        self,
        agregado_id: int | str,
        periodos: str = "ultimo",
        variaveis: str = "allxp",
        nivel: str = "N3",
        localidades: str = "all",
        classificacao: Optional[str] = None,
    ) -> list[dict[str, Any]]:
        """Achata a resposta de get_resultados em linhas observacionais.

        A API retorna uma estrutura aninhada:
          variavel → resultado (uma combinação de categorias) → localidade →
          { periodo: valor }

        Este método produz uma linha por combinação (variável, categorias,
        localidade, período). ``classificacoes`` guarda as categorias da linha
        em JSON, com os nomes; ``classificacoes_chave``, só os IDs (ver
        ``chave_classificacoes``).
        """
        raw = self.get_resultados(
            agregado_id=agregado_id,
            periodos=periodos,
            variaveis=variaveis,
            nivel=nivel,
            localidades=localidades,
            classificacao=classificacao,
        )

        linhas: list[dict[str, Any]] = []
        for variavel in raw:
            var_id = variavel.get("id")
            var_nome = variavel.get("variavel")
            var_unidade = variavel.get("unidade")
            for resultado in variavel.get("resultados", []):
                classificacoes = resultado.get("classificacoes", [])
                classificacoes_json = (
                    json.dumps(classificacoes, ensure_ascii=False)
                    if classificacoes
                    else None
                )
                chave = chave_classificacoes(classificacoes)
                for serie in resultado.get("series", []):
                    localidade = serie.get("localidade", {})
                    for periodo, valor in serie.get("serie", {}).items():
                        linhas.append(
                            {
                                "agregado_id": str(agregado_id),
                                "variavel_id": str(var_id),
                                "variavel_nome": var_nome,
                                "unidade": var_unidade,
                                "localidade_id": str(localidade.get("id", "")),
                                "localidade_nome": localidade.get("nome", ""),
                                "nivel": nivel,
                                "periodo": periodo,
                                "valor": valor,
                                "classificacoes": classificacoes_json,
                                "classificacoes_chave": chave,
                            }
                        )
        return linhas
