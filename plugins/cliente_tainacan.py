"""Cliente da API REST do Tainacan, para os repositórios do Iphan e do Ibram.

O Tainacan é um plugin de WordPress para acervos. Quatro instâncias públicas, sem
autenticação, alimentam os eixos 3 e 5 do Cultura em Números:

========  ======================================================  =====  ===========
chave     instância                                               fonte  coleções
========  ======================================================  =====  ===========
bcr       bcr.iphan.gov.br, Bem Brasileiro (bens registrados)     iphan  todas
inrc      inrc.iphan.gov.br, Inventário Nacional de Referências   iphan  todas
indl      indl.iphan.gov.br, Inventário Nacional da Diversidade   iphan  todas
          Linguística
cadastro  cadastro.museus.gov.br, Museus e Pontos de Memória      ibram  208, 179824
========  ======================================================  =====  ===========

Dois endpoints::

    GET /wp-json/tainacan/v2/collections?perpage=100
    GET /wp-json/tainacan/v2/collection/{id}/items
        ?perpage=N&paged=P&orderby=slug&order=ASC

O total de itens e o de páginas vêm nos headers ``X-WP-Total`` e
``X-WP-TotalPages``. Sem autenticação, a API só devolve item publicado.

**Página.** O Ibram corta a página em 96 itens sem avisar: ``perpage=100``
devolve 96. O Iphan aceita 50.

**Ordem.** Por ``slug``, que o WordPress mantém único entre os itens publicados
de uma coleção. A ordem padrão, por data, empata nos Pontos de Memória, que
foram carregados em lote, e o empate deixa a paginação por deslocamento repetir
um item numa página e pular outro. ``orderby=id`` é aceito e ignorado em
silêncio (testado em 16/11/2026); ``orderby=ID`` é recusado com 400.

**Conferência.** ``Coleta`` acumula as páginas e recusa a coleta (com
``PaginacaoInconsistente``) se os ids únicos não baterem com o ``X-WP-Total``,
se o total mudar entre a primeira página e a última ou se um id aparecer duas
vezes. O cadastro é vivo: item publicado ou despublicado no meio da paginação
desloca as páginas seguintes, e a nova tentativa da task resolve.

**Linha.** ``linha`` faz de cada item uma linha de texto (ADR 0012). Cada campo
do topo vira coluna, com o nome em minúsculas e ``_`` no lugar do que não é
letra, número ou ``_``. Lista e objeto viram JSON, e ``metadata`` (os campos do
formulário da coleção) vai inteiro, como veio, numa coluna JSON: um slug de
metadado chega a 120 caracteres, e o Postgres corta nome de coluna em 63. Nada é
achatado: o termo de taxonomia continua objeto, o relacionamento continua o id
de outro item, e o vazio continua como veio: ``""`` ou o booleano ``false``,
que o extrator do eixo 3 escrevia como o texto ``"False"``. Achatar é da silver.
"""

from __future__ import annotations

import json
import re
from collections import Counter
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

import httpx
import pandas as pd

import schemas_minc as schemas
import seeds_carga
from retry_helpers import retry_on_exception

CAMINHO_API = "/wp-json/tainacan/v2"
# O que o extrator do eixo 3 já usava (``_extract_tainacan``): uma página de 96
# itens do Ibram passa de 5 MB e leva dezenas de segundos.
TIMEOUT_S = 120.0
TENTATIVAS = 5
FATOR_BACKOFF = 2.0
STATUS_TRANSITORIO = frozenset({429, 500, 502, 503, 504})
# Ver o docstring do módulo: ``slug`` é a única ordem aceita sem empate.
ORDEM = {"orderby": "slug", "order": "ASC"}
COLECOES_POR_PAGINA = 100

# A coluna que identifica a coleção em toda linha: é o período de
# ``seeds_carga.substituir_periodo``, e cada tabela tem uma coleção só.
COLUNA_COLECAO = "collection_id"
COLUNA_METADADOS = "metadata"
COLUNA_PAGINA = "_pagina"
COLUNA_POSICAO = "_posicao"
# Os campos do topo de um item nas quatro instâncias, em 16/11/2026. A tabela
# nasce com eles mesmo quando a coleção não tem item publicado, para a bronze ter
# a forma certa; campo novo, ou um dos que só o Iphan tem
# (``document_content_index``, ``supports_document_content_extraction``,
# ``tainacan_inventarios_inventario_post_id``), entra quando aparece.
COLUNAS_BASE = (
    "id",
    "title",
    "description",
    "status",
    "slug",
    "url",
    COLUNA_COLECAO,
    "author_id",
    "author_name",
    "creation_date",
    "modification_date",
    "comment_status",
    "document",
    "document_type",
    "document_mimetype",
    "document_as_html",
    "document_options",
    "_thumbnail_id",
    "terms",
    "exposer_urls",
    COLUNA_METADADOS,
    COLUNA_PAGINA,
    COLUNA_POSICAO,
)
# Postgres corta nome de tabela em 63 caracteres, e a tabela de seeds é
# ``<fonte>_<entidade>`` e o modelo dbt ``bronze_<entidade>``.
LIMITE_ENTIDADE = seeds_carga.LIMITE_IDENTIFICADOR - len("bronze_")

_FORA_DO_NOME = re.compile(r"[^a-z0-9_]")


# ── catálogo ─────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class Instancia:
    chave: str
    fonte: str
    host: str
    nome: str
    itens_por_pagina: int
    # Iphan: toda coleção que a instância lista é carregada, e uma que não esteja
    # no catálogo faz o run falhar no fim. Ibram: só as do catálogo.
    todas_as_colecoes: bool

    @property
    def base_url(self) -> str:
        return f"https://{self.host}{CAMINHO_API}"


INSTANCIAS: dict[str, Instancia] = {
    i.chave: i
    for i in (
        Instancia(
            "bcr", schemas.FONTE_IPHAN, "bcr.iphan.gov.br", "Bem Brasileiro", 50, True
        ),
        Instancia(
            "inrc",
            schemas.FONTE_IPHAN,
            "inrc.iphan.gov.br",
            "Inventário Nacional de Referências Culturais",
            50,
            True,
        ),
        Instancia(
            "indl",
            schemas.FONTE_IPHAN,
            "indl.iphan.gov.br",
            "Inventário Nacional da Diversidade Linguística",
            50,
            True,
        ),
        Instancia(
            "cadastro",
            schemas.FONTE_IBRAM,
            "cadastro.museus.gov.br",
            "Cadastro Nacional de Museus e Pontos de Memória",
            96,
            False,
        ),
    )
}


@dataclass(frozen=True)
class Colecao:
    """Uma coleção, identificada pelo id na instância.

    A entidade é ``<instância>_<slug>``, com o slug como a instância o tinha
    quando a coleção entrou no catálogo. O slug muda se alguém o editar no
    WordPress, e o id não: a carga segue o id, e um slug diferente só gera aviso.
    ``entidade_fixa`` é para o slug que não cabe em ``LIMITE_ENTIDADE``.
    """

    instancia: str
    id: int
    slug: str
    nome: str
    entidade_fixa: str | None = None

    @property
    def entidade(self) -> str:
        return self.entidade_fixa or f"{self.instancia}_{seeds_carga.slug(self.slug)}"

    @property
    def fonte(self) -> str:
        return INSTANCIAS[self.instancia].fonte

    @property
    def tabela(self) -> str:
        """A tabela em ``seeds``, ``<fonte>_<entidade>`` (ADR 0012)."""
        return f"{self.fonte}_{self.entidade}"

    @property
    def url_itens(self) -> str:
        return f"{INSTANCIAS[self.instancia].base_url}/collection/{self.id}/items"


# As coleções de cada instância em 16/11/2026, pelo id. As com zero item
# publicado (INRC 2052 e 1931854, INDL 184289) entram mesmo assim: a carga é de
# todas as coleções do Iphan, e a tabela fica vazia até a primeira publicação.
COLECOES: tuple[Colecao, ...] = (
    Colecao("bcr", 65733, "bens-culturais", "Bens Culturais Imateriais Registrados"),
    Colecao("bcr", 65968, "documentos-do-process", "Documentos"),
    Colecao("bcr", 65998, "midias-registro", "Imagem, som e vídeo"),
    Colecao("bcr", 103951, "organizacoes", "Instituições Parceiras"),
    Colecao("bcr", 104725, "grupos-detentores", "Grupos detentores"),
    Colecao("bcr", 106027, "acoes-de-salvaguarda", "Ações de salvaguarda"),
    Colecao("bcr", 110421, "bibliografias", "Bibliografia"),
    Colecao("inrc", 1327, "audiovisuais", "11 - Imagem, som e vídeo"),
    Colecao("inrc", 1399, "bibliografia", "12 - Bibliografia"),
    Colecao("inrc", 1933, "agentes-do-patrimonio", "02 - Agentes do Patrimônio"),
    Colecao("inrc", 1954, "organizacoes", "03 - Organizações"),
    Colecao("inrc", 2052, "03-1identificacao", "04 - Comunidade"),
    Colecao("inrc", 2198, "04-identificacao-territorio", "05 - Território"),
    Colecao("inrc", 2292, "05-identificacao-bem-cultural", "06 - Bem Cultural"),
    Colecao("inrc", 2581, "06-grupo", "08 - Grupo"),
    Colecao("inrc", 2792, "07-identificacao-detentores", "07 - Detentores"),
    Colecao(
        "inrc",
        3727,
        "2-2-detalhamento-dos-projetos-de-identificacao",
        "01 - Detalhamento dos Projetos de Identificação",
    ),
    Colecao("inrc", 26488, "documentos", "10 - Documentos"),
    Colecao("inrc", 37719, "2-1-projetos", "0 - Projetos de Identificação"),
    Colecao("inrc", 1931854, "diagnostico-comunitario", "09 - Diagnóstico Comunitário"),
    Colecao("indl", 6, "identificacao-da-pesquisa", "Identificação da Pesquisa"),
    Colecao("indl", 19, "caracterizacao-territorial", "Caracterização Territorial"),
    Colecao("indl", 86, "comunidade-linguistica", "Comunidade Linguística"),
    Colecao(
        "indl",
        93,
        "identificacao-e-caracterizacao-da-lingua-referencia",
        "Identificação e Caracterização da Língua Referência",
    ),
    Colecao("indl", 100, "diagnostico-sociolinguistico", "Diagnóstico Sociolinguístico"),
    Colecao(
        "indl",
        107,
        "avaliacao-da-vitalidade-linguistica-revitalizacao",
        "Avaliação da Vitalidade Linguística, Revitalização",
    ),
    Colecao("indl", 350, "documentos", "Documentos"),
    Colecao("indl", 357, "midias", "Imagens, sons e vídeos"),
    Colecao("indl", 1983, "pessoas-de-referencia", "Pessoas de referência"),
    Colecao(
        "indl",
        5062,
        "instituicoes-responsaveis",
        "Instituições responsáveis pelos inventários",
    ),
    Colecao("indl", 11434, "referencias-bibliograficas-pp2", "Bibliografia"),
    Colecao("indl", 14653, "projeto-de-identificacao", "Projeto de Identificação"),
    Colecao("indl", 19322, "instituicoes-modulo-4", "Escolas"),
    Colecao(
        "indl", 19505, "instituicoes-que-promovem", "Instituições que promovem a língua"
    ),
    Colecao(
        "indl",
        20829,
        "identificacao-de-linguas-e-variedades",
        "Identificação de línguas e variedades",
    ),
    Colecao(
        "indl",
        26887,
        "identificacao-das-localidades-de-pesquisa",
        "Identificação das localidades de pesquisa",
    ),
    # O slug tem 111 caracteres; a entidade fica com o começo dele.
    Colecao(
        "indl",
        48393,
        "modulo-2-identificacao-de-localidades-de-ocorrencia-da-lingua-"
        "conhecidos-fora-da-area-de-abrangencia-da-pesquisa",
        "Localidades",
        entidade_fixa="indl_modulo_2_identificacao_de_localidades",
    ),
    Colecao("indl", 50770, "grafias-existentes", "Grafias existentes"),
    Colecao(
        "indl", 90288, "lexicos-e-listas-de-palavras", "Léxicos e Listas de Palavras"
    ),
    Colecao("indl", 184289, "resumo-executivo", "Resumo executivo"),
    Colecao("cadastro", 208, "museus", "Museus"),
    Colecao("cadastro", 179824, "pontos-de-memoria", "Pontos de Memória"),
)


def colecoes_selecionadas(
    instancias: Sequence[str] = (), ids: Sequence[int] = ()
) -> list[Colecao]:
    """As coleções do catálogo nas ``instancias`` e com os ``ids`` pedidos.

    Vazio é tudo. Instância ou id que não está no catálogo falha, em vez de o
    run terminar verde sem ter carregado o que foi pedido.
    """
    desconhecidas = sorted(set(instancias) - set(INSTANCIAS))
    if desconhecidas:
        raise ValueError(
            f"instância fora do catálogo: {desconhecidas}; as que existem são "
            f"{sorted(INSTANCIAS)}"
        )
    escolhidas = [c for c in COLECOES if not instancias or c.instancia in instancias]
    if ids:
        faltam = sorted(set(ids) - {c.id for c in escolhidas})
        if faltam:
            raise ValueError(
                f"coleção fora do catálogo (ou de outra instância): {faltam}"
            )
        escolhidas = [c for c in escolhidas if c.id in ids]
    return escolhidas


def conferir_catalogo(
    instancia: Instancia, listadas: Sequence[Mapping[str, Any]]
) -> tuple[list[str], list[str]]:
    """O que a listagem de coleções da instância diz do catálogo.

    Devolve ``(problemas, avisos)``. Problema faz o run falhar no fim, depois
    da carga do resto: coleção do catálogo que a instância não lista mais, e, no
    Iphan, coleção listada que o catálogo não tem (falta a bronze dela).
    Aviso é só log: o slug mudou, mas o id é o mesmo.
    """
    catalogo = {c.id: c for c in COLECOES if c.instancia == instancia.chave}
    por_id = {int(c["id"]): c for c in listadas}
    problemas, avisos = [], []
    for colecao_id in sorted(set(catalogo) - set(por_id)):
        problemas.append(
            f"{instancia.host}: a coleção {colecao_id} "
            f"({catalogo[colecao_id].entidade}) não está mais na listagem"
        )
    if instancia.todas_as_colecoes:
        for colecao_id in sorted(set(por_id) - set(catalogo)):
            listada = por_id[colecao_id]
            problemas.append(
                f"{instancia.host}: coleção nova {colecao_id} "
                f"({listada.get('slug')!r}, {listada.get('name')!r}) fora do "
                "catálogo de cliente_tainacan; ela precisa de entidade, source "
                "e bronze"
            )
    for colecao_id in sorted(set(catalogo) & set(por_id)):
        slug = por_id[colecao_id].get("slug")
        if slug != catalogo[colecao_id].slug:
            avisos.append(
                f"{instancia.host}: a coleção {colecao_id} mudou de slug, de "
                f"{catalogo[colecao_id].slug!r} para {slug!r}; a entidade continua "
                f"{catalogo[colecao_id].entidade}"
            )
    return problemas, avisos


# ── HTTP ─────────────────────────────────────────────────────────────────


class ErroTransitorio(Exception):
    """Status que vale tentar de novo (``STATUS_TRANSITORIO``)."""


class PaginacaoInconsistente(Exception):
    """As páginas de uma coleção não fecham com o total que a API declarou."""


def _header_inteiro(resposta: httpx.Response, nome: str) -> int:
    valor = resposta.headers.get(nome)
    if valor is None or not valor.strip().isdigit():
        raise ValueError(
            f"{resposta.request.url}: header {nome} ausente ou inválido ({valor!r})"
        )
    return int(valor)


@dataclass(frozen=True)
class Pagina:
    """Uma resposta da API, com o corpo como veio."""

    numero: int
    url: str
    corpo: bytes
    total_itens: int
    total_paginas: int

    def itens(self) -> list[dict[str, Any]]:
        dados = json.loads(self.corpo)
        itens = dados.get("items") if isinstance(dados, dict) else None
        if not isinstance(itens, list):
            raise ValueError(f"{self.url}: a resposta não tem a lista 'items'")
        return itens


class ClienteTainacan:
    """GET na API de uma instância, com retry nos status transitórios.

    ``TENTATIVAS`` tentativas, esperando 2, 4, 8 e 16 s entre elas
    (``FATOR_BACKOFF``), em ``STATUS_TRANSITORIO`` e em erro de rede. Outro
    status de erro (400, 404) falha na primeira.
    """

    def __init__(
        self, instancia: Instancia, transporte: httpx.BaseTransport | None = None
    ) -> None:
        self.instancia = instancia
        self.client = httpx.Client(
            base_url=instancia.base_url,
            timeout=TIMEOUT_S,
            follow_redirects=True,
            headers={"Accept": "application/json"},
            transport=transporte,
        )

    @retry_on_exception(
        max_attempts=TENTATIVAS,
        initial_delay=FATOR_BACKOFF,
        backoff_factor=FATOR_BACKOFF,
        exceptions_to_retry=(ErroTransitorio, httpx.TransportError),
    )
    def _get(self, caminho: str, params: Mapping[str, Any]) -> httpx.Response:
        resposta = self.client.get(caminho, params=dict(params))
        if resposta.status_code in STATUS_TRANSITORIO:
            raise ErroTransitorio(f"{resposta.status_code} em {resposta.request.url}")
        resposta.raise_for_status()
        return resposta

    def colecoes(self) -> tuple[bytes, list[dict[str, Any]]]:
        """A listagem de coleções da instância: o corpo como veio, e lido."""
        resposta = self._get("/collections", {"perpage": COLECOES_POR_PAGINA})
        if _header_inteiro(resposta, "X-WP-TotalPages") > 1:
            raise ValueError(
                f"{self.instancia.host} tem mais de {COLECOES_POR_PAGINA} coleções; "
                "a listagem precisa paginar"
            )
        listadas = resposta.json()
        if not isinstance(listadas, list):
            raise ValueError(f"{resposta.request.url}: a listagem não é uma lista")
        return resposta.content, listadas

    def pagina(self, colecao_id: int, numero: int) -> Pagina:
        resposta = self._get(
            f"/collection/{colecao_id}/items",
            {"perpage": self.instancia.itens_por_pagina, "paged": numero, **ORDEM},
        )
        return Pagina(
            numero=numero,
            url=str(resposta.request.url),
            corpo=resposta.content,
            total_itens=_header_inteiro(resposta, "X-WP-Total"),
            total_paginas=_header_inteiro(resposta, "X-WP-TotalPages"),
        )

    def paginas(self, colecao_id: int) -> Iterator[Pagina]:
        """Todas as páginas da coleção, pelo ``X-WP-TotalPages`` da primeira.

        A primeira é pedida mesmo quando a coleção está vazia: é ela que diz o
        total, e o total zero também é conferido.
        """
        primeira = self.pagina(colecao_id, 1)
        yield primeira
        for numero in range(2, primeira.total_paginas + 1):
            yield self.pagina(colecao_id, numero)


# ── conferência ──────────────────────────────────────────────────────────


@dataclass
class Coleta:
    """As páginas de uma coleção num run, e a conferência do que elas somam."""

    colecao: Colecao
    paginas: list[dict[str, Any]] = field(default_factory=list)
    ids: list[str] = field(default_factory=list)
    fora_da_colecao: list[str] = field(default_factory=list)

    def registrar(
        self, pagina: Pagina, itens: Sequence[Mapping[str, Any]], key: str
    ) -> None:
        self.paginas.append(
            {
                "pagina": pagina.numero,
                "url": pagina.url,
                "key": key,
                "itens": len(itens),
                "bytes": len(pagina.corpo),
                "x_wp_total": pagina.total_itens,
                "x_wp_totalpages": pagina.total_paginas,
            }
        )
        for item in itens:
            self.ids.append(str(item.get("id")))
            if str(item.get(COLUNA_COLECAO)) != str(self.colecao.id):
                self.fora_da_colecao.append(str(item.get("id")))

    @property
    def total_declarado(self) -> int:
        if not self.paginas:
            raise PaginacaoInconsistente(f"{self.colecao.entidade}: nenhuma página")
        return int(self.paginas[0]["x_wp_total"])

    def conferir(self) -> None:
        """Falha se as páginas não somam o total que a API declarou."""
        entidade = self.colecao.entidade
        total = self.total_declarado
        erros = []
        totais = sorted({p["x_wp_total"] for p in self.paginas})
        if len(totais) > 1:
            erros.append(f"o X-WP-Total mudou durante a paginação: {totais}")
        declaradas = max(1, int(self.paginas[0]["x_wp_totalpages"]))
        if len(self.paginas) != declaradas:
            erros.append(f"{len(self.paginas)} páginas lidas, {declaradas} declaradas")
        repetidos = sorted(i for i, n in Counter(self.ids).items() if n > 1)
        if repetidos:
            erros.append(f"{len(repetidos)} id(s) repetido(s), como {repetidos[:5]}")
        if len(set(self.ids)) != total:
            erros.append(f"{len(set(self.ids))} ids únicos, {total} declarados")
        if self.fora_da_colecao:
            erros.append(
                f"{len(self.fora_da_colecao)} item(ns) com outro {COLUNA_COLECAO}, "
                f"como {self.fora_da_colecao[:5]}"
            )
        if erros:
            raise PaginacaoInconsistente(f"{entidade}: " + "; ".join(erros))

    def resumo(self) -> dict[str, Any]:
        """O registro da coleta que vai para o raw, ao lado das páginas."""
        return {
            "instancia": self.colecao.instancia,
            "host": INSTANCIAS[self.colecao.instancia].host,
            "colecao_id": self.colecao.id,
            "entidade": self.colecao.entidade,
            "url": self.colecao.url_itens,
            "parametros": {
                "perpage": INSTANCIAS[self.colecao.instancia].itens_por_pagina,
                **ORDEM,
            },
            "x_wp_total": self.total_declarado,
            "itens": len(self.ids),
            "paginas": self.paginas,
        }


# ── linha ────────────────────────────────────────────────────────────────


def nome_de_coluna(chave: str) -> str:
    """O nome da coluna: ``document-type`` vira ``document_type``."""
    nome = _FORA_DO_NOME.sub("_", str(chave).lower())
    return str(seeds_carga.validar_identificador(nome))


def texto(valor: Any) -> str | None:
    """O valor como texto, sem mudar o que ele diz.

    ``None`` e ``""`` viram nulo: no ``COPY`` de ``seeds_carga`` os dois já
    chegariam nulos ao banco, e assim o Parquet de staging diz o mesmo. Lista e
    objeto viram JSON compacto, na ordem em que vieram.
    """
    if valor is None or valor == "":
        return None
    if isinstance(valor, bool):
        return "true" if valor else "false"
    if isinstance(valor, (list, dict)):
        return json.dumps(valor, ensure_ascii=False, separators=(",", ":"))
    return str(valor)


def linha(item: Mapping[str, Any], pagina: int, posicao: int) -> dict[str, str | None]:
    """Um item da API como linha de texto, mais a página e a posição nela."""
    registro: dict[str, str | None] = {}
    for chave, valor in item.items():
        nome = nome_de_coluna(chave)
        if nome in registro or nome in (COLUNA_PAGINA, COLUNA_POSICAO):
            raise ValueError(
                f"o campo {chave!r} do item {item.get('id')} colide com outra "
                f"coluna ({nome})"
            )
        registro[nome] = texto(valor)
    registro[COLUNA_PAGINA] = str(pagina)
    registro[COLUNA_POSICAO] = str(posicao)
    return registro


def garantir_colunas(df: pd.DataFrame) -> pd.DataFrame:
    """Acrescenta, vazias, as ``COLUNAS_BASE`` que o Parquet não tem.

    O Parquet de uma coleção sem item publicado só tem ``dt_ingest``.
    """
    faltam = [c for c in COLUNAS_BASE if c not in df.columns]
    if not faltam:
        return df
    vazias = {
        c: pd.Series([None] * len(df), index=df.index, dtype="string") for c in faltam
    }
    return df.assign(**vazias)
