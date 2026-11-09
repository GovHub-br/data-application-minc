"""Carga da bronze do SICONFI: do raw no MinIO para o Postgres, pelo Trino.

O raw do SICONFI é uma página da API por arquivo (``siconfi_storage``), e
o Trino não lê JSON do MinIO sem um catálogo Hive com metastore, que o
servidor não tem. A ponte é o Airflow: ele lê cada página do raw e manda os
itens ao Trino num ``INSERT``, sem tocar o Postgres (ADR 0009). O dbt declara
as tabelas como source e copia cada uma em ``siconfi/bronze_<entidade>``
(ADR 0010, como o TransfereGov e o BACEN).

**Duas tabelas, como no raw: a página e os itens.** O arquivo do raw é um
envelope (de qual requisição, de qual run, quando foi buscada) com os itens
dentro. A bronze guarda isso do mesmo jeito:

- ``<catálogo>.siconfi.paginas``: uma linha por página carregada, com tudo o
  que o envelope tem fora dos itens;
- ``<catálogo>.siconfi.<entidade>``: uma linha por item, com uma coluna por
  campo, mais ``_pagina_id`` e ``_item``, que apontam para a página.

Com o envelope repetido em cada item, a linha da MSC orçamentária tinha 745
bytes no Postgres; sem ele, 180. Nos 233 milhões de itens que o raw terá, isso
é a diferença entre uns 174 GB e uns 47 GB (medido com páginas reais em
05/11/2026).

**Cópia fiel do raw.** Cada campo do item vira uma coluna ``varchar`` com o
valor como está no arquivo do raw: o número é o texto que está lá
(``18449.34``), não o que um ``float`` faria dele. Esse texto é o que a
ingestão escreveu ao gravar a resposta da API lida por ``response.json()``,
então ``1.50`` da API chega ao raw como ``1.5``. Nada é filtrado nem
deduplicado aqui: a silver fica com uma linha por ``(_pagina_id, _item)`` e,
por ``request_hash``, com a tentativa de ``fetched_at`` mais recente (ADR 0014).

**Uma página é a unidade.** A página grande vem dividida em partes no raw
(``<hash>_<offset>_p01de03.json``); a carga espera todas as partes, junta os
itens na ordem e numera ``_item`` na página inteira. Página com parte faltando
fica para depois, e a que aparece inteira e dividida ao mesmo tempo é pulada
com aviso: os itens dela entrariam duas vezes.

**O que já foi carregado** é o que está em ``paginas``: a linha da página entra
depois dos itens. Se a task cair entre os dois, a página é carregada de novo no
próximo run, e a silver descarta a repetição por ``(_pagina_id, _item)``.

**O tamanho do comando.** O Trino recusa consulta com mais de 1 milhão de
caracteres (``query.max-length``). Os itens vão como uma lista de listas de
texto, sem repetir o nome dos campos, e o ``INSERT`` é dividido para ficar
abaixo de ``LIMITE_SQL``. Uma página de 5.000 itens da MSC dá cerca de
815 mil caracteres, e levou 1,1 s num Trino 478 local (4.500 linhas/s).
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Iterable, Mapping, Protocol, Sequence

import datalakehouse
from siconfi_controle import texto
from trino_bronze import quote_ident, sql_literal

SCHEMA_PADRAO = "siconfi"
VARIABLE_SCHEMA = "siconfi_trino_bronze_schema"
TABELA_PAGINAS = "paginas"
# Abaixo do query.max-length padrão do Trino (1.000.000), com folga para o
# cabeçalho do INSERT.
LIMITE_SQL = 900_000

# As colunas de cada item que não vêm da API, na ordem. Tudo varchar: a bronze
# não tipa.
COLUNAS_ITEM = ("_pagina_id", "_item")

# As colunas de ``paginas``, na ordem. As do envelope têm o nome que têm no
# raw; ``migrado_de`` é o ``_migrado_de`` da conversão das páginas antigas.
COLUNAS_PAGINA = (
    "pagina_id",
    "pagina",
    "arquivos",
    "itens",
    "endpoint",
    "request_hash",
    "request_params",
    "page_offset",
    "fetched_at",
    "run_id",
    "item_count",
    "response_headers",
    "payload_sem_itens",
    "migrado_de",
    "carga_run_id",
    "carregado_em",
)
_CAMPO_VALIDO = re.compile(r"^[a-z][a-z0-9_]*$")


class Executor(Protocol):
    def run(self, sql: str) -> None: ...

    def records(self, sql: str) -> list[Sequence[Any]]: ...


class ConexaoTrino:
    """``Executor`` sobre uma conexão DBAPI já aberta, para usar dentro de uma thread.

    No Airflow 3 a Connection só se resolve na thread principal da task: um
    ``TrinoHook`` criado numa thread do pool falha com ``The conn_id
    'trino_default' isn't defined``. Por isso a DAG abre as conexões na thread
    principal, como a ``salic_ingestion_trino`` e a ``mapas_ingestion_trino``, e
    entrega uma a cada thread, que só fala com o Trino por este objeto.
    """

    def __init__(self, conexao: Any) -> None:
        self.conexao = conexao

    def run(self, sql: str) -> None:
        cursor = self.conexao.cursor()
        cursor.execute(sql)
        # O Trino só termina o comando (o INSERT, o ALTER) quando o resultado é lido.
        cursor.fetchall()

    def records(self, sql: str) -> list[Sequence[Any]]:
        cursor = self.conexao.cursor()
        cursor.execute(sql)
        return list(cursor.fetchall())


# ── páginas no raw ───────────────────────────────────────────────────────


@dataclass
class Paginas:
    """As páginas de um endpoint no raw, prontas ou não para a carga.

    ``completas`` mapeia a key da página (a que ela teria inteira) para os
    arquivos que a compõem, em ordem. ``incompletas``: falta parte.
    ``ambiguas``: inteira e dividida, ou dividida de dois jeitos.
    """

    completas: dict[str, list[str]] = field(default_factory=dict)
    incompletas: set[str] = field(default_factory=set)
    ambiguas: set[str] = field(default_factory=set)


def agrupar_paginas(keys: Iterable[str]) -> Paginas:
    """Agrupa as keys do raw por página (ver ``siconfi_storage.contar_paginas``)."""
    inteiras: set[str] = set()
    partes: dict[str, dict[int, dict[int, str]]] = {}  # base -> total -> índice -> key
    for key in keys:
        nome = key.removesuffix(".json")
        achado = datalakehouse.parte_do_nome(nome)
        if achado is None:
            inteiras.add(nome)
            continue
        base, indice, total = achado
        partes.setdefault(base, {}).setdefault(total, {})[indice] = key
    paginas = Paginas()
    paginas.ambiguas = {base for base, por_total in partes.items() if len(por_total) > 1}
    paginas.ambiguas |= inteiras & set(partes)
    for nome in sorted(inteiras - paginas.ambiguas):
        paginas.completas[f"{nome}.json"] = [f"{nome}.json"]
    for base, por_total in sorted(partes.items()):
        if base in paginas.ambiguas:
            continue
        ((total, vistas),) = por_total.items()
        if set(vistas) == set(range(1, total + 1)):
            paginas.completas[f"{base}.json"] = [vistas[i] for i in range(1, total + 1)]
        else:
            paginas.incompletas.add(f"{base}.json")
    paginas.ambiguas = {f"{base}.json" for base in paginas.ambiguas}
    return paginas


def pagina_id(pagina: str) -> str:
    """Identificador curto da página: 16 hex do SHA-256 da key.

    Vai em cada item no lugar da key inteira (cerca de 150 caracteres), e é o
    mesmo toda vez que a página é carregada, o que deixa a silver reconhecer a
    repetição. Em 64 bits, a chance de duas das ~300 mil páginas colidirem é da
    ordem de 1 em 400 milhões.
    """
    return hashlib.sha256(pagina.encode("utf-8")).hexdigest()[:16]


# ── itens como texto ─────────────────────────────────────────────────────


def _ler_parte(texto_json: str) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """O envelope de um arquivo do raw e os itens dele, com todo número como texto."""
    envelope: dict[str, Any] = json.loads(texto_json, parse_float=str, parse_int=str)
    return envelope, itens_do_envelope(envelope)


def ler_itens(texto_json: str) -> list[dict[str, Any]]:
    """Os itens de um arquivo do raw, com todo número como o texto do arquivo."""
    return _ler_parte(texto_json)[1]


def valor_texto(valor: Any) -> str | None:
    """Um valor do item como texto, sem mudar o que ele diz."""
    if valor is None or isinstance(valor, str):
        return valor
    if isinstance(valor, bool):
        return "true" if valor else "false"
    return json.dumps(valor, ensure_ascii=False, separators=(",", ":"))


def itens_do_envelope(envelope: Mapping[str, Any]) -> list[dict[str, Any]]:
    payload = envelope.get("payload")
    if not isinstance(payload, Mapping):
        return []
    return [item for item in payload.get("items") or [] if isinstance(item, dict)]


def campos(itens: Iterable[Mapping[str, Any]]) -> list[str]:
    """Os campos dos itens, na ordem em que aparecem; falha em nome inesperado."""
    vistos: dict[str, None] = {}
    for item in itens:
        for chave in item:
            vistos.setdefault(chave, None)
    invalidos = [c for c in vistos if not _CAMPO_VALIDO.match(c)]
    if invalidos:
        raise ValueError(
            f"campos do SICONFI fora do padrão de coluna: {invalidos}. A bronze "
            "não renomeia campo; confira a resposta da API antes de carregar."
        )
    return list(vistos)


@dataclass(frozen=True)
class PaginaLida:
    pagina: str
    arquivos: list[str]
    envelope: dict[str, Any]
    itens: list[dict[str, Any]]
    # O que cada arquivo diz da página: ``fetched_at``, ``run_id``,
    # ``item_count`` e quantos itens ele trouxe (ver ``conferir_pagina``).
    partes: list[dict[str, str | None]] = field(default_factory=list)

    @property
    def id(self) -> str:
        return pagina_id(self.pagina)


def juntar_partes(pagina: str, arquivos: list[str], textos: list[str]) -> PaginaLida:
    """Os itens da página na ordem, juntando as partes.

    O envelope (de onde sai a linha de ``paginas``) é o da primeira parte, lido
    com os tipos normais: ``request_params`` fica como a requisição foi feita.
    Só os itens são lidos com o número como texto. De cada parte fica também o
    que ``conferir_pagina`` compara.
    """
    if not textos:
        raise ValueError(f"{pagina}: página sem arquivo")
    if len(textos) != len(arquivos):
        raise ValueError(f"{pagina}: {len(arquivos)} arquivos e {len(textos)} textos")
    envelope: dict[str, Any] = json.loads(textos[0])
    lidas = [_ler_parte(t) for t in textos]
    itens = [item for _, itens_parte in lidas for item in itens_parte]
    partes = [
        {
            "arquivo": arquivo,
            "fetched_at": valor_texto(env.get("fetched_at")),
            "run_id": valor_texto(env.get("run_id")),
            "item_count": valor_texto(env.get("item_count")),
            "itens": str(len(itens_parte)),
        }
        for arquivo, (env, itens_parte) in zip(arquivos, lidas)
    ]
    return PaginaLida(pagina, arquivos, envelope, itens, partes)


def conferir_pagina(lida: PaginaLida) -> list[str]:
    """O que impede a página de entrar; lista vazia quando ela está inteira.

    As partes de uma página dividida são gravadas uma a uma
    (``siconfi_storage.gravar_envelope``), e um retry da ingestão regrava a mesma
    key com outra tentativa. Se o raw ficou com partes de duas tentativas,
    juntá-las daria uma página que a API nunca devolveu, registrada como
    carregada e nunca mais relida. Toda parte repete o envelope da página, então
    as partes têm de concordar em ``fetched_at``, ``run_id`` e ``item_count``, e a
    soma dos itens tem de dar o ``item_count``. A página que não passa fica de
    fora, com aviso, e é tentada de novo no run seguinte, como a que tem parte
    faltando.
    """
    problemas: list[str] = []
    for campo in ("fetched_at", "run_id", "item_count"):
        valores = {parte[campo] for parte in lida.partes}
        if None in valores:
            problemas.append(f"parte sem {campo}")
        elif len(valores) > 1:
            problemas.append(
                f"partes com {campo} diferente: {sorted(str(v) for v in valores)}"
            )
    contagens = {parte["item_count"] for parte in lida.partes}
    if len(contagens) == 1 and None not in contagens:
        (esperado,) = contagens
        if not str(esperado).isdigit() or int(str(esperado)) != len(lida.itens):
            problemas.append(
                f"{len(lida.itens)} itens nas partes, e o item_count diz {esperado}"
            )
    return problemas


def conferir_endpoint(lida: PaginaLida, endpoint: str) -> None:
    """Falha se o envelope diz outro endpoint que não o da pasta do raw.

    O que já foi carregado é procurado em ``paginas`` pelo ``endpoint`` do
    envelope. Uma página da pasta de um endpoint com o envelope de outro nunca
    seria achada ali, e entraria de novo a cada run.
    """
    if lida.envelope.get("endpoint") != endpoint:
        raise ValueError(
            f"{lida.pagina}: o envelope diz endpoint "
            f"{lida.envelope.get('endpoint')!r}, e a página está na pasta de "
            f"{endpoint!r}"
        )


def registro_da_pagina(
    lida: PaginaLida, carga_run_id: str, carregado_em: datetime
) -> dict[str, str | None]:
    """A linha de ``paginas``: o envelope sem os itens, e de qual carga ela veio."""
    env = lida.envelope
    payload = env.get("payload")
    resto = (
        {k: v for k, v in payload.items() if k != "items"}
        if isinstance(payload, Mapping)
        else payload
    )
    return {
        "pagina_id": lida.id,
        "pagina": lida.pagina,
        "arquivos": str(len(lida.arquivos)),
        "itens": str(len(lida.itens)),
        "endpoint": valor_texto(env.get("endpoint")),
        "request_hash": valor_texto(env.get("request_hash")),
        "request_params": valor_texto(env.get("request_params")),
        "page_offset": valor_texto(env.get("page_offset")),
        "fetched_at": valor_texto(env.get("fetched_at")),
        "run_id": valor_texto(env.get("run_id")),
        "item_count": valor_texto(env.get("item_count")),
        "response_headers": valor_texto(env.get("response_headers")),
        "payload_sem_itens": valor_texto(resto),
        "migrado_de": valor_texto(env.get("_migrado_de")),
        "carga_run_id": carga_run_id,
        "carregado_em": carregado_em.astimezone(timezone.utc).isoformat(),
    }


# ── SQL do Trino ─────────────────────────────────────────────────────────


def tabela(catalogo: str, schema: str, nome: str) -> str:
    return f"{quote_ident(catalogo)}.{quote_ident(schema)}.{quote_ident(nome)}"


def _ddl(catalogo: str, schema: str, nome: str, colunas: Sequence[str]) -> str:
    definicoes = ", ".join(f"{quote_ident(c)} varchar" for c in colunas)
    return f"CREATE TABLE IF NOT EXISTS {tabela(catalogo, schema, nome)} ({definicoes})"


def ddl_tabela(catalogo: str, schema: str, nome: str) -> str:
    """A tabela dos itens de uma entidade; as colunas da API entram depois."""
    return _ddl(catalogo, schema, nome, COLUNAS_ITEM)


def ddl_paginas(catalogo: str, schema: str) -> str:
    return _ddl(catalogo, schema, TABELA_PAGINAS, COLUNAS_PAGINA)


def sql_colunas_existentes(catalogo: str, schema: str, nome: str) -> str:
    return (
        f"SELECT column_name FROM {quote_ident(catalogo)}.information_schema.columns "
        f"WHERE table_schema = {sql_literal(schema)} "
        f"AND table_name = {sql_literal(nome)}"
    )


def ddl_coluna(catalogo: str, schema: str, nome: str, coluna: str) -> str:
    return (
        f"ALTER TABLE {tabela(catalogo, schema, nome)} "
        f"ADD COLUMN IF NOT EXISTS {quote_ident(coluna)} varchar"
    )


def _linha_json(item: Mapping[str, Any], nomes: Sequence[str]) -> str:
    return json.dumps(
        [valor_texto(item.get(c)) for c in nomes],
        ensure_ascii=False,
        separators=(",", ":"),
    )


def _insert(
    destino: str,
    id_pagina: str,
    nomes: Sequence[str],
    linhas: Sequence[str],
    primeiro_item: int,
) -> str:
    """Um ``INSERT ... SELECT`` com os itens como uma lista JSON de listas de texto.

    O Trino desfaz a lista com ``UNNEST``: o texto do comando cresce com o valor
    dos itens, não com o nome dos campos, e o ``_pagina_id`` entra uma vez só.
    """
    colunas = [*COLUNAS_ITEM, *nomes]
    fixos = [sql_literal(id_pagina), f"CAST(n + {primeiro_item - 1} AS varchar)"]
    valores = [f"v[{i}]" for i in range(1, len(nomes) + 1)]
    lista = sql_literal("[" + ",".join(linhas) + "]")
    return (
        f"INSERT INTO {destino} ({', '.join(quote_ident(c) for c in colunas)}) "
        f"SELECT {', '.join(fixos + valores)} "
        f"FROM UNNEST(CAST(json_parse({lista}) AS array(array(varchar)))) "
        "WITH ORDINALITY AS t(v, n)"
    )


def inserts_da_pagina(
    destino: str,
    lida: PaginaLida,
    nomes: Sequence[str],
    limite: int = LIMITE_SQL,
) -> list[str]:
    """Os ``INSERT`` dos itens da página, cada um abaixo de ``limite`` caracteres."""
    if not lida.itens:
        return []
    vazio = _insert(destino, lida.id, nomes, [], 1)
    linhas = [_linha_json(item, nomes) for item in lida.itens]
    # Cada linha entra na lista com a vírgula; o apóstrofo vira dois no literal.
    grupos = datalakehouse.fatiar_por_tamanho(
        list(range(len(linhas))),
        lambda i: len(linhas[i]) + linhas[i].count("'") + 1,
        limite - len(vazio),
    )
    return [
        _insert(destino, lida.id, nomes, [linhas[i] for i in grupo], grupo[0] + 1)
        for grupo in grupos
    ]


def sql_paginas_carregadas(catalogo: str, schema: str, endpoint: str) -> str:
    return (
        f"SELECT DISTINCT pagina FROM {tabela(catalogo, schema, TABELA_PAGINAS)} "
        f"WHERE endpoint = {sql_literal(endpoint)}"
    )


def inserts_paginas(
    catalogo: str,
    schema: str,
    registros: Sequence[Mapping[str, str | None]],
    limite: int = LIMITE_SQL,
) -> list[str]:
    """Os ``INSERT`` das linhas de ``paginas``, cada um abaixo de ``limite``."""
    if not registros:
        return []
    cabecalho = (
        f"INSERT INTO {tabela(catalogo, schema, TABELA_PAGINAS)} "
        f"({', '.join(quote_ident(c) for c in COLUNAS_PAGINA)}) VALUES "
    )
    tuplas = [
        "(" + ", ".join(texto(r[c]) for c in COLUNAS_PAGINA) + ")" for r in registros
    ]
    grupos = datalakehouse.fatiar_por_tamanho(
        tuplas, lambda t: len(t) + 2, limite - len(cabecalho)
    )
    return [cabecalho + ", ".join(grupo) for grupo in grupos]


# ── a carga ──────────────────────────────────────────────────────────────


class Destino:
    """A tabela da bronze de um endpoint, com as colunas que ela já tem.

    ``executor`` é o da thread principal. Dentro de uma thread do pool, quem
    chama passa o próprio (``ConexaoTrino``) a ``acrescentar``; as colunas que
    já existem têm de ter sido lidas antes, na thread principal (``colunas()``).
    """

    def __init__(
        self, executor: Executor, catalogo: str, schema: str, entidade: str
    ) -> None:
        self.executor = executor
        self.catalogo = catalogo
        self.schema = schema
        self.entidade = entidade
        self.nome = tabela(catalogo, schema, entidade)
        self._colunas: set[str] | None = None

    def garantir(self) -> None:
        self.executor.run(
            f"CREATE SCHEMA IF NOT EXISTS {quote_ident(self.catalogo)}."
            f"{quote_ident(self.schema)}"
        )
        self.executor.run(ddl_tabela(self.catalogo, self.schema, self.entidade))

    def colunas(self, executor: Executor | None = None) -> set[str]:
        if self._colunas is None:
            self._colunas = {
                str(linha[0])
                for linha in (executor or self.executor).records(
                    sql_colunas_existentes(self.catalogo, self.schema, self.entidade)
                )
            }
        return self._colunas

    def acrescentar(
        self, nomes: Iterable[str], executor: Executor | None = None
    ) -> list[str]:
        """Cria as colunas que faltam e devolve as que criou."""
        executor = executor or self.executor
        novas = [c for c in nomes if c not in self.colunas(executor)]
        for coluna in novas:
            executor.run(ddl_coluna(self.catalogo, self.schema, self.entidade, coluna))
            self.colunas(executor).add(coluna)
        return novas


def carregar_pagina(
    destino: Destino, lida: PaginaLida, executor: Executor | None = None
) -> int:
    """Insere os itens da página na bronze e devolve quantos foram.

    ``executor`` é o da thread que chama; sem ele, vale o do ``destino``. Não
    registra a página em ``paginas``: quem chama faz isso depois, em lote
    (``inserts_paginas``).
    """
    executor = executor or destino.executor
    nomes = campos(lida.itens)
    destino.acrescentar(nomes, executor)
    for comando in inserts_da_pagina(destino.nome, lida, nomes):
        executor.run(comando)
    return len(lida.itens)
