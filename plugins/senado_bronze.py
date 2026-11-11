"""Carga da bronze da legislação do Senado: do raw no MinIO para o Postgres, pelo Trino.

O Trino não lê JSON do MinIO sem um catálogo Hive, que o servidor não tem. Como
no SICONFI (``siconfi_bronze``), quem faz a ponte é o Airflow: lê cada arquivo do
raw e manda as linhas ao Trino num ``INSERT``, sem tocar o Postgres (ADR 0009).
O dbt declara as tabelas como source e copia cada uma em
``senado/bronze_<tabela>`` (ADR 0010).

**Quatro tabelas, toda coluna ``varchar``:**

- ``legislacao_lista``: um documento de uma listagem por linha, com os campos
  em minúsculas (``normaNome`` vira ``normanome``) e ``_arquivo``/``_item``;
- ``legislacao_detalhe``: um detalhe por linha, com ``identificacao`` achatada
  em ``identificacao__<campo>`` (o separador do ADR 0011);
- ``legislacao_detalhe_secoes``: um elemento de seção do detalhe por linha
  (publicação, vide, ediv, disp, frase do catálogo e da indexação, classe). O
  elemento texto fica texto; o objeto vira JSON compacto. As seções não vão
  como JSON numa coluna do detalhe porque o Trino recusa comando acima de
  1 milhão de caracteres, e a Lei 8.112 sozinha tem 725 KiB de detalhe;
- ``arquivos``: um arquivo do raw carregado por linha. É o registro do que já
  entrou, e entra depois das linhas do arquivo: se a task cair entre os dois, o
  arquivo é carregado de novo no run seguinte, e a silver descarta a repetição
  por ``(_arquivo, _item)``.

**Cópia fiel.** Nada é filtrado nem deduplicado: a listagem do ano corrente
entra de novo a cada run, e a silver fica com a mais recente. Campo novo da API
vira coluna nova (``ADD COLUMN IF NOT EXISTS``); campo cujo nome em minúsculas
não serve de coluna faz a carga falhar, em vez de ser renomeado.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Iterable, Mapping, Protocol, Sequence

import datalakehouse
import schemas_minc as schemas
from cliente_senadores import ClienteSenadores
from trino_bronze import quote_ident, sql_literal

SCHEMA_PADRAO = schemas.SCHEMA_SENADO
CATALOGO_PADRAO = "dw"
VARIABLE_SCHEMA = "senado_trino_bronze_schema"
VARIABLE_CATALOGO = "senado_trino_target_catalog"
# Abaixo do query.max-length padrão do Trino (1.000.000), com folga para o
# cabeçalho do INSERT.
LIMITE_SQL = 900_000

TABELA_LISTA = schemas.ENTIDADE_LEGISLACAO_LISTA
TABELA_DETALHE = schemas.ENTIDADE_LEGISLACAO_DETALHE
TABELA_SECOES = f"{schemas.ENTIDADE_LEGISLACAO_DETALHE}_secoes"
TABELA_ARQUIVOS = "arquivos"

# As colunas que a API devolvia em 10/11/2026. A tabela nasce com elas, para o
# bronze_* do dbt ter a forma certa antes do primeiro dado; campo novo entra
# depois, por ALTER.
COLUNAS_LISTA = (
    "_arquivo",
    "_item",
    "id",
    "tipo",
    "descricao",
    "numero",
    "norma",
    "normanome",
    "ementa",
    "dataassinatura",
    "anoassinatura",
    "apelido",
    # Só nos decretos sem número (DEC-sn): o sequencial da assinatura na data.
    "sequencia",
)
COLUNAS_DETALHE = (
    "_arquivo",
    "id",
    "identificacao__tipo",
    "identificacao__descricao",
    "identificacao__numero",
    "identificacao__norma",
    "identificacao__normanome",
    "identificacao__dataassinatura",
    "identificacao__apelido",
    "identificacao__urldocumento",
    "identificacao__sequencia",
    "ementa",
    "observacao",
)
COLUNAS_SECOES = ("_arquivo", "id", "secao", "_item", "valor")
COLUNAS_ARQUIVOS = (
    "arquivo",
    "entidade",
    "partes",
    "itens_na_origem",
    "linhas",
    "linhas_secoes",
    "requisicao",
    "status_http",
    "buscado_em",
    "run_id",
    "resposta_sem_documentos",
    "carga_run_id",
    "carregado_em",
)
COLUNAS = {
    TABELA_LISTA: COLUNAS_LISTA,
    TABELA_DETALHE: COLUNAS_DETALHE,
    TABELA_SECOES: COLUNAS_SECOES,
    TABELA_ARQUIVOS: COLUNAS_ARQUIVOS,
}

# Seção do detalhe -> nome do elemento dentro dela (DetalheDocumentov3.xsd).
SECOES = {
    "publicacoes": "publicacao",
    "vides": "vide",
    "edivs": "ediv",
    "disps": "disp",
    "catalogo": "frase",
    "indexacao": "frase",
    "classes": "classe",
}
# Objeto só de campos de texto, que vira colunas ``<objeto>__<campo>``.
ACHATADOS = ("identificacao",)

_COLUNA_VALIDA = re.compile(r"^[a-z_][a-z0-9_]*$")


class Executor(Protocol):
    def run(self, sql: str) -> None: ...

    def records(self, sql: str) -> list[Sequence[Any]]: ...


# ── do envelope para linhas ──────────────────────────────────────────────


def valor_texto(valor: Any) -> str | None:
    """O valor como texto: texto fica como está, objeto e lista viram JSON."""
    if valor is None or isinstance(valor, str):
        return valor
    if isinstance(valor, bool):
        return "true" if valor else "false"
    return json.dumps(valor, ensure_ascii=False, separators=(",", ":"))


def coluna(nome: str) -> str:
    """O nome do campo em minúsculas; falha se ele não serve de coluna."""
    minusculo = str(nome).lower()
    if not _COLUNA_VALIDA.match(minusculo):
        raise ValueError(
            f"campo {nome!r} da API do Senado não serve de coluna. A bronze não "
            "renomeia campo; confira a resposta antes de carregar."
        )
    return minusculo


def _por(linha: dict[str, str | None], nome: str, valor: Any) -> None:
    alvo = coluna(nome)
    if alvo in linha:
        raise ValueError(f"dois campos viram a coluna {alvo!r}")
    linha[alvo] = valor_texto(valor)


def linhas_da_lista(
    arquivo: str, envelope: Mapping[str, Any]
) -> list[dict[str, str | None]]:
    """Uma linha por documento da listagem, na ordem da resposta."""
    linhas = []
    documentos = ClienteSenadores.documentos_da_lista(envelope.get("resposta"))
    for item, documento in enumerate(documentos, start=1):
        linha: dict[str, str | None] = {"_arquivo": arquivo, "_item": str(item)}
        for nome, valor in documento.items():
            _por(linha, nome, valor)
        linhas.append(linha)
    return linhas


def _documentos_do_detalhe(envelope: Mapping[str, Any]) -> list[dict[str, Any]]:
    return ClienteSenadores.documentos_do_detalhe(envelope.get("resposta"))


def linhas_do_detalhe(
    arquivo: str, envelope: Mapping[str, Any]
) -> list[dict[str, str | None]]:
    """Uma linha por documento do detalhe (na prática, uma por arquivo).

    As seções vão para ``secoes_do_detalhe``; ``identificacao`` é achatada; um
    campo que a API passe a mandar e não seja texto entra como JSON, sem se perder.
    """
    linhas = []
    for documento in _documentos_do_detalhe(envelope):
        linha: dict[str, str | None] = {"_arquivo": arquivo}
        for nome, valor in documento.items():
            if nome in SECOES:
                continue
            if nome in ACHATADOS and isinstance(valor, dict):
                for subnome, subvalor in valor.items():
                    _por(linha, f"{nome}__{subnome}", subvalor)
            else:
                _por(linha, nome, valor)
        linhas.append(linha)
    return linhas


def _elementos(valor: Any, elemento: str) -> list[Any]:
    """Os elementos de uma seção: ``""`` não tem nenhum, o objeto da seção
    traz um elemento ou uma lista deles. Seção de outra forma entra inteira."""
    if isinstance(valor, dict) and set(valor) == {elemento}:
        valor = valor[elemento]
    return ClienteSenadores.como_lista(valor)


def secoes_do_detalhe(
    arquivo: str, envelope: Mapping[str, Any]
) -> list[dict[str, str | None]]:
    """Uma linha por elemento de cada seção do detalhe, na ordem da resposta."""
    linhas = []
    for documento in _documentos_do_detalhe(envelope):
        norma_id = valor_texto(documento.get("id"))
        for secao, elemento in SECOES.items():
            if secao not in documento:
                continue
            for item, valor in enumerate(_elementos(documento[secao], elemento), start=1):
                linhas.append(
                    {
                        "_arquivo": arquivo,
                        "id": norma_id,
                        "secao": secao,
                        "_item": str(item),
                        "valor": valor_texto(valor),
                    }
                )
    return linhas


def resposta_sem_documentos(resposta: Any) -> Any:
    """A resposta sem ``documentos``: o cabeçalho e os metadados da API."""
    if not isinstance(resposta, dict):
        return resposta
    return {
        raiz: (
            {k: v for k, v in corpo.items() if k != "documentos"}
            if isinstance(corpo, dict)
            else corpo
        )
        for raiz, corpo in resposta.items()
    }


@dataclass
class Convertido:
    """Um arquivo do raw pronto para a bronze."""

    arquivo: str
    entidade: str
    partes: int
    envelope: dict[str, Any]
    linhas: list[dict[str, str | None]]
    secoes: list[dict[str, str | None]] = field(default_factory=list)

    @property
    def tabela(self) -> str:
        return TABELA_LISTA if self.entidade == TABELA_LISTA else TABELA_DETALHE


def converter(
    arquivo: str, entidade: str, partes: int, envelope: dict[str, Any]
) -> Convertido:
    """As linhas de um arquivo do raw. Falha se o envelope diz outra entidade
    que não a da pasta: o arquivo nunca seria achado em ``arquivos`` e entraria
    de novo a cada run."""
    if envelope.get("entidade") != entidade:
        raise ValueError(
            f"{arquivo}: o envelope diz {envelope.get('entidade')!r}, e o arquivo "
            f"está na pasta de {entidade!r}"
        )
    if entidade == TABELA_LISTA:
        return Convertido(
            arquivo, entidade, partes, envelope, linhas_da_lista(arquivo, envelope)
        )
    if entidade == TABELA_DETALHE:
        return Convertido(
            arquivo,
            entidade,
            partes,
            envelope,
            linhas_do_detalhe(arquivo, envelope),
            secoes_do_detalhe(arquivo, envelope),
        )
    raise ValueError(f"entidade desconhecida: {entidade!r}")


def registro(
    convertido: Convertido, carga_run_id: str, carregado_em: datetime
) -> dict[str, str | None]:
    """A linha de ``arquivos`` de um arquivo carregado."""
    env = convertido.envelope
    detalhe = convertido.entidade == TABELA_DETALHE
    return {
        "arquivo": convertido.arquivo,
        "entidade": convertido.entidade,
        "partes": str(convertido.partes),
        "itens_na_origem": valor_texto(env.get("itens")),
        "linhas": str(len(convertido.linhas)),
        "linhas_secoes": str(len(convertido.secoes)) if detalhe else None,
        "requisicao": valor_texto(env.get("requisicao")),
        "status_http": valor_texto(env.get("status_http")),
        "buscado_em": valor_texto(env.get("buscado_em")),
        "run_id": valor_texto(env.get("run_id")),
        "resposta_sem_documentos": valor_texto(
            resposta_sem_documentos(env.get("resposta"))
        ),
        "carga_run_id": carga_run_id,
        "carregado_em": carregado_em.astimezone(timezone.utc).isoformat(),
    }


# ── SQL do Trino ─────────────────────────────────────────────────────────


def tabela(catalogo: str, schema: str, nome: str) -> str:
    return f"{quote_ident(catalogo)}.{quote_ident(schema)}.{quote_ident(nome)}"


def ddl_schema(catalogo: str, schema: str) -> str:
    return f"CREATE SCHEMA IF NOT EXISTS {quote_ident(catalogo)}.{quote_ident(schema)}"


def ddl_tabela(catalogo: str, schema: str, nome: str, colunas: Sequence[str]) -> str:
    definicoes = ", ".join(f"{quote_ident(c)} varchar" for c in colunas)
    return f"CREATE TABLE IF NOT EXISTS {tabela(catalogo, schema, nome)} ({definicoes})"


def ddl_coluna(catalogo: str, schema: str, nome: str, nova: str) -> str:
    return (
        f"ALTER TABLE {tabela(catalogo, schema, nome)} "
        f"ADD COLUMN IF NOT EXISTS {quote_ident(nova)} varchar"
    )


def sql_colunas(catalogo: str, schema: str, nome: str) -> str:
    return (
        f"SELECT column_name FROM {quote_ident(catalogo)}.information_schema.columns "
        f"WHERE table_schema = {sql_literal(schema)} AND table_name = {sql_literal(nome)}"
    )


def _insert(destino: str, colunas: Sequence[str], linhas_json: Sequence[str]) -> str:
    """``INSERT ... SELECT`` com as linhas como uma lista JSON de listas de texto,
    desfeita pelo ``UNNEST``, como na bronze do SICONFI."""
    valores = ", ".join(f"v[{i}]" for i in range(1, len(colunas) + 1))
    lista = sql_literal("[" + ",".join(linhas_json) + "]")
    return (
        f"INSERT INTO {destino} ({', '.join(quote_ident(c) for c in colunas)}) "
        f"SELECT {valores} "
        f"FROM UNNEST(CAST(json_parse({lista}) AS array(array(varchar)))) AS t(v)"
    )


def inserts(
    catalogo: str,
    schema: str,
    nome: str,
    colunas: Sequence[str],
    linhas: Sequence[Mapping[str, str | None]],
    limite: int = LIMITE_SQL,
) -> list[str]:
    """Os ``INSERT`` das ``linhas``, cada um abaixo de ``limite`` caracteres.

    Uma linha que sozinha passa do limite falha aqui, com o tamanho, em vez de
    o Trino recusar o comando.
    """
    if not linhas:
        return []
    destino = tabela(catalogo, schema, nome)
    em_json = [
        json.dumps(
            [linha.get(c) for c in colunas], ensure_ascii=False, separators=(",", ":")
        )
        for linha in linhas
    ]
    vazio = _insert(destino, colunas, [])
    # Cada linha entra na lista com a vírgula; o apóstrofo vira dois no literal.
    grupos = datalakehouse.fatiar_por_tamanho(
        list(range(len(em_json))),
        lambda i: len(em_json[i]) + em_json[i].count("'") + 1,
        limite - len(vazio),
    )
    return [_insert(destino, colunas, [em_json[i] for i in grupo]) for grupo in grupos]


def colunas_das_linhas(
    base: Sequence[str], linhas: Iterable[Mapping[str, Any]]
) -> list[str]:
    """As colunas da tabela e, depois delas, as novas, na ordem em que aparecem."""
    vistas: dict[str, None] = dict.fromkeys(base)
    for linha in linhas:
        for nome in linha:
            vistas.setdefault(nome, None)
    return list(vistas)


def sql_carregados(catalogo: str, schema: str) -> str:
    return f"SELECT arquivo FROM {tabela(catalogo, schema, TABELA_ARQUIVOS)}"


def sql_registros(catalogo: str, schema: str) -> str:
    return (
        "SELECT arquivo, entidade, linhas, linhas_secoes "
        f"FROM {tabela(catalogo, schema, TABELA_ARQUIVOS)}"
    )


def sql_contagem_por_arquivo(catalogo: str, schema: str, nome: str) -> str:
    return (
        f"SELECT _arquivo, count(*) FROM {tabela(catalogo, schema, nome)} "
        "GROUP BY _arquivo"
    )


# ── a carga ──────────────────────────────────────────────────────────────


class Destino:
    """Uma tabela da bronze, com as colunas que ela já tem (lidas uma vez)."""

    def __init__(self, executor: Executor, catalogo: str, schema: str, nome: str) -> None:
        self.executor = executor
        self.catalogo = catalogo
        self.schema = schema
        self.nome = nome
        self.base = COLUNAS[nome]
        self._colunas: set[str] | None = None

    def garantir(self) -> None:
        self.executor.run(ddl_tabela(self.catalogo, self.schema, self.nome, self.base))
        self._colunas = None

    def colunas(self) -> set[str]:
        if self._colunas is None:
            self._colunas = {
                str(linha[0])
                for linha in self.executor.records(
                    sql_colunas(self.catalogo, self.schema, self.nome)
                )
            }
        return self._colunas

    def acrescentar(self, nomes: Iterable[str]) -> list[str]:
        """Cria as colunas que faltam e devolve as que criou."""
        novas = [c for c in nomes if c not in self.colunas()]
        for nova in novas:
            self.executor.run(ddl_coluna(self.catalogo, self.schema, self.nome, nova))
            self.colunas().add(nova)
        return novas

    def inserir(self, linhas: Sequence[Mapping[str, str | None]]) -> int:
        if not linhas:
            return 0
        colunas = colunas_das_linhas(self.base, linhas)
        self.acrescentar(colunas)
        for comando in inserts(self.catalogo, self.schema, self.nome, colunas, linhas):
            self.executor.run(comando)
        return len(linhas)


def destinos(executor: Executor, catalogo: str, schema: str) -> dict[str, Destino]:
    return {nome: Destino(executor, catalogo, schema, nome) for nome in COLUNAS}


def gravar(
    alvos: Mapping[str, Destino],
    convertidos: Sequence[Convertido],
    carga_run_id: str,
    carregado_em: datetime,
) -> dict[str, int]:
    """Insere as linhas de um lote de arquivos e, depois, registra os arquivos.

    O registro vem por último: o que entrou antes de uma falha fica sem registro
    e é carregado de novo no run seguinte.
    """
    feito = {
        TABELA_LISTA: alvos[TABELA_LISTA].inserir(
            [linha for c in convertidos if c.tabela == TABELA_LISTA for linha in c.linhas]
        ),
        TABELA_DETALHE: alvos[TABELA_DETALHE].inserir(
            [
                linha
                for c in convertidos
                if c.tabela == TABELA_DETALHE
                for linha in c.linhas
            ]
        ),
        TABELA_SECOES: alvos[TABELA_SECOES].inserir(
            [linha for c in convertidos for linha in c.secoes]
        ),
    }
    feito[TABELA_ARQUIVOS] = alvos[TABELA_ARQUIVOS].inserir(
        [registro(c, carga_run_id, carregado_em) for c in convertidos]
    )
    return feito


# ── a conferência ────────────────────────────────────────────────────────


def conferir(
    registros: Iterable[Sequence[Any]],
    contagens: Mapping[str, Mapping[str, int]],
) -> tuple[list[str], list[str]]:
    """Compara, por arquivo, o que ``arquivos`` diz ter carregado com as linhas
    que estão nas tabelas.

    ``registros`` são ``(arquivo, entidade, linhas, linhas_secoes)``;
    ``contagens`` é ``tabela -> {_arquivo: linhas}``. Linha a menos é problema
    (algo se perdeu); a mais é aviso: o arquivo foi carregado de novo depois de
    uma queda, e a silver descarta a repetição. Linha de arquivo sem registro
    também é aviso: ele entra de novo no run seguinte.
    """
    problemas: list[str] = []
    avisos: list[str] = []
    registrados: dict[str, set[str]] = {}
    for arquivo, entidade, linhas, linhas_secoes in registros:
        arquivo = str(arquivo)
        esperadas = [
            (TABELA_LISTA if entidade == TABELA_LISTA else TABELA_DETALHE, linhas)
        ]
        if entidade == TABELA_DETALHE:
            esperadas.append((TABELA_SECOES, linhas_secoes))
        for nome, esperado in esperadas:
            registrados.setdefault(nome, set()).add(arquivo)
            n = int(esperado or 0)
            achado = contagens.get(nome, {}).get(arquivo, 0)
            if achado < n:
                problemas.append(f"{nome}: {arquivo} tem {achado} de {n} linhas")
            elif achado > n:
                avisos.append(f"{nome}: {arquivo} tem {achado} linhas, {n} esperadas")
    for nome, por_arquivo in contagens.items():
        soltos = set(por_arquivo) - registrados.get(nome, set())
        if soltos:
            avisos.append(
                f"{nome}: {len(soltos)} arquivo(s) com linhas e sem registro "
                f"(ex.: {sorted(soltos)[0]})"
            )
    return problemas, avisos
