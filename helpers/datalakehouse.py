"""Porta única de escrita e leitura no datalakehouse do MinC (MinIO).

Dado de API não vai mais direto para o Postgres: cada extração grava o que
recebeu em ``raw/`` e uma cópia tabular em Parquet em ``staging/``, no bucket
``minc-datalakehouse``::

    raw/<fonte>/<entidade>/ano=AAAA/mes=MM/dia=DD/<run_id>.json
    staging/<fonte>/<entidade>/ano=AAAA/mes=MM/dia=DD/<run_id>.parquet

As duas camadas têm o mesmo caminho, só muda o prefixo e a extensão -- dado
um arquivo de staging, o raw de onde ele saiu é sempre recuperável.

As funções de caminho e de conversão são puras (não tocam Airflow nem rede)
para serem testáveis sem subir o ambiente; o ``S3Hook`` é importado só dentro
das funções que falam com o MinIO.

O encadeamento entre DAGs é por Asset, um por entidade: quem grava o staging
publica um evento com a key exata do arquivo no ``extra``, e quem consome lê
essa key do evento que o disparou. O ``extra`` carrega a *cadeia* inteira
(``entidade -> key``) desde o programa, para que um consumidor que precise de
várias entidades -- anexos, relatórios e planos -- leia todas do mesmo run.

Fonte que grava muitos arquivos por run (SICONFI: um por página da API) usa
``prefixo_do_run``/``caminho_no_run`` e publica no ``extra`` a **pasta** do run,
não a key de um arquivo: quem consome lista a pasta. E cada pasta só tem o que
aquele run processou, então o consumidor precisa ler todos os eventos recebidos
(``eventos_em_ordem``), não só o mais novo.

O proxy na frente do MinIO recusa arquivo acima de 1 MiB. ``gravar_raw``,
``raw_para_staging`` e ``gravar_bytes_em_partes`` gravam o que passa do limite
como partes ``<nome>_pNNdeMM.<ext>``, e ``ler_raw``, ``ler_parquet`` e
``ler_bytes`` juntam: quem chama continua usando a key do arquivo. Um arquivo
existe inteiro ou em partes completas, nunca as duas coisas (ADR 0011, adendo).
"""

from __future__ import annotations

import functools
import io
import json
import logging
import math
import re
from datetime import datetime, timezone
from pathlib import PurePosixPath
from typing import TYPE_CHECKING, Any, Callable, Iterable, Mapping, Sequence, TypeVar

import pandas as pd

if TYPE_CHECKING:
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook
    from airflow.sdk import Asset

CONN_ID = "minio_datalakehouse"
BUCKET = "minc-datalakehouse"

CAMADA_RAW = "raw"
CAMADA_STAGING = "staging"
_EXTENSAO = {CAMADA_RAW: "json", CAMADA_STAGING: "parquet"}

# O run_id do Airflow carrega ':' e '+' (manual__2026-10-01T12:00:00+00:00),
# que viram escape em URL de S3 e atrapalham quem lista o bucket pelo console.
_CARACTERE_INSEGURO = re.compile(r"[^A-Za-z0-9._=-]")

# O proxy na frente do MinIO do servidor recusa corpo de requisição acima de
# 1 MiB (413 Request Entity Too Large; medido em 26/10/2026 entre uma página de
# 0,97 MB que passou e uma de 1,40 MB que não passou). Quem grava um JSON que
# pode passar disso divide o conteúdo em partes abaixo deste valor, com folga
# para o cabeçalho da requisição. O upload em partes do S3 (multipart) não
# resolve: cada parte tem no mínimo 5 MB.
LIMITE_OBJETO_BYTES = 900 * 1024  # 900 KiB
_PARTE_NO_NOME = re.compile(r"^(?P<base>.+)_p(?P<indice>\d+)de(?P<total>\d+)$")
# Variable com o limite em KiB para quem grava pelas funções de raw, staging e
# anexo deste módulo (o TransfereGov). Ausente, vale LIMITE_OBJETO_BYTES; 0
# desliga a divisão, para quando o proxy passar a aceitar arquivos maiores. A
# leitura continua juntando partes de qualquer jeito.
VARIABLE_LIMITE_KIB = "datalakehouse_limite_objeto_kib"

_T = TypeVar("_T")


# ── caminhos ─────────────────────────────────────────────────────────────


def sanitizar_run_id(run_id: str) -> str:
    return _CARACTERE_INSEGURO.sub("_", run_id)


def prefixo(camada: str, fonte: str, entidade: str) -> str:
    """Prefixo de uma entidade numa camada, terminado em barra."""
    if camada not in _EXTENSAO:
        raise ValueError(f"camada inválida: {camada}")
    return f"{camada}/{fonte}/{entidade}/"


def caminho(camada: str, fonte: str, entidade: str, run_id: str, data: datetime) -> str:
    """Key completa de um arquivo de ``camada`` para o run ``run_id``."""
    return (
        f"{prefixo(camada, fonte, entidade)}"
        f"ano={data:%Y}/mes={data:%m}/dia={data:%d}/"
        f"{sanitizar_run_id(run_id)}.{_EXTENSAO[camada]}"
    )


def caminho_particionado(
    camada: str,
    fonte: str,
    entidade: str,
    particao: Mapping[str, Any],
    run_id: str,
    extensao: str,
) -> str:
    """Key de um arquivo particionado pelo que identifica o dado, não pela data do run.

    Para fonte estática (um arquivo por ano de referência, como os censos do
    INEP) a partição útil é o ano do dado: recarregar um ano é achar o prefixo
    dele. Cada run grava a própria key, então o raw continua imutável.
    """
    particoes = "".join(f"{nome}={valor}/" for nome, valor in particao.items())
    return (
        f"{prefixo(camada, fonte, entidade)}{particoes}"
        f"{sanitizar_run_id(run_id)}.{extensao}"
    )


def prefixo_do_run(
    camada: str, fonte: str, entidade: str, run_id: str, data: datetime
) -> str:
    """Pasta de um run que grava muitos arquivos da mesma entidade.

    O ``caminho`` dá um arquivo por run; uma fonte paginada (SICONFI) grava
    milhares de páginas por run, e cada uma vira um arquivo dentro desta pasta.
    Quem consome o run lê a pasta inteira -- nunca "o arquivo mais recente".
    """
    return (
        f"{prefixo(camada, fonte, entidade)}"
        f"ano={data:%Y}/mes={data:%m}/dia={data:%d}/{sanitizar_run_id(run_id)}/"
    )


def caminho_no_run(
    camada: str, fonte: str, entidade: str, run_id: str, data: datetime, nome: str
) -> str:
    """Key de um arquivo ``nome`` dentro da pasta do run (ver ``prefixo_do_run``)."""
    return (
        f"{prefixo_do_run(camada, fonte, entidade, run_id, data)}"
        f"{sanitizar_run_id(nome)}.{_EXTENSAO[camada]}"
    )


def fatiar_por_tamanho(
    itens: Sequence[_T], tamanho: Callable[[_T], int], limite: int
) -> list[list[_T]]:
    """Divide ``itens``, na ordem, em grupos cuja soma de ``tamanho`` cabe em ``limite``.

    Sempre devolve ao menos um grupo (vazio, se não há itens). Um item que sozinho
    passa do limite não tem como ser gravado: falha aqui, com o tamanho, em vez de
    falhar no MinIO com 413.
    """
    grupos: list[list[_T]] = []
    atual: list[_T] = []
    soma = 0
    for item in itens:
        bytes_item = tamanho(item)
        if bytes_item > limite:
            raise ValueError(
                f"um item sozinho tem {bytes_item} bytes, acima do limite de "
                f"{limite} bytes por arquivo"
            )
        if atual and soma + bytes_item > limite:
            grupos.append(atual)
            atual, soma = [], 0
        atual.append(item)
        soma += bytes_item
    if atual or not grupos:
        grupos.append(atual)
    return grupos


def nome_da_parte(nome: str, indice: int, total: int) -> str:
    """``<nome>_p02de03``: a parte ``indice`` (a partir de 1) de ``total``.

    Os números têm a mesma largura, então as partes de um arquivo ficam em
    ordem também numa listagem por nome.
    """
    if not 1 <= indice <= total:
        raise ValueError(f"parte {indice} de {total} não existe")
    largura = max(2, len(str(total)))
    return f"{nome}_p{indice:0{largura}d}de{total:0{largura}d}"


def parte_do_nome(nome: str) -> tuple[str, int, int] | None:
    """O inverso de ``nome_da_parte``: ``(nome, indice, total)``, ou ``None``."""
    achado = _PARTE_NO_NOME.match(nome)
    if not achado:
        return None
    return achado["base"], int(achado["indice"]), int(achado["total"])


def key_da_parte(key: str, indice: int, total: int) -> str:
    """Key da parte ``indice`` de ``total`` de ``key``, na mesma pasta e com a
    mesma extensão: ``.../run.json`` vira ``.../run_p02de03.json``.

    A extensão fica no fim para quem filtra por ela (a extração dos anexos só
    abre ``.xlsx``, ``.xls``...) continuar reconhecendo a parte.
    """
    caminho_key = PurePosixPath(key)
    return str(
        caminho_key.with_name(
            nome_da_parte(caminho_key.stem, indice, total) + caminho_key.suffix
        )
    )


def parte_da_key(key: str) -> tuple[str, int, int] | None:
    """O inverso de ``key_da_parte``: ``(key do arquivo, indice, total)``, ou ``None``."""
    caminho_key = PurePosixPath(key)
    achado = parte_do_nome(caminho_key.stem)
    if achado is None:
        return None
    base, indice, total = achado
    return str(caminho_key.with_name(base + caminho_key.suffix)), indice, total


class PartesInconsistentes(ValueError):
    """Arquivo com parte faltando, inteiro e dividido ao mesmo tempo, ou dividido
    de dois jeitos. Lido assim, devolveria registros a menos ou repetidos."""


def agrupar_partes(keys: Iterable[str]) -> tuple[dict[str, list[str]], set[str]]:
    """Agrupa as keys de um bucket pelo arquivo a que pertencem.

    Devolve ``(completos, problemas)``. ``completos``: ``key do arquivo -> keys
    para ler``, em ordem -- a própria key, se o arquivo está inteiro, ou todas as
    partes de uma divisão. ``problemas``: arquivos com parte faltando, inteiros e
    divididos ao mesmo tempo, ou divididos de dois jeitos (``total`` diferente);
    é a mesma conferência do SICONFI (``siconfi_storage.contar_paginas``).
    """
    inteiros: set[str] = set()
    partes: dict[str, dict[int, dict[int, str]]] = {}  # arquivo -> total -> indice -> key
    for key in keys:
        achado = parte_da_key(key)
        if achado is None:
            inteiros.add(key)
            continue
        arquivo, indice, total = achado
        partes.setdefault(arquivo, {}).setdefault(total, {})[indice] = key

    problemas = {arquivo for arquivo, por_total in partes.items() if len(por_total) > 1}
    problemas |= inteiros & set(partes)
    completos = {key: [key] for key in inteiros - problemas}
    for arquivo, por_total in partes.items():
        if arquivo in problemas:
            continue
        ((total, vistas),) = por_total.items()
        if set(vistas) != set(range(1, total + 1)):
            problemas.add(arquivo)
            continue
        completos[arquivo] = [vistas[indice] for indice in range(1, total + 1)]
    return completos, problemas


def resolver_partes(key: str, keys: Iterable[str]) -> list[str]:
    """As keys que se leem, em ordem, para ter o arquivo ``key`` inteiro.

    ``keys`` é a listagem do bucket ao redor de ``key`` (pode trazer outros
    arquivos). Falha se o arquivo não existe ou se está inconsistente.
    """
    completos, problemas = agrupar_partes(
        k for k in keys if k == key or (parte_da_key(k) or ("",))[0] == key
    )
    if key in problemas:
        raise PartesInconsistentes(
            f"s3://{BUCKET}/{key}: parte faltando, ou inteiro e dividido ao mesmo "
            "tempo, ou dividido de dois jeitos"
        )
    if key not in completos:
        raise FileNotFoundError(f"s3://{BUCKET}/{key} não existe, nem dividido em partes")
    return completos[key]


def dividir_bytes(conteudo: bytes, limite: int | None) -> list[bytes]:
    """``conteudo`` em pedaços de até ``limite`` bytes; inteiro sem ``limite``.

    Para binário (planilha): os pedaços só fazem sentido juntos, na ordem.
    """
    if limite is None or len(conteudo) <= limite:
        return [conteudo]
    return [conteudo[i : i + limite] for i in range(0, len(conteudo), limite)]


def _json_bytes(valor: Any) -> bytes:
    return json.dumps(valor, ensure_ascii=False, default=str).encode()


def partes_do_json(registros: list[dict[str, Any]], limite: int | None) -> list[bytes]:
    """A lista ``registros`` em JSON: um arquivo, ou partes de até ``limite`` bytes.

    Cada parte é uma lista JSON válida com um trecho dos registros, na ordem;
    juntar as listas dá a original. Sem ``limite``, um arquivo só.
    """
    corpo = _json_bytes(registros)
    if limite is None or len(corpo) <= limite:
        return [corpo]
    # Cada registro ocupa o próprio JSON mais o separador ", "; os colchetes da
    # lista cabem nos 2 bytes de separador que o último registro não usa.
    grupos = fatiar_por_tamanho(
        registros, lambda registro: len(_json_bytes(registro)) + 2, limite
    )
    return [_json_bytes(grupo) for grupo in grupos]


def raw_para_staging_key(key_raw: str) -> str:
    """Key de staging equivalente a uma key de raw."""
    if not key_raw.startswith(f"{CAMADA_RAW}/") or not key_raw.endswith(".json"):
        raise ValueError(f"key de raw inválida: {key_raw}")
    miolo = key_raw[len(CAMADA_RAW) + 1 : -len(".json")]
    return f"{CAMADA_STAGING}/{miolo}.parquet"


# ── cadeia entre DAGs ────────────────────────────────────────────────────


def uri_asset(key_prefixo: str) -> str:
    """URI do Asset que representa tudo o que existe sob ``key_prefixo``.

    É estático por entidade, sem ``run_id``: a identidade do Asset é o que o
    scheduler usa para casar produtor e consumidor. A key do run vai no
    ``extra`` do evento.
    """
    return f"s3://{BUCKET}/{key_prefixo.rstrip('/')}"


def eventos_em_ordem(eventos: Iterable[Any]) -> list[Any]:
    """Eventos de Asset do mais antigo para o mais novo, pelo ``timestamp``.

    O Airflow não garante a ordem de ``triggering_asset_events``: os eventos
    voltam pela relação ``consumed_asset_events``, sem ``ORDER BY``. Quem lê
    mais de um evento -- a produtora rodou de novo antes da consumidora --
    precisa ordenar antes de escolher o mais novo ou de aplicar em sequência.
    """
    return sorted(eventos, key=lambda evento: evento.timestamp)


def cadeia_do_gatilho(
    extras: Iterable[Mapping[str, Any]], conf: Mapping[str, Any] | None = None
) -> dict[str, str]:
    """Cadeia ``entidade -> key`` do evento que disparou o run.

    ``extras`` são os ``extra`` dos eventos do Asset de gatilho, do mais
    antigo para o mais novo (ver ``eventos_em_ordem``); vale o mais novo. Sem evento (run manual), vale
    o ``conf`` do run -- é por ele que se reprocessa um arquivo específico.
    """
    ultimo: Mapping[str, Any] = {}
    for extra in extras:
        ultimo = extra
    origem = ultimo or conf or {}
    return {str(k): str(v) for k, v in origem.items()}


def key_da_cadeia(cadeia: Mapping[str, str], entidade: str) -> str:
    try:
        return cadeia[entidade]
    except KeyError:
        raise KeyError(
            f"a cadeia que disparou este run não traz '{entidade}' "
            f"(tem: {sorted(cadeia)}). Para rodar à mão, passe a key no conf: "
            f'{{"{entidade}": "staging/..."}}'
        ) from None


# ── conversão JSON → Parquet ─────────────────────────────────────────────


def _como_texto(valor: Any) -> str | None:
    """Toda coluna de staging é texto, como era no Postgres; a tipagem é do dbt.

    Listas e dicts viram JSON (o ``insert_data`` gravava o ``repr`` do Python,
    que nenhum parser lê de volta).
    """
    if valor is None:
        return None
    if isinstance(valor, float) and math.isnan(valor):
        return None
    if isinstance(valor, bool):
        return "true" if valor else "false"
    if isinstance(valor, (list, dict)):
        return json.dumps(valor, ensure_ascii=False, default=str)
    return str(valor)


def registros_para_parquet(
    registros: list[dict[str, Any]],
    enriquecer: Callable[[dict[str, Any]], dict[str, Any]] | None = None,
    dt_ingest: datetime | None = None,
) -> bytes:
    """Achata ``registros`` (``__`` entre níveis) e devolve o Parquet em bytes.

    ``enriquecer`` recebe cada registro e devolve os campos a acrescentar --
    é onde entra o que não vem da origem (território, política pública), para
    o raw continuar sendo exatamente o que a API respondeu.
    """
    return _df_para_parquet(_registros_para_df(registros, enriquecer, dt_ingest))


def _registros_para_df(
    registros: list[dict[str, Any]],
    enriquecer: Callable[[dict[str, Any]], dict[str, Any]] | None,
    dt_ingest: datetime | None,
) -> pd.DataFrame:
    if enriquecer is not None:
        registros = [{**r, **enriquecer(r)} for r in registros]

    df = pd.json_normalize(registros, sep="__") if registros else pd.DataFrame()
    # O insert_data baixava as chaves para minúsculo, e é com esses nomes que
    # as tabelas e a bronze existem: o backend do TransfereGov responde em
    # camelCase (tipoAnexo.auditLogin), e a coluna é tipoanexo__auditlogin.
    df.columns = [str(coluna).lower() for coluna in df.columns]
    df["dt_ingest"] = (dt_ingest or datetime.now(timezone.utc)).isoformat()
    df = df.astype(object).apply(lambda coluna: coluna.map(_como_texto))
    texto: pd.DataFrame = df.astype("string")
    return texto


def _df_para_parquet(df: pd.DataFrame) -> bytes:
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False, engine="pyarrow")
    return buffer.getvalue()


def partes_do_parquet(
    registros: list[dict[str, Any]],
    limite: int | None,
    enriquecer: Callable[[dict[str, Any]], dict[str, Any]] | None = None,
    dt_ingest: datetime | None = None,
) -> list[bytes]:
    """O Parquet de ``registros``: um arquivo, ou partes de até ``limite`` bytes.

    As partes são fatias de linhas do mesmo DataFrame, então todas têm as mesmas
    colunas, na mesma ordem, e o mesmo ``dt_ingest``: concatenadas, dão o Parquet
    inteiro. O tamanho de um Parquet não é proporcional ao número de linhas (a
    compressão varia), então as fatias começam pelo tamanho médio e dobram de
    quantidade até todas caberem.
    """
    df = _registros_para_df(registros, enriquecer, dt_ingest)
    inteiro = _df_para_parquet(df)
    if limite is None or len(inteiro) <= limite:
        return [inteiro]
    fatias = math.ceil(len(inteiro) / limite)
    while True:
        fatias = min(fatias, len(df))
        tamanho = math.ceil(len(df) / fatias)
        partes = [
            _df_para_parquet(df.iloc[inicio : inicio + tamanho].reset_index(drop=True))
            for inicio in range(0, len(df), tamanho)
        ]
        if all(len(parte) <= limite for parte in partes):
            return partes
        if tamanho == 1:
            raise ValueError(
                f"uma linha sozinha passa de {limite} bytes em Parquet: não há "
                "como gravá-la abaixo do limite"
            )
        fatias *= 2


def parquet_para_registros(df: pd.DataFrame, chave: list[str]) -> list[dict[str, Any]]:
    """Linhas de um Parquet de staging prontas para ``insert_data`` com upsert.

    ``pd.NA`` vira ``None`` (o psycopg2 não sabe adaptar ``NA``), e linhas
    com a mesma ``chave`` ficam só na última ocorrência: duas no mesmo
    INSERT fariam o ``ON CONFLICT DO UPDATE`` recusar o lote inteiro.
    """
    if df.empty:
        return []
    unicas = df.drop_duplicates(subset=chave, keep="last")
    if len(unicas) < len(df):
        logging.warning(
            "[datalakehouse] %d linhas com %s repetido -- fica a última",
            len(df) - len(unicas),
            chave,
        )
    registros: list[dict[str, Any]] = (
        unicas.astype(object).where(unicas.notna(), None).to_dict(orient="records")  # type: ignore[assignment]
    )
    return registros


# ── MinIO ────────────────────────────────────────────────────────────────


@functools.cache
def get_hook() -> S3Hook:
    """``S3Hook`` da conexão ``minio_datalakehouse``, com o bucket garantido.

    Em cache: sem isso, cada leitura e cada escrita pagava um HeadBucket --
    3000 a mais numa rodada da ``download_anexos_transferegov_dag``.
    """
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook

    hook = S3Hook(aws_conn_id=CONN_ID)
    if not hook.check_for_bucket(BUCKET):
        hook.create_bucket(bucket_name=BUCKET)
    return hook


def run_atual() -> tuple[str, datetime]:
    """``run_id`` e data de partição da execução corrente.

    Em run manual no Airflow 3 o ``logical_date`` pode vir vazio; aí vale o
    ``run_after``, que sempre existe.
    """
    from airflow.sdk import get_current_context

    contexto = get_current_context()
    dag_run = contexto["dag_run"]
    data = contexto.get("logical_date") or dag_run.run_after
    return dag_run.run_id, data


@functools.cache
def limite_objeto_bytes() -> int | None:
    """Limite por arquivo de quem grava raw, staging e anexo por este módulo.

    Lido da Variable ``VARIABLE_LIMITE_KIB`` uma vez por processo; ausente, vale
    ``LIMITE_OBJETO_BYTES``. ``None`` (Variable em 0) desliga a divisão.
    """
    from airflow.sdk import Variable

    kib = int(Variable.get(VARIABLE_LIMITE_KIB, default=LIMITE_OBJETO_BYTES // 1024))
    return kib * 1024 if kib > 0 else None


def _prefixo_das_formas(key: str) -> str:
    """Prefixo que lista ``key`` e as partes dela (e talvez outros arquivos,
    que ``resolver_partes`` e ``_gravar_partes`` descartam)."""
    return str(PurePosixPath(key).with_suffix(""))


def _gravar_partes(partes: Sequence[bytes], key: str) -> list[str]:
    """Grava o arquivo ``key``: inteiro, se ``partes`` tem uma só, ou como
    ``key_da_parte(key, i, total)``. Devolve as keys gravadas.

    Mesma regra do SICONFI (``siconfi_storage.gravar_envelope``): só depois de
    gravar tudo, apaga a outra forma do mesmo arquivo -- o inteiro de uma
    tentativa anterior, ou partes de uma divisão com outro total. Se a task cair
    antes, fica a forma antiga; nunca as duas, que juntas repetiriam registros.
    """
    total = len(partes)
    keys = (
        [key]
        if total == 1
        else [key_da_parte(key, i, total) for i in range(1, total + 1)]
    )
    hook = get_hook()
    for key_parte, corpo in zip(keys, partes):
        hook.load_bytes(bytes_data=corpo, key=key_parte, bucket_name=BUCKET, replace=True)
    for objeto in listar_objetos(_prefixo_das_formas(key)):
        outra = objeto["Key"]
        if outra in keys:
            continue
        if outra == key or (parte_da_key(outra) or ("",))[0] == key:
            apagar_objeto(outra)
    return keys


def partes_do_objeto(key: str) -> list[str]:
    """As keys que compõem ``key`` no bucket, em ordem (ver ``resolver_partes``)."""
    return resolver_partes(
        key, (objeto["Key"] for objeto in listar_objetos(_prefixo_das_formas(key)))
    )


def _ler_corpo(key: str) -> bytes:
    corpo: bytes = get_hook().get_key(key=key, bucket_name=BUCKET).get()["Body"].read()
    return corpo


def gravar_raw(registros: list[dict[str, Any]], fonte: str, entidade: str) -> str:
    """Grava ``registros`` como JSON em ``raw/`` e devolve a key.

    Acima do limite do proxy (``limite_objeto_bytes``) o JSON vira partes, cada
    uma uma lista com um trecho dos registros; a key devolvida é a do arquivo,
    e ``ler_raw`` junta as partes. Só a key deve trafegar por XCom -- a lista
    inteira estoura o IPC do Airflow 3 com alguns milhares de registros.
    """
    run_id, data = run_atual()
    key = caminho(CAMADA_RAW, fonte, entidade, run_id, data)
    keys = _gravar_partes(partes_do_json(registros, limite_objeto_bytes()), key)
    logging.info(
        "[datalakehouse] %d registros gravados em s3://%s/%s (%d arquivo(s))",
        len(registros),
        BUCKET,
        key,
        len(keys),
    )
    return key


def ler_json(key: str) -> Any:
    """Conteúdo de um JSON do bucket, com os tipos como foram gravados."""
    return json.loads(get_hook().read_key(key=key, bucket_name=BUCKET))


def ler_raw(key: str) -> list[dict[str, Any]]:
    """Os registros de um raw gravado por ``gravar_raw``, inteiro ou em partes."""
    registros: list[dict[str, Any]] = []
    for key_parte in partes_do_objeto(key):
        registros.extend(json.loads(_ler_corpo(key_parte)))
    return registros


def raw_para_staging(
    key_raw: str,
    enriquecer: Callable[[dict[str, Any]], dict[str, Any]] | None = None,
) -> str:
    """Converte o JSON de ``key_raw`` em Parquet no staging e devolve a key.

    Acima do limite do proxy o Parquet vira partes com as mesmas colunas;
    ``ler_parquet`` junta.
    """
    registros = ler_raw(key_raw)
    key_staging = raw_para_staging_key(key_raw)
    keys = _gravar_partes(
        partes_do_parquet(registros, limite_objeto_bytes(), enriquecer=enriquecer),
        key_staging,
    )
    logging.info(
        "[datalakehouse] %d registros convertidos em s3://%s/%s (%d arquivo(s))",
        len(registros),
        BUCKET,
        key_staging,
        len(keys),
    )
    return key_staging


def existe_objeto(prefixo_busca: str) -> bool:
    """Se há ao menos um objeto sob ``prefixo_busca``, sem listar o prefixo inteiro.

    Uma pasta de run da MSC pode ter centenas de milhares de páginas; para saber
    se está vazia basta pedir uma chave.
    """
    resposta = (
        get_hook()
        .get_conn()
        .list_objects_v2(Bucket=BUCKET, Prefix=prefixo_busca, MaxKeys=1)
    )
    return int(resposta.get("KeyCount", 0)) > 0


def listar_objetos(prefixo_busca: str) -> list[dict[str, Any]]:
    """Objetos (``Key``, ``LastModified``...) sob ``prefixo_busca``."""
    paginador = get_hook().get_conn().get_paginator("list_objects_v2")
    objetos: list[dict[str, Any]] = []
    for pagina in paginador.paginate(Bucket=BUCKET, Prefix=prefixo_busca):
        objetos.extend(pagina.get("Contents", []))
    return objetos


def ler_parquet(key: str) -> pd.DataFrame:
    """Um Parquet de staging, inteiro ou juntando as partes na ordem."""
    quadros = [
        pd.read_parquet(io.BytesIO(_ler_corpo(key_parte)))
        for key_parte in partes_do_objeto(key)
    ]
    if len(quadros) == 1:
        return quadros[0]
    return pd.concat(quadros, ignore_index=True)


def apagar_objeto(key: str) -> None:
    """Apaga ``key`` do bucket. Não falha se ela não existe: o S3 responde igual."""
    get_hook().get_conn().delete_object(Bucket=BUCKET, Key=key)


def gravar_bytes(conteudo: bytes, key: str) -> str:
    """Grava um binário em ``key`` e devolve a key, sem dividir.

    Para quem já grava abaixo do limite (o SICONFI divide as páginas antes de
    chamar). Binário que pode passar do limite vai por ``gravar_bytes_em_partes``.
    """
    get_hook().load_bytes(bytes_data=conteudo, key=key, bucket_name=BUCKET, replace=True)
    return key


def gravar_bytes_em_partes(conteudo: bytes, key: str) -> str:
    """Grava um binário (planilha anexa) em ``key``, dividido em partes se passar
    do limite do proxy, e devolve a key do arquivo. ``ler_bytes`` junta."""
    _gravar_partes(dividir_bytes(conteudo, limite_objeto_bytes()), key)
    return key


def ler_bytes(key: str) -> bytes:
    """Um binário gravado por ``gravar_bytes_em_partes``, inteiro ou em partes."""
    return b"".join(_ler_corpo(key_parte) for key_parte in partes_do_objeto(key))


def gravar_arquivo(caminho_local: str, key: str) -> str:
    """Envia um arquivo local para ``key`` sem carregá-lo em memória.

    Para o que não cabe com folga num ``bytes`` -- o ZIP de 2025 do Censo
    Escolar tem 537 MB.
    """
    get_hook().load_file(
        filename=caminho_local, key=key, bucket_name=BUCKET, replace=True
    )
    logging.info("[datalakehouse] %s enviado para s3://%s/%s", caminho_local, BUCKET, key)
    return key


def baixar_arquivo(key: str, caminho_local: str) -> str:
    """Baixa ``key`` para ``caminho_local`` em streaming e devolve o caminho."""
    get_hook().get_key(key=key, bucket_name=BUCKET).download_file(caminho_local)
    return caminho_local


# ── Assets ───────────────────────────────────────────────────────────────


def asset(key_prefixo: str) -> Asset:
    """Asset de um prefixo do bucket (ver ``uri_asset``)."""
    from airflow.sdk import Asset

    return Asset(uri=uri_asset(key_prefixo))


def asset_staging(fonte: str, entidade: str) -> Asset:
    return asset(prefixo(CAMADA_STAGING, fonte, entidade))


def ler_cadeia(asset_gatilho: Asset) -> dict[str, str]:
    """Cadeia ``entidade -> key`` do evento de ``asset_gatilho`` que disparou o run."""
    from airflow.sdk import get_current_context

    contexto = get_current_context()
    eventos = eventos_em_ordem(contexto["triggering_asset_events"].get(asset_gatilho, []))
    return cadeia_do_gatilho(
        (evento.extra for evento in eventos), contexto["dag_run"].conf
    )


def publicar_cadeia(asset_saida: Asset, cadeia: Mapping[str, str]) -> None:
    """Põe ``cadeia`` no ``extra`` do evento que a task vai emitir em ``asset_saida``.

    A task precisa declarar ``asset_saida`` em ``outlets``; o evento só sai se
    ela terminar com sucesso.
    """
    from airflow.sdk import get_current_context

    get_current_context()["outlet_events"][asset_saida].extra.update(cadeia)
    logging.info("[datalakehouse] Evento em %s: %s", asset_saida.uri, dict(cadeia))
