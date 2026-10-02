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
"""

from __future__ import annotations

import io
import json
import logging
import math
import re
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Callable, Iterable

import pandas as pd

if TYPE_CHECKING:
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook

CONN_ID = "minio_datalakehouse"
BUCKET = "minc-datalakehouse"

CAMADA_RAW = "raw"
CAMADA_STAGING = "staging"
_EXTENSAO = {CAMADA_RAW: "json", CAMADA_STAGING: "parquet"}

# O run_id do Airflow carrega ':' e '+' (manual__2026-10-01T12:00:00+00:00),
# que viram escape em URL de S3 e atrapalham quem lista o bucket pelo console.
_CARACTERE_INSEGURO = re.compile(r"[^A-Za-z0-9._=-]")


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


def raw_para_staging_key(key_raw: str) -> str:
    """Key de staging equivalente a uma key de raw."""
    if not key_raw.startswith(f"{CAMADA_RAW}/") or not key_raw.endswith(".json"):
        raise ValueError(f"key de raw inválida: {key_raw}")
    miolo = key_raw[len(CAMADA_RAW) + 1 : -len(".json")]
    return f"{CAMADA_STAGING}/{miolo}.parquet"


def escolher_mais_recente(objetos: Iterable[dict[str, Any]], ext: str) -> str | None:
    """Key mais recente (por ``LastModified``) entre objetos de ``list_objects_v2``.

    Não dá para ordenar pela key: dentro do mesmo dia, ``manual__`` e
    ``scheduled__`` não saem em ordem cronológica.
    """
    candidatos = [o for o in objetos if o["Key"].endswith(f".{ext}")]
    if not candidatos:
        return None
    mais_recente: str = max(candidatos, key=lambda o: o["LastModified"])["Key"]
    return mais_recente


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
    if enriquecer is not None:
        registros = [{**r, **enriquecer(r)} for r in registros]

    df = pd.json_normalize(registros, sep="__") if registros else pd.DataFrame()
    df["dt_ingest"] = (dt_ingest or datetime.now(timezone.utc)).isoformat()
    df = df.astype(object).apply(lambda coluna: coluna.map(_como_texto))
    df = df.astype("string")

    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False, engine="pyarrow")
    return buffer.getvalue()


# ── MinIO ────────────────────────────────────────────────────────────────


def get_hook() -> S3Hook:
    """``S3Hook`` da conexão ``minio_datalakehouse``, com o bucket garantido."""
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook

    hook = S3Hook(aws_conn_id=CONN_ID)
    if not hook.check_for_bucket(BUCKET):
        hook.create_bucket(bucket_name=BUCKET)
    return hook


def _run_atual() -> tuple[str, datetime]:
    """``run_id`` e data de partição da execução corrente.

    Em run manual no Airflow 3 o ``logical_date`` pode vir vazio; aí vale o
    ``run_after``, que sempre existe.
    """
    from airflow.sdk import get_current_context

    contexto = get_current_context()
    dag_run = contexto["dag_run"]
    data = contexto.get("logical_date") or dag_run.run_after
    return dag_run.run_id, data


def gravar_raw(registros: list[dict[str, Any]], fonte: str, entidade: str) -> str:
    """Grava ``registros`` como JSON em ``raw/`` e devolve a key.

    Só a key deve trafegar por XCom -- a lista inteira estoura o IPC do
    Airflow 3 com alguns milhares de registros.
    """
    run_id, data = _run_atual()
    key = caminho(CAMADA_RAW, fonte, entidade, run_id, data)
    get_hook().load_string(
        string_data=json.dumps(registros, ensure_ascii=False, default=str),
        key=key,
        bucket_name=BUCKET,
        replace=True,
    )
    logging.info(
        "[datalakehouse] %d registros gravados em s3://%s/%s",
        len(registros),
        BUCKET,
        key,
    )
    return key


def ler_raw(key: str) -> list[dict[str, Any]]:
    registros: list[dict[str, Any]] = json.loads(
        get_hook().read_key(key=key, bucket_name=BUCKET)
    )
    return registros


def raw_para_staging(
    key_raw: str,
    enriquecer: Callable[[dict[str, Any]], dict[str, Any]] | None = None,
) -> str:
    """Converte o JSON de ``key_raw`` em Parquet no staging e devolve a key."""
    registros = ler_raw(key_raw)
    key_staging = raw_para_staging_key(key_raw)
    get_hook().load_bytes(
        bytes_data=registros_para_parquet(registros, enriquecer=enriquecer),
        key=key_staging,
        bucket_name=BUCKET,
        replace=True,
    )
    logging.info(
        "[datalakehouse] %d registros convertidos em s3://%s/%s",
        len(registros),
        BUCKET,
        key_staging,
    )
    return key_staging


def listar_objetos(prefixo_busca: str) -> list[dict[str, Any]]:
    """Objetos (``Key``, ``LastModified``...) sob ``prefixo_busca``."""
    paginador = get_hook().get_conn().get_paginator("list_objects_v2")
    objetos: list[dict[str, Any]] = []
    for pagina in paginador.paginate(Bucket=BUCKET, Prefix=prefixo_busca):
        objetos.extend(pagina.get("Contents", []))
    return objetos


def ler_parquet(key: str) -> pd.DataFrame:
    corpo = get_hook().get_key(key=key, bucket_name=BUCKET).get()["Body"].read()
    return pd.read_parquet(io.BytesIO(corpo))


def ler_staging_recente(fonte: str, entidade: str) -> pd.DataFrame:
    """Último Parquet de ``entidade`` no staging.

    É o que substitui a leitura da tabela Postgres entre DAGs encadeadas: a
    DAG-filha consome o que a DAG-mãe acabou de escrever.
    """
    key = escolher_mais_recente(
        listar_objetos(prefixo(CAMADA_STAGING, fonte, entidade)), "parquet"
    )
    if key is None:
        raise FileNotFoundError(
            f"nenhum Parquet em s3://{BUCKET}/"
            f"{prefixo(CAMADA_STAGING, fonte, entidade)} -- a DAG que produz "
            f"'{entidade}' já rodou?"
        )
    logging.info("[datalakehouse] Lendo s3://%s/%s", BUCKET, key)
    return ler_parquet(key)


def gravar_bytes(conteudo: bytes, key: str) -> str:
    """Grava um binário (anexo, por exemplo) em ``key`` e devolve a key."""
    get_hook().load_bytes(bytes_data=conteudo, key=key, bucket_name=BUCKET, replace=True)
    return key
