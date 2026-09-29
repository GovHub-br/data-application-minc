"""Cliente resiliente para a API pública SICONFI e regras de montagem das consultas.

O limite divulgado pelo Tesouro é de cerca de uma requisição por segundo, e não
se sabe se ele vale por IP nem como a API reage quando é ultrapassado. O
``SiconfiRateLimiter`` usa PostgreSQL como coordenador para que o intervalo
entre chamadas e o número de chamadas simultâneas valham entre tasks, workers
e DAGs diferentes.

A API não reclama de parâmetro errado: responde HTTP 200 com ``items: []``.
Por isso toda consulta passa por ``validate_params`` antes de sair, e as
regras abaixo — verificadas com chamadas reais em 29/09/2026 — valem mais que
a documentação Swagger, que está parcialmente desatualizada.
"""

from __future__ import annotations

import hashlib
import json
import logging
import random
import re
import time
import unicodedata
import zlib
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable, Iterator, Mapping

import httpx
import psycopg2
from psycopg2 import sql

logger = logging.getLogger(__name__)

BASE_URL = "https://apidatalake.tesouro.gov.br/ords/siconfi/tt"
RATE_LIMIT_NAME = "siconfi_public_api"
CONTROL_SCHEMA = "siconfi_control"
RETRYABLE_STATUS = {408, 429, 500, 502, 503, 504}
# Chave dos advisory locks que limitam as chamadas simultâneas.
_SLOT_LOCK_CLASS = zlib.crc32(RATE_LIMIT_NAME.encode()) & 0x7FFFFFFF

# --- Domínio verificado da API -------------------------------------------------

REFERENCE_ENDPOINTS = ("anexos-relatorios", "entes")
MSC_ENDPOINTS = ("msc_orcamentaria", "msc_patrimonial", "msc_controle")
FACT_ENDPOINTS = ("rreo", "rgf", "dca", *MSC_ENDPOINTS)
# Chave em ``siconfi_extracao_config.endpoints`` → caminho na API.
ENDPOINT_POR_CHAVE = {
    "anexos_relatorios": "anexos-relatorios",
    "entes": "entes",
    "extrato_entregas": "extrato_entregas",
    **{endpoint: endpoint for endpoint in FACT_ENDPOINTS},
}
ESFERAS = ("M", "E", "D", "U")
PODERES = ("E", "L", "J", "M", "D")
TIPOS_VALOR = ("beginning_balance", "ending_balance", "period_change")
TIPOS_MATRIZ = ("MSCC", "MSCE")
# Primeiro exercício com dados: 2014 do RREO e 2018 da MSC voltam vazios.
ANO_MINIMO = {
    "extrato_entregas": 2013,
    "dca": 2013,
    "rreo": 2015,
    "rgf": 2015,
    **{endpoint: 2019 for endpoint in MSC_ENDPOINTS},
}
CLASSES_MSC = {
    "msc_patrimonial": (1, 2, 3, 4),
    "msc_orcamentaria": (5, 6),
    "msc_controle": (7, 8),
}
# A matriz de encerramento só responde com dezembro: com 13, ou com o
# ``periodo=1`` que o extrato informa, volta vazia.
_MSCE_MONTH = 12
# Fora do Executivo, o RGF só tem os anexos 01, 05 e 06.
_ANEXOS_RGF_OUTROS_PODERES = frozenset({"01", "05", "06"})

_PARAMS_OBRIGATORIOS: dict[str, tuple[str, ...]] = {
    "anexos-relatorios": (),
    "entes": (),
    "extrato_entregas": ("id_ente", "an_referencia"),
    "rreo": ("an_exercicio", "nr_periodo", "co_tipo_demonstrativo", "id_ente"),
    "rgf": (
        "an_exercicio",
        "in_periodicidade",
        "nr_periodo",
        "co_tipo_demonstrativo",
        "co_poder",
        "id_ente",
    ),
    "dca": ("an_exercicio", "id_ente"),
    **{
        endpoint: (
            "id_ente",
            "an_referencia",
            "me_referencia",
            "co_tipo_matriz",
            "classe_conta",
            "id_tv",
        )
        for endpoint in MSC_ENDPOINTS
    },
}
_PARAMS_OPCIONAIS: dict[str, tuple[str, ...]] = {
    "rreo": ("no_anexo", "co_esfera"),
    "rgf": ("no_anexo", "co_esfera"),
    "dca": ("no_anexo",),
}


class SiconfiRequestError(RuntimeError):
    """Erro HTTP da SICONFI com contexto suficiente para a fila de controle."""

    def __init__(
        self, status_code: int | None, detail: str, retry_after: str | None = None
    ) -> None:
        super().__init__(detail)
        self.status_code = status_code
        self.retry_after = retry_after


class SiconfiRetryableError(SiconfiRequestError):
    """Falha transitória: a unidade de trabalho deve voltar para a fila."""


class SiconfiPermanentError(SiconfiRequestError):
    """Falha de contrato/parâmetro: repetir não muda o resultado."""


class SiconfiParametroInvalido(ValueError):
    """Consulta que a API responderia com lista vazia em vez de erro."""


@dataclass(frozen=True)
class SiconfiPage:
    endpoint: str
    params: dict[str, Any]
    offset: int
    items: list[dict[str, Any]]
    has_more: bool
    headers: dict[str, str]
    payload: dict[str, Any]


def request_hash(endpoint: str, params: Mapping[str, Any]) -> str:
    """Identificador determinístico da consulta, sem depender da ordem do dict."""
    canonical = json.dumps(
        {"endpoint": endpoint, "params": dict(params)},
        ensure_ascii=False,
        sort_keys=True,
        default=str,
        separators=(",", ":"),
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def year_key(endpoint: str) -> str:
    return "an_exercicio" if endpoint in ("rreo", "rgf", "dca") else "an_referencia"


def validate_params(endpoint: str, params: Mapping[str, Any]) -> None:
    """Recusa a consulta que a API responderia com vazio em vez de erro."""
    if endpoint not in _PARAMS_OBRIGATORIOS:
        raise SiconfiParametroInvalido(f"endpoint SICONFI desconhecido: {endpoint}")
    obrigatorios = _PARAMS_OBRIGATORIOS[endpoint]
    presentes = {k for k, v in params.items() if v is not None}
    faltando = [p for p in obrigatorios if p not in presentes]
    sobrando = sorted(
        presentes - set(obrigatorios) - set(_PARAMS_OPCIONAIS.get(endpoint, ()))
    )
    problemas = []
    if faltando:
        problemas.append(f"faltam {faltando}")
    if sobrando:
        problemas.append(f"parâmetros não aceitos {sobrando}")
    if not problemas:
        problemas = _problemas_de_valor(endpoint, params)
    if problemas:
        raise SiconfiParametroInvalido(
            f"/{endpoint} {dict(params)}: " + "; ".join(problemas)
        )


class _Problemas(list[str]):
    """Problemas de valor de uma consulta, acumulados para uma mensagem só."""

    def __init__(self, params: Mapping[str, Any]) -> None:
        super().__init__()
        self.params = params

    def inteiro(self, nome: str) -> int | None:
        valor = self.params[nome]
        if isinstance(valor, bool) or not isinstance(valor, int):
            self.append(f"{nome}={valor!r} não é inteiro")
            return None
        return valor

    def faixa(self, nome: str, minimo: int, maximo: int, rotulo: str) -> int | None:
        valor = self.inteiro(nome)
        if valor is not None and not minimo <= valor <= maximo:
            self.append(f"{nome}={valor} fora {rotulo}")
        return valor


def _problemas_rreo(p: _Problemas, endpoint: str) -> None:
    p.faixa("nr_periodo", 1, 6, "dos bimestres 1–6")
    if p.params["co_tipo_demonstrativo"] not in ("RREO", "RREO Simplificado"):
        p.append(f"co_tipo_demonstrativo={p.params['co_tipo_demonstrativo']!r}")


def _problemas_rgf(p: _Problemas, endpoint: str) -> None:
    # Quadrimestral só com 'RGF', semestral só com 'RGF Simplificado': a
    # combinação trocada volta vazia.
    periodicidade = p.params["in_periodicidade"]
    esperado = {"Q": ("RGF", 3), "S": ("RGF Simplificado", 2)}.get(periodicidade)
    if esperado is None:
        p.append(f"in_periodicidade={periodicidade!r} não é Q nem S")
    else:
        tipo, ultimo = esperado
        if p.params["co_tipo_demonstrativo"] != tipo:
            p.append(
                f"in_periodicidade={periodicidade} exige co_tipo_demonstrativo={tipo!r}"
            )
        p.faixa("nr_periodo", 1, ultimo, f"de 1–{ultimo}")
    if p.params["co_poder"] not in PODERES:
        p.append(f"co_poder={p.params['co_poder']!r} não é um de {PODERES}")


def _problemas_msc(p: _Problemas, endpoint: str) -> None:
    mes = p.faixa("me_referencia", 1, 12, "de 1–12")
    matriz = p.params["co_tipo_matriz"]
    if matriz not in TIPOS_MATRIZ:
        p.append(f"co_tipo_matriz={matriz!r}")
    elif matriz == "MSCE" and mes != _MSCE_MONTH:
        p.append("MSCE só existe com me_referencia=12")
    if p.params["classe_conta"] not in CLASSES_MSC[endpoint]:
        p.append(
            f"classe_conta={p.params['classe_conta']!r} não existe em /{endpoint} "
            f"(só {list(CLASSES_MSC[endpoint])})"
        )
    if p.params["id_tv"] not in TIPOS_VALOR:
        p.append(f"id_tv={p.params['id_tv']!r}")


_PROBLEMAS_POR_ENDPOINT = {
    "rreo": _problemas_rreo,
    "rgf": _problemas_rgf,
    **{endpoint: _problemas_msc for endpoint in MSC_ENDPOINTS},
}


def _problemas_de_valor(endpoint: str, params: Mapping[str, Any]) -> list[str]:
    if endpoint in REFERENCE_ENDPOINTS:
        return []
    p = _Problemas(params)
    ente = p.inteiro("id_ente")
    if ente is not None and ente <= 0:
        p.append(f"id_ente={ente} inválido")
    ano = p.inteiro(year_key(endpoint))
    if ano is not None and ano < ANO_MINIMO[endpoint]:
        p.append(f"{year_key(endpoint)}={ano} anterior a {ANO_MINIMO[endpoint]}")
    if endpoint in _PROBLEMAS_POR_ENDPOINT:
        _PROBLEMAS_POR_ENDPOINT[endpoint](p, endpoint)
    return list(p)


# --- HTTP ----------------------------------------------------------------------


class SiconfiRateLimiter:
    """Intervalo entre chamadas e teto de chamadas simultâneas, via PostgreSQL.

    O intervalo é uma reserva de horário com lock transacional: a transação só
    segura o lock durante a reserva, e o ``sleep`` acontece fora dela. O teto de
    simultâneas são ``max_concurrency`` advisory locks de sessão — se a conexão
    cair, o Postgres solta a vaga sozinho.

    A conexão é reaproveitada entre reservas: a 1 req/s, abrir uma conexão nova
    por requisição custaria milhares de handshakes por hora contra um Postgres
    que normalmente está em outra infra.
    """

    def __init__(
        self,
        conn_str: str,
        schema: str = CONTROL_SCHEMA,
        interval_s: float = 1.02,
        max_concurrency: int = 2,
    ) -> None:
        self.conn_str = conn_str
        self.schema = schema
        self.interval_s = interval_s
        self.max_concurrency = max_concurrency
        self._conn: Any | None = None

    def _connection(self) -> Any:
        if self._conn is None or self._conn.closed:
            conn = psycopg2.connect(self.conn_str)
            with conn:
                with conn.cursor() as cur:
                    cur.execute(
                        sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(
                            sql.Identifier(self.schema)
                        )
                    )
                    cur.execute(sql.SQL("""
                            CREATE TABLE IF NOT EXISTS {}.api_rate_limit (
                                limiter_name TEXT PRIMARY KEY,
                                next_allowed_at TIMESTAMPTZ NOT NULL
                            )
                            """).format(sql.Identifier(self.schema)))
                    cur.execute(
                        sql.SQL("""
                            INSERT INTO {}.api_rate_limit (limiter_name, next_allowed_at)
                            VALUES (%s, now())
                            ON CONFLICT (limiter_name) DO NOTHING
                            """).format(sql.Identifier(self.schema)),
                        (RATE_LIMIT_NAME,),
                    )
            self._conn = conn
        return self._conn

    def close(self) -> None:
        if self._conn is not None and not self._conn.closed:
            self._conn.close()
        self._conn = None

    @contextmanager
    def slot(self) -> Iterator[None]:
        """Ocupa uma das ``max_concurrency`` vagas de chamada em andamento."""
        vaga = self._acquire_slot()
        try:
            yield
        finally:
            self._release_slot(vaga)

    def _acquire_slot(self) -> int:
        while True:
            try:
                conn = self._connection()
                with conn, conn.cursor() as cur:
                    for vaga in range(self.max_concurrency):
                        cur.execute(
                            "SELECT pg_try_advisory_lock(%s, %s)",
                            (_SLOT_LOCK_CLASS, vaga),
                        )
                        if cur.fetchone()[0]:
                            return vaga
            except psycopg2.Error as exc:
                logger.warning("[siconfi] rate limiter: reconectando após %s", exc)
                self.close()
            time.sleep(0.25)

    def _release_slot(self, vaga: int) -> None:
        # Se a conexão caiu no meio, o Postgres já soltou a vaga.
        if self._conn is None or self._conn.closed:
            return
        try:
            with self._conn, self._conn.cursor() as cur:
                cur.execute("SELECT pg_advisory_unlock(%s, %s)", (_SLOT_LOCK_CLASS, vaga))
        except psycopg2.Error:
            self.close()

    def _reserve_slot(self) -> tuple[datetime, datetime]:
        interval = timedelta(seconds=self.interval_s)
        conn = self._connection()
        with conn:
            with conn.cursor() as cur:
                now = datetime.now(timezone.utc)
                cur.execute(
                    sql.SQL(
                        "SELECT next_allowed_at FROM {}.api_rate_limit "
                        "WHERE limiter_name = %s FOR UPDATE"
                    ).format(sql.Identifier(self.schema)),
                    (RATE_LIMIT_NAME,),
                )
                scheduled = max(now, cur.fetchone()[0])
                cur.execute(
                    sql.SQL(
                        "UPDATE {}.api_rate_limit SET next_allowed_at = %s "
                        "WHERE limiter_name = %s"
                    ).format(sql.Identifier(self.schema)),
                    (scheduled + interval, RATE_LIMIT_NAME),
                )
        return now, scheduled

    def reserve(self) -> None:
        # Uma conexão reaproveitada pode ter morrido entre reservas (queda de
        # rede, reinício do Postgres). Reconectar e repetir é seguro: a reserva
        # perdida só devolve um slot de tempo à fila.
        try:
            now, scheduled = self._reserve_slot()
        except psycopg2.Error as exc:
            logger.warning("[siconfi] rate limiter: reconectando após %s", exc)
            self.close()
            now, scheduled = self._reserve_slot()
        wait_s = max(0.0, (scheduled - now).total_seconds())
        if wait_s:
            logger.debug("[siconfi] rate limiter: aguardando %.2fs", wait_s)
            time.sleep(wait_s)


class SiconfiClient:
    """GET paginado, validado, rate-limited e com retry classificado da SICONFI."""

    def __init__(
        self,
        conn_str: str,
        *,
        timeout_s: float = 90.0,
        max_retries: int = 4,
        page_limit: int = 5000,
        requests_per_second: float = 1.0,
        max_concurrency: int = 2,
        rate_limiter: Any | None = None,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        self.timeout_s = timeout_s
        self.max_retries = max_retries
        self.page_limit = page_limit
        # 1,02 s entre chamadas a 1 req/s: a folga que o código legado usava.
        self.rate_limiter = rate_limiter or SiconfiRateLimiter(
            conn_str,
            interval_s=1.02 / requests_per_second,
            max_concurrency=max_concurrency,
        )
        self.client = httpx.Client(
            base_url=BASE_URL,
            headers={"Accept": "application/json"},
            timeout=httpx.Timeout(timeout_s),
            follow_redirects=True,
            transport=transport,
        )

    @classmethod
    def from_config(cls, conn_str: str, config: Mapping[str, Any]) -> SiconfiClient:
        glob = config["global"]
        return cls(
            conn_str,
            max_retries=int(glob["retentativas"]),
            page_limit=int(glob["tamanho_pagina"]),
            requests_per_second=float(glob["requisicoes_por_segundo"]),
            max_concurrency=int(glob["max_paralelismo"]),
        )

    def close(self) -> None:
        self.client.close()
        self.rate_limiter.close()

    def _get(
        self, endpoint: str, params: dict[str, Any]
    ) -> tuple[dict[str, Any], dict[str, str]]:
        path = f"/{endpoint.lstrip('/')}"
        for attempt in range(1, self.max_retries + 1):
            try:
                return self._attempt(path, params)
            except SiconfiRetryableError as exc:
                if attempt == self.max_retries:
                    raise
                self._backoff(attempt, exc.retry_after)
        raise AssertionError("loop de retry terminou inesperadamente")

    def _attempt(
        self, path: str, params: dict[str, Any]
    ) -> tuple[dict[str, Any], dict[str, str]]:
        """Uma chamada; falha transitória vira ``SiconfiRetryableError``."""
        with self.rate_limiter.slot():
            self.rate_limiter.reserve()
            try:
                response = self.client.get(path, params=params)
            except httpx.HTTPError as exc:
                raise SiconfiRetryableError(None, f"erro de rede: {exc}") from exc

        if response.status_code == 200:
            try:
                payload = response.json()
            except ValueError as exc:
                # Falha momentânea conhecida da API: resposta que não é JSON e
                # que funciona na tentativa seguinte.
                raise SiconfiRetryableError(200, "resposta 200 sem JSON válido") from exc
            if not isinstance(payload, dict):
                raise SiconfiPermanentError(200, "resposta não possui envelope JSON")
            return payload, {k.lower(): v for k, v in response.headers.items()}

        detail = response.text[:500]
        if response.status_code in RETRYABLE_STATUS:
            raise SiconfiRetryableError(
                response.status_code, detail, response.headers.get("Retry-After")
            )
        raise SiconfiPermanentError(response.status_code, detail)

    @staticmethod
    def _backoff(attempt: int, retry_after: str | None) -> None:
        try:
            base = float(retry_after) if retry_after else min(60.0, 2.0**attempt)
        except ValueError:
            base = min(60.0, 2.0**attempt)
        wait_s = base + random.uniform(0, min(1.0, base * 0.2))
        logger.warning("[siconfi] retry HTTP em %.2fs (tentativa %s)", wait_s, attempt)
        time.sleep(wait_s)

    def iter_pages(self, endpoint: str, params: dict[str, Any]) -> Iterator[SiconfiPage]:
        """Valida a consulta e itera o envelope ORDS usando ``hasMore`` e ``offset``.

        O tamanho real padrão do servidor já variou (a documentação cita 5 mil
        e a resposta de ``/entes`` já informou 6 mil), por isso o limite é
        enviado explicitamente e o avanço usa a quantidade realmente recebida.
        """
        base_params = {k: v for k, v in params.items() if v is not None}
        validate_params(endpoint, base_params)
        offset = 0
        while True:
            page_params = {**base_params, "offset": offset, "limit": self.page_limit}
            payload, headers = self._get(endpoint, page_params)
            raw_items = payload.get("items", [])
            if not isinstance(raw_items, list) or not all(
                isinstance(item, dict) for item in raw_items
            ):
                raise SiconfiPermanentError(200, "envelope sem lista 'items' de objetos")
            has_more = bool(payload.get("hasMore", False))
            yield SiconfiPage(
                endpoint=endpoint,
                params=base_params,
                offset=int(payload.get("offset", offset)),
                items=raw_items,
                has_more=has_more,
                headers=headers,
                payload=payload,
            )
            if not has_more:
                return
            if not raw_items:
                raise SiconfiPermanentError(200, "hasMore=true com página vazia")
            offset += len(raw_items)


# --- Recorte configurado -------------------------------------------------------


class PlanScope:
    """O que entra na fila e o que sai dela, a partir da configuração efetiva.

    A mesma regra vale no planejamento (``plan_units``) e no claim
    (``constraints``): estreitar a configuração depois que a fila já foi
    montada também para de buscar o que ficou de fora.

    ``config`` é o resultado de ``siconfi_config.carregar`` — validado e com
    ``ano_fim`` já resolvido. ``entes`` é o recorte de entes já aplicado
    (``None`` = todos), que vai para o claim como filtro de ``id_ente``.
    """

    def __init__(
        self, config: Mapping[str, Any], entes: Iterable[int] | None = None
    ) -> None:
        self.config = config
        self.entes = None if entes is None else frozenset(int(e) for e in entes)
        self._por_endpoint: dict[str, Mapping[str, Any]] = {
            ENDPOINT_POR_CHAVE[chave]: cfg for chave, cfg in config["endpoints"].items()
        }

    def cfg(self, endpoint: str) -> Mapping[str, Any]:
        return self._por_endpoint[endpoint]

    def enabled(self, endpoint: str) -> bool:
        return bool(self.cfg(endpoint)["ativo"])

    def years(self, endpoint: str) -> range:
        cfg = self.cfg(endpoint)
        return range(int(cfg["ano_inicio"]), int(cfg["ano_fim"]) + 1)

    def wants(self, endpoint: str, year: int) -> bool:
        return self.enabled(endpoint) and year in self.years(endpoint)

    def poderes(self, esfera: str) -> tuple[str, ...]:
        return tuple(self.cfg("rgf")["poderes_por_esfera"].get(esfera, ()))

    def constraints(self, endpoint: str) -> dict[str, frozenset]:
        """Valores permitidos por parâmetro da unidade de trabalho."""
        allowed: dict[str, frozenset | None] = {
            year_key(endpoint): frozenset(self.years(endpoint)),
            "id_ente": self.entes,
        }
        cfg = self.cfg(endpoint)
        if endpoint == "rreo":
            allowed["nr_periodo"] = frozenset(cfg["periodos"])
        if endpoint == "rgf":
            allowed["co_poder"] = frozenset(
                poder
                for esfera in self.config["global"]["esferas"]
                for poder in self.poderes(esfera)
            )
        if endpoint in ("rreo", "rgf", "dca") and cfg["no_anexo"] is not None:
            allowed["no_anexo"] = frozenset(cfg["no_anexo"]) | frozenset(
                nome_anexo_dca(a, 2013) for a in cfg["no_anexo"]
            )
        if endpoint in MSC_ENDPOINTS:
            allowed["me_referencia"] = frozenset(cfg["meses"])
            allowed["classe_conta"] = frozenset(cfg["classes"])
            allowed["id_tv"] = frozenset(cfg["id_tv"])
            allowed["co_tipo_matriz"] = frozenset(cfg["tipos_matriz"])
        return {key: values for key, values in allowed.items() if values is not None}

    def fingerprint(self) -> str:
        """Muda quando o recorte muda — e aí o planejamento relê o extrato."""
        endpoints = {
            chave: {
                k: v
                for k, v in cfg.items()
                if k not in ("recarga_horas", "rebusca_anos", "rebusca_dias")
            }
            for chave, cfg in self.config["endpoints"].items()
        }
        state = {
            "endpoints": endpoints,
            "esferas": sorted(self.config["global"]["esferas"]),
            "entes": sorted(self.entes) if self.entes is not None else None,
        }
        return hashlib.sha256(
            json.dumps(state, sort_keys=True, default=str).encode()
        ).hexdigest()[:16]


# --- Planejamento a partir do extrato de entregas ------------------------------
#
# O extrato identifica o demonstrativo pelo nome por extenso ("Relatório
# Resumido de Execução Orçamentária Simplificado", "Balanço Anual (DCA)", "MSC
# Encerramento"...), não pela sigla. As funções abaixo traduzem o extrato de um
# ente num ano nas consultas que os endpoints de fatos aceitam.

# (endpoint, params, revision_marker, esperado)
WorkUnit = tuple[str, dict[str, Any], str, bool]

# Instituição do extrato → poder no RGF, conferido na API: o Tribunal de Contas
# responde em ``co_poder=L``. A ordem importa: "Tribunal de Contas" antes de
# "Tribunal", "Ministério Público" antes de qualquer outro.
_PODER_POR_INSTITUICAO = (
    ("DEFENSORIA", "D"),
    ("MINISTERIO PUBLICO", "M"),
    ("TRIBUNAL DE CONTAS", "L"),
    ("TRIBUNAL", "J"),
    ("JUSTICA", "J"),
    ("ASSEMBLEIA", "L"),
    ("CAMARA", "L"),
    ("SENADO", "L"),
    ("PREFEITURA", "E"),
    ("GOVERNO", "E"),
    ("EXECUTIVO", "E"),
)


def _normalise_text(value: Any) -> str:
    text = unicodedata.normalize("NFKD", str(value or ""))
    return "".join(ch for ch in text if not unicodedata.combining(ch)).upper()


def classify_delivery(entregavel: Any) -> str | None:
    """Devolve ``rreo``, ``rgf``, ``dca``, ``msc`` ou ``None``."""
    text = _normalise_text(entregavel)
    if "RREO" in text or "RESUMIDO DE EXECUCAO ORCAMENTARIA" in text:
        return "rreo"
    if "RGF" in text or "GESTAO FISCAL" in text:
        return "rgf"
    if "DCA" in text or "QDCC" in text or "BALANCO ANUAL" in text:
        return "dca"
    if "MSC" in text:
        return "msc"
    return None


def poder_da_instituicao(instituicao: Any) -> str | None:
    """Poder do RGF (E, L, J, M, D) de quem entregou, ou ``None`` se desconhecido."""
    text = _normalise_text(instituicao)
    for keyword, poder in _PODER_POR_INSTITUICAO:
        if keyword in text:
            return poder
    return None


def esfera_do_ente(cod_ibge: int) -> str:
    """Esfera pelo código IBGE, para entes que ainda não estão em ``/entes``."""
    if cod_ibge == 1:
        return "U"
    if cod_ibge == 53:
        return "D"
    return "E" if cod_ibge < 100 else "M"


def nome_anexo_dca(anexo: str, ano: int) -> str:
    """Em 2013 os anexos da DCA não têm o prefixo: 'Anexo I-E', não 'DCA-Anexo I-E'."""
    if ano == 2013 and anexo.startswith("DCA-"):
        return anexo[len("DCA-") :]
    return anexo


def _numero_anexo(anexo: str) -> str | None:
    match = re.search(r"(\d{2})", anexo)
    return match.group(1) if match else None


def _revision(item: Mapping[str, Any]) -> str:
    return "|".join(
        str(item.get(k, "")) for k in ("data_status", "status_relatorio", "forma_envio")
    )


def _com_anexos(
    params: dict[str, Any], anexos: Iterable[str] | None
) -> Iterator[dict[str, Any]]:
    if anexos is None:
        yield params
        return
    for anexo in anexos:
        yield {**params, "no_anexo": anexo}


@dataclass(frozen=True)
class _Linha:
    """Uma linha do extrato, já normalizada para o planejamento."""

    entity: int
    year: int
    period: int | None
    periodicity: str
    text: str
    simplificado: bool
    instituicao: Any

    @classmethod
    def de(cls, item: Mapping[str, Any]) -> _Linha | None:
        entity = item.get("cod_ibge") or item.get("id_ente")
        year = item.get("exercicio") or item.get("an_referencia")
        if entity is None or year is None:
            return None
        text = _normalise_text(item.get("entregavel"))
        period = item.get("periodo")
        return cls(
            entity=int(entity),
            year=int(year),
            period=None if period is None else int(period),
            periodicity=str(item.get("periodicidade", "")).upper(),
            text=text,
            simplificado="SIMPLIFICADO" in text
            or str(item.get("tipo_relatorio")).upper() == "S",
            instituicao=item.get("instituicao"),
        )


_Unidade = tuple[str, dict[str, Any], bool]


def _units_rreo(linha: _Linha, esfera: str, scope: PlanScope) -> Iterator[_Unidade]:
    cfg = scope.cfg("rreo")
    if linha.period is None or linha.period not in cfg["periodos"]:
        return
    # Quem entrega o simplificado só aparece com 'RREO Simplificado'.
    base = {
        "id_ente": linha.entity,
        "an_exercicio": linha.year,
        "nr_periodo": linha.period,
        "co_tipo_demonstrativo": "RREO Simplificado" if linha.simplificado else "RREO",
    }
    for params in _com_anexos(base, cfg["no_anexo"]):
        yield "rreo", params, True


def _anexos_do_poder(anexos: list[str] | None, co_poder: str) -> list[str] | None:
    if anexos is None or co_poder == "E":
        return anexos
    return [
        a
        for a in anexos
        if _numero_anexo(a) is None or _numero_anexo(a) in _ANEXOS_RGF_OUTROS_PODERES
    ]


def _units_rgf(linha: _Linha, esfera: str, scope: PlanScope) -> Iterator[_Unidade]:
    if linha.period is None:
        return
    semestral = linha.periodicity == "S" or linha.simplificado
    permitidos = scope.poderes(esfera)
    poder = poder_da_instituicao(linha.instituicao)
    # Instituição reconhecida: só o poder dela. Desconhecida: todos os poderes
    # da esfera, marcados como não esperados — o vazio ali é normal.
    candidatos = [(poder, True)] if poder else [(p, False) for p in permitidos]
    for co_poder, esperado in candidatos:
        if co_poder not in permitidos:
            continue
        base = {
            "id_ente": linha.entity,
            "an_exercicio": linha.year,
            "in_periodicidade": "S" if semestral else "Q",
            "nr_periodo": linha.period,
            "co_tipo_demonstrativo": "RGF Simplificado" if semestral else "RGF",
            "co_poder": co_poder,
        }
        anexos = _anexos_do_poder(scope.cfg("rgf")["no_anexo"], co_poder)
        for params in _com_anexos(base, anexos):
            yield "rgf", params, esperado


def _units_dca(linha: _Linha, esfera: str, scope: PlanScope) -> Iterator[_Unidade]:
    anexos = scope.cfg("dca")["no_anexo"]
    if anexos is not None:
        anexos = list(dict.fromkeys(nome_anexo_dca(a, linha.year) for a in anexos))
    base = {"id_ente": linha.entity, "an_exercicio": linha.year}
    for params in _com_anexos(base, anexos):
        yield "dca", params, True


def _units_msc(linha: _Linha, esfera: str, scope: PlanScope) -> Iterator[_Unidade]:
    if linha.period is None:
        return
    closing = linha.periodicity == "A" or "ENCERRAMENTO" in linha.text
    matriz = "MSCE" if closing else "MSCC"
    mes = _MSCE_MONTH if closing else linha.period
    for endpoint in MSC_ENDPOINTS:
        cfg = scope.cfg(endpoint)
        if not scope.wants(endpoint, linha.year):
            continue
        if matriz not in cfg["tipos_matriz"] or mes not in cfg["meses"]:
            continue
        for classe in cfg["classes"]:
            for id_tv in cfg["id_tv"]:
                params = {
                    "id_ente": linha.entity,
                    "an_referencia": linha.year,
                    "me_referencia": mes,
                    "co_tipo_matriz": matriz,
                    "classe_conta": classe,
                    "id_tv": id_tv,
                }
                yield endpoint, params, True


# O filtro de endpoint ligado e de ano vale para os três primeiros aqui; a MSC
# confere cada um dos seus endpoints dentro de ``_units_msc``.
_UNITS_POR_TIPO = {
    "rreo": _units_rreo,
    "rgf": _units_rgf,
    "dca": _units_dca,
    "msc": _units_msc,
}


def _units_da_linha(
    item: Mapping[str, Any], esfera: str, scope: PlanScope
) -> Iterator[_Unidade]:
    linha = _Linha.de(item)
    kind = classify_delivery(item.get("entregavel"))
    if linha is None or kind is None:
        return
    if kind != "msc" and not scope.wants(kind, linha.year):
        return
    yield from _UNITS_POR_TIPO[kind](linha, esfera, scope)


def plan_units(
    items: Iterable[Mapping[str, Any]], esfera: str, scope: PlanScope
) -> list[WorkUnit]:
    """Consultas geradas pelo extrato de um ente num ano, já no recorte.

    Recebe todas as linhas do extrato de um ente e ano de uma vez porque mais
    de uma linha pode levar à mesma consulta — a Assembleia e o Tribunal de
    Contas entregam RGF separados, e os dois respondem em ``co_poder=L``. O
    ``revision_marker`` da consulta junta o de todas as linhas que a geraram, e
    só muda quando uma delas muda: é isso que põe uma retificação de volta na
    fila sem rebuscar o que não mudou.
    """
    acumulado: dict[str, tuple[str, dict[str, Any], set[str], list[bool]]] = {}
    for item in items:
        for endpoint, params, esperado in _units_da_linha(item, esfera, scope):
            try:
                validate_params(endpoint, params)
            except SiconfiParametroInvalido as exc:
                logger.warning("[siconfi] consulta descartada no planejamento: %s", exc)
                continue
            key = request_hash(endpoint, params)
            entry = acumulado.setdefault(key, (endpoint, params, set(), [False]))
            entry[2].add(_revision(item))
            entry[3][0] = entry[3][0] or esperado
    return [
        (endpoint, params, ";".join(sorted(markers)), esperado[0])
        for endpoint, params, markers, esperado in acumulado.values()
    ]
