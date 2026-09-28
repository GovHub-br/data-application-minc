"""Cliente resiliente para a API pública SICONFI.

O limite divulgado pelo Tesouro é de uma requisição por segundo.  O
``SiconfiRateLimiter`` usa PostgreSQL como coordenador para que esse contrato
continue valendo entre tasks, workers e DAGs diferentes.
"""

from __future__ import annotations

import hashlib
import json
import logging
import random
import time
import unicodedata
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Iterator, Mapping

import httpx
import psycopg2
from psycopg2 import sql

logger = logging.getLogger(__name__)

BASE_URL = "https://apidatalake.tesouro.gov.br/ords/cdwhprd/siconfi/tt"
RATE_LIMIT_NAME = "siconfi_public_api"
CONTROL_SCHEMA = "siconfi_control"
RETRYABLE_STATUS = {408, 429, 500, 502, 503, 504}


class SiconfiRequestError(RuntimeError):
    """Erro HTTP da SICONFI com contexto suficiente para a fila de controle."""

    def __init__(self, status_code: int | None, detail: str) -> None:
        super().__init__(detail)
        self.status_code = status_code


class SiconfiRetryableError(SiconfiRequestError):
    """Falha transitória: a unidade de trabalho deve voltar para a fila."""


class SiconfiPermanentError(SiconfiRequestError):
    """Falha de contrato/parâmetro: repetir não muda o resultado."""


@dataclass(frozen=True)
class SiconfiPage:
    endpoint: str
    params: dict[str, Any]
    offset: int
    items: list[dict[str, Any]]
    has_more: bool
    headers: dict[str, str]
    payload: dict[str, Any]


def request_hash(endpoint: str, params: dict[str, Any]) -> str:
    """Identificador determinístico da consulta, sem depender da ordem do dict."""
    canonical = json.dumps(
        {"endpoint": endpoint, "params": params},
        ensure_ascii=False,
        sort_keys=True,
        default=str,
        separators=(",", ":"),
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


class SiconfiRateLimiter:
    """Reserva horários de chamada com lock transacional no PostgreSQL.

    A transação só mantém o lock durante a reserva; o ``sleep`` acontece fora
    dela. Assim, várias tasks podem reservar posições consecutivas sem produzir
    rajadas contra a API.

    A conexão é reaproveitada entre reservas: a 1 req/s, abrir uma conexão nova
    por requisição custaria milhares de handshakes por hora contra um Postgres
    que normalmente está em outra infra. O DDL idempotente roda uma vez por
    conexão, não uma vez por requisição.
    """

    def __init__(
        self,
        conn_str: str,
        schema: str = CONTROL_SCHEMA,
        interval_s: float = 1.05,
    ) -> None:
        self.conn_str = conn_str
        self.schema = schema
        self.interval_s = interval_s
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
                    cur.execute(
                        sql.SQL(
                            """
                            CREATE TABLE IF NOT EXISTS {}.api_rate_limit (
                                limiter_name TEXT PRIMARY KEY,
                                next_allowed_at TIMESTAMPTZ NOT NULL
                            )
                            """
                        ).format(sql.Identifier(self.schema))
                    )
                    cur.execute(
                        sql.SQL(
                            """
                            INSERT INTO {}.api_rate_limit (limiter_name, next_allowed_at)
                            VALUES (%s, now())
                            ON CONFLICT (limiter_name) DO NOTHING
                            """
                        ).format(sql.Identifier(self.schema)),
                        (RATE_LIMIT_NAME,),
                    )
            self._conn = conn
        return self._conn

    def close(self) -> None:
        if self._conn is not None and not self._conn.closed:
            self._conn.close()
        self._conn = None

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
            logger.info("[siconfi] rate limiter: aguardando %.2fs", wait_s)
            time.sleep(wait_s)


class SiconfiClient:
    """GET paginado, rate-limited e com retry classificado da SICONFI."""

    def __init__(
        self,
        conn_str: str,
        *,
        timeout_s: float = 90.0,
        max_retries: int = 4,
        page_limit: int = 5000,
        rate_limit_interval_s: float = 1.05,
    ) -> None:
        self.timeout_s = timeout_s
        self.max_retries = max_retries
        self.page_limit = page_limit
        self.rate_limiter = SiconfiRateLimiter(conn_str, interval_s=rate_limit_interval_s)
        self.client = httpx.Client(
            base_url=BASE_URL,
            headers={"Accept": "application/json"},
            timeout=httpx.Timeout(timeout_s),
            follow_redirects=True,
        )

    def close(self) -> None:
        self.client.close()
        self.rate_limiter.close()

    def _get(
        self, endpoint: str, params: dict[str, Any]
    ) -> tuple[dict[str, Any], dict[str, str]]:
        path = f"/{endpoint.lstrip('/')}"
        for attempt in range(1, self.max_retries + 1):
            self.rate_limiter.reserve()
            try:
                response = self.client.get(path, params=params)
            except httpx.HTTPError as exc:
                if attempt == self.max_retries:
                    raise SiconfiRetryableError(None, f"erro de rede: {exc}") from exc
                self._backoff(attempt, None)
                continue

            if response.status_code == 200:
                try:
                    payload = response.json()
                except ValueError as exc:
                    raise SiconfiRetryableError(
                        200, "resposta 200 sem JSON válido"
                    ) from exc
                if not isinstance(payload, dict):
                    raise SiconfiPermanentError(200, "resposta não possui envelope JSON")
                return payload, {k.lower(): v for k, v in response.headers.items()}

            detail = response.text[:500]
            if response.status_code in RETRYABLE_STATUS:
                if attempt == self.max_retries:
                    raise SiconfiRetryableError(response.status_code, detail)
                self._backoff(attempt, response.headers.get("Retry-After"))
                continue
            raise SiconfiPermanentError(response.status_code, detail)

        raise AssertionError("loop de retry terminou inesperadamente")

    @staticmethod
    def _backoff(attempt: int, retry_after: str | None) -> None:
        try:
            base = float(retry_after) if retry_after else min(60.0, 2.0 ** attempt)
        except ValueError:
            base = min(60.0, 2.0 ** attempt)
        wait_s = base + random.uniform(0, min(1.0, base * 0.2))
        logger.warning("[siconfi] retry HTTP em %.2fs (tentativa %s)", wait_s, attempt)
        time.sleep(wait_s)

    def iter_pages(self, endpoint: str, params: dict[str, Any]) -> Iterator[SiconfiPage]:
        """Itera o envelope ORDS usando ``hasMore`` e ``offset``.

        O tamanho real padrão do servidor já variou (a documentação cita 5 mil
        e a resposta atual de ``/entes`` informa 6 mil), por isso o limite é
        enviado explicitamente e o avanço usa a quantidade realmente recebida.
        """
        offset = 0
        base_params = {k: v for k, v in params.items() if v is not None}
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


# --- Planejamento a partir do extrato de entregas --------------------------
#
# O extrato identifica o demonstrativo pelo nome por extenso ("Relatório
# Resumido de Execução Orçamentária", "Balanço Anual (DCA)", "MSC
# Encerramento"...), não pela sigla. As funções abaixo traduzem uma linha dele
# nos parâmetros que os endpoints de fatos aceitam.

FACT_ENDPOINTS = (
    "rreo",
    "rgf",
    "dca",
    "msc_patrimonial",
    "msc_orcamentaria",
    "msc_controle",
)
# Mês que a API de MSC espera para a matriz de encerramento: o extrato informa
# ``periodo=1`` nessas linhas, mas ``me_referencia=1`` com ``MSCE`` volta vazio.
_MSCE_MONTH = 12
_MSC_CLASSES = (
    ("msc_patrimonial", (1, 2, 3, 4)),
    ("msc_orcamentaria", (5, 6)),
    ("msc_controle", (7, 8)),
)
_MSC_VALUE_TYPES = ("beginning_balance", "period_change", "ending_balance")
_MSC_MATRIX_TYPES = ("MSCC", "MSCE")
_PODERES = ("E", "L", "J", "M", "D")
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

WorkUnit = tuple[str, dict[str, Any], str]


def _normalise_text(value: Any) -> str:
    text = unicodedata.normalize("NFKD", str(value or ""))
    return "".join(ch for ch in text if not unicodedata.combining(ch)).upper()


def _normalise_type(value: Any, prefix: str) -> str:
    return f"{prefix} Simplificado" if str(value).upper() == "S" else prefix


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


def work_units(item: dict[str, Any]) -> list[WorkUnit]:
    """Unidades ``(endpoint, params, revision)`` geradas por uma linha do extrato.

    Gera tudo o que a linha permite; o recorte configurado é aplicado depois,
    por ``PlanScope.allows``. No RGF, só o poder da instituição que entregou —
    os cinco apenas quando a instituição não é reconhecida.
    """
    entity = item.get("cod_ibge") or item.get("id_ente")
    year = item.get("exercicio") or item.get("an_referencia")
    if entity is None or year is None:
        return []
    kind = classify_delivery(item.get("entregavel"))
    period = item.get("periodo")
    periodicity = str(item.get("periodicidade", "")).upper()
    revision = "|".join(
        str(item.get(k, "")) for k in ("data_status", "status_relatorio", "forma_envio")
    )
    entity, year = int(entity), int(year)

    if kind == "rreo" and period is not None:
        return [
            (
                "rreo",
                {
                    "id_ente": entity,
                    "an_exercicio": year,
                    "nr_periodo": int(period),
                    "co_tipo_demonstrativo": _normalise_type(
                        item.get("tipo_relatorio"), "RREO"
                    ),
                },
                revision,
            )
        ]
    if kind == "rgf" and period is not None:
        poder = poder_da_instituicao(item.get("instituicao"))
        return [
            (
                "rgf",
                {
                    "id_ente": entity,
                    "an_exercicio": year,
                    "in_periodicidade": "S" if periodicity == "S" else "Q",
                    "nr_periodo": int(period),
                    "co_tipo_demonstrativo": _normalise_type(
                        item.get("tipo_relatorio"), "RGF"
                    ),
                    "co_poder": co_poder,
                },
                revision,
            )
            for co_poder in ((poder,) if poder else _PODERES)
        ]
    if kind == "dca":
        return [("dca", {"id_ente": entity, "an_exercicio": year}, revision)]
    if kind == "msc" and period is not None:
        closing = periodicity == "A" or "ENCERRAMENTO" in _normalise_text(
            item.get("entregavel")
        )
        common = {
            "id_ente": entity,
            "an_referencia": year,
            "me_referencia": _MSCE_MONTH if closing else int(period),
            "co_tipo_matriz": "MSCE" if closing else "MSCC",
        }
        return [
            (
                endpoint,
                {**common, "classe_conta": account_class, "id_tv": value_type},
                revision,
            )
            for endpoint, classes in _MSC_CLASSES
            for account_class in classes
            for value_type in _MSC_VALUE_TYPES
        ]
    return []


# --- Recorte configurável ---------------------------------------------------


def _optional_set(
    config: Mapping[str, Any], key: str, cast: Callable[[Any], Any]
) -> frozenset | None:
    value = config.get(key)
    return None if value is None else frozenset(cast(v) for v in value)


@dataclass(frozen=True)
class PlanScope:
    """O que entra na fila e o que sai dela. ``None`` num campo = sem filtro.

    A mesma regra vale no planejamento (``allows``) e no claim
    (``constraints``): estreitar o recorte depois que a fila já foi montada
    também para de buscar o que ficou de fora.
    """

    start_year: int
    end_year: int
    endpoints: frozenset[str] | None = None
    rgf_poderes: frozenset[str] | None = None
    msc_months: frozenset[int] | None = None
    msc_classes: frozenset[int] | None = None
    msc_value_types: frozenset[str] | None = None
    msc_matrix_types: frozenset[str] | None = None

    @classmethod
    def from_config(cls, config: Mapping[str, Any]) -> PlanScope:
        scope = cls(
            start_year=int(config["start_year"]),
            end_year=int(config["end_year"]),
            endpoints=_optional_set(config, "fact_endpoints", str),
            rgf_poderes=_optional_set(config, "rgf_poderes", lambda v: str(v).upper()),
            msc_months=_optional_set(config, "msc_months", int),
            msc_classes=_optional_set(config, "msc_classes", int),
            msc_value_types=_optional_set(config, "msc_value_types", str),
            msc_matrix_types=_optional_set(
                config, "msc_matrix_types", lambda v: str(v).upper()
            ),
        )
        for name, value, valid in (
            ("fact_endpoints", scope.endpoints, FACT_ENDPOINTS),
            ("rgf_poderes", scope.rgf_poderes, _PODERES),
            ("msc_value_types", scope.msc_value_types, _MSC_VALUE_TYPES),
            ("msc_matrix_types", scope.msc_matrix_types, _MSC_MATRIX_TYPES),
        ):
            unknown = sorted((value or frozenset()) - set(valid))
            if unknown:
                raise ValueError(f"siconfi_config: {name} inválido: {unknown}")
        return scope

    def enabled(self, endpoint: str) -> bool:
        return endpoint not in FACT_ENDPOINTS or (
            self.endpoints is None or endpoint in self.endpoints
        )

    def constraints(self, endpoint: str) -> dict[str, frozenset]:
        """Valores permitidos por parâmetro da unidade de trabalho."""
        year_key = (
            "an_exercicio" if endpoint in ("rreo", "rgf", "dca") else "an_referencia"
        )
        allowed: dict[str, frozenset | None] = {
            year_key: frozenset(range(self.start_year, self.end_year + 1))
        }
        if endpoint == "rgf":
            allowed["co_poder"] = self.rgf_poderes
        if endpoint.startswith("msc_"):
            allowed["me_referencia"] = self.msc_months
            allowed["classe_conta"] = self.msc_classes
            allowed["id_tv"] = self.msc_value_types
            allowed["co_tipo_matriz"] = self.msc_matrix_types
        return {key: values for key, values in allowed.items() if values is not None}

    def allows(self, endpoint: str, params: Mapping[str, Any]) -> bool:
        return self.enabled(endpoint) and all(
            params.get(key) in values
            for key, values in self.constraints(endpoint).items()
        )

    def fingerprint(self) -> str:
        """Muda quando o recorte muda — e aí o planejamento relê o extrato."""
        state = {
            name: sorted(value) if isinstance(value, frozenset) else value
            for name, value in vars(self).items()
        }
        return hashlib.sha256(json.dumps(state, sort_keys=True).encode()).hexdigest()[:16]
