"""Configuração da extração SICONFI: padrões, precedência e validação.

A configuração efetiva de uma execução é montada nesta ordem de precedência::

    dag_run.conf  >  Variable ``siconfi_extracao_config``  >  DEFAULT_CONFIG

A mescla é profunda: um ``dag_run.conf`` com só
``{"endpoints": {"dca": {"ano_inicio": 2020}}}`` muda esse campo e herda todo o
resto. Listas e ``null`` substituem o valor de baixo, não se somam a ele.

A validação é rígida porque a API não reclama de parâmetro errado: ela
responde HTTP 200 com ``items: []``, e uma configuração com um ano, uma classe
ou uma esfera inválida produziria milhares de "vazios" sem nenhum erro. Por
isso qualquer chave desconhecida ou valor fora do domínio verificado derruba a
execução antes da primeira chamada, com a lista de todos os problemas.

``DEFAULT_CONFIG`` é idêntico a ``dags/data_ingest/siconfi/siconfi_extracao_config.json``
(um teste garante): a DAG funciona sem a Variable criada.
"""

from __future__ import annotations

import copy
import unicodedata
from typing import Annotated, Any, Iterable, Literal, Mapping

from pydantic import BaseModel, ConfigDict, Field, ValidationError

from cliente_siconfi import (
    ANO_MINIMO,
    CLASSES_MSC,
    ENDPOINT_POR_CHAVE,
    ESFERAS,
    MSC_ENDPOINTS,
    nome_anexo_dca,
)

VARIABLE_NAME = "siconfi_extracao_config"

# Status com que cada partição é registrada em ``siconfi_control.partition_log``.
# ``reprocessar`` (só via dag_run.conf) aceita os mesmos nomes.
STATUS_PARTICAO = ("sucesso_com_dados", "vazio_esperado", "vazio_inesperado", "erro")

DEFAULT_CONFIG: dict[str, Any] = {
    "global": {
        "esferas": ["M", "E", "D", "U"],
        "incluir_cod_ibge": [],
        "excluir_cod_ibge": [],
        "requisicoes_por_segundo": 1,
        "max_paralelismo": 2,
        "retentativas": 4,
        "tamanho_pagina": 5000,
        "max_minutos_por_execucao": 45,
        "max_particoes_por_execucao": 2000,
        "max_tentativas_por_particao": 5,
        "max_extratos_por_planejamento": 20000,
        "reprocessar": [],
    },
    "endpoints": {
        "anexos_relatorios": {"ativo": True, "recarga_horas": 168},
        "entes": {"ativo": True, "recarga_horas": 168},
        "extrato_entregas": {
            "ativo": True,
            "ano_inicio": 2013,
            "ano_fim": "corrente",
            "rebusca_anos": 2,
            "rebusca_dias": 7,
        },
        "dca": {
            "ativo": True,
            "ano_inicio": 2013,
            "ano_fim": "corrente",
            "no_anexo": None,
        },
        "rreo": {
            "ativo": True,
            "ano_inicio": 2015,
            "ano_fim": "corrente",
            "periodos": [1, 2, 3, 4, 5, 6],
            "no_anexo": None,
        },
        "rgf": {
            "ativo": True,
            "ano_inicio": 2015,
            "ano_fim": "corrente",
            "no_anexo": None,
            "poderes_por_esfera": {
                "M": ["E", "L"],
                "E": ["E", "L", "J", "M", "D"],
                "D": ["E", "L", "J", "M", "D"],
                "U": ["E", "L", "J", "M", "D"],
            },
        },
        "msc_orcamentaria": {
            "ativo": True,
            "ano_inicio": 2019,
            "ano_fim": "corrente",
            "classes": [5, 6],
            "meses": [12],
            "tipos_matriz": ["MSCC"],
            "id_tv": ["ending_balance"],
        },
        # Fora do escopo do datalake de análise; mantidos desligados para quem
        # já dependia deles.
        "msc_patrimonial": {
            "ativo": False,
            "ano_inicio": 2019,
            "ano_fim": "corrente",
            "classes": [1, 2, 3, 4],
            "meses": [12],
            "tipos_matriz": ["MSCC"],
            "id_tv": ["ending_balance"],
        },
        "msc_controle": {
            "ativo": False,
            "ano_inicio": 2019,
            "ano_fim": "corrente",
            "classes": [7, 8],
            "meses": [12],
            "tipos_matriz": ["MSCC"],
            "id_tv": ["ending_balance"],
        },
    },
}


# --- Estrutura -----------------------------------------------------------------
#
# Os modelos não têm valores padrão: a mescla com DEFAULT_CONFIG vem antes, e
# assim o único lugar onde um padrão mora é o dicionário acima.

Esfera = Literal["M", "E", "D", "U"]
Poder = Literal["E", "L", "J", "M", "D"]
AnoFim = int | Literal["corrente"]
# ``null`` = todos os anexos numa chamada; lista vazia seria ambígua.
Anexos = Annotated[list[str], Field(min_length=1)] | None


class _Modelo(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)


class _Global(_Modelo):
    esferas: list[Esfera] = Field(min_length=1)
    incluir_cod_ibge: list[int]
    excluir_cod_ibge: list[int]
    requisicoes_por_segundo: float = Field(gt=0, le=3)
    max_paralelismo: int = Field(ge=1, le=5)
    retentativas: int = Field(ge=1, le=10)
    tamanho_pagina: int = Field(ge=1, le=5000)
    # Precisa ficar abaixo do lease da fila (60 min), senão outra task pega a
    # mesma partição enquanto esta ainda trabalha nela.
    max_minutos_por_execucao: float = Field(gt=0, lt=60)
    max_particoes_por_execucao: int = Field(ge=1)
    max_tentativas_por_particao: int = Field(ge=1)
    max_extratos_por_planejamento: int = Field(ge=1)
    reprocessar: list[
        Literal["sucesso_com_dados", "vazio_esperado", "vazio_inesperado", "erro"]
    ]


class _Referencia(_Modelo):
    ativo: bool
    recarga_horas: int = Field(ge=1)


class _Anual(_Modelo):
    ativo: bool
    ano_inicio: int
    ano_fim: AnoFim


class _Extrato(_Anual):
    rebusca_anos: int = Field(ge=0)
    rebusca_dias: int = Field(ge=1)


class _Dca(_Anual):
    no_anexo: Anexos


class _Rreo(_Anual):
    periodos: list[int] = Field(min_length=1)
    no_anexo: Anexos


class _Rgf(_Anual):
    no_anexo: Anexos
    poderes_por_esfera: dict[Esfera, list[Poder]]


class _Msc(_Anual):
    classes: list[int] = Field(min_length=1)
    meses: list[int] = Field(min_length=1)
    tipos_matriz: list[Literal["MSCC", "MSCE"]] = Field(min_length=1)
    id_tv: list[Literal["beginning_balance", "ending_balance", "period_change"]] = Field(
        min_length=1
    )


class _Endpoints(_Modelo):
    anexos_relatorios: _Referencia
    entes: _Referencia
    extrato_entregas: _Extrato
    dca: _Dca
    rreo: _Rreo
    rgf: _Rgf
    msc_orcamentaria: _Msc
    msc_patrimonial: _Msc
    msc_controle: _Msc


class _Config(_Modelo):
    global_: _Global = Field(alias="global")
    endpoints: _Endpoints


# --- Montagem ------------------------------------------------------------------


def mesclar(base: Any, sobre: Any) -> Any:
    """Mescla profunda: dicionários se combinam, o resto é substituído."""
    if isinstance(base, dict) and isinstance(sobre, dict):
        resultado = copy.deepcopy(base)
        for chave, valor in sobre.items():
            resultado[chave] = (
                mesclar(base[chave], valor) if chave in base else copy.deepcopy(valor)
            )
        return resultado
    return copy.deepcopy(sobre)


def carregar(variable: Any, conf: Any, ano_corrente: int) -> dict[str, Any]:
    """Configuração efetiva, validada e com ``"corrente"`` já resolvido.

    ``variable`` é o conteúdo da Variable (``None`` se ela não existe) e
    ``conf`` o ``dag_run.conf`` (``None`` ou ``{}`` numa execução agendada).
    Levanta ``ValueError`` listando todos os problemas encontrados.
    """
    erros: list[str] = []
    for nome, camada in ((VARIABLE_NAME, variable), ("dag_run.conf", conf)):
        if camada is not None and not isinstance(camada, dict):
            erros.append(
                f"{nome} precisa ser um objeto JSON, veio {type(camada).__name__}"
            )
    if erros:
        raise ValueError(_mensagem(erros))

    variable = variable or {}
    conf = conf or {}
    # Numa Variable, ``reprocessar`` rebuscaria as mesmas partições a cada hora.
    if (variable.get("global") or {}).get("reprocessar"):
        erros.append(
            f"{VARIABLE_NAME}.global.reprocessar: use só no dag_run.conf de uma "
            "execução manual; na Variable ele rebuscaria tudo a cada execução"
        )

    bruta = mesclar(mesclar(DEFAULT_CONFIG, variable), conf)
    try:
        _Config.model_validate(bruta)
    except ValidationError as exc:
        erros.extend(_erros_pydantic(exc))
        raise ValueError(_mensagem(erros)) from None

    config = _resolver(bruta, ano_corrente)
    erros.extend(_regras(config, ano_corrente))
    if erros:
        raise ValueError(_mensagem(erros))
    return config


def _resolver(config: dict[str, Any], ano_corrente: int) -> dict[str, Any]:
    """Resolve ``"corrente"`` e tira repetições das listas, mantendo a ordem."""
    config = copy.deepcopy(config)
    for endpoint in config["endpoints"].values():
        if endpoint.get("ano_fim") == "corrente":
            endpoint["ano_fim"] = ano_corrente
        for chave in (
            "periodos",
            "classes",
            "meses",
            "tipos_matriz",
            "id_tv",
            "no_anexo",
        ):
            if isinstance(endpoint.get(chave), list):
                endpoint[chave] = _sem_repeticao(endpoint[chave])
        if "poderes_por_esfera" in endpoint:
            endpoint["poderes_por_esfera"] = {
                esfera: _sem_repeticao(poderes)
                for esfera, poderes in endpoint["poderes_por_esfera"].items()
            }
    glob = config["global"]
    for chave in ("esferas", "incluir_cod_ibge", "excluir_cod_ibge", "reprocessar"):
        glob[chave] = _sem_repeticao(glob[chave])
    return config


def _regras(config: dict[str, Any], ano_corrente: int) -> list[str]:
    """Regras que cruzam campos, ou que dependem do ano corrente."""
    glob = config["global"]
    endpoints = config["endpoints"]
    erros: list[str] = []

    comuns = set(glob["incluir_cod_ibge"]) & set(glob["excluir_cod_ibge"])
    if comuns:
        erros.append(
            f"global: {sorted(comuns)} aparecem em incluir_cod_ibge e em excluir_cod_ibge"
        )
    for chave, cfg in endpoints.items():
        if "ano_inicio" in cfg:
            erros.extend(_regras_anos(chave, cfg, ano_corrente))
            erros.extend(_regras_cobertura_do_extrato(chave, cfg, endpoints))
        if chave in ("msc_orcamentaria", "msc_patrimonial", "msc_controle"):
            erros.extend(_regras_msc(chave, cfg))

    fora = [p for p in endpoints["rreo"]["periodos"] if not 1 <= p <= 6]
    if fora:
        erros.append(f"endpoints.rreo.periodos: {fora} fora dos bimestres 1–6")

    rgf = endpoints["rgf"]
    sem_poder = [e for e in glob["esferas"] if e not in rgf["poderes_por_esfera"]]
    if rgf["ativo"] and sem_poder:
        erros.append(
            "endpoints.rgf.poderes_por_esfera: faltam as esferas "
            f"{sem_poder}, listadas em global.esferas (use [] para não buscar)"
        )
    return erros


def _regras_anos(chave: str, cfg: Mapping[str, Any], ano_corrente: int) -> list[str]:
    erros = []
    minimo = ANO_MINIMO[ENDPOINT_POR_CHAVE[chave]]
    if cfg["ano_inicio"] < minimo:
        erros.append(
            f"endpoints.{chave}.ano_inicio: {cfg['ano_inicio']} é anterior ao "
            f"primeiro exercício com dados ({minimo})"
        )
    if cfg["ano_inicio"] > cfg["ano_fim"]:
        erros.append(
            f"endpoints.{chave}: ano_inicio ({cfg['ano_inicio']}) maior que "
            f"ano_fim ({cfg['ano_fim']})"
        )
    if cfg["ano_fim"] > ano_corrente:
        erros.append(
            f"endpoints.{chave}.ano_fim: {cfg['ano_fim']} está no futuro "
            f"(ano corrente: {ano_corrente})"
        )
    return erros


def _regras_cobertura_do_extrato(
    chave: str, cfg: Mapping[str, Any], endpoints: Mapping[str, Any]
) -> list[str]:
    # Os demonstrativos só são planejados a partir do extrato: um ano fora
    # dele nunca seria buscado, sem nenhum aviso.
    extrato = endpoints["extrato_entregas"]
    if chave == "extrato_entregas" or not cfg["ativo"]:
        return []
    if (
        extrato["ano_inicio"] <= cfg["ano_inicio"]
        and cfg["ano_fim"] <= extrato["ano_fim"]
    ):
        return []
    return [
        f"endpoints.{chave}: anos {cfg['ano_inicio']}–{cfg['ano_fim']} fora do "
        f"extrato_entregas ({extrato['ano_inicio']}–{extrato['ano_fim']}); "
        "o que não está no extrato nunca é planejado"
    ]


def _regras_msc(chave: str, cfg: Mapping[str, Any]) -> list[str]:
    erros = []
    validas = CLASSES_MSC[ENDPOINT_POR_CHAVE[chave]]
    classes = [c for c in cfg["classes"] if c not in validas]
    if classes:
        erros.append(
            f"endpoints.{chave}.classes: {classes} inválidas; este endpoint "
            f"só tem as classes {list(validas)}"
        )
    meses = [m for m in cfg["meses"] if not 1 <= m <= 12]
    if meses:
        erros.append(f"endpoints.{chave}.meses: {meses} fora de 1–12")
    if "MSCE" in cfg["tipos_matriz"] and 12 not in cfg["meses"]:
        erros.append(
            f"endpoints.{chave}.tipos_matriz: MSCE só existe com me_referencia=12; "
            "inclua 12 em meses"
        )
    return erros


def validar_anexos(
    config: Mapping[str, Any], anexos: Iterable[Mapping[str, Any]]
) -> list[str]:
    """Confere cada ``no_anexo`` configurado contra ``/anexos-relatorios``."""
    por_demonstrativo: dict[str, set[str]] = {"rreo": set(), "rgf": set(), "dca": set()}
    for item in anexos:
        demonstrativo = _normalizar(item.get("demonstrativo"))
        for endpoint, nomes in por_demonstrativo.items():
            if endpoint.upper() in demonstrativo:
                nomes.add(str(item.get("anexo")))

    erros = []
    for endpoint, conhecidos in por_demonstrativo.items():
        cfg = config["endpoints"][endpoint]
        if not cfg["ativo"] or cfg["no_anexo"] is None:
            continue
        if not conhecidos:
            erros.append(
                f"endpoints.{endpoint}.no_anexo: /anexos-relatorios não carregado; "
                "ative endpoints.anexos_relatorios ou use null"
            )
            continue
        # O nome de 2013 da DCA ('Anexo I-E') também é aceito.
        aceitos = conhecidos | {nome_anexo_dca(a, 2013) for a in conhecidos}
        desconhecidos = [a for a in cfg["no_anexo"] if a not in aceitos]
        if desconhecidos:
            erros.append(
                f"endpoints.{endpoint}.no_anexo: {desconhecidos} não existem em "
                f"/anexos-relatorios para {endpoint.upper()}"
            )
    return erros


def config_da_chave(config: Mapping[str, Any], endpoint: str) -> dict[str, Any]:
    """Bloco de configuração de um endpoint da API (``anexos-relatorios`` etc.)."""
    chave = next(k for k, v in ENDPOINT_POR_CHAVE.items() if v == endpoint)
    return dict(config["endpoints"][chave])


def endpoints_msc_ativos(config: Mapping[str, Any]) -> list[str]:
    return [e for e in MSC_ENDPOINTS if config_da_chave(config, e)["ativo"]]


# --- Utilidades ----------------------------------------------------------------


def _sem_repeticao(valores: list[Any]) -> list[Any]:
    return list(dict.fromkeys(valores))


def _normalizar(valor: Any) -> str:
    texto = unicodedata.normalize("NFKD", str(valor or ""))
    return "".join(c for c in texto if not unicodedata.combining(c)).upper()


def _erros_pydantic(exc: ValidationError) -> list[str]:
    erros = []
    for erro in exc.errors():
        local = ".".join(str(p) for p in erro["loc"])
        erros.append(f"{local}: {erro['msg']} (veio {erro.get('input')!r})")
    return erros


def _mensagem(erros: list[str]) -> str:
    return (
        f"Configuração SICONFI inválida ({VARIABLE_NAME} / dag_run.conf):\n- "
        + "\n- ".join(erros)
    )


__all__ = [
    "DEFAULT_CONFIG",
    "ESFERAS",
    "STATUS_PARTICAO",
    "VARIABLE_NAME",
    "carregar",
    "config_da_chave",
    "endpoints_msc_ativos",
    "mesclar",
    "validar_anexos",
]
