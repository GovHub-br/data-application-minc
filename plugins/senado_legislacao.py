"""O que a ``senado_legislacao_dag`` busca na API do Senado e onde grava no raw.

Duas entidades, um arquivo do raw por requisição::

    raw/senado/legislacao_lista/ano=AAAA/tipo=<LEI|LCP|DEC>/<run_id>.json
    raw/senado/legislacao_detalhe/ano=AAAA/id=<id>/<run_id>.json

Cada arquivo é um envelope, como no SICONFI (ADR 0014): de que requisição veio,
quando foi buscado, por qual run, quantos documentos o cliente contou e a
``resposta`` da API intacta. Acima do limite do proxy do MinIO o arquivo vira
partes de bytes (``datalakehouse.gravar_bytes_em_partes``); quem lê junta.
Cada run grava a própria key: o raw não é sobrescrito.

**Listagem.** Um ano é listado enquanto for o corrente e, depois, até existir
no raw uma listagem dele feita a partir de 1º de abril do ano seguinte
(``FECHAMENTO``). Daí em diante não é mais listado. Listar o ano fechado uma vez
só, no primeiro run depois de 31/12, perderia as normas de fim de dezembro que
o Senado indexa com atraso. O Param ``anos`` da DAG força a relistagem.

**Detalhe.** Pendente é o id que está na listagem mais recente de algum tipo e
ano e ainda não tem detalhe no raw. Cada norma é baixada uma vez: as alterações
que ela sofrer depois (os ``vides``) não chegam ao raw.

Este módulo não fala com a API; só ``ler_envelope`` lê o MinIO.
"""

from __future__ import annotations

import json
import re
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Iterable, Mapping, Sequence

import datalakehouse
import schemas_minc as schemas
from cliente_senadores import ClienteSenadores

FONTE = schemas.FONTE_SENADO
ENTIDADE_LISTA = schemas.ENTIDADE_LEGISLACAO_LISTA
ENTIDADE_DETALHE = schemas.ENTIDADE_LEGISLACAO_DETALHE

# Siglas de /legislacao/tiposNorma. LEI traz só as leis numeradas; DEC traz os
# decretos numerados e os sem número (medido em 10/11/2026).
TIPOS = ("LEI", "LCP", "DEC")
ANO_INICIAL = 1988
# Mês e dia do ano seguinte a partir dos quais uma listagem fecha o ano.
FECHAMENTO = (4, 1)
# O run fica vermelho quando os detalhes que falharam passam disto: o Eixo 3
# tem um decreto cujo detalhe nunca baixa, e ele não pode pintar todo mês.
FALHAS_TOLERADAS = 10
FRACAO_DE_FALHAS_TOLERADA = 0.05

_PARTICAO = re.compile(r"^(?P<nome>[a-z_]+)=(?P<valor>[^/]+)$")


# ── keys ─────────────────────────────────────────────────────────────────


def prefixo(entidade: str) -> str:
    return datalakehouse.prefixo(datalakehouse.CAMADA_RAW, FONTE, entidade)


def key_lista(tipo: str, ano: int, run_id: str) -> str:
    return datalakehouse.caminho_particionado(
        datalakehouse.CAMADA_RAW,
        FONTE,
        ENTIDADE_LISTA,
        {"ano": ano, "tipo": tipo},
        run_id,
        "json",
    )


def key_detalhe(ano: int, norma_id: str, run_id: str) -> str:
    if not str(norma_id).isdigit():
        raise ValueError(f"id de norma inesperado: {norma_id!r}")
    return datalakehouse.caminho_particionado(
        datalakehouse.CAMADA_RAW,
        FONTE,
        ENTIDADE_DETALHE,
        {"ano": ano, "id": norma_id},
        run_id,
        "json",
    )


def particao(key: str) -> dict[str, str]:
    """Os ``nome=valor`` do caminho de ``key``: ``{"ano": "1990", "tipo": "DEC"}``."""
    saida: dict[str, str] = {}
    for trecho in key.split("/")[:-1]:
        achado = _PARTICAO.match(trecho)
        if achado:
            saida[achado["nome"]] = achado["valor"]
    return saida


# ── envelope ─────────────────────────────────────────────────────────────


def envelope(
    entidade: str,
    requisicao: Mapping[str, Any],
    status_http: int,
    resposta: Any,
    itens: int,
    run_id: str,
    buscado_em: datetime,
) -> dict[str, Any]:
    """O arquivo do raw: a resposta da API intacta e de onde ela veio."""
    return {
        "fonte": FONTE,
        "entidade": entidade,
        "requisicao": dict(requisicao),
        "status_http": int(status_http),
        "buscado_em": buscado_em.astimezone(timezone.utc).isoformat(),
        "run_id": run_id,
        "itens": itens,
        "resposta": resposta,
    }


def corpo_json(conteudo: Mapping[str, Any]) -> bytes:
    """JSON compacto: a maior listagem (decretos de 1998) já tem 858 KiB."""
    return json.dumps(conteudo, ensure_ascii=False, separators=(",", ":")).encode("utf-8")


# ── o que já está no raw ─────────────────────────────────────────────────


@dataclass(frozen=True)
class Arquivo:
    """Um arquivo do raw: a key dele inteiro e as keys que se leem, em ordem."""

    key: str
    partes: tuple[str, ...]
    modificado: datetime

    @property
    def particao(self) -> dict[str, str]:
        return particao(self.key)


def arquivos(objetos: Iterable[Mapping[str, Any]]) -> tuple[list[Arquivo], set[str]]:
    """Agrupa a listagem do bucket por arquivo (``datalakehouse.agrupar_partes``).

    Devolve os arquivos completos, em ordem de key, e as keys dos que têm parte
    faltando ou estão divididos de dois jeitos: esses não se leem.
    """
    quando = {str(o["Key"]): o["LastModified"] for o in objetos}
    completos, problemas = datalakehouse.agrupar_partes(quando)
    lista = [
        Arquivo(key, tuple(partes), max(quando[p] for p in partes))
        for key, partes in sorted(completos.items())
    ]
    return lista, problemas


def listagens_por_particao(
    lista: Iterable[Arquivo],
) -> dict[tuple[str, int], list[Arquivo]]:
    """Os arquivos de listagem por ``(tipo, ano)``."""
    saida: dict[tuple[str, int], list[Arquivo]] = {}
    for arquivo in lista:
        p = arquivo.particao
        if "tipo" in p and p.get("ano", "").isdigit():
            saida.setdefault((p["tipo"], int(p["ano"])), []).append(arquivo)
    return saida


def ultimas_listagens(
    por_particao: Mapping[tuple[str, int], Sequence[Arquivo]],
) -> dict[tuple[str, int], Arquivo]:
    """A listagem mais recente de cada ``(tipo, ano)``."""
    return {
        chave: max(lista, key=lambda a: (a.modificado, a.key))
        for chave, lista in por_particao.items()
        if lista
    }


def fechamento(ano: int) -> datetime:
    mes, dia = FECHAMENTO
    return datetime(ano + 1, mes, dia, tzinfo=timezone.utc)


def a_listar(
    hoje: datetime,
    por_particao: Mapping[tuple[str, int], Sequence[Arquivo]],
    anos_pedidos: Sequence[int] = (),
) -> list[tuple[str, int]]:
    """Os ``(tipo, ano)`` que este run lista.

    Com ``anos_pedidos``, exatamente esses anos, mesmo os fechados. Sem eles, o
    ano corrente e todo ano que ainda não tem listagem feita a partir do
    ``fechamento`` dele.
    """
    if anos_pedidos:
        fora = sorted(a for a in anos_pedidos if not ANO_INICIAL <= a <= hoje.year)
        if fora:
            raise ValueError(f"anos fora de {ANO_INICIAL}..{hoje.year}: {fora}")
        return [(tipo, ano) for ano in sorted(set(anos_pedidos)) for tipo in TIPOS]
    saida = []
    for ano in range(ANO_INICIAL, hoje.year + 1):
        for tipo in TIPOS:
            feitas = por_particao.get((tipo, ano), ())
            fechado = ano < hoje.year and any(
                a.modificado >= fechamento(ano) for a in feitas
            )
            if not fechado:
                saida.append((tipo, ano))
    return saida


def ler_envelope(arquivo: Arquivo) -> dict[str, Any]:
    """O envelope de um arquivo do raw, juntando as partes (lê o MinIO)."""
    conteudo: dict[str, Any] = json.loads(
        datalakehouse.ler_bytes_das_partes(arquivo.partes)
    )
    return conteudo


# ── o que falta baixar ───────────────────────────────────────────────────


@dataclass(frozen=True)
class Norma:
    """Onde uma norma foi listada: o ano e o tipo pedidos à API."""

    ano: int
    tipo: str


def ids_listados(
    listagens: Iterable[tuple[tuple[str, int], Mapping[str, Any]]],
) -> dict[str, Norma]:
    """``id -> Norma`` a partir de ``((tipo, ano), envelope)`` das listagens."""
    saida: dict[str, Norma] = {}
    for (tipo, ano), conteudo in listagens:
        for documento in ClienteSenadores.documentos_da_lista(conteudo.get("resposta")):
            norma_id = documento.get("id")
            if isinstance(norma_id, str) and norma_id.isdigit():
                saida.setdefault(norma_id, Norma(ano, tipo))
    return saida


def ids_baixados(lista: Iterable[Arquivo]) -> set[str]:
    """Os ids que já têm detalhe completo no raw."""
    return {a.particao["id"] for a in lista if "id" in a.particao}


def pendentes(
    listados: Mapping[str, Norma], baixados: set[str]
) -> list[tuple[str, Norma]]:
    """Os ids sem detalhe, do ano mais recente para o mais antigo."""
    return sorted(
        ((i, n) for i, n in listados.items() if i not in baixados),
        key=lambda par: (-par[1].ano, int(par[0])),
    )


def excedeu_falhas(tentadas: int, falhas: int) -> bool:
    """Se as falhas de detalhe passaram do que um run tolera."""
    return falhas > max(FALHAS_TOLERADAS, FRACAO_DE_FALHAS_TOLERADA * tentadas)


def resumo_por_tipo(
    listados: Mapping[str, Norma], baixados: set[str]
) -> dict[str, dict[str, int]]:
    """Por tipo: normas na listagem mais recente, com detalhe e pendentes."""
    total = Counter(n.tipo for n in listados.values())
    com_detalhe = Counter(n.tipo for i, n in listados.items() if i in baixados)
    return {
        tipo: {
            "listadas": total[tipo],
            "com_detalhe": com_detalhe[tipo],
            "pendentes": total[tipo] - com_detalhe[tipo],
        }
        for tipo in sorted(total)
    }
