"""Legislação federal do Senado (Resultado 5 do Eixo 3): raw no MinIO e bronze pelo Trino.

Leis, leis complementares e decretos federais de 1988 até o ano corrente, da
API de Dados Abertos do Senado (``cliente_senadores``)::

    /legislacao/lista  ─▶ raw/senado/legislacao_lista/ano=AAAA/tipo=XXX/<run>.json
    /legislacao/{id}   ─▶ raw/senado/legislacao_detalhe/ano=AAAA/id=<id>/<run>.json
    raw                ─▶ <catálogo>.senado.{legislacao_lista, legislacao_detalhe,
                          legislacao_detalhe_secoes, arquivos}  (Trino)

As tabelas são as sources de ``dbt/minc/models/senado/bronze_*``. O que cada run
lista e baixa está em ``senado_legislacao``; como o raw vira bronze, em
``senado_bronze``.

**O que cada run faz.** Lista o ano corrente e os anos ainda não fechados (um ano
fecha com uma listagem feita a partir de 1º de abril do ano seguinte). Baixa o
detalhe de toda norma listada que ainda não tem detalhe no raw, **sem triagem**:
inclui os decretos sem número, que ``tipo=DEC`` também devolve. A escolha das
normas do Resultado 5 é regra de negócio e fica para a Gold. Por fim, carrega na
bronze todo arquivo do raw que ainda não está em ``senado.arquivos``, e confere,
por arquivo, as linhas carregadas.

**Primeira carga.** São cerca de 40 mil detalhes, uns 85 minutos com 4 em paralelo.
O ``detalhar`` para de pegar ids novos em ``max_run_minutes``, e o próximo run
continua de onde parou: dispare à mão até o ``conferir`` dizer que não resta
pendente. Depois, cada mês só baixa as normas novas.

**Limitações.** Cada norma é baixada uma vez: as alterações que ela sofrer depois
(``vides``) não chegam. Legislação estadual não sai desta API.

**Ambiente.** Catálogo e schema do Trino nas Variables ``senado_trino_target_catalog``
(padrão ``dw``, o nome local; no servidor é o mesmo valor da
``siconfi_trino_target_catalog``) e ``senado_trino_bronze_schema`` (padrão
``senado``). Conexões ``trino_default`` e ``minio_datalakehouse``. Mensal por
padrão (Variable ``dynamic_schedules``).
"""

import logging
import time
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from datetime import datetime, timedelta, timezone
from http import HTTPStatus
from typing import Any, Iterator, Sequence

from airflow.sdk import Param, Variable, dag, get_current_context, task
from airflow.sdk.exceptions import AirflowFailException

import datalakehouse
import senado_bronze as bronze
import senado_legislacao as sl
from cliente_senadores import ClienteSenadores
from schedule_loader import get_dynamic_schedule
from trino_bronze import quote_ident

logger = logging.getLogger(__name__)

TRINO_CONN_ID = "trino_default"
_MAX_RUN_MINUTES = 110
# Arquivos do raw por lote da carga da bronze: os INSERTs de um lote saem
# juntos e o registro em ``arquivos`` vem depois deles.
_LOTE_BRONZE = 500
_EXEMPLOS = 20

default_args = {
    "owner": "MinC",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


class _Trino:
    """``senado_bronze.Executor`` pelo ``TrinoHook``. Só na thread principal."""

    def __init__(self) -> None:
        from airflow.providers.trino.hooks.trino import TrinoHook

        self.hook = TrinoHook(trino_conn_id=TRINO_CONN_ID)

    def run(self, sql: str) -> None:
        self.hook.run(sql)

    def records(self, sql: str) -> list[Sequence[Any]]:
        return list(self.hook.get_records(sql))


def _alvo() -> tuple[str, str]:
    """Catálogo e schema da bronze, das Variables."""
    return (
        Variable.get(bronze.VARIABLE_CATALOGO, default=bronze.CATALOGO_PADRAO),
        Variable.get(bronze.VARIABLE_SCHEMA, default=bronze.SCHEMA_PADRAO),
    )


def _contexto() -> tuple[str, dict[str, Any]]:
    contexto = get_current_context()
    return str(contexto["dag_run"].run_id), dict(contexto["params"])


def _prazo(params: dict[str, Any]) -> float:
    minutos = min(float(params["max_run_minutes"]), _MAX_RUN_MINUTES)
    return time.monotonic() + 60 * minutos


def _arquivos_do_raw(entidade: str) -> tuple[list[sl.Arquivo], set[str]]:
    return sl.arquivos(datalakehouse.listar_objetos(sl.prefixo(entidade)))


# ── listagem ─────────────────────────────────────────────────────────────


def _listar(
    cliente: ClienteSenadores, pedidos: list[tuple[str, int]], run_id: str
) -> dict[str, Any]:
    """Lista cada ``(tipo, ano)`` e grava a resposta no raw. Uma falha não para
    as outras: fica no resumo, e o ``conferir`` deixa o run vermelho."""
    gravadas, normas, falhas = 0, 0, []
    for tipo, ano in pedidos:
        status, resposta = cliente.get_legislacao_lista(tipo, ano)
        if status != HTTPStatus.OK or not isinstance(resposta, dict):
            falhas.append(f"{tipo} {ano}: HTTP {int(status)}")
            continue
        documentos = cliente.documentos_da_lista(resposta)
        conteudo = sl.envelope(
            sl.ENTIDADE_LISTA,
            {"path": "/legislacao/lista", "params": {"tipo": tipo, "ano": ano}},
            status,
            resposta,
            len(documentos),
            run_id,
            datetime.now(timezone.utc),
        )
        datalakehouse.gravar_bytes_em_partes(
            sl.corpo_json(conteudo), sl.key_lista(tipo, ano, run_id)
        )
        gravadas += 1
        normas += len(documentos)
    resumo = {
        "pedidas": len(pedidos),
        "gravadas": gravadas,
        "normas": normas,
        "falhas": falhas,
    }
    logger.info("[senado_legislacao_dag] listagem: %s", resumo)
    return resumo


# ── detalhe ──────────────────────────────────────────────────────────────


def _buscar(cliente: ClienteSenadores, norma_id: str) -> tuple[HTTPStatus, Any, datetime]:
    """O que roda nas threads: só HTTP, nada do Airflow nem do MinIO."""
    status, resposta = cliente.get_legislacao_detalhe(norma_id)
    return status, resposta, datetime.now(timezone.utc)


def _detalhar(
    cliente: ClienteSenadores,
    pendentes: list[tuple[str, sl.Norma]],
    run_id: str,
    prazo: float,
    paralelo: int,
) -> dict[str, Any]:
    """Baixa os detalhes pendentes, ``paralelo`` por vez, até o ``prazo``.

    As threads só fazem a requisição; a gravação no MinIO fica aqui, na thread
    principal, porque no Airflow 3 a Connection e a Variable só se resolvem nela.
    """
    baixados: list[str] = []
    falhas: list[str] = []
    tentadas = 0
    fila: Iterator[tuple[str, sl.Norma]] = iter(pendentes)
    with ThreadPoolExecutor(max_workers=paralelo) as pool:
        em_voo: dict[Future[tuple[HTTPStatus, Any, datetime]], tuple[str, sl.Norma]] = {}

        def encher() -> None:
            while len(em_voo) < 2 * paralelo and time.monotonic() < prazo:
                proximo = next(fila, None)
                if proximo is None:
                    return
                em_voo[pool.submit(_buscar, cliente, proximo[0])] = proximo

        encher()
        while em_voo:
            prontos, _ = wait(em_voo, return_when=FIRST_COMPLETED)
            for futuro in prontos:
                norma_id, norma = em_voo.pop(futuro)
                tentadas += 1
                try:
                    status, resposta, buscado_em = futuro.result()
                except Exception as exc:  # resposta cortada, JSON inválido
                    falhas.append(f"{norma_id}: {type(exc).__name__}")
                    continue
                if status != HTTPStatus.OK or not isinstance(resposta, dict):
                    falhas.append(f"{norma_id}: HTTP {int(status)}")
                    continue
                conteudo = sl.envelope(
                    sl.ENTIDADE_DETALHE,
                    {
                        "path": f"/legislacao/{norma_id}",
                        "id": norma_id,
                        "ano": norma.ano,
                        "tipo": norma.tipo,
                    },
                    status,
                    resposta,
                    len(cliente.documentos_do_detalhe(resposta)),
                    run_id,
                    buscado_em,
                )
                datalakehouse.gravar_bytes_em_partes(
                    sl.corpo_json(conteudo), sl.key_detalhe(norma.ano, norma_id, run_id)
                )
                baixados.append(norma_id)
                if len(baixados) % 1_000 == 0:
                    logger.info(
                        "[senado_legislacao_dag] detalhe: %d de %d baixados",
                        len(baixados),
                        len(pendentes),
                    )
            encher()
    return {
        "pendentes_no_inicio": len(pendentes),
        "tentadas": tentadas,
        "baixados": len(baixados),
        "falhas": len(falhas),
        "exemplos_de_falha": falhas[:_EXEMPLOS],
        "restantes": len(pendentes) - len(baixados),
        "ids_baixados": baixados,
    }


# ── bronze ───────────────────────────────────────────────────────────────


def _carregar(
    alvos: dict[str, bronze.Destino],
    feitos: set[str],
    prazo: float,
    carga_run_id: str,
) -> dict[str, Any]:
    """Carrega, em lotes, os arquivos do raw que ainda não estão em ``arquivos``."""
    linhas: dict[str, int] = {}
    resumo: dict[str, Any] = {
        "arquivos": 0,
        "inconsistentes": 0,
        "restantes": 0,
        "linhas": linhas,
    }
    for entidade in (sl.ENTIDADE_LISTA, sl.ENTIDADE_DETALHE):
        lista, problemas = _arquivos_do_raw(entidade)
        if problemas:
            # Parte faltando ou dividido de dois jeitos: não se lê. Um detalhe
            # assim volta a ser baixado; uma listagem, relistada se o ano não fechou.
            logger.warning(
                "[senado_legislacao_dag] %s: %d arquivo(s) inconsistentes ficaram de "
                "fora (ex.: %s)",
                entidade,
                len(problemas),
                sorted(problemas)[0],
            )
            resumo["inconsistentes"] += len(problemas)
        pendentes = [a for a in lista if a.key not in feitos]
        for inicio in range(0, len(pendentes), _LOTE_BRONZE):
            if time.monotonic() >= prazo:
                resumo["restantes"] += len(pendentes) - inicio
                break
            lote = pendentes[inicio : inicio + _LOTE_BRONZE]
            convertidos = [
                bronze.converter(a.key, entidade, len(a.partes), sl.ler_envelope(a))
                for a in lote
            ]
            feito = bronze.gravar(
                alvos, convertidos, carga_run_id, datetime.now(timezone.utc)
            )
            resumo["arquivos"] += len(lote)
            for tabela, n in feito.items():
                linhas[tabela] = linhas.get(tabela, 0) + n
    logger.info("[senado_legislacao_dag] bronze: %s", resumo)
    return resumo


# ── conferência ──────────────────────────────────────────────────────────


def _veredito(
    lista: dict[str, Any],
    detalhe: dict[str, Any],
    carga: dict[str, Any],
    problemas: list[str],
    avisos: list[str],
) -> dict[str, Any]:
    """Loga o resumo do run e falha se algo não pode passar em silêncio."""
    for tipo, numeros in detalhe.get("por_tipo", {}).items():
        logger.info("[senado_legislacao_dag] %s: %s", tipo, numeros)
    for aviso in avisos[:_EXEMPLOS]:
        logger.warning("[senado_legislacao_dag] %s", aviso)
    if detalhe["falhas"]:
        logger.warning(
            "[senado_legislacao_dag] %d detalhe(s) falharam e ficam para o próximo "
            "run: %s",
            detalhe["falhas"],
            detalhe["exemplos_de_falha"],
        )
    if detalhe["restantes"] or carga["restantes"]:
        logger.info(
            "[senado_legislacao_dag] o prazo acabou antes do fim: %d detalhe(s) e %d "
            "arquivo(s) da bronze continuam no próximo run",
            detalhe["restantes"],
            carga["restantes"],
        )
    motivos = []
    if lista["falhas"]:
        motivos.append(f"{len(lista['falhas'])} listagem(ns) falharam: {lista['falhas']}")
    if sl.excedeu_falhas(detalhe["tentadas"], detalhe["falhas"]):
        motivos.append(f"{detalhe['falhas']} de {detalhe['tentadas']} detalhes falharam")
    if problemas:
        motivos.append(
            f"{len(problemas)} arquivo(s) com linha perdida na bronze: "
            f"{problemas[:_EXEMPLOS]}"
        )
    if motivos:
        raise AirflowFailException("; ".join(motivos))
    return {
        "por_tipo": detalhe.get("por_tipo", {}),
        "detalhes_restantes": detalhe["restantes"],
        "arquivos_restantes_na_bronze": carga["restantes"],
    }


@dag(
    dag_id="senado_legislacao_dag",
    schedule=get_dynamic_schedule("senado_legislacao_dag", default="@monthly"),
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    params={
        "anos": Param(
            [],
            type="array",
            items={"type": "integer", "minimum": sl.ANO_INICIAL},
            title="Anos",
            description=(
                "Anos a listar e detalhar, mesmo os já fechados. Vazio segue a "
                "política: o ano corrente e os ainda não fechados."
            ),
        ),
        "max_run_minutes": Param(
            _MAX_RUN_MINUTES,
            type="integer",
            minimum=1,
            maximum=_MAX_RUN_MINUTES,
            title="Minutos de detalhe e de carga antes de parar",
        ),
        "paralelo": Param(
            4, type="integer", minimum=1, maximum=8, title="Detalhes baixados por vez"
        ),
    },
    tags=["minc", "senado", "legislacao", "datalakehouse", "bronze", "trino"],
    doc_md=__doc__,
)
def senado_legislacao_dag() -> None:
    """Legislação federal do Senado: raw no MinIO e bronze pelo Trino."""

    @task
    def preparar() -> None:
        """Confere o catálogo e cria o schema e as tabelas da bronze."""
        catalogo, schema = _alvo()
        trino = _Trino()
        try:
            trino.records(f"SHOW SCHEMAS FROM {quote_ident(catalogo)}")
        except Exception as exc:
            raise RuntimeError(
                f"o Trino não abriu o catálogo {catalogo!r}. Confira a Variable "
                f"{bronze.VARIABLE_CATALOGO}: ela deve ter o nome do catálogo do "
                f"banco no Trino deste ambiente. Erro original: {exc}"
            ) from exc
        trino.run(bronze.ddl_schema(catalogo, schema))
        for alvo in bronze.destinos(trino, catalogo, schema).values():
            alvo.garantir()
        logger.info("[senado_legislacao_dag] bronze em %s.%s", catalogo, schema)

    @task(execution_timeout=timedelta(hours=1))
    def listar() -> dict[str, Any]:
        run_id, params = _contexto()
        lista, _ = _arquivos_do_raw(sl.ENTIDADE_LISTA)
        pedidos = sl.a_listar(
            datetime.now(timezone.utc), sl.listagens_por_particao(lista), params["anos"]
        )
        return _listar(ClienteSenadores(), pedidos, run_id)

    @task(execution_timeout=timedelta(hours=2))
    def detalhar() -> dict[str, Any]:
        run_id, params = _contexto()
        prazo = _prazo(params)
        # Tudo o que fala com o MinIO e com as Variables antes das threads.
        lista, _ = _arquivos_do_raw(sl.ENTIDADE_LISTA)
        ultimas = sl.ultimas_listagens(sl.listagens_por_particao(lista))
        if params["anos"]:
            ultimas = {k: v for k, v in ultimas.items() if k[1] in params["anos"]}
        listados = sl.ids_listados(
            (k, sl.ler_envelope(a)) for k, a in sorted(ultimas.items())
        )
        baixados = sl.ids_baixados(_arquivos_do_raw(sl.ENTIDADE_DETALHE)[0])
        datalakehouse.limite_objeto_bytes()
        resumo = _detalhar(
            ClienteSenadores(),
            sl.pendentes(listados, baixados),
            run_id,
            prazo,
            int(params["paralelo"]),
        )
        resumo["por_tipo"] = sl.resumo_por_tipo(
            listados, baixados | set(resumo.pop("ids_baixados"))
        )
        logger.info(
            "[senado_legislacao_dag] detalhe: %s",
            {k: v for k, v in resumo.items() if k != "por_tipo"},
        )
        return resumo

    @task(execution_timeout=timedelta(hours=2))
    def carregar_bronze() -> dict[str, Any]:
        run_id, params = _contexto()
        catalogo, schema = _alvo()
        trino = _Trino()
        feitos = {
            str(linha[0])
            for linha in trino.records(bronze.sql_carregados(catalogo, schema))
        }
        return _carregar(
            bronze.destinos(trino, catalogo, schema), feitos, _prazo(params), run_id
        )

    @task
    def conferir(
        lista: dict[str, Any], detalhe: dict[str, Any], carga: dict[str, Any]
    ) -> dict[str, Any]:
        """Linhas da bronze por arquivo contra o registro, e o veredito do run."""
        catalogo, schema = _alvo()
        trino = _Trino()
        registros = trino.records(bronze.sql_registros(catalogo, schema))
        contagens = {
            nome: {
                str(arquivo): int(n)
                for arquivo, n in trino.records(
                    bronze.sql_contagem_por_arquivo(catalogo, schema, nome)
                )
            }
            for nome in (bronze.TABELA_LISTA, bronze.TABELA_DETALHE, bronze.TABELA_SECOES)
        }
        problemas, avisos = bronze.conferir(registros, contagens)
        return _veredito(lista, detalhe, carga, problemas, avisos)

    lista = listar()
    detalhe = detalhar()
    carga = carregar_bronze()
    preparar() >> lista >> detalhe >> carga
    conferir(lista, detalhe, carga)


senado_legislacao_dag()
