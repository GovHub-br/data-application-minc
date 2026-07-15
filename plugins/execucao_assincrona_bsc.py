"""Ponte generica TaskFlow (sincrono) -> ``AsyncBscClient`` (assincrono).

Cada ``@task`` do Airflow roda como uma chamada Python sincrona; para
aproveitar o cliente assincrono (com seu Semaphore/throttle/retry) dentro de
uma task, o padrao e abrir um event loop com ``asyncio.run()`` na fronteira
da task e devolver so o resultado (contagens), nunca os dados brutos, via
XCom. Este modulo concentra esse padrao para nao repeti-lo em cada DAG.
"""

import asyncio
import logging
from typing import Any, Awaitable, Callable, NamedTuple, Optional

import aiohttp

from cliente_bsc import AsyncBscClient, BscRequestError, build_async_client

logger = logging.getLogger(__name__)


class ResultadoItem(NamedTuple):
    """Resultado de uma chamada individual, mantido em memoria (nunca via
    XCom) para quem chama ``executar_lote`` persistir raw + controle em
    lote no Postgres depois que o loop assincrono termina."""

    item: Any
    status: str  # "ok" | "sem_dados" | "erro"
    dados: Optional[dict[str, Any]]
    mensagem_erro: Optional[str]


async def _extrair_lote_async(
    session: aiohttp.ClientSession,
    itens_pendentes: list[Any],
    chamar_api: Callable[[AsyncBscClient, Any], Awaitable[Any]],
    tratar_resposta_vazia: Optional[Callable[[BscRequestError], Optional[dict]]],
) -> list[ResultadoItem]:
    client = build_async_client(session)

    async def _extrair_um(item: Any) -> ResultadoItem:
        try:
            dados = await chamar_api(client, item)
            return ResultadoItem(item, "ok", dados, None)
        except BscRequestError as exc:
            if exc.status_code in (401, 403, 429):
                # Credencial invalida ou rate limit: aborta a execucao
                # inteira, nao adianta seguir tentando os demais itens.
                raise

            if tratar_resposta_vazia is not None:
                marcador = tratar_resposta_vazia(exc)
                if marcador is not None:
                    return ResultadoItem(item, "sem_dados", None, marcador.get("erro"))

            logger.error(
                "[execucao_assincrona_bsc] Erro nao retriavel | item=%s | status=%s",
                item,
                exc.status_code,
            )
            return ResultadoItem(item, "erro", None, str(exc))
        except Exception as exc:
            logger.exception("[execucao_assincrona_bsc] Falha inesperada | item=%s", item)
            return ResultadoItem(item, "erro", None, str(exc))

    return await asyncio.gather(*[_extrair_um(item) for item in itens_pendentes])


def executar_lote(
    itens_pendentes: list[Any],
    chamar_api: Callable[[AsyncBscClient, Any], Awaitable[Any]],
    tratar_resposta_vazia: Optional[Callable[[BscRequestError], Optional[dict]]] = None,
) -> list[ResultadoItem]:
    """Roda ``chamar_api`` para cada item pendente (concorrencia/retry
    controlados pelo ``AsyncBscClient``) e devolve o resultado de cada um,
    acumulado em memoria. Quem chama insere no Postgres em lote (raw +
    controle) depois que ``asyncio.run`` retorna -- psycopg2 e sincrono, uma
    escrita por resposta dentro do ``asyncio.gather`` serializaria a
    concorrencia controlada pelo Semaphore/throttle do ``AsyncBscClient``."""

    if not itens_pendentes:
        logger.info("[execucao_assincrona_bsc] Nada pendente, nenhuma chamada necessaria")
        return []

    async def _main() -> list[ResultadoItem]:
        async with aiohttp.ClientSession() as session:
            return await _extrair_lote_async(
                session, itens_pendentes, chamar_api, tratar_resposta_vazia
            )

    resultados = asyncio.run(_main())

    contagem: dict[str, int] = {"ok": 0, "sem_dados": 0, "erro": 0}
    for resultado in resultados:
        contagem[resultado.status] = contagem.get(resultado.status, 0) + 1
    logger.info("[execucao_assincrona_bsc] Resultado do lote: %s", contagem)

    return resultados
