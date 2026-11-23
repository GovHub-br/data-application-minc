"""Catálogo do SIDRA (API de Agregados v3 do IBGE) no schema ``ibge_sidra``.

``extrair_catalogo`` grava ``pesquisas`` e ``agregados_catalogo``. As tasks
mapeadas gravam ``metadados``, ``periodos``, ``variaveis`` e ``localidades``,
um lote de agregados por task. ``extrair_resultados`` grava em ``resultados``
os valores dos agregados da Variable ``ibge_config``, uma task por agregado.

Os agregados de ``resultados`` vêm da chave ``resultados`` da Variable, um item
por agregado, cada um com suas variáveis, períodos, categorias e níveis::

    {"resultados": [
        {"agregado": "9605", "variaveis": "1000093", "periodos": "2022",
         "classificacao": "86[2776,2777,2778,2779,2780]", "niveis": ["N6", "N3"]}
    ]}

Só ``agregado`` é obrigatório: sem o resto, valem todas as variáveis
(``allxp``), o último período e o nível de ``resultados_nivel``. O formato
antigo (``resultados_agregados_ids`` com um período e um nível para todos)
continua aceito, mas o mesmo agregado nos dois formatos faz o run falhar.

Reexecutar não duplica e não derruba tabela: o dbt tem views em cima delas
(``ibge_sidra.bronze_*``), e o Postgres recusa o ``DROP``. Cada carga
substitui, numa transação, o que ela traz de novo:

- ``pesquisas`` e ``agregados_catalogo``: a tabela inteira;
- ``periodos``, ``variaveis`` e ``localidades``: as linhas dos agregados que
  voltaram da API no lote. O agregado em que a API falhou fica com as linhas
  do run anterior;
- ``resultados``: as linhas do agregado, em todos os níveis. A tabela espelha a
  Variable: período ou categoria que sai dela sai do banco, e o agregado que
  sai dela é apagado por ``apagar_resultados_fora_da_config``. Com a lista
  vazia nada é apagado: a Variable que sumiu não esvazia a tabela. Se um nível
  vier vazio, a task falha sem gravar, e o agregado fica com as linhas
  anteriores;
- ``metadados``: upsert pela chave.

``localidades`` é substituída por agregado, não por nível: trocar
``localidades_nivel`` apaga o nível anterior dos agregados que têm o novo.
"""

import json
import logging
from datetime import datetime, timedelta
from typing import Any

from airflow.sdk import dag, task
from airflow.sdk import Variable
from airflow.sdk.exceptions import AirflowFailException

from cliente_postgres import ClientPostgresDB
from extracao_ibge import (
    ClienteIBGE,
    EspecificacaoResultados,
    especificacoes_resultados,
)
from postgres_helpers import get_postgres_conn

logger = logging.getLogger(__name__)

SCHEMA = "ibge_sidra"

default_args = {
    "owner": "Wallyson Souza",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


_IBGE_CONFIG_DEFAULTS: dict[str, Any] = {
    "batch_size": 50,
    "localidades_nivel": "N3",
    # Agregados cujos valores entram em ibge_sidra.resultados, um item por
    # agregado (ver especificacoes_resultados). Exemplo: a população residente
    # do Censo 2022 por município, que o Eixo 1 do Cultura em Números usa no
    # per capita e no porte municipal:
    #   {"resultados": [{"agregado": "4709", "periodos": "2022",
    #                    "niveis": ["N6"]}]}
    "resultados": [],
    # Formato antigo, ainda aceito: os mesmos período e nível para todos.
    "resultados_agregados_ids": [],
    "resultados_periodos": "ultimo",
    "resultados_nivel": "N3",
}

# Uma linha por agregado, variável, localidade, período e classificação. Sem
# chave, cada run semanal gravava os mesmos valores de novo.
CHAVE_RESULTADOS = [
    "agregado_id",
    "variavel_id",
    "nivel",
    "localidade_id",
    "periodo",
    "classificacoes_chave",
]


def _get_config() -> dict[str, Any]:
    """Lê a variável ``ibge_config`` e mescla com os defaults."""
    config = Variable.get("ibge_config", default={}, deserialize_json=True)
    return {**_IBGE_CONFIG_DEFAULTS, **config}


# Cada task mapeada substitui só os agregados do seu lote, para não apagar o
# que as outras gravaram.
RECORTE_LOTE = "agregado_id"


def _batch(lista: list, tamanho: int) -> list[list]:
    """Divide uma lista em sublistas de tamanho fixo."""
    return [lista[i : i + tamanho] for i in range(0, len(lista), tamanho)]


def _ids_em_batches(
    catalogo: list[dict[str, Any]] | None, tamanho: int
) -> list[list[str]]:
    """IDs do catálogo em lotes, para o dynamic task mapping.

    ``catalogo`` é o retorno de ``extrair_catalogo``. O Airflow entrega
    ``None`` quando essa task terminou em sucesso sem ter rodado (marcada como
    sucesso na interface), e iterar sobre ``None`` dava ``TypeError:
    'NoneType' object is not iterable``. Tentar de novo não traz o XCom, então
    a falha é sem retry.

    Cada ID entra num lote só: o mesmo agregado em dois lotes paralelos seria
    substituído duas vezes ao mesmo tempo, e as duas cargas ficariam.
    """
    if catalogo is None:
        raise AirflowFailException(
            "[ibge_dag] extrair_catalogo não deixou o catálogo no XCom: a task "
            "foi marcada como sucesso sem rodar? Limpe-a (clear) para rodar de novo"
        )
    ids = list(dict.fromkeys(str(agr["id"]) for agr in catalogo))
    return _batch(ids, tamanho)


# O corpo das tasks mora aqui, fora da função da DAG: dentro dela, cada ``if``
# de cada task somava na complexidade de uma função só (C901).


def _pesquisas_planas(pesquisas: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Uma linha por pesquisa, com os agregados dela em JSON."""
    if not pesquisas:
        raise ValueError("[ibge_dag] Nenhuma pesquisa retornada da API")
    return [
        {
            "id": p.get("id"),
            "nome": p.get("nome"),
            "agregados": json.dumps(p.get("agregados", []), ensure_ascii=False),
        }
        for p in pesquisas
    ]


def _upsert_metadados(dados: list[dict[str, Any]], batch: list[str]) -> None:
    """Metadados têm chave: upsert, e o agregado em que a API falhou fica."""
    if not dados:
        logger.warning("[ibge_dag] metadados: batch sem resultados %s", batch[:3])
        return
    db = ClientPostgresDB(get_postgres_conn())
    db.insert_data(
        dados,
        table_name="metadados",
        schema=SCHEMA,
        primary_key=["agregado_id"],
        conflict_fields=["agregado_id"],
    )
    logger.info("[ibge_dag] metadados inseridos: %s registros", len(dados))


def _substituir_lote(dados: list[dict[str, Any]], tabela: str, batch: list[str]) -> None:
    """Grava o que um lote trouxe no lugar das linhas dos mesmos agregados."""
    if not dados:
        logger.warning("[ibge_dag] %s: batch sem resultados %s", tabela, batch[:3])
        return
    db = ClientPostgresDB(get_postgres_conn())
    db.substituir_dados(
        dados, table_name=tabela, schema=SCHEMA, coluna_recorte=RECORTE_LOTE
    )
    logger.info("[ibge_dag] %s inseridos: %s registros", tabela, len(dados))


def _especificacoes(config: dict[str, Any]) -> list[EspecificacaoResultados]:
    """``especificacoes_resultados``, com a configuração inválida sem retry."""
    try:
        return especificacoes_resultados(config)
    except ValueError as erro:
        raise AirflowFailException(
            f"[ibge_dag] ibge_config inválida para resultados: {erro}"
        ) from erro


def _resultados_do_agregado(
    api: ClienteIBGE, spec: EspecificacaoResultados
) -> list[dict[str, Any]]:
    """As linhas do agregado em todos os níveis pedidos.

    Nível vazio levanta ``RuntimeError``: a API devolve ``[]`` quando responde
    com erro, e gravar os outros níveis apagaria as linhas dele.
    """
    agregado = spec["agregado"]
    linhas: list[dict[str, Any]] = []
    for nivel in spec["niveis"]:
        dados = api.get_resultados_achatar(
            agregado_id=agregado,
            periodos=spec["periodos"],
            variaveis=spec["variaveis"],
            nivel=nivel,
            classificacao=spec["classificacao"],
        )
        if not dados:
            raise RuntimeError(
                f"[ibge_dag] resultados: agregado={agregado} nivel={nivel} "
                "sem dados; nada foi gravado e ficam as linhas do run anterior. "
                "Se a falha se repete a cada run, confira se o agregado tem esse "
                f"nível (GET /agregados/{agregado}/localidades/{nivel}); se não "
                "tiver, tire-o de 'niveis' na ibge_config"
            )
        logger.info(
            "[ibge_dag] resultados: agregado=%s nivel=%s → %s linhas, %s localidades",
            agregado,
            nivel,
            len(dados),
            len({linha["localidade_id"] for linha in dados}),
        )
        linhas += dados
    return linhas


@dag(
    dag_id="ibge_agregados_dag",
    schedule="0 3 * * 0",  # toda semana às 3h de domingo
    start_date=datetime(2024, 1, 1),
    catchup=False,
    # O catálogo leva horas. Um run manual no meio do semanal gravava os mesmos
    # agregados ao mesmo tempo: em resultados, a chave primária barra o
    # segundo, que falha (visto no teste local de 19/11/2026).
    max_active_runs=1,
    default_args=default_args,
    tags=["extração", "ibge", "agregados", "raw"],
    doc_md=__doc__,
)
def ibge_agregados_dag() -> None:

    # ------------------------------------------------------------------
    # Task 1 – Catálogo: pesquisas e agregados
    # ------------------------------------------------------------------

    @task
    def extrair_catalogo() -> list[dict[str, Any]]:
        """Extrai GET /agregados e carrega pesquisas + catálogo de agregados."""
        api = ClienteIBGE()

        pesquisas = api.get_pesquisas()
        pesquisas_planas = _pesquisas_planas(pesquisas)

        db = ClientPostgresDB(get_postgres_conn())
        db.substituir_dados(pesquisas_planas, table_name="pesquisas", schema=SCHEMA)
        logger.info("[ibge_dag] pesquisas carregadas: %s", len(pesquisas_planas))

        # Tabela plana de todos os agregados (reusa pesquisas já buscadas)
        agregados = api.listar_agregados(pesquisas=pesquisas)
        db.substituir_dados(agregados, table_name="agregados_catalogo", schema=SCHEMA)
        logger.info("[ibge_dag] agregados_catalogo carregados: %s", len(agregados))

        return agregados

    # ------------------------------------------------------------------
    # Task 2 – Prepara batches para extração paralela
    # ------------------------------------------------------------------

    @task
    def preparar_batches(catalogo: list[dict[str, Any]] | None) -> list[list[str]]:
        """Agrupa IDs de agregados em batches para o dynamic task mapping."""
        batch_size = int(_get_config()["batch_size"])
        batches = _ids_em_batches(catalogo, batch_size)
        logger.info(
            "[ibge_dag] %s IDs → %s batches de %s",
            sum(len(b) for b in batches),
            len(batches),
            batch_size,
        )
        return batches

    # ------------------------------------------------------------------
    # Tasks dinâmicas – metadados, períodos, variáveis, localidades
    # ------------------------------------------------------------------

    @task(max_active_tis_per_dagrun=4)
    def extrair_metadados(batch: list[str]) -> None:
        """GET /agregados/{id}/metadados para cada ID do batch."""
        _upsert_metadados(ClienteIBGE().get_metadados_batch(batch), batch)

    @task(max_active_tis_per_dagrun=4)
    def extrair_periodos(batch: list[str]) -> None:
        """GET /agregados/{id}/periodos para cada ID do batch."""
        _substituir_lote(ClienteIBGE().get_periodos_batch(batch), "periodos", batch)

    @task(max_active_tis_per_dagrun=4)
    def extrair_variaveis(batch: list[str]) -> None:
        """GET /agregados/{id}/variaveis para cada ID do batch."""
        _substituir_lote(ClienteIBGE().get_variaveis_batch(batch), "variaveis", batch)

    @task(max_active_tis_per_dagrun=4)
    def extrair_localidades(batch: list[str]) -> None:
        """GET /agregados/{id}/localidades/{nivel} para cada ID do batch."""
        nivel = _get_config()["localidades_nivel"]
        dados = ClienteIBGE().get_localidades_batch(batch, nivel=nivel)
        _substituir_lote(dados, "localidades", batch)

    # ------------------------------------------------------------------
    # Task de resultados (séries) – apenas para agregados configurados
    # ------------------------------------------------------------------

    @task
    def preparar_resultados() -> list[EspecificacaoResultados]:
        """Os agregados de ``ibge_config`` cujos valores vão para ``resultados``.

        Configuração inválida falha sem retry: tentar de novo não a conserta.
        Lista vazia deixa ``extrair_resultados`` sem nada a fazer (pulada).
        """
        especificacoes = _especificacoes(_get_config())
        logger.info(
            "[ibge_dag] resultados: %s agregados configurados: %s",
            len(especificacoes),
            [e["agregado"] for e in especificacoes],
        )
        return especificacoes

    @task(max_active_tis_per_dagrun=2)
    def extrair_resultados(spec: EspecificacaoResultados) -> None:
        """GET /agregados/{id}/periodos/{per}/variaveis/{var} de um agregado,
        em cada nível pedido, gravados juntos no lugar das linhas anteriores.

        Nível vazio é falha, não dado: a API devolve ``[]`` quando responde com
        erro. A task falha antes de gravar (o retry tenta de novo), e o agregado
        fica com as linhas do run anterior.
        """
        agregado = spec["agregado"]
        linhas = _resultados_do_agregado(ClienteIBGE(), spec)

        with ClientPostgresDB(get_postgres_conn()) as db:
            db.substituir_dados(
                linhas,
                table_name="resultados",
                schema=SCHEMA,
                coluna_recorte="agregado_id",
                primary_key=CHAVE_RESULTADOS,
            )
        logger.info(
            "[ibge_dag] resultados: agregado=%s → %s linhas gravadas",
            agregado,
            len(linhas),
        )

    @task
    def apagar_resultados_fora_da_config(
        especificacoes: list[EspecificacaoResultados],
    ) -> None:
        """Tira de ``resultados`` os agregados que saíram da ``ibge_config``.

        ``extrair_resultados`` só substitui os agregados que estão na Variable:
        sem esta task, o que sai dela ficava no banco e na silver. Com a lista
        vazia nada é apagado.
        """
        manter = [e["agregado"] for e in especificacoes]
        with ClientPostgresDB(get_postgres_conn()) as db:
            db.apagar_fora_do_recorte("resultados", SCHEMA, "agregado_id", manter)

    # ------------------------------------------------------------------
    # Grafo da DAG
    # ------------------------------------------------------------------

    catalogo = extrair_catalogo()
    batches = preparar_batches(catalogo)

    extrair_metadados.expand(batch=batches)
    extrair_periodos.expand(batch=batches)
    extrair_variaveis.expand(batch=batches)
    extrair_localidades.expand(batch=batches)

    # Resultados só corre após o catálogo estar carregado
    especificacoes = preparar_resultados()
    catalogo >> especificacoes
    extrair_resultados.expand(spec=especificacoes)
    apagar_resultados_fora_da_config(especificacoes)


ibge_agregados_dag()
