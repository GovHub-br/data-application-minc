"""Ponte staging -> Postgres das entidades do TransfereGov.

As DAGs de API gravam no datalakehouse; o dbt (``transferegov_bronze`` e as
silvers de ``cotas_dbt``) continua lendo a source ``transferegov`` do
Postgres. Esta ponte mantém as duas pontas ligadas: a cada Parquet novo no
staging, faz upsert na tabela que a ingestão alimentava antes.

Uma DAG por entidade, cada uma disparada pelo Asset do staging dela. A key do
arquivo vem no ``extra`` do evento -- nunca "o mais recente do prefixo" --,
e o upsert pela chave natural mantém a tabela cumulativa: um run em que a API
devolveu menos linhas não apaga as que já estavam lá.

O Parquet já chega com o contrato que a tabela tem: colunas achatadas com
``__``, em minúsculas, tudo texto. Por isso nenhum modelo dbt muda.
"""

from datetime import datetime, timedelta

from airflow.sdk import dag, get_current_context, task

import datalakehouse
import schemas_minc as schemas
from cliente_postgres import ClientPostgresDB
from openmetadata.lineage import publicar_linhagem, tabela
from postgres_helpers import get_postgres_conn

# entidade no datalakehouse -> (tabela no Postgres, chave do upsert). A
# chave é a mesma que o insert_data usava antes da mudança para o MinIO.
_PONTES: dict[str, tuple[str, list[str]]] = {
    schemas.TABELA_PROGRAMA: (schemas.TABELA_PROGRAMA, ["id_programa"]),
    schemas.TABELA_PLANO_ACAO: (schemas.TABELA_PLANO_ACAO, ["id_plano_acao"]),
    schemas.TABELA_PLANO_ACAO_META: (
        schemas.TABELA_PLANO_ACAO_META,
        ["id_meta_plano_acao"],
    ),
    schemas.TABELA_PLANO_ACAO_DADO_BANCARIO: (
        schemas.TABELA_PLANO_ACAO_DADO_BANCARIO,
        ["id_plano_acao_dado_bancario"],
    ),
    schemas.TABELA_RELATORIO_GESTAO: (
        schemas.TABELA_RELATORIO_GESTAO,
        ["id_relatorio_gestao"],
    ),
    schemas.TABELA_ANEXO_RELATORIO: (schemas.TABELA_ANEXO_RELATORIO, ["id"]),
    schemas.ENTIDADE_LANCAMENTOS: (
        "raw_gestao_financeira_lancamentos",
        ["id_lancamento_gestao_financeira"],
    ),
    schemas.ENTIDADE_SUBTRANSACOES: (
        "raw_gestao_financeira_subtransacoes",
        ["id_subtransacao_gestao_financeira"],
    ),
}

default_args = {
    "owner": "Caio Borges",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def _criar_ponte(entidade: str, tabela_destino: str, chave: list[str]) -> None:
    asset_staging = datalakehouse.asset_staging(schemas.FONTE_TRANSFEREGOV, entidade)

    @dag(
        dag_id=f"staging_para_postgres_{entidade}_dag",
        schedule=[asset_staging],
        start_date=datetime(2023, 1, 1),
        catchup=False,
        default_args=default_args,
        # Dois runs da mesma ponte em paralelo disputariam o mesmo upsert.
        max_active_runs=1,
        tags=["minc", "transferegov", "datalakehouse", "ponte"],
    )
    def ponte() -> None:
        @task(outlets=[tabela(schemas.SCHEMA_TRANSFEREGOV, tabela_destino)])
        def carregar() -> None:
            """Upsert de cada Parquet que disparou o run, do mais antigo ao mais novo.

            Se a DAG produtora rodou mais de uma vez antes desta, os eventos
            chegam juntos; carregar todos em ordem é o que o upsert faria se
            cada um tivesse tido o seu run.
            """
            contexto = get_current_context()
            eventos = contexto["triggering_asset_events"].get(asset_staging, [])
            # Sem evento (run manual), a key vem do conf: {"<entidade>": "staging/..."}
            extras = [e.extra for e in eventos] or [contexto["dag_run"].conf or {}]
            keys = [
                datalakehouse.key_da_cadeia(
                    datalakehouse.cadeia_do_gatilho([extra]), entidade
                )
                for extra in extras
            ]

            db = ClientPostgresDB(get_postgres_conn())
            for key in keys:
                db.insert_data(
                    datalakehouse.parquet_para_registros(
                        datalakehouse.ler_parquet(key), chave
                    ),
                    table_name=tabela_destino,
                    primary_key=chave,
                    conflict_fields=chave,
                    schema=schemas.SCHEMA_TRANSFEREGOV,
                )

        carregar() >> publicar_linhagem()

    ponte()


for _entidade, (_tabela, _chave) in _PONTES.items():
    _criar_ponte(_entidade, _tabela, _chave)
