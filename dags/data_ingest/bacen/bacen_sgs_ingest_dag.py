"""DAG de ingestão de séries temporais do SGS/BACEN.

Por que um indicador do BACEN no banco do MinC: os valores de repasse e de
pagamento das políticas (LPG 2022, PNAB 2023...) são nominais e se espalham
por anos, então comparar 2022 com 2026 sem deflator engana. A série que
motivou a DAG é o IPCA-Serviços (SGS 10844) — o recorte do IPCA que mede
preço de serviço, que é o que a maior parte do edital de cultura contrata
(cachê, produção, oficina), e por isso corrige esses valores melhor do que o
IPCA cheio:
https://dadosabertos.bcb.gov.br/dataset/10844-indice-de-precos-ao-consumidor-amplo-ipca---servicos

**Quais séries entram é configuração, não código.** A Variable
``bacen_series_sgs`` diz o que ingerir, e puxar mais um código do BACEN é
editá-la — nenhuma DAG nova, nenhum deploy::

    {
        "ipca_servicos":    10844,
        "ipca":               433,
        "igpm":               189
    }

Para série que precisa de recorte de datas (série diária com histórico
longo, que o SGS não entrega de uma vez), a forma longa também vale::

    [
        {
            "serie": "selic",
            "codigo": 11,
            "data_inicial": "01/01/2020"
        }
    ]

Sem a Variable, a DAG ingere só o IPCA-Serviços. Todas as séries caem na
mesma tabela, identificadas por ``serie``/``codigo_serie`` — assim uma série
nova não exige tabela nova nem source dbt novo.

A cada execução as séries são recarregadas por inteiro — a 10844 são ~430
pontos, 15 KB — e o UPSERT por (``codigo_serie``, ``data``) reaproveita as
linhas que já existem. Não há estado a manter entre execuções, e revisão do
IBGE em ponto antigo entra sozinha.
"""

import logging
from datetime import datetime, timedelta
from typing import Any

from airflow.sdk import Variable, dag, task

from cliente_bacen import ClienteBacen, configuracoes_de_series, normalizar_registros
from cliente_postgres import ClientPostgresDB
from postgres_helpers import get_postgres_conn
from schedule_loader import get_dynamic_schedule

# Schema próprio: é indicador externo, não dado de programa do MinC — a
# origem, a cadência e o dono do dado são outros.
_SCHEMA = "bacen"
_TABLE = "serie_sgs"

# SGS 10844 — IPCA Serviços (variação % mensal). Só o default: o que vale é
# a Variable ``bacen_series_sgs``.
_SERIES_PADRAO = {"ipca_servicos": 10844}

default_args = {
    "owner": "Lucas Bottino",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="bacen_sgs_ingest_dag",
    schedule=get_dynamic_schedule("bacen_sgs_ingest_dag", default="@monthly"),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["minc", "bacen", "sgs", "indicadores", "raw"],
)
def bacen_sgs_ingest_dag() -> None:
    @task
    def extrair_series() -> list[dict[str, Any]]:
        configuracoes = configuracoes_de_series(
            Variable.get(
                "bacen_series_sgs", default=_SERIES_PADRAO, deserialize_json=True
            )
        )
        logging.info(
            "[bacen_sgs_ingest_dag.py] %d série(s) configurada(s): %s",
            len(configuracoes),
            ", ".join(f"{c['serie']}={c['codigo']}" for c in configuracoes),
        )

        api = ClienteBacen()
        linhas: list[dict[str, Any]] = []
        vazias: list[str] = []

        for configuracao in configuracoes:
            serie, codigo = configuracao["serie"], configuracao["codigo"]

            registros = api.get_serie(
                codigo,
                data_inicial=configuracao["data_inicial"],
                data_final=configuracao["data_final"],
            )
            linhas_da_serie = normalizar_registros(serie, codigo, registros)

            if not linhas_da_serie:
                # O SGS responde 404 tanto para série inexistente quanto para
                # intervalo sem ponto, e o cliente devolve [] nos dois casos.
                vazias.append(f"{serie}={codigo}")
                logging.warning(
                    "[bacen_sgs_ingest_dag.py] Série %s (%s) não retornou nenhum ponto",
                    serie,
                    codigo,
                )
                continue

            datas = [linha["data"] for linha in linhas_da_serie]
            logging.info(
                "[bacen_sgs_ingest_dag.py] Série %s (%s): %d pontos, de %s a %s",
                serie,
                codigo,
                len(linhas_da_serie),
                min(datas),
                max(datas),
            )
            linhas.extend(linhas_da_serie)

        if vazias:
            # Uma série mal configurada não impede a carga das outras, mas
            # precisa aparecer no log — o sintoma silencioso seria a tabela
            # simplesmente sem os pontos dela.
            logging.warning(
                "[bacen_sgs_ingest_dag.py] %d de %d série(s) sem dados: %s",
                len(vazias),
                len(configuracoes),
                ", ".join(vazias),
            )

        if not linhas:
            # Nenhuma série trouxe ponto: falhar evita a execução "verde" que
            # não gravou nada.
            raise ValueError(
                "[bacen_sgs_ingest_dag.py] Nenhuma das séries configuradas "
                "retornou dados do SGS"
            )

        return linhas

    @task
    def carregar_series(linhas: list[dict[str, Any]]) -> None:
        db = ClientPostgresDB(get_postgres_conn())
        db.insert_data(
            linhas,
            table_name=_TABLE,
            primary_key=["codigo_serie", "data"],
            conflict_fields=["codigo_serie", "data"],
            schema=_SCHEMA,
        )

        logging.info(
            "[bacen_sgs_ingest_dag.py] Carga concluída com %d registros em %s.%s",
            len(linhas),
            _SCHEMA,
            _TABLE,
        )

    carregar_series(extrair_series())


bacen_sgs_ingest_dag()
