import base64
import logging
from collections import Counter

import pandas as pd
import requests
from airflow.sdk import dag, task
from airflow.sdk import TriggerRule
from datetime import datetime, timedelta


import datalakehouse
import schemas_minc as schemas
from extracao_por_plano_acao import (
    ids_ja_baixados,
    juntar_anexos_ao_plano,
    key_anexo_arquivo,
    politica_do_programa,
)

URL_BASE_RG = "https://fundos.transferegov.sistema.gov.br/maisbrasil-transferencia-backend/api/public/anexos/rg/"

_EXTENSOES_PLANILHA = (".xls", ".xlsx", ".ods")
# Teto por rodada: com max_active_tis_per_dag=3 isso ja e horas de download.
_LIMITE_POR_RODADA = 3000

# Anexos por task mapeada. O Airflow recusa expandir acima de `max_map_length`
# (padrao 1024; o compose local sobe 15000, homologacao nao) e derruba a
# buscar_ids_pendentes depois do return. Mapear sobre lotes, como a
# salic_ingestion_trino e a extracao_anexos_dag, nao depende dessa config:
# 3000 / 50 = 60 tasks.
_ANEXOS_POR_LOTE = 50

ASSET_ANEXOS = datalakehouse.asset_staging(
    schemas.FONTE_TRANSFEREGOV, schemas.TABELA_ANEXO_RELATORIO
)
# Evento que dispara a extracao_anexos_dag, com a mesma cadeia de keys.
ASSET_ANEXOS_ARQUIVOS = datalakehouse.asset(schemas.PREFIXO_ANEXOS_ARQUIVOS)

default_args = {
    "owner": "Wallyson Souza",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="download_anexos_transferegov_dag",
    default_args=default_args,
    schedule=[ASSET_ANEXOS],
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["minc", "transferegov", "extracao", "anexos"],
)
def download_anexos_dag() -> None:

    @task
    def buscar_ids_pendentes() -> list[list[dict]]:
        """Anexos de planilha que ainda não estão em ``raw/.../anexos_arquivos/``,
        em lotes de ``_ANEXOS_POR_LOTE``.

        A lista sai do staging (anexo -> relatório -> plano de ação, para saber
        o programa e daí a pasta LPG/PNAB) menos o que já foi baixado. Quem
        diz "já baixado" é o próprio bucket, não uma coluna no banco. Os três
        arquivos de staging são os da cadeia que disparou o run.
        """
        cadeia = datalakehouse.ler_cadeia(ASSET_ANEXOS)

        def staging(entidade: str) -> pd.DataFrame:
            return datalakehouse.ler_parquet(
                datalakehouse.key_da_cadeia(cadeia, entidade)
            )

        anexos = juntar_anexos_ao_plano(
            staging(schemas.TABELA_ANEXO_RELATORIO),
            staging(schemas.TABELA_RELATORIO_GESTAO),
            staging(schemas.TABELA_PLANO_ACAO),
        )
        anexos = anexos[
            anexos["nome"].fillna("").str.lower().str.endswith(_EXTENSOES_PLANILHA)
        ]

        baixados = ids_ja_baixados(
            o["Key"]
            for o in datalakehouse.listar_objetos(schemas.PREFIXO_ANEXOS_ARQUIVOS)
        )
        pendentes = [
            {"id": str(linha.id), "politica": politica_do_programa(linha.id_programa)}
            for linha in anexos.drop_duplicates("id").itertuples()
            if str(linha.id) not in baixados
        ]
        # LPG e PNAB antes de `outros`, e so depois o teto: so as duas primeiras
        # viram tabela na extracao_anexos_dag, entao sao elas que nao podem
        # esperar outra rodada. `outros` continua sendo baixado, por ultimo. O
        # sorted e estavel: dentro de cada grupo, a ordem do staging se mantem.
        pendentes = sorted(pendentes, key=lambda p: p["politica"] == "outros")
        pendentes = pendentes[:_LIMITE_POR_RODADA]

        lotes = [
            pendentes[i : i + _ANEXOS_POR_LOTE]
            for i in range(0, len(pendentes), _ANEXOS_POR_LOTE)
        ]

        por_politica = Counter(p["politica"] for p in pendentes)
        logging.info(
            "%d anexos de planilha no staging, %d já baixados, %d nesta rodada "
            "em %d lotes (lpg %d, pnab %d, outros %d)",
            len(anexos),
            len(baixados),
            len(pendentes),
            len(lotes),
            por_politica["lpg"],
            por_politica["pnab"],
            por_politica["outros"],
        )
        return lotes

    def _baixar_e_salvar(pendente: dict) -> None:
        """Faz o request na API, decodifica o base64 e grava o binário em
        ``raw/transferegov/anexos_arquivos/<politica>/`` -- em partes, se passar
        do limite do proxy do MinIO (``datalakehouse.gravar_bytes_em_partes``).
        """
        anexo_id = pendente["id"]

        response = requests.get(f"{URL_BASE_RG}{anexo_id}", timeout=15)
        response.raise_for_status()

        dados_json = response.json()
        arquivo_base64 = dados_json.get("arquivo")
        nome_arquivo = dados_json.get("nome", "arquivo_sem_nome.bin")

        if not arquivo_base64:
            logging.warning(
                f"Anexo {anexo_id} não possui a chave 'arquivo' com conteúdo em base64."
            )
            return

        key = datalakehouse.gravar_bytes_em_partes(
            base64.b64decode(arquivo_base64),
            key_anexo_arquivo(pendente["politica"], anexo_id, nome_arquivo),
        )
        logging.info(f"Anexo {anexo_id} salvo em s3://{datalakehouse.BUCKET}/{key}")

    @task(max_active_tis_per_dag=3)
    def baixar_e_salvar_anexos(lote: list[dict]) -> None:
        """
        Processa um lote de anexos por invocação (Dynamic Task Mapping), um
        anexo de cada vez. A concorrência é controlada por
        max_active_tis_per_dag=3 para evitar HTTP 429 (rate limit) na API do
        governo: são no máximo três requisições ao mesmo tempo.

        Anexo que falha não derruba o lote: fica fora do bucket e volta como
        pendente na próxima rodada. O lote só falha se nenhum anexo dele
        passou -- sinal de API fora ou bloqueada, que tem que aparecer
        vermelho na grade.
        """
        falhas = 0
        for pendente in lote:
            try:
                _baixar_e_salvar(pendente)
            except Exception as exc:
                falhas += 1
                logging.warning("Anexo %s falhou: %r", pendente["id"], exc)

        logging.info("Lote: %d de %d anexos com falha", falhas, len(lote))
        if lote and falhas == len(lote):
            raise RuntimeError(f"Todos os {len(lote)} anexos do lote falharam")

    # Roda mesmo se alguns downloads falharem (ALL_DONE): o que foi baixado
    # já pode ser extraído, e o que falhou volta como pendente na próxima.
    @task(outlets=[ASSET_ANEXOS_ARQUIVOS], trigger_rule=TriggerRule.ALL_DONE)
    def anunciar_downloads() -> None:
        datalakehouse.publicar_cadeia(
            ASSET_ANEXOS_ARQUIVOS, datalakehouse.ler_cadeia(ASSET_ANEXOS)
        )

    # Fluxo da DAG — Dynamic Task Mapping: cada lote de pendentes vira uma task
    lotes_pendentes = buscar_ids_pendentes()
    baixar_e_salvar_anexos.expand(lote=lotes_pendentes) >> anunciar_downloads()


# Instancia a DAG
dag_instance = download_anexos_dag()
