import io
import logging
import re
from datetime import datetime, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

import pandas as pd
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from cliente_postgres import ClientPostgresDB
from extracao_planilhas import extrair_tabela_raw
from postgres_helpers import get_postgres_conn

_REGEX_LPG = re.compile(r"edita(?:is|l)", re.IGNORECASE)
_REGEX_PNAB = re.compile(r"contemplad|contratad|acompanhament", re.IGNORECASE)

_PROGRAMAS = [
    {
        "nome_programa": "LPG",
        "id_programa": [46, 47],
        "regex_header": r"edita(?:is|l)",
        "regex_flags": "IGNORECASE",
        "col_header_idx": 1,
        "col_categoria_idx": 0,
        "min_len_categoria": 6,
        "bucket": "anexos-lpg",
        "prefix": "",
        "schema": "transferegov_fundo_a_fundo",
        "table": "raw_tabelas_anexos_lpg",
    },
    {
        "nome_programa": "PNAB",
        "id_programa": [60, 61, 62],
        "regex_header": r"contemplad|contratad|acompanhament",
        "regex_flags": "IGNORECASE",
        "col_header_idx": 1,
        "col_categoria_idx": 0,
        "min_len_categoria": 6,
        "bucket": "anexos-pnab",
        "prefix": "",
        "schema": "transferegov_fundo_a_fundo",
        "table": "raw_tabelas_anexos_pnab",
    },
]

_S3_CONN_ID = "minio_default"

default_args = {
    "owner": "Caio Borges",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="minc_extracao_anexos_dag",
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["transfere_gov", "anexos", "planilhas", "raw"],
)
def minc_extracao_anexos_dag() -> None:
    """DAG de extração de tabelas de anexos (XLSX/XLS/XLSM/ODS) para os
    programas LPG e PNAB, com Dynamic Task Mapping por arquivo.

    Fluxo:

    1. ``listar_arquivos_s3`` — descobre todos os arquivos de planilha nos
       buckets do MinIO e enriquece cada entrada com os parâmetros do
       programa (regex, bucket, tabela destino).
    2. ``baixar_e_extrair`` — para CADA arquivo (via ``.map()``), baixa do
       MinIO para diretório temporário, extrai as subtabelas com
       ``extrair_tabela_raw`` e limpa o arquivo temporário.
    3. ``carregar_no_postgres`` — persiste os registros no PostgreSQL.
    """

    @task
    def listar_arquivos_s3() -> list[dict[str, Any]]:
        """Lista arquivos de planilha em cada bucket do MinIO e enriquece
        cada entrada com os parâmetros de extração do programa."""
        ids_validos = set(
            Variable.get(
                "transferegov_programas_ids",
                default_var=[46, 47, 60, 61, 62],
                deserialize_json=True,
            )
        )

        hook = S3Hook(aws_conn_id=_S3_CONN_ID)
        arquivos_meta: list[dict[str, Any]] = []

        for prog in _PROGRAMAS:
            ids_prog = prog.get("id_programa", [])
            if isinstance(ids_prog, int):
                ids_prog = [ids_prog]
            if ids_prog and not any(i in ids_validos for i in ids_prog):
                logging.info(
                    "[minc_extracao_anexos_dag.py] Programa %s (IDs %s) "
                    "nao esta em transferegov_programas_ids — pulando",
                    prog["nome_programa"],
                    ids_prog,
                )
                continue

            nome = prog["nome_programa"]
            bucket = prog["bucket"]
            prefix = prog.get("prefix", "")

            logging.info(
                "[minc_extracao_anexos_dag.py] Listando arquivos para programa %s "
                "no bucket s3://%s/%s",
                nome,
                bucket,
                prefix,
            )

            try:
                keys = hook.list_keys(
                    bucket_name=bucket,
                    prefix=prefix,
                )
            except Exception as exc:
                logging.warning(
                    "[minc_extracao_anexos_dag.py] Erro ao listar bucket %s: %s — pulando",
                    bucket,
                    exc,
                )
                continue

            if not keys:
                logging.warning(
                    "[minc_extracao_anexos_dag.py] Nenhum arquivo encontrado no "
                    "bucket %s para o programa %s",
                    bucket,
                    nome,
                )
                continue

            extensoes_validas = {".xlsx", ".xls", ".xlsm", ".xlsb", ".ods"}

            for key in keys:
                ext = Path(key).suffix.lower()
                if ext not in extensoes_validas:
                    continue
                if Path(key).name.startswith("~$"):
                    continue

                arquivos_meta.append({
                    "key": key,
                    "bucket": bucket,
                    "nome_programa": nome,
                    "regex_header": prog["regex_header"],
                    "regex_flags": prog.get("regex_flags", "IGNORECASE"),
                    "col_header_idx": prog.get("col_header_idx", 1),
                    "col_categoria_idx": prog.get("col_categoria_idx", 0),
                    "min_len_categoria": prog.get("min_len_categoria", 6),
                    "schema": prog["schema"],
                    "table": prog["table"],
                })

            logging.info(
                "[minc_extracao_anexos_dag.py] Programa %s: %d arquivos encontrados",
                nome,
                sum(
                    1
                    for a in arquivos_meta
                    if a["nome_programa"] == nome
                ),
            )

        if not arquivos_meta:
            logging.warning(
                "[minc_extracao_anexos_dag.py] Nenhum arquivo encontrado em nenhum programa"
            )

        return arquivos_meta

    @task
    def baixar_e_extrair(file_meta: dict[str, Any]) -> list[dict[str, Any]]:
        """Baixa um arquivo do MinIO para memória, grava em diretório
        temporário local, extrai as subtabelas e remove o arquivo.

        O download usa S3Hook.get_key() + io.BytesIO em vez de
        download_file() para evitar FileNotFoundError quando o hook
        tenta resolver o path do objeto no MinIO.

        Fail-safe: nunca levanta exceção para não quebrar o Dynamic Task
        Mapping. Arquivos com erro retornam lista vazia com flag de erro.
        """
        nome_programa = file_meta["nome_programa"]
        bucket = file_meta["bucket"]
        key = file_meta["key"]

        flags = 0
        for flag_name in file_meta.get("regex_flags", "IGNORECASE").split("|"):
            flag_val = getattr(re, flag_name.strip(), None)
            if flag_val is not None:
                flags |= flag_val

        regex_header = re.compile(file_meta["regex_header"], flags)

        s3_hook = S3Hook(aws_conn_id=_S3_CONN_ID)
        file_name = Path(key).name

        # --- Download direto para memória via get_key ---
        logging.info(
            "[minc_extracao_anexos_dag.py] Baixando s3://%s/%s para memória",
            bucket,
            key,
        )

        try:
            obj = s3_hook.get_key(key=key, bucket_name=bucket)
            file_content = obj.get()["Body"].read()
        except Exception as exc:
            logging.error(
                "[minc_extracao_anexos_dag.py] Erro ao baixar s3://%s/%s: %s",
                bucket,
                key,
                exc,
            )
            return [{
                "nome_programa": nome_programa,
                "nome_arquivo": file_name,
                "aba": None,
                "tipo_edital": None,
                "dados_json": None,
                "colunas": None,
                "n_linhas": 0,
                "erro": repr(exc),
                "schema": file_meta["schema"],
                "table": file_meta["table"],
            }]

        # --- Escreve bytes em arquivo temporário para extrair_tabela_raw ---
        with TemporaryDirectory(prefix="extracao_") as tmpdir:
            local_path = Path(tmpdir) / file_name
            local_path.write_bytes(file_content)

            logging.info(
                "[minc_extracao_anexos_dag.py] Extraindo '%s' (%s, regex=%s)",
                file_name,
                nome_programa,
                file_meta["regex_header"],
            )

            try:
                resultados = extrair_tabela_raw(
                    file_path=local_path,
                    regex_header=regex_header,
                    col_header_idx=file_meta.get("col_header_idx", 1),
                    col_categoria_idx=file_meta.get("col_categoria_idx", 0),
                    min_len_categoria=file_meta.get("min_len_categoria", 6),
                )
            except Exception as exc:
                logging.error(
                    "[minc_extracao_anexos_dag.py] Erro ao extrair '%s': %s",
                    file_name,
                    exc,
                )
                return [{
                    "nome_programa": nome_programa,
                    "nome_arquivo": file_name,
                    "aba": None,
                    "tipo_edital": None,
                    "dados_json": None,
                    "colunas": None,
                    "n_linhas": 0,
                    "erro": repr(exc),
                    "schema": file_meta["schema"],
                    "table": file_meta["table"],
                }]

            registros: list[dict[str, Any]] = []
            for res in resultados:
                df = res["dados"]
                registros.append({
                    "nome_programa": nome_programa,
                    "nome_arquivo": res["nome_arquivo"],
                    "aba": res["aba"],
                    "tipo_edital": res["tipo_edital"],
                    "dados_json": df.to_json(orient="records"),
                    "colunas": list(df.columns),
                    "n_linhas": len(df),
                    "erro": None,
                    "schema": file_meta["schema"],
                    "table": file_meta["table"],
                })

            logging.info(
                "[minc_extracao_anexos_dag.py] Extraidos %d subtabelas de '%s'",
                len(registros),
                file_name,
            )

            # TemporaryDirectory limpa o dir ao sair do with-block
            return registros

    @task
    def carregar_no_postgres(
        resultados_por_arquivo: list[list[dict[str, Any]]],
    ) -> dict[str, int]:
        """Consolida resultados e carrega no PostgreSQL, particionando
        por schema/table de cada programa.

        Returns
        -------
        dict[str, int]
            Contagem de registros inseridos por (schema.table).
        """
        db = ClientPostgresDB(get_postgres_conn())

        particoes: dict[tuple[str, str], list[dict[str, Any]]] = {}
        erros: list[dict[str, Any]] = []

        for resultados_arquivo in resultados_por_arquivo:
            if resultados_arquivo is None:
                continue
            for reg in resultados_arquivo:
                if reg.get("erro"):
                    erros.append(reg)
                    continue
                if reg.get("dados_json") is None:
                    continue

                key = (reg["schema"], reg["table"])
                particoes.setdefault(key, []).append(reg)

        contagem: dict[str, int] = {}

        for (schema, table), registros in particoes.items():
            linhas: list[dict[str, Any]] = []

            for reg in registros:
                df = pd.read_json(reg["dados_json"], orient="records")
                # Adiciona metadados ELT
                df["nome_arquivo"] = reg["nome_arquivo"]
                df["aba"] = reg["aba"]
                df["tipo_edital"] = reg.get("tipo_edital")
                df["nome_programa"] = reg["nome_programa"]
                df["dt_ingest"] = datetime.now().isoformat()

                for record in df.to_dict(orient="records"):
                    linhas.append(record)

            if linhas:
                try:
                    db.insert_data(
                        linhas,
                        table_name=table,
                        primary_key=None,
                        conflict_fields=None,
                        schema=schema,
                    )
                    contagem[f"{schema}.{table}"] = len(linhas)
                    logging.info(
                        "[minc_extracao_anexos_dag.py] Carregados %d registros em %s.%s",
                        len(linhas),
                        schema,
                        table,
                    )
                except Exception as exc:
                    logging.error(
                        "[minc_extracao_anexos_dag.py] Erro ao carregar em %s.%s: %s",
                        schema,
                        table,
                        exc,
                    )
                    contagem[f"{schema}.{table}_erros"] = len(linhas)

        if erros:
            logging.warning(
                "[minc_extracao_anexos_dag.py] %d arquivos com erro de extracao (pulados)",
                len(erros),
            )
            for err in erros[:5]:
                logging.warning(
                    "[minc_extracao_anexos_dag.py] Erro: %s — %s",
                    err.get("nome_arquivo"),
                    err.get("erro"),
                )

        return contagem

    arquivos = listar_arquivos_s3()
    resultados = baixar_e_extrair.expand(file_meta=arquivos)
    carregar_no_postgres(resultados)


minc_extracao_anexos_dag()
