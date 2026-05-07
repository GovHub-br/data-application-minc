import inspect
import io
import inspect
import logging
import re
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from cliente_postgres import ClientPostgresDB
from extracao_planilhas import extrair_tabela_raw
from postgres_helpers import get_postgres_conn

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
        "regex_header": r"contemplad|contratad|acompanhament|plan|sheet|dados|tabela|resumo|relat[oó]rio",
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
    2. ``baixar_e_extrair`` — para CADA arquivo (via ``.expand()``), baixa
       do MinIO para memória, extrai as subtabelas com
       ``extrair_tabela_raw`` e insere diretamente no PostgreSQL.
       Retorna apenas metadados leves (sem dados_json) para evitar
       estouro de memória no XCom.
    3. ``fechar_pipeline`` — task de fechamento que consolida os
       metadados e encerra a DAG.
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
    def baixar_e_extrair(file_meta: dict[str, Any]) -> dict[str, Any]:
        """Baixa um arquivo do MinIO para memória, extrai as subtabelas,
        insere diretamente no PostgreSQL e retorna apenas metadados leves.

        O INSERT no banco é feito dentro desta task para evitar o tráfego
        pesado de dados_json pelo XCom, que causava OOM na task
        carregar_no_postgres com 2.000+ arquivos mapeados.

        Fail-safe: nunca levanta exceção para não quebrar o Dynamic Task
        Mapping. Arquivos com erro retornam metadados com flag de erro.
        """
        nome_programa = file_meta["nome_programa"]
        bucket = file_meta["bucket"]
        key = file_meta["key"]
        schema = file_meta["schema"]
        table = file_meta["table"]

        flags = 0
        for flag_name in file_meta.get("regex_flags", "IGNORECASE").split("|"):
            flag_val = getattr(re, flag_name.strip(), None)
            if flag_val is not None:
                flags |= flag_val

        regex_header = re.compile(file_meta["regex_header"], flags)

        s3_hook = S3Hook(aws_conn_id=_S3_CONN_ID)
        db = ClientPostgresDB(get_postgres_conn())
        file_name = Path(key).name

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
            return {
                "nome_programa": nome_programa,
                "nome_arquivo": file_name,
                "n_subtabelas": 0,
                "n_linhas_inseridas": 0,
                "status": "erro_download",
                "erro": repr(exc),
            }

        buffer = io.BytesIO(file_content)

        logging.info(
            "[minc_extracao_anexos_dag.py] Extraindo '%s' (%s, regex=%s)",
            file_name,
            nome_programa,
            file_meta["regex_header"],
        )

        try:
            # Detecta se a versão de extrair_tabela_raw aceita file_name
            # (versão nova com suporte a BytesIO) ou não (versão legada)
            _fn_params = inspect.signature(extrair_tabela_raw).parameters
            _extra_kwargs = (
                {"file_name": file_name} if "file_name" in _fn_params else {}
            )
            resultados = extrair_tabela_raw(
                file_path=buffer,
                regex_header=regex_header,
                col_header_idx=file_meta.get("col_header_idx", 1),
                col_categoria_idx=file_meta.get("col_categoria_idx", 0),
                min_len_categoria=file_meta.get("min_len_categoria", 6),
                **_extra_kwargs,
            )

            logging.info(f"Abas encontradas pelo regex: {len(resultados)}")

            total_linhas = 0
            n_subtabelas = len(resultados)

            for res in resultados:
                df = res["dados"]
                df = df.loc[:, ~df.columns.duplicated()]

                logging.info(f"Linhas originais da aba {res['aba']}: {len(df)}")

                # --- Data Cleaning: remove lixo estrutural do Excel ---
                colunas_antes = len(df.columns)
                linhas_antes = len(df)

                # 1. Colunas 100% vazias
                df = df.dropna(axis=1, how="all")

                # 2. Linhas 100% vazias
                df = df.dropna(how="all")

                # 3. Heurística: linhas onde a imensa maioria dos dados
                #    reais é nula. Colunas de metadado (tipo_edital) sempre
                #    têm valor, então calculamos thresh apenas sobre as
                #    colunas originais do Excel.
                _col_meta = {"tipo_edital"}
                col_dados = [c for c in df.columns if c not in _col_meta]
                if col_dados:
                    # Exige que pelo menos 30% das colunas de dados tenham
                    # valor não-nulo; isso elimina linhas de espaçamento
                    # visual e índices numéricos soltos com resto nulo.
                    thresh_dados = max(1, int(len(col_dados) * 0.3))
                    df = df.dropna(subset=col_dados, thresh=thresh_dados)

                df = df.reset_index(drop=True)

                logging.info(f"Linhas restantes após limpeza: {len(df)}")

                linhas_removidas = linhas_antes - len(df)
                colunas_removidas = colunas_antes - len(df.columns)
                if linhas_removidas or colunas_removidas:
                    logging.info(
                        "[minc_extracao_anexos_dag.py] Limpeza '%s' aba '%s': "
                        "removidas %d/%d linhas, %d/%d colunas",
                        file_name,
                        res["aba"],
                        linhas_removidas,
                        linhas_antes,
                        colunas_removidas,
                        colunas_antes,
                    )

                if df.empty:
                    logging.warning(f"Aba {res['aba']} descartada por estar vazia após limpeza.")
                    continue

                df["nome_arquivo"] = res["nome_arquivo"]
                df["aba"] = res["aba"]
                df["tipo_edital"] = res.get("tipo_edital")
                df["nome_programa"] = nome_programa
                df["dt_ingest"] = datetime.now().isoformat()

                linhas = df.to_dict(orient="records")

                logging.info(
                    "Inserindo %d registros do programa %s na tabela %s.%s",
                    len(df),
                    nome_programa,
                    schema,
                    table,
                )
                db.insert_data(
                    linhas,
                    table_name=table,
                    primary_key=None,
                    conflict_fields=None,
                    schema=schema,
                )
                total_linhas += len(linhas)

            logging.info(
                "[minc_extracao_anexos_dag.py] Arquivo '%s': %d subtabelas, "
                "%d linhas inseridas em %s.%s",
                file_name,
                n_subtabelas,
                total_linhas,
                schema,
                table,
            )

            return {
                "nome_programa": nome_programa,
                "nome_arquivo": file_name,
                "n_subtabelas": n_subtabelas,
                "n_linhas_inseridas": total_linhas,
                "status": "sucesso",
                "erro": None,
            }

        except Exception as exc:
            logging.warning(
                f"Arquivo ignorado por corrupção ou erro de leitura: {file_name} | Erro: {exc}"
            )
            return {
                "nome_programa": nome_programa,
                "nome_arquivo": file_name,
                "n_subtabelas": 0,
                "n_linhas_inseridas": 0,
                "status": "erro_extracao",
                "erro": repr(exc),
            }

    @task
    def fechar_pipeline(metadados: list[dict[str, Any]]) -> dict[str, int]:
        """Task de fechamento que consolida os metadados leves das tasks
        mapeadas e encerra a DAG. Não faz INSERT — os dados já foram
        persistidos diretamente por cada ``baixar_e_extrair``.

        Returns
        -------
        dict[str, int]
            Contagem de arquivos por status e total de linhas inseridas.
        """
        contagem: dict[str, int] = {"sucesso": 0, "erro_download": 0, "erro_extracao": 0}
        total_linhas = 0

        for meta in metadados:
            if meta is None:
                continue
            status = meta.get("status", "desconhecido")
            contagem[status] = contagem.get(status, 0) + 1
            total_linhas += meta.get("n_linhas_inseridas", 0)

        contagem["total_linhas_inseridas"] = total_linhas

        if contagem.get("erro_download") or contagem.get("erro_extracao"):
            logging.warning(
                "[minc_extracao_anexos_dag.py] %d arquivos com erro de download, "
                "%d com erro de extração",
                contagem.get("erro_download", 0),
                contagem.get("erro_extracao", 0),
            )

        logging.info(
            "[minc_extracao_anexos_dag.py] Pipeline finalizado: %d arquivos OK, "
            "%d linhas inseridas no total",
            contagem.get("sucesso", 0),
            total_linhas,
        )

        return contagem

    arquivos = listar_arquivos_s3()
    resultados = baixar_e_extrair.expand(file_meta=arquivos)
    fechar_pipeline(resultados)


minc_extracao_anexos_dag()
