import logging
import re
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd
from airflow.decorators import dag, task
from airflow.models import Variable

from cliente_postgres import ClientPostgresDB
from extracao_planilhas import extrair_tabela_raw, listar_arquivos_locais
from postgres_helpers import get_postgres_conn
from schedule_loader import get_dynamic_schedule

_REGEX_LPG = re.compile(r"edita(?:is|l)", re.IGNORECASE)
_REGEX_PNAB = re.compile(r"contemplad|contratad|acompanhament", re.IGNORECASE)

_DEFAULT_PROGRAMAS = [
    {
        "nome_programa": "LPG",
        "regex_header": r"edita(?:is|l)",
        "regex_flags": "IGNORECASE",
        "col_header_idx": 1,
        "col_categoria_idx": 0,
        "min_len_categoria": 6,
        "base_dir": "/opt/airflow/data/raw/transferegov/anexos/ESTADO",
        "schema": "transferegov_lpg",
        "table": "raw_tabelas_anexos",
    },
    {
        "nome_programa": "PNAB",
        "regex_header": r"contemplad|contratad|acompanhament",
        "regex_flags": "IGNORECASE",
        "col_header_idx": 1,
        "col_categoria_idx": 0,
        "min_len_categoria": 6,
        "base_dir": "/opt/airflow/data/raw/transferegov/anexos/PNAB",
        "schema": "transferegov_pnab",
        "table": "raw_tabelas_anexos",
    },
]

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
    params={
        "programas": _DEFAULT_PROGRAMAS,
    },
)
def minc_extracao_anexos_dag() -> None:
    """DAG de extração de tabelas de anexos (XLSX/XLS/XLSM/ODS) para os
    programas LPG e PNAB, com Dynamic Task Mapping por arquivo.

    Fluxo:
      1. ``listar_arquivos`` — descobre todos os arquivos de planilha e
         enriquece cada entrada com os parâmetros do programa (regex, dirs).
      2. ``extrair_tabela`` — processa UM arquivo por vez (via ``.map()``),
         extraindo as subtabelas com a função ELT ``extrair_tabela_raw``.
      3. ``carregar_no_postgres`` — persiste os registros no PostgreSQL.
    """

    @task
    def listar_arquivos() -> list[dict[str, Any]]:
        """Lista arquivos de planilha para cada programa configurado e
        enriquece cada entrada com os parâmetros de extração."""
        programas: list[dict[str, Any]] = Variable.get(
            "transferegov_extracao_config",
            default_var=_DEFAULT_PROGRAMAS,
            deserialize_json=True,
        )

        arquivos_meta: list[dict[str, Any]] = []

        for prog in programas:
            nome = prog["nome_programa"]
            base_dir = prog["base_dir"]

            log.info(
                "[minc_extracao_anexos_dag.py] Listando arquivos para programa %s em %s",
                nome,
                base_dir,
            )

            try:
                caminhos = listar_arquivos_locais(base_dir)
            except FileNotFoundError:
                log.warning(
                    "[minc_extracao_anexos_dag.py] Diretório não encontrado para %s: %s — pulando",
                    nome,
                    base_dir,
                )
                continue

            for caminho in caminhos:
                arquivos_meta.append({
                    "file_path": caminho,
                    "nome_programa": nome,
                    "regex_header": prog["regex_header"],
                    "regex_flags": prog.get("regex_flags", "IGNORECASE"),
                    "col_header_idx": prog.get("col_header_idx", 1),
                    "col_categoria_idx": prog.get("col_categoria_idx", 0),
                    "min_len_categoria": prog.get("min_len_categoria", 6),
                    "schema": prog["schema"],
                    "table": prog["table"],
                })

            log.info(
                "[minc_extracao_anexos_dag.py] Programa %s: %d arquivos encontrados",
                nome,
                len(caminhos),
            )

        if not arquivos_meta:
            log.warning(
                "[minc_extracao_anexos_dag.py] Nenhum arquivo encontrado em nenhum programa"
            )

        return arquivos_meta

    @task
    def extrair_tabela(file_meta: dict[str, Any]) -> list[dict[str, Any]]:
        """Extrai subtabelas de um único arquivo de planilha.

        Fail-safe: nunca levanta exceção para não quebrar o Dynamic Task
        Mapping.  Arquivos com erro retornam lista vazia com flag de erro.
        """
        file_path = file_meta["file_path"]
        nome_programa = file_meta["nome_programa"]

        flags = 0
        for flag_name in file_meta.get("regex_flags", "IGNORECASE").split("|"):
            match getattr(re, flag_name.strip(), None):
                case f if f is not None:
                    flags |= f
                case _:
                    pass

        regex_header = re.compile(file_meta["regex_header"], flags)

        log.info(
            "[minc_extracao_anexos_dag.py] Extraindo '%s' (%s, regex=%s)",
            Path(file_path).name,
            nome_programa,
            file_meta["regex_header"],
        )

        try:
            resultados = extrair_tabela_raw(
                file_path=file_path,
                regex_header=regex_header,
                col_header_idx=file_meta.get("col_header_idx", 1),
                col_categoria_idx=file_meta.get("col_categoria_idx", 0),
                min_len_categoria=file_meta.get("min_len_categoria", 6),
            )
        except Exception as exc:
            log.error(
                "[minc_extracao_anexos_dag.py] Erro ao extrair '%s': %s",
                Path(file_path).name,
                exc,
            )
            return [{
                "nome_programa": nome_programa,
                "nome_arquivo": Path(file_path).name,
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

        log.info(
            "[minc_extracao_anexos_dag.py] Extraídos %d subtabelas de '%s'",
            len(registros),
            Path(file_path).name,
        )

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

        # Agrupa por (schema, table)
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
                    log.info(
                        "[minc_extracao_anexos_dag.py] Carregados %d registros em %s.%s",
                        len(linhas),
                        schema,
                        table,
                    )
                except Exception as exc:
                    log.error(
                        "[minc_extracao_anexos_dag.py] Erro ao carregar em %s.%s: %s",
                        schema,
                        table,
                        exc,
                    )
                    contagem[f"{schema}.{table}_erros"] = len(linhas)

        if erros:
            log.warning(
                "[minc_extracao_anexos_dag.py] %d arquivos com erro de extração (pulados)",
                len(erros),
            )
            for err in erros[:5]:
                log.warning(
                    "[minc_extracao_anexos_dag.py] Erro: %s — %s",
                    err.get("nome_arquivo"),
                    err.get("erro"),
                )

        return contagem

    arquivos = listar_arquivos()
    resultados = extrair_tabela.map(file_meta=arquivos)
    carregar_no_postgres(resultados)


minc_extracao_anexos_dag()
