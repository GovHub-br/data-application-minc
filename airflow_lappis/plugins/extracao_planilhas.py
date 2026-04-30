import logging
import re
import unicodedata
from pathlib import Path
from typing import Any, Optional

import pandas as pd


log = logging.getLogger(__name__)


def normalizar_nome(nome: Any) -> str:
    """Normaliza string para comparação: lowercase, sem acentos, sem números,
    sem pontuação, espaços colapsados."""
    if nome is None:
        return ""
    s = str(nome).strip().lower()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = re.sub(r"\d+", " ", s)
    s = re.sub(r"[^a-z\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

_ENGINE_MAP = {
    ".xlsx": "openpyxl",
    ".xlsm": "openpyxl",
    ".xls": "xlrd",
    ".xlsb": "pyxlsb",
    ".ods": "odf",
}


def detectar_engine(file_path: str | Path) -> Optional[str]:
    """Retorna a engine pandas adequada para a extensão do arquivo."""
    ext = Path(file_path).suffix.lower()
    return _ENGINE_MAP.get(ext)


def abrir_excel(file_path: str | Path) -> pd.ExcelFile:
    """Abre planilha tentando a engine correta, com fallbacks para casos
    comuns (extensão incorreta, XLSB disfarçado, etc.).

    Levanta RuntimeError se nenhuma tentativa funcionar.
    """
    file_path = Path(file_path)
    suffix = file_path.suffix.lower()
    engine_principal = detectar_engine(file_path)

    tentativas: list[tuple[str, str]] = []
    if engine_principal:
        tentativas.append((str(file_path), engine_principal))

    # Fallbacks comuns
    if suffix == ".xls":
        tentativas.append((str(file_path), "xlrd"))
    if _eh_zip_excel(file_path):
        tentativas.extend([
            (str(file_path), "pyxlsb"),
            (str(file_path), "openpyxl"),
        ])
    elif suffix in {".xlsx", ".xlsm"}:
        tentativas.append((str(file_path), "openpyxl"))
    elif suffix == ".xlsb":
        tentativas.append((str(file_path), "pyxlsb"))
    elif suffix == ".ods":
        tentativas.append((str(file_path), "odf"))

    # Desduplica preservando ordem
    vistos: set[tuple[str, str]] = set()
    tentativas_unicas: list[tuple[str, str]] = []
    for p, e in tentativas:
        chave = (p, e)
        if chave not in vistos:
            tentativas_unicas.append((p, e))
            vistos.add(chave)

    ultimo_erro: Exception | None = None
    for path_tentativa, engine_tentativa in tentativas_unicas:
        try:
            xls = pd.ExcelFile(path_tentativa, engine=engine_tentativa)
            if xls.sheet_names:
                log.info(
                    "[extracao_planilhas.py] Aberto '%s' com engine '%s'",
                    Path(path_tentativa).name,
                    engine_tentativa,
                )
                return xls
            # openpyxl abre mas lista abas vazias (Strict OOXML) — tenta calamine
            if engine_tentativa == "openpyxl":
                try:
                    return pd.ExcelFile(path_tentativa, engine="calamine")
                except Exception:
                    pass
        except Exception as exc:
            ultimo_erro = exc
            log.debug(
                "[extracao_planilhas.py] Falha '%s' com engine '%s': %s",
                Path(path_tentativa).name,
                engine_tentativa,
                exc,
            )

    raise RuntimeError(
        f"Não foi possível abrir o arquivo '{file_path.name}'. "
        f"Último erro: {type(ultimo_erro).__name__}: {ultimo_erro}"
    ) from ultimo_erro


def _eh_zip_excel(path: Path) -> bool:
    """XLSX/XLSM/XLSB são ZIPs (assinatura PK\\x03\\x04)."""
    try:
        with open(path, "rb") as f:
            return f.read(4) == b"PK\x03\x04"
    except OSError:
        return False


def extrair_tabela_raw(
    file_path: str | Path,
    regex_header: re.Pattern,
    col_header_idx: int = 1,
    col_categoria_idx: int = 0,
    min_len_categoria: int = 6,
) -> list[dict]:
    """Extrai tabelas de um arquivo de planilha usando o padrão ELT.

    **Comportamento**:

    1. Lê **todas** as abas do arquivo com ``dtype=str``.
    2. Para cada aba, procura a **linha âncora** onde:
       - ``col_categoria_idx`` (default: col 0) está vazia
       - ``col_header_idx`` (default: col 1) normalizada bate em ``regex_header``
    3. Fatia o DataFrame a partir dessa linha âncora.
    4. Detecta "subtabelas" (blocos de categoria) usando heurística de
       separadores: linhas onde a coluna ``col_categoria_idx`` tem texto
       com comprimento > ``min_len_categoria`` e as demais colunas são NaN.
    5. Para cada subtabela, atribui a coluna ``tipo_edital`` com o nome
       da categoria detectada.
    6. **NÃO** renomeia colunas para ``c001``, **NÃO** exclui colunas
       residuais, **NÃO** empilha header como dado.

    Parameters
    ----------
    file_path : str | Path
        Caminho do arquivo de planilha.
    regex_header : re.Pattern
        Pattern compilado usado para identificar a linha âncora de cabeçalho.
        Para LPG: ``re.compile(r"edita(?:is|l)", re.IGNORECASE)``
        Para PNAB: ``re.compile(r"contemplad|acompanhament", re.IGNORECASE)``
    col_header_idx : int
        Índice da coluna usada na busca do regex (default: 1, segunda coluna).
    col_categoria_idx : int
        Índice da coluna onde se busca o nome da categoria (default: 0).
    min_len_categoria : int
        Comprimento mínimo de texto na col 0 para considerar uma "linha de
        categoria" (default: 6).

    Returns
    -------
    list[dict]
        Lista de dicts, um por subtabela extraída. Cada dict contém:
        - ``nome_arquivo`` (str): nome do arquivo de origem
        - ``aba`` (str): nome da aba de origem
        - ``tipo_edital`` (str | None): categoria detectada
        - ``dados`` (pd.DataFrame): DataFrame com colunas originais do Excel
          + coluna ``tipo_edital``. Todas as colunas são ``str``.
    """
    file_path = Path(file_path)
    resultados: list[dict] = []

    try:
        xls = abrir_excel(file_path)
    except Exception as exc:
        log.error(
            "[extracao_planilhas.py] Falha ao abrir '%s': %s",
            file_path.name,
            exc,
        )
        return resultados

    with xls:
        abas = xls.sheet_names
        for aba in abas:
            try:
                df = pd.read_excel(xls, sheet_name=aba, dtype=str)
            except Exception as exc:
                log.warning(
                    "[extracao_planilhas.py] Erro ao ler aba '%s' de '%s': %s",
                    aba,
                    file_path.name,
                    exc,
                )
                continue

            if df.empty or df.shape[1] < (max(col_header_idx, col_categoria_idx) + 1):
                continue

            # --- Busca da linha âncora ---
            start_idx: int | None = None
            categoria_edital: str | None = None

            for i in range(len(df)):
                try:
                    val_cat = df.iloc[i, col_categoria_idx]
                except IndexError:
                    continue

                col_cat_vazia = (
                    val_cat is None
                    or pd.isna(val_cat)
                    or str(val_cat).strip() == ""
                )

                try:
                    val_header = df.iloc[i, col_header_idx]
                except IndexError:
                    continue

                col_hdr_norm = normalizar_nome(val_header)

                if col_cat_vazia and regex_header.search(col_hdr_norm):
                    start_idx = i
                    # Categoria = último valor não-nulo na col_categoria_idx
                    # antes da linha âncora
                    col_cat_slice = df.iloc[:i, col_categoria_idx]
                    nao_nulos = col_cat_slice.dropna()
                    if nao_nulos.any():
                        categoria_edital = str(nao_nulos.iloc[-1]).strip()
                    break

            if start_idx is None:
                continue

            # --- Fatia a partir do header ---
            header = df.iloc[start_idx].tolist()
            body = df.iloc[start_idx + 1:].copy()
            body.columns = header
            body = body.reset_index(drop=True)

            # --- Delimitação de subtabelas ---
            list_start_idx = [0]
            lista_categorias = [categoria_edital]

            for idx, row in body.iterrows():
                valores = row.tolist()
                try:
                    texto = str(valores[col_categoria_idx]).strip()
                except IndexError:
                    continue

                demais = [
                    v for j, v in enumerate(valores) if j != col_categoria_idx
                ]
                if len(texto) > min_len_categoria and all(
                    pd.isna(v) for v in demais
                ):
                    lista_categorias.append(texto)
                    list_start_idx.append(idx)

            bounds = list_start_idx + [len(body)]
            tuplas = list(zip(bounds[:-1], bounds[1:]))

            # --- Extração de cada subtabela ---
            for j, (a, b) in enumerate(tuplas):
                if j < len(tuplas) - 1:
                    df_y = body.iloc[a:b].copy()
                else:
                    df_y = body.iloc[a:].copy()

                tipo = lista_categorias[j]

                # Para subtabelas após a primeira (j > 0), a primeira linha
                # é o "separador de categoria" e a segunda é o header real
                if j > 0 and len(df_y) >= 2:
                    header2 = df_y.iloc[1].tolist()
                    df_y = df_y.iloc[2:].copy()
                    df_y.columns = header2
                    df_y = df_y.reset_index(drop=True)

                df_y["tipo_edital"] = tipo

                resultados.append({
                    "nome_arquivo": file_path.name,
                    "aba": aba,
                    "tipo_edital": tipo,
                    "dados": df_y,
                })

    return resultados


def listar_arquivos_locais(
    base_dir: str | Path,
    extensoes: set[str] | None = None,
) -> list[str]:
    """Lista arquivos de planilha recursivamente em um diretório local.

    Parameters
    ----------
    base_dir : str | Path
        Diretório raiz da busca.
    extensoes : set[str] | None
        Extensões válidas (default: {".xlsx", ".xls", ".xlsm", ".xlsb", ".ods"}).

    Returns
    -------
    list[str]
        Caminhos absolutos dos arquivos encontrados.
    """
    if extensoes is None:
        extensoes = {".xlsx", ".xls", ".xlsm", ".xlsb", ".ods"}

    base_dir = Path(base_dir)
    if not base_dir.exists():
        raise FileNotFoundError(f"Diretório não encontrado: {base_dir}")

    arquivos: list[str] = []
    for f in base_dir.rglob("*"):
        if f.is_file() and f.suffix.lower() in extensoes and not f.name.startswith("~$"):
            arquivos.append(str(f))

    return sorted(arquivos)


def listar_arquivos_s3(
    bucket: str,
    prefix: str,
    extensoes: set[str] | None = None,
) -> list[dict[str, str]]:
    """Lista objetos de planilha em um bucket S3.

    Parameters
    ----------
    bucket : str
        Nome do bucket S3.
    prefix : str
        Prefixo (pasta) dentro do bucket.
    extensoes : set[str] | None
        Extensões válidas (default: {".xlsx", ".xls", ".xlsm", ".xlsb", ".ods"}).

    Returns
    -------
    list[dict[str, str]]
        Lista de dicts com ``{"bucket": ..., "key": ...}`` para cada arquivo.
    """
    if extensoes is None:
        extensoes = {".xlsx", ".xls", ".xlsm", ".xlsb", ".ods"}

    from airflow.providers.amazon.aws.hooks.s3 import S3Hook

    hook = S3Hook(aws_conn_id="aws_default")
    client = hook.get_client_type("s3")

    paginator = client.get_paginator("list_objects_v2")
    arquivos: list[dict[str, str]] = []

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            ext = "." + key.rsplit(".", 1)[-1].lower() if "." in key else ""
            if ext in extensoes and not Path(key).name.startswith("~$"):
                arquivos.append({"bucket": bucket, "key": key})

    return arquivos
