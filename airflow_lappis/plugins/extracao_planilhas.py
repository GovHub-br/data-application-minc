import io
import logging
import re
import tempfile
import unicodedata
import zipfile
from pathlib import Path
from typing import Any, Optional, Union

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
    ".xlsx": "calamine",
    ".xlsm": "calamine",
    ".xls": "calamine",
    ".xlsb": "calamine",
    ".ods": "calamine",
}


def detectar_engine(file_path: str | Path) -> Optional[str]:
    """Retorna a engine pandas adequada para a extensão do arquivo."""
    ext = Path(file_path).suffix.lower()
    return _ENGINE_MAP.get(ext)


def _eh_zip_excel(source: str | Path | io.BytesIO) -> bool:
    """Verifica se o conteúdo é um ZIP (XLSX/XLSM/XLSB usam esse formato).

    Funciona tanto com caminho de arquivo quanto com buffer em memória
    (``io.BytesIO``).  No caso de buffer, usa ``zipfile.is_zipfile()``
    que aceita objetos file-like nativamente.
    """
    try:
        if isinstance(source, io.BytesIO):
            pos = source.tell()
            source.seek(0)
            result = zipfile.is_zipfile(source)
            source.seek(pos)
            return result
        # Caminho em disco — abre normalmente
        with open(source, "rb") as f:
            return f.read(4) == b"PK\x03\x04"
    except (OSError, zipfile.BadZipFile):
        return False


def abrir_excel(
    source: str | Path | io.BytesIO,
    file_name: str | None = None,
) -> pd.ExcelFile:
    """Abre planilha a partir de caminho ou buffer em memória.

    Prioriza a leitura 100% em memória (BytesIO) com o motor ``calamine``,
    que é memory-safe e transforma corrupções em exceções normais em vez
    de causar OOM. Motores legados (openpyxl, xlrd, pyxlsb, odf) são
    tentados como fallback — e estes podem exigir arquivo em disco, que
    é criado sob demanda apenas nessa situação.

    Parameters
    ----------
    source : str | Path | io.BytesIO
        Caminho do arquivo físico OU buffer em memória com os bytes.
    file_name : str | None
        Nome do arquivo (para log e detecção de extensão). Obrigatório
        quando ``source`` é ``io.BytesIO``.

    Levanta RuntimeError se nenhuma tentativa funcionar.
    """
    # --- Resolve nome, extensão e buffer ---
    if isinstance(source, io.BytesIO):
        if not file_name:
            raise ValueError("file_name é obrigatório quando source é BytesIO")
        nome = file_name
        suffix = Path(nome).suffix.lower()
        buffer = source
        buffer.seek(0)
    else:
        source = Path(source)
        nome = source.name
        suffix = source.suffix.lower()
        buffer = None

    # --- Monta lista de tentativas (engine, precisa_disco) ---
    tentativas: list[tuple[str, str, bool]] = []
    # 1a tentativa: calamine em memória
    tentativas.append(("calamine", nome, False))

    # Fallbacks por extensão (podem exigir disco)
    fallback_engine: str | None = None
    if suffix == ".xls":
        fallback_engine = "xlrd"
    elif suffix in {".xlsx", ".xlsm"}:
        fallback_engine = "openpyxl"
    elif suffix == ".xlsb":
        fallback_engine = "pyxlsb"
    elif suffix == ".ods":
        fallback_engine = "odf"

    if fallback_engine:
        tentativas.append((fallback_engine, nome, True))

    # Fallbacks extras para XLS que na verdade é ZIP disfarçado
    if suffix == ".xls":
        if _eh_zip_excel(source):
            tentativas.append(("openpyxl", nome, True))
            tentativas.append(("pyxlsb", nome, True))

    # Desduplica preservando ordem
    vistos: set[str] = set()
    tentativas_unicas: list[tuple[str, str, bool]] = []
    for engine, n, disco in tentativas:
        if engine not in vistos:
            tentativas_unicas.append((engine, n, disco))
            vistos.add(engine)

    # --- Tenta cada engine ---
    tmp_path: str | None = None
    ultimo_erro: Exception | None = None

    for engine, n, precisa_disco in tentativas_unicas:
        try:
            if precisa_disco and buffer is not None:
                # Motor legado que exige arquivo físico — grava sob demanda
                if tmp_path is None:
                    tmp_dir = tempfile.mkdtemp(prefix="extracao_fallback_")
                    tmp_path = str(Path(tmp_dir) / n)
                    with open(tmp_path, "wb") as f:
                        buffer.seek(0)
                        f.write(buffer.read())
                    log.debug(
                        "[extracao_planilhas.py] Fallback gravou buffer em '%s'",
                        tmp_path,
                    )
                read_source = tmp_path
            elif not precisa_disco and buffer is not None:
                # calamine com BytesIO — 100% em memória
                buffer.seek(0)
                read_source = buffer
            else:
                # source é caminho de arquivo (str ou Path)
                read_source = str(source)

            xls = pd.ExcelFile(read_source, engine=engine)
            if xls.sheet_names:
                log.info(
                    "[extracao_planilhas.py] Aberto '%s' com engine '%s'",
                    n,
                    engine,
                )
                return xls
        except Exception as exc:
            ultimo_erro = exc
            log.debug(
                "[extracao_planilhas.py] Falha '%s' com engine '%s': %s",
                n,
                engine,
                exc,
            )

    raise RuntimeError(
        f"Não foi possível abrir o arquivo '{nome}'. "
        f"Último erro: {type(ultimo_erro).__name__}: {ultimo_erro}"
    ) from ultimo_erro


def extrair_tabela_raw(
    file_path: str | Path | io.BytesIO,
    regex_header: re.Pattern,
    col_header_idx: int = 1,
    col_categoria_idx: int = 0,
    min_len_categoria: int = 6,
    file_name: str | None = None,
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
    file_path : str | Path | io.BytesIO
        Caminho do arquivo físico OU buffer em memória com os bytes da planilha.
        Quando ``io.BytesIO``, o parâmetro ``file_name`` deve ser informado.
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
    file_name : str | None
        Nome do arquivo (para log e metadados). Obrigatório quando
        ``file_path`` é ``io.BytesIO``.

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
    # Resolve nome do arquivo para logs e metadados
    if isinstance(file_path, io.BytesIO):
        nome_arquivo = file_name or "unknown"
        source = file_path
    else:
        source = Path(file_path)
        nome_arquivo = source.name

    resultados: list[dict] = []

    try:
        xls = abrir_excel(source, file_name=nome_arquivo)
    except Exception as exc:
        log.error(
            "[extracao_planilhas.py] Falha ao abrir '%s': %s",
            nome_arquivo,
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
                    nome_arquivo,
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
                    "nome_arquivo": nome_arquivo,
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

    hook = S3Hook(aws_conn_id="minio_default")
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
