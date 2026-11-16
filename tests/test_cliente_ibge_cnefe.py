"""Testes do CNEFE 2022 do IBGE (issue #168): cliente, partes de staging e DAG.

O que importa aqui: a listagem de cada UF só passa com os municípios
esperados; nenhuma linha do CSV se perde, se funde ou muda de valor no
caminho até o Parquet (CEP com zero à esquerda, coordenada, ``NA``, aspa
solta); só as espécies 4 e 6 chegam ao staging; e recarregar uma UF no
Postgres não duplica nem toca as outras.

Os testes de carga rodam num Postgres de verdade, apontado por
``TEST_POSTGRES_DSN`` (ver ``test_cliente_postgres.py``), e são pulados sem
ele. Os da DAG precisam do Airflow 3 e da linhagem do OpenMetadata, que só
existem na imagem do Airflow.
"""

import importlib.util
import io
import os
import uuid
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import requests

import cliente_ibge_cnefe as cnefe
import datalakehouse
import retry_helpers

_DT = datetime(2026, 11, 16, 12, 0, tzinfo=timezone.utc)

# O cabeçalho como está nos 5.570 CSVs (o último nome vem cortado assim).
CABECALHO = (
    "COD_UNICO_ENDERECO;COD_UF;COD_MUNICIPIO;COD_DISTRITO;COD_SUBDISTRITO;"
    "COD_SETOR;NUM_QUADRA;NUM_FACE;CEP;DSC_LOCALIDADE;NOM_TIPO_SEGLOGR;"
    "NOM_TITULO_SEGLOGR;NOM_SEGLOGR;NUM_ENDERECO;DSC_MODIFICADOR;NOM_COMP_ELEM1;"
    "VAL_COMP_ELEM1;NOM_COMP_ELEM2;VAL_COMP_ELEM2;NOM_COMP_ELEM3;VAL_COMP_ELEM3;"
    "NOM_COMP_ELEM4;VAL_COMP_ELEM4;NOM_COMP_ELEM5;VAL_COMP_ELEM5;LATITUDE;"
    "LONGITUDE;NV_GEO_COORD;COD_ESPECIE;DSC_ESTABELECIMENTO;"
    "COD_INDICADOR_ESTAB_ENDERECO;COD_INDICADOR_CONST_ENDERECO;"
    "COD_INDICADOR_FINALIDADE_CONST;COD_TIPO_ESPECI"
)
COLUNAS_CSV = CABECALHO.split(";")


def linha(**valores: str) -> str:
    """Uma linha do CSV com os ``valores`` dados e o resto vazio."""
    return ";".join(valores.get(c, "") for c in COLUNAS_CSV)


def endereco(codigo: str, especie: str, **extra: str) -> str:
    return linha(
        COD_UNICO_ENDERECO=codigo,
        COD_UF="14",
        COD_MUNICIPIO="1400605",
        COD_SETOR="140060505000001P",
        COD_ESPECIE=especie,
        **extra,
    )


def csv_bytes(*linhas: str, final: str = "\r\n") -> bytes:
    return ("\r\n".join([CABECALHO, *linhas]) + final).encode("ascii")


def abridor(conteudo: bytes) -> Any:
    return lambda: io.BytesIO(conteudo)


def zip_com(membros: dict[str, bytes]) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as arquivo:
        for nome, conteudo in membros.items():
            arquivo.writestr(nome, conteudo)
    return buffer.getvalue()


# ── catálogo e listagem ──────────────────────────────────────────────────


def test_catalogo_tem_as_27_ufs_e_os_5570_municipios() -> None:
    assert len(cnefe.UFS) == 27
    assert sum(uf.municipios for uf in cnefe.UFS.values()) == 5570
    codigos = [uf.codigo for uf in cnefe.UFS.values()]
    assert len(set(codigos)) == 27
    assert all(len(c) == 2 and c.isdigit() for c in codigos)


def test_url_da_pasta_usa_codigo_e_sigla() -> None:
    assert cnefe.url_da_pasta("RR").endswith("/CSV/Municipio/14_RR/")


# Trecho do índice do Apache como o FTP do IBGE serve: links de ordenação, a
# pasta acima e nomes com hífen e com acento percent-encoded.
_HTML = """
<tr><th><a href="?C=N;O=D">Name</a></th><th><a href="?C=M;O=A">Last modified</a></th></tr>
<tr><td><a href="/Cadastro/CSV/Municipio/">Parent Directory</a></td></tr>
<tr><td><a href="2907400_CATOL%c3%82NDIA.zip">2907400_CATOLÂNDIA.zip</a></td></tr>
<tr><td><a href="1100122_JI-PARANA.zip">1100122_JI-PARANA.zip</a></td></tr>
<tr><td><a href="1100122_JI-PARANA.zip">1100122_JI-PARANA.zip</a></td></tr>
<tr><td><a href="Dicionario_CNEFE_Censo_2022.xls">dicionário</a></td></tr>
"""


def test_extrair_zips_decodifica_o_nome_e_mantem_o_href_na_url() -> None:
    arquivos = cnefe.extrair_zips(_HTML, "https://ftp/pasta/")
    assert arquivos == [
        cnefe.ArquivoMunicipio(
            "1100122", "1100122_JI-PARANA.zip", "https://ftp/pasta/1100122_JI-PARANA.zip"
        ),
        cnefe.ArquivoMunicipio(
            "2907400",
            "2907400_CATOLÂNDIA.zip",
            "https://ftp/pasta/2907400_CATOL%c3%82NDIA.zip",
        ),
    ]


def _arquivos(codigos: list[str]) -> list[cnefe.ArquivoMunicipio]:
    return [cnefe.ArquivoMunicipio(c, f"{c}_X.zip", f"u/{c}_X.zip") for c in codigos]


def test_conferir_listagem_aceita_a_pasta_completa() -> None:
    cnefe.conferir_listagem("RR", _arquivos([f"14{n:05d}" for n in range(15)]))


@pytest.mark.parametrize(
    "codigos",
    [
        [f"14{n:05d}" for n in range(14)],  # um a menos
        [f"14{n:05d}" for n in range(14)] + ["1500107"],  # um de outra UF
        [f"14{n:05d}" for n in range(14)] + ["1400000"],  # repetido
    ],
    ids=["faltando", "outra_uf", "repetido"],
)
def test_conferir_listagem_falha_fora_do_catalogo(codigos: list[str]) -> None:
    with pytest.raises(ValueError, match="RR: a listagem"):
        cnefe.conferir_listagem("RR", _arquivos(codigos))


# ── ZIP e CSV ────────────────────────────────────────────────────────────


def test_membro_csv_aceita_nome_em_cp437() -> None:
    # O CSV de Catolândia vem com o "Â" em cp437 dentro do ZIP.
    conteudo = zip_com({"2907400_CATOL├éNDIA.csv": b"x"})
    with zipfile.ZipFile(io.BytesIO(conteudo)) as arquivo:
        assert cnefe.membro_csv(arquivo, "2907400").filename.startswith("2907400_")


@pytest.mark.parametrize(
    "membros",
    [
        {"leia-me.txt": b"x"},
        {"1400605_A.csv": b"x", "1400605_B.csv": b"x"},
        {"1400100_BOA_VISTA.csv": b"x"},
    ],
    ids=["sem_csv", "dois_csv", "outro_municipio"],
)
def test_membro_csv_exige_um_csv_do_municipio(membros: dict[str, bytes]) -> None:
    with zipfile.ZipFile(io.BytesIO(zip_com(membros))) as arquivo:
        with pytest.raises(ValueError, match="1400605"):
            cnefe.membro_csv(arquivo, "1400605")


def test_contar_linhas_por_quebra_e_especie_pela_posicao_do_cabecalho() -> None:
    conteudo = csv_bytes(
        endereco("1", "1"),
        endereco("2", "4"),
        "",  # linha vazia: o parser também não conta
        endereco("3", "6"),
        endereco("4", "7"),
        final="",  # última linha sem quebra também é linha
    )
    assert cnefe.contar_linhas(abridor(conteudo)) == (4, 2)


def test_contar_linhas_de_arquivo_so_com_cabecalho() -> None:
    assert cnefe.contar_linhas(abridor(csv_bytes())) == (0, 0)


def test_csv_para_tabela_filtra_as_especies_e_nao_muda_o_valor() -> None:
    conteudo = csv_bytes(
        endereco("1", "1", DSC_ESTABELECIMENTO="CASA"),
        endereco(
            "2",
            "4",
            CEP="01001000",
            NUM_ENDERECO="0",
            DSC_MODIFICADOR="SN",
            LATITUDE="-1.0179750",
            LONGITUDE="-60.051402",
            NV_GEO_COORD="1",
            DSC_ESTABELECIMENTO="ESCOLA MUNICIPAL",
        ),
        endereco("3", "6", NOM_SEGLOGR="NA", DSC_ESTABELECIMENTO='BAR "DO ZE", 2'),
        endereco("4", "8", DSC_ESTABELECIMENTO="IGREJA"),
    )
    tabela, lidas = cnefe.csv_para_tabela(abridor(conteudo), "RR", _DT)

    assert lidas == 4
    assert tabela.schema == cnefe.SCHEMA_ARROW
    assert tabela.column_names == cnefe.COLUNAS_DESTINO
    linhas = tabela.to_pylist()
    assert [r["cod_unico_endereco"] for r in linhas] == ["2", "3"]
    escola, bar = linhas
    assert escola["cep"] == "01001000"
    assert escola["num_endereco"] == "0"
    assert escola["latitude"] == "-1.0179750"
    assert escola["dsc_localidade"] is None  # vazio vira nulo
    assert bar["nom_seglogr"] == "NA"  # NA não é nulo
    assert bar["dsc_estabelecimento"] == 'BAR "DO ZE", 2'  # aspa não delimita
    assert {r["uf"] for r in linhas} == {"RR"}
    assert {r["dt_ingest"] for r in linhas} == {_DT.isoformat()}


def test_csv_com_coluna_a_mais_falha() -> None:
    conteudo = csv_bytes(endereco("1", "4"), endereco("2", "6") + ";extra")
    with pytest.raises(pa.ArrowInvalid):
        cnefe.csv_para_tabela(abridor(conteudo), "RR", _DT)


def test_csv_com_byte_fora_do_utf8_falha() -> None:
    conteudo = csv_bytes(endereco("1", "4")).replace(b"1400605;", b"1400605\xe9;", 1)
    with pytest.raises(pa.ArrowInvalid):
        cnefe.csv_para_tabela(abridor(conteudo), "RR", _DT)


def test_cabecalho_sem_coluna_do_eixo_falha() -> None:
    conteudo = csv_bytes(endereco("1", "4")).replace(b"DSC_ESTABELECIMENTO", b"DSC_X", 1)
    with pytest.raises(ValueError, match="DSC_ESTABELECIMENTO"):
        cnefe.csv_para_tabela(abridor(conteudo), "RR", _DT)


def test_converter_municipio_confere_bytes_contra_parser() -> None:
    csv = csv_bytes(*(endereco(str(i), str(1 + i % 8)) for i in range(100)))
    conversao = cnefe.converter_municipio(
        zip_com({"1400605_SAO_LUIZ.csv": csv}), "1400605", "RR", _DT
    )
    assert conversao.arquivo == "1400605_SAO_LUIZ.csv"
    assert conversao.linhas_arquivo == 100
    # Espécies 1..8 em rodízio: 4 e 6 são 25 das 100.
    assert conversao.linhas_especies == conversao.tabela.num_rows == 25


def test_converter_municipio_falha_quando_o_parser_perde_linha(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    csv = csv_bytes(endereco("1", "4"), endereco("2", "6"))
    original = cnefe.csv_para_tabela
    monkeypatch.setattr(
        cnefe,
        "csv_para_tabela",
        lambda *a: (original(*a)[0], 1),
    )
    with pytest.raises(ValueError, match="o CSV tem 2 linhas e o parser leu 1"):
        cnefe.converter_municipio(zip_com({"1400605_X.csv": csv}), "1400605", "RR", _DT)


# ── staging em partes ────────────────────────────────────────────────────


def _tabela(n: int, uf: str = "RR") -> pa.Table:
    linhas = [
        {c: f"{c}-{i}" for c in cnefe.COLUNAS_DESTINO} | {cnefe.COLUNA_UF: uf}
        for i in range(n)
    ]
    return pa.Table.from_pylist(linhas, schema=cnefe.SCHEMA_ARROW)


def test_partes_da_tabela_cabem_no_limite_e_juntas_dao_a_tabela() -> None:
    tabela = _tabela(2000)
    limite = 40 * 1024
    partes = datalakehouse.partes_da_tabela(tabela, limite)

    assert len(partes) > 1
    assert all(len(p) <= limite for p in partes)
    lidas = [pq.read_table(pa.BufferReader(p)) for p in partes]
    assert all(t.schema == tabela.schema for t in lidas)
    assert pa.concat_tables(lidas).equals(tabela)


def test_partes_da_tabela_sem_limite_ou_vazia_e_um_arquivo() -> None:
    assert len(datalakehouse.partes_da_tabela(_tabela(2000), None)) == 1
    # Município sem nenhuma linha das espécies: Parquet só com as colunas.
    (vazia,) = datalakehouse.partes_da_tabela(_tabela(0), 900 * 1024)
    lida = pq.read_table(pa.BufferReader(vazia))
    assert lida.num_rows == 0 and lida.schema == cnefe.SCHEMA_ARROW


def test_partes_da_tabela_falha_quando_nao_ha_como_dividir() -> None:
    with pytest.raises(ValueError, match="não há como dividi-lo"):
        datalakehouse.partes_da_tabela(_tabela(1), 100)


# ── Postgres ─────────────────────────────────────────────────────────────


def test_csv_para_copy_escreve_nulo_sem_aspas_e_texto_entre_aspas() -> None:
    tabela = pa.table(
        {"a": ["X", None, 'BAR "ZE"'], "b": ["1;2", "y", "3,4"]},
        schema=pa.schema([("a", pa.string()), ("b", pa.string())]),
    )
    assert cnefe.csv_para_copy(tabela).read() == (
        b'"X","1;2"\n,"y"\n"BAR ""ZE""","3,4"\n'
    )


def test_conferir_parte_recusa_outra_uf_e_outras_colunas() -> None:
    cnefe.conferir_parte(_tabela(3), "RR")
    cnefe.conferir_parte(_tabela(0), "RR")
    with pytest.raises(ValueError, match="fora da UF RR"):
        cnefe.conferir_parte(_tabela(3, uf="AP"), "RR")
    with pytest.raises(ValueError, match="colunas"):
        cnefe.conferir_parte(_tabela(3).drop_columns(["dt_ingest"]), "RR")


DSN = os.environ.get("TEST_POSTGRES_DSN")


@pytest.fixture
def schema() -> Iterator[str]:
    if not DSN:
        pytest.skip("TEST_POSTGRES_DSN não definido — sem Postgres para testar")
    import psycopg2

    nome = f"teste_{uuid.uuid4().hex[:10]}"
    yield nome
    conn = psycopg2.connect(DSN)
    with conn, conn.cursor() as cursor:
        cursor.execute(f'DROP SCHEMA IF EXISTS "{nome}" CASCADE')
    conn.close()


def _parquet(tabela: pa.Table) -> bytes:
    return datalakehouse.partes_da_tabela(tabela, None)[0]


def _contar(schema: str) -> dict[str, int]:
    import psycopg2

    conn = psycopg2.connect(DSN)
    with conn, conn.cursor() as cursor:
        cursor.execute(f'SELECT uf, count(*) FROM "{schema}".ibge_cnefe_2022 GROUP BY uf')
        contagem = dict(cursor.fetchall())
    conn.close()
    return contagem


def test_recarregar_uma_uf_nao_duplica_nem_toca_as_outras(schema: str) -> None:
    assert DSN
    assert cnefe.carregar_uf(DSN, schema, "RR", [_parquet(_tabela(5))]) == 5
    assert cnefe.carregar_uf(DSN, schema, "AP", [_parquet(_tabela(3, "AP"))]) == 3
    # De novo, em duas partes e com uma linha a menos: substitui.
    partes = [_parquet(_tabela(2)), _parquet(_tabela(4).slice(2))]
    assert cnefe.carregar_uf(DSN, schema, "RR", partes) == 4
    assert _contar(schema) == {"RR": 4, "AP": 3}


def test_falha_no_meio_deixa_a_uf_anterior_intacta(schema: str) -> None:
    assert DSN
    cnefe.carregar_uf(DSN, schema, "RR", [_parquet(_tabela(5))])
    with pytest.raises(ValueError, match="fora da UF"):
        cnefe.carregar_uf(
            DSN, schema, "RR", [_parquet(_tabela(2)), _parquet(_tabela(1, "AP"))]
        )
    assert _contar(schema) == {"RR": 5}


def test_valores_chegam_ao_banco_como_no_csv(schema: str) -> None:
    import psycopg2

    assert DSN
    conteudo = csv_bytes(
        endereco("2", "4", CEP="01001000", LATITUDE="-1.0179750"),
        endereco("3", "6", NOM_SEGLOGR="NA", DSC_ESTABELECIMENTO='BAR "DO ZE", 2'),
    )
    tabela, _ = cnefe.csv_para_tabela(abridor(conteudo), "RR", _DT)
    cnefe.carregar_uf(DSN, schema, "RR", [_parquet(tabela)])

    conn = psycopg2.connect(DSN)
    with conn, conn.cursor() as cursor:
        cursor.execute(
            f"SELECT cep, latitude, nom_seglogr, dsc_estabelecimento, dsc_localidade "
            f'FROM "{schema}".ibge_cnefe_2022 ORDER BY cod_unico_endereco'
        )
        linhas = cursor.fetchall()
    conn.close()
    assert linhas == [
        ("01001000", "-1.0179750", None, None, None),
        (None, None, "NA", 'BAR "DO ZE", 2', None),
    ]


# ── rede ─────────────────────────────────────────────────────────────────


class _Resposta:
    def __init__(self, corpo: bytes, tamanho: int) -> None:
        self.corpo, self.headers = corpo, {"Content-Length": str(tamanho)}

    def __enter__(self) -> "_Resposta":
        return self

    def __exit__(self, *_: Any) -> None:
        return None

    def raise_for_status(self) -> None:
        return None

    def iter_content(self, chunk_size: int) -> Iterator[bytes]:
        yield self.corpo


def test_baixar_tenta_de_novo_e_falha_no_download_incompleto(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    chamadas: list[str] = []

    def get(url: str, **_: Any) -> _Resposta:
        chamadas.append(url)
        return _Resposta(b"abc", 10)

    monkeypatch.setattr(requests, "get", get)
    monkeypatch.setattr(retry_helpers.time, "sleep", lambda _: None)
    with pytest.raises(cnefe.DownloadIncompleto, match="3 de 10 bytes"):
        cnefe.baixar("https://ftp/x.zip")
    assert len(chamadas) == 5


def test_baixar_devolve_o_corpo_quando_o_tamanho_bate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(requests, "get", lambda url, **_: _Resposta(b"abc", 3))
    assert cnefe.baixar("https://ftp/x.zip") == b"abc"


# ── DAG ──────────────────────────────────────────────────────────────────

_ARQUIVO_DAG = (
    Path(__file__).resolve().parents[1]
    / "dags"
    / "data_ingest"
    / "ibge"
    / "ibge_cnefe_2022_dag.py"
)


@pytest.fixture(scope="module")
def dag() -> Any:
    pytest.importorskip("airflow.sdk", reason="DAG do Airflow 3: roda na imagem")
    if importlib.util.find_spec("metadata") is None:
        pytest.skip("openmetadata-ingestion ausente: roda na imagem do Airflow")
    from airflow.dag_processing.dagbag import DagBag

    bag = DagBag(str(_ARQUIVO_DAG), include_examples=False)
    assert not bag.import_errors, bag.import_errors
    return bag.dags["ibge_cnefe_2022_dag"]


def test_dag_vai_de_raw_a_seeds_e_publica_a_linhagem(dag: Any) -> None:
    caminho = ["listar_ufs", "baixar_para_raw", "raw_para_parquet", "parquet_para_seeds"]
    for anterior, seguinte in zip(caminho, caminho[1:]):
        assert seguinte in dag.get_task(anterior).downstream_task_ids
    assert "publicar_linhagem" in dag.get_task("parquet_para_seeds").downstream_task_ids
    assert dag.get_task("parquet_para_seeds").max_active_tis_per_dag == 1


def test_dag_e_manual_e_aceita_so_as_ufs_do_catalogo(dag: Any) -> None:
    assert type(dag.timetable).__name__ == "NullTimetable"
    assert dag.max_active_runs == 1
    assert dag.params["ufs"] == []
    assert dag.params.get_param("ufs").schema["items"]["enum"] == list(cnefe.UFS)


def test_dag_declara_as_tabelas_de_seeds(dag: Any) -> None:
    nomes = " ".join(
        getattr(o, "name", str(o)) for o in dag.get_task("parquet_para_seeds").outlets
    )
    assert "ibge_cnefe_2022" in nomes
    assert "ibge_controle_carga" in nomes


def _modulo_dag() -> Any:
    spec = importlib.util.spec_from_file_location("ibge_cnefe_2022_dag", _ARQUIVO_DAG)
    assert spec and spec.loader
    modulo = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(modulo)
    return modulo


def test_download_retoma_do_municipio_em_que_parou(
    dag: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    modulo = _modulo_dag()
    arquivos = [
        cnefe.ArquivoMunicipio("1400027", "1400027_AMAJARI.zip", "u/amajari"),
        cnefe.ArquivoMunicipio("1400050", "1400050_ALTO_ALEGRE.zip", "u/alto"),
        cnefe.ArquivoMunicipio("1400100", "1400100_BOA_VISTA.zip", "u/boa_vista"),
    ]
    raw = datalakehouse.CAMADA_RAW
    inteiro = modulo._key(raw, "RR", "1400027", "r1")
    pela_metade = modulo._key(raw, "RR", "1400050", "r1")

    def partes_do_objeto(key: str) -> list[str]:
        if key == inteiro:
            return [key]
        if key == pela_metade:
            raise datalakehouse.PartesInconsistentes(key)
        raise FileNotFoundError(key)

    baixados: list[str] = []
    gravados: list[str] = []

    def baixar(url: str) -> bytes:
        baixados.append(url)
        return b"zip"

    def gravar(conteudo: bytes, key: str) -> str:
        gravados.append(key)
        return key

    monkeypatch.setattr(cnefe, "obter_listagem", lambda uf: arquivos)
    monkeypatch.setattr(datalakehouse, "partes_do_objeto", partes_do_objeto)
    monkeypatch.setattr(cnefe, "baixar", baixar)
    monkeypatch.setattr(datalakehouse, "gravar_bytes_em_partes", gravar)

    assert modulo._baixar_uf("RR", "r1") == ["1400027", "1400050", "1400100"]
    # O inteiro fica; o que caiu no meio e o que faltava são baixados de novo.
    assert baixados == ["u/alto", "u/boa_vista"]
    assert gravados == [pela_metade, modulo._key(raw, "RR", "1400100", "r1")]
