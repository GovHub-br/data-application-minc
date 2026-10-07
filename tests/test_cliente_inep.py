"""Testes das partes puras do cliente do Censo Escolar do INEP.

O que importa aqui: cada arquivo que o INEP publicou de 1995 a 2025 cai numa
entidade conhecida (e um arquivo novo falha em vez de inventar tabela), os
dois layouts de CSV viram Parquet sem perder linha nem acento, e o CENSOESC
-- largo demais para o Postgres -- vai para ``jsonb`` sem os nulos.
"""

import io
import json
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import cliente_inep as inep

_DT = datetime(2026, 10, 7, 12, 0, tzinfo=timezone.utc)


# ── descoberta e nomes ───────────────────────────────────────────────────


def test_extrair_urls_aceita_o_sublinhado_final_de_2025() -> None:
    html = """
    <a href="https://download.inep.gov.br/dados_abertos/microdados_censo_escolar_1995.zip">1995</a>
    <a href="https://download.inep.gov.br/dados_abertos/microdados_censo_escolar_2024.zip">2024</a>
    <a href="https://download.inep.gov.br/dados_abertos/microdados_censo_escolar_2025_.zip">2025</a>
    <a href="https://download.inep.gov.br/dados_abertos/microdados_enem_2024.zip">enem</a>
    """
    assert inep.extrair_urls(html) == {
        1995: "https://download.inep.gov.br/dados_abertos/microdados_censo_escolar_1995.zip",
        2024: "https://download.inep.gov.br/dados_abertos/microdados_censo_escolar_2024.zip",
        2025: "https://download.inep.gov.br/dados_abertos/microdados_censo_escolar_2025_.zip",
    }


# Todos os arquivos de dados que os 31 ZIPs de 1995 a 2025 trazem
# (inventário completo em outubro de 2026), com o caminho interno como está no ZIP.
@pytest.mark.parametrize(
    ("arquivo", "entidade"),
    [
        ("microdados_educaç╞o_básica_1995/DADOS/CENSOESC_1995.CSV", "censoesc"),
        ("microdados_educaç╞o_básica_1995/DADOS/DADOSCURSO_1995.CSV", "dadoscurso"),
        ("microdados_educaç╞o_básica_1995/DADOS/DADOS_DESP_1995.CSV", "dados_desp"),
        ("microdados_educaç╞o_básica_1996/Dados/EM8_1996.CSV", "em8"),
        ("microdados_educaç╞o_básica_1996/Dados/EM11_1996.CSV", "em11"),
        ("microdados_educaç╞o_básica_1996/Dados/ES6_1996.CSV", "es6"),
        ("microdados_educaç╞o_básica_1997/Dados/EM12_1997.CSV", "em12"),
        ("microdados_educaç╞o_básica_2000/Dados/INDICESC_2000.CSV", "indicesc"),
        ("microdados_educaç╞o_básica_2000/Dados/INDICREG_2000.CSV", "indicreg"),
        ("microdados_educaç╞o_básica_2000/Dados/MEDPROF_2000.CSV", "medprof"),
        ("microdados_educaç╞o_básica_2005/DADOS/EDUCPROF_2005.CSV", "educprof"),
        ("microdados_educaç╞o_básica_2005/DADOS/EM22_2005.CSV", "em22"),
        ("microdados_ed_basica_2015/dados/microdados_ed_basica_2015.csv", "ed_basica"),
        (
            "microdados_censo_escolar_2024_defeso/dados/microdados_ed_basica_2024.csv",
            "ed_basica",
        ),
        (
            "microdados_censo_escolar_2023/dados/suplemento_cursos_tecnicos_2023.csv",
            "suplemento_cursos_tecnicos",
        ),
        ("microdados_censo_escolar_2025_v2/dados/Tabela_Escola_2025_V2.csv", "escola"),
        ("microdados_censo_escolar_2025_v2/dados/Tabela_Docente_2025_V2.csv", "docente"),
        (
            "microdados_censo_escolar_2025_v2/dados/Tabela_Matricula_2025_V2.csv",
            "matricula",
        ),
        ("microdados_censo_escolar_2025_v2/dados/Tabela_Turma_2025_V2.csv", "turma"),
        (
            "microdados_censo_escolar_2025_v2/dados/Tabela_Gestor_Escolar_2025_v2.csv",
            "gestor_escolar",
        ),
        (
            "microdados_censo_escolar_2025_v2/dados/Tabela_Curso_Tecnico_2025_V2.csv",
            "curso_tecnico",
        ),
    ],
)
def test_entidade_do_arquivo_cobre_a_serie_inteira(arquivo: str, entidade: str) -> None:
    assert inep.entidade_do_arquivo(arquivo) == entidade
    assert entidade in inep.ENTIDADES


def test_arquivo_desconhecido_falha_com_o_nome() -> None:
    with pytest.raises(ValueError, match="Tabela_Aluno_2026.csv"):
        inep.entidade_do_arquivo("x/dados/Tabela_Aluno_2026.csv")


def test_tabela_seeds_leva_o_prefixo_da_fonte() -> None:
    assert inep.tabela_seeds("ed_basica") == "inep_ed_basica"


def _zip(arquivos: dict[str, bytes]) -> zipfile.ZipFile:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as z:
        for nome, conteudo in arquivos.items():
            z.writestr(nome, conteudo)
    buffer.seek(0)
    return zipfile.ZipFile(buffer)


def test_csvs_de_dados_ignora_anexos_md5_e_lixo_do_office() -> None:
    arquivo = _zip(
        {
            "m_2024/dados/microdados_ed_basica_2024.csv": b"a;b\n",
            "m_2024/dados/md5_microdados_ed_basica_2024.txt": b"x",
            "m_2024/dados/suplemento_cursos_tecnicos_2024.csv": b"a;b\n",
            "m_2024/Anexos/ANEXO I/~$dicionario.xlsx": b"x",
            "m_2024/Anexos/ANEXO I/dicionario.xlsx": b"x",
            "m_2024/leia-me/Leia-me.pdf": b"x",
            "m_1995/DADOS/CENSOESC_1995.CSV": b"a|b\n",
        }
    )
    nomes = sorted(Path(i.filename).name for i in inep.csvs_de_dados(arquivo))
    assert nomes == [
        "CENSOESC_1995.CSV",
        "microdados_ed_basica_2024.csv",
        "suplemento_cursos_tecnicos_2024.csv",
    ]


@pytest.mark.parametrize(
    ("cabecalho", "separador"),
    [
        ("MASCARA|CO_IBGE|NU_ANO|UF", "|"),
        ("NU_ANO_CENSO;NO_REGIAO;CO_REGIAO", ";"),
    ],
)
def test_detectar_separador(cabecalho: str, separador: str) -> None:
    assert inep.detectar_separador(cabecalho) == separador


# ── contagem e conversão ─────────────────────────────────────────────────


def _abrir(conteudo: bytes):  # type: ignore[no-untyped-def]
    return lambda: io.BytesIO(conteudo)


@pytest.mark.parametrize(
    ("conteudo", "linhas"),
    [
        (b"a;b\n1;2\n3;4\n", 2),
        (b"a;b\r\n1;2\r\n3;4", 2),
        (b"a;b\n", 0),
    ],
)
def test_contar_linhas_desconta_o_cabecalho(conteudo: bytes, linhas: int) -> None:
    assert inep.contar_linhas(_abrir(conteudo)) == linhas


def test_csv_para_parquet_layout_novo(tmp_path: Path) -> None:
    conteudo = (
        "NU_ANO_CENSO;NO_MUNICIPIO;CO_MUNICIPIO;DS_COMPLEMENTO\r\n"
        "2015;São João d'Aliança;5220009;\r\n"
        "2015;Alta Floresta D'Oeste;1100015;ALDEIA\r\n"
    ).encode("latin-1")
    destino = str(tmp_path / "x.parquet")

    linhas = inep.csv_para_parquet(_abrir(conteudo), destino, 2015, _DT)

    tabela = pq.read_table(destino)
    assert linhas == 2 == tabela.num_rows
    assert tabela.column_names == [
        "nu_ano_censo",
        "no_municipio",
        "co_municipio",
        "ds_complemento",
        "ano_censo",
        "dt_ingestao",
    ]
    assert all(campo.type == pa.string() for campo in tabela.schema)
    registros = tabela.to_pylist()
    assert registros[0]["no_municipio"] == "São João d'Aliança"
    # Código IBGE fica texto: não vira número nem perde zero à esquerda.
    assert registros[0]["co_municipio"] == "5220009"
    # Vazio vira nulo, nunca string vazia.
    assert registros[0]["ds_complemento"] is None
    assert registros[0]["ano_censo"] == "2015"
    assert registros[0]["dt_ingestao"] == _DT.isoformat()


def test_csv_para_parquet_layout_antigo_com_pipe(tmp_path: Path) -> None:
    conteudo = "MASCARA|CO_IBGE|UF|VPE1001\n1|5300108|DF|0\n2|5300108|DF|\n".encode(
        "latin-1"
    )
    destino = str(tmp_path / "x.parquet")
    assert inep.csv_para_parquet(_abrir(conteudo), destino, 1995, _DT) == 2
    assert pq.read_table(destino).column("vpe1001").to_pylist() == ["0", None]


def test_csv_para_parquet_recusa_coluna_repetida(tmp_path: Path) -> None:
    conteudo = b"A;a\n1;2\n"
    with pytest.raises(ValueError, match="repetidas"):
        inep.csv_para_parquet(_abrir(conteudo), str(tmp_path / "x.parquet"), 2015, _DT)


# ── destino no Postgres ──────────────────────────────────────────────────


def _lote(colunas: dict[str, list[str | None]]) -> pa.RecordBatch:
    return pa.RecordBatch.from_pydict(
        {nome: pa.array(valores, pa.string()) for nome, valores in colunas.items()}
    )


def test_tabela_comum_vai_coluna_a_coluna() -> None:
    lote = _lote({"co_entidade": ["1", "2"], "no_entidade": ["A", None]})
    colunas = inep.colunas_destino("ed_basica", lote.schema.names)
    assert colunas == ["co_entidade", "no_entidade"]
    assert list(inep.linhas_para_destino("ed_basica", lote, colunas)) == [
        ["1", "A"],
        ["2", None],
    ]


def test_censoesc_separa_chaves_e_joga_o_resto_no_jsonb_sem_nulos() -> None:
    lote = _lote(
        {
            "mascara": ["10"],
            "co_ibge": ["5300108"],
            "uf": ["DF"],
            "vpe1001": ["3"],
            "vpe1002": [None],
            "ano_censo": ["1995"],
            "dt_ingestao": ["2026-10-07"],
        }
    )
    colunas = inep.colunas_destino("censoesc", lote.schema.names)
    assert colunas == [
        "mascara",
        "co_ibge",
        "uf",
        "ano_censo",
        "dt_ingestao",
        "dados",
    ]

    [linha] = list(inep.linhas_para_destino("censoesc", lote, colunas))
    assert linha[:5] == ["10", "5300108", "DF", "1995", "2026-10-07"]
    assert json.loads(linha[5]) == {"vpe1001": "3"}


def test_payload_do_copy_distingue_nulo_e_escapa_separador() -> None:
    buffer = inep._copy_csv(iter([["1", None, 'Escola "A", Centro']]))
    # Nulo é campo vazio sem aspas (o NULL padrão do COPY csv); o resto é
    # citado quando precisa.
    assert buffer.getvalue() == '1,,"Escola ""A"", Centro"\n'


def test_lote_do_copy_cabe_em_dois_milhoes_de_celulas() -> None:
    assert inep._linhas_por_lote(426) == 4694
    assert inep._linhas_por_lote(3808) == 525
    assert inep._linhas_por_lote(10_000) == 500


def test_lote_do_censoesc_conta_as_colunas_do_parquet_e_nao_as_do_destino(
    tmp_path: Path,
) -> None:
    colunas = {f"vpe{i}": pa.array(["1"], pa.string()) for i in range(3800)}
    colunas |= {"mascara": pa.array(["1"]), "ano_censo": pa.array(["2006"])}
    destino = str(tmp_path / "censoesc.parquet")
    pq.write_table(pa.table(colunas), destino)

    arquivo = pq.ParquetFile(destino)
    assert len(inep.colunas_destino("censoesc", arquivo.schema_arrow.names)) < 10
    # 3.802 colunas no Parquet -> 526 linhas por lote; pelo destino (4 colunas), 500 mil.
    assert inep.tamanho_do_lote(arquivo) == 526


def test_conferir_contagens_aponta_o_ano_e_a_entidade_divergentes() -> None:
    ok = {"ano": 2015, "entidade": "ed_basica", "linhas_csv": 3}
    ok |= {"linhas_parquet": 3, "linhas_carregadas": 3}
    inep.conferir_contagens([ok])
    ruim = ok | {"ano": 2016, "linhas_carregadas": 2}
    with pytest.raises(ValueError, match="2016 ed_basica: 3/3/2"):
        inep.conferir_contagens([ok, ruim])


def test_intermediaria_com_impressao_digital_errada_e_recusada() -> None:
    with pytest.raises(ValueError, match="SHA-256"):
        inep.intermediaria_pem(b"nao e o certificado")


def test_intermediaria_esperada_vira_pem(monkeypatch: pytest.MonkeyPatch) -> None:
    import hashlib

    der = b"qualquer conteudo"
    monkeypatch.setattr(inep, "_SHA256_INTERMEDIARIA", hashlib.sha256(der).hexdigest())
    pem = inep.intermediaria_pem(der)
    assert pem.startswith("-----BEGIN CERTIFICATE-----")
