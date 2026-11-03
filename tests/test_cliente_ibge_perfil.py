"""Testes do cliente da MUNIC e da ESTADIC do IBGE.

O que importa aqui: o catálogo gera nomes de tabela únicos que o Postgres
não corta, uma planilha republicada com outra aba ou outro número de linhas
falha em vez de carregar pela metade, e o ZIP do IBGE entrega a planilha que
está dentro dele.

Com ``IBGE_PERFIL_ARQUIVOS`` apontando para uma pasta com os arquivos do FTP
do IBGE, os testes do fim leem as planilhas de verdade e conferem cada aba
contra o catálogo. Sem a variável, são pulados.
"""

import io
import os
import zipfile
from pathlib import Path

import pytest

import cliente_ibge_perfil as ibge


def test_nomes_de_tabela_sao_unicos_e_cabem_no_postgres() -> None:
    tabelas = ibge.tabelas_seeds()
    assert len(tabelas) == len(set(tabelas))
    assert max(len(t) for t in tabelas) <= 63
    assert "ibge_munic_2021_cultura" in tabelas
    assert "ibge_estadic_cultura_2014_orgao_gestor" in tabelas
    assert "ibge_munic_cultura_2006_conselhos_municipais" in tabelas


def test_toda_edicao_tem_dicionario_e_url_no_ftp() -> None:
    for chave, edicao in ibge.EDICOES.items():
        assert ibge.ABA_DICIONARIO in edicao.abas, chave
        assert edicao.url.startswith("https://ftp.ibge.gov.br/"), chave
        assert ibge.extensao(edicao.caminho) in {"zip", "xls", "xlsx"}, chave
        assert str(edicao.ano) in chave, chave


def test_as_edicoes_que_o_eixo_1_usa_estao_no_catalogo() -> None:
    # MUNIC.R e ESTADIC.R do repositório cultura-em-numeros-eixo-1.
    assert set(ibge.EDICOES) >= {
        "munic_cultura_2006",
        "munic_cultura_2014",
        "munic_2018",
        "munic_2021",
        "estadic_2012",
        "estadic_cultura_2014",
        "estadic_2018",
        "estadic_2021",
        "estadic_2023",
        "estadic_2024",
    }


def test_aba_nova_ou_sumida_falha() -> None:
    abas = list(ibge.EDICOES["estadic_2021"].abas)
    ibge.conferir_abas("estadic_2021", list(reversed(abas)))
    with pytest.raises(ValueError, match=r"Abas novas: \['Turismo'\]"):
        ibge.conferir_abas("estadic_2021", abas + ["Turismo"])
    with pytest.raises(ValueError, match=r"sumiram: \['Cultura'\]"):
        ibge.conferir_abas("estadic_2021", [a for a in abas if a != "Cultura"])


def _aba(linhas_de_dados: int) -> list[list[object]]:
    cabecalho: list[object] = ["Cod UF", "Nome UF", "ECUL01"]
    dados: list[list[object]] = [
        [11.0 + i, f"UF {i}", "Sim"] for i in range(linhas_de_dados)
    ]
    return [cabecalho, *dados, ["", None, ""]]


def test_aba_de_dados_vira_uma_linha_por_uf_com_a_edicao() -> None:
    registros = ibge.registros_da_aba("estadic_2021", "Cultura", _aba(27))
    assert len(registros) == 27
    assert registros[0] == {
        "cod_uf": "11",
        "nome_uf": "UF 0",
        "ecul01": "Sim",
        "edicao": "estadic_2021",
    }


def test_aba_de_dados_com_outro_numero_de_linhas_falha() -> None:
    with pytest.raises(ValueError, match="26 linhas, esperava 27"):
        ibge.registros_da_aba("estadic_2021", "Cultura", _aba(26))


def test_dicionario_nao_precisa_ter_uma_linha_por_ente() -> None:
    linhas = [["Temas disponíveis", "variável", ""], ["Cultura", "ECUL01", "texto"]]
    registros = ibge.registros_da_aba("estadic_2021", ibge.ABA_DICIONARIO, linhas)
    assert registros == [
        {
            "temas_disponiveis": "Cultura",
            "variavel": "ECUL01",
            "coluna_3": "texto",
            "edicao": "estadic_2021",
        }
    ]


def test_planilha_ja_com_coluna_edicao_falha() -> None:
    with pytest.raises(ValueError, match="já tem coluna edicao"):
        ibge.registros_da_aba("estadic_2021", "Cultura", [["Edição"], ["x"]])


def test_zip_entrega_a_planilha_de_dentro() -> None:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as arquivo:
        arquivo.writestr("Base ESTADIC 2012.xls", b"conteudo")
    nome, planilha = ibge.planilha_do_arquivo(
        buffer.getvalue(), "Base_ESTADIC_2012_xls.zip"
    )
    assert (nome, planilha) == ("Base ESTADIC 2012.xls", b"conteudo")
    assert ibge.planilha_do_arquivo(b"x", "Base_Estadic_2021.xlsx") == (
        "Base_Estadic_2021.xlsx",
        b"x",
    )


def test_zip_com_mais_de_uma_planilha_falha() -> None:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as arquivo:
        arquivo.writestr("a.xls", b"1")
        arquivo.writestr("b.xls", b"2")
    with pytest.raises(ValueError, match="esperava uma planilha"):
        ibge.planilha_do_arquivo(buffer.getvalue(), "x.zip")


# ── planilhas de verdade ─────────────────────────────────────────────────

_PASTA = os.environ.get("IBGE_PERFIL_ARQUIVOS")


@pytest.mark.skipif(not _PASTA, reason="IBGE_PERFIL_ARQUIVOS não definida")
@pytest.mark.parametrize("chave", sorted(ibge.EDICOES))
def test_planilha_do_ftp_bate_com_o_catalogo(chave: str) -> None:
    pytest.importorskip("python_calamine")
    edicao = ibge.EDICOES[chave]
    caminho = Path(str(_PASTA)) / Path(edicao.caminho).name
    nome, planilha = ibge.planilha_do_arquivo(caminho.read_bytes(), caminho.name)
    livro = ibge.abrir_livro(planilha)
    ibge.conferir_abas(chave, list(livro.sheet_names))
    for aba in livro.sheet_names:
        registros = ibge.registros_da_aba(chave, aba, ibge.linhas_da_aba(livro, aba))
        assert registros, f"{chave} / {aba}"
        assert len(registros[0]) <= 1600
