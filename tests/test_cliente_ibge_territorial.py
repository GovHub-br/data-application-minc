"""Testes do cliente das planilhas territoriais do IBGE (áreas, REGIC, RMs).

O que importa aqui: o catálogo gera nomes de tabela únicos que o Postgres não
corta e que não colidem com os da MUNIC (as duas fontes dividem
``seeds.ibge_controle_carga``); a nota de rodapé das planilhas de área não
entra como dado; e uma planilha republicada com outra aba ou outro número de
linhas falha em vez de carregar pela metade.

Com ``IBGE_TERRITORIAL_ARQUIVOS`` apontando para uma pasta com os arquivos do
geoftp do IBGE, o teste do fim lê as planilhas de verdade e confere cada aba
contra o catálogo. Sem a variável, é pulado.
"""

import os
from pathlib import Path

import pytest

import cliente_ibge_perfil as perfil
import cliente_ibge_territorial as ibge


def test_nomes_de_tabela_sao_unicos_e_cabem_no_postgres() -> None:
    tabelas = ibge.tabelas_seeds()
    assert len(tabelas) == len(set(tabelas))
    assert max(len(t) for t in tabelas) <= 63
    assert tabelas == [
        "ibge_areas_territoriais_mun",
        "ibge_areas_territoriais_uf",
        "ibge_composicao_au",
        "ibge_composicao_rm",
        "ibge_regic_arranjos_populacionais",
        "ibge_regic_cidades",
        "ibge_regic_hierarquia",
    ]


def test_tabelas_nao_colidem_com_as_da_munic() -> None:
    assert not set(ibge.tabelas_seeds()) & set(perfil.tabelas_seeds())


def test_todo_arquivo_esta_no_geoftp_com_versao() -> None:
    for chave, arquivo in ibge.ARQUIVOS.items():
        assert arquivo.url.startswith("https://geoftp.ibge.gov.br/"), chave
        assert ibge.extensao(arquivo.caminho) in {"xls", "xlsx"}, chave
        assert arquivo.versao in {"2018", "2025"}, chave
        assert arquivo.abas, chave


def test_aba_nova_ou_sumida_falha() -> None:
    abas = ["Hierarquia e região", "Notas, fonte e dicionário"]
    ibge.conferir_abas("regic_hierarquia", list(reversed(abas)))
    with pytest.raises(ValueError, match=r"Abas novas: \['Extra'\]"):
        ibge.conferir_abas("regic_hierarquia", abas + ["Extra"])
    with pytest.raises(ValueError, match=r"sumiram: \['Hierarquia e região'\]"):
        ibge.conferir_abas("regic_hierarquia", ["Notas, fonte e dicionário"])


_UF = ibge.ARQUIVOS["areas_territoriais"].abas[1]


def _aba_de_area(linhas_de_dados: int) -> list[list[object]]:
    """Como a planilha do IBGE: cabeçalho, dados, linha vazia e nota de rodapé."""
    cabecalho: list[object] = ["CD_UF", "NM_UF", "NM_UF_SIGLA", "AR_UF_2025"]
    dados: list[list[object]] = [
        [11.0 + i, f"UF {i}", "XX", 1000.5 + i] for i in range(linhas_de_dados)
    ]
    rodape: list[list[object]] = [
        ["", "", "", ""],
        ["", "", "", ""],
        ["OBS: Estes valores de área foram calculados a partir da malha.", "", "", ""],
    ]
    return [cabecalho, *dados, *rodape]


def test_nota_de_rodape_nao_entra_como_dado() -> None:
    registros = ibge.registros_da_aba("areas_territoriais", _UF, _aba_de_area(27))
    assert len(registros) == 27
    assert registros[0] == {
        "cd_uf": "11",
        "nm_uf": "UF 0",
        "nm_uf_sigla": "XX",
        "ar_uf_2025": "1000.5",
        "versao": "2025",
    }
    assert not any("OBS" in str(r["cd_uf"]) for r in registros)


def test_aba_com_outro_numero_de_linhas_falha() -> None:
    with pytest.raises(ValueError, match="26 linhas, esperava 27"):
        ibge.registros_da_aba("areas_territoriais", _UF, _aba_de_area(26))


def test_linha_vazia_no_meio_dos_dados_corta_a_tabela_e_falha() -> None:
    # A tabela termina na primeira linha vazia. Se o IBGE deixar uma no meio,
    # a contagem não bate e a carga falha, em vez de perder a metade de baixo.
    linhas = _aba_de_area(27)
    linhas.insert(10, ["", None, "", ""])
    with pytest.raises(ValueError, match="9 linhas, esperava 27"):
        ibge.registros_da_aba("areas_territoriais", _UF, linhas)


def test_codigo_com_zero_a_esquerda_fica_como_veio() -> None:
    aba = ibge.ARQUIVOS["composicao_rm"].abas[1]
    linhas: list[list[object]] = [["COD_RECAU", "COD_MUN"]]
    linhas += [["003", 3516200.0]] * aba.linhas
    registros = ibge.registros_da_aba("composicao_rm", aba, linhas)
    assert registros[0] == {"cod_recau": "003", "cod_mun": "3516200", "versao": "2025"}


def test_planilha_ja_com_coluna_versao_falha() -> None:
    with pytest.raises(ValueError, match="já tem coluna versao"):
        ibge.registros_da_aba("areas_territoriais", _UF, [["Versão"], ["x"]])


def test_aba_vazia_falha() -> None:
    with pytest.raises(ValueError, match="aba vazia"):
        ibge.registros_da_aba("areas_territoriais", _UF, [])


# ── planilhas de verdade ─────────────────────────────────────────────────

_PASTA = os.environ.get("IBGE_TERRITORIAL_ARQUIVOS")


@pytest.mark.skipif(not _PASTA, reason="IBGE_TERRITORIAL_ARQUIVOS não definida")
@pytest.mark.parametrize("chave", sorted(ibge.ARQUIVOS))
def test_planilha_do_geoftp_bate_com_o_catalogo(chave: str) -> None:
    pytest.importorskip("python_calamine")
    arquivo = ibge.ARQUIVOS[chave]
    caminho = Path(str(_PASTA)) / Path(arquivo.caminho).name
    livro = ibge.abrir_livro(caminho.read_bytes())
    ibge.conferir_abas(chave, list(livro.sheet_names))
    for aba in arquivo.abas:
        registros = ibge.registros_da_aba(chave, aba, ibge.linhas_da_aba(livro, aba.nome))
        assert len(registros) == aba.linhas
        assert all(r[ibge.COLUNA_VERSAO] == arquivo.versao for r in registros)
