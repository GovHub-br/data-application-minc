"""Testes do cliente do portal de dados abertos do MinC (lista do SNC).

O que importa aqui: a planilha que o portal chama de XLS e é xlsx abre, cada
ente vira um registro com a versão, e uma planilha publicada com outra forma
(sem ``Cod.IBGE``, com código repetido ou vazio, cortada) falha em vez de
carregar.

Com ``DADOS_CULTURA_REDE=1`` o último teste baixa a planilha de verdade e
confere a forma dela. Sem a variável, é pulado.
"""

import io
import os
from typing import Any

import pytest

import cliente_dados_cultura as portal

openpyxl = pytest.importorskip("openpyxl")
pytest.importorskip("python_calamine")

_SNC = portal.RECURSOS["snc_entes"]
_PEQUENO = portal.Recurso("x", "y", chave="cod_ibge", minimo_de_linhas=2)


def _xlsx(linhas: list[list[Any]]) -> bytes:
    livro = openpyxl.Workbook()
    aba = livro.active
    aba.title = "SNC"
    for linha in linhas:
        aba.append(linha)
    buffer = io.BytesIO()
    livro.save(buffer)
    return buffer.getvalue()


_CABECALHO = ["Ente Federado", "UF", "Cod.IBGE", "Situação", "População [2022]"]


def test_planilha_vira_um_registro_por_ente_com_a_versao() -> None:
    conteudo = _xlsx(
        [
            _CABECALHO,
            ["Estado de Rondônia", "RO", 11, "Publicado no DOU", 1581196],
            ["Porto Velho", "RO", 1100205, "Nao possui adesão", 460434],
            [None, None, None, None, None],
        ]
    )
    registros = portal.registros(
        _PEQUENO, portal.linhas_da_planilha(conteudo), "2026-05-28"
    )
    assert registros == [
        {
            "ente_federado": "Estado de Rondônia",
            "uf": "RO",
            "cod_ibge": "11",
            "situacao": "Publicado no DOU",
            "populacao_2022": "1581196",
            "versao": "2026-05-28",
        },
        {
            "ente_federado": "Porto Velho",
            "uf": "RO",
            "cod_ibge": "1100205",
            "situacao": "Nao possui adesão",
            "populacao_2022": "460434",
            "versao": "2026-05-28",
        },
    ]


@pytest.mark.parametrize(
    ("linhas", "erro"),
    [
        ([["Ente", "UF"], ["A", "RO"], ["B", "RO"]], "não tem a coluna cod_ibge"),
        ([_CABECALHO, ["A", "RO", 11, "x", 1], ["B", "RO", 11, "x", 1]], "repetido"),
        ([_CABECALHO, ["A", "RO", 11, "x", 1], ["B", "RO", None, "x", 1]], "vazio"),
        ([_CABECALHO, ["A", "RO", 11, "x", 1]], "esperava ao menos 2"),
        ([_CABECALHO[:2] + ["Cod.IBGE", "Versão"], ["A", "RO", 1, "v"]], "coluna versao"),
    ],
)
def test_planilha_com_outra_forma_falha(linhas: list[list[Any]], erro: str) -> None:
    with pytest.raises(ValueError, match=erro):
        portal.registros(_PEQUENO, portal.linhas_da_planilha(_xlsx(linhas)), "v")


def test_tabela_no_schema_seeds() -> None:
    assert portal.tabela_seeds("snc_entes") == "dados_cultura_snc_entes"


class _Resposta:
    def __init__(self, corpo: dict[str, Any], status_code: int = 200) -> None:
        self.corpo = corpo
        self.status_code = status_code

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")

    def json(self) -> dict[str, Any]:
        return self.corpo


def test_descrever_acha_o_recurso_pelo_id(monkeypatch: pytest.MonkeyPatch) -> None:
    pacote = {
        "metadata_modified": "2026-09-08T10:00:00",
        "resources": [
            {"id": "outro", "url": "https://x/dicionario.pdf"},
            {
                "id": _SNC.recurso_id,
                "url": "https://x/snc.xls",
                "last_modified": "2026-05-28T09:00:00",
            },
        ],
    }
    monkeypatch.setattr(
        portal.requests, "get", lambda url, **kw: _Resposta({"result": pacote})
    )
    assert portal.descrever(_SNC) == {"url": "https://x/snc.xls", "versao": "2026-05-28"}


def test_sem_last_modified_falha_em_vez_de_adivinhar_a_versao(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # O created não muda quando o arquivo é trocado, e o metadata_modified do
    # pacote muda a cada edição de metadado: nenhum dos dois é a versão.
    pacote = {
        "metadata_modified": "2026-09-08T10:00:00",
        "resources": [
            {
                "id": _SNC.recurso_id,
                "url": "https://x/snc.xls",
                "created": "2026-05-28T09:00:00",
                "last_modified": None,
            }
        ],
    }
    monkeypatch.setattr(
        portal.requests, "get", lambda url, **kw: _Resposta({"result": pacote})
    )
    with pytest.raises(ValueError, match="sem last_modified"):
        portal.descrever(_SNC)


def test_dados_sensiveis_sao_de_recursos_do_catalogo() -> None:
    assert set(portal.DADOS_SENSIVEIS) <= set(portal.RECURSOS)
    colunas = [c for grupo in portal.DADOS_SENSIVEIS["snc_entes"].values() for c in grupo]
    assert len(colunas) == len(set(colunas))
    assert "dados_bancarios_do_fundo_de_cultura" in colunas


def test_erro_500_tenta_de_novo_e_desiste_na_terceira(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    respostas = [_Resposta({}, 500), _Resposta({"ok": 1}, 200)]
    monkeypatch.setattr(portal.requests, "get", lambda url, **kw: respostas.pop(0))
    monkeypatch.setattr(portal, "_ESPERA_S", 0)
    assert portal._get("https://x", 5).json() == {"ok": 1}

    monkeypatch.setattr(portal.requests, "get", lambda url, **kw: _Resposta({}, 500))
    with pytest.raises(RuntimeError, match="HTTP 500"):
        portal._get("https://x", 5)


def test_erro_404_nao_tenta_de_novo(monkeypatch: pytest.MonkeyPatch) -> None:
    chamadas: list[str] = []

    def falso(url: str, **kw: Any) -> _Resposta:
        chamadas.append(url)
        return _Resposta({}, 404)

    monkeypatch.setattr(portal.requests, "get", falso)
    with pytest.raises(RuntimeError, match="HTTP 404"):
        portal._get("https://x", 5)
    assert len(chamadas) == 1


@pytest.mark.skipif(not os.environ.get("DADOS_CULTURA_REDE"), reason="sem rede")
def test_planilha_publicada_tem_a_forma_que_os_scripts_usam() -> None:
    descricao = portal.descrever(_SNC)
    linhas = portal.linhas_da_planilha(portal.baixar(descricao["url"]))
    registros = portal.registros(_SNC, linhas, descricao["versao"])
    colunas = set(registros[0])
    assert {
        "cod_ibge",
        "situacao",
        "data_adesao",
        "situacao_da_lei_do_sistema_de_cultura",
        "perfil_do_orgao_gestor",
        "situacao_da_lei_do_conselho_de_politica_cultural",
        "conselho_paritario",
        "natureza_do_conselho",
        "situacao_da_lei_do_fundo_de_cultura",
        "situacao_do_plano_de_cultura",
        "possui_metas",
        "idh_2021",
        "populacao_2022",
    } <= colunas
    assert sum(1 for r in registros if len(r["cod_ibge"] or "") == 2) == 27
    # A documentação diz quais colunas têm dado bancário, CNPJ, nome e
    # endereço; se o portal mudar alguma, o texto tem de mudar junto.
    sensiveis = {c for g in portal.DADOS_SENSIVEIS["snc_entes"].values() for c in g}
    assert sensiveis <= colunas
    # A única linha que não é ente é a "Não cadastrado" (ver o módulo).
    codigos = {r["cod_ibge"] for r in registros}
    fora_do_padrao = {c for c in codigos if not (c.isdigit() and len(c) in (2, 7))}
    assert fora_do_padrao <= {"Não cadastrado"}
