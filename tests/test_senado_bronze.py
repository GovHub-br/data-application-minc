"""Bronze da legislação do Senado: do envelope do raw para linhas e SQL do Trino.

O SQL é conferido como texto, como em ``test_siconfi_bronze.py``; a forma do
``INSERT`` (``json_parse`` com ``UNNEST``) é a que a bronze do SICONFI já
executa no Trino.
"""

import json
from datetime import datetime, timezone
from typing import Any, Sequence

import pytest

import senado_bronze as sb

_ARQ_LISTA = "raw/senado/legislacao_lista/ano=1990/tipo=DEC/r.json"
_ARQ_DETALHE = "raw/senado/legislacao_detalhe/ano=1991/id=550189/r.json"
_AGORA = datetime(2026, 11, 10, 12, 0, tzinfo=timezone.utc)


def _envelope(entidade: str, resposta: Any, itens: int = 1) -> dict[str, Any]:
    return {
        "fonte": "senado",
        "entidade": entidade,
        "requisicao": {
            "path": "/legislacao/lista",
            "params": {"tipo": "DEC", "ano": 1990},
        },
        "status_http": 200,
        "buscado_em": "2026-11-10T12:00:00+00:00",
        "run_id": "r",
        "itens": itens,
        "resposta": resposta,
    }


def _lista(documentos: Any) -> dict[str, Any]:
    return _envelope(
        sb.TABELA_LISTA,
        {
            "ListaDocumento": {
                "Metadados": {"Versao": "10/11/2026"},
                "documentos": documentos,
            }
        },
    )


_DOC_LISTA = {
    "id": "522608",
    "tipo": "DEC-n",
    "numero": "99967",
    "normaNome": "Decreto nº 99.967 de 28/12/1990",
    "ementa": "ABRE AO ORÇAMENTO FISCAL DA UNIÃO, CREDITO D'AGUA",
}


def test_linhas_da_lista_em_minusculas_e_numeradas() -> None:
    linhas = sb.linhas_da_lista(
        _ARQ_LISTA, _lista({"documento": [_DOC_LISTA, _DOC_LISTA]})
    )
    assert [ln["_item"] for ln in linhas] == ["1", "2"]
    assert linhas[0]["normanome"] == "Decreto nº 99.967 de 28/12/1990"
    assert linhas[0]["_arquivo"] == _ARQ_LISTA
    assert set(linhas[0]) <= set(sb.COLUNAS_LISTA)


def test_lista_vazia_nao_tem_linha() -> None:
    assert sb.linhas_da_lista(_ARQ_LISTA, _lista("")) == []


def test_campo_que_nao_serve_de_coluna_falha() -> None:
    with pytest.raises(ValueError, match="não serve de coluna"):
        sb.linhas_da_lista(_ARQ_LISTA, _lista({"documento": {"data-assinatura": "x"}}))
    with pytest.raises(ValueError, match="dois campos"):
        sb.linhas_da_lista(_ARQ_LISTA, _lista({"documento": {"id": "1", "ID": "2"}}))


_DOC_DETALHE = {
    "id": "550189",
    "identificacao": {
        "tipo": "LEI-n",
        "normaNome": "Lei nº 8.313 de 23/12/1991",
        "urlDocumento": "https://normas.leg.br/?urn=urn:lex:br:federal:lei:1991-12-23;8313",
    },
    "publicacoes": {"publicacao": {"id": "14254685", "tipo": "PUB"}},
    "vides": {"vide": [{"codnormaposterior": "1"}, {"codnormaposterior": "2"}]},
    "edivs": "",
    "disps": "",
    "ementa": "Restabelece princípios da Lei nº 7.505",
    "observacao": "AUTOR: EXECUTIVO",
    "catalogo": {"frase": [" POLITICA CULTURAL ."]},
    "indexacao": {"frase": [" CRIAÇÃO , FICART .", " OBJETIVO , PRONAC ."]},
    "classes": {"classe": "Classificação Temática Unificada / Política Social / Cultura"},
    "novidade": {"a": 1},
}


def _detalhe(documento: Any) -> dict[str, Any]:
    return _envelope(
        sb.TABELA_DETALHE, {"DetalheDocumento": {"documentos": {"documento": documento}}}
    )


def test_detalhe_achata_a_identificacao_e_guarda_o_campo_novo() -> None:
    (linha,) = sb.linhas_do_detalhe(_ARQ_DETALHE, _detalhe([_DOC_DETALHE]))
    assert linha["identificacao__normanome"] == "Lei nº 8.313 de 23/12/1991"
    assert str(linha["identificacao__urldocumento"]).startswith("https://normas.leg.br")
    assert linha["ementa"] == "Restabelece princípios da Lei nº 7.505"
    assert linha["novidade"] == '{"a":1}'
    assert not set(linha) & set(sb.SECOES)


def test_secoes_vazia_um_e_varios_elementos() -> None:
    linhas = sb.secoes_do_detalhe(_ARQ_DETALHE, _detalhe(_DOC_DETALHE))
    por_secao: dict[str, list[dict[str, Any]]] = {}
    for linha in linhas:
        por_secao.setdefault(str(linha["secao"]), []).append(linha)

    assert "edivs" not in por_secao and "disps" not in por_secao  # ""
    assert len(por_secao["publicacoes"]) == 1  # objeto
    assert [ln["_item"] for ln in por_secao["vides"]] == ["1", "2"]  # lista
    assert por_secao["vides"][0]["valor"] == '{"codnormaposterior":"1"}'
    # Elemento texto fica texto, para a silver filtrar sem desfazer JSON.
    assert por_secao["classes"][0]["valor"] == (
        "Classificação Temática Unificada / Política Social / Cultura"
    )
    assert {ln["id"] for ln in linhas} == {"550189"}


def test_converter_recusa_envelope_de_outra_pasta() -> None:
    with pytest.raises(ValueError, match="pasta"):
        sb.converter(_ARQ_LISTA, sb.TABELA_LISTA, 1, _detalhe(_DOC_DETALHE))


def test_registro_tira_os_documentos_da_resposta() -> None:
    convertido = sb.converter(
        _ARQ_LISTA, sb.TABELA_LISTA, 2, _lista({"documento": _DOC_LISTA})
    )
    linha = sb.registro(convertido, "carga", _AGORA)
    assert linha["linhas"] == "1" and linha["partes"] == "2"
    assert linha["linhas_secoes"] is None
    assert json.loads(str(linha["resposta_sem_documentos"])) == {
        "ListaDocumento": {"Metadados": {"Versao": "10/11/2026"}}
    }
    assert json.loads(str(linha["requisicao"]))["params"] == {"tipo": "DEC", "ano": 1990}
    assert set(linha) == set(sb.COLUNAS_ARQUIVOS)


def test_insert_fica_abaixo_do_limite_e_dobra_o_apostrofo() -> None:
    linhas = sb.linhas_da_lista(_ARQ_LISTA, _lista({"documento": [_DOC_LISTA] * 50}))
    colunas = sb.colunas_das_linhas(sb.COLUNAS_LISTA, linhas)
    comandos = sb.inserts("dw", "senado", sb.TABELA_LISTA, colunas, linhas, limite=4_000)

    assert len(comandos) > 1
    assert all(len(c) <= 4_000 for c in comandos)
    assert "D''AGUA" in comandos[0]
    assert comandos[0].startswith(
        'INSERT INTO "dw"."senado"."legislacao_lista" ("_arquivo"'
    )
    assert "AS array(array(varchar)))) AS t(v)" in comandos[0]


def test_linha_maior_que_o_limite_falha_antes_do_trino() -> None:
    linhas = [{"_arquivo": "a", "valor": "x" * 5_000}]
    with pytest.raises(ValueError, match="acima do limite"):
        sb.inserts(
            "dw", "senado", sb.TABELA_SECOES, ["_arquivo", "valor"], linhas, limite=1_000
        )


class _TrinoFalso:
    def __init__(self, colunas: Sequence[str] = ()) -> None:
        self.comandos: list[str] = []
        self.existentes = list(colunas)

    def run(self, sql: str) -> None:
        self.comandos.append(sql)

    def records(self, sql: str) -> list[Sequence[Any]]:
        self.comandos.append(sql)
        return [(c,) for c in self.existentes]


def test_gravar_cria_coluna_nova_insere_e_registra_por_ultimo() -> None:
    trino = _TrinoFalso(sb.COLUNAS_DETALHE)
    alvos = sb.destinos(trino, "dw", "senado")
    convertidos = [
        sb.converter(_ARQ_DETALHE, sb.TABELA_DETALHE, 1, _detalhe(_DOC_DETALHE)),
        sb.converter(_ARQ_LISTA, sb.TABELA_LISTA, 1, _lista({"documento": _DOC_LISTA})),
    ]
    feito = sb.gravar(alvos, convertidos, "carga", _AGORA)

    assert feito == {
        sb.TABELA_LISTA: 1,
        sb.TABELA_DETALHE: 1,
        sb.TABELA_SECOES: 7,  # 1 publicação, 2 vides, 1 catálogo, 2 indexação, 1 classe
        sb.TABELA_ARQUIVOS: 2,
    }
    assert any('ADD COLUMN IF NOT EXISTS "novidade" varchar' in c for c in trino.comandos)
    inserts = [c for c in trino.comandos if c.startswith("INSERT")]
    assert '"arquivos"' in inserts[-1]


def test_conferir_linha_perdida_e_repetida() -> None:
    registros = [
        (_ARQ_LISTA, sb.TABELA_LISTA, "3", None),
        (_ARQ_DETALHE, sb.TABELA_DETALHE, "1", "8"),
    ]
    certo = {
        sb.TABELA_LISTA: {_ARQ_LISTA: 3},
        sb.TABELA_DETALHE: {_ARQ_DETALHE: 1},
        sb.TABELA_SECOES: {_ARQ_DETALHE: 8},
    }
    assert sb.conferir(registros, certo) == ([], [])

    perdeu = {**certo, sb.TABELA_SECOES: {_ARQ_DETALHE: 7}}
    problemas, _ = sb.conferir(registros, perdeu)
    assert problemas == [f"legislacao_detalhe_secoes: {_ARQ_DETALHE} tem 7 de 8 linhas"]

    repetiu = {**certo, sb.TABELA_LISTA: {_ARQ_LISTA: 6, "solto.json": 2}}
    problemas, avisos = sb.conferir(registros, repetiu)
    assert problemas == [] and len(avisos) == 2
