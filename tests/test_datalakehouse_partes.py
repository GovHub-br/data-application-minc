"""Testes da divisão de arquivos acima do limite do proxy do MinIO.

O proxy na frente do MinIO do servidor recusa corpo acima de 1 MiB, e o
TransfereGov grava JSON de dezenas de MB (planos de ação, relatórios) e
planilhas maiores que isso. A regra é a do SICONFI (ADR 0014):

- o que passa do limite vira partes ``<nome>_g<geração>_pNNdeMM.<ext>``, cada uma
  abaixo dele; a geração vem do conteúdo;
- ler o arquivo dividido devolve exatamente o que o inteiro devolveria;
- um arquivo existe inteiro **ou** dividido, nunca as duas coisas, e nunca
  dividido de dois jeitos -- a gravação apaga a outra forma depois de gravar tudo;
- arquivo com parte faltando ou ambíguo não é lido nem conta como baixado.
"""

import io
import json
import logging
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any, Iterator
from unittest.mock import patch

import pandas as pd
import pytest

import datalakehouse
from datalakehouse import (
    PartesInconsistentes,
    agrupar_partes,
    dividir_bytes,
    key_da_parte,
    limite_da_variable,
    neutralizar_sufixo_de_parte,
    parte_da_key,
    partes_do_json,
    partes_do_parquet,
    registros_para_parquet,
)
from extracao_por_plano_acao import AnexoBaixado, ids_ja_baixados, key_anexo_arquivo

_DATA = datetime(2026, 10, 1, 12, 0, tzinfo=timezone.utc)
_RUN = "manual__2026-10-01T12:00:00+00:00"
_G = "0123abcd"  # uma geração qualquer, para os testes de nome


def _registros(n: int) -> list[dict[str, Any]]:
    """Registros parecidos com os da API: texto acentuado, número, aninhado e
    campo que só aparece em parte deles (a coluna precisa existir em toda parte)."""
    return [
        {
            "id_plano_acao": i,
            "diagnostico_plano_acao": f"diagnóstico do município {i} " * 5,
            "ente": {"uf": "DF", "cod_ibge": 5300108 + i},
            **({"emenda": f"E{i}"} if i % 7 == 0 else {}),
        }
        for i in range(n)
    ]


def _ler_parquet(corpo: bytes) -> pd.DataFrame:
    return pd.read_parquet(io.BytesIO(corpo))


# ── nomes ────────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    ("key", "esperada"),
    [
        (
            "raw/transferegov/plano_acao_minc/ano=2026/mes=10/dia=01/run.json",
            "raw/transferegov/plano_acao_minc/ano=2026/mes=10/dia=01/"
            "run_g0123abcd_p02de03.json",
        ),
        ("staging/t/e/run.x.parquet", "staging/t/e/run.x_g0123abcd_p02de03.parquet"),
        (
            "raw/transferegov/anexos_arquivos/lpg/anexo_10_Planilha Final.xlsx",
            "raw/transferegov/anexos_arquivos/lpg/"
            "anexo_10_Planilha Final_g0123abcd_p02de03.xlsx",
        ),
        ("raw/t/sem_extensao", "raw/t/sem_extensao_g0123abcd_p02de03"),
    ],
)
def test_parte_mantem_pasta_e_extensao_e_volta_para_a_key(
    key: str, esperada: str
) -> None:
    assert key_da_parte(key, 2, 3, _G) == esperada
    assert parte_da_key(esperada) == (key, _G, 2, 3)


def test_key_inteira_nao_e_parte() -> None:
    assert parte_da_key("raw/t/e/run.json") is None


def test_sufixo_de_parte_sem_geracao_nao_e_parte() -> None:
    """``_pNdeM`` sem a geração é o que o SICONFI e qualquer nome de usuário
    podem ter; não é parte de um arquivo gravado por este módulo."""
    assert parte_da_key("raw/t/cronograma_p1de2.xlsx") is None


def test_geracao_invalida_e_recusada() -> None:
    with pytest.raises(ValueError, match="geração inválida"):
        key_da_parte("raw/t/run.json", 1, 2, "XYZ")


@pytest.mark.parametrize(
    ("nome", "esperado"),
    [
        ("cronograma_p1de2.xlsx", "cronograma-p1de2.xlsx"),
        ("relatorio_g0123abcd_p01de02.xlsx", "relatorio_g0123abcd-p01de02.xlsx"),
        ("sem_extensao_p3de3", "sem_extensao-p3de3"),
        ("planilha_p1de2_final.xlsx", "planilha_p1de2_final.xlsx"),  # não está no fim
        ("Planilha Final.xlsx", "Planilha Final.xlsx"),
    ],
)
def test_nome_vindo_de_fora_perde_so_o_sufixo_de_parte(nome: str, esperado: str) -> None:
    assert neutralizar_sufixo_de_parte(nome) == esperado


# ── conteúdo das partes ──────────────────────────────────────────────────


def test_json_que_cabe_fica_num_arquivo_so() -> None:
    registros = _registros(3)
    assert partes_do_json(registros, 10_000) == [
        json.dumps(registros, ensure_ascii=False, default=str).encode()
    ]


def test_json_grande_vira_listas_validas_abaixo_do_limite() -> None:
    registros = _registros(300)
    partes = partes_do_json(registros, 8_000)
    assert len(partes) > 1
    assert all(len(parte) <= 8_000 for parte in partes)
    assert [r for parte in partes for r in json.loads(parte)] == json.loads(
        json.dumps(registros, ensure_ascii=False, default=str)
    )


def test_registro_maior_que_o_limite_falha_antes_do_minio() -> None:
    with pytest.raises(ValueError, match="um item sozinho"):
        partes_do_json([{"texto": "x" * 500}], 100)


def test_limite_desligado_grava_inteiro() -> None:
    assert len(partes_do_json(_registros(300), None)) == 1
    assert dividir_bytes(b"x" * 10, None) == [b"x" * 10]


def test_parquet_grande_vira_partes_com_as_mesmas_linhas_e_colunas() -> None:
    registros = _registros(2_000)
    inteiro = _ler_parquet(registros_para_parquet(registros, dt_ingest=_DATA))
    partes = partes_do_parquet(registros, 20_000, dt_ingest=_DATA)
    assert len(partes) > 1
    assert all(len(parte) <= 20_000 for parte in partes)
    junto = pd.concat([_ler_parquet(p) for p in partes], ignore_index=True)
    # A coluna "emenda" só existe em 1 a cada 7 registros: sai em toda parte.
    pd.testing.assert_frame_equal(junto, inteiro)


def test_parquet_que_cabe_e_o_mesmo_de_antes() -> None:
    registros = _registros(5)
    assert partes_do_parquet(registros, 10**9, dt_ingest=_DATA) == [
        registros_para_parquet(registros, dt_ingest=_DATA)
    ]


def test_bytes_divididos_voltam_iguais() -> None:
    conteudo = bytes(range(256)) * 40
    partes = dividir_bytes(conteudo, 1_000)
    assert all(len(parte) <= 1_000 for parte in partes)
    assert b"".join(partes) == conteudo


# ── limite configurável ──────────────────────────────────────────────────


@pytest.mark.parametrize(
    ("valor", "esperado"),
    [(900, 900 * 1024), ("900", 900 * 1024), (" 2048 ", 2048 * 1024), (0, None)],
)
def test_limite_da_variable(valor: object, esperado: int | None) -> None:
    assert limite_da_variable(valor) == esperado


@pytest.mark.parametrize("valor", [-1, "-900", "1.5", "novecentos", "", None])
def test_limite_negativo_ou_invalido_falha_em_vez_de_desligar(valor: object) -> None:
    with pytest.raises(ValueError, match="0 para desligar"):
        limite_da_variable(valor)


# ── conferência: inteiro ou partes completas, nunca as duas coisas ───────


def _p(arquivo: str, indice: int, total: int, geracao: str = _G) -> str:
    return key_da_parte(arquivo, indice, total, geracao)


def test_agrupa_inteiros_e_partes_completas_em_ordem() -> None:
    keys = ["raw/a.json", _p("raw/b.json", 2, 2), _p("raw/b.json", 1, 2)]
    completos, problemas = agrupar_partes(keys)
    assert completos == {
        "raw/a.json": ["raw/a.json"],
        "raw/b.json": [_p("raw/b.json", 1, 2), _p("raw/b.json", 2, 2)],
    }
    assert problemas == set()


@pytest.mark.parametrize(
    "keys",
    [
        # parte faltando
        [_p("raw/b.json", 1, 3), _p("raw/b.json", 3, 3)],
        # inteiro e dividido
        ["raw/b.json", _p("raw/b.json", 1, 2), _p("raw/b.json", 2, 2)],
        # dois totais
        [_p("raw/b.json", 1, 2), _p("raw/b.json", 2, 2), _p("raw/b.json", 1, 1)],
        # duas gerações com o mesmo total: uma regravação que caiu no meio
        [_p("raw/b.json", 1, 2), _p("raw/b.json", 2, 2, "ffffffff")],
        # total gigante no nome: recusado sem montar o intervalo declarado
        [_p("raw/b.json", 1, 1_000_000_000)],
    ],
)
def test_arquivo_incompleto_ou_ambiguo_nao_e_completo(keys: list[str]) -> None:
    completos, problemas = agrupar_partes(keys)
    assert completos == {}
    assert problemas == {"raw/b.json"}


# ── gravação e leitura no bucket ─────────────────────────────────────────


class _MinioFalso:
    """Bucket em memória com a parte do ``S3Hook`` e do boto3 que o módulo usa."""

    def __init__(self) -> None:
        self.objetos: dict[str, bytes] = {}
        self.cair_depois_de: int | None = None

    def load_bytes(
        self, bytes_data: bytes, key: str, bucket_name: str, replace: bool
    ) -> None:
        if self.cair_depois_de is not None:
            if self.cair_depois_de == 0:
                raise ConnectionError("conexão caiu no meio da gravação")
            self.cair_depois_de -= 1
        self.objetos[key] = bytes(bytes_data)

    def get_key(self, key: str, bucket_name: str) -> SimpleNamespace:
        corpo = self.objetos[key]
        return SimpleNamespace(
            content_length=len(corpo), get=lambda: {"Body": io.BytesIO(corpo)}
        )

    def get_conn(self) -> "_MinioFalso":
        return self

    def get_paginator(self, nome: str) -> "_MinioFalso":
        return self

    def paginate(self, Bucket: str, Prefix: str) -> Iterator[dict[str, Any]]:
        yield {
            "Contents": [{"Key": k} for k in sorted(self.objetos) if k.startswith(Prefix)]
        }

    def delete_object(self, Bucket: str, Key: str) -> None:
        self.objetos.pop(Key, None)


@pytest.fixture
def minio() -> Iterator[_MinioFalso]:
    falso = _MinioFalso()
    with (
        patch.object(datalakehouse, "get_hook", return_value=falso),
        patch.object(datalakehouse, "run_atual", return_value=(_RUN, _DATA)),
        patch.object(datalakehouse, "limite_objeto_bytes", return_value=8_000),
    ):
        yield falso


def test_raw_grande_e_gravado_em_partes_e_lido_inteiro(minio: _MinioFalso) -> None:
    registros = _registros(300)
    key = datalakehouse.gravar_raw(registros, "transferegov", "plano_acao_minc")
    assert key not in minio.objetos
    assert all(parte_da_key(k)[0] == key for k in minio.objetos)  # type: ignore[index]
    assert all(len(corpo) <= 8_000 for corpo in minio.objetos.values())
    assert datalakehouse.ler_raw(key) == json.loads(
        json.dumps(registros, ensure_ascii=False, default=str)
    )


def test_staging_dividido_e_lido_como_um_parquet_so(minio: _MinioFalso) -> None:
    registros = _registros(2_000)
    key_raw = datalakehouse.gravar_raw(registros, "transferegov", "plano_acao_minc")
    key_staging = datalakehouse.raw_para_staging(key_raw)
    partes = [k for k in minio.objetos if k.startswith("staging/")]
    assert len(partes) > 1
    assert all(len(minio.objetos[k]) <= 8_000 for k in partes)
    df = datalakehouse.ler_parquet(key_staging)
    assert len(df) == 2_000
    assert df["id_plano_acao"].tolist() == [str(i) for i in range(2_000)]


def test_planilha_grande_e_gravada_em_partes_e_lida_igual(minio: _MinioFalso) -> None:
    conteudo = bytes(range(256)) * 100
    key = key_anexo_arquivo("lpg", 10, "planilha.xlsx")
    assert datalakehouse.gravar_bytes_em_partes(conteudo, key) == key
    assert len(minio.objetos) == 4
    assert datalakehouse.ler_bytes(key) == conteudo


def test_regravar_inteiro_apaga_as_partes_antigas(minio: _MinioFalso) -> None:
    key = key_anexo_arquivo("lpg", 10, "planilha.xlsx")
    datalakehouse.gravar_bytes_em_partes(b"x" * 20_000, key)
    datalakehouse.gravar_bytes_em_partes(b"y" * 10, key)
    assert list(minio.objetos) == [key]
    assert datalakehouse.ler_bytes(key) == b"y" * 10


def test_regravar_dividido_apaga_o_inteiro_e_a_divisao_antiga(minio: _MinioFalso) -> None:
    key = key_anexo_arquivo("lpg", 10, "planilha.xlsx")
    vizinho = key_anexo_arquivo("lpg", 10, "planilha.xlsx.bak")
    minio.objetos[key] = b"inteiro antigo"
    minio.objetos[vizinho] = b"outro arquivo"
    datalakehouse.gravar_bytes_em_partes(b"z" * 30_000, key)  # 4 partes
    datalakehouse.gravar_bytes_em_partes(b"w" * 20_000, key)  # 3 partes
    partes = sorted(k for k in minio.objetos if k != vizinho)
    assert [parte_da_key(k)[2:] for k in partes] == [  # type: ignore[index]
        (1, 3),
        (2, 3),
        (3, 3),
    ]
    assert minio.objetos[vizinho] == b"outro arquivo"
    assert datalakehouse.ler_bytes(key) == b"w" * 20_000


def test_regravacao_que_cai_no_meio_nao_vira_mistura(minio: _MinioFalso) -> None:
    """Mesmo número de partes e outro conteúdo: sem a geração no nome, a parte 1
    nova sobrescreveria a antiga e a leitura aceitaria nova + antigas como completo."""
    key = key_anexo_arquivo("lpg", 10, "planilha.xlsx")
    datalakehouse.gravar_bytes_em_partes(b"a" * 20_000, key)  # 3 partes
    minio.cair_depois_de = 1
    with pytest.raises(ConnectionError):
        datalakehouse.gravar_bytes_em_partes(b"b" * 20_000, key)  # 3 partes
    with pytest.raises(PartesInconsistentes):
        datalakehouse.ler_bytes(key)
    minio.cair_depois_de = None
    datalakehouse.gravar_bytes_em_partes(b"b" * 20_000, key)
    assert datalakehouse.ler_bytes(key) == b"b" * 20_000
    assert len(minio.objetos) == 3


def test_regravar_o_mesmo_conteudo_reusa_os_mesmos_nomes(minio: _MinioFalso) -> None:
    key = key_anexo_arquivo("lpg", 10, "planilha.xlsx")
    datalakehouse.gravar_bytes_em_partes(b"c" * 20_000, key)
    antes = sorted(minio.objetos)
    datalakehouse.gravar_bytes_em_partes(b"c" * 20_000, key)
    assert sorted(minio.objetos) == antes


def test_arquivo_com_parte_faltando_nao_e_lido(minio: _MinioFalso) -> None:
    key = key_anexo_arquivo("lpg", 10, "planilha.xlsx")
    datalakehouse.gravar_bytes_em_partes(b"z" * 30_000, key)
    del minio.objetos[sorted(minio.objetos)[1]]
    with pytest.raises(PartesInconsistentes, match="faltando"):
        datalakehouse.ler_bytes(key)


def test_arquivo_inexistente_diz_qual(minio: _MinioFalso) -> None:
    with pytest.raises(FileNotFoundError, match="raw/t/nada.json"):
        datalakehouse.ler_raw("raw/t/nada.json")


# ── anexos já baixados ───────────────────────────────────────────────────


def test_anexo_dividido_so_conta_como_baixado_com_todas_as_partes() -> None:
    inteiro = key_anexo_arquivo("pnab", 11, "outra.xlsx")
    dividido = key_anexo_arquivo("lpg", 10, "planilha.xlsx")
    incompleto = key_anexo_arquivo("lpg", 12, "faltando.xlsx")
    keys = [inteiro, _p(dividido, 2, 2), _p(dividido, 1, 2), _p(incompleto, 1, 2)]
    assert ids_ja_baixados(keys) == {
        "11": AnexoBaixado(key=inteiro, partes=[inteiro]),
        "10": AnexoBaixado(key=dividido, partes=[_p(dividido, 1, 2), _p(dividido, 2, 2)]),
    }


def test_anexo_com_nome_de_parte_conta_como_baixado() -> None:
    """O nome vem do usuário: ``cronograma_p1de2.xlsx`` não pode virar parte 1 de 2
    de outro arquivo, senão nunca conta como baixado e nunca é extraído."""
    key = key_anexo_arquivo("lpg", 42, "cronograma_p1de2.xlsx")
    assert key.endswith("/anexo_42_cronograma-p1de2.xlsx")
    assert ids_ja_baixados([key]) == {"42": AnexoBaixado(key=key, partes=[key])}
    dividido = [_p(key, 1, 2), _p(key, 2, 2)]
    assert ids_ja_baixados(dividido) == {"42": AnexoBaixado(key=key, partes=dividido)}


def test_aviso_de_anexo_incompleto_nao_mostra_o_nome_do_arquivo(
    caplog: pytest.LogCaptureFixture,
) -> None:
    incompleto = key_anexo_arquivo("lpg", 12, "Fulano de Tal - perfil.xlsx")
    with caplog.at_level(logging.WARNING):
        assert ids_ja_baixados([_p(incompleto, 1, 2)]) == {}
    assert "12" in caplog.text
    assert "Fulano" not in caplog.text
