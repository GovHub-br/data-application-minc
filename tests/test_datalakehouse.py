"""Testes das partes puras do datalakehouse: caminho, conversão e seleção.

O que importa aqui: raw e staging de um mesmo run sempre se encontram pelo
caminho, e o Parquet de staging tem o mesmo shape que o Postgres tinha
(colunas achatadas com ``__``, tudo texto) -- é o que o dbt vai ler quando a
ponte existir.
"""

import io
import json
from datetime import datetime, timezone

import pandas as pd
import pytest

from datalakehouse import (
    caminho,
    escolher_mais_recente,
    raw_para_staging_key,
    registros_para_parquet,
)
from extracao_por_plano_acao import (
    carregar_planos_acao,
    ids_ja_baixados,
    juntar_anexos_ao_plano,
    key_anexo_arquivo,
    politica_do_programa,
)

_DATA = datetime(2026, 10, 1, 12, 0, tzinfo=timezone.utc)


def _ler(parquet: bytes) -> pd.DataFrame:
    return pd.read_parquet(io.BytesIO(parquet))


# ── caminho ──────────────────────────────────────────────────────────────


def test_caminho_particiona_por_data_e_sanitiza_run_id() -> None:
    key = caminho(
        "raw",
        "transferegov",
        "plano_acao_minc",
        "manual__2026-10-01T12:00:00+00:00",
        _DATA,
    )
    assert key == (
        "raw/transferegov/plano_acao_minc/ano=2026/mes=10/dia=01/"
        "manual__2026-10-01T12_00_00_00_00.json"
    )


def test_staging_e_o_raw_com_outro_prefixo_e_extensao() -> None:
    raw = caminho("raw", "transferegov", "programa_minc", "run", _DATA)
    staging = caminho("staging", "transferegov", "programa_minc", "run", _DATA)
    assert raw_para_staging_key(raw) == staging


def test_camada_desconhecida_e_recusada() -> None:
    with pytest.raises(ValueError):
        caminho("bronze", "transferegov", "x", "run", _DATA)


def test_key_que_nao_e_raw_nao_vira_staging() -> None:
    with pytest.raises(ValueError):
        raw_para_staging_key("staging/transferegov/x/run.parquet")


# ── conversão ────────────────────────────────────────────────────────────


def test_parquet_achata_e_grava_tudo_como_texto() -> None:
    registros = [
        {"id": 1, "ente": {"uf": "DF"}, "tags": ["a", "b"], "ativo": True, "valor": None},
        {"id": 2, "ente": {"uf": "GO"}, "tags": [], "ativo": False, "valor": 1.5},
    ]
    df = _ler(registros_para_parquet(registros, dt_ingest=_DATA))

    assert set(df.columns) == {"id", "ente__uf", "tags", "ativo", "valor", "dt_ingest"}
    assert all(str(t) == "string" for t in df.dtypes)
    assert df["id"].tolist() == ["1", "2"]
    assert json.loads(df["tags"][0]) == ["a", "b"]
    assert df["ativo"].tolist() == ["true", "false"]
    assert pd.isna(df["valor"][0]) and df["valor"][1] == "1.5"
    assert set(df["dt_ingest"]) == {_DATA.isoformat()}


def test_enriquecer_entra_no_staging_sem_mexer_no_registro_de_origem() -> None:
    registros = [{"id": 7}]
    df = _ler(registros_para_parquet(registros, enriquecer=lambda r: {"sigla": "LPG"}))

    assert df["sigla"].tolist() == ["LPG"]
    assert registros == [{"id": 7}]


def test_lista_vazia_gera_parquet_legivel() -> None:
    assert len(_ler(registros_para_parquet([]))) == 0


# ── seleção do mais recente ──────────────────────────────────────────────


def test_mais_recente_e_por_data_de_escrita_nao_pela_key() -> None:
    objetos = [
        {
            "Key": "staging/t/e/ano=2026/mes=10/dia=01/scheduled__x.parquet",
            "LastModified": datetime(2026, 10, 1, 1, tzinfo=timezone.utc),
        },
        {
            "Key": "staging/t/e/ano=2026/mes=10/dia=01/manual__y.parquet",
            "LastModified": datetime(2026, 10, 1, 9, tzinfo=timezone.utc),
        },
        {
            "Key": "staging/t/e/ano=2026/mes=10/dia=01/lixo.txt",
            "LastModified": datetime(2026, 10, 2, tzinfo=timezone.utc),
        },
    ]
    mais_recente = escolher_mais_recente(objetos, "parquet")
    assert mais_recente is not None and mais_recente.endswith("manual__y.parquet")


def test_sem_objetos_nao_ha_mais_recente() -> None:
    assert escolher_mais_recente([], "parquet") is None


# ── leitura do staging nas DAGs encadeadas ───────────────────────────────


def test_planos_do_staging_viram_chaves_sem_na() -> None:
    planos = pd.DataFrame(
        {"id_plano_acao": ["1"], "id_programa": ["46"], "cod_ibge": [pd.NA], "x": ["y"]}
    ).astype("string")
    assert carregar_planos_acao(planos) == [
        {"id_plano_acao": "1", "id_programa": "46", "cod_ibge": None}
    ]


def test_join_de_anexos_descarta_orfaos_e_nao_devolve_na() -> None:
    anexos = pd.DataFrame(
        {
            "id": ["10", "11"],
            "nome": ["a.xlsx", "b.ods"],
            "id_relatorio_gestao": ["1", "9"],
        }
    )
    relatorios = pd.DataFrame({"id_relatorio_gestao": ["1"], "id_plano_acao": ["100"]})
    planos = pd.DataFrame(
        {"id_plano_acao": ["100"], "id_programa": ["46"], "cod_ibge": [pd.NA]}
    )
    juntos = juntar_anexos_ao_plano(anexos, relatorios, planos)

    assert juntos["id"].tolist() == ["10"]
    assert juntos["cod_ibge"].tolist() == [None]


@pytest.mark.parametrize(
    ("id_programa", "politica"),
    [("46", "lpg"), (61, "pnab"), ("999", "outros"), (None, "outros")],
)
def test_politica_do_programa(id_programa: object, politica: str) -> None:
    assert politica_do_programa(id_programa) == politica


def test_anexo_ja_baixado_e_reconhecido_pelo_id() -> None:
    keys = [
        key_anexo_arquivo("lpg", 10, "planilha_anexo_99.xlsx"),
        key_anexo_arquivo("pnab", 11, "outra.ods"),
        "raw/transferegov/anexos_arquivos/lpg/README",
    ]
    assert ids_ja_baixados(keys) == {"10": keys[0], "11": keys[1]}
