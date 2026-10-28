"""O gerador do rename da Bronze do SALIC (ADR 0017) concorda com a DAG."""

import datetime as dt
import importlib.util
from pathlib import Path

import pytest

RAIZ = Path(__file__).resolve().parents[1]
_spec = importlib.util.spec_from_file_location(
    "gerar_mover_bronze_salic", RAIZ / "scripts" / "gerar_mover_bronze_salic.py"
)
mod = importlib.util.module_from_spec(_spec)
assert _spec.loader is not None
_spec.loader.exec_module(mod)


def test_destino_usa_o_nome_da_dag():
    from plugins.trino_bronze import bronze_table_name

    assert mod.destino("sac__projetos") == bronze_table_name("sac", "projetos")
    assert mod.destino("sac__projetos") == "bronze_sac_projetos"
    assert mod.destino("SAC__bcoagmar2011$") == "bronze_sac_bcoagmar2011$"


def test_nome_sem_separador_e_erro():
    with pytest.raises(ValueError):
        mod.destino("projetos")


def test_sql_move_renomeia_e_cita_a_quantidade():
    sql = mod.gerar_sql(["sac__projetos", "agentes__agentes"], dt.date(2026, 10, 27))
    assert "-- 2 tabelas." in sql
    assert 'alter table if exists salic_bronze."sac__projetos" set schema salic;' in sql
    assert (
        'alter table if exists salic."sac__projetos" rename to "bronze_sac_projetos";'
        in sql
    )
    assert (
        "select 'agentes__agentes' as origem, 'bronze_agentes_agentes' as destino" in sql
    )
    assert sql.index("begin;") < sql.index("alter table") < sql.index("commit;")


def test_tabelas_dos_sources_ignora_schema_que_nao_e_salic_bronze(tmp_path):
    (tmp_path / "sources_a.yml").write_text(
        "sources:\n  - name: bronze_a\n    schema: salic_bronze\n"
        "    tables:\n      - name: a__x\n      - name: a__y\n"
    )
    (tmp_path / "sources_b.yml").write_text(
        "sources:\n  - name: bronze_b\n    schema: bronze\n"
        "    tables:\n      - name: b__z\n"
    )
    assert mod.tabelas_dos_sources(tmp_path) == ["a__x", "a__y"]


def test_arquivo_colado_do_banco(tmp_path):
    arq = tmp_path / "t.txt"
    arq.write_text(
        "# comentario\nsac__projetos\n\n  tabelas__uf \ntmp_trino_82c2f333_38dfb590\n"
    )
    assert mod.tabelas_do_arquivo(arq) == ["sac__projetos", "tabelas__uf"]
