"""Bronze SICONFI: uma tabela de páginas por endpoint, itens só onde ainda faltam no dbt.

A MSC orçamentária chegou a 45 GB em linhas JSONB de ~830 bytes. As três MSC e a DCA
guardam só a página crua e são estruturadas no dbt; os demais endpoints seguem com a
tabela de itens.
"""

from unittest.mock import MagicMock, patch

import pytest
from psycopg2 import sql

import siconfi_storage
from cliente_siconfi import FACT_ENDPOINTS, SiconfiPage
from siconfi_storage import PAGES_ONLY_ENDPOINTS, VALID_ENDPOINTS, SiconfiStorage


def _page(endpoint: str, items: list[dict]) -> SiconfiPage:
    return SiconfiPage(
        endpoint=endpoint,
        params={"id_ente": 3550308},
        offset=0,
        items=items,
        has_more=False,
        headers={},
        payload={"items": items},
    )


def _storage_com_cursor_falso() -> tuple[SiconfiStorage, MagicMock]:
    cur = MagicMock()
    cur.fetchone.return_value = (7,)
    conn = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cur
    patcher = patch.object(siconfi_storage.psycopg2, "connect", return_value=conn)
    patcher.start()
    return SiconfiStorage("postgresql://falso"), cur


@pytest.mark.parametrize(
    ("endpoint", "tabela"),
    [
        ("msc_orcamentaria", "raw_pages_msc_orcamentaria"),
        ("anexos-relatorios", "raw_pages_anexos_relatorios"),
        ("extrato_entregas", "raw_pages_extrato_entregas"),
    ],
)
def test_cada_endpoint_tem_a_sua_tabela_de_paginas(endpoint: str, tabela: str) -> None:
    assert SiconfiStorage._pages_table(endpoint) == tabela


def test_nomes_de_tabela_de_paginas_nao_se_repetem() -> None:
    nomes = {SiconfiStorage._pages_table(e) for e in VALID_ENDPOINTS}
    assert len(nomes) == len(VALID_ENDPOINTS)


def test_endpoint_invalido_e_recusado() -> None:
    with pytest.raises(ValueError, match="inválido"):
        SiconfiStorage._pages_table("nao_existe")


_SO_PAGINAS = ["dca", "msc_patrimonial", "msc_orcamentaria", "msc_controle"]


@pytest.mark.parametrize("endpoint", _SO_PAGINAS)
def test_endpoint_so_de_paginas_nao_tem_tabela_de_itens(endpoint: str) -> None:
    assert endpoint in PAGES_ONLY_ENDPOINTS
    with pytest.raises(ValueError, match="não tem tabela de itens"):
        SiconfiStorage._table(endpoint)


def test_so_as_msc_e_a_dca_perderam_os_itens() -> None:
    assert PAGES_ONLY_ENDPOINTS == set(_SO_PAGINAS)
    com_itens = {e for e in VALID_ENDPOINTS if e not in PAGES_ONLY_ENDPOINTS}
    assert {"rreo", "rgf"} <= com_itens
    # O planejamento da DAG lê estas duas direto do banco.
    assert {"entes", "extrato_entregas"} <= com_itens
    assert all(e in FACT_ENDPOINTS for e in PAGES_ONLY_ENDPOINTS)


@pytest.mark.parametrize("endpoint", _SO_PAGINAS)
def test_grava_a_pagina_e_nao_expande_itens(endpoint: str) -> None:
    storage, cur = _storage_com_cursor_falso()
    with patch.object(siconfi_storage, "execute_values") as execute_values:
        gravados = storage.persist_page(
            _page(endpoint, [{"valor": 1.5}, {"valor": 2}]), "run-1"
        )
    assert gravados == 2
    assert f"raw_pages_{endpoint}" in repr(cur.execute.call_args.args[0])
    execute_values.assert_not_called()


def test_outros_endpoints_continuam_expandindo_itens() -> None:
    storage, cur = _storage_com_cursor_falso()
    with (
        patch.object(siconfi_storage, "execute_values") as execute_values,
        patch.object(sql.Composed, "as_string", return_value="INSERT"),
    ):
        gravados = storage.persist_page(_page("rreo", [{"valor": 1}]), "run-1")
    assert gravados == 1
    assert "raw_pages_rreo" in repr(cur.execute.call_args.args[0])
    execute_values.assert_called_once()
