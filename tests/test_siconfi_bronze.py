"""Testes da carga da bronze do SICONFI (raw → Trino), sem Trino.

O que importa aqui: a página dividida só entra com todas as partes, e a que
está inteira e dividida ao mesmo tempo fica de fora; o número chega como o
texto do JSON; o item leva só ``_pagina_id`` e ``_item``, e o envelope vai uma
vez para ``paginas``; nenhum ``INSERT`` passa do limite de texto do Trino e a
numeração dos itens continua entre eles; e o controle só cria a coluna que
falta.

A semântica no Trino de verdade está em ``test_siconfi_bronze_trino.py``.
"""

import json
from datetime import datetime, timezone
from typing import Any, Sequence

import pytest

import siconfi_bronze as bronze

_PASTA = "raw/siconfi/msc_orcamentaria/ano=2026/mes=10/dia=27/run"
_DT = datetime(2026, 11, 3, 12, 0, tzinfo=timezone.utc)


# ── páginas no raw ───────────────────────────────────────────────────────


def test_agrupa_pagina_inteira_e_dividida_completa() -> None:
    paginas = bronze.agrupar_paginas(
        [
            f"{_PASTA}/h1_0.json",
            f"{_PASTA}/h2_0_p02de02.json",
            f"{_PASTA}/h2_0_p01de02.json",
        ]
    )
    assert paginas.completas == {
        f"{_PASTA}/h1_0.json": [f"{_PASTA}/h1_0.json"],
        f"{_PASTA}/h2_0.json": [
            f"{_PASTA}/h2_0_p01de02.json",
            f"{_PASTA}/h2_0_p02de02.json",
        ],
    }
    assert not paginas.incompletas and not paginas.ambiguas


def test_parte_faltando_espera_o_proximo_run() -> None:
    paginas = bronze.agrupar_paginas(
        [f"{_PASTA}/h2_0_p01de03.json", f"{_PASTA}/h2_0_p03de03.json"]
    )
    assert paginas.completas == {}
    assert paginas.incompletas == {f"{_PASTA}/h2_0.json"}


@pytest.mark.parametrize(
    "keys",
    [
        ["h3_0.json", "h3_0_p01de02.json", "h3_0_p02de02.json"],
        ["h3_0_p01de02.json", "h3_0_p02de02.json", "h3_0_p01de03.json"],
    ],
)
def test_pagina_ambigua_fica_de_fora(keys: list[str]) -> None:
    paginas = bronze.agrupar_paginas([f"{_PASTA}/{k}" for k in keys])
    assert paginas.completas == {}
    assert paginas.ambiguas == {f"{_PASTA}/h3_0.json"}


# ── itens como texto ─────────────────────────────────────────────────────


def _envelope(itens: list[dict[str, Any]], **extra: Any) -> str:
    return json.dumps(
        {
            "endpoint": "msc_orcamentaria",
            "request_hash": "h2",
            "request_params": {"id_ente": 17, "an_referencia": 2023},
            "page_offset": 0,
            "fetched_at": "2026-10-27T10:00:00+00:00",
            "run_id": "manual__2026-10-27",
            "payload": {"items": itens, "hasMore": False},
            "item_count": len(itens),
            **extra,
        }
    )


def test_numero_chega_como_o_texto_do_json() -> None:
    texto = (
        '{"payload": {"items": [{"valor": 18449.34, "cod_ibge": 17, '
        '"taxa": 1e-05, "funcao": null, "ativo": true}]}}'
    )
    (item,) = bronze.ler_itens(texto)
    assert item["valor"] == "18449.34"
    assert item["cod_ibge"] == "17"
    assert item["taxa"] == "1e-05"
    assert bronze.valor_texto(item["funcao"]) is None
    assert bronze.valor_texto(item["ativo"]) == "true"


def test_juntar_partes_na_ordem_e_o_envelope_vai_para_paginas() -> None:
    lida = bronze.juntar_partes(
        f"{_PASTA}/h2_0.json",
        ["p1", "p2"],
        # Como o siconfi_storage grava as partes: o envelope inteiro em cada
        # uma, com o item_count da página toda.
        [
            _envelope([{"valor": 1}], response_headers={"etag": "x"}, item_count=3),
            _envelope([{"valor": 2}, {"valor": 3}], item_count=3),
        ],
    )
    assert [i["valor"] for i in lida.itens] == ["1", "2", "3"]
    registro = bronze.registro_da_pagina(lida, "run-bronze", _DT)
    assert list(registro) == list(bronze.COLUNAS_PAGINA)
    assert registro["pagina_id"] == bronze.pagina_id(f"{_PASTA}/h2_0.json")
    assert registro["request_params"] == '{"id_ente":17,"an_referencia":2023}'
    assert registro["page_offset"] == "0"
    assert registro["arquivos"] == "2"
    assert registro["itens"] == registro["item_count"] == "3"
    assert registro["response_headers"] == '{"etag":"x"}'
    assert registro["payload_sem_itens"] == '{"hasMore":false}'
    assert registro["migrado_de"] is None
    assert registro["carregado_em"] == "2026-11-03T12:00:00+00:00"


def test_pagina_id_curto_e_estavel() -> None:
    a = bronze.pagina_id(f"{_PASTA}/h2_0.json")
    assert len(a) == 16 and int(a, 16) >= 0
    assert a == bronze.pagina_id(f"{_PASTA}/h2_0.json")
    assert a != bronze.pagina_id(f"{_PASTA}/h2_5000.json")


def test_envelope_de_outro_endpoint_falha() -> None:
    lida = bronze.juntar_partes(f"{_PASTA}/h2_0.json", ["p"], [_envelope([])])
    bronze.conferir_endpoint(lida, "msc_orcamentaria")
    with pytest.raises(ValueError, match="pasta de 'rreo'"):
        bronze.conferir_endpoint(lida, "rreo")


def _partes(*envelopes: str) -> bronze.PaginaLida:
    arquivos = [
        f"{_PASTA}/h2_0_p{i:02d}de{len(envelopes):02d}.json"
        for i in range(1, len(envelopes) + 1)
    ]
    return bronze.juntar_partes(f"{_PASTA}/h2_0.json", arquivos, list(envelopes))


def test_conferir_pagina_aceita_partes_da_mesma_tentativa() -> None:
    lida = _partes(
        _envelope([{"v": 1}, {"v": 2}], item_count=3),
        _envelope([{"v": 3}], item_count=3),
    )
    assert bronze.conferir_pagina(lida) == []
    assert [p["itens"] for p in lida.partes] == ["2", "1"]


def test_conferir_pagina_recusa_partes_de_tentativas_diferentes() -> None:
    lida = _partes(
        _envelope([{"v": 1}], item_count=2),
        _envelope(
            [{"v": 2}],
            item_count=2,
            fetched_at="2026-10-28T09:00:00+00:00",
            run_id="manual__2026-10-28",
        ),
    )
    problemas = bronze.conferir_pagina(lida)
    assert any("fetched_at" in p for p in problemas)
    assert any("run_id" in p for p in problemas)


def test_conferir_pagina_recusa_contagem_que_nao_fecha() -> None:
    # Duas tentativas com a mesma data, mas a segunda parte é de uma página menor.
    lida = _partes(
        _envelope([{"v": 1}], item_count=3), _envelope([{"v": 2}], item_count=3)
    )
    assert bronze.conferir_pagina(lida) == ["2 itens nas partes, e o item_count diz 3"]
    sem_contagem = json.loads(_envelope([{"v": 1}]))
    del sem_contagem["item_count"]
    lida = bronze.juntar_partes(f"{_PASTA}/h2_0.json", ["a"], [json.dumps(sem_contagem)])
    assert bronze.conferir_pagina(lida) == ["parte sem item_count"]


def test_juntar_partes_exige_um_texto_por_arquivo() -> None:
    with pytest.raises(ValueError, match="2 arquivos e 1 textos"):
        bronze.juntar_partes(f"{_PASTA}/h2_0.json", ["a", "b"], [_envelope([])])


def test_campos_na_ordem_e_nome_fora_do_padrao_falha() -> None:
    assert bronze.campos([{"b": 1, "a": 2}, {"a": 3, "c": 4}]) == ["b", "a", "c"]
    with pytest.raises(ValueError, match="fora do padrão"):
        bronze.campos([{"Valor Total": 1}])


# ── SQL ──────────────────────────────────────────────────────────────────


def _lida(n: int, valor: str = "x") -> bronze.PaginaLida:
    itens = [{"conta": f"{i:09d}", "valor": valor} for i in range(n)]
    return bronze.PaginaLida(
        f"{_PASTA}/h2_0.json", ["a"], json.loads(_envelope([])), itens
    )


def test_insert_cabe_no_limite_e_numera_os_itens_em_sequencia() -> None:
    destino = bronze.tabela("hml_analytics", "siconfi", "msc_orcamentaria")
    comandos = bronze.inserts_da_pagina(
        destino, _lida(300), ["conta", "valor"], limite=2_000
    )
    assert len(comandos) > 1
    assert all(len(c) <= 2_000 for c in comandos)
    id_pagina = bronze.pagina_id(f"{_PASTA}/h2_0.json")
    assert all(
        c.startswith(f'INSERT INTO {destino} ("_pagina_id", "_item", "conta", "valor")')
        and f"SELECT '{id_pagina}', CAST(n + " in c
        for c in comandos
    )
    primeiros = [int(c.split("CAST(n + ")[1].split(" ")[0]) + 1 for c in comandos]
    tamanhos = [c.count('","x"]') for c in comandos]
    assert primeiros[0] == 1
    for anterior, seguinte, n in zip(primeiros, primeiros[1:], tamanhos):
        assert seguinte == anterior + n
    assert sum(tamanhos) == 300


def test_apostrofo_no_valor_e_escapado() -> None:
    (comando,) = bronze.inserts_da_pagina(
        '"c"."s"."t"', _lida(1, "d'água"), ["conta", "valor"]
    )
    assert "d''água" in comando
    assert "array(array(varchar))" in comando


def test_pagina_sem_itens_nao_gera_insert() -> None:
    assert bronze.inserts_da_pagina('"c"."s"."t"', _lida(0), []) == []


def test_item_maior_que_o_limite_falha() -> None:
    with pytest.raises(ValueError):
        bronze.inserts_da_pagina(
            '"c"."s"."t"',
            _lida(1, "x" * 5_000),
            ["conta", "valor"],
            limite=3_000,
        )


def test_ddl_e_tudo_varchar_e_o_item_so_aponta_para_a_pagina() -> None:
    ddl = bronze.ddl_tabela("hml_analytics", "siconfi", "rreo")
    assert ddl == (
        'CREATE TABLE IF NOT EXISTS "hml_analytics"."siconfi"."rreo" '
        '("_pagina_id" varchar, "_item" varchar)'
    )
    ddl = bronze.ddl_paginas("hml_analytics", "siconfi")
    assert ddl.startswith(
        'CREATE TABLE IF NOT EXISTS "hml_analytics"."siconfi"."paginas" '
    )
    assert ddl.count(" varchar") == len(bronze.COLUNAS_PAGINA)


def test_registro_das_paginas_em_lote_abaixo_do_limite() -> None:
    lidas = [
        bronze.juntar_partes(f"{_PASTA}/h{i}_0.json", ["a"], [_envelope([{"v": 1}])])
        for i in range(40)
    ]
    registros = [bronze.registro_da_pagina(lida, "r", _DT) for lida in lidas]
    comandos = bronze.inserts_paginas("hml_analytics", "siconfi", registros, limite=4_000)
    assert len(comandos) > 1
    assert all(len(c) <= 4_000 for c in comandos)
    assert sum(c.count(f"'{_PASTA}/h") for c in comandos) == 40
    (um,) = bronze.inserts_paginas("hml_analytics", "siconfi", registros[:1])
    assert um.startswith(
        'INSERT INTO "hml_analytics"."siconfi"."paginas" ("pagina_id", "pagina",'
    )
    # migrado_de nulo vira NULL tipado, não o texto 'None'.
    assert "CAST(NULL AS varchar)" in um and "'None'" not in um
    assert bronze.inserts_paginas("hml_analytics", "siconfi", []) == []


# ── Destino, com Trino falso ─────────────────────────────────────────────


class _TrinoFalso:
    def __init__(self, colunas: list[str]) -> None:
        self.executados: list[str] = []
        self.colunas = colunas

    def run(self, sql: str) -> None:
        self.executados.append(sql)

    def records(self, sql: str) -> list[Sequence[Any]]:
        assert "information_schema.columns" in sql
        return [(c,) for c in self.colunas]


def test_so_cria_a_coluna_que_falta_e_uma_vez() -> None:
    trino = _TrinoFalso([*bronze.COLUNAS_ITEM, "conta"])
    destino = bronze.Destino(trino, "hml_analytics", "siconfi", "msc_orcamentaria")
    assert destino.acrescentar(["conta", "valor"]) == ["valor"]
    assert destino.acrescentar(["conta", "valor"]) == []
    (alter,) = trino.executados
    assert alter == (
        'ALTER TABLE "hml_analytics"."siconfi"."msc_orcamentaria" '
        'ADD COLUMN IF NOT EXISTS "valor" varchar'
    )


def test_carregar_pagina_insere_e_devolve_os_itens() -> None:
    trino = _TrinoFalso(list(bronze.COLUNAS_ITEM))
    destino = bronze.Destino(trino, "hml_analytics", "siconfi", "msc_orcamentaria")
    assert bronze.carregar_pagina(destino, _lida(3)) == 3
    assert [s.split()[0] for s in trino.executados] == ["ALTER", "ALTER", "INSERT"]


# ── conexão de cada thread ───────────────────────────────────────────────


class _CursorDbapi:
    def __init__(self, log: list[str]) -> None:
        self.log = log

    def execute(self, sql: str) -> None:
        self.log.append(f"execute {sql}")

    def fetchall(self) -> list[tuple[str]]:
        self.log.append("fetchall")
        return [("x",)]


class _ConexaoDbapi:
    def __init__(self) -> None:
        self.log: list[str] = []

    def cursor(self) -> _CursorDbapi:
        return _CursorDbapi(self.log)


def test_conexao_trino_le_o_resultado_de_todo_comando() -> None:
    conexao = _ConexaoDbapi()
    trino = bronze.ConexaoTrino(conexao)
    trino.run("INSERT 1")
    assert trino.records("SELECT 1") == [("x",)]
    # Sem ler o resultado, o Trino não termina o INSERT.
    assert conexao.log == ["execute INSERT 1", "fetchall", "execute SELECT 1", "fetchall"]


def test_destino_usa_o_executor_da_thread() -> None:
    principal = _TrinoFalso([*bronze.COLUNAS_ITEM, "conta"])
    destino = bronze.Destino(principal, "hml_analytics", "siconfi", "msc_orcamentaria")
    destino.colunas()  # na thread principal
    da_thread = _TrinoFalso([])
    assert bronze.carregar_pagina(destino, _lida(2), da_thread) == 2
    assert [s.split()[0] for s in da_thread.executados] == ["ALTER", "INSERT"]
    assert principal.executados == []
