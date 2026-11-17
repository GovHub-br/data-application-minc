"""Cliente do Tainacan: catálogo, paginação, retry, conferência e linha.

Sem rede: o ``httpx.MockTransport`` faz o papel da API, com os headers
``X-WP-Total`` e ``X-WP-TotalPages`` que ela devolve.
"""

import json
import math
from collections.abc import Callable
from pathlib import Path
from typing import Any

import httpx
import pandas as pd
import pytest
import yaml

import cliente_tainacan as tainacan
import retry_helpers
import seeds_carga

_RAIZ = Path(__file__).resolve().parents[1]
_MODELOS = _RAIZ / "dbt" / "minc" / "models"

BEM = next(c for c in tainacan.COLECOES if c.id == 2292)
MUSEUS = next(c for c in tainacan.COLECOES if c.id == 208)


@pytest.fixture(autouse=True)
def sem_espera(monkeypatch: pytest.MonkeyPatch) -> list[float]:
    esperas: list[float] = []
    monkeypatch.setattr(retry_helpers.time, "sleep", esperas.append)
    return esperas


def _item(item_id: int, colecao_id: int = 2292, **extra: Any) -> dict[str, Any]:
    return {
        "id": item_id,
        "title": f"item {item_id}",
        "collection_id": str(colecao_id),
        "creation_date": "19 de outubro de 2026",
        "metadata": {"descricao": {"name": "Descrição", "value": "False"}},
        **extra,
    }


def _api(
    itens: list[dict[str, Any]], por_pagina: int, total: int | None = None
) -> Callable[[httpx.Request], httpx.Response]:
    """A API de uma coleção com ``itens``, devolvendo ``por_pagina`` por página."""
    declarado = len(itens) if total is None else total
    paginas = max(1, math.ceil(declarado / por_pagina)) if declarado else 0

    def responder(request: httpx.Request) -> httpx.Response:
        pagina = int(request.url.params["paged"])
        trecho = itens[(pagina - 1) * por_pagina : pagina * por_pagina]
        return httpx.Response(
            200,
            json={"items": trecho, "template": "", "filters": []},
            headers={"X-WP-Total": str(declarado), "X-WP-TotalPages": str(paginas)},
        )

    return responder


def _coletar(
    cliente: tainacan.ClienteTainacan, colecao: tainacan.Colecao
) -> tainacan.Coleta:
    coleta = tainacan.Coleta(colecao)
    for pagina in cliente.paginas(colecao.id):
        coleta.registrar(pagina, pagina.itens(), f"raw/pagina_{pagina.numero:04d}.json")
    return coleta


# ── catálogo ─────────────────────────────────────────────────────────────


def test_entidades_e_tabelas_unicas_e_dentro_do_limite_do_postgres() -> None:
    entidades = [c.entidade for c in tainacan.COLECOES]
    assert len(entidades) == len(set(entidades))
    assert len({(c.instancia, c.id) for c in tainacan.COLECOES}) == len(tainacan.COLECOES)
    for colecao in tainacan.COLECOES:
        assert len(f"bronze_{colecao.entidade}") <= seeds_carga.LIMITE_IDENTIFICADOR
        assert len(colecao.tabela) <= seeds_carga.LIMITE_IDENTIFICADOR
        assert colecao.entidade == seeds_carga.slug(colecao.entidade)
        assert colecao.entidade.startswith(f"{colecao.instancia}_")


def test_entidade_fixa_so_onde_o_slug_nao_cabe() -> None:
    for colecao in tainacan.COLECOES:
        pela_regra = f"{colecao.instancia}_{seeds_carga.slug(colecao.slug)}"
        if colecao.entidade_fixa:
            assert len(pela_regra) > tainacan.LIMITE_ENTIDADE, colecao
        else:
            assert colecao.entidade == pela_regra


def test_escopo_da_issue_todas_do_iphan_e_duas_do_ibram() -> None:
    por_instancia = {
        chave: sorted(c.id for c in tainacan.COLECOES if c.instancia == chave)
        for chave in tainacan.INSTANCIAS
    }
    assert por_instancia["cadastro"] == [208, 179824]
    assert [len(por_instancia[k]) for k in ("bcr", "inrc", "indl")] == [7, 13, 20]
    assert {c.fonte for c in tainacan.COLECOES if c.instancia == "cadastro"} == {"ibram"}
    assert {c.fonte for c in tainacan.COLECOES if c.instancia != "cadastro"} == {"iphan"}
    assert tainacan.INSTANCIAS["cadastro"].itens_por_pagina == 96
    assert {tainacan.INSTANCIAS[k].itens_por_pagina for k in ("bcr", "inrc", "indl")} == {
        50
    }


def test_selecao_por_instancia_e_por_id() -> None:
    assert [c.id for c in tainacan.colecoes_selecionadas(["cadastro"])] == [208, 179824]
    assert tainacan.colecoes_selecionadas(["inrc"], [2292]) == [BEM]
    assert tainacan.colecoes_selecionadas() == list(tainacan.COLECOES)
    with pytest.raises(ValueError, match="instância fora do catálogo"):
        tainacan.colecoes_selecionadas(["brasiliana"])
    with pytest.raises(ValueError, match="coleção fora do catálogo"):
        tainacan.colecoes_selecionadas(["cadastro"], [2292])


def _listagem(chave: str, **trocas: str) -> list[dict[str, Any]]:
    return [
        {"id": c.id, "slug": trocas.get(str(c.id), c.slug), "name": c.nome}
        for c in tainacan.COLECOES
        if c.instancia == chave
    ]


def test_catalogo_confere_com_a_listagem() -> None:
    assert tainacan.conferir_catalogo(tainacan.INSTANCIAS["inrc"], _listagem("inrc")) == (
        [],
        [],
    )


def test_colecao_nova_no_iphan_e_problema_e_no_ibram_e_ignorada() -> None:
    nova = {"id": 999, "slug": "nova", "name": "Nova"}
    problemas, _ = tainacan.conferir_catalogo(
        tainacan.INSTANCIAS["indl"], [*_listagem("indl"), nova]
    )
    assert len(problemas) == 1 and "coleção nova 999" in problemas[0]
    assert tainacan.conferir_catalogo(
        tainacan.INSTANCIAS["cadastro"], [*_listagem("cadastro"), nova]
    ) == ([], [])


def test_colecao_que_sumiu_e_problema_e_slug_mudado_e_aviso() -> None:
    listadas = [
        c for c in _listagem("cadastro", **{"208": "museus-br"}) if c["id"] != 179824
    ]
    problemas, avisos = tainacan.conferir_catalogo(
        tainacan.INSTANCIAS["cadastro"], listadas
    )
    assert len(problemas) == 1 and "179824" in problemas[0]
    assert (
        len(avisos) == 1 and "'museus-br'" in avisos[0] and "cadastro_museus" in avisos[0]
    )


# ── HTTP ─────────────────────────────────────────────────────────────────


def test_paginacao_le_todas_as_paginas_com_a_ordem_e_o_tamanho_da_instancia() -> None:
    pedidos: list[httpx.Request] = []
    api = _api([_item(i, 208) for i in range(1, 201)], por_pagina=96)

    def registrar(request: httpx.Request) -> httpx.Response:
        pedidos.append(request)
        return api(request)

    cliente = tainacan.ClienteTainacan(
        tainacan.INSTANCIAS["cadastro"], httpx.MockTransport(registrar)
    )
    coleta = _coletar(cliente, MUSEUS)
    coleta.conferir()
    assert [p["itens"] for p in coleta.paginas] == [96, 96, 8]
    assert {r.url.params["perpage"] for r in pedidos} == {"96"}
    assert {(r.url.params["orderby"], r.url.params["order"]) for r in pedidos} == {
        ("slug", "ASC")
    }
    assert str(pedidos[0].url).startswith(
        "https://cadastro.museus.gov.br/wp-json/tainacan/v2/collection/208/items?"
    )


def test_colecao_vazia_pede_uma_pagina_e_confere_zero() -> None:
    cliente = tainacan.ClienteTainacan(
        tainacan.INSTANCIAS["inrc"], httpx.MockTransport(_api([], por_pagina=50))
    )
    coleta = _coletar(cliente, BEM)
    coleta.conferir()
    assert len(coleta.paginas) == 1 and coleta.total_declarado == 0


def test_retry_em_status_transitorio_com_backoff(sem_espera: list[float]) -> None:
    respostas = iter([503, 429, 200])
    api = _api([_item(1)], por_pagina=50)

    def instavel(request: httpx.Request) -> httpx.Response:
        status = next(respostas)
        return api(request) if status == 200 else httpx.Response(status)

    cliente = tainacan.ClienteTainacan(
        tainacan.INSTANCIAS["inrc"], httpx.MockTransport(instavel)
    )
    assert cliente.pagina(2292, 1).total_itens == 1
    assert sem_espera == [2.0, 4.0]


def test_retry_desiste_na_quinta_tentativa(sem_espera: list[float]) -> None:
    chamadas: list[int] = []

    def fora(request: httpx.Request) -> httpx.Response:
        chamadas.append(1)
        return httpx.Response(502)

    cliente = tainacan.ClienteTainacan(
        tainacan.INSTANCIAS["inrc"], httpx.MockTransport(fora)
    )
    with pytest.raises(tainacan.ErroTransitorio):
        cliente.pagina(2292, 1)
    assert len(chamadas) == tainacan.TENTATIVAS == 5
    assert sem_espera == [2.0, 4.0, 8.0, 16.0]


def test_erro_de_rede_tambem_tenta_de_novo(sem_espera: list[float]) -> None:
    falhas = iter([httpx.ReadTimeout("lento"), None])
    api = _api([_item(1)], por_pagina=50)

    def lenta(request: httpx.Request) -> httpx.Response:
        falha = next(falhas)
        if falha:
            raise falha
        return api(request)

    cliente = tainacan.ClienteTainacan(
        tainacan.INSTANCIAS["inrc"], httpx.MockTransport(lenta)
    )
    assert cliente.pagina(2292, 1).total_itens == 1
    assert sem_espera == [2.0]


def test_status_de_erro_que_nao_e_transitorio_falha_na_primeira(
    sem_espera: list[float],
) -> None:
    cliente = tainacan.ClienteTainacan(
        tainacan.INSTANCIAS["inrc"], httpx.MockTransport(lambda r: httpx.Response(400))
    )
    with pytest.raises(httpx.HTTPStatusError):
        cliente.pagina(2292, 1)
    assert sem_espera == []


def test_header_de_total_ausente_falha() -> None:
    cliente = tainacan.ClienteTainacan(
        tainacan.INSTANCIAS["inrc"],
        httpx.MockTransport(lambda r: httpx.Response(200, json={"items": []})),
    )
    with pytest.raises(ValueError, match="X-WP-Total"):
        cliente.pagina(2292, 1)


def test_listagem_de_colecoes_devolve_o_corpo_como_veio() -> None:
    corpo = json.dumps([{"id": 208, "slug": "museus"}]).encode()
    cliente = tainacan.ClienteTainacan(
        tainacan.INSTANCIAS["cadastro"],
        httpx.MockTransport(
            lambda r: httpx.Response(
                200, content=corpo, headers={"X-WP-Total": "1", "X-WP-TotalPages": "1"}
            )
        ),
    )
    assert cliente.colecoes() == (corpo, [{"id": 208, "slug": "museus"}])


# ── conferência ──────────────────────────────────────────────────────────


def test_item_repetido_entre_paginas_recusa_a_coleta() -> None:
    itens = (
        [_item(i) for i in range(1, 51)]
        + [_item(50)]
        + [_item(i) for i in range(52, 100)]
    )
    cliente = tainacan.ClienteTainacan(
        tainacan.INSTANCIAS["inrc"], httpx.MockTransport(_api(itens, por_pagina=50))
    )
    with pytest.raises(tainacan.PaginacaoInconsistente, match="repetido") as erro:
        _coletar(cliente, BEM).conferir()
    assert "98 ids únicos, 99 declarados" in str(erro.value)


def test_total_declarado_maior_que_os_itens_recusa_a_coleta() -> None:
    cliente = tainacan.ClienteTainacan(
        tainacan.INSTANCIAS["inrc"],
        httpx.MockTransport(
            _api([_item(i) for i in range(1, 50)], por_pagina=50, total=50)
        ),
    )
    with pytest.raises(
        tainacan.PaginacaoInconsistente, match="49 ids únicos, 50 declarados"
    ):
        _coletar(cliente, BEM).conferir()


def test_total_que_muda_durante_a_paginacao_recusa_a_coleta() -> None:
    itens = [_item(i) for i in range(1, 61)]
    primeira = _api(itens, por_pagina=50)
    segunda = _api(itens[:-1], por_pagina=50, total=61)

    def cresceu(request: httpx.Request) -> httpx.Response:
        return (primeira if request.url.params["paged"] == "1" else segunda)(request)

    cliente = tainacan.ClienteTainacan(
        tainacan.INSTANCIAS["inrc"], httpx.MockTransport(cresceu)
    )
    with pytest.raises(
        tainacan.PaginacaoInconsistente, match="mudou durante a paginação"
    ):
        _coletar(cliente, BEM).conferir()


def test_item_de_outra_colecao_recusa_a_coleta() -> None:
    cliente = tainacan.ClienteTainacan(
        tainacan.INSTANCIAS["inrc"],
        httpx.MockTransport(_api([_item(1), _item(2, colecao_id=1327)], por_pagina=50)),
    )
    with pytest.raises(tainacan.PaginacaoInconsistente, match="outro collection_id"):
        _coletar(cliente, BEM).conferir()


def test_resumo_da_coleta_guarda_o_total_e_as_keys() -> None:
    cliente = tainacan.ClienteTainacan(
        tainacan.INSTANCIAS["inrc"],
        httpx.MockTransport(_api([_item(i) for i in range(1, 61)], por_pagina=50)),
    )
    resumo = _coletar(cliente, BEM).resumo()
    assert resumo["x_wp_total"] == resumo["itens"] == 60
    assert [p["key"] for p in resumo["paginas"]] == [
        "raw/pagina_0001.json",
        "raw/pagina_0002.json",
    ]
    assert resumo["parametros"] == {"perpage": 50, "orderby": "slug", "order": "ASC"}


# ── linha ────────────────────────────────────────────────────────────────


def test_linha_guarda_o_item_como_veio_em_texto() -> None:
    metadados = {
        "tematica": {"value": [{"name": "Arquivo", "id": 1}], "multiple": "yes"},
        "vazio": {"value": ""},
        "falso": {"value": "False"},
        "booleano": {"value": False},
    }
    item = _item(
        7,
        description="",
        terms=None,
        exposer_urls=[],
        metadata=metadados,
        **{
            "tainacan-inventarios-inventario-post-id": 12,
            "_thumbnail_id": "",
            "flag": True,
        },
    )
    registro = tainacan.linha(item, pagina=3, posicao=9)
    assert registro["id"] == "7"
    assert registro["description"] is None and registro["_thumbnail_id"] is None
    assert registro["terms"] is None
    assert registro["exposer_urls"] == "[]"
    assert registro["tainacan_inventarios_inventario_post_id"] == "12"
    assert registro["flag"] == "true"
    assert json.loads(registro["metadata"] or "") == metadados
    assert '"value":"False"' in (registro["metadata"] or "")
    assert '"booleano":{"value":false}' in (registro["metadata"] or "")
    assert (registro["_pagina"], registro["_posicao"]) == ("3", "9")


def test_campo_que_colide_com_outro_falha() -> None:
    with pytest.raises(ValueError, match="colide"):
        tainacan.linha({"id": 1, "a-b": 1, "a_b": 2}, 1, 1)
    with pytest.raises(ValueError, match="colide"):
        tainacan.linha({"id": 1, "_pagina": 1}, 1, 1)


def test_colunas_base_entram_vazias_na_colecao_sem_item() -> None:
    df = tainacan.garantir_colunas(
        pd.DataFrame({"dt_ingest": pd.Series([], dtype="string")})
    )
    assert set(tainacan.COLUNAS_BASE) <= set(df.columns) and df.empty
    cheio = pd.DataFrame({"id": ["1"], "collection_id": ["2292"]}, index=[5])
    completo = tainacan.garantir_colunas(cheio)
    assert completo.loc[5, "id"] == "1" and pd.isna(completo.loc[5, "metadata"])


# ── catálogo e dbt andam juntos ──────────────────────────────────────────


def _sources() -> dict[str, dict[str, Any]]:
    dados = yaml.safe_load((_MODELOS / "sources.yml").read_text(encoding="utf-8"))
    return {s["name"]: s for s in dados["sources"]}


@pytest.mark.parametrize("fonte", ["iphan", "ibram"])
def test_toda_colecao_tem_source_bronze_e_schema_yml(fonte: str) -> None:
    colecoes = {c.entidade: c for c in tainacan.COLECOES if c.fonte == fonte}
    source = _sources()[fonte]
    assert source["schema"] == "seeds"
    tabelas = {t["name"]: t for t in source["tables"]}
    assert set(tabelas) == set(colecoes)
    for entidade, tabela in tabelas.items():
        assert tabela["identifier"] == colecoes[entidade].tabela
        assert str(colecoes[entidade].id) in tabela["description"]

    pasta = _MODELOS / fonte
    modelos = {p.stem for p in pasta.glob("*.sql")}
    assert modelos == {f"bronze_{e}" for e in colecoes}
    for entidade in colecoes:
        sql = (pasta / f"bronze_{entidade}.sql").read_text(encoding="utf-8")
        assert f'source("{fonte}", "{entidade}")' in sql

    documentados = yaml.safe_load((pasta / "schema.yml").read_text(encoding="utf-8"))
    assert {m["name"] for m in documentados["models"]} == modelos


# ── carga em seeds, contra um Postgres de verdade ────────────────────────
#
# Pulado sem TEST_POSTGRES_DSN; um banco descartável basta (ver o docstring de
# test_cliente_postgres.py). É o que um mock de psycopg2 não prova: o JSON do
# formulário, com aspas, vírgula e quebra de linha, atravessa o COPY em CSV sem
# mudar, e a segunda carga da mesma coleção substitui a primeira.


@pytest.fixture
def banco() -> Any:
    import os
    import uuid

    import psycopg2

    dsn = os.environ.get("TEST_POSTGRES_DSN")
    if not dsn:
        pytest.skip("TEST_POSTGRES_DSN não definido — sem Postgres para testar")
    schema = f"teste_{uuid.uuid4().hex[:10]}"
    yield dsn, schema
    with psycopg2.connect(dsn) as conn, conn.cursor() as cursor:
        cursor.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
    conn.close()


def _carregar(dsn: str, schema: str, itens: list[dict[str, Any]]) -> int:
    import datalakehouse

    linhas = [tainacan.linha(item, 1, n) for n, item in enumerate(itens, start=1)]
    df = tainacan.garantir_colunas(datalakehouse._registros_para_df(linhas, None, None))
    return seeds_carga.substituir_periodo(
        dsn, df, schema, BEM.tabela, tainacan.COLUNA_COLECAO, str(BEM.id)
    )


def test_recarregar_a_colecao_substitui_e_preserva_o_json(banco: Any) -> None:
    import psycopg2

    dsn, schema = banco
    with psycopg2.connect(dsn) as conn, conn.cursor() as cursor:
        cursor.execute(f'CREATE SCHEMA "{schema}"')
    conn.close()
    dificil: dict[str, Any] = {
        "texto": {"value": 'aspas "duplas", vírgula,\nquebra e "False"'},
        "vazio": {"value": ""},
        "falso": {"value": False},
    }
    itens = [
        _item(i, metadata=dificil if i == 1 else {"a": {"value": str(i)}})
        for i in range(1, 6)
    ]

    assert _carregar(dsn, schema, itens) == 5
    assert _carregar(dsn, schema, itens) == 5
    assert _carregar(dsn, schema, itens[1:]) == 4

    with psycopg2.connect(dsn) as conn, conn.cursor() as cursor:
        cursor.execute(
            f'SELECT count(*), count(DISTINCT id) FROM "{schema}"."{BEM.tabela}"'
        )
        assert cursor.fetchone() == (4, 4)
        cursor.execute(f'SELECT id FROM "{schema}"."{BEM.tabela}" ORDER BY id::int')
        assert [r[0] for r in cursor.fetchall()] == ["2", "3", "4", "5"]
    conn.close()

    assert _carregar(dsn, schema, itens) == 5
    with psycopg2.connect(dsn) as conn, conn.cursor() as cursor:
        cursor.execute(f'SELECT metadata FROM "{schema}"."{BEM.tabela}" WHERE id = \'1\'')
        lida = cursor.fetchone()
        assert lida is not None
        (metadata,) = lida
    conn.close()
    assert json.loads(metadata) == dificil


def test_colecao_vazia_cria_a_tabela_com_as_colunas_base(banco: Any) -> None:
    import psycopg2

    dsn, schema = banco
    with psycopg2.connect(dsn) as conn, conn.cursor() as cursor:
        cursor.execute(f'CREATE SCHEMA "{schema}"')
    conn.close()
    assert _carregar(dsn, schema, []) == 0
    with psycopg2.connect(dsn) as conn, conn.cursor() as cursor:
        cursor.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = %s AND table_name = %s",
            (schema, BEM.tabela),
        )
        colunas = {r[0] for r in cursor.fetchall()}
    conn.close()
    assert set(tainacan.COLUNAS_BASE) | {"dt_ingest"} <= colunas
