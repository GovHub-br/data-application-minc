"""Testes da montagem do SQL da ingestão SALIC via Trino.

O que importa aqui é uma coisa só: **as fatias cobrem a tabela inteira, sem
buraco e sem sobreposição**. Uma fatia que deixa buraco não quebra nada — só
carrega a tabela pela metade, e o erro aparece semanas depois como um número
errado num painel.
"""

from trino_bronze import (
    SLICE_COLUMN,
    bronze_ddl,
    build_statements,
    index_row_counts,
    metadata_key,
    cast_to_text,
    parse_only_tables,
    pick_key_columns,
    plan_slices,
    quote_ident,
    slice_delete_clause,
    slice_predicate,
    sql_literal,
)


def _cobertura(slices: list[tuple[int | None, int | None]], chaves: range) -> list[int]:
    """Quantas fatias cobrem cada chave. Deve ser exatamente 1 para todas."""
    contagem = []
    for k in chaves:
        n = sum(
            1
            for lo, hi in slices
            if (lo is None or k >= lo) and (hi is None or k < hi)
        )
        contagem.append(n)
    return contagem


# ── plan_slices ──────────────────────────────────────────────────────────────


def test_tabela_menor_que_a_fatia_carrega_de_uma_vez() -> None:
    assert plan_slices(1, 1000, row_count=1000, rows_per_slice=5000) == [(None, None)]


def test_tabela_sem_chave_carrega_de_uma_vez() -> None:
    assert plan_slices(None, None, row_count=10**9, rows_per_slice=1000) == [
        (None, None)
    ]


def test_as_fatias_cobrem_toda_a_chave_exatamente_uma_vez() -> None:
    slices = plan_slices(1, 1000, row_count=1000, rows_per_slice=100)

    assert len(slices) == 10
    assert _cobertura(slices, range(1, 1001)) == [1] * 1000


def test_as_pontas_ficam_abertas_para_pegar_linha_fora_do_min_max() -> None:
    # A origem e um banco vivo: entre ler o max() e terminar a carga, ela ganha
    # linhas com chave maior. A ultima fatia tem que levar essas junto.
    slices = plan_slices(100, 200, row_count=1000, rows_per_slice=100)

    assert slices[0][0] is None, "primeira fatia deve ser aberta a esquerda"
    assert slices[-1][1] is None, "ultima fatia deve ser aberta a direita"
    assert _cobertura(slices, range(-50, 100)) == [1] * 150
    assert _cobertura(slices, range(200, 400)) == [1] * 200


def test_dominio_de_um_valor_so_nao_e_fatiado() -> None:
    # span == 1: fatiar aqui produziria uma faixa unica com as duas pontas
    # fechadas de um lado so, que deixaria buraco.
    assert plan_slices(7, 7, row_count=10**6, rows_per_slice=10) == [(None, None)]


def test_numero_de_fatias_tem_teto() -> None:
    slices = plan_slices(1, 10**9, row_count=10**10, rows_per_slice=1)

    assert len(slices) == 200


def test_max_menor_que_min_nao_e_fatiado() -> None:
    assert plan_slices(500, 100, row_count=10**6, rows_per_slice=10) == [(None, None)]


# ── slice_predicate ──────────────────────────────────────────────────────────


def test_primeira_fatia_nao_tem_limite_inferior() -> None:
    assert slice_predicate("Id", None, 50) == 'WHERE "Id" < 50'


def test_ultima_fatia_recolhe_as_chaves_nulas() -> None:
    assert slice_predicate("Id", 50, None) == 'WHERE "Id" >= 50 OR "Id" IS NULL'


def test_fatia_do_meio_e_meio_aberta() -> None:
    assert slice_predicate("Id", 50, 100) == 'WHERE "Id" >= 50 AND "Id" < 100'


def test_carga_unica_nao_tem_predicado() -> None:
    assert slice_predicate("Id", None, None) == ""


def test_apenas_uma_fatia_recolhe_os_nulos() -> None:
    slices = plan_slices(1, 1000, row_count=1000, rows_per_slice=100)
    predicados = [slice_predicate("Id", lo, hi) for lo, hi in slices]

    assert sum("IS NULL" in p for p in predicados) == 1


# ── slice_delete_clause ──────────────────────────────────────────────────────


def test_delete_recorta_pela_coluna_tecnica_e_nao_pela_chave() -> None:
    # Recortar pela chave exigiria CAST("k" AS bigint) — a bronze guarda tudo
    # como texto — e o conector NAO empurra isso: o Trino recusa o DELETE com
    # "can not perform merge on the target table without primary keys".
    # Comparar um inteiro simples empurra limpo.
    assert slice_delete_clause(3) == 'WHERE "_fatia" = 3'


def test_delete_nao_aceita_indice_forjado() -> None:
    # O indice vai concatenado no SQL; int() impede injecao por ali.
    import pytest

    with pytest.raises((ValueError, TypeError)):
        slice_delete_clause("1 OR 1=1")  # type: ignore[arg-type]


# ── cast_to_text ─────────────────────────────────────────────────────────────


def test_bit_vira_True_maiusculo_como_na_v1() -> None:
    # O CAST do Trino daria 'true'; a v1, via str() do Python, gravava 'True'.
    assert cast_to_text("Ativo", "boolean") == (
        "CASE WHEN \"Ativo\" THEN 'True' WHEN NOT \"Ativo\" THEN 'False' END"
    )


def test_varbinary_vira_hexadecimal() -> None:
    # O Trino recusa CAST(varbinary AS varchar).
    assert cast_to_text("Arquivo", "varbinary") == 'to_hex("Arquivo")'


def test_tipo_com_precisao_e_reconhecido_pela_base() -> None:
    assert cast_to_text("Valor", "decimal(18,2)") == 'CAST("Valor" AS varchar)'
    assert cast_to_text("Quando", "timestamp(3)") == 'CAST("Quando" AS varchar)'


# ── Quoting ──────────────────────────────────────────────────────────────────


def test_aspas_no_identificador_sao_dobradas() -> None:
    assert quote_ident('Estranho"Nome') == '"Estranho""Nome"'


def test_apostrofo_no_literal_e_dobrado() -> None:
    assert sql_literal("d'Água") == "'d''Água'"


# ── build_statements ─────────────────────────────────────────────────────────


def _alvo(key_column: str | None = "Id") -> dict:
    return {
        "catalog": "salic_sac",
        "database": "SAC",
        "schema": "dbo",
        "table": "Projetos",
        "bronze_table": "sac__projetos",
        "key_column": key_column,
    }


def test_insert_escreve_em_coluna_minuscula_lendo_o_nome_original() -> None:
    statements = build_statements(
        _alvo(), [("IdPRONAC", "integer"), ("NomeProjeto", "varchar(80)")], [(1, 50)]
    )
    insert = statements[0]["insert"]

    assert (
        'INSERT INTO dw.bronze."sac__projetos" '
        '("idpronac", "nomeprojeto", "_fatia")' in insert
    )
    assert 'CAST("IdPRONAC" AS varchar)' in insert
    assert 'FROM salic_sac."dbo"."Projetos"' in insert
    assert 'WHERE "Id" >= 1 AND "Id" < 50' in insert


def test_cada_fatia_grava_o_proprio_numero() -> None:
    # Sem isso o DELETE de repeticao nao teria por onde recortar.
    slices = plan_slices(1, 1000, row_count=1000, rows_per_slice=250)
    statements = build_statements(_alvo(), [("Nome", "varchar(80)")], slices)

    for i, st in enumerate(statements):
        # o numero da fatia e a ultima expressao do SELECT
        select = st["insert"].split("SELECT", 1)[1].split("\nFROM", 1)[0]
        assert select.strip().split(",")[-1].strip() == str(i)
        assert st["delete"] == (
            f'DELETE FROM dw.bronze."sac__projetos" WHERE "_fatia" = {i}'
        )


def test_tabela_sem_chave_gera_um_comando_sem_where() -> None:
    statements = build_statements(
        _alvo(key_column=None), [("Nome", "varchar(80)")], [(None, None)]
    )

    assert len(statements) == 1
    assert "WHERE" not in statements[0]["insert"].split("SELECT")[0]
    assert statements[0]["delete"] == (
        'DELETE FROM dw.bronze."sac__projetos" WHERE "_fatia" = 0'
    )


def test_ddl_cria_tudo_varchar_mais_a_coluna_tecnica() -> None:
    drop, create = bronze_ddl(_alvo(), [("IdPRONAC", "integer"), ("Nome", "varchar(80)")])

    assert drop == 'DROP TABLE IF EXISTS dw.bronze."sac__projetos"'
    assert create == (
        'CREATE TABLE dw.bronze."sac__projetos" '
        '("idpronac" varchar, "nome" varchar, "_fatia" integer)'
    )
    assert SLICE_COLUMN in create


# ── Metadados ────────────────────────────────────────────────────────────────


def test_cruzamento_normaliza_a_caixa_dos_dois_lados() -> None:
    # O bug que isso trava: o information_schema do Trino devolve
    # 'tbmovimentacaobancariaitem' e o sys.partitions devolve
    # 'tbMovimentacaoBancariaItem'. Cruzar cru nao casa nada, e o efeito nao e
    # erro — e o fatiamento desligado em silencio.
    do_trino = ("dbo", "tbmovimentacaobancariaitem")
    do_sql_server = [("dbo", "tbMovimentacaoBancariaItem", 18588833)]

    contagens = index_row_counts(do_sql_server)

    assert contagens[metadata_key(*do_trino)] == 18588833


def test_chave_tambem_casa_com_caixa_diferente() -> None:
    keys = pick_key_columns([("dbo", "tbPlanilhaProposta", "idPlanilhaProposta", 1)])

    assert keys[metadata_key("dbo", "tbplanilhaproposta")] == "idPlanilhaProposta"


def test_chave_primaria_ganha_da_coluna_identity() -> None:
    linhas = [
        ("dbo", "Projetos", "Sequencial", 2),
        ("dbo", "Projetos", "IdPRONAC", 1),
    ]

    # A chave sai normalizada; o NOME DA COLUNA preserva a caixa de origem.
    assert pick_key_columns(linhas) == {("dbo", "projetos"): "IdPRONAC"}


def test_identity_serve_quando_nao_ha_chave_primaria_inteira() -> None:
    assert pick_key_columns([("dbo", "Log", "Seq", 2)]) == {("dbo", "log"): "Seq"}


def test_only_tables_aceita_espaco_e_caixa_livre() -> None:
    assert parse_only_tables(" SAC.Projetos , Agentes.Agentes ") == {
        ("sac", "projetos"),
        ("agentes", "agentes"),
    }


def test_only_tables_vazio_nao_restringe_nada() -> None:
    assert parse_only_tables("") == set()
    assert parse_only_tables("  ,  ") == set()
