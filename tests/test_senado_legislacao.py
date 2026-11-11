"""Política de coleta da legislação do Senado: que anos listar, que detalhes
faltam e onde cada coisa cai no raw."""

import json
from datetime import datetime, timezone
from typing import Any

import pytest

import senado_legislacao as sl
from senado_legislacao import Arquivo, Norma

_RUN = "manual__2026-11-10T12:00:00+00:00"
_HOJE = datetime(2026, 11, 10, 12, 0, tzinfo=timezone.utc)


def _quando(ano: int, mes: int, dia: int) -> datetime:
    return datetime(ano, mes, dia, tzinfo=timezone.utc)


def test_keys_seguem_a_particao_da_issue() -> None:
    assert sl.key_lista("DEC", 1998, _RUN) == (
        "raw/senado/legislacao_lista/ano=1998/tipo=DEC/"
        "manual__2026-11-10T12_00_00_00_00.json"
    )
    assert sl.key_detalhe(1990, "549988", _RUN).startswith(
        "raw/senado/legislacao_detalhe/ano=1990/id=549988/"
    )
    assert sl.particao(sl.key_lista("LCP", 2001, _RUN)) == {"ano": "2001", "tipo": "LCP"}


def test_id_que_nao_e_numero_nao_vira_pasta() -> None:
    with pytest.raises(ValueError):
        sl.key_detalhe(1990, "../x", _RUN)


def test_envelope_guarda_a_resposta_intacta() -> None:
    resposta = {"ListaDocumento": {"documentos": ""}}
    env = sl.envelope(
        sl.ENTIDADE_LISTA,
        {"path": "/legislacao/lista", "params": {"tipo": "LEI", "ano": 2026}},
        200,
        resposta,
        0,
        _RUN,
        _HOJE,
    )
    assert env["resposta"] is resposta
    assert env["buscado_em"] == "2026-11-10T12:00:00+00:00"
    assert json.loads(sl.corpo_json(env)) == json.loads(json.dumps(env))
    assert "ã" in sl.corpo_json({"x": "ação"}).decode("utf-8")


def _objeto(key: str, quando: datetime) -> dict[str, Any]:
    return {"Key": key, "LastModified": quando}


def test_arquivos_juntam_partes_e_separam_os_inconsistentes() -> None:
    base = "raw/senado/legislacao_detalhe/ano=1990/id=549988/run"
    incompleto = "raw/senado/legislacao_detalhe/ano=1990/id=1/run"
    objetos = [
        _objeto(f"{base}_g0123abcd_p01de02.json", _quando(2026, 11, 1)),
        _objeto(f"{base}_g0123abcd_p02de02.json", _quando(2026, 11, 2)),
        _objeto(f"{incompleto}_g0123abcd_p01de02.json", _quando(2026, 11, 1)),
        _objeto(
            "raw/senado/legislacao_detalhe/ano=1990/id=2/run.json", _quando(2026, 11, 1)
        ),
    ]
    lista, problemas = sl.arquivos(objetos)

    assert [a.key for a in lista] == [
        "raw/senado/legislacao_detalhe/ano=1990/id=2/run.json",
        f"{base}.json",
    ]
    assert lista[1].partes == (
        f"{base}_g0123abcd_p01de02.json",
        f"{base}_g0123abcd_p02de02.json",
    )
    assert lista[1].modificado == _quando(2026, 11, 2)
    assert problemas == {f"{incompleto}.json"}
    # O id com parte faltando não conta como baixado: volta a ser baixado.
    assert sl.ids_baixados(lista) == {"2", "549988"}


def _lista(tipo: str, ano: int, quando: datetime, run: str = "r") -> Arquivo:
    key = f"raw/senado/legislacao_lista/ano={ano}/tipo={tipo}/{run}.json"
    return Arquivo(key, (key,), quando)


def _por_particao(*arquivos: Arquivo) -> dict[tuple[str, int], list[Arquivo]]:
    return sl.listagens_por_particao(arquivos)


def test_primeiro_run_lista_todos_os_tipos_e_anos() -> None:
    pedidos = sl.a_listar(_HOJE, {})
    assert len(pedidos) == 3 * (2026 - 1988 + 1)
    assert pedidos[0] == ("LEI", 1988) and pedidos[-1] == ("DEC", 2026)


def test_ano_corrente_e_sempre_listado() -> None:
    feitas = _por_particao(*(_lista(t, 2026, _quando(2026, 11, 1)) for t in sl.TIPOS))
    assert ("LEI", 2026) in sl.a_listar(_HOJE, feitas)


def test_ano_anterior_so_fecha_com_listagem_depois_de_abril() -> None:
    hoje = _quando(2027, 2, 1)
    antes = _por_particao(_lista("LEI", 2026, _quando(2027, 1, 2)))
    depois = _por_particao(
        _lista("LEI", 2026, _quando(2027, 1, 2)),
        _lista("LEI", 2026, _quando(2027, 4, 1), "r2"),
    )
    assert ("LEI", 2026) in sl.a_listar(hoje, antes)
    assert ("LEI", 2026) not in sl.a_listar(_quando(2027, 5, 1), depois)


def test_ano_antigo_listado_uma_vez_nao_volta() -> None:
    feitas = _por_particao(_lista("DEC", 1998, _quando(2026, 11, 1)))
    pedidos = sl.a_listar(_HOJE, feitas)
    assert ("DEC", 1998) not in pedidos
    assert ("LEI", 1998) in pedidos  # outro tipo do mesmo ano ainda não foi


def test_anos_pedidos_forcam_a_relistagem() -> None:
    feitas = _por_particao(_lista("DEC", 1998, _quando(2026, 11, 1)))
    assert sl.a_listar(_HOJE, feitas, [1998]) == [
        ("LEI", 1998),
        ("LCP", 1998),
        ("DEC", 1998),
    ]
    with pytest.raises(ValueError, match="1987"):
        sl.a_listar(_HOJE, {}, [1987])


def test_ultima_listagem_e_a_mais_recente() -> None:
    velha = _lista("LEI", 2026, _quando(2026, 10, 1), "velha")
    nova = _lista("LEI", 2026, _quando(2026, 11, 1), "nova")
    assert sl.ultimas_listagens(_por_particao(nova, velha)) == {("LEI", 2026): nova}


def _envelope_lista(*ids: str) -> dict[str, Any]:
    documentos = [{"id": i} for i in ids]
    return {"resposta": {"ListaDocumento": {"documentos": {"documento": documentos}}}}


def test_pendentes_do_ano_mais_recente_para_o_mais_antigo() -> None:
    listados = sl.ids_listados(
        [
            (("LEI", 1990), _envelope_lista("549988", "550001")),
            (("DEC", 2026), _envelope_lista("40035256")),
            (("LCP", 2026), {"resposta": {"ListaDocumento": {"documentos": ""}}}),
        ]
    )
    assert listados["549988"] == Norma(1990, "LEI")

    faltam = sl.pendentes(listados, baixados={"550001"})
    assert [i for i, _ in faltam] == ["40035256", "549988"]


def test_resumo_por_tipo() -> None:
    listados = {"1": Norma(2026, "LEI"), "2": Norma(2026, "LEI"), "3": Norma(1990, "DEC")}
    assert sl.resumo_por_tipo(listados, {"1"}) == {
        "DEC": {"listadas": 1, "com_detalhe": 0, "pendentes": 1},
        "LEI": {"listadas": 2, "com_detalhe": 1, "pendentes": 1},
    }


@pytest.mark.parametrize(
    ("tentadas", "falhas", "excedeu"),
    [
        (40_000, 1, False),
        (40_000, 2_000, False),
        (40_000, 2_001, True),
        (5, 5, False),
        (50, 11, True),
    ],
)
def test_limite_de_falhas(tentadas: int, falhas: int, excedeu: bool) -> None:
    assert sl.excedeu_falhas(tentadas, falhas) is excedeu
