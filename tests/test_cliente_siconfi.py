"""Montagem das consultas SICONFI a partir do extrato, validação e paginação.

As linhas de extrato abaixo seguem ``/extrato_entregas`` real. O extrato usa o
nome por extenso do demonstrativo; procurar só a sigla fazia RREO e RGF nunca
entrarem na fila. As regras de parâmetro foram verificadas com chamadas reais
em 29/09/2026: combinação errada volta HTTP 200 com lista vazia, sem erro.
"""

from contextlib import contextmanager
from typing import Any, Iterator

import httpx
import pytest

import cliente_siconfi
from cliente_siconfi import (
    PlanScope,
    SiconfiClient,
    SiconfiParametroInvalido,
    SiconfiPermanentError,
    SiconfiRetryableError,
    classify_delivery,
    esfera_do_ente,
    plan_units,
    poder_da_instituicao,
    validate_params,
)
from siconfi_config import carregar
from siconfi_storage import classify_outcome

ANO = 2026


def _scope(variable: dict[str, Any] | None = None, entes: Any = None) -> PlanScope:
    return PlanScope(carregar(variable, None, ANO), entes)


def _linha(**campos: Any) -> dict[str, Any]:
    return {
        "exercicio": 2025,
        "cod_ibge": 3550308,
        "periodo": 1,
        "data_status": "2025-05-30T10:00:00Z",
        "status_relatorio": "HO",
        "forma_envio": "P",
        **campos,
    }


def _params(units: list[tuple]) -> list[dict[str, Any]]:
    return [u[1] for u in units]


# --- Classificação do extrato -----------------------------------------------------


@pytest.mark.parametrize(
    ("entregavel", "expected"),
    [
        ("Relatório Resumido de Execução Orçamentária", "rreo"),
        ("Relatório Resumido de Execução Orçamentária Simplificado", "rreo"),
        ("Relatório de Gestão Fiscal", "rgf"),
        ("Relatório de Gestão Fiscal Simplificado", "rgf"),
        ("Balanço Anual (DCA)", "dca"),
        ("MSC Agregada", "msc"),
        ("MSC Encerramento", "msc"),
        ("RREO", "rreo"),
        ("Algo novo", None),
    ],
)
def test_classifica_nome_por_extenso(entregavel: str, expected: str | None) -> None:
    assert classify_delivery(entregavel) == expected


@pytest.mark.parametrize(
    ("instituicao", "poder"),
    [
        ("Prefeitura Municipal de São Paulo - SP", "E"),
        ("Governo do Estado do Espírito Santo", "E"),
        ("Governo do Distrito Federal", "E"),
        ("Câmara de Vereadores de São Paulo - SP", "L"),
        ("Câmara Legislativa do Distrito Federal", "L"),
        ("Assembleia Legislativa do Estado do Espírito Santo", "L"),
        # Conferido na API: o RGF do TC sai em co_poder=L, não em J.
        ("Tribunal de Contas do Município de São Paulo", "L"),
        ("Tribunal de Justiça do Estado do Espírito Santo", "J"),
        ("Ministério Público do Estado do Espírito Santo", "M"),
        ("Defensoria Pública do Estado do Espírito Santo", "D"),
        ("Consórcio Intermunicipal", None),
    ],
)
def test_poder_da_instituicao(instituicao: str, poder: str | None) -> None:
    assert poder_da_instituicao(instituicao) == poder


@pytest.mark.parametrize(
    ("cod_ibge", "esfera"), [(1, "U"), (32, "E"), (53, "D"), (3205309, "M")]
)
def test_esfera_pelo_codigo_ibge(cod_ibge: int, esfera: str) -> None:
    assert esfera_do_ente(cod_ibge) == esfera


# --- RREO ------------------------------------------------------------------------

_RREO = "Relatório Resumido de Execução Orçamentária"


def test_rreo_normal() -> None:
    units = plan_units(
        [_linha(entregavel=_RREO, periodicidade="B", periodo=3)], "M", _scope()
    )
    assert units == [
        (
            "rreo",
            {
                "id_ente": 3550308,
                "an_exercicio": 2025,
                "nr_periodo": 3,
                "co_tipo_demonstrativo": "RREO",
            },
            "2025-05-30T10:00:00Z|HO|P",
            True,
        )
    ]


def test_rreo_simplificado_vem_do_extrato() -> None:
    # Quem entrega o simplificado só aparece com 'RREO Simplificado'; com
    # 'RREO' a API devolve vazio.
    linha = _linha(entregavel=f"{_RREO} Simplificado", periodicidade="B", periodo=2)
    [(_, params, _, _)] = plan_units([linha], "M", _scope())
    assert params["co_tipo_demonstrativo"] == "RREO Simplificado"


def test_rreo_respeita_os_periodos_e_os_anos_configurados() -> None:
    scope = _scope({"endpoints": {"rreo": {"periodos": [6]}}})
    linhas = [_linha(entregavel=_RREO, periodicidade="B", periodo=p) for p in (1, 6)]
    assert [p["nr_periodo"] for p in _params(plan_units(linhas, "M", scope))] == [6]
    # 2014 vem vazio: nem entra na fila.
    antigo = _linha(entregavel=_RREO, periodicidade="B", exercicio=2014)
    assert plan_units([antigo], "M", _scope()) == []


def test_rreo_com_anexos_gera_uma_consulta_por_anexo() -> None:
    scope = _scope(
        {"endpoints": {"rreo": {"no_anexo": ["RREO-Anexo 01", "RREO-Anexo 02"]}}}
    )
    units = plan_units([_linha(entregavel=_RREO, periodicidade="B")], "M", scope)
    assert [p["no_anexo"] for p in _params(units)] == ["RREO-Anexo 01", "RREO-Anexo 02"]


# --- RGF -------------------------------------------------------------------------

_RGF = "Relatório de Gestão Fiscal"


def _rgf(instituicao: str, **campos: Any) -> dict[str, Any]:
    return _linha(entregavel=_RGF, periodicidade="Q", instituicao=instituicao, **campos)


def test_rgf_quadrimestral_usa_rgf() -> None:
    [(_, params, _, esperado)] = plan_units(
        [_rgf("Prefeitura Municipal de São Paulo - SP", periodo=3)], "M", _scope()
    )
    assert params == {
        "id_ente": 3550308,
        "an_exercicio": 2025,
        "in_periodicidade": "Q",
        "nr_periodo": 3,
        "co_tipo_demonstrativo": "RGF",
        "co_poder": "E",
    }
    assert esperado is True


def test_rgf_semestral_usa_rgf_simplificado() -> None:
    linha = _linha(
        entregavel=f"{_RGF} Simplificado",
        periodicidade="S",
        periodo=2,
        instituicao="Prefeitura Municipal de Divino de São Lourenço - ES",
    )
    [(_, params, _, _)] = plan_units([linha], "M", _scope())
    assert (params["in_periodicidade"], params["co_tipo_demonstrativo"]) == (
        "S",
        "RGF Simplificado",
    )


def test_rgf_consulta_um_poder_por_vez_so_de_quem_entregou() -> None:
    linhas = [
        _rgf("Prefeitura Municipal de São Paulo - SP"),
        _rgf("Câmara de Vereadores de São Paulo - SP"),
    ]
    units = plan_units(linhas, "M", _scope())
    assert [(p["co_poder"], esp) for _, p, _, esp in units] == [("E", True), ("L", True)]


def test_rgf_municipal_nunca_consulta_j_m_d() -> None:
    # Município só tem E e L: J, M e D voltam vazios.
    units = plan_units([_rgf("Consórcio Intermunicipal")], "M", _scope())
    assert [(p["co_poder"], esp) for _, p, _, esp in units] == [
        ("E", False),
        ("L", False),
    ]
    assert plan_units([_rgf("Tribunal de Justiça de SP")], "M", _scope()) == []


@pytest.mark.parametrize("esfera", ["E", "D", "U"])
def test_rgf_estado_df_e_uniao_usam_os_cinco_poderes(esfera: str) -> None:
    units = plan_units([_rgf("Instituição sem nome conhecido")], esfera, _scope())
    assert [p["co_poder"] for p in _params(units)] == ["E", "L", "J", "M", "D"]


def test_rgf_assembleia_e_tce_viram_uma_consulta_so() -> None:
    assembleia = _rgf("Assembleia Legislativa do Estado do Espírito Santo", cod_ibge=32)
    tce = _rgf(
        "Tribunal de Contas do Estado do Espírito Santo",
        cod_ibge=32,
        status_relatorio="RE",
    )
    [(_, params, marker, _)] = plan_units([assembleia, tce], "E", _scope())
    assert params["co_poder"] == "L"
    # A ordem das linhas não muda o marcador: senão a fila rebuscaria à toa.
    [(_, _, invertido, _)] = plan_units([tce, assembleia], "E", _scope())
    assert marker == invertido and "HO" in marker and "RE" in marker


def test_rgf_anexos_fora_do_executivo_so_01_05_06() -> None:
    scope = _scope({"endpoints": {"rgf": {"no_anexo": ["RGF-Anexo 01", "RGF-Anexo 02"]}}})
    units = plan_units(
        [_rgf("Prefeitura Municipal de X"), _rgf("Câmara Municipal de X")], "M", scope
    )
    assert [(p["co_poder"], p["no_anexo"]) for p in _params(units)] == [
        ("E", "RGF-Anexo 01"),
        ("E", "RGF-Anexo 02"),
        ("L", "RGF-Anexo 01"),
    ]


def test_rgf_poderes_por_esfera_configuraveis() -> None:
    scope = _scope({"endpoints": {"rgf": {"poderes_por_esfera": {"M": ["E"]}}}})
    assert plan_units([_rgf("Câmara de Vereadores de X")], "M", scope) == []


# --- DCA -------------------------------------------------------------------------

_DCA = "Balanço Anual (DCA)"


def test_dca_sem_anexo_traz_todos_numa_chamada() -> None:
    units = plan_units([_linha(entregavel=_DCA, periodicidade="A")], "M", _scope())
    assert _params(units) == [{"id_ente": 3550308, "an_exercicio": 2025}]


def test_dca_2013_usa_o_nome_de_anexo_sem_prefixo() -> None:
    scope = _scope({"endpoints": {"dca": {"no_anexo": ["DCA-Anexo I-E"]}}})
    em_2013 = plan_units(
        [_linha(entregavel="QDCC", periodicidade="A", exercicio=2013)], "M", scope
    )
    em_2014 = plan_units(
        [_linha(entregavel=_DCA, periodicidade="A", exercicio=2014)], "M", scope
    )
    assert _params(em_2013)[0]["no_anexo"] == "Anexo I-E"
    assert _params(em_2014)[0]["no_anexo"] == "DCA-Anexo I-E"


# --- MSC -------------------------------------------------------------------------


def _msc(mes: int, **campos: Any) -> dict[str, Any]:
    return _linha(
        **{
            "entregavel": "MSC Agregada",
            "periodicidade": "M",
            "periodo": mes,
            "cod_ibge": 32,
            "exercicio": 2024,
            **campos,
        }
    )


def test_msc_padrao_e_so_orcamentaria_classes_5_e_6_de_dezembro() -> None:
    units = plan_units([_msc(m) for m in range(1, 13)], "E", _scope())
    assert [(e, p["classe_conta"], p["me_referencia"]) for e, p, _, _ in units] == [
        ("msc_orcamentaria", 5, 12),
        ("msc_orcamentaria", 6, 12),
    ]
    assert {p["id_tv"] for p in _params(units)} == {"ending_balance"}
    assert {p["co_tipo_matriz"] for p in _params(units)} == {"MSCC"}


def test_msc_fase_2_so_os_meses_entregues() -> None:
    scope = _scope({"endpoints": {"msc_orcamentaria": {"meses": list(range(1, 13))}}})
    # O extrato mostra só janeiro a março entregues.
    units = plan_units([_msc(m) for m in (1, 2, 3)], "E", scope)
    assert sorted({p["me_referencia"] for p in _params(units)}) == [1, 2, 3]
    assert len(units) == 6


def test_msce_so_quando_configurada_e_sempre_em_dezembro() -> None:
    encerramento = _linha(
        entregavel="MSC Encerramento", periodicidade="A", periodo=1, exercicio=2024
    )
    assert plan_units([encerramento], "E", _scope()) == []
    scope = _scope(
        {"endpoints": {"msc_orcamentaria": {"tipos_matriz": ["MSCC", "MSCE"]}}}
    )
    units = plan_units([encerramento], "E", scope)
    assert {(p["co_tipo_matriz"], p["me_referencia"]) for p in _params(units)} == {
        ("MSCE", 12)
    }


def test_msc_antes_de_2019_nao_entra() -> None:
    assert plan_units([_msc(12, exercicio=2018)], "E", _scope()) == []


def test_msc_patrimonial_ligada_usa_as_proprias_classes() -> None:
    scope = _scope({"endpoints": {"msc_patrimonial": {"ativo": True}}})
    units = plan_units([_msc(12)], "E", scope)
    assert {(e, p["classe_conta"]) for e, p, _, _ in units} == {
        ("msc_orcamentaria", 5),
        ("msc_orcamentaria", 6),
        ("msc_patrimonial", 1),
        ("msc_patrimonial", 2),
        ("msc_patrimonial", 3),
        ("msc_patrimonial", 4),
    }


def test_endpoint_desligado_nao_gera_nada() -> None:
    scope = _scope({"endpoints": {"dca": {"ativo": False}}})
    assert plan_units([_linha(entregavel=_DCA)], "M", scope) == []


def test_linha_sem_ente_e_ignorada() -> None:
    assert plan_units([{"entregavel": _RGF}], "M", _scope()) == []


# --- Recorte no claim ----------------------------------------------------------------


def test_constraints_vao_para_o_claim() -> None:
    scope = _scope(
        {"endpoints": {"rgf": {"poderes_por_esfera": {"M": ["E"], "E": ["E", "L"]}}}},
        entes=[32, 3205309],
    )
    constraints = scope.constraints("rgf")
    assert constraints["co_poder"] == frozenset({"E", "L", "J", "M", "D"})
    assert constraints["id_ente"] == frozenset({32, 3205309})
    assert constraints["an_exercicio"] == frozenset(range(2015, ANO + 1))
    assert set(_scope().constraints("extrato_entregas")) == {"an_referencia"}
    msc = _scope().constraints("msc_orcamentaria")
    assert msc["classe_conta"] == frozenset({5, 6})
    assert msc["me_referencia"] == frozenset({12})


def test_constraints_da_dca_aceitam_o_nome_de_2013() -> None:
    scope = _scope({"endpoints": {"dca": {"no_anexo": ["DCA-Anexo I-E"]}}})
    assert scope.constraints("dca")["no_anexo"] == frozenset(
        {"DCA-Anexo I-E", "Anexo I-E"}
    )


def test_fingerprint_muda_com_o_recorte() -> None:
    assert _scope().fingerprint() == _scope().fingerprint()
    fase_2 = _scope({"endpoints": {"msc_orcamentaria": {"meses": [11, 12]}}})
    assert _scope().fingerprint() != fase_2.fingerprint()
    assert _scope().fingerprint() != _scope(entes=[32]).fingerprint()
    # Recarga e rebusca não mudam o que se planeja.
    rebusca = _scope({"endpoints": {"extrato_entregas": {"rebusca_dias": 1}}})
    assert _scope().fingerprint() == rebusca.fingerprint()


# --- Validação dos parâmetros antes da chamada ---------------------------------------

_MSC_OK = {
    "id_ente": 32,
    "an_referencia": 2024,
    "me_referencia": 12,
    "co_tipo_matriz": "MSCC",
    "classe_conta": 6,
    "id_tv": "ending_balance",
}
_RGF_OK = {
    "id_ente": 32,
    "an_exercicio": 2024,
    "in_periodicidade": "Q",
    "nr_periodo": 3,
    "co_tipo_demonstrativo": "RGF",
    "co_poder": "L",
}


@pytest.mark.parametrize(
    ("endpoint", "params"),
    [
        ("entes", {}),
        ("anexos-relatorios", {}),
        ("extrato_entregas", {"id_ente": 32, "an_referencia": 2013}),
        ("dca", {"id_ente": 32, "an_exercicio": 2013, "no_anexo": "Anexo I-E"}),
        (
            "rreo",
            {
                "id_ente": 32,
                "an_exercicio": 2015,
                "nr_periodo": 6,
                "co_tipo_demonstrativo": "RREO Simplificado",
            },
        ),
        ("rgf", _RGF_OK),
        (
            "rgf",
            {
                **_RGF_OK,
                "in_periodicidade": "S",
                "nr_periodo": 2,
                "co_tipo_demonstrativo": "RGF Simplificado",
            },
        ),
        ("msc_orcamentaria", _MSC_OK),
        ("msc_orcamentaria", {**_MSC_OK, "co_tipo_matriz": "MSCE", "classe_conta": 5}),
    ],
)
def test_parametros_validos_passam(endpoint: str, params: dict[str, Any]) -> None:
    validate_params(endpoint, params)


@pytest.mark.parametrize(
    ("endpoint", "params", "trecho"),
    [
        # Sem id_ente a API devolve vazio, mesmo com co_esfera.
        (
            "rreo",
            {
                "an_exercicio": 2024,
                "nr_periodo": 1,
                "co_tipo_demonstrativo": "RREO",
                "co_esfera": "E",
            },
            "faltam ['id_ente']",
        ),
        (
            "rgf",
            {k: v for k, v in _RGF_OK.items() if k != "co_poder"},
            "faltam ['co_poder']",
        ),
        ("rgf", {**_RGF_OK, "co_poder": None}, "faltam ['co_poder']"),
        (
            "msc_orcamentaria",
            {k: v for k, v in _MSC_OK.items() if k != "id_tv"},
            "faltam ['id_tv']",
        ),
        ("extrato_entregas", {"id_ente": 32}, "faltam ['an_referencia']"),
        ("entes", {"exercicio": 2024}, "não aceitos ['exercicio']"),
        ("dca", {"id_ente": 32, "an_exercicio": 2024, "no_anexos": "x"}, "não aceitos"),
        (
            "rreo",
            {
                "id_ente": 32,
                "an_exercicio": 2014,
                "nr_periodo": 1,
                "co_tipo_demonstrativo": "RREO",
            },
            "anterior a 2015",
        ),
        (
            "rreo",
            {
                "id_ente": 32,
                "an_exercicio": 2024,
                "nr_periodo": 7,
                "co_tipo_demonstrativo": "RREO",
            },
            "bimestres",
        ),
        (
            "rgf",
            {**_RGF_OK, "co_tipo_demonstrativo": "RGF Simplificado"},
            "exige co_tipo_demonstrativo='RGF'",
        ),
        (
            "rgf",
            {
                **_RGF_OK,
                "in_periodicidade": "S",
                "co_tipo_demonstrativo": "RGF Simplificado",
            },
            "fora de 1–2",
        ),
        ("rgf", {**_RGF_OK, "co_poder": "X"}, "co_poder='X'"),
        ("msc_orcamentaria", {**_MSC_OK, "classe_conta": 7}, "só [5, 6]"),
        ("msc_orcamentaria", {**_MSC_OK, "an_referencia": 2018}, "anterior a 2019"),
        (
            "msc_orcamentaria",
            {**_MSC_OK, "co_tipo_matriz": "MSCE", "me_referencia": 13},
            "fora de 1–12",
        ),
        (
            "msc_orcamentaria",
            {**_MSC_OK, "co_tipo_matriz": "MSCE", "me_referencia": 11},
            "MSCE só existe",
        ),
        ("msc_orcamentaria", {**_MSC_OK, "id_ente": "32"}, "não é inteiro"),
        ("siconfi_x", {}, "desconhecido"),
    ],
)
def test_parametros_incompletos_ou_invalidos_sao_recusados(
    endpoint: str, params: dict[str, Any], trecho: str
) -> None:
    with pytest.raises(SiconfiParametroInvalido) as excinfo:
        validate_params(endpoint, params)
    assert trecho in str(excinfo.value)


# --- Vazio esperado × inesperado ---------------------------------------------------


@pytest.mark.parametrize(
    ("rows", "esperado", "exc", "attempts", "status"),
    [
        (10, True, None, 1, "success"),
        (0, True, None, 1, "unexpected_empty"),
        (0, False, None, 1, "no_data"),
        (0, None, None, 1, "no_data"),  # extrato: ente sem entregas no ano
        (0, True, SiconfiPermanentError(404, "x"), 1, "unexpected_empty"),
        (0, True, SiconfiPermanentError(400, "x"), 1, "permanent_error"),
        (0, True, SiconfiRetryableError(503, "x"), 1, "retry"),
        (0, True, SiconfiRetryableError(503, "x"), 5, "permanent_error"),
        (0, True, SiconfiParametroInvalido("x"), 1, "permanent_error"),
    ],
)
def test_classifica_o_resultado_da_particao(
    rows: int, esperado: bool | None, exc: Exception | None, attempts: int, status: str
) -> None:
    assert classify_outcome(rows, esperado, exc, attempts, max_attempts=5) == status


# --- Paginação --------------------------------------------------------------------


class _LimiterFalso:
    def __init__(self) -> None:
        self.reservas = 0

    @contextmanager
    def slot(self) -> Iterator[None]:
        yield

    def reserve(self) -> None:
        self.reservas += 1

    def close(self) -> None:
        pass


def _client(
    handler: Any, monkeypatch: pytest.MonkeyPatch, **kwargs: Any
) -> tuple[SiconfiClient, _LimiterFalso]:
    monkeypatch.setattr(SiconfiClient, "_backoff", staticmethod(lambda *_: None))
    limiter = _LimiterFalso()
    client = SiconfiClient(
        "dsn-falso",
        rate_limiter=limiter,
        transport=httpx.MockTransport(handler),
        **kwargs,
    )
    return client, limiter


def test_pagina_por_offset_enquanto_has_more(monkeypatch: pytest.MonkeyPatch) -> None:
    pedidos: list[dict[str, str]] = []
    paginas = [[{"n": 1}, {"n": 2}], [{"n": 3}, {"n": 4}], [{"n": 5}]]

    def handler(request: httpx.Request) -> httpx.Response:
        params = dict(request.url.params)
        pedidos.append(params)
        indice = int(params["offset"]) // 2
        return httpx.Response(
            200,
            json={
                "items": paginas[indice],
                "hasMore": indice < len(paginas) - 1,
                "limit": 2,
                "offset": int(params["offset"]),
            },
        )

    client, limiter = _client(handler, monkeypatch, page_limit=2)
    pages = list(
        client.iter_pages("extrato_entregas", {"id_ente": 32, "an_referencia": 2024})
    )

    assert [p.offset for p in pages] == [0, 2, 4]
    assert [item["n"] for p in pages for item in p.items] == [1, 2, 3, 4, 5]
    assert [(p["offset"], p["limit"]) for p in pedidos] == [
        ("0", "2"),
        ("2", "2"),
        ("4", "2"),
    ]
    assert {p["id_ente"] for p in pedidos} == {"32"}
    assert limiter.reservas == 3
    # O parâmetro da página não vaza para a identidade da consulta.
    assert pages[0].params == {"id_ente": 32, "an_referencia": 2024}


def test_avanca_pelo_que_veio_e_nao_pelo_limite(monkeypatch: pytest.MonkeyPatch) -> None:
    # O servidor já devolveu páginas menores que o limite pedido.
    offsets: list[int] = []

    def handler(request: httpx.Request) -> httpx.Response:
        offset = int(request.url.params["offset"])
        offsets.append(offset)
        return httpx.Response(
            200, json={"items": [{"n": offset}] * 3, "hasMore": offset == 0}
        )

    client, _ = _client(handler, monkeypatch, page_limit=5000)
    list(client.iter_pages("entes", {}))
    assert offsets == [0, 3]


def test_has_more_com_pagina_vazia_e_erro(monkeypatch: pytest.MonkeyPatch) -> None:
    client, _ = _client(
        lambda _: httpx.Response(200, json={"items": [], "hasMore": True}), monkeypatch
    )
    with pytest.raises(SiconfiPermanentError, match="hasMore=true"):
        list(client.iter_pages("entes", {}))


def test_resposta_sem_json_e_repetida(monkeypatch: pytest.MonkeyPatch) -> None:
    respostas = iter(
        [
            httpx.Response(200, text="<html>erro momentâneo</html>"),
            httpx.Response(503, text="indisponível"),
            httpx.Response(200, json={"items": [{"n": 1}], "hasMore": False}),
        ]
    )
    client, limiter = _client(lambda _: next(respostas), monkeypatch, max_retries=4)
    [page] = list(client.iter_pages("entes", {}))
    assert page.items == [{"n": 1}]
    assert limiter.reservas == 3


def test_falha_momentanea_persistente_vira_erro_retentavel(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, _ = _client(lambda _: httpx.Response(200, text="não é json"), monkeypatch)
    with pytest.raises(SiconfiRetryableError, match="sem JSON"):
        list(client.iter_pages("entes", {}))


def test_erro_de_contrato_nao_e_repetido(monkeypatch: pytest.MonkeyPatch) -> None:
    client, limiter = _client(lambda _: httpx.Response(400, text="ruim"), monkeypatch)
    with pytest.raises(SiconfiPermanentError):
        list(client.iter_pages("entes", {}))
    assert limiter.reservas == 1


def test_parametro_invalido_nao_chega_a_chamar_a_api(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    chamadas: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        chamadas.append(request)
        return httpx.Response(200, json={"items": [], "hasMore": False})

    client, _ = _client(handler, monkeypatch)
    with pytest.raises(SiconfiParametroInvalido):
        list(
            client.iter_pages(
                "rgf", {k: v for k, v in _RGF_OK.items() if k != "co_poder"}
            )
        )
    assert chamadas == []


def test_url_base_da_api() -> None:
    assert (
        cliente_siconfi.BASE_URL == "https://apidatalake.tesouro.gov.br/ords/siconfi/tt"
    )
