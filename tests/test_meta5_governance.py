"""Guardas de governanca da cadeia da Meta 5 no ADR 0010.

As silvers do SALIC em salic_dbt seguem um padrao de documentacao feito para o
RAG e para o OpenMetadata (ver test_salic_silver_governance). A Meta 5
refatorada substitui a de la, entao a cadeia dela -- das silvers das fontes as
golds do Cultura em Numeros -- precisa declarar a mesma politica: sem isso, o
exportador de RAG nao tem como decidir o que pode sair, e ausencia nao pode
significar "publicavel".

A selecao e pela cadeia, nao pela pasta: pelo ADR 0010 as pastas sao por
fonte e compartilhadas entre produtos, entao `transferegov/` e
`cultura_em_numeros/` tem modelos de outras metas que nao assumiram este
padrao. Modelo novo na cadeia entra em CADEIA_META5 no mesmo PR; as tres
primeiras guardas avisam quando a lista envelhece.
"""

import re
from pathlib import Path

import pytest
import yaml

from tests.governanca import PADROES_PII, POLITICAS_RAG, TAGS_PII

MODELS_DIR = Path(__file__).resolve().parents[1] / "dbt/minc/models"

# Os modelos da cadeia, das silvers das fontes as golds. Os dois controles de
# extracao do BB Agil nao sao lidos pelas golds, mas auditam a base delas.
CADEIA_META5 = {
    # silvers das fontes
    "silver_sac_captacao_proponente",
    "silver_consulta",
    "silver_extrato_bbagil",
    "silver_subtransacao_bbagil",
    "silver_pagamento_extrato_bbagil",
    "silver_pagamento_subtransacao_bbagil",
    "silver_pagamento_bbagil",
    "silver_controle_extracao_bbagil_extrato",
    "silver_controle_extracao_bbagil_subtransacoes",
    "silver_planilha_contemplados",
    # intermediate
    "int_ancine_bbagil_salic",
    "int_ancine_bbagil_salic_transferegov",
    # golds do Cultura em Numeros
    "eixo2_meta5_evento_fomento",
    "eixo2_meta5_primeiro_acesso_agente",
    "eixo2_meta5_primeiro_acesso_anual",
    "eixo2_meta5_primeiro_acesso_contemplados",
    "eixo2_meta5_trajetoria_fomento_direto_rouanet",
    "eixo2_meta5_conversao_direto_rouanet_coorte",
}

# O formato que o RAG le: secoes nomeadas, e nao um paragrafo corrido.
SECOES_OBRIGATORIAS = [
    "GRÃO:",
    "UNIVERSO:",
    "PARA QUE SERVE:",
    "LIMITAÇÕES:",
    "ATUALIZAÇÃO:",
]

DESABILITADO = re.compile(r"config\([^)]*enabled\s*=\s*false", re.IGNORECASE)
REF = re.compile(r"ref\(\s*[\"']([^\"']+)[\"']\s*\)")


def _todos_os_modelos() -> dict[str, tuple[dict, Path]]:
    """Bloco `models:` de todo schema*.yml do projeto, indexado por nome."""
    modelos: dict[str, tuple[dict, Path]] = {}
    for caminho in sorted(MODELS_DIR.rglob("*.yml")):
        conteudo = yaml.safe_load(caminho.read_text(encoding="utf-8")) or {}
        for modelo in conteudo.get("models") or []:
            modelos[modelo["name"]] = (modelo, caminho.parent)
    return modelos


TODOS = _todos_os_modelos()
MODELOS = {nome: TODOS[nome] for nome in sorted(CADEIA_META5) if nome in TODOS}


def _meta(modelo: dict) -> dict:
    return (modelo.get("config") or {}).get("meta", {}) or {}


def test_todo_modelo_da_lista_esta_documentado() -> None:
    faltam = sorted(CADEIA_META5 - set(TODOS))
    assert not faltam, f"CADEIA_META5 cita modelo sem schema.yml: {faltam}"


def test_toda_gold_da_meta5_esta_na_lista() -> None:
    golds = {nome for nome in TODOS if nome.startswith("eixo2_meta5_")}
    faltam = sorted(golds - CADEIA_META5)
    assert not faltam, f"gold da Meta 5 fora de CADEIA_META5: {faltam}"


@pytest.mark.parametrize("nome", sorted(MODELOS))
def test_dependencia_fora_da_bronze_esta_na_cadeia(nome: str) -> None:
    """Silver ou intermediate nova lida pela cadeia tambem precisa do padrao."""
    _, pasta = MODELOS[nome]
    sql = (pasta / f"{nome}.sql").read_text(encoding="utf-8")
    fora = sorted(
        ref
        for ref in set(REF.findall(sql)) - CADEIA_META5
        if not ref.startswith("bronze_")
    )
    assert not fora, f"{nome} le {fora}, que nao estao em CADEIA_META5"


@pytest.mark.parametrize("nome", sorted(MODELOS))
def test_modelo_declara_governanca_completa(nome: str) -> None:
    meta = _meta(MODELOS[nome][0])
    om = meta.get("openmetadata") or {}
    gov = meta.get("governance") or {}

    assert om.get("domain"), f"{nome}: sem dominio"
    assert om.get("owner") == "minc-data-engineering", f"{nome}: dono errado"
    assert str(om.get("tier", "")).startswith("Tier.Tier"), f"{nome}: sem tier"
    assert meta.get("status") in {"Active", "Disabled"}, f"{nome}: sem status"
    assert meta.get("meta_objetivo"), f"{nome}: sem meta_objetivo"
    assert meta.get("origem_dados"), f"{nome}: sem origem_dados"
    assert gov.get("rag_publication") in POLITICAS_RAG, f"{nome}: sem politica de RAG"
    assert gov.get("produto_dados"), f"{nome}: sem produto de dados"
    assert gov.get("classificacao_uso"), f"{nome}: sem classificacao de uso"


@pytest.mark.parametrize("nome", sorted(MODELOS))
def test_descricao_tem_as_secoes_do_padrao(nome: str) -> None:
    descricao = MODELOS[nome][0].get("description") or ""
    faltam = [s for s in SECOES_OBRIGATORIAS if s not in descricao]
    assert not faltam, f"{nome}: descricao sem as secoes {faltam}"


@pytest.mark.parametrize("nome", sorted(MODELOS))
def test_status_bate_com_o_enabled_do_sql(nome: str) -> None:
    """`status: Active` num modelo com enabled=false publica tabela que nao existe."""
    modelo, pasta = MODELOS[nome]
    sql = (pasta / f"{nome}.sql").read_text(encoding="utf-8")
    esperado = "Disabled" if DESABILITADO.search(sql) else "Active"
    assert (
        _meta(modelo).get("status") == esperado
    ), f"{nome}: status deveria ser {esperado}"


@pytest.mark.parametrize("nome", sorted(MODELOS))
def test_certificacao_so_em_modelo_ativo(nome: str) -> None:
    meta = _meta(MODELOS[nome][0])
    tags = set((meta.get("openmetadata") or {}).get("tags") or [])
    if tags & {"Certification.Silver", "Certification.Gold"}:
        assert meta.get("status") == "Active", f"{nome}: certificado mas Disabled"


@pytest.mark.parametrize("nome", sorted(MODELOS))
def test_salic_nunca_vai_para_fomento_direto(nome: str) -> None:
    """Mesma regra das silvers do salic_dbt: SALIC e incentivo fiscal."""
    modelo, pasta = MODELOS[nome]
    if pasta.name != "salic":
        return
    dominio = (_meta(modelo).get("openmetadata") or {}).get("domain")
    assert dominio != "Cultura.Fomento Direto", f"{nome}: SALIC em Fomento Direto"


@pytest.mark.parametrize("nome", sorted(MODELOS))
def test_toda_coluna_tem_descricao_propria_e_tipo(nome: str) -> None:
    colunas = MODELOS[nome][0].get("columns") or []
    assert colunas, f"{nome}: nenhuma coluna documentada"
    for coluna in colunas:
        rotulo = f"{nome}.{coluna['name']}"
        descricao = (coluna.get("description") or "").strip()
        assert len(descricao) >= 20, f"{rotulo}: descricao ausente ou generica demais"
        assert coluna.get("data_type"), f"{rotulo}: sem data_type"


@pytest.mark.parametrize("nome", sorted(MODELOS))
def test_coluna_com_cara_de_pii_esta_classificada(nome: str) -> None:
    modelo = MODELOS[nome][0]
    gov = _meta(modelo).get("governance") or {}
    dispensadas = set(gov.get("derivadas_nao_identificantes") or [])
    nomes = {coluna["name"] for coluna in modelo.get("columns") or []}
    orfas = sorted(dispensadas - nomes)
    assert not orfas, f"{nome}: dispensa de PII para coluna inexistente {orfas}"

    for coluna in modelo.get("columns") or []:
        if coluna["name"] in dispensadas or not PADROES_PII.search(coluna["name"]):
            continue
        tags = set((coluna.get("meta") or {}).get("openmetadata", {}).get("tags") or [])
        assert (
            tags & TAGS_PII
        ), f"{nome}.{coluna['name']} parece PII e nao tem classificacao"


@pytest.mark.parametrize("nome", sorted(MODELOS))
def test_modelo_com_pii_nao_vai_para_o_rag(nome: str) -> None:
    """Coluna marcada como PII e politica de RAG que deixa publicar nao convivem."""
    modelo = MODELOS[nome][0]
    tem_pii = any(
        set((c.get("meta") or {}).get("openmetadata", {}).get("tags") or []) & TAGS_PII
        for c in modelo.get("columns") or []
    )
    if tem_pii:
        gov = _meta(modelo).get("governance") or {}
        assert (
            gov.get("rag_publication") == "prohibited"
        ), f"{nome}: tem PII e nao e prohibited"
