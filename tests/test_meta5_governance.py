"""Guardas de governanca da cadeia da Meta 5 no ADR 0010.

As silvers do SALIC em salic_dbt seguem um padrao de documentacao feito para o
RAG e para o OpenMetadata (ver test_salic_silver_governance). A Meta 5
refatorada substitui a de la, entao a cadeia dela -- das silvers das fontes as
golds do Cultura em Numeros -- precisa declarar a mesma politica: sem isso, o
exportador de RAG nao tem como decidir o que pode sair, e ausencia nao pode
significar "publicavel".

A bronze fica de fora pelo mesmo motivo de la: e copia fiel, com outro
contrato.
"""

import re
from pathlib import Path

import pytest
import yaml

from tests.test_salic_silver_governance import PADROES_PII, POLITICAS_RAG, TAGS_PII

MODELS_DIR = Path(__file__).resolve().parents[1] / "dbt/minc/models"

# As pastas que a Meta 5 atravessa. Pasta nova na cadeia entra aqui no mesmo PR.
PASTAS_META5 = [
    "salic",
    "bbagil",
    "ancine",
    "transferegov",
    "intermediate",
    "data_products/cultura_em_numeros",
]

# O formato que o RAG le: secoes nomeadas, e nao um paragrafo corrido.
SECOES_OBRIGATORIAS = ["GRÃO:", "UNIVERSO:", "PARA QUE SERVE:", "ATUALIZAÇÃO:"]

DESABILITADO = re.compile(r"config\([^)]*enabled\s*=\s*false", re.IGNORECASE)


def _modelos() -> dict[str, tuple[dict, Path]]:
    """Bloco `models:` de cada schema.yml da cadeia, sem a bronze."""
    modelos: dict[str, tuple[dict, Path]] = {}
    for pasta in PASTAS_META5:
        for caminho in sorted((MODELS_DIR / pasta).glob("*.yml")):
            conteudo = yaml.safe_load(caminho.read_text(encoding="utf-8")) or {}
            for modelo in conteudo.get("models") or []:
                if modelo["name"].startswith("bronze_"):
                    continue
                modelos[modelo["name"]] = (modelo, caminho.parent)
    return modelos


def _meta(modelo: dict) -> dict:
    return (modelo.get("config") or {}).get("meta", {}) or {}


MODELOS = _modelos()


def test_ha_modelos_para_verificar() -> None:
    assert len(MODELOS) >= 18, "a cadeia da Meta 5 encolheu ou a coleta quebrou"


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
