"""Guardas de governanca das silvers do SALIC.

A documentacao das silvers nao e enfeite: ela e o que o conector dbt leva para
o OpenMetadata e o que o exportador de RAG le para decidir o que pode sair
daqui. Um modelo sem politica declarada, ou uma coluna de CPF sem classificacao,
vira metadado publicado -- e nesse ponto o erro ja saiu do repositorio.

Estes testes cobrem a parte do plano (docs/openmetadata/MEMORY.md, secao 13) que
da para verificar offline. Os gates G1..G5, que dependem do banco, nao estao
aqui e nao sao substituidos por nenhum destes casos.
"""

import re
from pathlib import Path

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]
SILVER_DIR = REPO_ROOT / "dbt/minc/models/salic_dbt"
GLOSSARIO_CSV = REPO_ROOT / "helpers/openmetadata/glossaries/minc.csv"

DOMINIO_ESPERADO = "Cultura.Incentivo Fiscal"
DONO_ESPERADO = "minc-data-engineering"
POLITICAS_RAG = {
    "prohibited",
    "eligible_after_security_validation",
    "determined_per_model",
}

# Nomes de coluna que nao podem sair sem classificacao de PII. Vem da lista do
# plano: documento, nome, endereco, contato, conta bancaria, logon e IP -- mais
# as chaves tecnicas, que identificam pessoa indiretamente e por isso contam.
PADROES_PII = re.compile(
    r"(documento|cpf|cnpj|nome_proponente|nome_agente|nome_prestador|nome_fornecedor"
    r"|endereco|cep|telefone|email|raca|cor|etnia|indigena|deficiencia|pcd"
    r"|conta|agencia|logon|usuario|_ip$|id_agente|id_solicitante|id_prestador)",
    re.IGNORECASE,
)

TAGS_PII = {"PII.Sensitive", "PII.NonSensitive"}


def _modelos_documentados() -> dict[str, dict]:
    """Todo bloco `models:` dos schema.yml da silver, indexado por nome."""
    modelos: dict[str, dict] = {}
    for caminho in SILVER_DIR.rglob("*.yml"):
        conteudo = yaml.safe_load(caminho.read_text(encoding="utf-8")) or {}
        for modelo in conteudo.get("models") or []:
            nome = modelo["name"]
            assert nome not in modelos, f"{nome} documentado em mais de um schema.yml"
            modelos[nome] = modelo
    return modelos


def _sqls_da_silver() -> list[Path]:
    return sorted(SILVER_DIR.rglob("*.sql"))


def _governanca(modelo: dict) -> dict:
    return (modelo.get("config") or {}).get("meta", {}).get("governance", {}) or {}


def _openmetadata(modelo: dict) -> dict:
    return (modelo.get("config") or {}).get("meta", {}).get("openmetadata", {}) or {}


def _tags_om(entidade: dict) -> set[str]:
    return set((entidade.get("meta") or {}).get("openmetadata", {}).get("tags") or [])


MODELOS = _modelos_documentados()
SQLS = _sqls_da_silver()
NOMES_SQL = [caminho.stem for caminho in SQLS]


def test_ha_modelos_para_verificar() -> None:
    """Guarda do proprio arquivo: teste que nao coleta nada passa por engano."""
    assert SQLS, "nenhum .sql encontrado em models/salic_dbt"


@pytest.mark.parametrize("nome", NOMES_SQL)
def test_todo_sql_tem_bloco_de_documentacao(nome: str) -> None:
    assert nome in MODELOS, f"{nome}.sql nao aparece em nenhum schema.yml da silver"


def test_nenhum_bloco_documenta_modelo_inexistente() -> None:
    orfaos = sorted(set(MODELOS) - set(NOMES_SQL))
    assert not orfaos, f"documentados sem .sql correspondente: {orfaos}"


@pytest.mark.parametrize("nome", sorted(MODELOS))
def test_modelo_declara_governanca_completa(nome: str) -> None:
    """Dominio, tier, dono, status e politica de RAG, todos explicitos.

    Ausencia nao pode significar default: o plano define que tabela sem
    declaracao e erro, e nao ativo publicavel.
    """
    modelo = MODELOS[nome]
    om = _openmetadata(modelo)
    gov = _governanca(modelo)
    meta = (modelo.get("config") or {}).get("meta", {})

    assert om.get("domain") == DOMINIO_ESPERADO, f"{nome}: dominio errado"
    assert om.get("owner") == DONO_ESPERADO, f"{nome}: dono errado"
    assert str(om.get("tier", "")).startswith("Tier.Tier"), f"{nome}: sem tier"
    assert meta.get("status") in {"Active", "Disabled"}, f"{nome}: sem status"
    assert gov.get("rag_publication") in POLITICAS_RAG, f"{nome}: sem politica de RAG"
    assert gov.get("produto_dados"), f"{nome}: sem produto de dados"
    assert gov.get("classificacao_uso"), f"{nome}: sem classificacao de uso"


@pytest.mark.parametrize("nome", sorted(MODELOS))
def test_salic_nunca_vai_para_fomento_direto(nome: str) -> None:
    """O SALIC e incentivo fiscal. Fomento direto e LPG/PNAB, outro dominio.

    A troca ja aconteceu em anotador automatico e nao produz erro nenhum -- so
    um catalogo que agrupa politicas incompativeis sob o mesmo guarda-chuva.
    """
    assert _openmetadata(MODELOS[nome]).get("domain") != "Cultura.Fomento Direto"


@pytest.mark.parametrize("nome", sorted(MODELOS))
def test_certificacao_silver_so_em_modelo_ativo(nome: str) -> None:
    """`Certification.Silver` e o selo de que os gates passaram."""
    modelo = MODELOS[nome]
    tags = set(_openmetadata(modelo).get("tags") or [])
    if "Certification.Silver" in tags:
        status = (modelo.get("config") or {}).get("meta", {}).get("status")
        assert status == "Active", f"{nome}: certificado mas ainda Disabled"


@pytest.mark.parametrize("nome", sorted(MODELOS))
def test_toda_coluna_tem_descricao_propria_e_tipo(nome: str) -> None:
    modelo = MODELOS[nome]
    colunas = modelo.get("columns") or []
    assert colunas, f"{nome}: nenhuma coluna documentada"
    for coluna in colunas:
        rotulo = f"{nome}.{coluna['name']}"
        descricao = (coluna.get("description") or "").strip()
        assert len(descricao) >= 20, f"{rotulo}: descricao ausente ou generica demais"
        assert coluna.get("data_type"), f"{rotulo}: sem data_type"


@pytest.mark.parametrize("nome", sorted(MODELOS))
def test_nenhuma_descricao_herda_o_marcador_da_bronze(nome: str) -> None:
    """A bronze marca o que o dicionario do SALIC nao documenta.

    Na silver esse marcador nao vale: o modelo e escrito aqui, e se a semantica
    nao esta clara o certo e nao publicar a coluna.
    """
    modelo = MODELOS[nome]
    textos = [modelo.get("description") or ""]
    textos += [c.get("description") or "" for c in modelo.get("columns") or []]
    for texto in textos:
        assert "[NAO VERIFICADO]" not in texto.upper()
        assert "NAO DOCUMENTADO NO DICIONARIO" not in texto.upper()


@pytest.mark.parametrize("nome", sorted(MODELOS))
def test_coluna_com_cara_de_pii_esta_classificada(nome: str) -> None:
    """Heuristica por nome, com dispensa declarada no proprio modelo.

    A dispensa existe porque ha coluna cujo nome parece identificador e cujo
    conteudo nao identifica ninguem -- `documento_valido` e um booleano. Ela
    fica no `schema.yml`, e nao na regex daqui, para aparecer no diff do modelo
    e nao no de um teste que ninguem revisa junto.
    """
    modelo = MODELOS[nome]
    dispensadas = set(_governanca(modelo).get("derivadas_nao_identificantes") or [])
    nomes = {coluna["name"] for coluna in modelo.get("columns") or []}
    orfas = sorted(dispensadas - nomes)
    assert not orfas, f"{nome}: dispensa de PII para coluna inexistente {orfas}"

    for coluna in modelo.get("columns") or []:
        if coluna["name"] in dispensadas or not PADROES_PII.search(coluna["name"]):
            continue
        tags = _tags_om(coluna)
        assert (
            tags & TAGS_PII
        ), f"{nome}.{coluna['name']} parece PII e nao tem classificacao"


@pytest.mark.parametrize("nome", sorted(MODELOS))
def test_modelo_publicavel_nao_carrega_pii(nome: str) -> None:
    """Fail-closed: qualquer PII na tabela derruba a elegibilidade do modelo.

    E a regra do plano -- tag de PII vence allowlist. Uma tabela com PII que se
    queira publicar em parte precisa ser dividida, nao anotada.
    """
    modelo = MODELOS[nome]
    if _governanca(modelo).get("rag_publication") == "prohibited":
        return
    with_pii = [
        coluna["name"]
        for coluna in modelo.get("columns") or []
        if _tags_om(coluna) & TAGS_PII
    ]
    assert not with_pii, f"{nome}: elegivel ao RAG com colunas de PII {with_pii}"


@pytest.mark.parametrize("nome", sorted(MODELOS))
def test_termos_de_glossario_existem_na_fonte_declarativa(nome: str) -> None:
    """FQN de termo inexistente nao falha a ingestao: ela ignora em silencio."""
    declarados = _fqns_do_glossario()
    modelo = MODELOS[nome]
    referenciados = list(_openmetadata(modelo).get("glossary") or [])
    for coluna in modelo.get("columns") or []:
        om_coluna = (coluna.get("meta") or {}).get("openmetadata", {})
        referenciados += list(om_coluna.get("glossary") or [])
    faltando = sorted(set(referenciados) - declarados)
    assert not faltando, f"{nome}: termos ausentes de minc.csv: {faltando}"


def _fqns_do_glossario() -> set[str]:
    """FQN de cada termo de `glossaries/minc.csv`, no formato `MinC.Raiz.Termo`.

    Leitura por `csv` de proposito: varias descricoes tem virgula dentro de
    aspas, e um `split(",")` produziria FQN truncado sem falhar.
    """
    import csv

    fqns = set()
    with GLOSSARIO_CSV.open(encoding="utf-8", newline="") as arquivo:
        for linha in csv.DictReader(arquivo):
            pai = (linha.get("parent") or "").strip()
            nome = (linha.get("name") or "").strip()
            if not nome:
                continue
            fqns.add(f"{pai}.{nome}" if pai else f"MinC.{nome}")
    return fqns


@pytest.mark.parametrize("caminho", SQLS, ids=NOMES_SQL)
def test_silver_le_a_bronze_por_ref_e_nunca_a_source_direto(caminho: Path) -> None:
    """A silver nao pode pular a camada de tipagem.

    `source()` aqui significaria ler texto cru do `salic_bronze` sem os casts
    guardados das macros `bronze_*` -- e a linhagem no OpenMetadata perderia o
    modelo intermediario.
    """
    sql = caminho.read_text(encoding="utf-8")
    assert "source(" not in sql, f"{caminho.name}: usa source() em vez de ref()"


@pytest.mark.parametrize("caminho", SQLS, ids=NOMES_SQL)
def test_modelo_restrito_nao_e_consumido_por_publicavel(caminho: Path) -> None:
    """Um modelo publicavel que le um restrito vaza pela porta dos fundos."""
    modelo = MODELOS.get(caminho.stem)
    if modelo is None:
        pytest.skip("modelo sem documentacao: coberto por outro teste")
    if _governanca(modelo).get("rag_publication") == "prohibited":
        return
    referenciados = set(
        re.findall(r"ref\(\s*[\"']([^\"']+)[\"']\s*\)", caminho.read_text())
    )
    restritos = sorted(
        nome
        for nome in referenciados
        if nome in MODELOS
        and _governanca(MODELOS[nome]).get("rag_publication") == "prohibited"
    )
    assert not restritos, f"{caminho.stem} e publicavel e le modelo restrito {restritos}"
