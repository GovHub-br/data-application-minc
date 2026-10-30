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
import unicodedata
from pathlib import Path

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]
# A estrutura do ADR 0010: as silvers migradas moram em models/salic/, pasta
# plana, documentadas em schema.yml e schema_<banco>.yml. As sources_*.yml da
# mesma pasta declaram a Bronze e nao tem bloco `models:`.
SALIC_NOVO_DIR = REPO_ROOT / "dbt/minc/models/salic"
GLOSSARIO_CSV = REPO_ROOT / "helpers/openmetadata/glossaries/minc.csv"


def _arquivos(padrao: str) -> list[Path]:
    """Os arquivos da silver do SALIC: a pasta plana models/salic/."""
    return sorted(SALIC_NOVO_DIR.glob(padrao))


def _sem_acento(texto: str) -> str:
    return "".join(
        c for c in unicodedata.normalize("NFD", texto) if not unicodedata.combining(c)
    )


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

# Um modelo publicavel pode ler uma silver restrita, mas cada coluna pessoal
# dela que o SQL usa tem de estar declarada em
# `governance.pii_lido_sem_expor` ("silver.coluna") -- e nenhuma coluna de
# saida do modelo pode ter tag de PII (test_modelo_publicavel_nao_carrega_pii).
# Usar CPF para derivar o tipo de pessoa e legitimo; copia-lo para a saida nao.
CHAVE_PII_LIDO = "pii_lido_sem_expor"


def _modelos_documentados() -> dict[str, dict]:
    """Todo bloco `models:` dos schema.yml da silver, indexado por nome."""
    modelos: dict[str, dict] = {}
    for caminho in _arquivos("*.yml"):
        conteudo = yaml.safe_load(caminho.read_text(encoding="utf-8")) or {}
        for modelo in conteudo.get("models") or []:
            nome = modelo["name"]
            assert nome not in modelos, f"{nome} documentado em mais de um schema.yml"
            modelos[nome] = modelo
    return modelos


def _sqls_da_silver() -> list[Path]:
    return _arquivos("*.sql")


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
    assert SQLS, "nenhum .sql de silver encontrado em models/salic"
    assert not any(
        "bronze" in c.parts for c in SQLS
    ), "a bronze entrou no escopo destas guardas; ela tem contrato proprio"


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
        # sem acento: o marcador aparece como [NÃO VERIFICADO] e [NAO VERIFICADO]
        normalizado = _sem_acento(texto).upper()
        assert "[NAO VERIFICADO]" not in normalizado
        assert "NAO DOCUMENTADO NO DICIONARIO" not in normalizado


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


RE_SOURCE = re.compile(r"source\(\s*[\"']([^\"']+)[\"']\s*,\s*[\"']([^\"']+)[\"']\s*\)")


@pytest.mark.parametrize("caminho", SQLS, ids=NOMES_SQL)
def test_silver_le_so_a_bronze_do_salic(caminho: Path) -> None:
    """A silver le a Bronze da ingestao e mais nada por source().

    Em models/salic/ (ADR 0017) a Bronze e a tabela salic.bronze_<banco>_<tabela>
    que a DAG grava, declarada como source `salic`; a silver le dela e tipa com
    as macros `bronze_*`. No legado salic_dbt/, as silvers leem os modelos da
    bronze por ref() e nunca por source().
    """
    sql = caminho.read_text(encoding="utf-8")
    fontes = RE_SOURCE.findall(sql)
    if caminho.is_relative_to(SALIC_NOVO_DIR):
        fora = [f for f in fontes if f[0] != "salic" or not f[1].startswith("bronze_")]
        assert not fora, f"{caminho.name}: source() fora da Bronze do SALIC {fora}"
    else:
        assert "source(" not in sql, f"{caminho.name}: usa source() em vez de ref()"


def _restritos_lidos(caminho: Path) -> list[str]:
    referenciados = set(
        re.findall(r"ref\(\s*[\"']([^\"']+)[\"']\s*\)", caminho.read_text())
    )
    return sorted(
        nome
        for nome in referenciados
        if nome in MODELOS
        and _governanca(MODELOS[nome]).get("rag_publication") == "prohibited"
    )


def _pii_usado(caminho: Path) -> set[str]:
    """`silver.coluna` de cada coluna com tag de PII das silvers restritas que
    o SQL do modelo menciona fora de comentario."""
    sem_comentario = re.sub(r"--[^\n]*", "", caminho.read_text())
    usado = set()
    for restrito in _restritos_lidos(caminho):
        for coluna in MODELOS[restrito].get("columns") or []:
            if _tags_om(coluna) & TAGS_PII and re.search(
                rf"\b{re.escape(coluna['name'])}\b", sem_comentario
            ):
                usado.add(f"{restrito}.{coluna['name']}")
    return usado


@pytest.mark.parametrize("caminho", SQLS, ids=NOMES_SQL)
def test_publicavel_so_le_pii_declarado(caminho: Path) -> None:
    """Um modelo publicavel que le um restrito pode vazar pela porta dos fundos.

    Ler a silver restrita e permitido; usar uma coluna pessoal dela, so se
    estiver declarada no modelo, e a declaracao nao pode ficar velha.
    """
    modelo = MODELOS.get(caminho.stem)
    if modelo is None:
        pytest.skip("modelo sem documentacao: coberto por outro teste")
    gov = _governanca(modelo)
    declarado = set(gov.get(CHAVE_PII_LIDO) or [])
    if gov.get("rag_publication") == "prohibited":
        assert not declarado, f"{caminho.stem}: restrito nao precisa de {CHAVE_PII_LIDO}"
        return
    usado = _pii_usado(caminho)
    assert (
        not usado - declarado
    ), f"{caminho.stem} e publicavel e usa PII nao declarada {sorted(usado - declarado)}"
    assert not declarado - usado, (
        f"{caminho.stem}: {CHAVE_PII_LIDO} declara o que o SQL nao usa "
        f"{sorted(declarado - usado)}"
    )


@pytest.mark.parametrize("nome", sorted(MODELOS))
def test_accepted_values_sem_aspas_so_em_coluna_numerica(nome: str) -> None:
    """``quote: false`` compara a coluna com número; em coluna texto o Postgres
    recusa a comparação e o teste dá ERROR no build, não FAIL."""
    for coluna in MODELOS[nome].get("columns") or []:
        for teste in coluna.get("tests") or []:
            if not isinstance(teste, dict) or "accepted_values" not in teste:
                continue
            argumentos = teste["accepted_values"].get("arguments") or {}
            if argumentos.get("quote") is False:
                assert (
                    coluna.get("data_type") != "text"
                ), f"{nome}.{coluna['name']}: accepted_values sem aspas em coluna text"


@pytest.mark.parametrize("caminho", SQLS, ids=NOMES_SQL)
def test_toda_coluna_do_select_esta_documentada(caminho: Path) -> None:
    """Coluna que a silver entrega e o schema.yml não lista some do catálogo.

    Vale para as silvers de models/salic/, geradas a partir do SQL. O sqlfmt
    pode quebrar a linha entre ``}}`` e ``as``, por isso o \\s+.
    """
    if not caminho.is_relative_to(SALIC_NOVO_DIR) or not caminho.stem.startswith(
        "silver_"
    ):
        pytest.skip("só as silvers geradas de models/salic/")
    modelo = MODELOS.get(caminho.stem)
    if modelo is None:
        pytest.skip("modelo sem documentacao: coberto por outro teste")
    sql = caminho.read_text()
    if not sql.startswith("-- Silver SALIC — "):
        pytest.skip("silver escrita à mão, fora da migração por lote")
    corpo = sql[sql.index("select") :]
    no_sql = set(re.findall(r"\}\}\s+as\s+([a-z0-9_]+)", corpo))
    if re.search(r"^\s*_fatia\b", corpo, re.M):
        no_sql.add("_fatia")
    documentadas = {c["name"] for c in modelo.get("columns") or []}
    faltam = sorted(no_sql - documentadas)
    assert not faltam, f"{caminho.stem}: colunas sem documentacao {faltam}"
