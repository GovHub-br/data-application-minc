#!/usr/bin/env python3
"""
Classifica colunas com dado pessoal segundo a taxonomia do OpenMetadata.

Taxonomia adotada (a do OM, nao a da LGPD art. 5o II):

  PII.Sensitive     documento, conta bancaria, cartao, credencial, biometria,
                    e os dados que a LGPD chama de sensiveis (raca, saude,
                    religiao, deficiencia). Perda direta ou reidentificacao.
  PII.NonSensitive  nome, endereco, telefone, e-mail, data de nascimento.
                    Dado pessoal, risco menor isolado.

Por que a do OM e nao a da LGPD: e a que a ferramenta espera e a que outras
integracoes assumem. Na LGPD, CPF nao e "dado sensivel" -- mas CPF junto de
conta bancaria merece o tratamento mais restrito de qualquer forma.

CUIDADO com regex em portugues: `raca` como substring casa dentro de operacao,
duracao, administracao, alteracao, liberacao, procuracao, geracao e
declaracao. Por isso exige fronteira de palavra.

CUIDADO com fonte em ingles: o bbagil vem da API do Banco do Brasil com nomes
como `beneficiaryname` e `beneficiarydocumentid`. Um padrao so em portugues
passa reto por 176 mil CPFs. Os dois idiomas estao cobertos abaixo.

Uso:
    python3 scripts/classificar_pii.py --arquivos dbt/minc/models/**/*.yml
    python3 scripts/classificar_pii.py --arquivos ... --dry-run
"""
import argparse
import collections
import glob
import re

SENSIVEL = re.compile(
    r"("
    # documento e identificacao
    r"cpf|cnpj|\brg\b|\bie\b|inscricao_?estadual|identidade|passaporte|"
    r"titulo_?eleitor|\bpis\b|\bnit\b|\bctps\b|cnh\b|documentid|document_?id|"
    r"taxid|tax_?id|ssn|"
    # bancario
    r"conta_?banc|contabanc|accountnumber|account_?number|numero_?conta|"
    r"agencia|branchcode|branch_?code|cartao|creditcard|credit_?card|iban|"
    r"\bpix\b|chave_?pix|"
    # credencial
    r"senha|password|token|apikey|api_?key|client_?secret|(^|_)secret($|_)|"
    r"(^|_)login($|_)|auditlogin|audit_?login|"
    # LGPD art. 5o II
    r"(^|[^a-z])raca([^a-z]|$)|cor_?raca|raca_?cor|etnia|religi|saude|health|"
    r"laudo|deficien|disabilit|\bpcd\b|(^|_)pcd($|_)|indigena|quilombola|"
    r"biometr|orientacaosexual|"
    # chave de identificacao do projeto: e CPF ou CNPJ normalizado
    r"identificador_unico|documento_?raw|(^|_)documento($|_)"
    r")", re.I)

NAO_SENSIVEL = re.compile(
    r"("
    r"nome|\bname\b|nom_|_nome|fullname|firstname|lastname|sobrenome|"
    r"email|e_mail|\bmail\b|correio_?eletronico|"
    r"telefone|\bfone\b|celular|phone|mobile|"
    r"(^|_)cep\d*($|_)|cep$|zipcode|zip_?code|endereco|address|logradouro|bairro|"
    r"nasc|birth|dt_?nascimento|"
    r"personname|beneficiaryname|payername|holdername|^nm[a-z]{3}"
    r")", re.I)

# nomes que casam nos padroes acima mas nao sao pessoa natural.
# Verificado com a mascara do dado -- ver scripts/perfilar_colunas.py.
# Nomes que casam nos padroes acima mas se referem a ENTIDADE, nao a pessoa
# natural: projeto, programa, orgao, municipio, sistema, arquivo. Verificado
# com a mascara do dado -- ver scripts/perfilar_colunas.py.
#
# A lista e deliberadamente estreita. Na duvida a coluna FICA marcada: tag a
# mais atrapalha a governanca, tag a menos deixa dado pessoal invisivel, e o
# segundo erro e o caro. Um `nome` solto continua sendo pessoa ate prova em
# contrario.
_ENTIDADE = (r"projeto|programa|subprograma|fundo|orgao|org|ente|municipio|"
             r"banco|meta|arquivo|file|attachment|tabela|coluna|campo|"
             r"sistema|sis|relatorio|edital|tipo|situacao|status|categoria|"
             r"segmento|area|setor|unidade|instituicao|empresarial|fantasia|"
             r"grupo|gru|log|mensagem|men|loc|pesquisa|government|purpose|"
             r"rule|description|institucional|gestao")

EXCECOES = re.compile(
    rf"(^({_ENTIDADE})_?nome|nome_?({_ENTIDADE})|({_ENTIDADE})name|"
    rf"^p?nome({_ENTIDADE})|^nm({_ENTIDADE})|nm(empresarial|fantasia))$", re.I)


def _normaliza(nome):
    """Normaliza o nome para casar os padroes.

    Tabela vinda de planilha traz o cabecalho do Excel como nome de coluna:
    aspas, espaco, acento e pontuacao. Sem normalizar, `"CNPJ Proponente"`
    nao casa com o padrao `cnpj` e a coluna passa sem classificacao.
    """
    n = nome.strip().strip('"').strip("'").lower()
    n = (n.replace("ç", "c").replace("ã", "a").replace("á", "a")
           .replace("â", "a").replace("é", "e").replace("ê", "e")
           .replace("í", "i").replace("ó", "o").replace("ô", "o")
           .replace("õ", "o").replace("ú", "u"))
    return re.sub(r"[^a-z0-9]+", "_", n).strip("_")


def classificar(coluna, tabela=""):
    c = _normaliza(coluna)
    if EXCECOES.search(c):
        return None
    if SENSIVEL.search(c):
        return "PII.Sensitive"
    if NAO_SENSIVEL.search(c):
        return "PII.NonSensitive"
    return None


def processar(caminho, dry):
    linhas = open(caminho, encoding="utf-8").read().split("\n")
    saida, i, n = [], 0, len(linhas)
    st = collections.Counter()
    tabela = ""

    # indentacao do `columns:` aberto no momento; None = fora de bloco de coluna.
    # Rastrear em vez de usar limiar fixo: em sources.yml a coluna fica em 10
    # espacos, no schema.yml dos modelos fica em 6.
    col_indent = None

    while i < n:
        l = linhas[i]
        indent = len(l) - len(l.lstrip())
        if l.strip() == "columns:":
            col_indent = indent
        elif l.strip() and col_indent is not None and indent <= col_indent \
                and l.strip() != "columns:":
            col_indent = None
        mt = re.match(r"^\s+- name: (.+?)\s*$", l)
        eh_coluna = bool(mt) and col_indent is not None and indent == col_indent + 2
        if not eh_coluna:
            saida.append(l)
            i += 1
            continue

        coluna = mt.group(1)
        bloco = [l]
        j = i + 1
        while j < n and (not linhas[j].strip() or
                         len(linhas[j]) - len(linhas[j].lstrip()) > indent):
            bloco.append(linhas[j])
            j += 1

        alvo = classificar(coluna, tabela)
        texto = "\n".join(bloco)
        # o projeto usa as duas formas: bloco (`- PII.X`) e inline
        # (`tags: ["PII.X"]`). Reconhecer so uma cria tag duplicada.
        atual = re.search(r"(PII\.\w+)", texto)
        atual = atual.group(1) if atual else None

        # Nunca remover tag existente. `identificador_unico`, `pcd_bruto` e
        # `quilombola_bruto` foram marcados a mao e nao casam com padrao
        # nenhum -- so quem conhece o dominio sabe o que sao. Tirar por
        # regex desfaz decisao humana em silencio, e o erro e caro.
        # Rebaixar Sensitive para NonSensitive tambem nao: so promover.
        if atual and not alvo:
            alvo = atual
        if atual == "PII.Sensitive" and alvo == "PII.NonSensitive":
            alvo = atual

        if alvo != atual:
            # remove bloco meta que so tenha PII
            novo, k = [], 0
            while k < len(bloco):
                if (bloco[k].strip() == "meta:" and "PII." in "\n".join(bloco[k:k + 6])):
                    m = k + 1
                    while m < len(bloco) and (len(bloco[m]) - len(bloco[m].lstrip())
                                              > len(bloco[k]) - len(bloco[k].lstrip())):
                        m += 1
                    resto = [x for x in bloco[k:m] if x.strip() and not re.match(
                        r"^(meta:|openmetadata:|tags:|- PII\.\w+)$", x.strip())]
                    if not resto:
                        k = m
                        continue
                novo.append(bloco[k])
                k += 1
            bloco = novo
            if alvo:
                ind = " " * (indent + 2)
                # Se a coluna JA tem um bloco meta.openmetadata (com glossary,
                # customProperties...), a tag entra dentro dele. Criar um
                # segundo `meta:` gera chave duplicada, que o dbt aceita hoje
                # com aviso de deprecacao e um dos dois silenciosamente vence.
                i_om = next((x for x, y in enumerate(bloco)
                             if y.strip() == "openmetadata:"), None)
                if i_om is not None:
                    i_tags = next(
                        (x for x in range(i_om + 1, len(bloco))
                         if re.match(r"^\s*tags:\s*$", bloco[x])
                         and len(bloco[x]) - len(bloco[x].lstrip())
                         == len(bloco[i_om]) - len(bloco[i_om].lstrip()) + 2), None)
                    if i_tags is not None:
                        bloco.insert(i_tags + 1, f"{bloco[i_tags]}  - {alvo}"
                                     .replace("tags:", "").replace("  - ", "  - ", 1))
                        bloco[i_tags + 1] = (" " * (len(bloco[i_tags])
                                                    - len(bloco[i_tags].lstrip()) + 2)
                                             + f"- {alvo}")
                    else:
                        ind_om = " " * (len(bloco[i_om]) - len(bloco[i_om].lstrip()))
                        bloco.insert(i_om + 1, f"{ind_om}  tags:")
                        bloco.insert(i_om + 2, f"{ind_om}    - {alvo}")
                else:
                    idx = next((x for x, y in enumerate(bloco)
                                if y.strip() == "tests:"), len(bloco))
                    while idx > 0 and not bloco[idx - 1].strip():
                        idx -= 1
                    bloco = bloco[:idx] + [
                        f"{ind}meta:", f"{ind}  openmetadata:",
                        f"{ind}    tags:", f"{ind}      - {alvo}"] + bloco[idx:]
            st[f"{atual or 'nenhuma'} -> {alvo or 'nenhuma'}"] += 1

        saida += bloco
        i = j

    if not dry:
        open(caminho, "w", encoding="utf-8").write("\n".join(saida))
    return st


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--arquivos", nargs="+", required=True)
    p.add_argument("--dry-run", action="store_true")
    a = p.parse_args()
    tot = collections.Counter()
    for padrao in a.arquivos:
        for f in sorted(glob.glob(padrao, recursive=True)):
            st = processar(f, a.dry_run)
            if st:
                tot.update(st)
                print(f"{f.split('models/')[-1]:52} {sum(st.values()):5} mudancas")
    print("\n=== transicoes ===")
    for k, v in sorted(tot.items(), key=lambda x: -x[1]):
        print(f"  {k:44} {v}")
    if a.dry_run:
        print("\ndry-run: nada gravado")


if __name__ == "__main__":
    main()
