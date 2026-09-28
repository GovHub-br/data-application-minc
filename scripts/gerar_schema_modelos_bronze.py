#!/usr/bin/env python3
"""
Gera o schema.yml dos modelos da camada bronze do SALIC.

Os 571 modelos em dbt/minc/models/salic_dbt/bronze/<prefixo>/*.sql tipam as colunas
que chegam como texto da ingestao (macros bronze_*), mas nao tem nenhuma
documentacao: sem descricao, sem teste, sem tag. Este script gera um schema.yml
por diretorio, extraindo:

  - a lista de colunas e o tipo resultante, lendo os casts do proprio .sql
    (bronze_inteiro -> integer, bronze_timestamp -> timestamp, ...)
  - a descricao de negocio, do `remarks` do dicionario SchemaSpy da origem
  - tests not_null / unique, do perfil estatistico do catalogo do banco
  - tests relationships, das FKs declaradas no SQL Server (o lake nao tem
    constraint nenhuma) -- apontando para o MODELO equivalente, via ref()
  - tags PII de coluna para o OpenMetadata

CUIDADO com o regex de PII em portugues: `raca` como substring casa dentro de
operacao, duracao, administracao, alteracao, liberacao, procuracao, geracao e
declaracao. Por isso o padrao exige fronteira de palavra. Nao simplifique.

Uso:
    python3 scripts/gerar_schema_modelos_bronze.py \
        --modelos dbt/minc/models/salic_dbt/bronze \
        --catalogo catalogo_full.csv \
        --dicionarios <dir_xmls_schemaspy>
"""
import argparse
import collections
import csv
import glob
import os
import re
import xml.etree.ElementTree as ET

CAST_TIPO = {
    "bronze_inteiro": "integer",
    "bronze_numerico": "numeric",
    "bronze_texto": "text",
    "bronze_data": "date",
    "bronze_timestamp": "timestamp",
    "bronze_booleano": "boolean",
}

PII_SENSIVEL = re.compile(
    r"((^|[^a-z])raca([^a-z]|$)|cor_?raca|raca_?cor|etnia|religi|saude|"
    r"deficien|biometr|orientacaosexual)", re.I)
PII_COMUM = re.compile(
    r"(cpf|cnpj|\brg\b|identidade|nome|nasc|email|e_mail|mail|telefone|fone|"
    r"celular|cep|endereco|logradouro|bairro|senha|password|token|renda|"
    r"salario|passaporte|\bpis\b|\bnit\b)", re.I)

RE_COL = re.compile(r"\{\{\s*(bronze_\w+)\(\s*\"([^\"]+)\"\s*\)\s*\}\}\s*as\s+(\w+)")
RE_SRC = re.compile(r"source\(\s*\"([^\"]+)\"\s*,\s*\"([^\"]+)\"\s*\)")


def dobrar(texto, largura=94, recuo=""):
    saida, linha = [], recuo
    for p in texto.split():
        if len(linha) + len(p) + 1 > largura and linha.strip():
            saida.append(linha.rstrip())
            linha = recuo + p + " "
        else:
            linha += p + " "
    if linha.strip():
        saida.append(linha.rstrip())
    return saida


def ler_dicionario(dirs):
    rt, rc, fks = {}, {}, {}
    xmls = []
    for d in dirs or []:
        xmls += glob.glob(os.path.join(d, "**", "*.xml"), recursive=True)
    for x in sorted(set(xmls)):
        try:
            raiz = ET.parse(x).getroot()
        except ET.ParseError:
            continue
        for t in raiz.iter("table"):
            cat = (t.get("catalog") or "").lower()
            tab = f"{cat}__{t.get('name')}".lower()
            if (t.get("remarks") or "").strip():
                rt[tab] = " ".join(t.get("remarks").split())
            for c in t.findall("column"):
                col = (c.get("name") or "").lower()
                if (c.get("remarks") or "").strip():
                    rc[(tab, col)] = " ".join(c.get("remarks").split())
                for p in c:
                    if p.tag == "parent":
                        pcat = (p.get("catalog") or cat).lower()
                        fks[(tab, col)] = (f"{pcat}__{p.get('table')}".lower(),
                                           (p.get("column") or "").lower())
                        break
    return rt, rc, fks


def ler_perfil(caminho):
    """(tabela, coluna) -> dict com null_frac, n_distinct, table_rows"""
    if not caminho or not os.path.exists(caminho):
        return {}
    p = {}
    for r in csv.DictReader(open(caminho, encoding="utf-8")):
        if r["schema_name"] != "salic_bronze":
            continue
        p[(r["table_name"], r["column_name"])] = r
    return p


def ler_modelo(caminho):
    txt = open(caminho, encoding="utf-8").read()
    cols = [(m.group(3).lower(), CAST_TIPO.get(m.group(1), "text"))
            for m in RE_COL.finditer(txt)]
    if "_fatia" in txt and not any(c[0] == "_fatia" for c in cols):
        cols.append(("_fatia", "integer"))
    src = RE_SRC.search(txt)
    return cols, (src.group(2).lower() if src else None)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--modelos", required=True)
    p.add_argument("--catalogo")
    p.add_argument("--dicionarios", nargs="*")
    a = p.parse_args()

    rt, rc, fks = ler_dicionario(a.dicionarios)
    perfil = ler_perfil(a.catalogo)
    print(f"dicionario: {len(rt)} tabelas, {len(rc)} colunas, {len(fks)} FKs")
    print(f"perfil do banco: {len(perfil)} colunas\n")

    dirs = sorted(d for d in glob.glob(os.path.join(a.modelos, "*"))
                  if os.path.isdir(d))
    modelos_existentes = {os.path.basename(f)[:-4]
                          for d in dirs for f in glob.glob(os.path.join(d, "*.sql"))}

    tot = collections.Counter()
    print(f"{'diretorio':22} {'modelos':>8} {'colunas':>8} {'desc':>6} {'rel':>5} {'PII':>5}")
    for d in dirs:
        sqls = sorted(glob.glob(os.path.join(d, "*.sql")))
        if not sqls:
            continue
        L = ["version: 2", "", "models:"]
        st = collections.Counter()
        for s in sqls:
            nome = os.path.basename(s)[:-4]
            cols, origem = ler_modelo(s)
            origem = origem or nome
            L.append(f"  - name: {nome}")
            L.append("    description: >")
            desc = rt.get(origem)
            if desc:
                st["desc"] += 1
            else:
                desc = ("[NÃO VERIFICADO] Objeto do SALIC ausente do dicionário de dados "
                        "original (export SchemaSpy). Semântica e grão ainda não confirmados.")
            L += dobrar(f"{desc} Camada bronze tipada a partir de "
                        f"salic_bronze.{origem}.", 94, "      ")
            L.append("    columns:")
            for col, tipo in cols:
                L.append(f"      - name: {col}")
                L.append("        description: >")
                if col == "_fatia":
                    d_col = ("Coluna técnica da ingestão: identifica a fatia de extração "
                             "que trouxe a linha. Não é campo de negócio.")
                else:
                    d_col = rc.get((origem, col)) or \
                        "[NÃO VERIFICADO] Coluna ausente do dicionário de dados original."
                L += dobrar(d_col, 94, "          ")
                L.append(f"        data_type: {tipo}")

                tg = ("PII.Sensitive" if PII_SENSIVEL.search(col)
                      else ("PII.NonSensitive" if PII_COMUM.search(col) else None))
                if tg and col != "_fatia":
                    L.append("        meta:")
                    L.append("          openmetadata:")
                    L.append("            tags:")
                    L.append(f"              - {tg}")
                    st["pii"] += 1

                # testes a partir do perfil real do banco
                ts, inferido = [], set()
                r = perfil.get((origem, col))
                if r and col != "_fatia":
                    linhas = int(float(r["table_rows"] or 0))
                    try:
                        nf = float(r["null_frac"]) if r["null_frac"] else None
                        nd = float(r["n_distinct"]) if r["n_distinct"] else None
                    except ValueError:
                        nf = nd = None
                    if nf == 0.0 and linhas > 0:
                        ts.append("not_null")
                        inferido.add("not_null")
                    if nd == -1.0 and linhas > 0:
                        ts.append("unique")
                        inferido.add("unique")
                fk = fks.get((origem, col))
                alvo = fk[0] if fk and fk[0] in modelos_existentes else None
                if ts or alvo:
                    L.append("        tests:")
                    for t in ts:
                        L.append(f"          - {t}"
                                 + ("   # inferido do perfil do banco" if t in inferido else ""))
                        st[t] += 1
                    if alvo:
                        L.append("          - relationships:")
                        L.append("              arguments:")
                        L.append(f"                to: ref('{alvo}')")
                        L.append(f"                field: {fk[1]}")
                        st["rel"] += 1
            st["colunas"] += len(cols)
            st["modelos"] += 1
            L.append("")
        open(os.path.join(d, "schema.yml"), "w", encoding="utf-8").write("\n".join(L) + "\n")
        tot.update(st)
        print(f"{os.path.basename(d):22} {st['modelos']:8} {st['colunas']:8} "
              f"{st['desc']:6} {st['rel']:5} {st['pii']:5}")
    print(f"\n{'TOTAL':22} {tot['modelos']:8} {tot['colunas']:8} "
          f"{tot['desc']:6} {tot['rel']:5} {tot['pii']:5}")
    print(f"\ntests: not_null={tot['not_null']} unique={tot['unique']} "
          f"relationships={tot['rel']}")


if __name__ == "__main__":
    main()
