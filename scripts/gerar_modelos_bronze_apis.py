#!/usr/bin/env python3
"""
Gera os modelos bronze e silver das fontes de API que nao vem do SALIC.

Essas fontes chegam 100% como `text` -- pior que o SALIC, que ao menos tipa
a coluna tecnica. Na estrutura do ADR 0010 cada tabela vira um par na pasta
da fonte: bronze_<tabela>, copia fiel (select * da source), e
silver_<tabela>, que tipa com as macros `bronze_*` lendo a bronze por ref().

O cast NAO e inferido pelo nome da coluna: e decidido pelo padrao medido em
scripts/perfilar_padroes.py, exigindo que 100% dos valores preenchidos casem.
Na duvida fica `bronze_texto` -- as macros convertem valor fora do padrao em
NULL, e coluna tipada errado vira nulo silencioso, que e pior que texto.

Duas travas explicitas:
  - zero a esquerda nunca vira inteiro (codigo de banco, CEP, IBGE)
  - coluna sempre nula ou constante vira texto, sem cast

O SQL vai para models/<fonte>/. O schema.yml NAO: as pastas de fonte ja tem
descricao escrita a mao, e o gerador nao pode sobrescreve-la. Os blocos vao
para --yaml-saida/<fonte>.yml, fora de models/ (senao o dbt le o mesmo
modelo duas vezes), e quem roda junta no schema.yml da pasta.

Uso:
    python3 scripts/gerar_modelos_bronze_apis.py \\
        --perfil perfil.json --padroes padroes.json \\
        --schemas transferegov ibge_sidra bacen --yaml-saida /tmp/gerado
"""
import argparse
import collections
import json
import os
import re

MACRO = {
    "inteiro": "bronze_inteiro", "decimal": "bronze_numerico",
    "data": "bronze_data", "timestamp": "bronze_timestamp",
    "booleano": "bronze_booleano", "texto": "bronze_texto",
}
TIPO_SQL = {
    "bronze_inteiro": "integer", "bronze_numerico": "numeric",
    "bronze_data": "date", "bronze_timestamp": "timestamp",
    "bronze_booleano": "boolean", "bronze_texto": "text",
}
DOMINIO = {
    "transferegov": "Cultura.Repasse Federativo",
    "bbagil": "Cultura.Repasse Federativo",
    "bacen": "Cultura.Indicadores",
    "ibge_sidra": "Cultura.Indicadores",
}


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


def decidir(pad, perf):
    """Escolhe a macro de cast. Exige 100% dos preenchidos casando."""
    if not pad:
        return "bronze_texto", "sem padrao medido"
    pre = pad["preenchidos"]
    if pre == 0:
        return "bronze_texto", "coluna sempre nula"
    if perf and perf.get("distintos") == 1:
        return "bronze_texto", "coluna constante"
    if pad.get("nan"):
        return "bronze_texto", f"{pad['nan']} valores 'NaN' gravados como texto"
    if pad.get("zero_esq"):
        return "bronze_texto", "zero a esquerda: tipar como inteiro perderia o dado"
    for chave in ("booleano", "timestamp", "data", "inteiro", "decimal"):
        if pad.get(chave, 0) == pre:
            return MACRO[chave], f"100% dos {pre} valores casam com {chave}"
    return "bronze_texto", "valores heterogeneos"


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--perfil", required=True)
    p.add_argument("--padroes", required=True)
    p.add_argument("--schemas", nargs="+", required=True)
    p.add_argument("--catalogo",
                   help="CSV do catalogo; usado como fallback para tabela sem perfil "
                        "medido (ex.: sem permissao de leitura), que entra toda como texto")
    p.add_argument("--saida", default="dbt/minc/models")
    p.add_argument("--yaml-saida", required=True,
                   help="pasta, fora de models/, onde gravar <fonte>.yml com os "
                        "blocos para juntar no schema.yml da pasta da fonte")
    a = p.parse_args()

    perfil = json.load(open(a.perfil, encoding="utf-8"))
    padroes = json.load(open(a.padroes, encoding="utf-8"))

    cols = collections.defaultdict(list)
    for k in padroes:
        s, t, c = k.split(".", 2)
        if s in a.schemas:
            cols[(s, t)].append(c)

    # tabela sem perfil (tipicamente sem permissao de leitura) ainda precisa de
    # modelo, senao a camada bronze fica com buraco silencioso
    sem_perfil = set()
    if a.catalogo:
        import csv as _csv
        com_perfil = set(cols)          # fixa antes de mexer em cols
        for r in _csv.DictReader(open(a.catalogo, encoding="utf-8")):
            s2, t2 = r["schema_name"], r["table_name"]
            if s2 in a.schemas and (s2, t2) not in com_perfil:
                cols[(s2, t2)].append(r["column_name"])
                sem_perfil.add((s2, t2))

    tot = collections.Counter()
    print(f"{'modelo':46} {'colunas':>8} {'tipadas':>8}")
    por_schema = collections.defaultdict(list)
    for (s, t), cs in sorted(cols.items()):
        por_schema[s].append((t, cs))

    for s, tabelas in sorted(por_schema.items()):
        d = os.path.join(a.saida, s)
        os.makedirs(d, exist_ok=True)
        Y = ["models:"]
        for t, cs in tabelas:
            linhas_sql, ncast = [], 0
            docs = []
            inacessivel = (s, t) in sem_perfil
            for c in cs:
                k = f"{s}.{t}.{c}"
                if inacessivel:
                    macro, motivo = ("bronze_texto",
                                     "tabela sem permissao de leitura para esta role: "
                                     "o padrao do dado nao pode ser medido, entao nao se tipa")
                else:
                    macro, motivo = decidir(padroes.get(k), perfil.get(k))
                if macro != "bronze_texto":
                    ncast += 1
                linhas_sql.append(f'    {{{{ {macro}("{c}") }}}} as {c},')
                docs.append((c, TIPO_SQL[macro], motivo))
            open(os.path.join(d, f"bronze_{t}.sql"), "w", encoding="utf-8").write(
                f"-- Bronze {s} — cópia fiel de {s}.{t}.\n"
                "-- Sem cast, filtro nem join (ADR 0010): tudo continua text.\n"
                f"-- Quem tipa é silver_{t}.\n"
                f'select * from {{{{ source("{s}", "{t}") }}}}  -- noqa: AM04\n')

            sql = [
                f"-- Silver {s} — {t}.",
                f"-- Origem: bronze_{t}, cópia fiel de {s}.{t}, onde tudo chega",
                "-- como text da ingestão via API.",
                "-- Tipar é o trabalho desta camada.",
                f"-- {len(cs)} colunas: {ncast} tipadas, {len(cs) - ncast} mantidas como texto.",
                "-- O cast vem do padrão medido no dado (scripts/perfilar_padroes.py),",
                "-- não do nome da coluna: exige 100% dos valores preenchidos casando.",
                "select",
            ]
            sql += linhas_sql
            sql[-1] = sql[-1].rstrip(",")
            sql.append(f'from {{{{ ref("bronze_{t}") }}}}')
            open(os.path.join(d, f"silver_{t}.sql"), "w", encoding="utf-8").write(
                "\n".join(sql) + "\n")

            for camada, tier, descricao in (
                ("bronze", "Tier.Tier3",
                 f"Camada Bronze — cópia fiel de {s}.{t}, sem cast, filtro nem join "
                 "(ADR 0010). Todas as colunas chegam como na origem."),
                ("silver", "Tier.Tier2",
                 f"Camada Silver — {s}.{t} tipada, lida de bronze_{t}. "
                 f"{len(cs)} colunas, {ncast} com cast aplicado."),
            ):
                Y.append(f"  - name: {camada}_{t}")
                Y.append("    description: >")
                Y += dobrar(descricao, 94, "      ")
                Y.append("    config:")
                Y.append("      tags:")
                Y.append(f"        - {camada}")
                Y.append(f"        - {s}")
                Y.append("      meta:")
                Y.append("        openmetadata:")
                Y.append(f"          domain: {DOMINIO.get(s, 'Cultura')}")
                Y.append(f"          tier: {tier}")
                Y.append("          owner: minc-data-engineering")
                Y.append("")
            Y.pop()
            Y.append("    columns:")
            for c, tipo, motivo in docs:
                Y.append(f"      - name: {c}")
                Y.append("        description: >")
                Y += dobrar(f"Tipagem: {motivo}.", 94, "          ")
                Y.append(f"        data_type: {tipo}")
            Y.append("")
            tot["modelos"] += 1
            tot["colunas"] += len(cs)
            tot["tipadas"] += ncast
            print(f"{s + '/silver_' + t:46} {len(cs):8} {ncast:8}")
        os.makedirs(a.yaml_saida, exist_ok=True)
        open(os.path.join(a.yaml_saida, f"{s}.yml"), "w", encoding="utf-8").write(
            "\n".join(Y) + "\n")

    print(f"\n{tot['modelos']} modelos, {tot['colunas']} colunas, "
          f"{tot['tipadas']} tipadas ({100*tot['tipadas']//max(tot['colunas'],1)}%)")


if __name__ == "__main__":
    main()
