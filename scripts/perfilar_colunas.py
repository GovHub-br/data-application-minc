#!/usr/bin/env python3
"""
Perfila as colunas de um schema para enriquecer a documentacao, SEM extrair
valor de linha.

Toda projecao e agregada. As unicas coisas que saem do banco sao:
  - contagens (linhas, preenchidos, distintos)
  - min/max de COMPRIMENTO (nunca min/max do texto, que devolveria o valor
    real de alguem)
  - faixa (min/max) de numero e de data -- que sao agregados de dominio
  - mascara de formato: digito vira 9, letra vira X
  - dominio de coluna categorica, e SO quando: nao esta marcada como PII,
    tem no maximo 40 valores distintos, e cada valor aparece 5+ vezes
    (supressao de celula pequena)

Uso:
    python3 scripts/perfilar_colunas.py --schemas transferegov bbagil bacen ibge_sidra \\
        --saida perfil.json
"""
import argparse
import json
import os
import re
import subprocess

PII = re.compile(
    r"((^|[^a-z])raca([^a-z]|$)|etnia|religi|saude|deficien|biometr|cpf|cnpj|"
    r"\brg\b|nome|nasc|email|e_mail|mail|telefone|fone|celular|cep|endereco|"
    r"logradouro|bairro|senha|password|token|renda|salario|passaporte)", re.I)

NUM = ("integer", "bigint", "smallint", "numeric", "double precision", "real")
DATA = ("date", "timestamp", "timestamp without time zone",
        "timestamp with time zone")
MIN_CELULA = 5
MAX_DOMINIO = 40


def psql(sql):
    env = dict(os.environ)
    env["PGPASSWORD"] = env["DB_DW_PASSWORD"]
    r = subprocess.run(
        ["psql", "-h", env["DB_DW_HOST"], "-p", env["DB_DW_PORT"],
         "-U", env["DB_DW_USER"], "-d", env["DB_DW_DBNAME"],
         "-At", "-F", "\t", "-c", sql],
        capture_output=True, text=True, env=env, timeout=300)
    if r.returncode:
        raise RuntimeError(r.stderr.strip()[:400])
    return [l.split("\t") for l in r.stdout.strip().split("\n") if l]


def perfil_tabela(schema, tabela, colunas):
    """Uma consulta por tabela, com todos os agregados de todas as colunas."""
    partes = ["count(*) AS _linhas"]
    for c, tipo in colunas:
        q = f'"{c}"'
        partes.append(f"count({q}) AS {c}__preenchidos")
        partes.append(f"count(DISTINCT {q}) AS {c}__distintos")
        if tipo in NUM:
            partes.append(f"min({q})::text AS {c}__min")
            partes.append(f"max({q})::text AS {c}__max")
        elif tipo in DATA:
            partes.append(f"min({q})::text AS {c}__min")
            partes.append(f"max({q})::text AS {c}__max")
        else:
            partes.append(f"min(length({q}::text)) AS {c}__lmin")
            partes.append(f"max(length({q}::text)) AS {c}__lmax")
    sql = f'SELECT {", ".join(partes)} FROM "{schema}"."{tabela}"'
    vals = psql(sql)[0]
    nomes = [p.split(" AS ")[1] for p in partes]
    return dict(zip(nomes, vals))


def _norm(expr):
    """Normaliza tabulacao e quebra de linha, que quebrariam o parser da saida."""
    return "regexp_replace(%s, '[\\n\\t]', ' ', 'g')" % expr


def _pares(sql):
    """Ultimo campo e a contagem; o resto e o valor."""
    return [("\t".join(r[:-1]), int(r[-1])) for r in psql(sql) if len(r) >= 2]


def mascara(schema, tabela, coluna):
    col = '"%s"::text' % coluna
    m = "regexp_replace(regexp_replace(%s, '[0-9]', '9', 'g'), '[A-Za-zA-u00ff]', 'X', 'g')" % col
    sql = (f"SELECT {_norm(m)} AS m, count(*) "
           f'FROM "{schema}"."{tabela}" WHERE "{coluna}" IS NOT NULL '
           f"GROUP BY 1 HAVING count(*) >= {MIN_CELULA} ORDER BY 2 DESC LIMIT 3")
    return _pares(sql)


def dominio(schema, tabela, coluna):
    sql = (f"SELECT {_norm('\"%s\"::text' % coluna)}, count(*) "
           f'FROM "{schema}"."{tabela}" WHERE "{coluna}" IS NOT NULL '
           f"GROUP BY 1 HAVING count(*) >= {MIN_CELULA} "
           f"ORDER BY 2 DESC LIMIT {MAX_DOMINIO}")
    return _pares(sql)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--schemas", nargs="+", required=True)
    p.add_argument("--saida", required=True)
    a = p.parse_args()

    cols = psql(f"""
        SELECT n.nspname, c.relname, a.attname, format_type(a.atttypid, a.atttypmod),
               c.reltuples::bigint
        FROM pg_attribute a
        JOIN pg_class c ON c.oid = a.attrelid
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relkind='r' AND a.attnum>0 AND NOT a.attisdropped
          AND n.nspname IN ({','.join("'" + s + "'" for s in a.schemas)})
        ORDER BY 1,2,a.attnum""")

    tabelas = {}
    for s, t, col, tipo, linhas in cols:
        tabelas.setdefault((s, t), []).append((col, tipo))

    out = {}
    for (s, t), cs in sorted(tabelas.items()):
        print(f"  {s}.{t} ({len(cs)} colunas)...", flush=True)
        try:
            ag = perfil_tabela(s, t, cs)
        except RuntimeError as e:
            print(f"     falhou: {e}")
            continue
        linhas = int(ag["_linhas"])
        for col, tipo in cs:
            d = {"tipo": tipo, "linhas": linhas,
                 "preenchidos": int(ag[f"{col}__preenchidos"]),
                 "distintos": int(ag[f"{col}__distintos"])}
            for suf in ("min", "max", "lmin", "lmax"):
                k = f"{col}__{suf}"
                if k in ag and ag[k] != "":
                    d[suf] = ag[k]
            eh_pii = bool(PII.search(col))
            d["pii"] = eh_pii
            try:
                if linhas and 0 < d["distintos"] <= MAX_DOMINIO and not eh_pii \
                        and tipo not in NUM + DATA:
                    d["dominio"] = dominio(s, t, col)
                elif eh_pii and tipo not in NUM + DATA and d["preenchidos"]:
                    d["mascara"] = mascara(s, t, col)
            except Exception as e:
                d["erro"] = str(e)[:120]
            out[f"{s}.{t}.{col}"] = d

    json.dump(out, open(a.saida, "w", encoding="utf-8"), ensure_ascii=False, indent=1)
    print(f"\n{len(out)} colunas perfiladas -> {a.saida}")


if __name__ == "__main__":
    main()
