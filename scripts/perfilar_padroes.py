#!/usr/bin/env python3
"""
Segunda passada de perfil: mede, por coluna de texto, quantos valores casam
com cada padrao (inteiro, decimal, data, timestamp, booleano).

E o que falta para decidir o cast da camada bronze. O perfil anterior traz
faixa e comprimento; isto responde "TODOS os valores sao inteiro?", que e a
unica pergunta que autoriza tipar.

So agregado: `count(*) FILTER (WHERE col ~ padrao)`. Nenhum valor sai.

Uso:
    python3 scripts/perfilar_padroes.py --schemas transferegov ibge_sidra bacen \\
        --saida padroes.json
"""
import argparse
import json
import os
import subprocess

PADROES = {
    "inteiro":   r"^-?[0-9]+$",
    "decimal":   r"^-?[0-9]+[.,][0-9]+$",
    "data":      r"^[0-9]{4}-[0-9]{2}-[0-9]{2}$",
    "timestamp": r"^[0-9]{4}-[0-9]{2}-[0-9]{2}[ T][0-9]{2}:[0-9]{2}",
    "booleano":  r"^(true|false|t|f|True|False|TRUE|FALSE)$",
    "zero_esq":  r"^0[0-9]+$",       # zero a esquerda: NAO pode virar inteiro
    "nan":       r"^(NaN|nan|None|null|NULL)$",
}


def psql(sql):
    env = dict(os.environ)
    env["PGPASSWORD"] = env["DB_DW_PASSWORD"]
    r = subprocess.run(
        ["psql", "-h", env["DB_DW_HOST"], "-p", env["DB_DW_PORT"],
         "-U", env["DB_DW_USER"], "-d", env["DB_DW_DBNAME"],
         "-At", "-F", "\t", "-c", sql],
        capture_output=True, text=True, env=env, timeout=600)
    if r.returncode:
        raise RuntimeError(r.stderr.strip()[:300])
    return [l.split("\t") for l in r.stdout.strip().split("\n") if l]


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--schemas", nargs="+", required=True)
    p.add_argument("--saida", required=True)
    a = p.parse_args()

    cols = psql(f"""
        SELECT n.nspname, c.relname, a.attname
        FROM pg_attribute a
        JOIN pg_class c ON c.oid = a.attrelid
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relkind='r' AND a.attnum>0 AND NOT a.attisdropped
          AND n.nspname IN ({','.join("'" + s + "'" for s in a.schemas)})
          AND format_type(a.atttypid, a.atttypmod) IN ('text','character varying')
        ORDER BY 1,2,a.attnum""")

    tabelas = {}
    for s, t, col in cols:
        tabelas.setdefault((s, t), []).append(col)

    out = {}
    for (s, t), cs in sorted(tabelas.items()):
        partes = ["count(*) AS _linhas"]
        for c in cs:
            q = f'"{c}"'
            partes.append(f"count({q}) AS {c}__pre")
            for nome, rx in PADROES.items():
                partes.append(
                    f"count(*) FILTER (WHERE {q} ~ '{rx}') AS {c}__{nome}")
        sql = f'SELECT {", ".join(partes)} FROM "{s}"."{t}"'
        try:
            vals = psql(sql)[0]
        except RuntimeError as e:
            print(f"  {s}.{t}: falhou ({str(e)[:60]})")
            continue
        nomes = [x.split(" AS ")[1] for x in partes]
        ag = dict(zip(nomes, vals))
        for c in cs:
            pre = int(ag[f"{c}__pre"])
            out[f"{s}.{t}.{c}"] = {
                "preenchidos": pre,
                **{k: int(ag[f"{c}__{k}"]) for k in PADROES},
            }
        print(f"  {s}.{t}: {len(cs)} colunas")

    json.dump(out, open(a.saida, "w", encoding="utf-8"), ensure_ascii=False, indent=1)
    print(f"\n{len(out)} colunas -> {a.saida}")


if __name__ == "__main__":
    main()
