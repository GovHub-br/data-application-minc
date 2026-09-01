"""Inventario do schema salic_bronze: o que existe, o que esta vazio, o que
ja tem modelo bronze. Rode no comeco de toda sessao -- o numero muda, e
tabelas novas chegam.

Le o .env da raiz do repositorio. Nao imprime credencial.
Sessao read-only, com statement_timeout. E banco de producao.

Uso:
    /tmp/dbtvenv/bin/python .claude/skills/bronze-salic-dbt/scripts/inventario.py
    /tmp/dbtvenv/bin/python .../inventario.py --contar   # + COUNT(*) real (~2 min)
"""

from __future__ import annotations

import argparse
import glob
import json
import sys
import time
from pathlib import Path

import psycopg2

SCHEMA = "salic_bronze"
SAIDA = Path("/tmp/salic_inventario.json")


def conectar():
    env = {}
    for linha in open(".env", encoding="utf-8"):
        linha = linha.strip()
        if linha and not linha.startswith("#") and "=" in linha:
            k, v = linha.split("=", 1)
            env[k.strip()] = v.strip().strip('"').strip("'")
    faltando = {"IP", "PORTA", "USER", "PASS", "DB"} - set(env)
    if faltando:
        sys.exit(f"faltam chaves no .env: {sorted(faltando)}")
    try:
        c = psycopg2.connect(
            host=env["IP"],
            port=env["PORTA"],
            user=env["USER"],
            password=env["PASS"],
            dbname=env["DB"],
            connect_timeout=20,
        )
    except psycopg2.OperationalError as e:
        sys.exit(f"nao conectou ({type(e).__name__}). A VPN esta de pe?")
    c.set_session(readonly=True)
    c.cursor().execute("set statement_timeout='60s'")
    return c


def modelos_existentes() -> set[str]:
    """Modelos bronze ja escritos -- e a fonte da verdade do progresso."""
    return {
        Path(p).stem
        for p in glob.glob("dbt/minc/models/salic_bronze/**/*.sql", recursive=True)
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--contar", action="store_true", help="COUNT(*) real (mais lento)")
    args = ap.parse_args()

    c = conectar()
    cur = c.cursor()
    cur.execute(
        "select table_name from information_schema.tables "
        "where table_schema=%s order by table_name",
        (SCHEMA,),
    )
    tabelas = [r[0] for r in cur.fetchall()]
    print(f"{len(tabelas)} tabelas em {SCHEMA}")

    # EXISTS e O(1) e DEFINITIVO. reltuples mente: o schema nunca passou por
    # ANALYZE, entao o planejador reporta 0 para tabela nunca analisada -- ja
    # deu 192 "vazias" quando eram 85.
    print("checando vazias com EXISTS...")
    vazias, cheias = [], []
    for t in tabelas:
        cur.execute(f'select exists(select 1 from {SCHEMA}."{t}" limit 1)')
        (cheias if cur.fetchone()[0] else vazias).append(t)

    lixo = [t for t in tabelas if t.startswith("tmp_trino_")]
    fora = set(vazias) | set(lixo)
    escopo = sorted(set(tabelas) - fora)

    contagem = {}
    if args.contar:
        print(f"contando linhas de {len(escopo)} tabelas...")
        t0 = time.time()
        for i, t in enumerate(escopo, 1):
            cur.execute(f'select count(*) from {SCHEMA}."{t}"')
            contagem[t] = cur.fetchone()[0]
            if i % 100 == 0:
                print(f"  {i}/{len(escopo)} ({time.time() - t0:.0f}s)")

    feitos = modelos_existentes()
    pendentes = [t for t in escopo if t not in feitos]

    print(f"\n{'=' * 46}")
    print(f"  total                {len(tabelas):5}")
    print(f"  - vazias             {len(vazias):5}")
    print(f"  - lixo do trino      {len(set(lixo) - set(vazias)):5}")
    print(f"  {'=' * 42}")
    print(f"  ESCOPO DA BRONZE     {len(escopo):5}")
    print(f"    ja modeladas       {len(escopo) - len(pendentes):5}")
    print(f"    PENDENTES          {len(pendentes):5}")
    if contagem:
        print(f"\n  linhas no escopo: {sum(contagem.values()):,}")

    SAIDA.write_text(
        json.dumps(
            {
                "schema": SCHEMA,
                "total": len(tabelas),
                "vazias": sorted(vazias),
                "lixo_trino": sorted(lixo),
                "escopo": escopo,
                "pendentes": pendentes,
                "contagem": contagem,
            },
            indent=2,
        )
    )
    print(f"\ngravado em {SAIDA}")
    if pendentes:
        print(f"proximo lote sugerido: {pendentes[:10]}")
    c.close()


if __name__ == "__main__":
    main()
