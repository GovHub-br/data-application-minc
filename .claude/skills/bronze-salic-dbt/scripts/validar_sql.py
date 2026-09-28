"""Valida o SQL compilado de cada modelo da bronze contra o banco, com EXPLAIN.

EXPLAIN planeja sem executar: pega nome de coluna errado, cast invalido e
sintaxe quebrada, sem ler uma linha das 157 milhoes nem materializar nada.
E a checagem mais barata que existe antes de um `dbt run` de verdade.

Rode depois de `dbt compile --select path:models/salic_dbt/bronze`.

Uso:
    /tmp/dbtvenv/bin/python .claude/skills/bronze-salic-dbt/scripts/validar_sql.py
"""

from __future__ import annotations

import json
from pathlib import Path

import psycopg2

COMPILADOS = Path("dbt/minc/target/compiled/minc/models/salic_dbt/bronze")
SAIDA = Path("/tmp/salic_validacao.json")


def conectar():
    env = {}
    for linha in open(".env", encoding="utf-8"):
        linha = linha.strip()
        if linha and not linha.startswith("#") and "=" in linha:
            k, v = linha.split("=", 1)
            env[k.strip()] = v.strip().strip('"').strip("'")
    c = psycopg2.connect(
        host=env["IP"],
        port=env["PORTA"],
        user=env["USER"],
        password=env["PASS"],
        dbname=env["DB"],
        connect_timeout=20,
    )
    c.set_session(readonly=True)
    return c


def main() -> None:
    arquivos = sorted(p for p in COMPILADOS.rglob("*.sql") if p.parent.name != "tests")
    print(f"{len(arquivos)} arquivos compilados")

    c = conectar()
    cur = c.cursor()
    cur.execute("set statement_timeout='30s'")

    erros: dict[str, str] = {}
    for n, p in enumerate(arquivos, 1):
        sql = p.read_text(encoding="utf-8")
        try:
            cur.execute("explain " + sql)
            cur.fetchall()
        except psycopg2.Error as e:
            erros[p.stem] = f"{type(e).__name__}: {str(e).strip().splitlines()[0][:160]}"
            c.rollback()
        if n % 100 == 0:
            print(f"  {n}/{len(arquivos)}  (erros: {len(erros)})")

    SAIDA.write_text(json.dumps(erros, indent=2, ensure_ascii=False))
    print(f"\nvalidados {len(arquivos)} | ERROS {len(erros)}")
    for m, e in list(erros.items())[:20]:
        print(f"  {m}\n    {e}")
    c.close()


if __name__ == "__main__":
    main()
