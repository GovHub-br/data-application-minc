"""Perfila as tabelas do escopo: colunas reais + amostra de 200 linhas.

Grava incremental em /tmp/salic_perfil.json e RETOMA de onde parou -- a VPN
cai no meio, e perfilar 571 tabelas duas vezes e desperdicio caro.

Sessao read-only com statement_timeout. E banco de producao. Nunca faz
SELECT DISTINCT: nao ha um unico indice em salic_bronze, e DISTINCT vira
varredura completa. A amostra sai de UMA leitura de 200 linhas por tabela,
de onde se extrai o perfil de TODAS as colunas.

Uso:
    /tmp/dbtvenv/bin/python .claude/skills/bronze-salic-dbt/scripts/perfilar.py
    /tmp/dbtvenv/bin/python .../perfilar.py --limite 50   # so as 50 primeiras pendentes
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import psycopg2

SCHEMA = "salic_bronze"
INVENTARIO = Path("/tmp/salic_inventario.json")
SAIDA = Path("/tmp/salic_perfil.json")
AMOSTRA = 200
# Valor distinto guardado por coluna. O bastante para achar o dominio de uma
# coluna categorica sem inchar o JSON de uma coluna de texto livre.
MAX_DISTINTOS = 25


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


def carregar() -> dict:
    if SAIDA.exists():
        return json.loads(SAIDA.read_text())
    return {
        "colunas": {},
        "tipo_objeto": {},
        "tipo_coluna": {},
        "perfil": {},
        "falhas": {},
    }


def gravar(dados: dict) -> None:
    SAIDA.write_text(json.dumps(dados, ensure_ascii=False))


def metadados(cur, dados: dict) -> None:
    """Colunas reais e tabela-vs-view. Duas consultas ao catalogo, nada pesado."""
    cur.execute(
        "select table_name, column_name, data_type "
        "from information_schema.columns "
        "where table_schema=%s order by table_name, ordinal_position",
        (SCHEMA,),
    )
    cols: dict[str, list[str]] = {}
    tipos: dict[str, dict[str, str]] = {}
    for t, col, dt in cur.fetchall():
        cols.setdefault(t, []).append(col)
        tipos.setdefault(t, {})[col] = dt
    dados["colunas"] = cols
    # Nem tudo em salic_bronze e texto: as views herdam o tipo da expressao.
    # Coluna que ja chega tipada nao precisa de cast nenhum.
    dados["tipo_coluna"] = tipos
    cur.execute(
        "select table_name, table_type from information_schema.tables "
        "where table_schema=%s",
        (SCHEMA,),
    )
    dados["tipo_objeto"] = dict(cur.fetchall())


def perfilar_tabela(cur, tabela: str, colunas: list[str]) -> dict:
    """Uma leitura de 200 linhas; o perfil de todas as colunas sai dela."""
    lista = ", ".join(f'"{c}"' for c in colunas)
    cur.execute(f'select {lista} from {SCHEMA}."{tabela}" limit {AMOSTRA}')
    linhas = cur.fetchall()

    perfil = {}
    for i, col in enumerate(colunas):
        # str(): coluna de view pode vir int, numeric ou date do proprio Postgres
        vals = [None if r[i] is None else str(r[i]) for r in linhas]
        nao_nulos = [v for v in vals if v is not None and v.strip() != ""]
        distintos = sorted({v for v in nao_nulos})
        perfil[col] = {
            "n": len(vals),
            "preenchidos": len(nao_nulos),
            "distintos": len(distintos),
            # so guarda o dominio quando ele e pequeno de verdade
            "valores": (
                distintos[:MAX_DISTINTOS] if len(distintos) <= MAX_DISTINTOS else []
            ),
            "amostra": nao_nulos[:5],
            "maxlen": max((len(v) for v in nao_nulos), default=0),
        }
    return perfil


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--limite", type=int, default=0)
    ap.add_argument("--timeout", default="25s")
    args = ap.parse_args()

    inv = json.loads(INVENTARIO.read_text())
    escopo = inv["escopo"]
    dados = carregar()

    c = conectar()
    cur = c.cursor()
    cur.execute(f"set statement_timeout='{args.timeout}'")
    if not dados["colunas"]:
        metadados(cur, dados)
        gravar(dados)
        print(f"metadados de {len(dados['colunas'])} objetos")

    pendentes = [
        t
        for t in escopo
        if t not in dados["perfil"] and t not in dados["falhas"] and t in dados["colunas"]
    ]
    if args.limite:
        pendentes = pendentes[: args.limite]
    print(f"{len(pendentes)} tabelas a perfilar ({len(dados['perfil'])} ja feitas)")

    for n, t in enumerate(pendentes, 1):
        try:
            dados["perfil"][t] = perfilar_tabela(cur, t, dados["colunas"][t])
        except psycopg2.OperationalError as e:
            gravar(dados)
            sys.exit(
                f"\nconexao caiu em {t} ({type(e).__name__}). "
                f"{len(dados['perfil'])} perfiladas e salvas. A VPN esta de pe?"
            )
        except psycopg2.Error as e:
            # view cara, permissao, timeout: registra e segue
            dados["falhas"][t] = f"{type(e).__name__}: {str(e).strip()[:120]}"
            c.rollback()
        if n % 25 == 0:
            gravar(dados)
            print(f"  {n}/{len(pendentes)}  (falhas: {len(dados['falhas'])})")

    gravar(dados)
    print(f"\nperfiladas {len(dados['perfil'])} | falhas {len(dados['falhas'])}")
    if dados["falhas"]:
        for t, e in list(dados["falhas"].items())[:10]:
            print(f"  {t}: {e}")
    c.close()


if __name__ == "__main__":
    main()
