#!/usr/bin/env python3
"""Gera o SQL que move a Bronze do SALIC de salic_bronze para salic (ADR 0017).

A ingestão salic_ingestion_trino passa a gravar salic.bronze_<banco>_<tabela>.
As 656 tabelas que já existem em salic_bronze.<banco>__<tabela> não precisam
ser recarregadas pela VPN: este script escreve scripts/salic_mover_bronze.sql,
que a pessoa roda uma vez no dbanalytics. Cada tabela recebe um
``RENAME`` e um ``ALTER TABLE ... SET SCHEMA salic``; as views dependentes
seguem a tabela pelo OID e continuam válidas.

De onde vem a lista de tabelas:

* sem argumento: dos ``sources_*.yml`` em dbt/minc/models/salic_dbt/bronze/
  cujo ``schema`` é ``salic_bronze`` (as declaradas no dbt);
* ``--todas ARQUIVO``: de um arquivo com um nome de tabela por linha, colado
  do banco (``select tablename from pg_tables where schemaname = 'salic_bronze'``),
  para incluir as que o dbt não declara.

O nome de destino sai de ``plugins.trino_bronze.bronze_table_name``, a mesma
função que a DAG usa, para o rename e a carga concordarem.

Uso:
    poetry run python scripts/gerar_mover_bronze_salic.py
    poetry run python scripts/gerar_mover_bronze_salic.py --todas tabelas.txt
"""

from __future__ import annotations

import argparse
import datetime as dt
import sys
from pathlib import Path

import yaml

RAIZ = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(RAIZ))

from plugins.trino_bronze import bronze_table_name  # noqa: E402

PASTA_SOURCES = RAIZ / "dbt" / "minc" / "models" / "salic_dbt" / "bronze"
SCHEMA_ORIGEM = "salic_bronze"
SCHEMA_DESTINO = "salic"
SAIDA = RAIZ / "scripts" / "salic_mover_bronze.sql"


def tabelas_dos_sources(pasta: Path = PASTA_SOURCES) -> list[str]:
    """Nomes <banco>__<tabela> declarados nos sources com schema salic_bronze."""
    nomes: set[str] = set()
    for arq in sorted(pasta.glob("sources_*.yml")):
        doc = yaml.safe_load(arq.read_text(encoding="utf-8")) or {}
        for src in doc.get("sources", []):
            if src.get("schema") != SCHEMA_ORIGEM:
                continue
            nomes.update(t["name"] for t in src.get("tables", []))
    return sorted(nomes)


def tabelas_do_arquivo(arq: Path) -> list[str]:
    """Um nome por linha; ignora vazias, comentários e o que não é <banco>__<tabela>.

    O banco tem sobras de carga do Trino (``tmp_trino_*``) que não são Bronze de
    nada; ficam em salic_bronze e caem com o schema, no fim da migração.
    """
    nomes = set()
    for linha in arq.read_text(encoding="utf-8").splitlines():
        nome = linha.strip()
        if not nome or nome.startswith("#"):
            continue
        if "__" not in nome.strip("_"):
            print(f"ignorado (não é <banco>__<tabela>): {nome}", file=sys.stderr)
            continue
        nomes.add(nome)
    return sorted(nomes)


def separar(nome: str) -> tuple[str, str]:
    """``sac__projetos`` -> (``sac``, ``projetos``)."""
    banco, sep, tabela = nome.partition("__")
    if not sep or not banco or not tabela:
        raise ValueError(f"{nome!r} não está no formato <banco>__<tabela>")
    return banco, tabela


def destino(nome: str) -> str:
    banco, tabela = separar(nome)
    return bronze_table_name(banco, tabela)


def _ident(nome: str) -> str:
    return '"' + nome.replace('"', '""') + '"'


def _lit(nome: str) -> str:
    return "'" + nome.replace("'", "''") + "'"


def gerar_sql(nomes: list[str], hoje: dt.date | None = None) -> str:
    hoje = hoje or dt.date.today()
    pares = [(n, destino(n)) for n in nomes]
    repetidos = {d for _, d in pares if sum(1 for _, x in pares if x == d) > 1}
    if repetidos:
        raise ValueError(f"destinos repetidos: {sorted(repetidos)}")

    partes = [
        f"-- Gerado por scripts/gerar_mover_bronze_salic.py em {hoje:%Y-%m-%d}.",
        "-- Roda uma vez, no dbanalytics (psql), como o DONO das tabelas",
        f"-- (dl_airflow), com CREATE em {SCHEMA_ORIGEM} (o RENAME exige) e em",
        f"-- {SCHEMA_DESTINO} (o SET SCHEMA exige); os dois schemas são de dl_analytics.",
        f"-- Idempotente: o que já foi movido não existe mais em {SCHEMA_ORIGEM} e o",
        "-- IF EXISTS pula.",
        f"-- {len(pares)} tabelas.",
        "\\set ON_ERROR_STOP on",
        "begin;",
        "",
        f"-- 1. colisão: nada em {SCHEMA_DESTINO} pode ter o nome de destino, salvo a",
        "--    própria tabela já movida.",
        "do $$",
        "declare r record;",
        "begin",
        "  for r in",
    ]
    uniao = "\n    union all ".join(
        f"select {_lit(o)} as origem, {_lit(d)} as destino" for o, d in pares
    )
    partes.append("    " + uniao)
    partes += [
        "  loop",
        f"    if to_regclass({_lit(SCHEMA_DESTINO + '.')} || quote_ident(r.destino)) "
        "is not null",
        f"       and to_regclass({_lit(SCHEMA_ORIGEM + '.')} || quote_ident(r.origem)) "
        "is not null then",
        f"      raise exception '{SCHEMA_DESTINO}.% já existe e {SCHEMA_ORIGEM}.% "
        "também; resolva antes do rename', r.destino, r.origem;",
        "    end if;",
        "  end loop;",
        "end $$;",
        "",
        "-- 2. renomeia dentro da origem e só então move. Na ordem inversa a tabela",
        f"--    colidiria com a view legada do dbt em {SCHEMA_DESTINO}, que tem o mesmo",
        "--    nome <banco>__<tabela> da origem.",
    ]
    for o, d in pares:
        partes.append(
            f"alter table if exists {SCHEMA_ORIGEM}.{_ident(o)} rename to {_ident(d)};"
        )
        partes.append(
            f"alter table if exists {SCHEMA_ORIGEM}.{_ident(d)} "
            f"set schema {SCHEMA_DESTINO};"
        )
    partes += [
        "",
        "commit;",
        "",
        "-- 3. conferência",
        "select",
        "    (select count(*) from pg_tables",
        f"      where schemaname = {_lit(SCHEMA_DESTINO)} "
        "and tablename like 'bronze\\_%') as bronzes_em_destino,",
        "    (select count(*) from pg_tables",
        f"      where schemaname = {_lit(SCHEMA_ORIGEM)}) as sobrou_na_origem;",
        "",
    ]
    return "\n".join(partes)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument(
        "--todas",
        type=Path,
        help="arquivo com um nome <banco>__<tabela> por linha, colado do banco",
    )
    ap.add_argument("--saida", type=Path, default=SAIDA)
    args = ap.parse_args(argv)

    nomes = tabelas_do_arquivo(args.todas) if args.todas else tabelas_dos_sources()
    if not nomes:
        print("nenhuma tabela encontrada", file=sys.stderr)
        return 1
    args.saida.write_text(gerar_sql(nomes), encoding="utf-8")
    print(f"{len(nomes)} tabelas -> {args.saida.relative_to(RAIZ)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
