"""Perfil das tabelas da Bronze do SALIC de um lote, sem expor valor de linha.

Quem roda é o usuário, com o .env dele; a saída volta para a conversa. Por
isso o relatório impresso só tem contagem, formato e domínio de coluna
categórica cujo nome não sugere dado pessoal. O JSON gravado em --saida tem o
mesmo conteúdo, para retomar depois de queda de VPN.

Por tabela:
- colunas reais e comprimento do nome (o Postgres corta em 63);
- amostra: UMA leitura de 200 linhas, de onde sai o perfil de todas as
  colunas (preenchimento, distintos, tamanho máximo, formatos);
- com --completo: UMA varredura agregada da tabela inteira, que conta linhas,
  preenchimento e quantos valores passam em cada regex de tipo.

Sessão read-only, com statement_timeout. Nunca SELECT DISTINCT: a Bronze não
tem índice.

Uso (da raiz do repositório):
    .venv/bin/python .claude/skills/silver-salic-dbt/scripts/perfilar.py \\
        /caminho/absoluto/.env --banco controledeacesso --saida /tmp/perfil.json
    ... --tabelas sgcacesso,sgcsistema       # só estas
    ... --completo                           # mais a varredura da tabela inteira
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any

import psycopg2

SCHEMA = "salic"
AMOSTRA = 200
MAX_DOMINIO = 10
MAX_LEN_DOMINIO = 40
LIMITE_NOME = 63
MODELOS = Path("dbt/minc/models/salic")

# Nome que sugere dado pessoal: o domínio nunca é impresso, só o formato.
SENSIVEL = re.compile(
    r"cpf|cnpj|nome|mail|fone|celular|endereco|logradouro|bairro|cep|nasc|"
    r"rg$|identidade|raca|etnia|deficien|sexo|genero|login|usuario|ip$|"
    r"senha|pass|hash|token|chave",
    re.I,
)
CREDENCIAL = re.compile(r"senha|passw|pwd|hash|token|secret|chave_api", re.I)

TIPOS = {
    "inteiro": r"^-?[0-9]+$",
    "numerico": r"^-?[0-9]+([.,][0-9]+)?$",
    "data_iso": r"^[0-9]{4}-[0-9]{2}-[0-9]{2}",
    "data_br": r"^[0-9]{2}/[0-9]{2}/[0-9]{4}",
}
BOOLEANOS = ("0", "1", "true", "false", "t", "f", "s", "n")

CONTRATOS_ENV = (
    ("DB_DW_HOST", "DB_DW_PORT", "DB_DW_USER", "DB_DW_PASSWORD", "DB_DW_DBNAME"),
    ("IP", "PORTA", "USER", "PASS", "DB"),
)


def ler_env(caminho: str) -> dict[str, str]:
    env: dict[str, str] = {}
    for linha in open(caminho, encoding="utf-8"):
        linha = linha.strip()
        if linha and not linha.startswith("#") and "=" in linha:
            k, v = linha.split("=", 1)
            env[k.strip().removeprefix("export ").strip()] = (
                v.strip().strip('"').strip("'")
            )
    return env


def conectar(caminho_env: str, timeout: str) -> Any:
    env = ler_env(caminho_env)
    for host, porta, user, senha, banco in CONTRATOS_ENV:
        if all(k in env for k in (host, porta, user, senha, banco)):
            break
    else:
        sys.exit("o .env não tem DB_DW_HOST/PORT/USER/PASSWORD/DBNAME")
    try:
        c = psycopg2.connect(
            host=env[host],
            port=env[porta],
            user=env[user],
            password=env[senha],
            dbname=env[banco],
            connect_timeout=10,
            application_name="perfilar_salic",
        )
    except psycopg2.OperationalError as e:
        sys.exit(f"não conectou ({type(e).__name__}). A VPN está de pé?")
    c.set_session(readonly=True, autocommit=True)
    c.cursor().execute(f"set statement_timeout = '{timeout}'")
    return c


def tabelas_do_repositorio(banco: str) -> list[str]:
    """As silvers do banco que já estão em salic/, pelo nome do arquivo."""
    prefixo = f"silver_{banco}_"
    return sorted(p.stem.removeprefix(prefixo) for p in MODELOS.glob(f"{prefixo}*.sql"))


def formato(valor: str) -> str:
    """Máscara do valor: dígito vira 9, letra vira a, o resto fica."""
    m = re.sub(r"[0-9]", "9", valor)
    m = re.sub(r"[^\W\d_]", "a", m)
    m = re.sub(r"(a{6,})", lambda g: f"a{{{len(g.group(1))}}}", m)
    return m if len(m) <= 30 else m[:30] + "…"


def colunas_da_tabela(cur: Any, tabela: str) -> list[str]:
    cur.execute(
        "select column_name from information_schema.columns "
        "where table_schema = %s and table_name = %s order by ordinal_position",
        (SCHEMA, tabela),
    )
    return [r[0] for r in cur.fetchall()]


def perfil_amostra(cur: Any, tabela: str, colunas: list[str]) -> dict:
    lista = ", ".join(f'"{c}"' for c in colunas)
    cur.execute(f'select {lista} from {SCHEMA}."{tabela}" limit {AMOSTRA}')
    linhas = cur.fetchall()
    perfil = {}
    for i, col in enumerate(colunas):
        vals = [None if r[i] is None else str(r[i]) for r in linhas]
        cheios = [v for v in vals if v is not None and v.strip() != ""]
        distintos = set(cheios)
        formatos: dict[str, int] = {}
        for v in cheios:
            f = formato(v)
            formatos[f] = formatos.get(f, 0) + 1
        top = sorted(formatos.items(), key=lambda x: -x[1])[:3]
        dominio = None
        if (
            not SENSIVEL.search(col)
            and 0 < len(distintos) <= MAX_DOMINIO
            and max(len(v) for v in distintos) <= MAX_LEN_DOMINIO
            and not any(re.fullmatch(r"[0-9.\-/ ]{8,}", v) for v in distintos)
            and not any("@" in v for v in distintos)
        ):
            dominio = sorted(distintos)
        perfil[col] = {
            "amostra_n": len(vals),
            "preenchidos": len(cheios),
            "distintos": len(distintos),
            "maxlen": max((len(v) for v in cheios), default=0),
            # de credencial nem a máscara: só o tamanho
            "formatos": [] if CREDENCIAL.search(col) else top,
            "dominio": dominio,
            "sensivel": bool(SENSIVEL.search(col)),
            "credencial": bool(CREDENCIAL.search(col)),
        }
    return perfil


def perfil_completo(cur: Any, tabela: str, colunas: list[str]) -> dict:
    """Uma varredura só, com todas as colunas agregadas na mesma consulta."""
    partes = ["count(*)"]
    for c in colunas:
        v = f"nullif(trim(\"{c}\"::text), '')"
        partes.append(f"count({v})")
        for rx in TIPOS.values():
            partes.append(f"count(*) filter (where {v} ~ '{rx}')")
        lista_b = ", ".join(f"'{b}'" for b in BOOLEANOS)
        partes.append(f"count(*) filter (where lower({v}) in ({lista_b}))")
    cur.execute(f'select {", ".join(partes)} from {SCHEMA}."{tabela}"')
    r = cur.fetchone()
    out: dict = {"linhas": r[0], "colunas": {}}
    passo = 2 + len(TIPOS)
    for i, c in enumerate(colunas):
        base = 1 + i * passo
        cheios = r[base]
        tipos = dict(zip(TIPOS, r[base + 1 : base + 1 + len(TIPOS)]))
        tipos["booleano"] = r[base + 1 + len(TIPOS)]
        out["colunas"][c] = {"preenchidos": cheios, "passam": tipos}
    return out


def pct(n: int, total: int) -> str:
    return f"{100 * n / total:.1f}%" if total else "-"


def imprimir(tabela: str, dados: dict) -> None:
    print(f"\n## {tabela}  (nome com {len(tabela)} caracteres)")
    if "erro" in dados:
        print(f"  ERRO: {dados['erro']}")
        return
    comp = dados.get("completo")
    if comp:
        print(f"  linhas: {comp['linhas']}")
    for col, p in dados["amostra"].items():
        marca = (
            " CREDENCIAL?" if p["credencial"] else (" [pessoal]" if p["sensivel"] else "")
        )
        linha = (
            f"  - {col}{marca}: amostra {p['preenchidos']}/{p['amostra_n']} cheios, "
            f"{p['distintos']} distintos, maxlen {p['maxlen']}"
        )
        if p["formatos"]:
            linha += "; formatos " + ", ".join(f"{f!r}×{n}" for f, n in p["formatos"])
        if p["dominio"] is not None:
            linha += f"; domínio {p['dominio']}"
        if comp:
            c = comp["colunas"][col]
            cheios = c["preenchidos"]
            passam = ", ".join(
                f"{t} {pct(n, cheios)}" for t, n in c["passam"].items() if n
            )
            linha += f"\n      tabela: preenchida {pct(cheios, comp['linhas'])}"
            if passam:
                linha += f"; dos preenchidos passam {passam}"
        print(linha)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("env", help="caminho absoluto do .env")
    ap.add_argument("--banco", required=True)
    ap.add_argument("--tabelas", default="", help="lista separada por vírgula")
    ap.add_argument("--saida", required=True, help="arquivo JSON de resultado")
    ap.add_argument("--completo", action="store_true")
    ap.add_argument("--timeout", default="")
    ap.add_argument("--refazer", action="store_true")
    args = ap.parse_args()

    tabelas = [
        t.strip() for t in args.tabelas.split(",") if t.strip()
    ] or tabelas_do_repositorio(args.banco)
    if not tabelas:
        sys.exit(f"nenhuma silver_{args.banco}_* em {MODELOS}; passe --tabelas")

    saida = Path(args.saida)
    feito: dict = json.loads(saida.read_text()) if saida.exists() else {}
    timeout = args.timeout or ("600s" if args.completo else "30s")
    c = conectar(args.env, timeout)
    cur = c.cursor()

    for t in tabelas:
        fisica = f"bronze_{args.banco}_{t}".lower()[:LIMITE_NOME]
        anterior = feito.get(fisica, {})
        if (
            not args.refazer
            and anterior.get("amostra")
            and (not args.completo or anterior.get("completo"))
        ):
            imprimir(fisica, anterior)
            continue
        try:
            colunas = colunas_da_tabela(cur, fisica)
            if not colunas:
                feito[fisica] = {"erro": "tabela não existe em salic"}
            else:
                dados = {"amostra": perfil_amostra(cur, fisica, colunas)}
                if args.completo:
                    dados["completo"] = perfil_completo(cur, fisica, colunas)
                feito[fisica] = dados
        except psycopg2.OperationalError as e:
            saida.write_text(json.dumps(feito, ensure_ascii=False, indent=1))
            sys.exit(
                f"\nconexão caiu em {fisica} ({type(e).__name__}). Rode de novo: retoma."
            )
        except psycopg2.Error as e:
            feito[fisica] = {"erro": f"{type(e).__name__}: {str(e).strip()[:160]}"}
        saida.write_text(json.dumps(feito, ensure_ascii=False, indent=1))
        imprimir(fisica, feito[fisica])
    c.close()
    print(f"\ngravado em {saida}")


if __name__ == "__main__":
    main()
