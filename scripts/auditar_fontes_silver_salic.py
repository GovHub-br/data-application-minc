"""Procura no banco as fontes que faltam para as silvers da Rouanet.

Por que este script existe: o `sources_*.yml` do dbt diz o que foi MODELADO,
nao o que foi ingerido. A lista de fontes da ingestao v2 e uma Variable do
Airflow (`salic_trino_data`), fora do repositorio, e com `"tables": []` ela
carrega TODAS as tabelas base do schema de origem. Ou seja: e perfeitamente
possivel que a tabela exista no raw e simplesmente nunca tenha ganhado modelo.
So o banco responde.

Cada conceito abaixo trava pelo menos um modelo silver (docs/openmetadata/
MEMORY.md, secao 17). O script procura por padrao de nome, e nao por nome
exato, porque o nome no SQL Server nem sempre e o que o desenho do FigJam diz.

Le o .env da raiz, no mesmo contrato de
.claude/skills/bronze-salic-dbt/scripts/inventario.py. Nao imprime credencial.
Sessao read-only, com statement_timeout. E banco de producao.

Uso:
    python scripts/auditar_fontes_silver_salic.py
    python scripts/auditar_fontes_silver_salic.py --contar    # COUNT(*) real
    python scripts/auditar_fontes_silver_salic.py --schemas salic_bronze,bronze
"""

from __future__ import annotations

import argparse
import glob
import sys
from pathlib import Path

import psycopg2

MODELOS_BRONZE = "dbt/minc/models/salic_dbt/bronze/**/*.sql"

# conceito -> (o que ele destrava, padrao SQL LIKE aplicado ao nome da tabela)
CONCEITOS: dict[str, tuple[str, list[str]]] = {
    "agente_mestre": (
        "dim_agente_perfil_rouanet_scd",
        ["%agente%", "%agentes%", "%pessoafisica%", "%pessoa_fisica%"],
    ),
    "autodeclaracao": (
        "dim_agente_perfil_rouanet_scd / Meta 3",
        [
            "%perfil%",
            "%autodeclar%",
            "%raca%",
            "%etnia%",
            "%deficien%",
            "%pcd%",
            "%indigena%",
            "%quilombol%",
        ],
    ),
    "endereco": (
        "residencia do proponente e do prestador",
        ["%endereco%", "%cep%", "%logradouro%"],
    ),
    "municipio_uf": (
        "dim_municipio_ibge / Meta 4",
        ["%municipio%", "%municipios%", "%localidade%", "%cidade%", "%populacao%"],
    ),
    "prestacao_contas": (
        "fct_pagamento_profissional_rouanet / Meta 3 P1",
        [
            "%prestcontas%",
            "%prestacao%",
            "%encaminhament%",
            "%comprovante%",
            "%comprovacao%",
            "%comprovad%",
        ],
    ),
    "projeto_v2": (
        "dim_projeto_rouanet (cadastro oficial, hoje vindo de view)",
        ["projetos", "projeto", "%preprojeto%", "situacao", "%tbapiprojeto%"],
    ),
    "ponte_chave": (
        "map_chave_projeto_rouanet / Meta 4",
        ["%idprojeto%", "%abrangencia%", "%itenscomprovados%"],
    ),
    "identidade": (
        "fct_proponente_ano_rouanet / Meta 5",
        ["%alteracaonome%", "%vinculacao%", "%verificacao%"],
    ),
}


# Dois contratos de nome convivem no repositorio: o curto, que a skill
# bronze-salic-dbt usa, e o `DB_DW_*` do profiles.yml do dbt. Aceitar os dois e
# mais barato que padronizar agora, e evita um script que so roda na maquina de
# quem o escreveu.
CONTRATOS_ENV = (
    {"host": "IP", "port": "PORTA", "user": "USER", "password": "PASS", "dbname": "DB"},
    {
        "host": "DB_DW_HOST",
        "port": "DB_DW_PORT",
        "user": "DB_DW_USER",
        "password": "DB_DW_PASSWORD",
        "dbname": "DB_DW_DBNAME",
    },
)


def ler_env() -> dict[str, str]:
    env = {}
    for linha in Path(".env").read_text(encoding="utf-8").splitlines():
        linha = linha.strip()
        if linha and not linha.startswith("#") and "=" in linha:
            k, v = linha.split("=", 1)
            env[k.strip()] = v.strip().strip('"').strip("'")
    return env


def conectar():
    env = ler_env()
    parametros = None
    for contrato in CONTRATOS_ENV:
        if set(contrato.values()) <= set(env):
            parametros = {alvo: env[chave] for alvo, chave in contrato.items()}
            break
    if parametros is None:
        # Só nomes de chave, nunca valores.
        sys.exit(
            "nenhum contrato de conexao completo no .env.\n"
            f"chaves presentes: {sorted(env)}\n"
            f"esperado um destes conjuntos: "
            f"{[sorted(c.values()) for c in CONTRATOS_ENV]}"
        )
    try:
        conexao = psycopg2.connect(**parametros, connect_timeout=20)
    except psycopg2.OperationalError as erro:
        sys.exit(f"nao conectou ({type(erro).__name__}). A VPN esta de pe?")
    conexao.set_session(readonly=True)
    conexao.cursor().execute("set statement_timeout='120s'")
    return conexao


def modelados() -> set[str]:
    """Tabelas que ja tem modelo bronze -- o nome do arquivo e o da tabela."""
    return {Path(p).stem for p in glob.glob(MODELOS_BRONZE, recursive=True)}


def objetos(cur, schemas: list[str]) -> list[tuple[str, str, str]]:
    """(schema, tabela, tipo) de tudo que existe nos schemas pedidos.

    Inclui VIEW de proposito: a ingestao v2 filtra `table_type = 'BASE TABLE'`
    na ORIGEM, mas o que chega ao Postgres e sempre tabela. Se aparecer view
    aqui, alguem a criou depois -- e isso e informacao.
    """
    cur.execute(
        """
        select table_schema, table_name, table_type
        from information_schema.tables
        where table_schema = any(%s)
        order by table_schema, table_name
        """,
        (schemas,),
    )
    return cur.fetchall()


def linhas_aprox(cur, schema: str, tabela: str) -> int:
    """Estimativa do planner. Nao varre a tabela."""
    cur.execute(
        """
        select coalesce(c.reltuples, -1)::bigint
        from pg_class c
        join pg_namespace n on n.oid = c.relnamespace
        where n.nspname = %s and c.relname = %s
        """,
        (schema, tabela),
    )
    linha = cur.fetchone()
    return int(linha[0]) if linha else -1


def linhas_exatas(cur, schema: str, tabela: str) -> int:
    cur.execute(f'select count(*) from "{schema}"."{tabela}"')
    return int(cur.fetchone()[0])


def casa(nome: str, padroes: list[str]) -> bool:
    alvo = nome.lower()
    for padrao in padroes:
        p = padrao.strip("%").lower()
        if padrao.startswith("%") and padrao.endswith("%"):
            if p in alvo:
                return True
        elif alvo == p or alvo.endswith(f"__{p}"):
            return True
    return False


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--contar", action="store_true", help="COUNT(*) real, mais lento")
    ap.add_argument("--schemas", default="salic_bronze,bronze")
    args = ap.parse_args()
    schemas = [s.strip() for s in args.schemas.split(",") if s.strip()]

    com_modelo = modelados()
    conexao = conectar()
    cur = conexao.cursor()
    todos = objetos(cur, schemas)
    if not todos:
        sys.exit(f"nenhum objeto em {schemas}. Schema errado?")

    print(f"# Fontes das silvers — busca em {', '.join(schemas)}\n")
    print(f"{len(todos)} objetos no banco · {len(com_modelo)} modelos bronze escritos\n")

    achados_totais = 0
    for conceito, (destrava, padroes) in CONCEITOS.items():
        achados = [t for t in todos if casa(t[1], padroes)]
        print(f"\n## {conceito} — destrava: {destrava}")
        if not achados:
            print("  NADA ENCONTRADO. A fonte precisa ser ingerida.")
            continue
        achados_totais += len(achados)
        print(f"  {len(achados)} objeto(s):\n")
        print("  | schema | tabela | tipo | linhas | modelo bronze |")
        print("  |---|---|---|---|---|")
        for schema, tabela, tipo in achados:
            try:
                n = (
                    linhas_exatas(cur, schema, tabela)
                    if args.contar
                    else linhas_aprox(cur, schema, tabela)
                )
            except psycopg2.Error:
                conexao.rollback()
                n = -1
            marca = "sim" if tabela in com_modelo else "**NAO**"
            quantas = "?" if n < 0 else f"{n:,}".replace(",", ".")
            print(f"  | {schema} | {tabela} | {tipo} | {quantas} | {marca} |")

    print(f"\n---\n{achados_totais} objeto(s) candidatos no total.")
    print("Tabela com linhas e SEM modelo bronze e o caso mais interessante:")
    print("o dado ja esta no banco e so falta a camada dbt.")
    conexao.close()


if __name__ == "__main__":
    main()
