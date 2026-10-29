"""Progresso da migração e da documentação das silvers do SALIC.

Só lê o repositório; não toca o banco. Rode no começo de toda sessão: é a
fonte da verdade do que falta, e não uma lista de memória.

Por banco de origem:
- legado: modelos que ainda estão em salic_dbt/bronze/<banco>/;
- silver: modelos já em salic/silver_<banco>_*.sql;
- colunas documentadas no schema_<banco>.yml e quantas ainda estão
  [NÃO VERIFICADO];
- testes marcados "# inferido", que precisam de confirmação;
- colunas com nome de credencial no select de alguma silver.

Uso (da raiz do repositório):
    python3 .claude/skills/silver-salic-dbt/scripts/inventario.py
"""

from __future__ import annotations

import re
from pathlib import Path

MODELOS = Path("dbt/minc/models")
LEGADO = MODELOS / "salic_dbt" / "bronze"
SILVER = MODELOS / "salic"
BANCOS = ("controledeacesso", "bdcorporativo", "agentes", "tabelas", "sac")
CREDENCIAL = re.compile(r"\b(senha\w*|\w*passw\w*|\w*hash\w*|\w*token\w*)\b", re.I)


def contar_schema(banco: str) -> tuple[int, int, int]:
    arq = SILVER / f"schema_{banco}.yml"
    if not arq.exists():
        return 0, 0, 0
    texto = arq.read_text(encoding="utf-8")
    colunas = len(re.findall(r"^      - name: ", texto, flags=re.M))
    nao_verificado = texto.count("[NÃO VERIFICADO]")
    inferidos = texto.count("# inferido")
    return colunas, nao_verificado, inferidos


def credenciais(banco: str) -> list[str]:
    achados = []
    for sql in sorted(SILVER.glob(f"silver_{banco}_*.sql")):
        corpo = "\n".join(
            linha
            for linha in sql.read_text(encoding="utf-8").splitlines()
            if not linha.lstrip().startswith("--")
        )
        for nome in sorted(set(m.group(1).lower() for m in CREDENCIAL.finditer(corpo))):
            achados.append(f"{sql.stem}.{nome}")
    return achados


def main() -> None:
    print(
        f"{'banco':<18}{'legado':>7}{'silver':>7}{'colunas':>9}"
        f"{'não verif.':>11}{'inferidos':>10}"
    )
    alertas = []
    for banco in BANCOS:
        legado = (
            len(list((LEGADO / banco).glob("*.sql"))) if (LEGADO / banco).exists() else 0
        )
        silver = len(list(SILVER.glob(f"silver_{banco}_*.sql")))
        colunas, nv, inf = contar_schema(banco)
        print(f"{banco:<18}{legado:>7}{silver:>7}{colunas:>9}{nv:>11}{inf:>10}")
        alertas += credenciais(banco)
    if alertas:
        print("\ncolunas com nome de credencial em silver (tirar do select):")
        for a in alertas:
            print(f"  - {a}")
    pendentes = [
        b for b in BANCOS if (LEGADO / b).exists() and any((LEGADO / b).glob("*.sql"))
    ]
    if pendentes:
        print(f"\npróximo lote a migrar: {pendentes[0]}")


if __name__ == "__main__":
    main()
