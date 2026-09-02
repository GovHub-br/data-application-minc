#!/usr/bin/env python3
"""
Substitui o marcador [NAO VERIFICADO] das colunas das fontes de API por uma
descricao derivada do perfil do banco (scripts/perfilar_colunas.py).

A descricao contem apenas ESTATISTICA -- contagem de registros, de
preenchidos e de distintos, proporcao de nulos, comprimento minimo e maximo,
e se o dominio e fechado. Nenhum valor de exemplo e reproduzido: sem dominio,
sem mascara de formato e sem faixa de valor.

A regra e deliberada. Documentacao de base de governo circula por gente e por
ferramenta que nao tem o mesmo controle de acesso do banco, e valor de
exemplo e dado -- ainda que agregado, ainda que publico. O que precisa de
valor real e consulta ao banco, com a credencial de quem tem direito a ela.

Pelo mesmo motivo nao se gera `accepted_values`: o teste enumera valor
literal da coluna dentro do repositorio. Se for desejado, a lista tem de vir
de tabela de dominio versionada, nao de leitura do banco.

Uso:
    python3 scripts/aplicar_perfil_sources.py --perfil perfil.json \
        [--schemas transferegov ibge_sidra]
"""
import argparse
import json
import re

ARQ = "dbt/minc/models/sources.yml"
MAX_ACEITOS = 15

# Reavaliado aqui, e nao herdado do perfil: o perfilamento marcou PII com um
# padrao so em portugues, entao coluna como `beneficiaryname` ou `auditlogin`
# passou e ganhou dominio publicado em vez de mascara. Publicar dominio de
# coluna pessoal expoe valor real -- foi assim que um login de usuario
# ("AE4274141", 1.400 ocorrencias) quase entrou no repositorio.
PESSOAL = re.compile(
    r"(cpf|cnpj|\brg\b|documentid|document_?id|taxid|nome|\bname\b|nm[a-z]{3}|"
    r"email|e_?mail|telefone|fone|celular|phone|\bcep\b|(^|_)cep\d*($|_)|"
    r"endereco|address|logradouro|bairro|nasc|birth|senha|password|token|"
    r"secret|apikey|api_?key|login|usuario|\buser\b|matricula|"
    r"conta|account|agencia|branch|beneficiar|payer|holder)", re.I)


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


def n_br(n):
    return f"{n:,}".replace(",", ".")


def descrever(v, coluna=""):
    """Frase descritiva a partir do perfil. Nunca expoe valor de coluna pessoal."""
    pessoal = bool(PESSOAL.search(coluna))
    linhas, pre, dist = v["linhas"], v["preenchidos"], v["distintos"]
    if pre == 0:
        return (f"Coluna sempre nula: nenhum dos {n_br(linhas)} registros tem valor. "
                f"Candidata a remoção ou a investigação da extração.")
    partes = []
    if dist == 1 and pre:
        partes.append(f"Coluna constante: um único valor distinto em {n_br(pre)} "
                      f"registros preenchidos. Não discrimina nada.")
    elif linhas and dist == linhas:
        partes.append(f"Identificador: {n_br(dist)} valores distintos em "
                      f"{n_br(linhas)} registros, sem repetição.")
    else:
        partes.append(f"{n_br(dist)} valores distintos em {n_br(pre)} registros "
                      f"preenchidos de {n_br(linhas)}.")
    if pre < linhas:
        partes.append(f"{100 * (linhas - pre) / linhas:.0f}% nulos.")
    if "lmin" in v and "lmax" in v and v["lmin"] not in ("", None):
        if v["lmin"] == v["lmax"]:
            partes.append(f"Comprimento fixo de {v['lmin']} caracteres.")
        else:
            partes.append(f"Comprimento entre {v['lmin']} e {v['lmax']} caracteres.")
    if v.get("dominio"):
        partes.append(f"Domínio fechado, com {len(v['dominio'])} valores distintos "
                      f"acima do limite de supressão.")
    partes.append("Perfil obtido só com agregado; nenhum valor de exemplo "
                  "é reproduzido aqui.")
    return " ".join(partes)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--perfil", required=True)
    p.add_argument("--schemas", nargs="*",
                   help="limita a estes schemas; util para deixar de fora "
                        "fonte com ingestao ainda rodando, cujo perfil e provisorio")
    a = p.parse_args()
    perfil = json.load(open(a.perfil, encoding="utf-8"))

    linhas = open(ARQ, encoding="utf-8").read().split("\n")
    saida = []
    schema = tabela = None
    i, n = 0, len(linhas)
    st = {"desc": 0}

    while i < n:
        l = linhas[i]
        m = re.match(r"^    schema: (\S+)", l)
        if m:
            schema = m.group(1)
        m = re.match(r"^      - name: (\S+)", l)
        if m:
            tabela = m.group(1)
        m = re.match(r"^          - name: (\S+)", l)
        if not m:
            saida.append(l)
            i += 1
            continue

        coluna = m.group(1)
        v = perfil.get(f"{schema}.{tabela}.{coluna}")
        if a.schemas and schema not in a.schemas:
            v = None
        bloco = [l]
        j = i + 1
        while j < n and not re.match(r"^          - name: |^      - name: |^  - name: ", linhas[j]):
            bloco.append(linhas[j])
            j += 1

        if v:
            # troca o corpo do description
            novo, k = [], 0
            while k < len(bloco):
                if bloco[k].strip() == "description: >":
                    novo.append(bloco[k])
                    k += 1
                    while k < len(bloco) and bloco[k].startswith("              "):
                        k += 1
                    novo += dobrar(descrever(v, coluna), 94, "              ")
                    st["desc"] += 1
                    continue
                novo.append(bloco[k])
                k += 1
            bloco = novo

        saida += bloco
        i = j

    open(ARQ, "w", encoding="utf-8").write("\n".join(saida) + "\n")
    print(f"descricoes substituidas : {st['desc']}")


if __name__ == "__main__":
    main()
