"""Renderizador dos PDFs de entrega da Plataforma de Dados MinC.

Cada documento é uma sequência de páginas físicas A4 (`.gh-page`), na
arquitetura de página fixa sem margem descrita na skill `govhub-visual-identity`
(`references/print-pages.md`): `@page { margin: 0 }`, cada página é uma caixa
`210x297mm` com `overflow: hidden`, e todo o conteúdo interno é posicionado
absolutamente. Isso evita o bug de barra fantasma do Chrome com margem negativa
+ quebra de página forçada.

A paginação é manual: quem escreve o conteúdo decide onde cada página termina,
listando os blocos de cada página separadamente. O que este módulo automatiza é
só o que dá para calcular sem renderizar: os números de página do índice, que
saem da própria estrutura (capa=1, identificação=2, índice=3, e daí em diante
uma página por lista de blocos).

Conferência visual do PDF gerado continua obrigatória: `overflow: hidden`
descarta em silêncio o que não couber. Ver `verificar.sh`.
"""

from __future__ import annotations

import html
from pathlib import Path

RAMPA = [
    "var(--editorial-01-purple)",
    "var(--editorial-02-magenta)",
    "var(--editorial-03-pink)",
    "var(--editorial-04-coral)",
]

INSTITUICOES = [
    "Universidade de Brasília",
    "Lab Livre · Faculdade do Gama, UnB",
    "Ministério da Cultura",
    "Gov Hub BR",
]

AUTORES = [
    (
        "Equipe técnica",
        "Ana Nunes · Arthur Melo · Caio Melo Borges · Davi de Aguiar Vieira · "
        "Lucas Bottino · Luiza Maluf · Wallyson Souza",
    ),
]

PROJETO = "Plataforma de Dados do Ministério da Cultura · Gov Hub BR"


def esc(t: str) -> str:
    return html.escape(t, quote=False)


# ─────────────────────────── blocos de conteúdo ───────────────────────────


def render_bloco(b: tuple) -> str:
    tipo = b[0]

    if tipo == "p":
        return f"<p>{b[1]}</p>"

    if tipo == "lead":
        return f'<p class="lead">{b[1]}</p>'

    if tipo == "h3":
        return f"<h3>{esc(b[1])}</h3>"

    if tipo == "h4":
        return f"<h4>{esc(b[1])}</h4>"

    if tipo == "ul":
        itens = "".join(f"<li>{i}</li>" for i in b[1])
        return f"<ul>{itens}</ul>"

    if tipo == "ol":
        itens = "".join(f"<li>{i}</li>" for i in b[1])
        return f"<ol>{itens}</ol>"

    if tipo == "table":
        _, cabecalho, linhas = b[0], b[1], b[2]
        legenda = b[3] if len(b) > 3 else None
        larguras = b[4] if len(b) > 4 else None
        cols = ""
        if larguras:
            cols = "<colgroup>" + "".join(f'<col style="width:{w}">' for w in larguras) + "</colgroup>"
        th = "".join(f"<th>{c}</th>" for c in cabecalho)
        trs = ""
        for linha in linhas:
            tds = "".join(f"<td>{c}</td>" for c in linha)
            trs += f"<tr>{tds}</tr>"
        out = f'<table class="gh-table">{cols}<thead><tr>{th}</tr></thead><tbody>{trs}</tbody></table>'
        if legenda:
            out += f'<p class="gh-table__caption">{legenda}</p>'
        return out

    if tipo == "callout":
        _, icone, titulo, paragrafos = b
        ps = "".join(f'<p class="gh-callout__text">{p}</p>' for p in paragrafos)
        return (
            '<div class="gh-callout">'
            f'<div class="gh-callout__badge"><img alt="" src="icons/name={icone}, background=Default.svg"></div>'
            f'<div><div class="gh-callout__title">{esc(titulo)}</div>{ps}</div>'
            "</div>"
        )

    if tipo == "stats":
        cards = ""
        for item in b[1]:
            numero, rotulo = item[0], item[1]
            classe = " warm" if len(item) > 2 and item[2] else ""
            cards += (
                '<div class="gh-stat">'
                f'<div class="gh-stat__num{classe}">{numero}</div>'
                f'<div class="gh-stat__label">{rotulo}</div>'
                "</div>"
            )
        return f'<div class="gh-stats">{cards}</div>'

    if tipo == "svg":
        return b[1]

    if tipo == "code":
        legenda = b[2] if len(b) > 2 else None
        out = f'<div class="gh-code">{esc(b[1])}</div>'
        if legenda:
            out += f'<p class="gh-code__caption">{legenda}</p>'
        return out

    if tipo == "ficha":
        _, identificador, subtitulo, pares = b
        corpo = ""
        for k, v in pares:
            corpo += f'<div class="gh-ficha__k">{esc(k)}</div><div class="gh-ficha__v">{v}</div>'
        return (
            '<div class="gh-ficha">'
            '<div class="gh-ficha__head">'
            f'<div class="gh-ficha__id">{esc(identificador)}</div>'
            f'<div class="gh-ficha__sub">{esc(subtitulo)}</div>'
            "</div>"
            f'<div class="gh-ficha__body">{corpo}</div>'
            "</div>"
        )

    raise ValueError(f"bloco desconhecido: {tipo}")


# ─────────────────────────────── páginas ──────────────────────────────────


def rodape(nome_curto: str) -> str:
    texto = f"{nome_curto} &middot; Plataforma de Dados MinC &middot; Gov Hub &middot; Lab Livre - UnB"
    return (
        '<div class="gh-footer-bar"></div>'
        '<div class="gh-footer">'
        f'<span class="gh-footer__text">{texto}</span>'
        '<img class="gh-footer__logo" alt="" src="logo/orientation=none, colour=primary.svg">'
        "</div>"
    )


def pagina_capa(doc: dict) -> str:
    return (
        '<div class="gh-page gh-cover">'
        '<div class="gh-cover__border"></div>'
        '<div class="gh-cover__content">'
        '<img class="gh-cover__logo" alt="GovHub" src="logo/orientation=horizontal, colour=light.svg">'
        f'<h1 class="gh-cover__title">{esc(doc["titulo"])}</h1>'
        f'<p class="gh-cover__subtitle">{esc(doc["subtitulo"])}</p>'
        "</div>"
        '<div class="gh-cover__footer">'
        '<img src="logo/parceiros/lab-livre.png" alt="Lab Livre">'
        '<img src="logo/parceiros/unb.png" alt="UnB">'
        "</div>"
        "</div>"
    )


def pagina_identificacao(doc: dict) -> str:
    orgs = "".join(
        f'<div class="gh-id-org-block"><p class="gh-id-org">{esc(i)}</p></div>'
        for i in INSTITUICOES
    )

    projeto = (
        '<div class="gh-id-org-block">'
        '<p class="gh-id-org">Projeto de Pesquisa</p>'
        f'<p class="gh-id-person">{esc(PROJETO)}</p>'
        "</div>"
    )
    for rotulo, descricao in doc["meta"]:
        projeto += (
            '<div class="gh-id-org-block">'
            f'<p class="gh-id-org">{esc(rotulo)}</p>'
            f'<p class="gh-id-person">{esc(descricao)}</p>'
            "</div>"
        )
    projeto += (
        '<div class="gh-id-org-block">'
        '<p class="gh-id-org">Documento</p>'
        f'<p class="gh-id-person">{esc(doc["titulo"])}</p>'
        "</div>"
    )

    return (
        '<div class="gh-page">'
        '<div class="gh-id-page">'
        f"<div>{orgs}</div>"
        '<div class="gh-id-divider"></div>'
        f"<div>{projeto}</div>"
        "</div>"
        f'{rodape(doc["rodape"])}'
        "</div>"
    )


def pagina_indice(doc: dict, paginas_por_capitulo: list[int]) -> str:
    itens = ""
    numero_pagina = 4  # capa=1, identificação=2, índice=3
    for cap, n_paginas in zip(doc["capitulos"], paginas_por_capitulo):
        itens += (
            "<li>"
            f'<span class="num">{cap["num"]}</span>'
            f'<span class="label">{esc(cap["titulo"])}</span>'
            '<span class="leader"></span>'
            f'<span class="page">{numero_pagina}</span>'
            "</li>"
        )
        numero_pagina += n_paginas

    return (
        '<div class="gh-page">'
        '<div class="gh-toc-page">'
        '<h2 class="gh-toc-title">Índice</h2>'
        f'<ul class="gh-toc-list">{itens}</ul>'
        "</div>"
        f'{rodape(doc["rodape"])}'
        "</div>"
    )


def paginas_capitulo(cap: dict, indice: int, nome_curto: str) -> str:
    cor = RAMPA[indice % len(RAMPA)]
    saida = ""

    for i, blocos in enumerate(cap["paginas"]):
        corpo = "".join(render_bloco(b) for b in blocos)

        if i == 0:
            icone = ""
            if cap.get("icone"):
                icone = (
                    '<div class="gh-band__icon">'
                    f'<img alt="" src="icons/name={cap["icone"]}, background=Default.svg">'
                    "</div>"
                )
            faixa = (
                f'<div class="gh-band" style="--section-color:{cor}">'
                '<div class="gh-band__row">'
                f'<div class="gh-band__num">{cap["num"]}</div>'
                "<div>"
                f'<div class="gh-band__eyebrow">{esc(cap["eyebrow"])}</div>'
                f'<h2 class="gh-band__title">{esc(cap["titulo"])}</h2>'
                "</div>"
                f"{icone}"
                "</div>"
                "</div>"
            )
            saida += (
                '<div class="gh-page">'
                f"{faixa}"
                f'<div class="gh-page-body">{corpo}</div>'
                f"{rodape(nome_curto)}"
                "</div>"
            )
        else:
            saida += (
                '<div class="gh-page">'
                f'<div class="gh-page-body no-band">{corpo}</div>'
                f"{rodape(nome_curto)}"
                "</div>"
            )

    return saida


def render_documento(doc: dict) -> str:
    paginas_por_capitulo = [len(c["paginas"]) for c in doc["capitulos"]]

    corpo = pagina_capa(doc)
    corpo += pagina_identificacao(doc)
    corpo += pagina_indice(doc, paginas_por_capitulo)
    for i, cap in enumerate(doc["capitulos"]):
        corpo += paginas_capitulo(cap, i, doc["rodape"])

    return (
        "<!doctype html>"
        '<html lang="pt-BR"><head><meta charset="utf-8">'
        f"<title>{esc(doc['titulo'])}</title>"
        '<link rel="stylesheet" href="gh-print.css">'
        f"</head><body>{corpo}</body></html>"
    )


def escrever(doc: dict, destino: Path) -> Path:
    caminho = destino / f"{doc['slug']}.html"
    caminho.write_text(render_documento(doc), encoding="utf-8")
    total = 3 + sum(len(c["paginas"]) for c in doc["capitulos"])
    print(f"  {caminho.name:44s} {total:2d} páginas")
    return caminho
