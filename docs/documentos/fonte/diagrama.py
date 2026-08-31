"""Diagramas de fluxo em SVG inline, para os PDFs de entrega.

Desenhados a partir da linhagem real do projeto dbt (as chamadas ``ref()`` e
``source()`` de cada modelo), não de um desenho mantido à parte. SVG inline em
vez de biblioteca de diagrama porque o PDF é gerado por Chrome headless a partir
de arquivo local: sem rede, sem script, sem fonte externa a resolver.

Sistema de coordenadas: as funções recebem coordenadas num espaço de 680 de
largura, cômodo de escrever, e internamente comprimem tudo pelo fator ``K``. O
viewBox resultante é mais estreito que a área útil da página, então o navegador
o estica de volta na hora de renderizar. Coordenada encolhe, tamanho de fonte
não: é assim que o rótulo dentro da caixa chega ao papel nos 9pt que a escala
tipográfica do projeto exige, em vez dos ~7pt que sairiam de um mapeamento
um-para-um.

**Verificação de colisão.** Nada aqui recorta, empurra ou reposiciona nada
sozinho: SVG deixa dois elementos se sobreporem em silêncio, e num diagrama de
linhagem uma seta passando por cima de uma caixa que ela não conecta é um erro
de leitura, não um detalhe estético. Por isso cada primitiva registra a própria
geometria e ``svg()`` confere o conjunto antes de devolver o desenho: caixa
sobre caixa, seta atravessando caixa alheia, rótulo sobre caixa e rótulo maior
que a caixa que o contém. Os avisos saem no ``build.py``. Corrigir é mover a
coordenada no ``conteudo_*.py``, não silenciar o aviso.
"""

from __future__ import annotations

# Compressão do espaço de coordenadas. 0.78 leva um rótulo de 9.5 a ~9pt no
# papel, medido no PDF gerado.
K = 0.78

LARGURA_BASE = 680

# Preenchimento e traço por camada. As cores saem dos tokens da identidade:
# roxo da marca para o que se consome (gold), tons progressivamente mais claros
# descendo até a origem.
ESTILO = {
    "fonte": ("#F8F9FA", "#B9A8DC", "#5B21B6", "4 3"),
    "bronze": ("#F3EEFC", "#C9B6EE", "#432379", None),
    "silver": ("#E7DCFB", "#A98CE6", "#3B1F6B", None),
    "gold": ("#7A34F3", "#5B21B6", "#FFFFFF", None),
    "macro": ("#FFF4E8", "#F19F42", "#8A4B08", None),
    "off": ("#FFFFFF", "#D8D8D8", "#9A9A9A", "4 3"),
}

ALTURA_PADRAO = 34

# Largura média de um caractere, como fração do tamanho da fonte. Monospace é
# previsível (0.60); a Inter itálica das notas é mais estreita.
LARGURA_CHAR_MONO = 0.60
LARGURA_CHAR_SANS = 0.50

# Folga mínima entre o texto e a borda da caixa que o contém, em unidades já
# comprimidas.
RESPIRO = 6.0

_REGISTRO: dict = {"caixas": [], "setas": [], "rotulos": []}
AVISOS: list[str] = []
_NOME_ATUAL = "diagrama"


def nomear(nome: str) -> None:
    """Identifica o diagrama que está sendo montado, para os avisos."""
    global _NOME_ATUAL
    _NOME_ATUAL = nome


def _reiniciar() -> None:
    _REGISTRO["caixas"] = []
    _REGISTRO["setas"] = []
    _REGISTRO["rotulos"] = []


# ───────────────────────────── desenho ─────────────────────────────────────


def _texto(x, y, linhas, cor, tamanho=10.5, peso=600, ancora="middle"):
    """x/y já vêm no espaço comprimido; `tamanho` é o tamanho final, não escalado."""
    n = len(linhas)
    y0 = y - (n - 1) * (tamanho + 1.5) / 2
    saida = ""
    for i, linha in enumerate(linhas):
        saida += (
            f'<text x="{x:.1f}" y="{y0 + i * (tamanho + 1.5):.1f}" '
            f'text-anchor="{ancora}" dominant-baseline="central" '
            f'font-family="SFMono-Regular, Menlo, monospace" '
            f'font-size="{tamanho}" font-weight="{peso}" fill="{cor}">{linha}</text>'
        )
    return saida


def caixa(x, y, w, rotulo, camada="bronze", h=ALTURA_PADRAO, tamanho=10.5):
    """Retângulo com rótulo. `rotulo` pode ser str ou lista de linhas."""
    x, y, w, h = x * K, y * K, w * K, h * K
    fundo, borda, cor_texto, tracejado = ESTILO[camada]
    linhas = rotulo if isinstance(rotulo, list) else [rotulo]

    nome = linhas[0] if len(linhas) == 1 else "".join(linhas)
    _REGISTRO["caixas"].append({"x": x, "y": y, "w": w, "h": h, "nome": nome})

    # O rótulo não é recortado pelo retângulo: se não couber, ele vaza por cima
    # da borda sem nenhum aviso do renderizador. Daí a conferência.
    maior = max(len(linha) for linha in linhas) * tamanho * LARGURA_CHAR_MONO
    if maior > w - RESPIRO:
        AVISOS.append(
            f"[{_NOME_ATUAL}] rótulo '{nome}' precisa de ~{maior:.0f} e a caixa "
            f"tem {w:.0f} de largura"
        )
    altura_texto = len(linhas) * (tamanho + 1.5)
    if altura_texto > h - 2:
        AVISOS.append(
            f"[{_NOME_ATUAL}] rótulo '{nome}' tem {len(linhas)} linhas e não cabe "
            f"na altura {h:.0f}"
        )

    dash = f' stroke-dasharray="{tracejado}"' if tracejado else ""
    return (
        f'<rect x="{x:.1f}" y="{y:.1f}" width="{w:.1f}" height="{h:.1f}" rx="6" '
        f'fill="{fundo}" stroke="{borda}" stroke-width="1.4"{dash}/>'
        + _texto(x + w / 2, y + h / 2, linhas, cor_texto, tamanho)
    )


def seta(x1, y1, x2, y2, rotulo=None, cor="#8A7BA8", curva=False):
    x1, y1, x2, y2 = x1 * K, y1 * K, x2 * K, y2 * K
    if curva:
        mx = (x1 + x2) / 2
        d = f"M {x1:.1f} {y1:.1f} C {mx:.1f} {y1:.1f}, {mx:.1f} {y2:.1f}, {x2:.1f} {y2:.1f}"
        pontos = _amostrar_bezier((x1, y1), (mx, y1), (mx, y2), (x2, y2))
    else:
        d = f"M {x1:.1f} {y1:.1f} L {x2:.1f} {y2:.1f}"
        pontos = [
            (x1 + (x2 - x1) * t / 24, y1 + (y2 - y1) * t / 24) for t in range(25)
        ]

    _REGISTRO["setas"].append({"pontos": pontos, "de": (x1, y1), "para": (x2, y2)})

    saida = (
        f'<path d="{d}" fill="none" stroke="{cor}" stroke-width="1.4" '
        f'marker-end="url(#ponta)"/>'
    )
    if rotulo:
        mx, my = (x1 + x2) / 2, (y1 + y2) / 2
        largura = len(rotulo) * 8.5 * LARGURA_CHAR_MONO + 8
        _REGISTRO["rotulos"].append(
            {"x": mx - largura / 2, "y": my - 8, "w": largura, "h": 16, "texto": rotulo}
        )
        saida += (
            f'<rect x="{mx - largura/2:.1f}" y="{my - 8:.1f}" width="{largura:.1f}" '
            f'height="16" rx="4" fill="#FFFFFF" stroke="#E3DAF3" stroke-width="1"/>'
            + _texto(mx, my, [rotulo], "#666666", 8.5, 600)
        )
    return saida


def caminho(pontos, rotulo=None, cor="#8A7BA8", raio=5):
    """Seta ortogonal por pontos de passagem, com cantos arredondados.

    Existe para o caso em que a ligação direta cruzaria uma caixa ou outra
    seta: em diagrama de linhagem, um desvio explícito em ângulo reto lê melhor
    que uma diagonal passando por cima de tudo.
    """
    pts = [(x * K, y * K) for x, y in pontos]
    _REGISTRO["setas"].append({"pontos": _densificar(pts), "de": pts[0], "para": pts[-1]})

    d = f"M {pts[0][0]:.1f} {pts[0][1]:.1f}"
    for i in range(1, len(pts) - 1):
        ax, ay = pts[i - 1]
        bx, by = pts[i]
        cx, cy = pts[i + 1]
        d += f" L {_recuar(bx, ax, raio):.1f} {_recuar(by, ay, raio):.1f}"
        d += f" Q {bx:.1f} {by:.1f} {_recuar(bx, cx, raio):.1f} {_recuar(by, cy, raio):.1f}"
    d += f" L {pts[-1][0]:.1f} {pts[-1][1]:.1f}"

    saida = (
        f'<path d="{d}" fill="none" stroke="{cor}" stroke-width="1.4" '
        f'marker-end="url(#ponta)"/>'
    )
    if rotulo:
        (ax, ay), (bx, by) = max(
            zip(pts, pts[1:]), key=lambda par: abs(par[1][0] - par[0][0]) + abs(par[1][1] - par[0][1])
        )
        mx, my = (ax + bx) / 2, (ay + by) / 2
        largura = len(rotulo) * 8.5 * LARGURA_CHAR_MONO + 8
        _REGISTRO["rotulos"].append(
            {"x": mx - largura / 2, "y": my - 8, "w": largura, "h": 16, "texto": rotulo}
        )
        saida += (
            f'<rect x="{mx - largura/2:.1f}" y="{my - 8:.1f}" width="{largura:.1f}" '
            f'height="16" rx="4" fill="#FFFFFF" stroke="#E3DAF3" stroke-width="1"/>'
            + _texto(mx, my, [rotulo], "#666666", 8.5, 600)
        )
    return saida


def _recuar(b, vizinho, raio):
    """Ponto a `raio` de distância de b, na direção do vizinho."""
    if abs(vizinho - b) <= raio:
        return vizinho
    return b + raio if vizinho > b else b - raio


def _densificar(pts, passo=3.0):
    saida = []
    for (x1, y1), (x2, y2) in zip(pts, pts[1:]):
        n = max(2, int(max(abs(x2 - x1), abs(y2 - y1)) / passo))
        saida += [(x1 + (x2 - x1) * i / n, y1 + (y2 - y1) * i / n) for i in range(n)]
    saida.append(pts[-1])
    return saida


def _amostrar_bezier(p0, p1, p2, p3, n=32):
    pontos = []
    for i in range(n + 1):
        t = i / n
        u = 1 - t
        x = u**3 * p0[0] + 3 * u**2 * t * p1[0] + 3 * u * t**2 * p2[0] + t**3 * p3[0]
        y = u**3 * p0[1] + 3 * u**2 * t * p1[1] + 3 * u * t**2 * p2[1] + t**3 * p3[1]
        pontos.append((x, y))
    return pontos


def rotulo_coluna(x, y, texto, cor="#7A34F3"):
    x, y = x * K, y * K
    return (
        f'<text x="{x:.1f}" y="{y:.1f}" text-anchor="middle" font-family="Inter, sans-serif" '
        f'font-size="9" font-weight="800" letter-spacing="1.4" fill="{cor}">'
        f"{texto.upper()}</text>"
    )


def nota(x, y, texto, ancora="start", cor="#666666", tamanho=9):
    x, y = x * K, y * K
    largura = len(texto) * tamanho * LARGURA_CHAR_SANS
    esquerda = x if ancora == "start" else x - largura / 2
    _REGISTRO["rotulos"].append(
        {
            "x": esquerda,
            "y": y - tamanho / 2,
            "w": largura,
            "h": tamanho,
            "texto": texto[:32],
        }
    )
    return (
        f'<text x="{x:.1f}" y="{y:.1f}" text-anchor="{ancora}" '
        f'font-family="Inter, sans-serif" font-size="{tamanho}" '
        f'font-style="italic" fill="{cor}">{texto}</text>'
    )


# ─────────────────────────── verificação ───────────────────────────────────


def _sobrepoe(a, b, folga=0.0):
    """Folga negativa infla os retângulos: encostar passa a contar como sobrepor."""
    return not (
        a["x"] + a["w"] <= b["x"] + folga
        or b["x"] + b["w"] <= a["x"] + folga
        or a["y"] + a["h"] <= b["y"] + folga
        or b["y"] + b["h"] <= a["y"] + folga
    )


def _dentro(ponto, r, folga=0.0):
    x, y = ponto
    return (
        r["x"] - folga <= x <= r["x"] + r["w"] + folga
        and r["y"] - folga <= y <= r["y"] + r["h"] + folga
    )


def _conferir():
    caixas = _REGISTRO["caixas"]

    for i, a in enumerate(caixas):
        for b in caixas[i + 1 :]:
            if _sobrepoe(a, b):
                AVISOS.append(f"[{_NOME_ATUAL}] caixas '{a['nome']}' e '{b['nome']}' se sobrepõem")

    # Seta que não encosta em caixa nenhuma na ponta aponta para o vazio. O SVG
    # desenha a flecha do mesmo jeito, e a leitura fica sugerindo uma ligação
    # que não existe.
    for s in _REGISTRO["setas"]:
        if not any(_dentro(s["para"], c, 6) for c in caixas):
            AVISOS.append(
                f"[{_NOME_ATUAL}] seta termina no vazio, em "
                f"({s['para'][0]/K:.0f}, {s['para'][1]/K:.0f})"
            )
        if not any(_dentro(s["de"], c, 6) for c in caixas):
            AVISOS.append(
                f"[{_NOME_ATUAL}] seta começa no vazio, em "
                f"({s['de'][0]/K:.0f}, {s['de'][1]/K:.0f})"
            )

    for s in _REGISTRO["setas"]:
        # A seta nasce e morre encostada nas caixas que conecta; essas duas não
        # contam como colisão. Qualquer outra que ela atravesse, sim.
        extremos = [
            c for c in caixas if _dentro(s["de"], c, 3) or _dentro(s["para"], c, 3)
        ]
        for c in caixas:
            if c in extremos:
                continue
            # Ignora o primeiro e o último trecho: são só o encosto na caixa.
            if any(_dentro(p, c, -1.5) for p in s["pontos"][2:-2]):
                AVISOS.append(f"[{_NOME_ATUAL}] seta atravessa a caixa '{c['nome']}'")
                break

    for r in _REGISTRO["rotulos"]:
        for c in caixas:
            if _sobrepoe(r, c, folga=-4.0):
                AVISOS.append(
                    f"[{_NOME_ATUAL}] rótulo '{r['texto']}' cobre a caixa '{c['nome']}'"
                )
                break

    rotulos = _REGISTRO["rotulos"]
    for i, a in enumerate(rotulos):
        for b in rotulos[i + 1 :]:
            if _sobrepoe(a, b):
                AVISOS.append(
                    f"[{_NOME_ATUAL}] rótulos '{a['texto']}' e '{b['texto']}' se sobrepõem"
                )

    # Duas setas que se cruzam no meio do caminho obrigam quem lê a decidir qual
    # é qual. Convergir no mesmo destino é normal e não conta.
    setas = _REGISTRO["setas"]
    for i, a in enumerate(setas):
        for b in setas[i + 1 :]:
            if _proximos(a["para"], b["para"], 14) or _proximos(a["de"], b["de"], 14):
                continue
            if _cruzam(a["pontos"], b["pontos"]):
                AVISOS.append(
                    f"[{_NOME_ATUAL}] setas se cruzam perto de "
                    f"({a['para'][0]/K:.0f}, {a['para'][1]/K:.0f})"
                )


def _proximos(p, q, limite):
    return abs(p[0] - q[0]) < limite and abs(p[1] - q[1]) < limite


def _cruzam(pa, pb):
    for s1, s2 in zip(pa, pa[1:]):
        for t1, t2 in zip(pb, pb[1:]):
            if _segmentos_cruzam(s1, s2, t1, t2):
                return True
    return False


def _lado(a, b, c):
    return (b[0] - a[0]) * (c[1] - a[1]) - (b[1] - a[1]) * (c[0] - a[0])


def _segmentos_cruzam(p1, p2, p3, p4):
    d1, d2 = _lado(p3, p4, p1), _lado(p3, p4, p2)
    d3, d4 = _lado(p1, p2, p3), _lado(p1, p2, p4)
    return ((d1 > 0) != (d2 > 0)) and ((d3 > 0) != (d4 > 0))


def svg(altura, corpo, legenda=None):
    """Empacota o corpo do diagrama num SVG pronto para a página."""
    _conferir()
    _reiniciar()

    largura_vb = LARGURA_BASE * K
    altura_vb = altura * K
    saida = (
        f'<svg viewBox="0 0 {largura_vb:.1f} {altura_vb:.1f}" '
        f'width="{largura_vb:.1f}" height="{altura_vb:.1f}" '
        f'style="display:block;width:100%;height:auto;margin:0 0 10px" '
        f'xmlns="http://www.w3.org/2000/svg" role="img">'
        '<defs><marker id="ponta" viewBox="0 0 10 10" refX="9" refY="5" '
        'markerWidth="6" markerHeight="6" orient="auto-start-reverse">'
        '<path d="M 0 0 L 10 5 L 0 10 z" fill="#8A7BA8"/></marker></defs>'
        f"{corpo}</svg>"
    )
    if legenda:
        saida += f'<p class="gh-fig__caption">{legenda}</p>'
    return ("svg", saida)
