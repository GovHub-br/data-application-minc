"""Documento 08 — Diagramas de Fluxos de Dados (Meta 02/03 · Produto 2).

Os fluxos aqui são os cruzamentos que produzem as camadas silver e gold: quais
modelos alimentam quais, por que chave se juntam e com que cardinalidade. Todos
os diagramas saem da linhagem real (`ref()` e `source()` de cada modelo).
"""

from diagrama import caixa, caminho, nomear, nota, rotulo_coluna, seta, svg

LEGENDA = (
    "svg",
    '<div class="gh-legenda">'
    '<span><i style="background:#F8F9FA;border-color:#B9A8DC;border-style:dashed"></i>fonte declarada</span>'
    '<span><i style="background:#F3EEFC;border-color:#C9B6EE"></i>bronze</span>'
    '<span><i style="background:#E7DCFB;border-color:#A98CE6"></i>silver</span>'
    '<span><i style="background:#7A34F3;border-color:#5B21B6"></i>gold</span>'
    '<span><i style="background:#FFF4E8;border-color:#F19F42"></i>operação</span>'
    '<span><i style="background:#FFF;border-color:#D8D8D8;border-style:dashed"></i>desabilitado</span>'
    "</div>",
)


# ───────────────────────── Diagrama 1 · visão geral ─────────────────────────

def _visao_geral():
    nomear("visão geral")
    c = ""
    for x, t in ((78, "origem"), (253, "bronze"), (428, "silver"), (603, "gold")):
        c += rotulo_coluna(x, 12, t)

    c += nota(8, 38, "Meta 3 · cotas", cor="#5B21B6", tamanho=9.5)
    c += caixa(8, 52, 140, ["6 planilhas", "+ território IBGE"], "fonte", 40, 9.5)
    c += caixa(183, 52, 140, ["9 modelos", "stg_*"], "bronze", 40, 9.5)
    c += caixa(358, 52, 140, ["9 modelos", "silver"], "silver", 40, 9.5)
    c += caixa(533, 52, 140, ["6 tabelas", "de consumo"], "gold", 40, 9.5)
    for x1, x2 in ((148, 183), (323, 358), (498, 533)):
        c += seta(x1, 72, x2 - 3, 72)

    c += nota(8, 128, "Meta 5 · agentes", cor="#5B21B6", tamanho=9.5)
    c += caixa(8, 142, 140, ["2 planilhas", "de proponentes"], "fonte", 40, 9.5)
    c += caixa(183, 142, 140, ["5 modelos", "de agentes"], "bronze", 40, 9.5)
    c += caixa(358, 142, 140, ["1 view", "+ 1 modelo"], "silver", 40, 9.5)
    c += caixa(533, 142, 140, ["4 tabelas", "de consumo"], "gold", 40, 9.5)
    for x1, x2 in ((148, 183), (323, 358), (498, 533)):
        c += seta(x1, 162, x2 - 3, 162)

    # Conector à direita da coluna de origem (as caixas terminam em x=148) e à
    # direita do rótulo de faixa, que é curto justamente para liberar este vão.
    c += caminho([(120, 92), (120, 138)], cor="#F19F42")
    c += nota(176, 113, "a mesma planilha alimenta os dois domínios", cor="#8A4B08")
    return svg(
        195,
        c,
        "Os dois domínios partem das mesmas planilhas de proponentes e só se "
        "separam no bronze: <code>cotas_dbt</code> normaliza documento para casar "
        "com pagamento, <code>agentes_dbt</code> preserva a resposta bruta sobre "
        "histórico de acesso.",
    )


# ──────────────────── Diagrama 2 · perfil unificado (cotas) ─────────────────

def _perfil_unificado():
    nomear("cruzamento 1 · perfil")
    c = ""
    nomes = [
        "stg_agentes_pf",
        "stg_agentes_pj",
        "stg_agentes_coletivos",
        "stg_agentes_pnab_pf",
        "stg_agentes_pnab_pj",
    ]
    for i, n in enumerate(nomes):
        y = 8 + i * 40
        c += caixa(6, y, 168, n, "bronze", 32, 9)
        c += seta(174, y + 16, 236, 104, curva=True)

    c += caixa(240, 88, 104, "UNION ALL", "macro", 32, 9.5)
    c += seta(344, 104, 384, 104)
    c += caixa(388, 80, 140, ["row_number()", "por documento"], "macro", 48, 9)
    c += seta(528, 104, 566, 104)
    c += caixa(570, 80, 104, ["perfil_", "agentes_", "normalizado"], "silver", 48, 8.5)
    c += nota(388, 148, "prioriza PF sobre PJ e coletivo", cor="#8A4B08")
    return svg(
        215,
        c,
        "Cruzamento 1: cinco perfis viram um. A deduplicação é por documento e "
        "não por tipo de proponente, e é isso que garante junção um-para-um com "
        "os pagamentos mais adiante.",
    )


# ─────────────────── Diagrama 3 · cascata de datação (cotas) ────────────────

def _cascata_datacao():
    nomear("cruzamento 2 · datação")
    c = ""
    c += caixa(110, 8, 200, "stg_contemplados_lpg", "bronze", 32, 9)
    c += caixa(370, 8, 200, "stg_contemplados_pnab", "bronze", 32, 9)
    c += caixa(292, 60, 96, "UNION ALL", "macro", 32, 9.5)
    c += seta(210, 40, 300, 56, curva=True)
    c += seta(470, 40, 380, 56, curva=True)

    # As três fontes de ano saem todas do conjunto unido, e não uma da outra: a
    # cascata é a ordem de consulta, não uma cadeia de dependência.
    c += caixa(6, 124, 210, ["1. nome_edital", "padrão NN/AAAA"], "macro", 44, 9)
    c += caixa(235, 124, 210, ["2. edital_ano_por_anexo", "número do edital, PNAB"], "macro", 44, 9)
    c += caixa(464, 124, 210, ["3. edital_ano_por_arquivo", "nome do arquivo XLSX"], "macro", 44, 9)
    c += seta(310, 92, 111, 120, curva=True)
    c += seta(340, 92, 340, 120)
    c += seta(370, 92, 569, 120, curva=True)

    # A caixa do coalesce cobre a largura das três, para que as três setas
    # cheguem nela em vez de morrerem no branco entre uma e outra.
    c += caixa(60, 188, 560, "coalesce(ano_nome, ano_anexo, ano_arquivo)", "macro", 32, 9)
    for x in (111, 340, 569):
        c += seta(x, 168, x, 184)

    c += caixa(220, 240, 240, ["contemplados_unif", "coluna ano_final"], "silver", 44, 9)
    c += seta(340, 220, 340, 236)
    c += nota(6, 300, "Fora do intervalo de 2013 a 2026 o ano é descartado; sem ano, a linha vira 'sem_ano'.")
    return svg(
        312,
        c,
        "Cruzamento 2: a datação em cascata. Cada fonte só é consultada quando a "
        "anterior não resolveu, e a coluna <code>origem_ano</code> registra qual "
        "delas respondeu por cada linha.",
    )


# ───────────────────── Diagrama 4 · o fato e as cotas ───────────────────────

def _fato_cotas():
    nomear("cruzamento 3 · fato")
    c = ""
    # Território numa linha própria: a junção com ele passa pelo perfil, não
    # pelos contemplados, e a seta precisa de vão para caber o rótulo da chave.
    c += caixa(440, 8, 200, "territorio_municipio", "silver", 32, 9)
    c += caixa(6, 76, 200, "contemplados_unif", "silver", 32, 9)
    c += caixa(236, 76, 200, ["perfil_agentes_", "normalizado"], "silver", 32, 8.5)

    # O vão entre as duas linhas é de 36 unidades de propósito: é o mínimo para
    # a pílula da chave caber entre a borda de baixo do território e a de cima
    # do perfil sem encostar em nenhuma das duas.
    c += caminho([(540, 40), (540, 58), (392, 58), (392, 72)], rotulo="chave_municipio_uf")
    c += seta(106, 108, 200, 148, curva=True)
    c += seta(336, 108, 336, 148, rotulo="identificador_unico")

    c += caixa(170, 152, 340, ["fct_pagamentos_elegiveis", "1 linha por pagamento"], "gold", 46, 9.5)

    c += seta(250, 198, 112, 234, curva=True)
    c += seta(340, 198, 340, 234)
    c += seta(430, 198, 568, 234, curva=True)
    c += caixa(6, 238, 208, "cobertura_pagamentos", "gold", 32, 9)
    c += caixa(236, 238, 208, "distribuicao_cotas_lpg", "gold", 32, 9)
    c += caixa(466, 238, 208, "distribuicao_cotas_pnab", "gold", 32, 9)
    c += nota(236, 288, "as duas pela macro distribuicao_cotas", cor="#8A4B08")
    return svg(
        300,
        c,
        "Cruzamento 3: o fato. As duas junções são <code>left join</code> de "
        "propósito, para que um pagamento sem perfil demográfico continue no "
        "denominador em vez de desaparecer da conta: ele permanece na tabela "
        "com <code>tem_perfil = false</code>.",
    )


# ────────────────────── Diagrama 5 · domínio de agentes ─────────────────────

def _fluxo_agentes():
    nomear("domínio de agentes")
    c = ""
    nomes = [
        "lpg_agentes_pf",
        "lpg_agentes_pj",
        "lpg_agentes_coletivos",
        "pnab_agentes_pf",
        "pnab_agentes_pj",
    ]
    for i, n in enumerate(nomes):
        y = 8 + i * 38
        c += caixa(6, y, 150, n, "bronze", 30, 8.5)
        c += seta(156, y + 15, 182, 106, curva=True)

    c += caixa(186, 84, 140, ["identificadores_", "agentes"], "silver", 44, 8.5)
    c += nota(186, 142, "UNION ALL", cor="#8A4B08")

    c += caixa(366, 16, 150, ["perfil_agentes_", "historico"], "silver", 40, 8.5)
    c += caixa(366, 104, 150, ["perfil_acesso_", "fomento"], "gold", 40, 8.5)
    c += caixa(366, 196, 150, ["perfil_agentes_", "completo"], "gold", 40, 8.5)
    c += caixa(546, 16, 128, ["primeiro_", "acesso_resumo"], "gold", 40, 8)
    c += caixa(546, 104, 128, ["primeiro_acesso_", "contemplados"], "gold", 44, 8)
    c += caixa(546, 200, 128, ["planilha_", "contemplados_*"], "fonte", 36, 8)

    c += seta(326, 96, 362, 36, curva=True)
    c += seta(326, 116, 362, 124, curva=True)
    # perfil_agentes_completo lê as duas pontas: a view (resposta bruta) e a
    # tabela de acesso (classificação). A ligação com a view desce por fora, em
    # ângulo reto: em diagonal ela roçaria a caixa de perfil_acesso_fomento e
    # chegaria emaranhada com a outra seta no mesmo destino.
    c += caminho([(256, 128), (256, 216), (362, 216)])
    c += seta(441, 144, 441, 192)
    c += seta(516, 124, 542, 124)
    c += seta(516, 36, 542, 36)
    c += seta(610, 200, 610, 152)
    return svg(
        250,
        c,
        "Domínio de agentes. <code>perfil_agentes_completo</code> lê as duas "
        "pontas, a view de identificadores e a tabela de acesso, para recuperar a "
        "resposta bruta ao lado da classificação final. A inferência de primeiro "
        "acesso acontece em <code>perfil_acesso_fomento</code>, por "
        "<code>row_number()</code> particionado por documento.",
    )


# ─────────────────────── Diagrama 6 · o ramo desligado ──────────────────────

def _ramo_desligado():
    nomear("ramo desligado")
    c = ""
    c += caixa(6, 8, 190, "bbagil.fato_bbagil", "fonte", 32, 9)
    c += caixa(6, 64, 190, ["transferegov.", "plano_acao_minc"], "fonte", 36, 8.5)
    c += seta(196, 24, 234, 24)
    c += seta(196, 82, 234, 82)
    c += caixa(238, 8, 150, "stg_bbagil", "off", 32, 9)
    c += caixa(238, 66, 150, "bbagil_ente_ano", "off", 32, 9)
    c += caixa(238, 124, 150, ["perfil_agentes_", "normalizado"], "silver", 32, 8.5)

    c += seta(388, 24, 430, 62, curva=True)
    c += seta(388, 82, 430, 72, curva=True)
    c += seta(388, 140, 430, 84, curva=True)
    c += caixa(434, 56, 190, "fct_pagamentos_bbagil", "off", 32, 9)
    c += seta(529, 88, 529, 128, rotulo="valor pago")
    c += caixa(434, 132, 190, ["comparativo_", "recebido_vs_pago"], "off", 36, 8.5)

    # O lado ativo entra no comparativo por baixo: a faixa entre y=156 e y=196
    # está livre, então a seta passa sob a caixa de perfil sem cruzá-la.
    c += caixa(6, 180, 190, "fct_pagamentos_elegiveis", "gold", 32, 8.5)
    c += caminho([(196, 196), (466, 196), (466, 172)], rotulo="valor recebido")
    c += nota(6, 232, "Bloqueado na origem: a DAG extracao_bbagil_dag falha na autenticação SCA.")
    return svg(
        244,
        c,
        "O ramo que mede valor efetivamente pago está desenhado e desligado. "
        "Quatro modelos aguardam a extração do BB Ágil concluir, e devem ser "
        "reativados em conjunto.",
    )


DOC = {
    "slug": "08-diagramas-de-fluxos-de-dados",
    "titulo": "Diagramas de Fluxos de Dados",
    "subtitulo": (
        "Os cruzamentos que produzem as camadas silver e gold da Plataforma de "
        "Dados MinC: quais modelos alimentam quais, por que chave se juntam e o "
        "que cada junção preserva ou descarta."
    ),
    "rodape": "Diagramas de Fluxos",
    "meta": [
        ("Meta 02/03 · Produto 2", "Arquitetura e diagramas de fluxos de dados."),
    ],
    "capitulos": [
        # ─────────────────────────────────────────────────────────────────
        {
            "num": "01",
            "eyebrow": "Sobre este documento",
            "titulo": "O que é um fluxo aqui",
            "icone": "workflow",
            "paginas": [
                [
                    (
                        "lead",
                        "Um fluxo de dados, neste documento, é um cruzamento: a operação que "
                        "pega dois ou mais conjuntos e produz um terceiro. Não é o desenho da "
                        "infraestrutura por onde o dado trafega, e sim a cadeia de junções, "
                        "uniões e agregações que transforma pouso bruto em tabela de consumo.",
                    ),
                    ("h3", "De onde vêm estes diagramas"),
                    (
                        "p",
                        "Nenhum foi desenhado à parte. Cada seta corresponde a uma chamada "
                        "<code>ref()</code> ou <code>source()</code> encontrada no SQL dos "
                        "modelos, e cada rótulo de junção corresponde à cláusula "
                        "<code>on</code> daquele cruzamento. Um modelo novo que ninguém "
                        "diagramar aparece como lacuna, porque o desenho é derivado da "
                        "linhagem e não da memória de quem escreveu.",
                    ),
                    (
                        "stats",
                        [
                            ("36", "modelos na linhagem"),
                            ("6", "cruzamentos principais"),
                            ("4", "modelos desligados", True),
                            ("2", "domínios"),
                        ],
                    ),
                    ("h3", "Como ler os diagramas"),
                    LEGENDA,
                    (
                        "p",
                        "As caixas seguem a camada: quanto mais escura, mais perto do "
                        "consumo. As laranjas não são tabelas, e sim operações "
                        "(<code>UNION ALL</code>, deduplicação, cascata de datação). Os "
                        "rótulos sobre as setas são a chave de junção.",
                    ),
                    (
                        "callout",
                        "shield-check",
                        "Junção é decisão, não detalhe",
                        [
                            "A escolha entre <code>inner join</code> e <code>left join</code> "
                            "muda o número publicado. Onde este documento marca "
                            "<code>left join</code>, a decisão foi preservar registros sem "
                            "correspondência, e cada uma dessas escolhas está anotada no "
                            "diagrama correspondente.",
                        ],
                    ),
                ],
            ],
        },
        # ─────────────────────────────────────────────────────────────────
        {
            "num": "02",
            "eyebrow": "Visão geral",
            "titulo": "O caminho completo",
            "icone": "table-cells",
            "paginas": [
                [
                    (
                        "p",
                        "A plataforma tem dois domínios que partem da mesma origem e não se "
                        "encontram de novo até a camada de consumo, onde compartilham a chave "
                        "de agente.",
                    ),
                    _visao_geral(),
                    ("h3", "Por que a mesma planilha vira dois bronzes diferentes"),
                    (
                        "p",
                        "As planilhas de proponentes alimentam tanto "
                        "<code>cotas_dbt</code> quanto <code>agentes_dbt</code>, e cada "
                        "domínio extrai delas uma coisa distinta. O bronze de cotas normaliza "
                        "o documento com <code>normaliza_documento</code>, porque precisa "
                        "casar com o pagamento; o bronze de agentes preserva a resposta bruta "
                        "sobre histórico de acesso a fomento, porque é dela que sai o "
                        "indicador de primeiro acesso.",
                    ),
                    (
                        "p",
                        "Duplicar a leitura da origem é deliberado. A alternativa, um bronze "
                        "único servindo aos dois, faria cada domínio carregar transformações "
                        "de que não precisa e acoplaria as duas metas num só modelo.",
                    ),
                ],
            ],
        },
        # ─────────────────────────────────────────────────────────────────
        {
            "num": "03",
            "eyebrow": "Meta 3",
            "titulo": "Os cruzamentos das cotas",
            "icone": "chart-pie",
            "paginas": [
                [
                    (
                        "p",
                        "Três cruzamentos encadeados produzem as tabelas de cota. O primeiro "
                        "resolve quem é o agente, o segundo resolve quando o pagamento "
                        "aconteceu, e o terceiro junta os dois lados.",
                    ),
                    ("h3", "Cruzamento 1: o lado-agente"),
                    _perfil_unificado(),
                    (
                        "callout",
                        "check-badge",
                        "A deduplicação define a cardinalidade de tudo o que vem depois",
                        [
                            "Uma pessoa pode aparecer como pessoa física num edital e como "
                            "representante de coletivo em outro. Deduplicar por documento, e "
                            "não pelo par documento e tipo, é o que reduz o perfil a uma linha "
                            "por agente e torna a junção com pagamento um-para-um. Sem isso, "
                            "um pagamento casaria com duas linhas de perfil e o valor seria "
                            "contado em dobro.",
                        ],
                    ),
                ],
                [
                    ("h3", "Cruzamento 2: o lado-valor e a data"),
                    (
                        "p",
                        "O ano do edital não vem preenchido em nenhuma das fontes. Ele é "
                        "derivado em cascata, e a cascata é ela própria um cruzamento: cada "
                        "tentativa é um <code>left join</code> por identificador de anexo.",
                    ),
                    _cascata_datacao(),
                    (
                        "table",
                        ["Fonte da data", "De onde o ano é extraído", "Alcance"],
                        [
                            [
                                "1. <code>nome_edital</code>",
                                "Do próprio nome do edital, quando segue o padrão "
                                "<code>NN/AAAA</code>.",
                                "LPG e PNAB",
                            ],
                            [
                                "2. <code>edital_ano_por_anexo</code>",
                                "Do campo “número do edital” nas abas de definição do PNAB. "
                                "Mais completo que o nome.",
                                "PNAB",
                            ],
                            [
                                "3. <code>edital_ano_por_arquivo</code>",
                                "Do nome do arquivo XLSX de origem, que costuma carregar o "
                                "ano.",
                                "LPG e PNAB",
                            ],
                        ],
                        "A coluna <code>origem_ano</code> do resultado guarda qual das três "
                        "respondeu, o que torna possível medir a confiabilidade da série "
                        "temporal por fonte de datação.",
                        ["27%", "55%", "18%"],
                    ),
                ],
                [
                    ("h3", "Cruzamento 3: o fato de pagamentos"),
                    (
                        "p",
                        "É aqui que os dois lados se encontram. O território entra por "
                        "tabela, não direto: ele se junta ao perfil, porque é o perfil que "
                        "carrega município e UF do agente.",
                    ),
                    _fato_cotas(),
                    (
                        "callout",
                        "shield-check",
                        "Por que a cota territorial só existe na LPG",
                        [
                            "O diagrama mostra a razão: <code>territorio_municipio</code> se "
                            "junta por <code>chave_municipio_uf</code>, que vem do perfil do "
                            "agente. No PNAB, o lado-valor não traz localização, então essa "
                            "chave é nula e a quarta cota não é calculável.",
                        ],
                    ),
                ],
            ],
        },
        # ─────────────────────────────────────────────────────────────────
        {
            "num": "04",
            "eyebrow": "Meta 5",
            "titulo": "Os cruzamentos dos agentes",
            "icone": "check-badge",
            "paginas": [
                [
                    (
                        "p",
                        "O domínio de agentes tem uma topologia diferente: em vez de juntar "
                        "conjuntos distintos, ele consolida cinco fontes equivalentes e depois "
                        "deriva classificações sucessivas sobre o mesmo conjunto.",
                    ),
                    _fluxo_agentes(),
                    (
                        "p",
                        "A view <code>identificadores_agentes</code> é o ponto de "
                        "estrangulamento do domínio: tudo passa por ela. Como é materializada "
                        "como <code>view</code>, consolidar os cinco bronzes não custa "
                        "armazenamento, e qualquer correção num bronze se propaga sem recarga.",
                    ),
                ],
                [
                    ("h3", "As duas classificações e a diferença entre elas"),
                    (
                        "table",
                        ["Modelo", "Grão", "Como classifica"],
                        [
                            [
                                "<code>perfil_agentes_historico</code>",
                                "agente × programa",
                                "Só higieniza e padroniza a resposta declarada. Não infere "
                                "nada.",
                            ],
                            [
                                "<code>perfil_acesso_fomento</code>",
                                "agente × programa",
                                "Mantém a resposta confirmada e infere as omissas por "
                                "<code>row_number()</code> sobre o programa.",
                            ],
                            [
                                "<code>perfil_agentes_completo</code>",
                                "1 linha por agente",
                                "Consolida por documento e acrescenta detecção de veterania "
                                "por presença em mais de um programa.",
                            ],
                        ],
                        None,
                        ["30%", "22%", "48%"],
                    ),
                    (
                        "callout",
                        "check-badge",
                        "Dois caminhos para o mesmo indicador",
                        [
                            "<code>primeiro_acesso_resumo</code> desce por "
                            "<code>perfil_agentes_historico</code>, que só usa resposta "
                            "declarada. <code>primeiro_acesso_contemplados</code> desce por "
                            "<code>perfil_acesso_fomento</code>, que inclui inferência, e "
                            "cruza com as listas de contemplados.",
                            "Os dois respondem à mesma pergunta por caminhos diferentes, e "
                            "por isso podem divergir. A divergência não é erro: é a medida do "
                            "quanto a inferência está pesando no indicador.",
                        ],
                    ),
                ],
            ],
        },
        # ─────────────────────────────────────────────────────────────────
        {
            "num": "05",
            "eyebrow": "Referência",
            "titulo": "Chaves e cardinalidade",
            "icone": "list-bullet",
            "paginas": [
                [
                    (
                        "p",
                        "Toda junção da plataforma, com a chave usada, o tipo e o efeito da "
                        "escolha sobre o resultado.",
                    ),
                    (
                        "table",
                        ["Cruzamento", "Chave", "Tipo", "O que a escolha preserva"],
                        [
                            [
                                "<code>perfil_agentes_normalizado</code>",
                                "<code>identificador_unico</code>",
                                "dedup",
                                "Uma linha por documento, com prioridade para pessoa física.",
                            ],
                            [
                                "<code>contemplados_unif</code> × anexo",
                                "<code>anexo_id</code>",
                                "left",
                                "A linha sem ano derivável permanece, marcada "
                                "<code>sem_ano</code>.",
                            ],
                            [
                                "<code>fct_*</code> × perfil",
                                "<code>identificador_unico</code>",
                                "left",
                                "O pagamento sem perfil casado permanece, com "
                                "<code>tem_perfil = false</code>.",
                            ],
                            [
                                "<code>fct_*</code> × território",
                                "<code>chave_municipio_uf</code>",
                                "left",
                                "O agente sem município identificado permanece, fora da cota "
                                "territorial.",
                            ],
                            [
                                "<code>perfil_agentes_completo</code>",
                                "<code>identificador_unico</code>",
                                "distinct on",
                                "O primeiro programa de cada agente vira o registro canônico.",
                            ],
                            [
                                "<code>identificadores_agentes</code>",
                                "sem chave",
                                "union all",
                                "Todas as linhas dos cinco bronzes, sem deduplicar.",
                            ],
                        ],
                        "<code>distinct on</code> é sintaxe do Postgres: mantém a primeira "
                        "linha de cada grupo segundo a ordenação declarada.",
                        ["30%", "24%", "14%", "32%"],
                    ),
                    (
                        "callout",
                        "shield-check",
                        "O padrão por trás de todas elas",
                        [
                            "Nenhuma junção da plataforma descarta registro por falta de "
                            "correspondência. A decisão é sempre preservar a linha e sinalizar "
                            "a ausência numa coluna, para que o denominador continue "
                            "auditável.",
                            "É por isso que existem <code>tem_perfil</code>, "
                            "<code>origem_ano</code> e <code>status_origem</code>: cada uma "
                            "delas é a cicatriz de uma junção que não casou.",
                        ],
                    ),
                ],
            ],
        },
        # ─────────────────────────────────────────────────────────────────
        {
            "num": "06",
            "eyebrow": "Fechamento",
            "titulo": "O ramo desligado",
            "icone": "governance",
            "paginas": [
                [
                    (
                        "p",
                        "Um quinto cruzamento está inteiramente escrito e não executa. Ele é "
                        "o que mediria valor efetivamente pago ao beneficiário final, em vez "
                        "de valor repassado aos entes.",
                    ),
                    _ramo_desligado(),
                    ("h3", "O que muda quando ele voltar"),
                    (
                        "p",
                        "<code>comparativo_recebido_vs_pago</code> existe para confrontar os "
                        "dois lados-valor: o que <code>fct_pagamentos_elegiveis</code> mede "
                        "hoje, na ordem de R$ 2,7 bilhões repassados, contra o que "
                        "<code>fct_pagamentos_bbagil</code> mediria, na ordem de R$ 447 "
                        "milhões pagos. Enquanto o ramo estiver desligado, esse confronto não "
                        "acontece e a diferença fica invisível para quem lê as cotas.",
                    ),
                    (
                        "callout",
                        "clipboard-document-check",
                        "Reativação é uma operação só",
                        [
                            "Os quatro modelos devem sair de <code>enabled=false</code> em "
                            "conjunto, depois que <code>extracao_bbagil_dag</code> concluir "
                            "com sucesso. A instrução está registrada na descrição de cada um. "
                            "Reativar parcialmente quebra a cadeia, porque cada um depende do "
                            "anterior.",
                        ],
                    ),
                ],
            ],
        },
    ],
}
