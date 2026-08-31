"""Documento 02 — Dicionário de Dados (Meta 02 · Produto 3)."""

DOC = {
    "slug": "02-dicionario-de-dados",
    "titulo": "Dicionário de Dados",
    "subtitulo": (
        "O significado de cada tabela e de cada coluna produzida pela camada de "
        "transformação da Plataforma de Dados MinC, organizado por domínio e por "
        "camada, com as ressalvas necessárias para citar seus números."
    ),
    "rodape": "Dicionário de Dados",
    "meta": [
        ("Meta 02 · Produto 3", "Modelagem de dados, dicionário de dados e metadados."),
    ],
    "capitulos": [
        # ─────────────────────────────────────────────────────────────────
        {
            "num": "01",
            "eyebrow": "Sobre este documento",
            "titulo": "Objetivo e escopo",
            "icone": "book-open",
            "paginas": [
                [
                    (
                        "lead",
                        "Este documento consolida o significado das tabelas produzidas pela "
                        "camada de transformação da Plataforma de Dados MinC. Ele descreve o "
                        "que cada modelo representa, qual o seu grão, de onde vêm seus dados "
                        "e o que é preciso saber antes de usar seus números.",
                    ),
                    ("h3", "O que está aqui"),
                    (
                        "p",
                        "Os 36 modelos do projeto dbt, distribuídos em dois domínios de "
                        "negócio e uma camada de infraestrutura, com as 71 colunas hoje "
                        "documentadas coluna a coluna. As descrições não foram escritas para "
                        "este documento: elas são as descrições que vivem nos arquivos "
                        "<code>schema.yml</code> e <code>descriptions.yml</code> do próprio "
                        "projeto, e que alimentam também o catálogo de metadados e o "
                        "catálogo externo.",
                    ),
                    (
                        "stats",
                        [
                            ("36", "modelos documentados"),
                            ("2", "domínios de negócio"),
                            ("71", "colunas descritas"),
                            ("4", "camadas"),
                        ],
                    ),
                    ("h3", "O que não está aqui"),
                    (
                        "p",
                        "As tabelas de pouso bruto da camada bronze do SALIC, que somam 561 "
                        "tabelas e 5.064 colunas declaradas como fontes. Elas têm nome, "
                        "schema e testes de integridade declarados, mas ainda não têm "
                        "descrição de negócio, porque nenhum modelo as consome. Estão "
                        "listadas no capítulo 05, no inventário de fontes.",
                    ),
                    (
                        "callout",
                        "document-magnifying-glass",
                        "Este documento é derivado, não redigido",
                        [
                            "Toda descrição aqui foi extraída dos arquivos de configuração do "
                            "projeto dbt. Corrigir uma descrição significa corrigir o "
                            "<code>schema.yml</code> correspondente e gerar este documento de "
                            "novo, nunca editar o PDF.",
                        ],
                    ),
                ],
            ],
        },
        # ─────────────────────────────────────────────────────────────────
        {
            "num": "02",
            "eyebrow": "Convenções",
            "titulo": "Como ler o dicionário",
            "icone": "list-bullet",
            "paginas": [
                [
                    ("h3", "As quatro camadas"),
                    (
                        "p",
                        "A plataforma segue a arquitetura Medallion. Cada camada tem uma "
                        "responsabilidade distinta, e a materialização de cada uma é definida "
                        "por camada no <code>dbt_project.yml</code>, não modelo a modelo.",
                    ),
                    (
                        "table",
                        ["Camada", "Responsabilidade", "Materialização"],
                        [
                            [
                                "<strong>bronze</strong>",
                                "Pouso do dado de origem com o mínimo de tratamento: "
                                "normalização de documento, descarte de resíduo de parsing e "
                                "unificação de fontes equivalentes.",
                                "<code>incremental</code> em agentes, <code>table</code> em "
                                "cotas",
                            ],
                            [
                                "<strong>silver</strong>",
                                "Regras de negócio, deduplicação, derivação de ano e "
                                "cruzamentos entre entidades.",
                                "<code>view</code> em cotas, <code>table</code> em agentes",
                            ],
                            [
                                "<strong>gold</strong>",
                                "Tabelas de consumo, com o grão da pergunta que respondem. É "
                                "o que se cita.",
                                "<code>table</code>",
                            ],
                            [
                                "<strong>views</strong>",
                                "Consolidação sem custo de materialização, usada como ponte "
                                "entre bronze e silver.",
                                "<code>view</code>",
                            ],
                        ],
                        None,
                        ["16%", "50%", "34%"],
                    ),
                    ("h3", "Convenções de nome"),
                    (
                        "ul",
                        [
                            "<code>stg_</code> marca modelo de bronze no domínio de cotas, "
                            "vindo direto de uma fonte crua.",
                            "<code>fct_</code> marca tabela-fato de gold, com uma linha por "
                            "evento.",
                            "<code>_unif</code> marca modelo que unifica LPG e PNAB numa "
                            "estrutura só.",
                            "<code>diag_</code> marca modelo de diagnóstico: existe para "
                            "auditar o pipeline, não para ser citado.",
                        ],
                    ),
                    (
                        "callout",
                        "shield-check",
                        "Grão importa mais que nome",
                        [
                            "Antes de somar qualquer coluna, confira o grão declarado na "
                            "descrição do modelo. <code>fct_pagamentos_elegiveis</code> tem "
                            "uma linha por pagamento; <code>perfil_agentes_completo</code> "
                            "tem uma linha por documento. Somar valor no segundo produz um "
                            "número sem significado.",
                        ],
                    ),
                ],
            ],
        },
        # ─────────────────────────────────────────────────────────────────
        {
            "num": "03",
            "eyebrow": "Domínio · Meta 3",
            "titulo": "Cotas e territórios",
            "icone": "chart-pie",
            "paginas": [
                [
                    (
                        "p",
                        "Domínio <code>cotas_dbt</code>, schema <code>minc_cotas</code>. "
                        "Acompanha o cumprimento das cotas legais de raça, etnia, deficiência "
                        "e território nos programas da Lei Paulo Gustavo e da Política "
                        "Nacional Aldir Blanc. São 24 modelos.",
                    ),
                    ("h3", "Bronze: pouso e limpeza"),
                    (
                        "table",
                        ["Modelo", "O que representa"],
                        [
                            [
                                "<code>stg_agentes_pf</code>",
                                "Perfil LPG de pessoa física, unindo 3 fontes (base, "
                                "audiovisual e multicultural). Descarta linhas sem CPF válido.",
                            ],
                            [
                                "<code>stg_agentes_pj</code>",
                                "Perfil LPG de pessoa jurídica. A demografia é do corpo "
                                "diretivo da PJ, não de um indivíduo.",
                            ],
                            [
                                "<code>stg_agentes_coletivos</code>",
                                "Perfil LPG de coletivos e grupos, unindo 4 fontes. A "
                                "demografia é da maioria do grupo.",
                            ],
                            [
                                "<code>stg_agentes_pnab_pf</code>",
                                "Perfil PNAB de pessoa física, com CPF real e raça, PCD, "
                                "indígena e quilombola do próprio indivíduo.",
                            ],
                            [
                                "<code>stg_agentes_pnab_pj</code>",
                                "Perfil PNAB de organizações, com CNPJ real e demografia do "
                                "representante legal.",
                            ],
                            [
                                "<code>stg_contemplados_lpg</code>",
                                "Lado-valor da LPG. Resolve colunas duplicadas por "
                                "schema-drift na ingestão das planilhas.",
                            ],
                            [
                                "<code>stg_contemplados_pnab</code>",
                                "Lado-valor do PNAB, unindo as listas geral e cultura viva.",
                            ],
                            [
                                "<code>stg_editais</code>",
                                "Editais da LPG, unindo a base e 4 tabelas de instrumentos.",
                            ],
                            [
                                "<code>stg_bbagil</code>",
                                "<strong>Desabilitado.</strong> Extrato bancário do BB Ágil, "
                                "aguardando a DAG de extração concluir.",
                            ],
                        ],
                        None,
                        ["30%", "70%"],
                    ),
                ],
                [
                    ("h3", "Silver: regras de negócio"),
                    (
                        "table",
                        ["Modelo", "O que representa"],
                        [
                            [
                                "<code>perfil_agentes_normalizado</code>",
                                "Perfil unificado LPG e PNAB a partir dos cinco bronzes. "
                                "Deduplicado por <code>identificador_unico</code>, uma linha "
                                "por documento, para permitir junção um-para-um com "
                                "pagamentos.",
                            ],
                            [
                                "<code>contemplados_unif</code>",
                                "Núcleo do lado-valor das cotas. Deriva o ano do edital numa "
                                "cascata de três fontes, todas validadas no intervalo de 2013 "
                                "a 2026.",
                            ],
                            [
                                "<code>edital_ano_por_anexo</code>",
                                "Segunda fonte da cascata de datação: ano extraído do número "
                                "do edital nas abas de definição do PNAB.",
                            ],
                            [
                                "<code>edital_ano_por_arquivo</code>",
                                "Terceira fonte da cascata: ano extraído do nome do arquivo "
                                "XLSX de origem, usada só quando as duas anteriores não "
                                "resolvem.",
                            ],
                            [
                                "<code>editais_unif</code>",
                                "Editais unificados com ano derivado. Mantido para "
                                "reconciliação e auditoria, não é o caminho de ano usado "
                                "pelas cotas.",
                            ],
                            [
                                "<code>territorio_municipio</code>",
                                "Crosswalk do IBGE (Censo 2022) colapsado do grão de setor "
                                "censitário para o grão de município.",
                            ],
                            [
                                "<code>identificadores_agentes_cotas</code>",
                                "Chaves distintas de agentes, usada para teste de unicidade e "
                                "contagem secundária de proponentes.",
                            ],
                            [
                                "<code>diag_valores_cortados</code>",
                                "Diagnóstico: audita o que o teto de R$ 10 milhões do parser "
                                "de valor descartou.",
                            ],
                            [
                                "<code>bbagil_ente_ano</code>",
                                "<strong>Desabilitado.</strong> Datação e localização do BB "
                                "Ágil por ente.",
                            ],
                        ],
                        None,
                        ["32%", "68%"],
                    ),
                ],
                [
                    ("h3", "Gold: tabelas de consumo"),
                    (
                        "table",
                        ["Modelo", "Grão", "O que representa"],
                        [
                            [
                                "<code>fct_pagamentos_elegiveis</code>",
                                "1 linha por pagamento",
                                "Base do denominador das cotas. Preserva todos os pagamentos, "
                                "inclusive os órfãos, sem perfil demográfico casado.",
                            ],
                            [
                                "<code>cobertura_pagamentos</code>",
                                "1 linha por ano",
                                "Teto de confiabilidade das cotas. Leitura recomendada "
                                "<strong>antes</strong> de consultar qualquer distribuição.",
                            ],
                            [
                                "<code>distribuicao_cotas_lpg</code>",
                                "ano × grupo",
                                "Cotas da LPG, ponderadas por valor. Quatro grupos: negra "
                                "25%, indígena 10%, PCD 5%, território 20%.",
                            ],
                            [
                                "<code>distribuicao_cotas_pnab</code>",
                                "ano × grupo",
                                "Cotas do PNAB. Três grupos, sem a cota territorial: o "
                                "lado-valor do PNAB não traz localização do agente.",
                            ],
                            [
                                "<code>fct_pagamentos_bbagil</code>",
                                "1 linha por pagamento",
                                "<strong>Desabilitado.</strong> Valor efetivamente pago ao "
                                "beneficiário final, via extrato bancário.",
                            ],
                            [
                                "<code>comparativo_recebido_vs_pago</code>",
                                "ano × ente",
                                "<strong>Desabilitado.</strong> Confronta os dois lados-valor: "
                                "o repassado e o pago.",
                            ],
                        ],
                        None,
                        ["30%", "18%", "52%"],
                    ),
                    (
                        "callout",
                        "shield-check",
                        "Ressalva metodológica registrada no código",
                        [
                            "Enquanto <code>fct_pagamentos_bbagil</code> estiver desabilitado, "
                            "o denominador das cotas do PNAB mede valor "
                            "<strong>recebido</strong> por entes federados, na ordem de R$ 2,7 "
                            "bilhões, e não valor <strong>pago</strong> a pessoas, na ordem de "
                            "R$ 447 milhões.",
                            "A ressalva está no comentário de "
                            "<code>distribuicao_cotas_pnab.sql</code> e deve acompanhar "
                            "qualquer citação desses percentuais.",
                        ],
                    ),
                ],
                [
                    ("h3", "Colunas com domínio de valores fechado"),
                    (
                        "p",
                        "As colunas abaixo têm o conjunto de valores permitidos declarado e "
                        "testado. Qualquer valor fora da lista faz o teste falhar na execução.",
                    ),
                    (
                        "table",
                        ["Modelo", "Coluna", "Papel"],
                        [
                            [
                                "<code>fct_pagamentos_elegiveis</code>",
                                "<code>origem_ano</code>",
                                "Qual das três fontes da cascata resolveu o ano daquele "
                                "pagamento.",
                            ],
                            [
                                "<code>fct_pagamentos_elegiveis</code>",
                                "<code>identificador_unico</code>",
                                "Documento normalizado do agente, só dígitos. Chave "
                                "estrangeira do modelo semântico.",
                            ],
                            [
                                "<code>distribuicao_cotas_lpg</code>",
                                "<code>grupo</code>",
                                "Grupo da cota: negra, indígena, PCD ou território.",
                            ],
                            [
                                "<code>distribuicao_cotas_lpg</code>",
                                "<code>status_sobre_com_perfil</code>",
                                "Veredito de cumprimento medido sobre o denominador com "
                                "perfil identificado.",
                            ],
                            [
                                "<code>distribuicao_cotas_pnab</code>",
                                "<code>grupo</code>",
                                "Mesmos grupos da LPG, sem território.",
                            ],
                            [
                                "<code>contemplados_unif</code>",
                                "<code>origem</code>",
                                "Programa de origem do registro.",
                            ],
                            [
                                "<code>contemplados_unif</code>",
                                "<code>origem_ano</code>",
                                "Fonte da datação daquela linha.",
                            ],
                            [
                                "<code>perfil_agentes_normalizado</code>",
                                "<code>raca_normalizada</code>",
                                "Raça ou cor padronizada do agente.",
                            ],
                            [
                                "<code>stg_contemplados_lpg</code>",
                                "<code>origem</code>",
                                "Lista de contemplados de onde a linha veio.",
                            ],
                            [
                                "<code>stg_contemplados_pnab</code>",
                                "<code>origem</code>",
                                "Lista de contemplados de onde a linha veio.",
                            ],
                        ],
                        None,
                        ["32%", "27%", "41%"],
                    ),
                ],
            ],
        },
        # ─────────────────────────────────────────────────────────────────
        {
            "num": "04",
            "eyebrow": "Domínio · Meta 5",
            "titulo": "Perfil e primeiro acesso",
            "icone": "check-badge",
            "paginas": [
                [
                    (
                        "p",
                        "Domínio <code>agentes_dbt</code>, schema <code>agentes</code>. "
                        "Mapeia os agentes culturais que acessam fomento público pela "
                        "primeira vez. São 11 modelos, e é o domínio com documentação de "
                        "coluna mais completa do projeto.",
                    ),
                    ("h3", "Bronze e views"),
                    (
                        "table",
                        ["Modelo", "O que representa"],
                        [
                            [
                                "<code>lpg_agentes_pf</code>",
                                "Pessoas físicas da LPG. CPF como identificador único, carga "
                                "incremental deduplicada, descartando CPF nulo, vazio ou com "
                                "os literais <code>nan</code> e <code>none</code>.",
                            ],
                            [
                                "<code>lpg_agentes_pj</code>",
                                "Pessoas jurídicas da LPG, com CNPJ como identificador.",
                            ],
                            [
                                "<code>lpg_agentes_coletivos</code>",
                                "Proponentes coletivos da LPG. O identificador é o CPF do "
                                "representante do grupo.",
                            ],
                            [
                                "<code>pnab_agentes_pf</code>",
                                "Pessoas físicas do PNAB, com histórico de acesso a fomento "
                                "nos últimos 5 anos.",
                            ],
                            [
                                "<code>pnab_agentes_pj</code>",
                                "Organizações do PNAB.",
                            ],
                            [
                                "<code>identificadores_agentes</code>",
                                "View que consolida os cinco bronzes em um único conjunto via "
                                "<code>UNION ALL</code>, padronizando o identificador e "
                                "acrescentando programa e tipo de proponente.",
                            ],
                        ],
                        None,
                        ["30%", "70%"],
                    ),
                    ("h3", "Silver"),
                    (
                        "table",
                        ["Modelo", "O que representa"],
                        [
                            [
                                "<code>perfil_agentes_historico</code>",
                                "Higieniza a resposta sobre acesso anterior a fomento: remove "
                                "nulos, o literal <code>nan</code> e pontuação inválida, e "
                                "classifica em três categorias padronizadas.",
                            ],
                        ],
                        None,
                        ["30%", "70%"],
                    ),
                ],
                [
                    ("h3", "Gold: colunas documentadas"),
                    (
                        "p",
                        "<code>perfil_agentes_completo</code> é o master data de proponentes "
                        "do domínio: uma linha por CPF ou CNPJ.",
                    ),
                    (
                        "table",
                        ["Coluna", "Descrição", "Testes"],
                        [
                            [
                                "<code>identificador_unico</code>",
                                "CPF ou CNPJ do proponente, normalizado. Chave única do "
                                "modelo.",
                                "<code>not_null</code> <code>unique</code>",
                            ],
                            [
                                "<code>tipo_proponente</code>",
                                "Pessoa Física, Pessoa Jurídica, Coletivo ou Organização.",
                                "<code>not_null</code>",
                            ],
                            [
                                "<code>programa_fomento</code>",
                                "Programa de origem do primeiro registro do proponente.",
                                "<code>not_null</code>",
                            ],
                            [
                                "<code>historico_acesso_bruto</code>",
                                "Resposta original do proponente, sem normalização.",
                                "",
                            ],
                            [
                                "<code>status_origem</code>",
                                "Flag de qualidade: Confirmado quando o proponente respondeu "
                                "explicitamente, Inferido quando a classificação foi deduzida.",
                                "<code>not_null</code> <code>accepted_values</code>",
                            ],
                            [
                                "<code>perfil_classificacao</code>",
                                "Classificação final consolidada, uma por proponente.",
                                "<code>not_null</code> <code>accepted_values</code>",
                            ],
                        ],
                        None,
                        ["27%", "48%", "25%"],
                    ),
                    (
                        "callout",
                        "shield-check",
                        "Inferência é sinalizada, não escondida",
                        [
                            "Quando o proponente não respondeu, a classificação de primeiro "
                            "acesso é inferida por ordem cronológica entre programas. Toda "
                            "linha inferida carrega <code>status_origem = 'Inferido'</code>, "
                            "e as tabelas de resumo separam "
                            "<code>total_campo_preenchido</code> de "
                            "<code>total_inferido</code>. Nenhum número agregado mistura os "
                            "dois sem dizer.",
                        ],
                    ),
                ],
                [
                    ("h3", "Demais tabelas de gold"),
                    (
                        "table",
                        ["Modelo", "Grão", "O que representa"],
                        [
                            [
                                "<code>perfil_acesso_fomento</code>",
                                "proponente × programa",
                                "Classifica cada proponente por programa. Respostas "
                                "confirmadas são mantidas; omitidas recebem inferência "
                                "cronológica.",
                            ],
                            [
                                "<code>primeiro_acesso_resumo</code>",
                                "programa × categoria",
                                "Resumo agregado do indicador de primeiro acesso. Cada "
                                "programa soma 100%.",
                            ],
                            [
                                "<code>primeiro_acesso_contemplados</code>",
                                "programa × categoria × contemplado",
                                "Mesma estrutura do resumo, cruzada com a flag de "
                                "contemplação em edital público.",
                            ],
                        ],
                        None,
                        ["30%", "24%", "46%"],
                    ),
                    ("h3", "Colunas de contagem"),
                    (
                        "table",
                        ["Coluna", "Descrição"],
                        [
                            [
                                "<code>total_proponentes</code>",
                                "Contagem distinta de proponentes no cruzamento.",
                            ],
                            [
                                "<code>total_campo_preenchido</code>",
                                "Proponentes cuja categoria veio de resposta declarada no "
                                "formulário.",
                            ],
                            [
                                "<code>total_inferido</code>",
                                "Proponentes cuja categoria foi inferida por sequência de "
                                "programas ou presença em mais de um.",
                            ],
                            [
                                "<code>percentual</code>",
                                "Percentual da linha sobre o total do mesmo programa e status "
                                "de contemplação.",
                            ],
                            [
                                "<code>sequencia_fomento</code>",
                                "Ordem de acesso do proponente dentro do próprio histórico.",
                            ],
                        ],
                        None,
                        ["32%", "68%"],
                    ),
                ],
            ],
        },
        # ─────────────────────────────────────────────────────────────────
        {
            "num": "05",
            "eyebrow": "Origem",
            "titulo": "Inventário de fontes",
            "icone": "database",
            "paginas": [
                [
                    (
                        "p",
                        "As fontes declaradas no projeto dbt, com o número de tabelas e de "
                        "colunas de cada uma. Nem toda fonte declarada é consumida por um "
                        "modelo: parte está documentada para quem consulta o banco ou o "
                        "catálogo externo sem passar pela linhagem do dbt.",
                    ),
                    (
                        "table",
                        ["Fonte", "Schema", "Tabelas", "Colunas", "Consumida"],
                        [
                            ["<code>bronze_sac</code>", "bronze", "431", "4.304", "Não"],
                            ["<code>bronze_tabelas</code>", "bronze", "67", "405", "Não"],
                            ["<code>bronze_agentes</code>", "bronze", "57", "324", "Não"],
                            ["<code>bronze_controledeacesso</code>", "bronze", "5", "26", "Não"],
                            ["<code>bronze_bdcorporativo</code>", "bronze", "1", "5", "Não"],
                            ["<code>dados_mapa_cultura</code>", "dados_mapa_cultura", "29", "0", "Não"],
                            ["<code>dados_salic</code>", "dados_salic", "19", "0", "Não"],
                            ["<code>transferegov</code>", "transferegov", "7", "0", "Sim"],
                            ["<code>relatorio_gestao</code>", "relatorio_gestao", "6", "0", "Sim"],
                            ["<code>agentes</code>", "agentes", "5", "0", "Não"],
                            ["<code>bbagil</code>", "bbagil", "3", "0", "Parcial"],
                            ["<code>bsc</code>", "bsc", "2", "0", "Não"],
                            ["<code>execucao_pnab</code>", "execucao_pnab", "2", "0", "Sim"],
                            ["<code>bb_agil</code>", "bb_agil", "1", "0", "Não"],
                        ],
                        "As cinco fontes <code>bronze_*</code> são a camada de pouso do "
                        "SALIC, carregada pela DAG <code>salic_ingestion</code>. Somam 561 "
                        "tabelas e 5.064 colunas declaradas.",
                        ["30%", "24%", "14%", "16%", "16%"],
                    ),
                ],
                [
                    (
                        "callout",
                        "database",
                        "Duas fontes declaradas que não são fontes externas",
                        [
                            "<code>agentes</code> e <code>bbagil</code> apontam para schemas "
                            "de saída do próprio projeto, e não para sistemas de origem. Estão "
                            "declaradas como fonte para aparecerem no catálogo externo, mas "
                            "nenhum modelo as lê via <code>source()</code>. A distinção está "
                            "registrada na descrição de cada uma.",
                        ],
                    ),
                    ("h3", "A camada bronze do SALIC"),
                    (
                        "p",
                        "As cinco fontes <code>bronze_*</code> concentram praticamente todo o "
                        "volume declarado do projeto. Elas são carregadas pela DAG "
                        "<code>salic_ingestion</code>, que faz apenas extração e carga: todos "
                        "os valores pousam como texto, e a tipagem correta é responsabilidade "
                        "da camada de transformação.",
                    ),
                    (
                        "stats",
                        [
                            ("561", "tabelas declaradas"),
                            ("5.064", "colunas declaradas"),
                            ("858", "verificações de integridade"),
                            ("0", "modelos que as consomem", True),
                        ],
                    ),
                    (
                        "p",
                        "O último número é o que define a situação desta camada: as tabelas "
                        "estão declaradas, nomeadas e verificadas, mas nenhum modelo do "
                        "projeto as lê. Enquanto isso durar, elas constituem um catálogo de "
                        "origem, e não um produto de dados. É por esse motivo que não têm "
                        "descrição de negócio neste dicionário.",
                    ),
                ],
            ],
        },
        # ─────────────────────────────────────────────────────────────────
        {
            "num": "06",
            "eyebrow": "Fechamento",
            "titulo": "Limites deste dicionário",
            "icone": "governance",
            "paginas": [
                [
                    (
                        "p",
                        "Três lacunas conhecidas, registradas aqui para que quem consulta o "
                        "documento saiba o que ele ainda não responde.",
                    ),
                    ("h3", "1. O bronze de cotas não tem descrição de coluna"),
                    (
                        "p",
                        "Nove modelos do bronze de <code>cotas_dbt</code> têm descrição de "
                        "modelo, mas nenhuma coluna descrita individualmente. São modelos de "
                        "pouso, com dezenas de colunas herdadas de planilhas, e descrever "
                        "coluna a coluna só faz sentido depois que o conjunto útil estabilizar.",
                    ),
                    ("h3", "2. A camada bronze do SALIC não tem semântica"),
                    (
                        "p",
                        "As 561 tabelas do SALIC têm nome, schema e testes de integridade, "
                        "mas nenhuma descrição de negócio. Enquanto nenhum modelo as "
                        "consumir, descrevê-las seria documentar um catálogo em vez de um "
                        "produto de dados.",
                    ),
                    ("h3", "3. Não há dicionário do Mapas Culturais"),
                    (
                        "p",
                        "As 29 tabelas de <code>dados_mapa_cultura</code> estão declaradas a "
                        "partir do esquema público do projeto Mapas Culturais. A própria "
                        "descrição da fonte registra que as colunas não foram verificadas "
                        "contra a base real, e não existe DAG de ingestão para essa fonte "
                        "neste repositório.",
                    ),
                    (
                        "callout",
                        "clipboard-document-check",
                        "Como este documento se mantém correto",
                        [
                            "Toda descrição vem do <code>schema.yml</code>. Um modelo novo sem "
                            "descrição aparece aqui como lacuna visível, e não como ausência "
                            "silenciosa. Regerar o documento depois de cada alteração no "
                            "projeto dbt é o que mantém a correspondência.",
                        ],
                    ),
                ],
            ],
        },
    ],
}
