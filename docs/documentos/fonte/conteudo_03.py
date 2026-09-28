"""Documento 03 — Critérios de Qualidade dos Dados (Meta 02 · Produto 4)."""

DOC = {
    "slug": "03-criterios-de-qualidade-dos-dados",
    "titulo": "Critérios de Qualidade dos Dados",
    "subtitulo": (
        "As cinco dimensões de qualidade adotadas pela Plataforma de Dados MinC, "
        "como cada uma é verificada de forma automatizada, e o inventário completo "
        "das verificações hoje em execução."
    ),
    "rodape": "Critérios de Qualidade",
    "meta": [
        ("Meta 02 · Produto 4", "Governança de dados: papéis, acesso e qualidade."),
    ],
    "capitulos": [
        # ─────────────────────────────────────────────────────────────────
        {
            "num": "01",
            "eyebrow": "Sobre este documento",
            "titulo": "Objetivo e escopo",
            "icone": "shield-check",
            "paginas": [
                [
                    (
                        "lead",
                        "Este documento define os critérios de qualidade dos dados da "
                        "Plataforma de Dados MinC e demonstra como cada critério é verificado "
                        "automaticamente a cada execução do pipeline.",
                    ),
                    ("h3", "Qualidade verificada, não declarada"),
                    (
                        "p",
                        "Um critério de qualidade só é útil quando alguma coisa o verifica "
                        "sem depender de disciplina humana. Por isso, cada dimensão descrita "
                        "aqui está associada a um mecanismo de verificação que roda junto com "
                        "a transformação: se o critério é violado, a execução falha e o "
                        "problema aparece, em vez de seguir silenciosamente para a camada de "
                        "consumo.",
                    ),
                    (
                        "stats",
                        [
                            ("923", "verificações automatizadas"),
                            ("5", "dimensões cobertas"),
                            ("440", "tabelas com verificação"),
                            ("2", "verificações próprias"),
                        ],
                    ),
                    (
                        "p",
                        "As verificações se dividem em dois grupos: 65 sobre os modelos "
                        "produzidos pela transformação e 858 sobre as tabelas de origem da "
                        "camada bronze. O segundo grupo é maior porque a camada bronze do "
                        "SALIC recebe dados de um sistema externo sobre o qual a plataforma "
                        "não tem controle, e é ali que a integridade precisa ser conferida "
                        "primeiro.",
                    ),
                    (
                        "callout",
                        "check-badge",
                        "Onde as verificações vivem",
                        [
                            "Todas são declaradas nos arquivos <code>schema.yml</code> do "
                            "projeto dbt, ao lado da definição de cada coluna, e executadas "
                            "pelo comando <code>dbt test</code>, orquestrado pela DAG "
                            "<code>minc_cosmos_dag</code>.",
                        ],
                    ),
                ],
            ],
        },
        # ─────────────────────────────────────────────────────────────────
        {
            "num": "02",
            "eyebrow": "Definição",
            "titulo": "As cinco dimensões",
            "icone": "list-bullet",
            "paginas": [
                [
                    (
                        "p",
                        "A matriz abaixo é o núcleo deste documento: para cada dimensão, o "
                        "que ela significa na prática, qual mecanismo a verifica e o que "
                        "acontece quando é violada.",
                    ),
                    (
                        "table",
                        ["Dimensão", "O que garante", "Mecanismo"],
                        [
                            [
                                "<strong>Completude</strong>",
                                "Campos obrigatórios estão preenchidos. Nenhuma chave de "
                                "junção, classificação ou carimbo de tempo pode ser nulo.",
                                "<code>not_null</code>",
                            ],
                            [
                                "<strong>Unicidade</strong>",
                                "O grão declarado do modelo é respeitado. Uma tabela de uma "
                                "linha por documento não pode ter o mesmo documento duas "
                                "vezes.",
                                "<code>unique</code>",
                            ],
                            [
                                "<strong>Validade</strong>",
                                "Colunas categóricas só assumem valores do conjunto "
                                "permitido. Uma raça, um grupo de cota ou um programa fora da "
                                "lista é erro, não variação.",
                                "<code>accepted_values</code>",
                            ],
                            [
                                "<strong>Consistência</strong>",
                                "Chaves estrangeiras apontam para registros que existem. Um "
                                "pagamento não pode referenciar um agente inexistente.",
                                "<code>relationships</code>",
                            ],
                            [
                                "<strong>Atualidade</strong>",
                                "Toda tabela produzida carrega o momento em que foi "
                                "calculada, permitindo saber se o número consultado está "
                                "fresco.",
                                "<code>dt_transform</code> no catálogo de metadados",
                            ],
                        ],
                        None,
                        ["18%", "56%", "26%"],
                    ),
                    (
                        "callout",
                        "shield-check",
                        "A dimensão que não se resolve com teste",
                        [
                            "Atualidade é a única das cinco que não é verificada por um teste "
                            "de aprovação ou reprovação, e sim por um carimbo consultável. A "
                            "tabela <code>metadata.models_metadata</code> registra "
                            "<code>dt_transform</code> para cada modelo, no fuso "
                            "America/Sao_Paulo, permitindo que quem consulta descubra sozinho "
                            "há quanto tempo aquele número não é recalculado.",
                        ],
                    ),
                ],
            ],
        },
        # ─────────────────────────────────────────────────────────────────
        {
            "num": "03",
            "eyebrow": "Situação",
            "titulo": "Inventário de verificações",
            "icone": "table-cells",
            "paginas": [
                [
                    ("h3", "Distribuição por tipo"),
                    (
                        "table",
                        ["Verificação", "Dimensão", "Modelos", "Fontes", "Total"],
                        [
                            ["<code>not_null</code>", "Completude", "36", "552", "<strong>588</strong>"],
                            ["<code>unique</code>", "Unicidade", "7", "257", "<strong>264</strong>"],
                            ["<code>accepted_values</code>", "Validade", "21", "49", "<strong>70</strong>"],
                            ["<code>relationships</code>", "Consistência", "1", "0", "<strong>1</strong>"],
                            ["<strong>Total</strong>", "", "<strong>65</strong>", "<strong>858</strong>", "<strong>923</strong>"],
                        ],
                        None,
                        ["30%", "22%", "16%", "16%", "16%"],
                    ),
                    ("h3", "Cobertura das fontes de origem"),
                    (
                        "p",
                        "Das 709 tabelas declaradas como fonte no projeto, 440 têm ao menos "
                        "uma verificação. A concentração acompanha o volume: a camada bronze "
                        "do SALIC responde por praticamente toda a verificação de origem.",
                    ),
                    (
                        "table",
                        ["Fonte", "Verificações"],
                        [
                            ["<code>bronze_sac</code>", "671"],
                            ["<code>bronze_agentes</code>", "97"],
                            ["<code>bronze_tabelas</code>", "80"],
                            ["<code>bronze_controledeacesso</code>", "8"],
                            ["<code>bronze_bdcorporativo</code>", "2"],
                        ],
                        None,
                        ["60%", "40%"],
                    ),
                ],
                [
                    ("h3", "Cobertura dos modelos, por camada"),
                    (
                        "p",
                        "A verificação dos modelos se concentra onde o dado é consumido, e "
                        "não onde ele pousa. Isso é deliberado: um modelo de bronze existe "
                        "para receber o que a origem mandou, inclusive o que veio errado; um "
                        "modelo de gold existe para ser citado.",
                    ),
                    (
                        "table",
                        ["Camada", "Modelos", "Colunas descritas", "Verificações"],
                        [
                            ["agentes · gold", "4", "23", "30"],
                            ["cotas · silver", "9", "9", "14"],
                            ["agentes · bronze", "5", "15", "5"],
                            ["agentes · silver", "1", "5", "4"],
                            ["cotas · gold", "6", "4", "6"],
                            ["metadata", "1", "7", "3"],
                            ["cotas · bronze", "9", "2", "2"],
                            ["agentes · views", "1", "4", "1"],
                        ],
                        "As quatro tabelas de gold do domínio de agentes concentram 30 das 65 "
                        "verificações de modelo, quase metade do total.",
                        ["30%", "18%", "28%", "24%"],
                    ),
                    (
                        "callout",
                        "shield-check",
                        "Lacuna reconhecida",
                        [
                            "O gold do domínio de cotas tem 6 verificações para 6 modelos, "
                            "contra 30 para 4 modelos no domínio de agentes. Quatro dos seis "
                            "modelos de cotas estão desabilitados por dependerem da extração "
                            "do BB Ágil, o que explica parte da diferença, mas não toda: "
                            "<code>fct_pagamentos_elegiveis</code> e "
                            "<code>cobertura_pagamentos</code> merecem cobertura equivalente "
                            "à do outro domínio.",
                        ],
                    ),
                ],
            ],
        },
        # ─────────────────────────────────────────────────────────────────
        {
            "num": "04",
            "eyebrow": "Verificações próprias",
            "titulo": "Além dos testes genéricos",
            "icone": "settings",
            "paginas": [
                [
                    (
                        "p",
                        "Duas verificações foram escritas para este projeto, em "
                        "<code>macros/data_quality/</code>. Elas cobrem o que os testes "
                        "genéricos do dbt não alcançam: a integridade de uma carga e a "
                        "estabilidade do esquema físico.",
                    ),
                    ("h3", "Conferência de contagem entre origem e destino"),
                    (
                        "p",
                        "A macro <code>test_row_count_match</code> compara a contagem de "
                        "linhas da tabela de origem com a da tabela de destino e falha quando "
                        "elas divergem. É a verificação de que uma carga não perdeu registros "
                        "no caminho.",
                    ),
                    (
                        "code",
                        "with\n"
                        "    source_count as (select count(*) as row_count from {{ source_table }}),\n"
                        "    target_count as (select count(*) as row_count from {{ target_table }}),\n"
                        "    comparison as (\n"
                        "        select source_count.row_count as source_row_count,\n"
                        "               target_count.row_count as target_row_count\n"
                        "        from source_count, target_count\n"
                        "    )\n"
                        "select * from comparison\n"
                        "where source_row_count != target_row_count",
                        "<code>macros/data_quality/row_count_match.sql</code>",
                    ),
                    ("h3", "Conferência de tipagem"),
                    (
                        "p",
                        "A macro <code>test_verificacao_tipagem</code> consulta o "
                        "<code>information_schema</code> do Postgres e falha quando o tipo "
                        "real de uma coluna difere do tipo esperado. É a proteção contra "
                        "mudança silenciosa de esquema, que é justamente o risco da ingestão "
                        "do SALIC, onde todos os valores pousam como texto e a tipagem "
                        "correta é responsabilidade da transformação.",
                    ),
                ],
            ],
        },
        # ─────────────────────────────────────────────────────────────────
        {
            "num": "05",
            "eyebrow": "Operação",
            "titulo": "Execução e limites",
            "icone": "governance",
            "paginas": [
                [
                    ("h3", "Quando as verificações rodam"),
                    (
                        "p",
                        "As verificações são executadas pela DAG "
                        "<code>minc_cosmos_dag</code>, que roda o projeto dbt inteiro todo "
                        "dia à 01h00. O Cosmos transforma cada modelo e cada teste em tarefa "
                        "própria do Airflow, de forma que uma verificação reprovada aparece "
                        "como tarefa vermelha na interface, identificando exatamente qual "
                        "coluna de qual modelo violou qual critério.",
                    ),
                    (
                        "table",
                        ["Propriedade", "Valor"],
                        [
                            ["Orquestrador", "<code>minc_cosmos_dag</code> (Airflow 3.2 + Cosmos)"],
                            ["Frequência", "Diária, 01h00 (<code>0 1 * * *</code>)"],
                            ["Retentativas", "2"],
                            ["Granularidade", "Uma tarefa do Airflow por modelo e por teste"],
                            ["Recuperação de histórico", "Desligada (<code>catchup=False</code>)"],
                        ],
                        None,
                        ["36%", "64%"],
                    ),
                    ("h3", "Três limites conhecidos"),
                    (
                        "ol",
                        [
                            "<strong>A dimensão de atualidade não tem limiar.</strong> O "
                            "carimbo <code>dt_transform</code> permite descobrir há quanto "
                            "tempo um modelo não roda, mas não existe alerta configurado para "
                            "quando esse intervalo ultrapassa um limite aceitável.",
                            "<strong>A consistência tem uma única verificação.</strong> "
                            "Apenas <code>fct_pagamentos_elegiveis</code> declara "
                            "<code>relationships</code>. Outras junções entre modelos "
                            "dependem da correção do SQL, sem verificação declarada.",
                            "<strong>Não há registro histórico de reprovações.</strong> O "
                            "resultado de cada execução fica nos logs do Airflow, mas não é "
                            "persistido numa tabela que permita acompanhar a evolução da "
                            "qualidade ao longo do tempo.",
                        ],
                    ),
                ],
                [
                    (
                        "callout",
                        "clipboard-document-check",
                        "O próximo passo natural",
                        [
                            "Os três limites acima se resolvem com o mesmo movimento: "
                            "persistir o resultado de <code>dbt test</code> numa tabela do "
                            "schema <code>metadata</code>, ao lado de "
                            "<code>models_metadata</code>. Isso dá série histórica de "
                            "qualidade, base para limiar de atualidade e evidência de "
                            "conformidade sem depender de leitura de log.",
                        ],
                    ),
                    ("h3", "O que reprova uma execução hoje"),
                    (
                        "p",
                        "Vale distinguir dois momentos em que a qualidade é verificada, "
                        "porque eles têm consequências diferentes.",
                    ),
                    (
                        "table",
                        ["Momento", "O que verifica", "Reprova?"],
                        [
                            [
                                "Execução do pipeline",
                                "As 923 verificações de qualidade dos dados, por meio de "
                                "<code>dbt test</code>.",
                                "Sim. A tarefa falha no Airflow.",
                            ],
                            [
                                "Integração contínua",
                                "Formatação do SQL, por meio de <code>make lint-ci</code>.",
                                "Não. A etapa termina com <code>|| true</code>.",
                            ],
                        ],
                        "A qualidade <em>dos dados</em> tem verificação que reprova; a "
                        "qualidade <em>do código</em> que produz esses dados, hoje, não.",
                        ["24%", "48%", "28%"],
                    ),
                    (
                        "p",
                        "Essa assimetria é a lacuna mais relevante do conjunto: um modelo com "
                        "erro de lógica que não viole nenhuma das cinco dimensões passa pelas "
                        "duas verificações sem ser notado. É o que torna a cobertura de teste "
                        "do código, e não apenas dos dados, parte dos critérios de qualidade.",
                    ),
                ],
            ],
        },
    ],
}
