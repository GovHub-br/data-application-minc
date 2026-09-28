"""Documento 01 — Catálogo de Metadados (Meta 02 · Produto 3)."""

DOC = {
    "slug": "01-catalogo-de-metadados",
    "titulo": "Catálogo de Metadados",
    "subtitulo": (
        "Como a Plataforma de Dados MinC registra, para cada tabela produzida, "
        "onde ela vive, como foi materializada, o que significa e quando foi "
        "atualizada pela última vez."
    ),
    "rodape": "Catálogo de Metadados",
    "meta": [
        ("Meta 02 · Produto 3", "Modelagem de dados, dicionário de dados e metadados."),
    ],
    "capitulos": [
        # ─────────────────────────────────────────────────────────────────
        {
            "num": "01",
            "eyebrow": "Sobre este documento",
            "titulo": "Objetivo e escopo",
            "icone": "document-text",
            "paginas": [
                [
                    (
                        "lead",
                        "Este documento descreve o catálogo de metadados da Plataforma de "
                        "Dados MinC: o que ele registra, como é produzido, o que já cobre "
                        "hoje e o que ainda falta para atender integralmente ao produto "
                        "previsto na Meta 02.",
                    ),
                    ("h3", "O que é um catálogo de metadados aqui"),
                    (
                        "p",
                        "Metadado é o dado sobre o dado: não o valor de um pagamento, mas "
                        "em que schema aquela tabela mora, como foi construída, o que ela "
                        "significa e em que momento foi calculada pela última vez. Sem isso, "
                        "quem consulta o banco encontra tabelas sem saber se estão frescas, "
                        "de onde vieram ou se podem ser citadas.",
                    ),
                    (
                        "p",
                        "Na Plataforma de Dados MinC esse registro não é uma planilha "
                        "mantida à mão. Ele é uma tabela do próprio banco, "
                        "<code>metadata.models_metadata</code>, reescrita a cada execução do "
                        "dbt a partir do grafo interno do projeto. O catálogo, portanto, "
                        "não pode divergir do que existe: ele é derivado do que existe.",
                    ),
                    (
                        "callout",
                        "check-badge",
                        "Por que isso importa",
                        [
                            "Um catálogo escrito à mão envelhece em silêncio: alguém renomeia "
                            "um modelo, ninguém atualiza a planilha, e a documentação passa a "
                            "descrever um banco que não existe mais.",
                            "Um catálogo derivado do grafo do dbt não tem como envelhecer: "
                            "se o modelo sumiu, ele sai do catálogo na execução seguinte.",
                        ],
                    ),
                    ("h3", "Escopo"),
                    (
                        "p",
                        "O catálogo cobre <strong>os modelos dbt do projeto</strong>, isto é, "
                        "as tabelas e views produzidas pela camada de transformação. Ele não "
                        "cobre as tabelas de pouso bruto criadas diretamente pelas DAGs de "
                        "ingestão, que estão documentadas no catálogo de fontes e nas fichas "
                        "de pipeline.",
                    ),
                ],
            ],
        },
        # ─────────────────────────────────────────────────────────────────
        {
            "num": "02",
            "eyebrow": "Mecanismo",
            "titulo": "Como o catálogo é gerado",
            "icone": "workflow",
            "paginas": [
                [
                    (
                        "p",
                        "O catálogo é um modelo dbt como qualquer outro, "
                        "<code>models/metadata/models_metadata.sql</code>, com uma diferença: "
                        "em vez de ler tabelas do banco, ele lê o <strong>grafo do próprio "
                        "projeto dbt</strong> em tempo de compilação.",
                    ),
                    ("h3", "A varredura do grafo"),
                    (
                        "p",
                        "Durante a compilação, o dbt expõe a variável <code>graph</code>, que "
                        "contém todos os nós do projeto. O modelo percorre esses nós, filtra "
                        "os que são do tipo <code>model</code> e monta uma linha por modelo "
                        "encontrado:",
                    ),
                    (
                        "code",
                        "{% for node in graph.nodes.values() %}\n"
                        "    {% if node.resource_type == 'model' %}\n"
                        "        {% do models_data.append({\n"
                        "            'schema_name': node.schema,\n"
                        "            'table_name': node.name,\n"
                        "            'database_name': node.database,\n"
                        "            'materialization': node.config.materialized,\n"
                        "            'description': node.description\n"
                        "        }) %}\n"
                        "    {% endif %}\n"
                        "{% endfor %}",
                        "Trecho de <code>models/metadata/models_metadata.sql</code>. A lista "
                        "de modelos não é digitada: é derivada do grafo a cada execução.",
                    ),
                    ("h3", "Carga incremental com chave composta"),
                    (
                        "p",
                        "O modelo é materializado como <code>incremental</code>, com "
                        "<code>unique_key</code> composta por <code>schema_name</code> e "
                        "<code>table_name</code>, e "
                        "<code>on_schema_change='sync_all_columns'</code>. Cada execução "
                        "atualiza a linha do modelo em vez de acrescentar uma nova, de forma "
                        "que a tabela mantém sempre o retrato mais recente de cada modelo.",
                    ),
                ],
                [
                    ("h3", "Quando o catálogo é atualizado"),
                    (
                        "p",
                        "A atualização acontece junto com a transformação: a DAG "
                        "<code>minc_cosmos_dag</code> roda o projeto dbt inteiro todo dia à "
                        "01h00 (<code>schedule=\"0 1 * * *\"</code>), e "
                        "<code>models_metadata</code> é um dos modelos executados nessa "
                        "passagem. Não existe um processo separado de coleta de metadados "
                        "que possa ficar para trás.",
                    ),
                    (
                        "table",
                        ["Propriedade", "Valor"],
                        [
                            ["Modelo", "<code>metadata.models_metadata</code>"],
                            ["Materialização", "<code>incremental</code>"],
                            ["Chave única", "<code>schema_name</code> + <code>table_name</code>"],
                            ["Mudança de schema", "<code>sync_all_columns</code>"],
                            ["Orquestração", "<code>minc_cosmos_dag</code> (Airflow + Cosmos)"],
                            ["Frequência", "Diária, 01h00 (<code>0 1 * * *</code>)"],
                            ["Fuso do carimbo", "America/Sao_Paulo (UTC-3)"],
                            ["Retentativas", "2 (<code>default_args</code> da DAG)"],
                        ],
                        None,
                        ["38%", "62%"],
                    ),
                    ("h3", "Integração com catálogo externo"),
                    (
                        "p",
                        "Os modelos carregam <code>tags</code> nativas do dbt, e não "
                        "<code>meta.tags</code>. A escolha é deliberada: é a tag nativa que o "
                        "conector dbt do OpenMetadata mapeia para tags do catálogo externo. O "
                        "próprio <code>models_metadata</code> é marcado com "
                        "<code>infraestrutura</code>, <code>metadata</code> e "
                        "<code>governance</code>, sinalizando que ele existe para operar o "
                        "pipeline e não como dado de negócio consumível.",
                    ),
                ],
            ],
        },
        # ─────────────────────────────────────────────────────────────────
        {
            "num": "03",
            "eyebrow": "Estrutura",
            "titulo": "Campos do catálogo",
            "icone": "table-cells",
            "paginas": [
                [
                    (
                        "p",
                        "A tabela tem sete campos. Três deles identificam o modelo, dois "
                        "descrevem como ele foi construído e dois registram quando e por qual "
                        "execução ele foi produzido.",
                    ),
                    (
                        "table",
                        ["Campo", "Descrição", "Teste"],
                        [
                            [
                                "<code>schema_name</code>",
                                "Schema onde o modelo está materializado.",
                                "<code>not_null</code>",
                            ],
                            [
                                "<code>table_name</code>",
                                "Nome da tabela ou view produzida pelo modelo.",
                                "<code>not_null</code>",
                            ],
                            [
                                "<code>database_name</code>",
                                "Banco de dados de destino.",
                                "",
                            ],
                            [
                                "<code>materialization</code>",
                                "Tipo de materialização: <code>table</code>, "
                                "<code>view</code> ou <code>incremental</code>.",
                                "",
                            ],
                            [
                                "<code>description</code>",
                                "Descrição do modelo, extraída do <code>schema.yml</code> "
                                "correspondente.",
                                "",
                            ],
                            [
                                "<code>dt_transform</code>",
                                "Data e hora da última transformação, correspondente ao "
                                "<code>run_started_at</code> da execução.",
                                "<code>not_null</code>",
                            ],
                            [
                                "<code>run_id</code>",
                                "Identificador único da execução do dbt "
                                "(<code>invocation_id</code>), para rastrear qual rodada "
                                "gerou a linha.",
                                "",
                            ],
                        ],
                        None,
                        ["24%", "58%", "18%"],
                    ),
                    (
                        "callout",
                        "clipboard-document-check",
                        "O campo que responde “esse dado está fresco?”",
                        [
                            "<code>dt_transform</code> é o campo de atualidade do catálogo. "
                            "Ele registra o início da execução do dbt que produziu a linha, "
                            "no fuso America/Sao_Paulo, e é a base da dimensão de atualidade "
                            "descrita no documento de critérios de qualidade dos dados.",
                        ],
                    ),
                ],
            ],
        },
        # ─────────────────────────────────────────────────────────────────
        {
            "num": "04",
            "eyebrow": "Situação",
            "titulo": "Cobertura atual",
            "icone": "chart-pie",
            "paginas": [
                [
                    (
                        "p",
                        "Os números abaixo foram apurados diretamente do repositório, "
                        "parseando os arquivos de configuração do projeto dbt.",
                    ),
                    (
                        "stats",
                        [
                            ("36", "modelos dbt no catálogo"),
                            ("3", "schemas de destino"),
                            ("71", "colunas documentadas"),
                            ("3", "materializações em uso"),
                        ],
                    ),
                    ("h3", "Distribuição por domínio e camada"),
                    (
                        "table",
                        ["Domínio", "Camada", "Modelos", "Materialização"],
                        [
                            ["agentes_dbt", "bronze", "5", "<code>incremental</code>"],
                            ["agentes_dbt", "silver", "1", "<code>table</code>"],
                            ["agentes_dbt", "gold", "4", "<code>table</code>"],
                            ["agentes_dbt", "views", "1", "<code>view</code>"],
                            ["cotas_dbt", "bronze", "9", "<code>table</code>"],
                            ["cotas_dbt", "silver", "9", "<code>view</code>"],
                            ["cotas_dbt", "gold", "6", "<code>table</code>"],
                            ["metadata", "metadata", "1", "<code>incremental</code>"],
                        ],
                        "A materialização é definida por camada em "
                        "<code>dbt_project.yml</code>, não modelo a modelo. O bronze de "
                        "<code>cotas_dbt</code> é <code>table</code> e não "
                        "<code>incremental</code> porque filtra o resíduo de parsing das "
                        "planilhas, reduzindo milhões de linhas a cerca de 20 mil.",
                        ["26%", "20%", "18%", "36%"],
                    ),
                ],
                [
                    ("h3", "Modelos desabilitados"),
                    (
                        "p",
                        "Quatro modelos estão declarados com "
                        "<code>config(enabled=false)</code> e, por isso, não aparecem no "
                        "catálogo gerado. Eles dependem da extração do BB Ágil, hoje "
                        "bloqueada por credencial de autenticação ausente. A situação está "
                        "registrada na descrição de cada um.",
                    ),
                    (
                        "table",
                        ["Modelo", "Camada", "Motivo"],
                        [
                            [
                                "<code>stg_bbagil</code>",
                                "cotas · bronze",
                                "Aguarda a DAG <code>extracao_bbagil_dag</code> concluir com "
                                "sucesso.",
                            ],
                            [
                                "<code>bbagil_ente_ano</code>",
                                "cotas · silver",
                                "Desabilitado junto com <code>stg_bbagil</code>.",
                            ],
                            [
                                "<code>fct_pagamentos_bbagil</code>",
                                "cotas · gold",
                                "Depende dos dois anteriores.",
                            ],
                            [
                                "<code>comparativo_recebido_vs_pago</code>",
                                "cotas · gold",
                                "Depende de <code>fct_pagamentos_bbagil</code>.",
                            ],
                        ],
                        None,
                        ["33%", "18%", "49%"],
                    ),
                    (
                        "callout",
                        "shield-check",
                        "Consequência para quem lê os números",
                        [
                            "Enquanto esses quatro modelos estiverem desabilitados, as "
                            "tabelas de distribuição de cotas medem valor "
                            "<strong>recebido</strong> por entes federados, não valor "
                            "<strong>pago</strong> a pessoas. A ressalva está registrada no "
                            "documento de exemplos de uso e no código de cada modelo.",
                        ],
                    ),
                ],
            ],
        },
        # ─────────────────────────────────────────────────────────────────
        {
            "num": "05",
            "eyebrow": "Fechamento",
            "titulo": "Limites e evolução",
            "icone": "governance",
            "paginas": [
                [
                    (
                        "p",
                        "O catálogo cobre hoje a dimensão técnica dos metadados. Duas "
                        "dimensões previstas no produto da Meta 02 ainda não estão "
                        "registradas, e ambas dependem de decisão, não de código.",
                    ),
                    ("h3", "O que falta"),
                    (
                        "table",
                        ["Atributo previsto", "Situação", "O que falta"],
                        [
                            [
                                "Origem",
                                "Parcial",
                                "A linhagem existe no grafo do dbt, mas não é gravada como "
                                "coluna do catálogo.",
                            ],
                            [
                                "Periodicidade",
                                "Parcial",
                                "Está no <code>schedule</code> de cada DAG, não no catálogo "
                                "dos modelos.",
                            ],
                            [
                                "Owner",
                                "Ausente",
                                "Exige atribuir responsável por domínio de dados, decisão de "
                                "governança.",
                            ],
                            [
                                "Classificação",
                                "Ausente",
                                "Exige classificar quais tabelas contêm dado pessoal e de que "
                                "natureza.",
                            ],
                        ],
                        None,
                        ["24%", "16%", "60%"],
                    ),
                    (
                        "callout",
                        "database",
                        "Como acrescentar sem quebrar nada",
                        [
                            "Os dois atributos ausentes cabem no bloco <code>meta:</code> do "
                            "<code>schema.yml</code> de cada modelo, que o dbt já expõe em "
                            "<code>node.config.meta</code>. Isso significa acrescentar duas "
                            "chaves ao laço de varredura do grafo e duas colunas à tabela, "
                            "sem mudar a arquitetura do catálogo.",
                            "A materialização <code>incremental</code> com "
                            "<code>on_schema_change='sync_all_columns'</code> absorve colunas "
                            "novas sem exigir recarga completa.",
                        ],
                    ),
                    (
                        "p",
                        "Com esses dois campos preenchidos, o catálogo passa a responder às "
                        "quatro perguntas de governança sobre qualquer tabela da plataforma: "
                        "de onde veio, quem responde por ela, com que frequência muda e se "
                        "contém dado pessoal.",
                    ),
                ],
            ],
        },
    ],
}
