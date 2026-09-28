"""Documento 07 — Manual de Evolução (Meta 03 · Produto 3)."""

DOC = {
    "slug": "07-manual-de-evolucao",
    "titulo": "Manual de Evolução",
    "subtitulo": (
        "Como acrescentar uma fonte, uma DAG, um modelo ou uma regra de qualidade "
        "à Plataforma de Dados MinC sem quebrar o que já existe, e quais decisões "
        "de arquitetura estão registradas e não devem ser refeitas."
    ),
    "rodape": "Manual de Evolução",
    "meta": [
        ("Meta 03 · Produto 3", "Manuais de uso, manutenção e evolução."),
    ],
    "capitulos": [
        # ─────────────────────────────────────────────────────────────────
        {
            "num": "01",
            "eyebrow": "Sobre este documento",
            "titulo": "A quem se destina",
            "icone": "book-open",
            "paginas": [
                [
                    (
                        "lead",
                        "Este manual é para quem vai estender a plataforma: acrescentar uma "
                        "fonte de dados, um pipeline, um modelo de transformação ou uma regra "
                        "de qualidade. Ele descreve o caminho já trilhado por cada uma das "
                        "fontes em produção, e as convenções que mantêm o repositório "
                        "coerente.",
                    ),
                    ("h3", "A arquitetura que sustenta a evolução"),
                    (
                        "p",
                        "A plataforma é modular por camada e por domínio. Uma fonte nova entra "
                        "pela ingestão, pousa no bronze e sobe até o gold sem tocar em nada "
                        "que já existe, porque cada camada só conhece a anterior. É essa "
                        "separação que torna a extensão previsível.",
                    ),
                    (
                        "code",
                        "API ou banco de origem\n"
                        "        ↓   plugins/cliente_*.py          cliente de acesso\n"
                        "        ↓   dags/data_ingest/<fonte>/     DAG de ingestão\n"
                        "   bronze     pouso bruto, sem regra de negócio\n"
                        "        ↓   dbt/minc/models/<dominio>/silver/\n"
                        "   silver     regra de negócio, deduplicação, cruzamento\n"
                        "        ↓   dbt/minc/models/<dominio>/gold/\n"
                        "   gold       tabela de consumo, no grão da pergunta",
                        "O caminho de um dado, da origem ao consumo.",
                    ),
                    (
                        "callout",
                        "check-badge",
                        "Existe uma skill que percorre esse caminho",
                        [
                            "<code>.claude/skills/govhub-pipeline-guide-minc/</code> descreve "
                            "as seis fases de uma fonte nova, com a estrutura de arquivos "
                            "esperada, os erros comuns e uma seção de diagnóstico por sintoma. "
                            "Ela é versionada no repositório: quem clona a recebe junto.",
                        ],
                    ),
                ],
            ],
        },
        # ─────────────────────────────────────────────────────────────────
        {
            "num": "02",
            "eyebrow": "Extensão",
            "titulo": "Acrescentar uma fonte",
            "icone": "database",
            "paginas": [
                [
                    (
                        "p",
                        "Seis fases, na ordem. Pular uma delas costuma custar retrabalho na "
                        "seguinte.",
                    ),
                    ("h4", "Fase 1 · Cliente de acesso"),
                    (
                        "p",
                        "Um módulo <code>plugins/cliente_&lt;fonte&gt;.py</code> concentra "
                        "autenticação, paginação e tratamento de erro da origem. Ele não "
                        "conhece Airflow nem banco: recebe parâmetros e devolve dados. Essa "
                        "separação é o que permite testar o cliente sem subir a orquestração.",
                    ),
                    ("h4", "Fase 2 · DAG de ingestão"),
                    (
                        "p",
                        "Um arquivo por endpoint, em "
                        "<code>dags/data_ingest/&lt;fonte&gt;/</code>. A DAG orquestra e "
                        "persiste; ela não transforma. Nomes de schema e tabela vêm de "
                        "<code>plugins/schemas_minc.py</code>, nunca de literal digitado no "
                        "arquivo.",
                    ),
                    (
                        "callout",
                        "shield-check",
                        "Por que o nome da tabela não pode ser literal",
                        [
                            "O cliente de banco executa <code>CREATE SCHEMA IF NOT "
                            "EXISTS</code> a cada inserção. Um literal digitado errado não "
                            "quebra a DAG: ele cria em silêncio um schema fora do padrão no "
                            "banco. Foi assim que os nomes divergiram da especificação antes "
                            "de serem centralizados, e é por isso que a centralização existe.",
                        ],
                    ),
                    ("h4", "Fase 3 · Modelos dbt"),
                    (
                        "p",
                        "Declare a tabela de pouso em <code>models/sources.yml</code>, escreva "
                        "o modelo de bronze e suba pelas camadas. A materialização de cada "
                        "camada já está definida em <code>dbt_project.yml</code>: não "
                        "redefina modelo a modelo sem motivo registrado.",
                    ),
                    ("h4", "Fase 4 · Verificações e testes"),
                    (
                        "p",
                        "Declare no <code>schema.yml</code> as verificações das cinco "
                        "dimensões de qualidade que se aplicam ao modelo novo, e escreva teste "
                        "em <code>tests/</code> para a lógica Python que não é trivial.",
                    ),
                ],
                [
                    ("h4", "Fase 5 · Documentação que acompanha"),
                    (
                        "p",
                        "Toda descrição de modelo e de coluna vive no <code>schema.yml</code> "
                        "e em <code>descriptions.yml</code>. É de lá que saem o catálogo de "
                        "metadados, o dicionário de dados e o catálogo externo. Um modelo sem "
                        "descrição aparece como lacuna nos três.",
                    ),
                    ("h4", "Fase 6 · Coleta do site e abertura do PR"),
                    (
                        "p",
                        "Quem altera um modelo ou uma DAG roda <code>make docs-collect</code> "
                        "e inclui o acervo atualizado no mesmo PR. A publicação do site é "
                        "automática, mas a coleta não: sem esse passo, o site segue "
                        "descrevendo o estado anterior sem nenhum sinal de que está "
                        "desatualizado.",
                    ),
                    (
                        "callout",
                        "clipboard-document-check",
                        "Antes de considerar pronto",
                        [
                            "Executar <code>make lint</code> e <code>make test</code> "
                            "localmente. A automação de integração hoje confere apenas o SQL e "
                            "não reprova o PR, de modo que a verificação de formatação, de "
                            "estilo e de tipos depende dessa execução local.",
                        ],
                    ),
                ],
            ],
        },
        # ─────────────────────────────────────────────────────────────────
        {
            "num": "03",
            "eyebrow": "Convenções",
            "titulo": "O que mantém o repositório coerente",
            "icone": "list-bullet",
            "paginas": [
                [
                    ("h3", "Ramificações"),
                    (
                        "p",
                        "Dois formatos aceitos. O segundo é gerado automaticamente pelo fluxo "
                        "de trabalho quando uma issue recebe a etiqueta <code>Código</code>.",
                    ),
                    (
                        "code",
                        "<tipo>/<descricao-curta>                feat/ingestao-salic\n"
                        "<numero-da-issue>-<tipo>-<descricao>    24-fix-dag-nota-de-credito",
                    ),
                    ("h3", "Mensagens de commit"),
                    (
                        "p",
                        "Conventional Commits, com os tipos definidos em "
                        "<code>.github/TEMPLATES/COMMIT_TEMPLATE.md</code>.",
                    ),
                    (
                        "table",
                        ["Tipo", "Quando usar"],
                        [
                            ["<code>feat</code>", "Nova funcionalidade ou capacidade."],
                            ["<code>fix</code>", "Correção de erro."],
                            ["<code>docs</code>", "Alteração exclusiva de documentação."],
                            ["<code>refactor</code>", "Mudança que não corrige erro nem acrescenta funcionalidade."],
                            ["<code>perf</code>", "Mudança que melhora desempenho."],
                            ["<code>test</code>", "Adição ou correção de teste automatizado."],
                            ["<code>build</code>", "Mudança no sistema de construção ou em dependências."],
                            ["<code>ci</code>", "Mudança nos arquivos de integração contínua."],
                            ["<code>chore</code>", "Demais mudanças que não tocam código nem teste."],
                            ["<code>style</code>", "Formatação, sem efeito sobre a lógica."],
                        ],
                        None,
                        ["22%", "78%"],
                    ),
                    ("h3", "Issues"),
                    (
                        "p",
                        "Sempre por formulário. São seis tipos em "
                        "<code>.github/ISSUE_TEMPLATE/</code>, cada um com campos obrigatórios "
                        "e etiqueta aplicada automaticamente. A caixa de texto em branco está "
                        "desligada de propósito.",
                    ),
                ],
                [
                    ("h3", "Segredos"),
                    (
                        "p",
                        "Nenhum valor real de credencial entra em arquivo versionado, em "
                        "nenhuma hipótese: nem em exemplo, nem em comentário, nem em teste. "
                        "Credencial vazada em commit permanece no histórico mesmo depois de "
                        "removida da árvore de trabalho.",
                    ),
                    (
                        "callout",
                        "shield-check",
                        "Dado sensível também sai em log",
                        [
                            "A plataforma trata CPF, CNPJ e dados de raça e de deficiência de "
                            "agentes culturais. Log de DAG, saída de execução e extração de "
                            "banco devem ser tratados como dado sensível pelo mesmo motivo que "
                            "as tabelas: eles carregam o mesmo conteúdo.",
                        ],
                    ),
                    ("h3", "Onde uma skill nova deve morar"),
                    (
                        "p",
                        "O critério está registrado: se o conhecimento da skill sobrevive a "
                        "uma mudança de lugar dentro deste repositório, ele pertence ao "
                        "repositório compartilhado de skills, não a este. Uma skill que cita "
                        "<code>dbt/minc</code> ou <code>plugins/cliente_*.py</code> mora aqui, "
                        "porque quebra junto com uma refatoração daqui e o mesmo PR conserta "
                        "as duas coisas.",
                    ),
                    ("h3", "O que é gerado e não se edita à mão"),
                    (
                        "table",
                        ["Caminho", "Origem"],
                        [
                            ["<code>dbt/minc/target/</code>", "Saída de <code>dbt run</code> e <code>dbt docs generate</code>."],
                            ["<code>docs-pages/src/_data/</code>", "Acervo produzido por <code>make docs-collect</code>."],
                            ["<code>data/</code>", "Extrações brutas geradas por quem executa as DAGs."],
                            ["<code>requirements.generated.txt</code>", "Exportação do Poetry feita por <code>make setup</code>."],
                        ],
                        "No site de documentação, número errado se corrige no coletor; "
                        "explicação errada se corrige em <code>docs-pages/src/dominios.yml</code>.",
                        ["38%", "62%"],
                    ),
                ],
            ],
        },
        # ─────────────────────────────────────────────────────────────────
        {
            "num": "04",
            "eyebrow": "Histórico",
            "titulo": "Decisões já tomadas",
            "icone": "governance",
            "paginas": [
                [
                    (
                        "p",
                        "Quatro decisões de arquitetura estão registradas em "
                        "<code>docs/adr/</code>, no formato de registro de decisão. Elas "
                        "existem para que uma discussão já encerrada não volte à pauta sem "
                        "informação nova.",
                    ),
                    (
                        "table",
                        ["ADR", "Decisão", "Data"],
                        [
                            [
                                "0001",
                                "Formulários de issue em YAML, com campos obrigatórios e "
                                "etiqueta preenchida, no lugar dos modelos Markdown herdados.",
                                "13/08/2026",
                            ],
                            [
                                "0002",
                                "Skills versionadas no próprio repositório, e não instaladas "
                                "por marketplace. Quem clona recebe todas, sem passo de "
                                "instalação; em troca, correção feita na origem não chega "
                                "sozinha.",
                                "13/08/2026",
                            ],
                            [
                                "0003",
                                "Etiquetas organizadas em quatro eixos independentes, e não "
                                "numa lista plana.",
                                "13/08/2026",
                            ],
                            [
                                "0004",
                                "A revisão semanal apura antes de narrar: a skill é dividida "
                                "em duas metades com responsabilidades separadas.",
                                "13/08/2026",
                            ],
                        ],
                        None,
                        ["12%", "66%", "22%"],
                    ),
                    (
                        "callout",
                        "clipboard-document-check",
                        "Quando escrever um ADR novo",
                        [
                            "A regra adotada é registrar uma decisão por discussão que tenha "
                            "custado debate. O registro descreve o contexto, a decisão e as "
                            "alternativas descartadas, e é isso que permite reabrir a questão "
                            "no futuro sabendo o que já foi considerado.",
                        ],
                    ),
                ],
            ],
        },
        # ─────────────────────────────────────────────────────────────────
        {
            "num": "05",
            "eyebrow": "Fechamento",
            "titulo": "Dívidas conhecidas",
            "icone": "shield-check",
            "paginas": [
                [
                    (
                        "p",
                        "Quem for estender a plataforma vai esbarrar nestas quatro situações. "
                        "Elas estão registradas aqui para que sejam encontradas antes, e não "
                        "durante.",
                    ),
                    ("h3", "1. A automação não reprova o que deveria"),
                    (
                        "p",
                        "A etapa de verificação de estilo termina com <code>|| true</code>, "
                        "por decisão registrada, e cobre apenas SQL. Formatação, estilo e "
                        "tipagem de Python são verificados só localmente. A suíte de testes "
                        "mede cobertura sem limite mínimo.",
                    ),
                    ("h3", "2. Há código sem uso em plugins"),
                    (
                        "p",
                        "Parte dos módulos de <code>plugins/</code> foi herdada de outro "
                        "repositório da mesma família e não é importada por nenhuma DAG deste "
                        "projeto. Antes de estender um cliente existente, confirme que ele é "
                        "de fato consumido, para não investir em código que não executa.",
                    ),
                    ("h3", "3. Uma cadeia de modelos está desligada"),
                    (
                        "p",
                        "Quatro modelos do domínio de cotas estão com "
                        "<code>enabled=false</code>, à espera de que a extração do BB Ágil "
                        "conclua. A situação está registrada na descrição de cada um, com a "
                        "instrução de reativar os quatro em conjunto.",
                    ),
                    ("h3", "4. Uma fonte declarada não tem pipeline"),
                    (
                        "p",
                        "As tabelas do Mapas Culturais estão declaradas como fonte no projeto "
                        "dbt, mas não há DAG de ingestão nem modelo que as consuma, e a "
                        "estrutura foi inferida do esquema público do projeto sem verificação "
                        "contra a base real. Tratá-las como fonte disponível levaria a erro.",
                    ),
                ],
            ],
        },
    ],
}
