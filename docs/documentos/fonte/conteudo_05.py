"""Documento 05 — Scripts de Implantação (Meta 03 · Produto 3)."""

DOC = {
    "slug": "05-scripts-de-implantacao",
    "titulo": "Scripts de Implantação",
    "subtitulo": (
        "A imagem de contêiner, os serviços, as variáveis de ambiente e os "
        "comandos que sobem a Plataforma de Dados MinC, com o que cada peça faz "
        "e o que precisa ser definido antes de rodar."
    ),
    "rodape": "Scripts de Implantação",
    "meta": [
        ("Meta 03 · Produto 3", "Scripts e procedimentos de implantação."),
    ],
    "capitulos": [
        # ─────────────────────────────────────────────────────────────────
        {
            "num": "01",
            "eyebrow": "Sobre este documento",
            "titulo": "Objetivo e escopo",
            "icone": "deploy",
            "paginas": [
                [
                    (
                        "lead",
                        "Este documento descreve os artefatos de implantação versionados no "
                        "repositório: o que cada um constrói, quais variáveis consome e em "
                        "que ordem os serviços sobem.",
                    ),
                    ("h3", "Os artefatos"),
                    (
                        "table",
                        ["Caminho", "O que é"],
                        [
                            ["<code>infra/docker/airflow/Dockerfile</code>", "Imagem do Airflow, em três estágios."],
                            ["<code>infra/docker/superset/Dockerfile</code>", "Imagem do Superset."],
                            ["<code>infra/docker/postgres/init.sh</code>", "Criação dos bancos na primeira subida."],
                            ["<code>infra/docker-compose.yml</code>", "Sete serviços e dois volumes."],
                            ["<code>infra/airflow/airflow.cfg</code>", "Configuração do Airflow, montada no contêiner."],
                            ["<code>infra/env/.env.example</code>", "Modelo de variáveis, com valores de desenvolvimento."],
                            ["<code>Makefile</code>", "Comandos de ciclo de vida."],
                            ["<code>requirements.txt</code>", "Dependências instaladas na imagem."],
                            ["<code>.github/workflows/main.yaml</code>", "Construção e publicação automatizadas."],
                        ],
                        None,
                        ["44%", "56%"],
                    ),
                    (
                        "callout",
                        "shield-check",
                        "Este documento descreve o ambiente local",
                        [
                            "O <code>docker-compose.yml</code> define o ambiente de "
                            "desenvolvimento. A topologia de produção depende de decisões de "
                            "infraestrutura ainda em aberto com o SERPRO, e por isso a "
                            "arquitetura física é objeto de outro produto, não deste.",
                        ],
                    ),
                ],
            ],
        },
        # ─────────────────────────────────────────────────────────────────
        {
            "num": "02",
            "eyebrow": "Imagem",
            "titulo": "A construção do Airflow",
            "icone": "code",
            "paginas": [
                [
                    (
                        "p",
                        "O Dockerfile tem três estágios. O primeiro prepara o sistema "
                        "operacional; os outros dois só diferem na variável "
                        "<code>ENVIRONMENT</code>, o que permite construir a mesma imagem para "
                        "desenvolvimento e para produção sem duplicar instruções.",
                    ),
                    (
                        "table",
                        ["Estágio", "O que faz"],
                        [
                            [
                                "<code>airflow-base</code>",
                                "Parte de <code>apache/airflow:3.2.2-python3.11</code>. Instala "
                                "bibliotecas de sistema, gera as localidades en_US e pt_BR, e "
                                "copia o arquivo de senhas do gerenciador de autenticação.",
                            ],
                            [
                                "<code>airflow-prod</code>",
                                "Instala as dependências, confere se a versão do Airflow "
                                "instalada é a esperada, roda <code>pip check</code> e define "
                                "<code>ENVIRONMENT=prod</code>.",
                            ],
                            [
                                "<code>airflow-dev</code>",
                                "Idêntico ao anterior, com <code>ENVIRONMENT=dev</code>. É o "
                                "alvo padrão do Compose.",
                            ],
                        ],
                        None,
                        ["24%", "76%"],
                    ),
                    ("h3", "Certificados da ICP-Brasil"),
                    (
                        "p",
                        "A imagem baixa e instala a cadeia de certificados da ICP-Brasil, e "
                        "rebaixa o nível mínimo de TLS e a política de cifras do OpenSSL. Isso "
                        "não é descuido: vários serviços de governo apresentam certificados "
                        "que a configuração padrão do Debian recusa, e sem esse ajuste as "
                        "chamadas de API falham no aperto de mão TLS.",
                    ),
                    (
                        "callout",
                        "check-badge",
                        "Verificação embutida na construção",
                        [
                            "Os dois estágios finais conferem, dentro do próprio "
                            "<code>RUN</code>, se <code>airflow.__version__</code> é igual à "
                            "versão pedida, e executam <code>pip check</code>. Uma "
                            "incompatibilidade de dependência quebra a construção da imagem, "
                            "não a execução da DAG.",
                        ],
                    ),
                ],
            ],
        },
        # ─────────────────────────────────────────────────────────────────
        {
            "num": "03",
            "eyebrow": "Topologia local",
            "titulo": "Os serviços do Compose",
            "icone": "server",
            "paginas": [
                [
                    (
                        "table",
                        ["Serviço", "Imagem ou origem", "Porta", "Papel"],
                        [
                            [
                                "<code>postgres</code>",
                                "<code>postgres:15-alpine</code>",
                                "5432",
                                "Banco de metadados do Airflow e data warehouse.",
                            ],
                            [
                                "<code>airflow</code>",
                                "Construída localmente",
                                "8080",
                                "Agendador, interface e execução, em modo <code>standalone</code>.",
                            ],
                            [
                                "<code>airflow-mcp</code>",
                                "Construída localmente",
                                "8000",
                                "Servidor MCP para operar o Airflow. Exposto só em <code>127.0.0.1</code>.",
                            ],
                            [
                                "<code>superset</code>",
                                "Construída localmente",
                                "8088",
                                "Visualização e exploração dos dados.",
                            ],
                            [
                                "<code>minio</code>",
                                "<code>minio/minio</code>",
                                "9000, 9001",
                                "Armazenamento de objetos: pouso dos anexos e dos lançamentos.",
                            ],
                            [
                                "<code>minio-init</code>",
                                "<code>minio/mc</code>",
                                "",
                                "Cria os buckets na primeira subida e encerra.",
                            ],
                        ],
                        "Dois volumes nomeados persistem o estado: <code>postgres-db</code> e "
                        "<code>minio-data</code>.",
                        ["22%", "26%", "16%", "36%"],
                    ),
                    ("h3", "Ordem de inicialização"),
                    (
                        "p",
                        "O <code>postgres</code> sobe primeiro e executa "
                        "<code>init.sh</code>, que cria os bancos <code>airflow</code> e "
                        "<code>data_warehouse</code>. Esse script roda uma única vez, na "
                        "criação do volume: se o volume já existe, ele não é reexecutado. Os "
                        "demais serviços declaram dependência do banco e só sobem depois que "
                        "ele responde.",
                    ),
                    (
                        "code",
                        "psql -v ON_ERROR_STOP=1 --username \"$POSTGRES_USER\" \\\n"
                        "     --dbname \"$POSTGRES_DB\" <<-EOSQL\n"
                        "  CREATE DATABASE airflow;\n"
                        "  CREATE DATABASE data_warehouse;\n"
                        "EOSQL",
                        "<code>infra/docker/postgres/init.sh</code>",
                    ),
                ],
            ],
        },
        # ─────────────────────────────────────────────────────────────────
        {
            "num": "04",
            "eyebrow": "Configuração",
            "titulo": "Variáveis de ambiente",
            "icone": "settings",
            "paginas": [
                [
                    (
                        "p",
                        "O Compose lê <code>infra/.env</code>. O modelo versionado é "
                        "<code>infra/env/.env.example</code>, com valores de desenvolvimento. "
                        "Nenhum arquivo com valor real de credencial entra no repositório.",
                    ),
                    (
                        "table",
                        ["Variável", "Papel"],
                        [
                            ["<code>AIRFLOW_VERSION</code>", "Versão da imagem base. Conferida durante a construção."],
                            ["<code>AIRFLOW_BUILD_TARGET</code>", "Estágio a construir. Padrão <code>airflow-dev</code>."],
                            ["<code>AIRFLOW_UID</code>", "Usuário do contêiner. Alinha a propriedade dos arquivos montados."],
                            ["<code>POSTGRES_USER</code> · <code>POSTGRES_PASSWORD</code>", "Credenciais do banco."],
                            ["<code>AIRFLOW__CORE__FERNET_KEY</code>", "Chave que cifra senhas e campos sensíveis das Connections."],
                            ["<code>AIRFLOW__API__SECRET_KEY</code>", "Segredo de autenticação entre os subprocessos."],
                            ["<code>AIRFLOW__API_AUTH__JWT_SECRET</code>", "Segredo de assinatura dos tokens internos."],
                            ["<code>AIRFLOW_PRUNE_STALE_DAGS</code>", "Remove DAGs que sumiram do repositório."],
                            ["<code>MINIO_ACCESS_KEY</code> · <code>MINIO_SECRET_KEY</code>", "Credenciais do armazenamento de objetos."],
                        ],
                        None,
                        ["40%", "60%"],
                    ),
                    (
                        "callout",
                        "shield-check",
                        "Duas variáveis que exigem atenção antes de qualquer implantação real",
                        [
                            "<strong>A chave Fernet nunca deve ser trocada sem rotação.</strong> "
                            "Trocá-la sem executar <code>airflow rotate-fernet-key</code> torna "
                            "toda Connection e Variable sensível existente indecifrável, "
                            "exigindo apagar e recriar tudo. O aviso está registrado no próprio "
                            "arquivo de exemplo porque o problema já ocorreu no projeto.",
                            "<strong>Os segredos de API e JWT não podem ficar vazios.</strong> "
                            "Se ficarem, cada subprocesso do Airflow gera um valor próprio e as "
                            "tarefas falham na verificação de assinatura.",
                        ],
                    ),
                ],
                [
                    ("h3", "Valores padrão que servem só a desenvolvimento"),
                    (
                        "p",
                        "Para que a subida local funcione sem configuração, o Compose define "
                        "valores padrão para alguns segredos. Eles cumprem o papel de reduzir "
                        "atrito no ambiente de desenvolvimento, e precisam ser substituídos "
                        "antes de qualquer implantação com dado real.",
                    ),
                    (
                        "table",
                        ["Item", "Situação", "Ação antes de produção"],
                        [
                            [
                                "Chave Fernet",
                                "Valor padrão embutido no Compose.",
                                "Gerar chave própria e definir no <code>.env</code>.",
                            ],
                            [
                                "Segredos de API e JWT",
                                "Valores padrão de desenvolvimento.",
                                "Gerar com <code>openssl rand -hex 32</code>.",
                            ],
                            [
                                "Usuário do Airflow",
                                "<code>airflow:admin</code> pelo gerenciador simples.",
                                "Substituir por autenticação institucional.",
                            ],
                            [
                                "Chave do Superset",
                                "Valor fixo no Compose.",
                                "Mover para o <code>.env</code> e gerar valor próprio.",
                            ],
                            [
                                "Credenciais do Postgres",
                                "<code>postgres/postgres</code> no exemplo.",
                                "Definir credenciais reais no <code>.env</code>.",
                            ],
                        ],
                        None,
                        ["24%", "36%", "40%"],
                    ),
                    (
                        "callout",
                        "clipboard-document-check",
                        "Fuso horário definido explicitamente",
                        [
                            "<code>AIRFLOW__CORE__DEFAULT_TIMEZONE</code> e o fuso da interface "
                            "estão fixados em <code>America/Sao_Paulo</code>. É o mesmo fuso do "
                            "carimbo <code>dt_transform</code> do catálogo de metadados, o que "
                            "evita divergência entre o horário de execução registrado no "
                            "Airflow e o horário gravado na tabela.",
                        ],
                    ),
                ],
            ],
        },
        # ─────────────────────────────────────────────────────────────────
        {
            "num": "05",
            "eyebrow": "Operação",
            "titulo": "Comandos e automação",
            "icone": "workflow",
            "paginas": [
                [
                    ("h3", "Comandos do Makefile"),
                    (
                        "table",
                        ["Comando", "O que faz"],
                        [
                            ["<code>make setup</code>", "Instala o Poetry, resolve as dependências, exporta o arquivo de requisitos e instala os ganchos de git."],
                            ["<code>make up</code>", "Sobe banco, Airflow e servidor MCP."],
                            ["<code>make down</code>", "Encerra os serviços."],
                            ["<code>make logs-airflow</code>", "Mostra as últimas 200 linhas do Airflow."],
                            ["<code>make compose-config</code>", "Valida o Compose, resolvendo variáveis, sem subir nada."],
                            ["<code>make format</code>", "Aplica black, ruff e sqlfmt."],
                            ["<code>make lint</code>", "Confere black, ruff, mypy, sqlfmt e sqlfluff."],
                            ["<code>make test</code>", "Executa a suíte de testes."],
                        ],
                        None,
                        ["28%", "72%"],
                    ),
                    (
                        "callout",
                        "check-badge",
                        "Validar antes de subir",
                        [
                            "<code>make compose-config</code> resolve todas as variáveis e "
                            "imprime o Compose final sem iniciar contêiner nenhum. É a forma "
                            "mais barata de descobrir que falta uma variável no "
                            "<code>.env</code>.",
                        ],
                    ),
                    ("h3", "Construção e publicação automatizadas"),
                    (
                        "p",
                        "O fluxo em <code>.github/workflows/main.yaml</code> tem quatro "
                        "etapas encadeadas. A publicação da imagem só acontece em integração "
                        "na ramificação principal.",
                    ),
                ],
                [
                    (
                        "table",
                        ["Etapa", "Quando", "O que faz"],
                        [
                            [
                                "<code>lint</code>",
                                "Todo PR e toda integração",
                                "Executa <code>make lint-ci</code>, que confere apenas o SQL.",
                            ],
                            [
                                "<code>test</code>",
                                "Todo PR e toda integração",
                                "Executa a suíte com relatório de cobertura, publicado como "
                                "artefato.",
                            ],
                            [
                                "<code>docker_build</code>",
                                "Depois de lint e test",
                                "Constrói o estágio <code>airflow-prod</code> com cache, sem "
                                "publicar.",
                            ],
                            [
                                "<code>docker_push</code>",
                                "Só na ramificação principal",
                                "Publica no registro do GitHub com as etiquetas do commit e "
                                "<code>latest</code>.",
                            ],
                        ],
                        None,
                        ["22%", "26%", "52%"],
                    ),
                    (
                        "callout",
                        "shield-check",
                        "O que a automação hoje não impede",
                        [
                            "A etapa de lint termina com <code>|| true</code>, por decisão "
                            "registrada da equipe: ela informa, mas não reprova o PR. Além "
                            "disso, <code>make lint-ci</code> cobre só SQL, de modo que black, "
                            "ruff e mypy não são executados na automação, embora estejam "
                            "configurados e sejam exigidos pelo <code>make lint</code> local.",
                            "A suíte de testes é executada com medição de cobertura, mas sem "
                            "limite mínimo. Na prática, a única etapa que reprova uma "
                            "integração hoje é a construção da imagem.",
                        ],
                    ),
                    ("h3", "Encerramento de escopo"),
                    (
                        "p",
                        "Procedimento de retorno a versão anterior, testes de verificação "
                        "pós-implantação e política de cópia de segurança não constam do "
                        "repositório e, por isso, não estão documentados aqui. Eles pertencem "
                        "ao runbook de implantação, que depende de decisões ainda não "
                        "tomadas.",
                    ),
                ],
            ],
        },
    ],
}
