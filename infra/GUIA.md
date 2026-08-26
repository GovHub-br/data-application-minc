# Infra

Arquivos de execucao local do projeto.

## Compose

Use os atalhos da raiz:

```bash
make compose-config
make up
make logs-airflow
make down
```

Ou chame o Compose diretamente:

```bash
docker compose -f infra/docker-compose.yml up postgres airflow airflow-mcp
```

O Trino fica atras do profile `trino` e nao sobe no `make up` — use
`make up-trino`. O porque, e onde ficam as conexoes dos bancos, esta em
[`trino/GUIA.md`](trino/GUIA.md).

## Layout

```text
infra/
├── airflow/              # airflow.cfg usado no ambiente local
├── docker/
│   ├── airflow/          # imagem principal do Airflow
│   ├── airflow-mcp/      # imagem leve do MCP
│   └── postgres/         # scripts de init do Postgres
├── env/                  # exemplos de variaveis de ambiente
├── trino/                # config e catalogos do Trino (ingestao SALIC v2)
└── docker-compose.yml
```
