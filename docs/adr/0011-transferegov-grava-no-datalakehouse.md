# ADR 0011 — A ingestão do TransfereGov grava no datalakehouse, e uma ponte devolve ao Postgres

- Status: aceito
- Data: 2026-10-05
- Escopo: `dags/data_ingest/transferegov_fundo_a_fundo/`, `helpers/datalakehouse.py`
- Origem: PR #58 e a revisão dele

## Contexto

As DAGs de API do TransfereGov Fundo a Fundo gravavam direto nas tabelas
`transferegov.*` do Postgres, com `insert_data` em upsert. Três problemas
vinham daí:

- **O que a API respondeu se perdia.** A linha do Postgres misturava o payload
  com o que é decisão do MinC — `sigla` e `politica_publica` do programa, os
  campos territoriais do plano de ação. Não havia como reprocessar a partir da
  resposta original.
- **Listas e dicts aninhados viravam `repr` do Python**, que nenhum parser lê
  de volta.
- **A lista inteira de registros trafegava por XCom** entre tasks, e com
  alguns milhares de planos isso estoura o IPC do Airflow 3.

## Decisão

Cada entidade é gravada em duas camadas do bucket `minc-datalakehouse`, no
mesmo caminho:

```text
raw/transferegov/<entidade>/ano=AAAA/mes=MM/dia=DD/<run_id>.json        resposta da API
staging/transferegov/<entidade>/ano=AAAA/mes=MM/dia=DD/<run_id>.parquet tabular, tudo texto
```

O encadeamento entre DAGs é por **Asset**, um por entidade, com URI estático
(`s3://minc-datalakehouse/staging/transferegov/<entidade>`). Quem grava o
staging emite um evento cujo `extra` carrega a *cadeia* `entidade -> key`
acumulada desde o programa; quem consome lê dali a key exata. A cadeia é:

```text
programas → planos de ação → metas
                           → dado bancário
                           → relatórios de gestão → anexos → download → extração das planilhas
movimentações financeiras (independente)
```

Uma DAG-ponte por entidade (`staging_para_postgres_<entidade>_dag`),
disparada pelo mesmo Asset, faz upsert do Parquet na tabela `transferegov.*`
que a ingestão alimentava antes, com a mesma chave. O dbt não muda.

## Por quê

**A key no evento, e não o arquivo mais recente do prefixo.** A primeira
versão do PR escolhia o Parquet de maior `LastModified`. Sem marca de "run
completo", um run parcial virava a verdade de todas as DAGs filhas; e as DAGs
de anexos liam três entidades por três chamadas independentes, que podiam
vir de runs diferentes — o INNER JOIN descartava anexos sem avisar. Com a
cadeia no `extra`, os três arquivos são sempre do mesmo run.

**A ponte devolve ao Postgres porque o dbt lê de lá.** A bronze
`transferegov_bronze` é `view` sobre a source `transferegov`. Sem a ponte, ela
não falharia: continuaria verde, devolvendo para sempre o dado do último run
antes da mudança.

**Upsert mantém a tabela cumulativa.** Cada staging é um snapshot do que a
API devolveu naquele dia. Se ela devolver menos — um programa que não
responde só gera aviso —, o upsert não apaga o que já estava carregado.

**O Parquet repete o contrato do `insert_data`.** Separador `__` entre
níveis, nomes de coluna em minúsculas (o backend responde em camelCase:
`tipoAnexo.auditLogin` vira `tipoanexo__auditlogin`) e tudo como texto.
Booleanos saem `true`/`false`, que é o que o Postgres já gravava ao converter
`bool` para `text`.

**O que pode entrar no raw.** O raw guarda o payload mais as chaves de
ligação: a chave da requisição que o payload não traz (`id_relatorio_gestao`
no anexo) e as chaves propagadas do plano-pai que a seção 9.2 exige
(`id_plano_acao`, `id_programa`, `cod_ibge`). Valor derivado de regra do
MinC — território, política pública, URL de consulta — entra só no staging,
pelo `enriquecer` de `raw_para_staging`.

## O que foi descartado

- **Trino lendo o Parquet direto.** É o precedente do ADR 0005, mas não
  existe catálogo Hive/Iceberg sobre o MinIO: os catálogos em
  `infra/trino/etc/catalog/` são todos `postgresql` e `sqlserver`, e um
  metastore é infra nova.
- **DuckDB.** Seria um terceiro motor no stack para um volume — a maior
  entidade tem ~95 mil linhas — que pandas resolve em segundos.
- **`TriggerDagRunOperator` entre as DAGs**, como antes. Ele dispara a filha,
  mas não diz qual arquivo ela deve ler.
- **Só `source freshness` nas sources `transferegov`.** Deixaria a bronze
  vermelha em vez de congelada, mas parada do mesmo jeito.

## Consequências

- `api_planos_acao_dag`, `api_plano_acao_meta_dag` e
  `api_plano_acao_dado_bancario_dag` deixam de ter agendamento próprio pela
  Variable `dynamic_schedules`: rodam quando a mãe produz um arquivo novo. O
  único cron da cadeia é o de `api_programas_dag`.
- Para rodar uma DAG da cadeia à mão, sem evento, a key vai no conf do run:
  `{"plano_acao_minc": "staging/transferegov/plano_acao_minc/.../x.parquet"}`.
- `dt_ingest` passa a ser um valor por conversão, e não por linha. O teste
  `unique` dele nas sources `transferegov` saiu.
- A linhagem OpenMetadata (`publicar_linhagem`) passa a ser publicada pelas
  pontes, que são quem escreve nas tabelas `transferegov.*`.
- A coluna `caminho_minio` de `transferegov.anexos_relatorios` deixa de ser
  atualizada. Quem diz se um anexo já foi baixado é a listagem de
  `raw/transferegov/anexos_arquivos/`.
- Se um dia o dbt ler o staging direto — por um catálogo sobre o MinIO —, as
  pontes saem e este ADR é substituído.
