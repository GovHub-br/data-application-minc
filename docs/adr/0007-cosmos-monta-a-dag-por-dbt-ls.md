# ADR 0007 — O Cosmos monta a DAG por `dbt ls`, não por manifest versionado

- Status: aceito
- Data: 2026-09-06
- Escopo: `dags/dbt/minc_cosmos_dag.py` e o parse do dag processor
- Substitui: a decisão não registrada que introduziu `dbt/minc/manifest.json`

## Contexto

Por padrão o Cosmos monta a `DbtDag` rodando `dbt ls` durante o parse do arquivo
da DAG. Medido em 06/09/2026, com os 647 modelos ativos deste projeto: **102s**
em container novo, sem `partial_parse.msgpack`, e **34s** com ele. O `dbt ls`
reporta 647 models, 5.727 data tests e 671 sources.

O `core.dagbag_import_timeout` do Airflow 3.2 é 30s por padrão, e o
`infra/airflow/airflow.cfg` deste repositório só define `dags_folder`,
`plugins_folder` e `extra_path`. Ou seja: todos os timeouts de parse estavam no
default, e os dois cenários — 102s e 34s — estouram o de 30s. O sintoma era a
DAG desaparecer da UI.

A resposta anterior foi trocar o load method por `LoadMode.DBT_MANIFEST` e
versionar `dbt/minc/manifest.json`, gerado por `scripts/gerar_manifest_dbt.sh`.
Isso tornava o parse imediato, ao preço de um artefato gerado dentro do git que
precisava ser regerado e commitado a cada mudança em modelo, source ou teste.

## Decisão

O load method volta a ser `LoadMode.DBT_LS` e os timeouts de parse sobem em
`infra/docker-compose.yml`:

```yaml
AIRFLOW__CORE__DAGBAG_IMPORT_TIMEOUT: 300
AIRFLOW__DAG_PROCESSOR__DAG_FILE_PROCESSOR_TIMEOUT: 360
```

Os dois sobem juntos de propósito. Mexer só no `dagbag_import_timeout` faria o
`dag_file_processor_timeout` — 50s por padrão — virar a parede seguinte, já que
ele é o teto do primeiro. Os valores dão ~3x de folga sobre o pior caso medido,
de propósito: o número cresce com o projeto, e o custo de um timeout alto é só
um parse travado demorar mais para ser abortado.

`dbt/minc/manifest.json` deixa de ser versionado, e saem com ele a exceção no
`.gitignore`, o `scripts/gerar_manifest_dbt.sh`, o alvo `make dbt-manifest` e o
`tests/test_dbt_manifest.py`.

## Por quê

**O custo não é recorrente.** O Cosmos 1.14.2 tem `enable_cache_dbt_ls`
ligado por padrão. O `cache.py` calcula um hash do conteúdo do projeto
(`_calculate_dbt_ls_cache_current_version`) e decide por `was_project_modified`
se precisa re-executar, guardando o resultado em Airflow Variable. O custo é
pago quando alguém mexe no dbt, não a cada `min_file_process_interval`. O
argumento que sustentava o manifest tratava esse tempo como custo de todo ciclo
de parse, e isso não se confirma na versão em uso.

**O manifest versionado tinha uma classe de bug própria, silenciosa.** Se ele
ficasse velho, modelo novo não virava task e modelo removido continuava
aparecendo — sem erro nenhum. `tests/test_dbt_manifest.py` existia só para
transformar esse desencontro em falha de CI. Com `dbt ls` a DAG passa a refletir
o projeto real por construção, e o vigia deixa de ser necessário.

**Timeout de parse alto é barato.** O único efeito de um valor generoso é um
parse travado demorar mais para ser abortado. Não há custo em regime normal.

**As duas DAGs são idênticas.** Montando a DAG pelos dois caminhos sobre o mesmo
projeto — `LoadMode.DBT_LS` e `LoadMode.DBT_MANIFEST` com o manifest de
`55fefcc` — saem 1.212 tasks dos dois lados, com os mesmos `task_id`. A troca
não muda o que roda.

## O que foi descartado

**Gerar o manifest no entrypoint do container.** Preserva o parse instantâneo
sem versionar artefato, mas paga os ~102s em todo boot e reintroduz o problema
da defasagem: o manifest nasce no start e envelhece durante a vida do container.

**Assar o manifest na imagem.** O `infra/docker-compose.yml` monta `../dbt` como
bind-mount em `/opt/airflow/dbt/`, então o container lê o checkout, não a
imagem. Assar exigiria largar o bind-mount, e aí editar um modelo passaria a
exigir rebuild — piora o loop de desenvolvimento para resolver um problema de
parse.

**Gerar por hook de pre-commit.** Tira o passo manual de esquecer, mas mantém
artefato gerado no histórico do git, que é a parte que incomoda.

## Consequências

O `dbt ls` roda no parse, então o dag processor precisa do dbt executável e de
um `profiles.yml` que resolva — `dbt ls` não conecta no banco, mas falha se o
profile não renderizar.

`parsing_processes` é 2. Num cache miss, o `dbt ls` segura uma das duas vagas de
parse por 34s a 102s, atrasando o refresh das outras DAGs nesse intervalo.
Aceitável porque só acontece depois de mudança no projeto dbt — mas é o custo
real desta decisão, e cresce junto com a bronze do SALIC.

Quem subir o Airflow com um `.env` antigo não herda os timeouts novos: eles
estão no `x-airflow-environment` do compose, então basta recriar os containers,
mas um deploy que fixe as envs em outro lugar precisa recebê-los à mão.

Se o `dbt ls` crescer a ponto de incomodar mesmo com cache, a saída não é voltar
ao manifest versionado: é `RenderConfig(select=...)` para parsear menos, ou o
`remote_cache_dir` do Cosmos 1.6+.
