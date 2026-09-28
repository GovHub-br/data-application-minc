# OpenMetadata

Ingestão de metadados para o OpenMetadata, rodando como DAG do Airflow 3. Publica
o catálogo de tabelas, a linhagem entre pipeline e tabela, e a documentação do
projeto dbt (descrição, domínio, tier, dono, glossário).

Este guia serve a dois leitores: quem mexe nisso aqui, e quem quer **levar o
módulo para outro projeto**. A tabela de portabilidade abaixo diz o que vai
inteiro e o que precisa de adaptação.

## Como uma recipe roda

```text
openmetadata_ingestion_dag
  ├─ glossary.sync_glossary                 primeira task, antes de tudo
  └─ runner.run_openmetadata_recipe         uma task por recipe, ligada por flag
       ├─ dbt_artifacts.prepare_dbt_artifacts   só na recipe de dbt
       ├─ rendering.render_recipe               ${VAR} -> valor, em tmpdir
       └─ workflows.execute_metadata            despacha e executa
```

`execute_metadata` lê o `source.type` da recipe já renderizada e escolhe a via:

| comando | source | via |
|---|---|---|
| `profile` | qualquer | API Python, no processo |
| `classify` | qualquer | API Python, no processo |
| `ingest` | `airflow` | API Python, no processo |
| `ingest` | outros | CLI `metadata -c <recipe>`, em subprocesso |

O source `airflow` não roda pelo CLI porque ele inicializa o próprio pacote
`airflow`, e num subprocesso isso falha com erro genérico de plugin ausente.
Profiler e classifier rodam no processo para que o patch de paginação
(`set_entity_list_page_size`) pegue — sem ele o fetcher busca as tabelas em
página de 100 e estoura timeout de 60s, que chega parecendo problema de rede.

A recipe renderizada vive num `TemporaryDirectory` que o `ExitStack` remove
mesmo se a execução levantar. **O token nunca toca disco fora dali.**

## Os módulos

| Módulo | O que faz | Portável? |
|---|---|---|
| `workflows.py` | executa a recipe, despacha CLI vs in-process, corrige a paginação do cliente | **Sim, inteiro** |
| `rendering.py` | substitui `${VAR}` e falha se sobrar marcador | **Sim, inteiro** |
| `runner.py` | orquestra os três acima | **Sim, inteiro** |
| `dbt_artifacts.py` | copia o projeto dbt e gera os artefatos que a recipe consome | **Sim, inteiro** |
| `glossary.py` | valida e sincroniza glossário, de forma idempotente. **Primeira task da DAG** | Sim — recebe os caminhos por argumento |
| `config.py` | catálogo de recipes, flags de liga/desliga, ordem do pipeline | Não — nomes e caminhos do MinC |
| `lineage.py` | `tabela()` para `inlets`/`outlets` e a task que publica | Quase — troque os defaults de serviço |
| `recipes/*.yaml` | uma recipe por fonte | Não — filtros de schema e nome de serviço |
| `glossaries/minc.*` | os termos do MinC | Não — é conteúdo |
| `semantic_relationships.py` | catálogo de relações semânticas | Ver "não conectado", abaixo |

`rendering.py` **levanta** se sobrar um `${...}` sem valor. É de propósito: sem
isso a recipe seguia com o literal `${INGESTION_TOKEN}` no lugar do token, o
servidor respondia erro de autenticação, e o rastro apontava para credencial
inválida em vez de para a chave que faltou nos replacements.

## Levar para outro projeto

Copie `helpers/openmetadata/` inteiro e a `dags/openmetadata_ingestion_dag.py`.
Depois, quatro ajustes:

1. **`config.py`** — `OM_DBT_PROJECT_DIR` (o projeto dbt, relativo a
   `AIRFLOW_REPO_BASE`), a tupla `_DEFINED_RECIPES` e a ordem em
   `RECIPE_PIPELINE`. A ordem importa: `postgres_metadata` cria as tabelas no
   catálogo, `dbt_metadata` anexa descrição e linhagem, profiler e classifier só
   têm o que medir depois disso.
2. **`lineage.py`** — `OM_SERVICE`, `OM_DATABASE` e `OM_PIPELINE_SERVICE` têm
   default para o MinC. Eles precisam casar com o `serviceName`/`database` das
   recipes, senão a linhagem aponta para tabelas que não existem no catálogo.
3. **`recipes/*.yaml`** — `serviceName` e o `schemaFilterPattern`.
4. **Variables do Airflow** — `OM_HOST`, `INGESTION_TOKEN`, `PROFILER_TOKEN`,
   `CLASSIFICATION_TOKEN`. Não são variáveis de ambiente de propósito: são
   segredo, e ficam criptografadas no banco de metadados.

As flags `OM_INGEST_*` são variáveis de **ambiente**, não Variables, porque a
decisão acontece no parse da DAG — buscar Variable a cada parse bate no banco de
metadados a cada poucos segundos. O custo é precisar recriar o container para
mudar uma flag, o que é aceitável para algo que muda quando a infra muda.

O pacote `openmetadata-ingestion` fica só em
[`infra/docker/airflow/requirements.lock.txt`](../../infra/docker/airflow/requirements.lock.txt),
fora do Poetry: resolvê-lo junto rebaixa pacotes que a imagem base já traz, e o
`uv pip check` do build reprova. Consequência para os testes: três guardas de
ambiente em `tests/test_openmetadata_execution.py` pulam onde ele não existe e
rodam dentro da imagem, no job `docker_build`.

## Armadilhas que já custaram tempo

**O conector dbt não cria tabela.** Ele faz `es_search_from_fqn` e *anexa*
metadado a tabela que já existe. Quem cria é a `postgres_metadata`. Se um modelo
dbt não tem tabela materializada no banco, ele não aparece no OpenMetadata, e
nenhuma configuração muda isso — o que falta é `dbt run`, não recipe.

**`markDeletedTables` tem default `true`.** Rodar `postgres_metadata` contra um
banco incompleto marca como deletado tudo que o catálogo tem e o banco não. Um
ambiente restaurado pela metade apaga catálogo inteiro sem avisar. A recipe
declara `false` explícito, e dois testes guardam isso: um exige a chave ali,
outro proíbe declará-la nas demais — profiler e classifier não são
`DatabaseMetadata` e não apagam nada, então protegê-los seria teatro.

**`meta` de modelo dbt vai sob `config`.** A partir do dbt 1.10, declarar `meta`
no topo do modelo *e* em `config.meta` aborta o parse. Coluna e source seguem no
topo. Ver [`dbt/README.md`](../../dbt/README.md).

**`dbt docs generate` precisa do banco.** É ele que monta o `catalog.json`. Sem
conexão, `prepare_dbt_artifacts` falha antes de qualquer contato com o
OpenMetadata — mesmo que só o `manifest.json` seja obrigatório.

**O caminho do CLI não sai de `sys.executable`.** O task runner do Airflow roda
com outro Python, e derivar dali resultava em `FileNotFoundError` num caminho que
não existe. É `shutil.which`.

**Linhagem é operator, não backend.** O `OpenMetadataLineageBackend`, que a
documentação apresenta como configuração de `airflow.cfg`, importa
`airflow.lineage.backend` — módulo removido no Airflow 3.

## Ordem, e por que ela não é negociável

O glossário abre a corrente, antes de qualquer recipe. O motivo não aparece em
erro nenhum: `meta.openmetadata.glossary` nos `schema.yml` apenas **referencia**
termo por FQN, e quem cria o termo é o sync. Se o termo ainda não existe quando
a recipe roda, a referência é descartada em silêncio e o ativo chega ao catálogo
sem o vínculo — nenhuma task falha, nada aparece no log.

Depois vem `postgres_metadata`, que cria as tabelas; depois `dbt_metadata`, que
anexa descrição e linhagem; profiler e classifier por último, porque só têm o
que medir depois disso.

O sync é idempotente e **não remove termo remoto**: ele aplica a hierarquia
primeiro e as relações depois, quando todos os FQNs já existem. Editar
`glossaries/minc.csv` e rodar a DAG basta.

## Não conectado

**`semantic_relationships.py`** — 675 linhas, e valida
`kind: MCIDSemanticRelationshipCatalog`: é do Ministério das **Cidades**, de
onde a integração foi portada. Nenhum catálogo desse formato existe neste
repositório, e criar um é uma decisão à parte — não efeito colateral de ligar o
glossário. Quem for absorver o módulo pode deixá-lo de fora sem perder nada do
que está descrito acima.

## As flags

| Flag | Default | Por quê |
|---|---|---|
| `OM_INGEST_GLOSSARY` | `true` | cria os termos que os `schema.yml` referenciam |
| `OM_INGEST_POSTGRES` | `true` | cria os ativos no catálogo — é o trabalho da DAG |
| `OM_INGEST_DBT` | `true` | anexa descrição, tag e linhagem aos ativos |
| `OM_INGEST_AIRFLOW` | `true` | cataloga pipelines; o catálogo de dados não depende dela |
| `OM_INGEST_CLASSIFIER` | `true` | acha PII **sem** persistir amostra (`storeSampleData: false`) |
| `OM_INGEST_PROFILER` | `false` | publica min, max e distribuição — estatística reveladora num banco com CPF, CNPJ e dados de raça e deficiência. Ligue só depois de verificar as exclusões de coluna sensível |
| `OM_INGEST_SUPERSET` | `false` | o MinC não tem instância própria |

Flag declarada no `config.py` precisa aparecer na `environment:` do
`docker-compose.yml`, senão ela é lida no parse **dentro do container**, não
encontra nada, e o default do código vence — mexer no `.env` não muda coisa
alguma. Há um teste para isso.
