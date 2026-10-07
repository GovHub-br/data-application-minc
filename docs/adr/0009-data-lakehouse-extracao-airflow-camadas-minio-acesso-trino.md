# ADR 0009 — A plataforma de dados do MinC é um Data Lakehouse: extração pelo Airflow, camadas no MinIO e todo acesso via Trino

- **Data:** 2026-10-06
- **Status:** aceito
- **Escopo:** arquitetura de produção da plataforma de dados do MinC
- **Desenho:** [`docs/arquitetura/arquitetura-dados-minc.png`](../arquitetura/arquitetura-dados-minc.png)
- **Complementado por:** [ADR 0010](0010-camadas-e-schemas-do-data-lakehouse.md), que fixa as camadas e a nomenclatura

## Contexto

Este repositório nasceu rodando o dbt sobre PostgreSQL: as DAGs do Airflow
gravam direto em schemas do Postgres, o dbt lê e grava no mesmo banco, e o
consumo (Superset, OpenMetadata, consultas à mão) lê o Postgres. Funciona
para o volume de hoje, mas três coisas não cabem nesse desenho:

- **Topologia.** Em produção o Airflow roda na infra do Serpro e os bancos
  ficam na infra do MinC, em redes separadas. O Airflow só tem rota até o
  Trino — o [ADR 0005](0005-ingestao-salic-por-trino-em-fatias.md) já
  registra que qualquer `psycopg2` reintroduzido na ingestão passa no ambiente
  local e quebra em produção.
- **Dado pessoal.** O repositório lida com CPF, CNPJ e dados de raça e
  deficiência de agentes culturais. Anonimizar em repouso exigiria uma cópia
  por perfil de acesso; não anonimizar exige que todo acesso passe por um ponto
  onde a política possa ser aplicada.
- **Auditoria e reprocessamento.** O dado bruto hoje é sobrescrito a cada
  carga (`DROP` + `CREATE` + recarga, como no ADR 0008). Não há como
  reconstruir uma camada a partir do que chegou numa data passada.

Em outubro de 2026 o MinC aprovou a arquitetura de produção desenhada em
`docs/arquitetura/arquitetura-dados-minc.png`. Este ADR a registra como
decisão. A seção "Arquitetura de dados" do `README.md` é a narrativa; este
documento é o registro do que foi decidido e do que foi descartado.

## Decisão

A plataforma é um **Data Lakehouse**. Os componentes e o papel de cada um:

| Componente | Papel |
|---|---|
| **Apache Airflow** | Orquestra a plataforma e faz a extração. As DAGs extraem os dados das fontes (SALIC, TransfereGov, PNAB, BSC) por *pull* e gravam os arquivos na zona `raw` do MinIO. CDC é a evolução prevista |
| **MinIO** | Object storage. Guarda a zona `raw` e os arquivos Parquet de todas as tabelas, da Bronze à Gold |
| **Apache Iceberg** | Formato de tabela da Bronze, Silver, Intermediate e Gold. Dá transações, snapshots, evolução de schema e *time travel* sobre os Parquet |
| **Apache Polaris** | Catálogo Iceberg (API REST). Ponto único de leitura e escrita das tabelas |
| **PostgreSQL** | Persistência do catálogo: guarda só os metadados do Iceberg, que o Polaris acessa via JDBC. **Não guarda dado das camadas** |
| **Trino** | Motor único de consulta. Carrega o `raw` na Bronze, executa o SQL do dbt, lê e grava as tabelas Iceberg através do Polaris e serve todo o consumo |
| **dbt** (`dbt-trino`) | Define as transformações da Bronze à Gold. Monta o SQL e o executa no Trino |
| **Apache Ranger** | Políticas de acesso aplicadas no Trino: controle por linha e por coluna, mascaramento, anonimização na consulta e auditoria |

O fluxo, em quatro passos:

1. **Extração.** As DAGs do Airflow extraem das fontes e gravam em `raw/`, no
   MinIO, do jeito que o dado chegou. **O `raw` é imutável: nunca é
   sobrescrito.**
2. **Carga na Bronze.** O Trino lê o `raw` e grava a Bronze, cópia fiel do
   dado. **Não existe etapa de staging entre as duas.**
3. **Transformação.** O dbt monta Silver, Intermediate e Gold e executa o SQL
   no Trino. Para saber o que existe, o Trino consulta o Polaris, que lê os
   metadados no PostgreSQL. As tabelas resultantes são gravadas como Iceberg
   no MinIO — a transformação acontece dentro do MinIO.
4. **Consumo.** Todo acesso passa pelo Trino, e o Ranger aplica as políticas
   e a anonimização no momento da consulta. **A Gold alimenta os indicadores
   oficiais** (dashboards, APIs, BI). **A Silver alimenta ciência de dados**
   (ML, treinamento de IA, RAG, análise exploratória). **Ninguém lê arquivo
   direto do MinIO**, porque esse caminho escaparia do Ranger.

Cinco regras decorrem daí e valem para qualquer código novo neste repositório:

1. **O Airflow extrai e grava no `raw`. Não transforma, não grava em camada.**
2. **O `raw` é imutável.** Carga nova não apaga carga anterior.
3. **O dbt roda pelo Trino**, nunca por conexão direta a um banco de camada.
4. **Todo consumo passa pelo Trino.** Nenhum componente lê o MinIO direto.
5. **Anonimização na consulta, não em repouso.** O dado fica íntegro nas
   camadas; o Ranger anonimiza conforme o perfil de quem consulta.

## Por quê

**Lakehouse, e não data warehouse tradicional.** Iceberg sobre MinIO dá
escala, versionamento (snapshots, *time travel*) e baixo acoplamento entre
armazenamento e motor. O custo é operar mais componentes — Polaris, MinIO,
Ranger — e esse custo foi aceito.

**`raw` imutável.** É o que permite auditar qualquer número e reconstruir
qualquer camada: a Bronze de uma data passada sai do `raw` daquela data. Com
sobrescrita, o passado some a cada carga.

**Catálogo único.** O Polaris centraliza leitura e escrita; o dbt roda pelo
Trino, que é o mesmo motor que serve o consumo. Não há dois caminhos para a
mesma tabela, então não há como uma consulta ver dado que outra não vê.

**Anonimização na consulta.** Mantém uma única cópia dos dados e aplica a
política no ponto por onde todo acesso passa. É por isso que todo acesso
*precisa* passar pelo Trino: uma leitura direta do MinIO seria uma leitura
sem política.

**Integração incremental.** Começa com *pull* pelas DAGs do Airflow, que já
existem, e evolui para CDC quando a fonte permitir. Nenhuma DAG precisa ser
reescrita para a arquitetura entrar em produção; o que muda é o destino da
gravação.

## O que foi descartado

**Data warehouse tradicional sobre PostgreSQL** — o desenho atual do
ambiente local. Não resolve a topologia (o Airflow não alcança o banco), não
versiona o dado e exige anonimizar em repouso.

**Data lake só de arquivos**, sem formato de tabela. Sem transação, sem
evolução de schema, sem *time travel*; cada consumidor reinventaria o
catálogo.

**Anonimização em repouso**, com uma cópia por perfil de acesso. Multiplica o
armazenamento e cria a pergunta "qual cópia é a verdadeira".

**Acesso direto ao MinIO** para ciência de dados, por ser mais rápido que
passar pelo Trino. Descartado porque contorna o Ranger.

**Etapa de staging entre `raw` e Bronze.** O `raw` já é o dado como chegou;
a Bronze é a cópia fiel em formato de tabela. Uma terceira cópia não
acrescenta informação.

## Consequências

**O ambiente local continua em PostgreSQL até a stack subir.** O `make up`
de hoje não sobe MinIO, Polaris nem Ranger. Regras que dependem desses
componentes (imutabilidade do `raw`, anonimização na consulta) ficam
declaradas e não verificáveis localmente. O que *é* verificável localmente
são as camadas e a nomenclatura, fixadas no ADR 0010.

**A ingestão por Trino já segue o desenho; a ingestão por API, não.** As
DAGs do SALIC v2 e do Mapas (ADR 0005 e 0008) mandam SQL ao Trino e não tocam
banco. As DAGs de API (TransfereGov, Bacen, IBGE, Siconfi, BB Ágil) gravam
direto no Postgres por `psycopg2`. Em produção elas precisam gravar no `raw`
do MinIO e deixar a carga da Bronze para o Trino. É a maior mudança de código
que esta decisão implica.

**O profile do dbt muda de `dbt-postgres` para `dbt-trino`.** Macros que usam
SQL específico do Postgres (`regexp_replace` com flags, `::` para cast,
`information_schema` via `run_query`) precisam ser conferidas contra o
dialeto do Trino antes da migração.

**Carga completa com `DROP` deixa de ser aceitável** como estratégia padrão,
porque sobrescreve. O ADR 0008 registra uma carga desse tipo para o Mapas; a
versão em produção precisa gravar no `raw` sem apagar o anterior.

**Fica em aberto**, e será decidido em ADR próprio: o formato dos arquivos
gravados em `raw`; a estratégia de particionamento das tabelas Iceberg; e o
caminho de CDC por fonte.
