# Trino

Motor de consulta federada usado pela ingestão SALIC v2
([`dags/data_ingest/salic/salic_ingestion_trino.py`](../../dags/data_ingest/salic/salic_ingestion_trino.py)).
Ele lê do SQL Server e escreve no Postgres sem que o dado passe por Python.

## Onde ficam as conexões dos bancos

Em `etc/catalog/`. **Um arquivo por banco** — é a regra do conector: a
`connection-url` do SQL Server carrega o `databaseName`, então um catálogo
enxerga um banco só. Não existe "um catálogo para o servidor inteiro".

| Arquivo | Aponta para | Papel |
|---|---|---|
| `salic_sac.properties` | SQL Server, banco `SAC` | origem (431 tabelas) |
| `salic_agentes.properties` | SQL Server, banco `Agentes` | origem |
| `salic_tabelas.properties` | SQL Server, banco `Tabelas` | origem |
| `salic_controledeacesso.properties` | SQL Server, banco `ControleDeAcesso` | origem |
| `salic_bdcorporativo.properties` | SQL Server, banco `BDCorporativo` | origem |
| `dw.properties` | PostgreSQL do data warehouse | destino |

O nome do arquivo **é** o nome do catálogo no SQL: `salic_sac.properties` vira
`SELECT ... FROM salic_sac.dbo.Projetos`. A DAG monta esses nomes a partir do
campo `catalog` da Variable `salic_trino_data`; omitido, ele assume
`salic_<database em minúsculas>`.

Se algum banco estiver em outro servidor, edite só o `.properties` dele — troque
`SALIC_MSSQL_HOST` por uma variável própria e declare-a no `docker-compose.yml`.

## Onde ficam as credenciais

**Em lugar nenhum destes arquivos.** Eles referenciam variáveis de ambiente, que
o Trino resolve na leitura. Os valores reais vivem no `.env` da raiz ou de
`infra/`, ambos no `.gitignore`. O template com os nomes está em
[`local.env`](../../local.env).

O modo `catalog.management=dynamic` (criar catálogo por `CREATE CATALOG` no SQL,
o que casaria bem com a Variable do Airflow) foi descartado justamente por isso:
ele registra a senha no log e na Web UI.

## Subir

```bash
make up-trino
```

Web UI em <http://localhost:8090>. Console SQL:

```bash
make trino-cli
```

Conferir que os catálogos carregaram, e que a origem responde:

```sql
SHOW CATALOGS;
SHOW SCHEMAS FROM salic_sac;
SELECT count(*) FROM salic_sac.information_schema.tables;
```

Se um catálogo não aparecer no `SHOW CATALOGS`, o erro está no
`make logs-trino` — quase sempre nome de propriedade errado ou variável de
ambiente não definida.

## O Airflow não fala com banco nenhum

Em produção o Airflow roda na infra do Serpro e o SQL Server, o Trino e o
Postgres ficam na infra do MinC — redes separadas. **A única Connection que a
DAG usa é `trino_default`.** Schemas, DDL da bronze, log de controle e o DELETE
de repetição saem todos pelo catálogo `dw`.

Nenhum byte do volume atravessa a fronteira entre as duas redes: o Airflow manda
SQL e recebe contagem de linhas. Um `psycopg2` reintroduzido na DAG passa no
ambiente local, onde tudo divide a mesma máquina, e quebra em produção.

## Dimensionamento

O heap da JVM é **80% da memória do container** (`jvm.config`), então quem
controla o tamanho do Trino é o `TRINO_MEM_LIMIT` do compose, não o `jvm.config`.
Uma cópia JDBC→JDBC é streaming, não agregação: heap grande ajuda pouco. O que
adianta memória é sustentar muitas fatias simultâneas.

O paralelismo real é o produto de dois números:

- `SALIC_TRINO_MAX_PARALLEL_TABLES` (ambiente do Airflow, padrão 4) — tabelas
  ao mesmo tempo;
- `slice_concurrency` (Variable `salic_trino_data`, padrão 4) — faixas da mesma
  tabela ao mesmo tempo.

Padrão: **16 conexões simultâneas ao SQL Server**. Subir os dois sem medir a VPN
recria o problema que a v2 existe para resolver.

## Por que estas propriedades

As quatro que não são óbvias, e o que quebra se alguém as remover:

- **`unsupported-type-handling=CONVERT_TO_VARCHAR`** (origem). No padrão
  (`IGNORE`) o Trino **descarta silenciosamente** as colunas cujo tipo ele não
  sabe mapear. Sem erro, sem aviso: a tabela chega na bronze com menos colunas
  do que as fontes do dbt declaram.
- **`insert.non-transactional-insert.enabled=true`** (destino). No padrão, todo
  INSERT escreve numa tabela temporária e depois renomeia — dobra a escrita e
  o disco, e inviabiliza o carregamento em fatias. A atomicidade passa a ser
  responsabilidade da DAG, que a assume: DROP+CREATE antes da carga, DELETE da
  faixa antes de repetir.
- **`case-insensitive-name-matching=true`** (origem). O SALIC nomeia em
  CamelCase (`Projetos`, `IdPRONAC`). Em `false` — o padrão — o Trino lista a
  tabela como `projetos` mas procura o nome remoto exatamente assim, e não acha
  a `Projetos` real: o `information_schema` mostra as 561 tabelas e **nenhuma
  delas pode ser lida**. Isso foi reproduzido num Postgres de teste antes de
  existir VPN; o erro é `Table ... does not exist` numa tabela que o
  `SHOW TABLES` acabou de listar.
- **`metadata.cache-ttl=0s`** (destino). A DAG cria a tabela bronze e insere
  nela no instante seguinte, tudo pelo Trino. Com cache, o INSERT não enxergaria
  a tabela recém-criada. Nos catálogos
  de origem o cache é longo (30min) de propósito — lá o schema não muda durante
  a carga e são 561 tabelas de metadado para ler.
