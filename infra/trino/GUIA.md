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

## Implantar em produção

O `docker-compose.yml` deste repositório sobe um Trino **de desenvolvimento**.
Em produção o Trino é o da infra do MinC, junto do SQL Server e do Postgres — o
Airflow do Serpro só o orquestra.

### 1. Imagem do Airflow com o provider

O `requirements.txt` ganhou `apache-airflow-providers-trino`. **Sem ele a DAG nem
importa.**

```bash
python -c "from airflow.providers.trino.hooks.trino import TrinoHook; print('ok')"
```

### 2. Catálogos no Trino de produção

Copiar os **seis** `.properties` de `infra/trino/etc/catalog/` para o
`etc/catalog/` daquele Trino. **Não copie** `config.properties`, `jvm.config` nem
`node.properties` — aquele Trino tem os seus.

### 3. Dez variáveis de ambiente, no processo do Trino

```bash
SALIC_MSSQL_HOST=  SALIC_MSSQL_PORT=1433  SALIC_MSSQL_USER=  SALIC_MSSQL_PASSWORD=
SALIC_MSSQL_URL_OPTS=encrypt=false;trustServerCertificate=true;loginTimeout=30
TRINO_DW_HOST=  TRINO_DW_PORT=5432  TRINO_DW_DB=  TRINO_DW_USER=  TRINO_DW_PASSWORD=
```

Faltando uma, aquele catálogo não carrega e o erro só aparece no log do Trino.
Reinicie o Trino e confira com `SHOW CATALOGS`.

### 4. Airflow: uma Connection e até quatro Variables

```bash
airflow connections add trino_default \
  --conn-type trino --conn-host <host> --conn-port <porta> \
  --conn-login <usuario> --conn-schema bronze \
  --conn-extra '{"catalog": "<catalogo-destino>", "protocol": "https"}'
```

É a **única** Connection. A DAG não fala com banco nenhum.

| Variable | Obrigatória? | Padrão |
|---|---|---|
| `salic_trino_data` | **sim** — as cinco fontes | — |
| `salic_trino_target_catalog` | se o catálogo não é `dw` | `dw` |
| `salic_trino_bronze_schema` | se o schema não é `bronze` | `bronze` |
| `salic_trino_control_schema` | se o schema não é `control` | `control` |

O usuário do `dw.properties` precisa de `CREATE` nos dois schemas — é ele que
cria as tabelas:

```sql
GRANT USAGE, CREATE ON SCHEMA <schema-bronze>   TO <usuario>;
GRANT USAGE, CREATE ON SCHEMA <schema-controle> TO <usuario>;
```

### 5. Primeira execução: estreite antes de soltar

A DAG nasce com `schedule=None`. Dispare à mão, nesta ordem:

| # | Parâmetros | Prova |
|---|---|---|
| 1 | `dry_run` ligado, `only_tables=BDCorporativo.sysdiagrams` | conecta e planeja; não escreve |
| 2 | `dry_run` ligado, `only_tables` vazio | retrato completo: 1139 tabelas |
| 3 | `full_refresh` ligado, `only_tables=BDCorporativo.sysdiagrams` | escrita ponta a ponta |
| 4 | `full_refresh` ligado, `only_tables=SAC.tbMovimentacao`, `rows_per_slice=250000` | fatiamento real |

Depois de cada um, `rows_loaded = rows_source` na tabela de controle é o sinal.

**`tables: []` na Variable significa as 1139 tabelas** — 1,14 TB. Não solte isso
antes dos quatro passos.

### Quando falhar

| Sintoma | Causa |
|---|---|
| DAG não aparece / erro de import | provider do Trino não instalado |
| `ImportError: cannot import name ... from 'trino_bronze'` | módulo em cache — **reinicie o dag-processor**. Mudança em `plugins/` não é reimportada sozinha |
| `Catalog 'X' not found` | defina `salic_trino_target_catalog` |
| `permission denied for schema X` | falta `CREATE` no schema; conceda o GRANT ou aponte para um schema seu |
| `SHOW CATALOGS` sem os `salic_*` | variável de ambiente ausente; ver log do Trino |
| `TCP/IP connection to the host ... has failed` | nome sem domínio que o Trino não resolve — use o FQDN |
| `Table ... does not exist` numa tabela que o `SHOW TABLES` lista | `case-insensitive-name-matching` desligado na origem |
| Carrega sem fatiar | passthrough `sys.*` indisponível; a DAG avisa e segue |

Retomada é barata: a DAG pula o que já concluiu com `success` no dia. Para
refazer tudo, `full_refresh`.

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
