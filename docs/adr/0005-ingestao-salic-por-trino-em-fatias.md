# ADR 0005 — A ingestão do SALIC passa a ser feita pelo Trino, em fatias de chave

- **Data:** 2026-08-24
- **Status:** aceito

## Contexto

A `salic_ingestion` copia cinco bancos SQL Server do SALIC — 561 tabelas, na
ordem de terabytes — para a camada bronze do Postgres. Ela abre um cursor com
`SELECT * FROM tabela`, itera com `fetchmany`, converte cada valor com `str()` e
insere com `execute_values`. Cada linha vira objeto Python três vezes.

Com tabela grande isso é lento o bastante para que o cursor fique horas aberto, e
é aí que a coisa quebra: a VPN cai, ou o `remote query timeout` do servidor (20
minutos) encerra a sessão. O `except` da v1 já reconhece esse modo de falha e
sugere pedir ao DBA para desligar o timeout no servidor — um pedido que não
depende de nós e que, mesmo atendido, não torna a ingestão mais rápida.

## Decisão

Uma segunda DAG, `salic_ingestion_trino`, na qual **o dado não passa por
Python**. O Trino lê do SQL Server e escreve no Postgres; o Airflow só emite SQL
e registra o resultado.

Trocar o motor, sozinho, não resolveria o problema. Um `CREATE TABLE AS SELECT *`
no Trino tem exatamente a mesma exposição: o conector JDBC lê a tabela por **uma
única conexão**, e a query fica aberta do começo ao fim. O que resolve é a
segunda metade da decisão — **fatiar por faixa de chave**:

1. Descobrir a chave inteira de cada tabela (PK de coluna única, ou coluna
   identity) e a contagem aproximada de linhas, lendo `sys.indexes` e
   `sys.partitions` pelo passthrough do conector — três queries por banco, sem
   varrer tabela nenhuma.
2. Dividir o domínio da chave em faixas de ~5 milhões de linhas.
3. Cada faixa vira um `INSERT ... SELECT ... WHERE k >= a AND k < b`, cujo
   predicado o Trino empurra para o SQL Server.
4. Faixas de uma tabela em paralelo entre si; tabelas em paralelo entre si.
5. Faixa que falha é repetida sozinha.

Tabela sem chave inteira, ou menor que o tamanho da fatia, é carregada de uma
vez só.

A v1 **fica no repositório e continua funcional**. A v2 nasce com
`schedule=None`, para ser exercitada à mão até a paridade estar demonstrada.

## Por quê

Três coisas, em ordem de importância:

**Cada query passa a ser curta.** É o que ataca o sintoma real. Uma fatia de 5
milhões de linhas leva minutos; nenhuma chega perto dos 20 minutos do timeout do
servidor. A carga deixa de depender de uma conexão sobreviver por horas.

**A repetição fica barata.** Na v1, uma queda no fim de uma tabela de 200 GB
custa recopiar os 200 GB. Na v2 custa uma fatia.

**O paralelismo NÃO compra vazão — isso foi medido e a expectativa inicial
estava errada.** Contra o SALIC em 2026-08-24: um fluxo único leu blob a
7,2 MB/s, três fluxos em paralelo somaram 7,3 MB/s, e a carga de 16,9 M linhas
da `tbSaldoBancario` com quatro fatias simultâneas deu 8,5 MB/s. O link da VPN
satura em torno de 8 MB/s e abrir mais conexões não muda isso.

O paralelismo fica porque distribui as fatias e mantém o pipeline cheio, não
porque acelera. O produto dos dois números
(`SALIC_TRINO_MAX_PARALLEL_TABLES` × `slice_concurrency`, padrão 4 × 4 = 16
conexões) segue documentado nos dois lugares — mas o motivo agora é o inverso do
que se pensava: subir esses valores não traz ganho e só multiplica sessões
abertas contra um banco de produção.

## O que foi descartado

**`catalog.management=dynamic`.** Criar os catálogos por `CREATE CATALOG` a
partir da Variable do Airflow seria mais elegante do que manter cinco arquivos
`.properties` — a lista de bancos já vive na Variable. Descartado porque o
recurso registra a senha do catálogo no log e na Web UI, o que contraria a regra
de segredos do repositório. Os catálogos ficam estáticos, com as credenciais
vindo do ambiente por referência `ENV`.

**Qualquer acesso direto do Airflow aos bancos.** A primeira versão fazia o DDL
da bronze e o log de controle por `psycopg2`, para que as colunas nascessem
`TEXT` como na v1. Isso quebra em produção: o Airflow roda na infra do Serpro e
os bancos ficam na infra do MinC, em redes separadas — **o Airflow só tem rota
até o Trino**. Tudo passou a sair pelo Trino, com duas consequências assumidas:
as colunas nascem `varchar` (mesmo armazenamento no PostgreSQL, e o `data_type`
das fontes do dbt é documentação, não contrato verificado), e a Connection
`postgres_default` deixou de ser usada por esta DAG.

## Consequências

O conteúdo de três tipos muda em relação à v1, e os modelos silver que
dependerem deles precisam ser conferidos: `bit` continua gravando `'True'`
(escrito à mão, porque o `CAST` do Trino daria `'true'`), `char(n)` perde o
preenchimento de espaços à direita, e `varbinary` vira hexadecimal em vez da
repr de `bytes` do Python. Nome de tabela, nome de coluna e tipo `TEXT` são
idênticos.

**O Airflow nunca fala com banco: só com o Trino.** Não é preferência, é a
topologia — Airflow no Serpro, SQL Server/Trino/Postgres no MinC. Nenhum byte do
volume atravessa a fronteira: a DAG manda SQL e recebe contagem de linhas. Um
`psycopg2` reintroduzido aqui passa no ambiente local, onde tudo divide a mesma
máquina, e quebra em produção.

Daí nasce a coluna técnica `_fatia`, gravada em toda tabela bronze: repetir uma
faixa exige apagar antes o que ela escreveu, e recortar pela chave da tabela
exigiria `CAST("k" AS bigint)` — que o conector não empurra, e o Trino recusa o
DELETE com *"can not perform merge on the target table without primary keys"*.
Comparar um inteiro simples empurra limpo.

**As conexões das fatias paralelas são abertas na thread principal e
entregues prontas às threads.** No Airflow 3 a resolução de uma Connection passa
pelo canal de comunicação da task, que só existe na thread principal: chamar
`TrinoHook(...)` dentro de uma thread falha com `The conn_id 'trino_default'
isn't defined`. Como cada fatia tem repetição própria, a falha aparecia como
aviso e a tabela vinha pela metade **com a task terminando em `success`** — foi
assim que apareceu, contra o SALIC real. Vale para qualquer hook do Airflow
usado fora da thread principal, não só o do Trino.

Duas outras coisas só apareceram contra o banco de verdade e estão registradas
onde quem for mexer vai olhar: `case-insensitive-name-matching` precisa estar
ligado nos catálogos de origem (ver `infra/trino/GUIA.md`), e o cruzamento entre
o `information_schema` do Trino e o T-SQL da origem precisa normalizar a caixa
do nome da tabela (ver `trino_bronze.metadata_key`). As duas falhavam em
silêncio: a segunda desligava o fatiamento inteiro sem emitir um único erro.

A montagem do SQL — fatias, predicados, conversão de tipo — mora em
`plugins/trino_bronze.py`, separada da DAG, e é testada sem Airflow, sem Trino e
sem VPN em `tests/test_trino_bronze.py`. A separação existe porque errar o
predicado de uma faixa não quebra nada: só carrega a tabela pela metade, e o
erro aparece semanas depois como número errado num painel.

Passa a haver mais uma peça de infraestrutura para manter de pé. Ela fica atrás
do profile `trino` do compose, fora do `make up`, para não onerar quem não
ingere SALIC.
