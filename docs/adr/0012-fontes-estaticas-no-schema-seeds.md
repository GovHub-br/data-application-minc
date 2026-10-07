# ADR 0012 — Fonte estática vai de raw a Parquet no MinIO e chega ao Postgres no schema `seeds`

- Status: aceito
- Data: 2026-10-07
- Escopo: `dags/data_ingest/inep/`, `plugins/cliente_inep.py`, `helpers/datalakehouse.py`
- Origem: issue #76 (microdados do Censo Escolar do INEP)

## Contexto

Algumas fontes não são API nem banco. São arquivos publicados uma vez por
período de referência: o Censo Escolar do INEP sai como um ZIP por ano, de
1995 em diante. O que o dbt faz com uma planilha pequena e estável, o `seed`,
não serve aqui. O seed é versionado no repositório, e os ZIPs do INEP somam
gigabytes e mudam de layout ao longo da série.

Também não serve o caminho das DAGs de API ([ADR 0011](0011-transferegov-grava-no-datalakehouse.md)),
que particiona o raw pela data do run e faz upsert por chave natural. Numa
fonte estática a unidade de recarga é o período de referência (o ano do
censo), não a execução. E nem toda tabela tem chave natural: o CENSOESC não
tem.

## Decisão

Fonte estática segue três passos, todos na mesma DAG:

```text
origem ─▶ raw/<fonte>/<conjunto>/<periodo>=X/<run_id>.<ext original>
       ─▶ staging/<fonte>/<entidade>/<periodo>=X/<run_id>.parquet
       ─▶ Postgres seeds.<fonte>_<entidade>
```

1. **Raw**: o arquivo como foi publicado (o ZIP), particionado pelo período
   de referência. Cada run grava a própria key, então o raw não é sobrescrito
   ([ADR 0009](0009-data-lakehouse-extracao-airflow-camadas-minio-acesso-trino.md)).
2. **Staging**: um Parquet por entidade e período, com toda coluna em texto,
   nomes em minúsculas, vazio como nulo, mais `ano_censo` e `dt_ingestao`.
3. **`seeds`**: uma tabela por entidade, `<fonte>_<entidade>`. Recarregar um
   período é `DELETE` daquele período e `COPY` na mesma transação. Coluna nova
   num período entra como `TEXT`.

A conferência contra a origem fica numa tabela de controle por fonte
(`seeds.inep_controle_carga`), com as linhas do arquivo, do Parquet e do banco.
A task falha se as três não baterem.

**Tabela larga demais para o Postgres** (acima de 1.600 colunas, como o
CENSOESC de 2006, que tem 3.808) vai com as colunas de identificação próprias
e o resto num `jsonb`, sem as chaves nulas. O Parquet de staging continua com
todas as colunas.

## Por quê

**Schema `seeds`, separado das fontes de API.** Deixa à vista que a tabela é
uma cópia de arquivo publicado, recarregada por período, e não um espelho
incremental de sistema. O prefixo `<fonte>_` no nome da tabela faz o papel
que a pasta faz no MinIO, porque o schema é compartilhado entre fontes.

**Partição pelo período, e não pela data do run.** A pergunta operacional é
"o ano X está carregado, e de que arquivo?". Com a partição pelo período, a
resposta sai de um prefixo.

**Substituir o período, e não fazer upsert.** O INEP republica um ano inteiro
quando corrige (o `_defeso` de 2024, o `_v2` de 2025). Linha que sumiu da
republicação precisa sumir do banco, e o upsert a manteria.

**Contagem por quebra de linha, além da do parser.** É a conferência
independente. Se o parser fundir duas linhas por causa de aspas, o Parquet
e o banco concordam entre si e só a contagem do arquivo denuncia.

## O que foi descartado

**`dbt seed`.** O dado teria de ser versionado no repositório: são gigabytes,
e o layout muda ao longo da série.

**Particionar pela data do run, como no ADR 0011.** Recarregar um ano
exigiria listar todos os runs e descobrir qual trazia aquele ano.

**Quebrar o CENSOESC em várias tabelas de até 1.600 colunas.** As partes
teriam de ser juntadas pela `MASCARA` em toda consulta, e a fronteira entre
elas seria arbitrária.

## Consequências

- Com `ano_censo` texto em todas as tabelas, a bronze tipa (ADR 0010).
- O `jsonb` do CENSOESC é lido com `dados->>'vpe1001'`. O dicionário de cada
  ano está no ZIP, no raw.
- Na conversão, o CENSOESC de 2003–2006 chega a ~2,1 GB de memória: é o
  buffer por coluna do writer de Parquet. Por isso a conversão roda um ano
  por vez.
- O `download.inep.gov.br` não envia a intermediária da cadeia TLS. Ela está
  em `plugins/certificados/` e vence em 2030-11-19. Quando o INEP trocar de
  emissora, o download falha com `CERTIFICATE_VERIFY_FAILED` e o arquivo
  precisa ser trocado.
- Uma nova fonte estática segue o mesmo caminho e entra em `seeds` com o
  próprio prefixo.
