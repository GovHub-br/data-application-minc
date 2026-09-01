# Tipagem: de texto puro para tipo de verdade

Tudo em `salic_bronze` é `character varying`, porque a ingestão via Trino
converte cada coluna para TEXT (`plugins/trino_bronze.py`, `cast_to_text`).
Descobrir o tipo certo é o trabalho central da bronze.

## Há duas rotas, e você precisa saber em qual está

Medição de 2026-09-01, sobre as 571 tabelas do escopo:

| rota | tabelas | de onde vem o tipo |
|---|---|---|
| dicionário | **279** | descrição das colunas em `dbt/minc/models/salic_bronze/sources_*.yml` |
| inferência | **292** | só olhando o dado |

**Nenhuma das 132 views tem dicionário** — o SchemaSpy documentou só tabelas
base. Toda view cai na rota de inferência, sem exceção.

## Rota 1 — o dicionário já está no repositório

**4.677 das 5.064 colunas declaradas (92,4%)** trazem o tipo original do SQL
Server dentro da própria descrição, no formato:

```
[não documentado no dicionário de dados original do SALIC] Tipo original
(SQL Server): int(4).
```

Extrair com `Tipo original \(SQL Server\):\s*([^.]+)\.` dá o mapa de graça.
São 106 tipos distintos que colapsam em 6 grupos.

⚠ **O dicionário descreve o schema `bronze` (legado), não o `salic_bronze`.**
Só 329 das 561 tabelas declaradas existem no `salic_bronze`. Sempre confira as
colunas reais no `information_schema` antes de aplicar o mapa — coluna que
mudou de nome ou sumiu vai gerar modelo quebrado.

## O mapa de casts

| SQL Server | Postgres | cuidado |
|---|---|---|
| `int`, `int identity`, `tinyint`, `smallint` | `integer` | `bigint` se estourar |
| `bigint` | `bigint` | |
| `datetime`, `smalldatetime`, `date` | `timestamp` / `date` | formato precisa ser conferido |
| `money`, `decimal`, `numeric`, `float`, `real` | `numeric` | separador decimal precisa ser conferido |
| `bit(1)` | `boolean` | pode vir `'0'/'1'` ou `'True'/'False'` |
| `char(n)`, `varchar(n)`, `text` | `text` | sem cast; considere `NULLIF(TRIM(x),'')` |
| `varbinary`, `binary`, `image` | `text` | mantenha como texto, não tente decodificar |

## ⚠ Valide o cast contra o dado ANTES de escrever o modelo

Esta é a regra que salva o projeto. O tipo declarado diz a **intenção** da
origem; o texto gravado diz a **realidade**. Os dois divergem, e um cast que
falha em 0,1% das linhas derruba o modelo inteiro em execução.

**Passo 1 — amostre sem torturar o banco.** Uma leitura de 200 linhas por
tabela, extraindo amostra de todas as colunas da mesma leitura:

```python
cur.execute(f'select {lista_colunas} from salic_bronze."{tabela}" limit 200')
linhas = cur.fetchall()   # depois varra `linhas` por coluna, em memória
```

**Nunca** `SELECT DISTINCT coluna` — não há índice em `salic_bronze`, e isso
vira varredura completa. Numa tabela de 18,6 M linhas trava a sessão.

**Passo 2 — conte as falhas antes de confiar.** Para cada coluna que vai
receber cast, meça quantos valores não passam:

```sql
select count(*) filter (where col !~ '^-?[0-9]+$' and col is not null) as falhas,
       count(*) as total
from salic_bronze."tabela"
```

Regex por grupo:

| grupo | regex de validação |
|---|---|
| inteiro | `^-?[0-9]+$` |
| numérico | `^-?[0-9]+([.,][0-9]+)?$` |
| data ISO | `^[0-9]{4}-[0-9]{2}-[0-9]{2}` |
| booleano | valor em `('0','1','true','false','t','f')` |

**Passo 3 — decida o que fazer com a falha.** Três saídas legítimas, e a
escolha vai na documentação:

- **Zero falha** → cast direto.
- **Falha rara** → `CASE WHEN col ~ '<regex>' THEN col::tipo END`, que vira
  NULL no lixo. **Documente quantas linhas viram NULL** — quem consome precisa
  saber.
- **Falha alta** → não converta. Deixe TEXT e registre na descrição que o tipo
  declarado não corresponde ao conteúdo. É informação valiosa, não fracasso.

Existem macros no projeto para casos recorrentes:
`parse_valor`, `parse_financial_value`, `normaliza_documento`, `sem_acento`.
Prefira reusar a inventar.

## Padrão do modelo

```sql
{{ config(materialized='view') }}

-- Bronze — <o que a tabela é, em uma linha>.
-- Origem: salic_bronze.<tabela> (raw, tudo TEXT).
-- Casts: <o que foi convertido e o que ficou TEXT, com o porquê>.

select
    nullif(trim(id_projeto), '')::integer            as id_projeto,
    nullif(trim(nome), '')                           as nome,
    case when dt_cadastro ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}'
         then dt_cadastro::timestamp end             as dt_cadastro,
    _fatia
from {{ source('bronze_sac', 'sac__tbprojetos') }}
```

`_fatia` é técnica: a fatia da ingestão que trouxe a linha (ADR 0005).
Preserve e documente — é o que permite auditar carga repetida.
