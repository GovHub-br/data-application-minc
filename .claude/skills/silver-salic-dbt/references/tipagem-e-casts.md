# Tipagem: de texto puro para tipo de verdade

Tudo na Bronze (`salic.bronze_<banco>_<tabela>`) é texto, porque a ingestão
via Trino converte cada coluna para TEXT (`plugins/trino_bronze.py`,
`cast_to_text`). As silvers migradas do `salic_dbt` já fazem casts: foram
gerados em setembro a partir do dicionário e de uma amostra. **O trabalho
agora é revisar esses casts contra o dado, não escrevê-los do zero.**

## De onde veio o tipo de cada coluna

Os casts existentes saíram por uma de duas rotas, e vale saber qual antes de
confiar:

- **Dicionário.** A maior parte das tabelas base tem o tipo original do SQL
  Server dentro da descrição da coluna na source, no formato
  `Tipo original (SQL Server): int(4).`. Extraia com
  `Tipo original \(SQL Server\):\s*([^.]+)\.`.
- **Inferência.** Nenhuma view da origem (`vw*`, `v*`) tem dicionário: o
  SchemaSpy documentou só tabelas base. O tipo foi deduzido de 200 linhas.
  É aqui que o cast mais erra.

O dicionário descreve o banco de dez/2025. Confira as colunas reais da Bronze
(`perfilar.py` lista) antes de confiar numa descrição: coluna que mudou de
nome ou sumiu quebra o modelo.

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
cur.execute(f'select {lista_colunas} from salic."bronze_{banco}_{tabela}" limit 200')
linhas = cur.fetchall()   # depois varra `linhas` por coluna, em memória
```

**Nunca** `SELECT DISTINCT coluna` — não há índice na Bronze, e isso
vira varredura completa. Numa tabela de 18,6 M linhas trava a sessão.

**Passo 2 — conte as falhas antes de confiar.** Para cada coluna que recebe
cast, meça quantos valores não passam. O `perfilar.py --completo` faz isso
para todas as colunas numa varredura só; a consulta equivalente, por coluna:

```sql
select count(*) filter (where col !~ '^-?[0-9]+$' and col is not null) as falhas,
       count(*) as total
from salic."bronze_<banco>_<tabela>"
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
-- Silver SALIC — silver_sac_tbprojetos (ex-sac__tbprojetos).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Casts: <o que foi convertido, o que ficou TEXT e por quê; quantas linhas viram NULL>.

select
    nullif(trim(id_projeto), '')::integer            as id_projeto,
    nullif(trim(nome), '')                           as nome,
    case when dt_cadastro ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}'
         then dt_cadastro::timestamp end             as dt_cadastro,
    _fatia
from {{ source('salic', 'bronze_sac_tbprojetos') }}
```

A materialização (`view`) vem do `dbt_project.yml`; não repita no arquivo.

`_fatia` é técnica: a fatia da ingestão que trouxe a linha (ADR 0005).
Preserve e documente — é o que permite auditar carga repetida.
