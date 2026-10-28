# ADR 0017 — A Bronze do SALIC é gravada pela ingestão no schema `salic`, como `bronze_<banco>_<tabela>`

- **Data:** 2026-10-27
- **Status:** proposto
- **Escopo:** Bronze e Silver do SALIC; regra R3 do ADR 0010 para fonte sem raw
- **Substitui:** [ADR 0016](0016-salic-sem-raw-a-source-e-a-bronze.md), inteiro
- **Substitui em parte:** [ADR 0010](0010-camadas-e-schemas-do-data-lakehouse.md),
  regra transversal "Silver lê a Bronze por `ref()`"; o resto continua valendo
- **Origem:** revisão do plano de migração do `salic_dbt`
  (`docs/superpowers/plans/2026-10-27-salic-adr-0010.md`, versão 2), no mesmo
  dia do 0016

## Contexto

O ADR 0010 desenha o SALIC como
`salic.bronze_agentes_agentes → salic.silver_agentes_agentes`: um schema por
fonte, camada no nome do arquivo e da relação. A ingestão v2
([ADR 0005](0005-ingestao-salic-por-trino-em-fatias.md)) grava a cópia fiel
num schema à parte, `salic_bronze`, como `<banco>__<tabela>`, e o ADR 0016
tinha aceitado isso como "a Bronze". Não é o que o 0010 pede, e deixava dois
schemas para uma fonte só. O Mapas já faz do jeito do 0010: a DAG grava
`mapas.bronze_<tabela>` e a silver lê `source('mapas', 'bronze_<tabela>')`.

Há ainda um problema operacional. A DAG recria cada tabela com `DROP TABLE`
pelo Trino, que não tem `CASCADE`, e o Postgres recusa o `DROP` quando há view
dependente. Em 2026-10-27 havia 589 views do dbt sobre as 589 tabelas de
`salic_bronze`: a carga estava bloqueada para todas.

## Decisão

1. A Bronze do SALIC é a tabela gravada pela `salic_ingestion_trino` no schema
   `salic`, com nome `bronze_<banco>_<tabela>` (`bronze_sac_projetos`). Não há
   raw nem modelo dbt `bronze_*` para o SALIC.
2. No dbt, a source chama-se `salic` (schema `salic`) e declara essas tabelas.
   A Silver é `salic/silver_<banco>_<tabela>.sql` e lê
   `source('salic', 'bronze_<banco>_<tabela>')`.
3. Regra geral, que também vale para o Mapas: fonte sem raw tem a Bronze
   gravada pela ingestão no schema da fonte, com prefixo `bronze_`; a Silver a
   lê por `source('<fonte>', 'bronze_<entidade>')`. A R3 aceita `source()`
   quando o nome da source é o da pasta e a tabela começa com `bronze_`. Fonte
   com raw continua com modelo `bronze_*` e `ref()`, como no 0010.
4. A ingestão não derruba tabela cuja forma não mudou: `TRUNCATE` + `INSERT`.
   `DROP` + `CREATE` só quando a lista de colunas da origem mudou, e aí a
   falha por view dependente é o aviso de que a silver precisa ser recriada.
5. O schema `salic_bronze` é encerrado: as tabelas mudam de schema e de nome
   por `ALTER TABLE`, sem recarga.

## Por quê

Um schema por fonte é o que o 0010 decidiu e o que Ranger, OpenMetadata e o
site esperam. Bronze física gravada pela DAG, sem cópia dbt, é o único desenho
que não duplica 58 GB nem prende a carga. `TRUNCATE` em vez de `DROP` é o que
permite silver como view sobre a bronze sem quebrar a ingestão.

## O que foi descartado

- Manter `salic_bronze` como source "que é Bronze" (ADR 0016): dois schemas
  para uma fonte, fora do desenho do 0010.
- Modelo dbt `bronze_*` lendo `salic_bronze` (padrão do TransfereGov): como
  view prende o `DROP`; como table duplica 58 GB a cada `dbt run`.
- Recarga completa para mudar o nome: ~2 h pela VPN; o rename é instantâneo e
  as views existentes seguem a tabela.

## Consequências

- `verificar.py`: a R3 aceita `source(<pasta>, bronze_*)`. Os 59 modelos do
  Mapas que liam `source('mapas', 'bronze_*')` deixam de violar.
- `salic/bronze_sac_projetos` e `salic/bronze_sac_captacao` saem antes de a
  ingestão gravar com esses nomes: com `target: prod`, modelo e tabela da
  ingestão resolveriam para a mesma relação e o `dbt run` derrubaria a carga.
- A Variable `salic_trino_bronze_schema` passa a `salic` no ambiente.
- R9 (Bronze guarda histórico) continua não cumprida: `TRUNCATE` apaga como o
  `DROP` apagava. Fica para decisão própria.
