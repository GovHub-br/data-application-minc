# ADR 0016 — Fonte sem raw: quando a ingestão já grava a cópia fiel, a source é a Bronze

- **Data:** 2026-10-27
- **Status:** substituído pelo [ADR 0017](0017-salic-bronze-no-schema-salic.md)
- **Escopo:** regra R3 do ADR 0010 (o que a Silver pode ler) e a Bronze do SALIC
- **Substitui em parte:** [ADR 0010](0010-camadas-e-schemas-do-data-lakehouse.md),
  regra transversal "Silver lê a Bronze por `ref()`"; o resto continua valendo
- **Origem:** migração do `salic_dbt` (plano de 2026-10-27,
  `docs/superpowers/plans/2026-10-27-salic-adr-0010.md`) e a pergunta aberta da
  R3 no Mapas

## Contexto

O ADR 0010 diz que a Silver lê a Bronze por `ref()` e nunca `source()`. Para
o SALIC ele mesmo previu a exceção ("a Bronze passa a ser `source` ou cópia
fiel"), mas o `verificar.py` só aceita `ref()`. Hoje 589 modelos do
`salic_dbt/bronze/` tipam tabelas do schema `salic_bronze` lendo-as por
`source()`, e 59 silvers do Mapas leem `source()` de tabelas `bronze_*` que
a DAG grava. Nos dois casos não há modelo Bronze porque a ingestão já
grava a cópia fiel: o SALIC é um banco interno do Ministério, copiado pelo
Trino direto para `salic_bronze` ([ADR 0005](0005-ingestao-salic-por-trino-em-fatias.md)),
sem passar por raw no MinIO.

Criar 589 modelos `bronze_*` teria dois custos. Como `table`, duplica 157 M
de linhas (52,7 GB) sem nenhum uso. Como `view`, prende as tabelas de
`salic_bronze`: a ingestão v2 recria cada tabela com `DROP TABLE` pelo
Trino, que não tem `CASCADE`, e o Postgres recusa o `DROP` enquanto houver
view dependente. Isso já acontece hoje: em 2026-10-27 o schema `salic` de
produção tinha 589 views dependendo de 589 tabelas do `salic_bronze`.

## Decisão

1. Fonte copiada pela ingestão direto para o Postgres, sem raw, tem a Bronze
   na própria tabela gravada. Não se escreve modelo `bronze_*` para ela.
2. Essa Bronze é declarada como `source` com nome `bronze_<fonte>` (no SALIC,
   `bronze_sac`, `bronze_tabelas`, `bronze_agentes`, `bronze_controledeacesso`,
   `bronze_bdcorporativo`). O prefixo no nome da source é o que diz ao
   verificador que a source é Bronze.
3. A Silver dessa fonte lê a Bronze por `source('bronze_<fonte>', '<tabela>')`.
   Continua valendo o resto da R3: nada de `ref()` a outra fonte, nada de
   `source()` cujo nome não comece com `bronze_`.
4. Fonte que tem raw (TransfereGov, BB Ágil, SIDRA, Bacen, Ancine) continua
   com modelo `bronze_*` e a Silver lê por `ref()`, como no ADR 0010.

## Por quê

A Bronze é definida pelo que ela é (cópia fiel, tudo texto, sem filtro), não
por ser modelo dbt. Quando a ingestão entrega exatamente isso, o modelo
`bronze_*` só acrescentaria custo e um ponto de quebra na carga.

## O que foi descartado

- 589 `bronze_*` como `table`: duplicação sem leitor.
- 589 `bronze_*` como `view`: quebra o `DROP` da ingestão.
- Exceção por lista de pastas no `verificar.py`: lista cresce e ninguém
  lembra de atualizar; o prefixo na source é declarativo e aparece no diff.

## Consequências

- `verificar.py`: a R3 aceita `source()` cujo nome comece com `bronze_`.
- O Mapas pode zerar a R3 renomeando a source `mapas` para `bronze_mapas` num
  PR de modelos; este ADR não faz isso.
- `salic/bronze_sac_projetos` e `salic/bronze_sac_captacao` ficam
  redundantes; sair delas é PR próprio, com contagem.
- R9 (Bronze guarda histórico) continua manual e continua não cumprida pela
  ingestão com `DROP`; este ADR não muda isso.
