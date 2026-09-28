# Checkpoint — silvers do SALIC

Branch `feat/dbt-salic-silver`. Escrito em 2026-09-04, nada commitado.

## Feito nesta sessão

**`dim_municipio_ibge` — pronto.** Dimensão territorial com dez colunas: os
dois formatos de código IBGE, nome, UF, região, mesorregião, microrregião e
população. Documentado em `core/schema.yml`.

**Três modelos bronze novos**, em `bronze/agentes/`:
`agentes__municipios`, `agentes__uf`, `agentes__populacaomunicipio`.

**Um modelo bronze novo** em `bronze/sac/`: `sac__tbapicomprovacoes`.

**Quatro sources declaradas** — as três de agentes em `sources_agentes.yml` e
`sac__tbapicomprovacoes` em `sources_sac.yml`. Todas apontam para
`salic_bronze`, nenhuma para o legado.

**`docs/silvers-pendentes.md`** — especificação dos seis modelos pendentes, com
o bloqueio de cada um atualizado.

## Decisões tomadas hoje

| decisão | efeito |
|---|---|
| categoria de cor/raça vem por enriquecimento da base | `dim_agente_perfil_rouanet_scd` deixa de depender de achar tabela de domínio |
| classificação de território vem por enriquecimento da base | idem para `brg_territorio_classificacao` |
| marcação de capital é assunto do gold | `dim_municipio_ibge` fica completo sem ela |
| primeiro acesso = primeiro **pagamento** | `fct_proponente_ano_rouanet` fica definido |
| anonimização é do Apache Ranger, não do pipeline | o HMAC deixa de ser bloqueio |
| `dim_meta_alvo_rouanet` é gold, não silver | sai da lista das silvers |

## O que eu ia fazer em seguida

**`fct_proponente_ano_rouanet` — escrito.** Fica em
`models/salic_dbt/meta5/`, com grão de **um proponente por ano com pagamento**
e a coluna `eh_primeiro_ano_pagamento`. Assim, uma única silver serve tanto à
série anual quanto ao indicador de primeiro acesso.

Fonte: `sac__tbapicomprovacoes` para os pagamentos, cruzado com
`brg_projeto_proponente_rouanet` pelo PRONAC para chegar ao proponente.

Governança já definida pela cadeia: a ponte de proponente é `restrito` e
`rag_publication: prohibited`, e o teste de governança proíbe modelo publicável
ler modelo restrito. Então o fato nasce restrito também.

## Estado dos seis

```
dim_municipio_ibge               escrito
fct_proponente_ano_rouanet       escrito
dim_agente_perfil_rouanet_scd    espera enriquecimento da base
brg_territorio_classificacao     espera enriquecimento da base
fct_execucao_municipal_rouanet   espera a regra de multicidade
dim_meta_alvo_rouanet            movido para o gold
```

## Achados que sustentam o acima

**A ponte da Meta 4 está medida.** `Projetos` resolve 155.574 pares
`idProjeto`↔`idPronac`, sem ambiguidade, contra os 4.269 da ponte atual. O
campo `idProjeto` foi introduzido em 2009: zero cobertura antes disso, 97% ou
mais a partir de 2013, 100% de 2021 em diante.

**A autodeclaração existe.** `tbAgenteFisico` tem `stCorRaca` com 29.086
respostas utilizáveis em códigos de 1 a 5, e `stNecessidadeEspecial` com 100%
de preenchimento. Corrige o Bloqueio 1 do `HANDOFF.md`, que afirma o contrário.

**Duas armadilhas de junção.** O código IBGE do SALIC tem 6 posições e o do
`transferegov` tem 7 — cruzar sem tratar não casa nada e não dá erro. E o
padrão `raca` casa dentro de `procuração`, `operação` e `alteração`, o que já
produziu falso positivo três vezes nesta sessão.

## Pendências que não são de modelo

- 18 tabelas do `salic_bronze` sem `SELECT` para a role de analytics
- 588 tabelas não podem ser recarregadas: as views do schema `salic` bloqueiam
  o `DROP` da ingestão, e isso derrubou 7 tabelas do SAC em 04/09
- `HANDOFF.md` afirma coisas hoje falsas e será apagado
