---
name: bronze-salic-dbt
description: >-
  Cria os modelos dbt da camada bronze do SALIC a partir do schema raw
  `salic_bronze` no Postgres `dbanalytics`, tipando as colunas e documentando
  cada tabela e cada coluna no padrão do dbt docs/OpenMetadata deste
  repositório. Use quando o usuário pedir para "fazer os modelos bronze do
  SALIC", "modelar o salic_bronze", "documentar as tabelas do SALIC",
  "continuar a bronze", "criar a camada bronze", ou citar o schema
  `salic_bronze`. Também quando pedir para retomar esse trabalho depois de
  queda de VPN ou de limpeza de contexto.
allowed-tools: Bash, Read, Grep, Glob, Edit, Write
---

# Camada bronze do SALIC

Transformar as ~571 tabelas úteis do schema raw `salic_bronze` em modelos dbt
tipados e **documentados**, para a camada semântica sair no `dbt docs` e no
OpenMetadata.

**A documentação é o produto, não um subproduto.** O cast de tipo é a parte
fácil e mecânica. O que ninguém consegue recuperar depois é o significado de
negócio de cada tabela e de cada coluna, e você só descobre isso **enquanto
está com o banco aberto na frente**. Aproveite o acesso.

## Antes de tudo: dois fatos que mudam o trabalho

**1. `salic_bronze` é RAW, apesar do nome.** Tudo é `character varying`, sem
constraint, sem índice, com uma coluna técnica `_fatia` em 100% das tabelas.
A bronze de verdade é o que você vai criar no dbt: tipagem, padronização e
descarte do que não se usa.

**2. Existem dois schemas SALIC, e o dbt aponta para o errado.**

| schema | tabelas | linhas | `_fatia` | o que é |
|---|---|---|---|---|
| `bronze` | 465 | 86,5 M | 1 de 465 | saída da ingestão v1, **legada** |
| `salic_bronze` | 656 | 157,7 M | **656 de 656** | ingestão v2 via Trino, **oficial** |

As 561 sources em `dbt/minc/models/salic_bronze/*.yml` declaram
`schema: bronze` — o legado. A causa está em
`dags/data_ingest/salic/salic_ingestion_trino.py`: o destino vem da Variable
`salic_trino_bronze_schema`, cujo default é `bronze`, sobrescrita no ambiente
para `salic_bronze`. **Confirme com o usuário antes de mexer nesses YAML** —
realinhar as sources é decisão dele, não sua.

## Fluxo

1. **Suba a VPN e conecte.** → `references/conexao-e-vpn.md`
2. **Levante o inventário.** `scripts/inventario.py` mede o que existe hoje.
3. **Aplique o corte de escopo.** Fora: tabelas vazias e lixo do Trino.
4. **Escolha um lote pequeno** (5 a 10 tabelas). Nunca o schema inteiro.
5. **Olhe o dado antes de escrever qualquer cast.** → `references/tipagem-e-casts.md`
6. **Escreva o modelo + a documentação juntos.** → `references/template-documentacao.md`
7. **Valide**: `dbt parse`, testes, manifest, coleta do site.
8. **Commite o lote** e repita a partir do passo 4.

## Regras de escopo

**Fora:** tabela vazia e lixo do Trino (`tmp_trino_*`). Na medição de
2026-09-01: 656 tabelas, **85 vazias**, sobrando **571** com 157.673.817
linhas. As 2 `tmp_trino_*` já caem como vazias.

**Dentro:** todo o resto, **inclusive as 132 views (`vw*`)** — decisão do
usuário, elas importam.

⚠ **Nunca use `reltuples` para decidir se a tabela está vazia.** Nenhum dos
schemas passou por `ANALYZE`, então o planejador reporta 0 para tabela nunca
analisada. Isso me fez contar 192 vazias quando eram 85 — **107 tabelas com
dado seriam descartadas.** Use `SELECT EXISTS(SELECT 1 FROM t LIMIT 1)`, que
é O(1) e definitivo. O `scripts/inventario.py` já faz certo.

## São muitas tabelas, e chegam mais

571 hoje, e o usuário avisou que **novas tabelas aparecem nos próximos dias**.
Duas consequências:

- **Trabalhe em lotes e commite cada lote.** Não tente as 571 numa tacada: a
  VPN cai, o contexto estoura, e você perde tudo o que não commitou.
- **Rode o inventário de novo a cada retomada.** O número muda. Nunca confie
  nos totais escritos aqui como se fossem o presente.

Para saber o que já foi feito, compare os modelos existentes em
`dbt/minc/models/salic_bronze/` com o inventário. É a fonte da verdade do
progresso — não uma lista sua em memória.

## A VPN cai

Cai mesmo, e no meio do trabalho. Sintoma: `TimeoutError` ou
`OperationalError: timeout expired`.

- Cheque com o teste de porta de `references/conexao-e-vpn.md`.
- **Peça ao usuário para subir a VPN**; você não tem como fazer isso.
- Enquanto estiver fora, **siga trabalhando no que não depende do banco**:
  escrever YAML do que já foi perfilado, revisar descrição, rodar `dbt parse`.
- Salve o perfilamento em disco (`/tmp/*.json`) **assim que obtiver**, para a
  queda não custar o trabalho de novo.

## Cuidado com a carga no banco

É banco de produção do MinC. Duas regras:

- **Sessão read-only e `statement_timeout`.** Sempre.
- **Nunca `SELECT DISTINCT` numa coluna sem índice.** Não há um único índice em
  `salic_bronze`; `DISTINCT` vira varredura completa. Numa tabela de 18,6 M
  linhas isso trava a sessão e pesa no servidor — já aconteceu, e foi preciso
  matar o processo. Para amostrar valores, faça **uma leitura de 200 linhas
  por tabela** e extraia as amostras de todas as colunas dessa mesma leitura.

## Dado sensível

O SALIC tem CPF, CNPJ e dados pessoais. Ao perfilar, **descreva o formato, não
despeje o valor**. Nada de listar CPFs numa descrição ou num log. Credencial
nunca entra em arquivo versionado — o `.env` está no `.gitignore` e continua lá.

## Convenções de modelo

- **Materialize como `view`.** O objetivo é a camada semântica no `dbt docs`, e
  view entrega 100% disso com zero armazenamento. Os 52,7 GB do raw não devem
  ser duplicados antes de alguém saber quais modelos importam. Promova a
  `table`/`incremental` só as pesadas depois — **90% das linhas estão em 80
  tabelas**, e 208 têm menos de 1.000 linhas.
- **Preserve o nome de origem** (`sac__tbprojetos`). São centenas de modelos:
  renomear destrói a rastreabilidade e convida a colisão.
- **Preserve `_fatia`**, documentando-a como coluna técnica de controle de carga
  da ingestão por fatias (ver ADR 0005).
- **Um arquivo `schema.yml` por origem** (`sac`, `tabelas`, `agentes`,
  `controledeacesso`, `bdcorporativo`), espelhando a divisão que já existe em
  `dbt/minc/models/salic_bronze/sources_*.yml`.

## Depois de cada lote

```bash
make dbt-manifest    # manifest é versionado; o Cosmos lê ele, não `dbt ls`
make docs-collect    # o site descreve o estado anterior até isto rodar
python3 -m pytest tests/ -q
```

⚠ **Se você declarar um schema novo em source**, acrescente-o ao
`schemaFilterPattern` das TRÊS recipes em `helpers/openmetadata/recipes/`
(`postgres_metadata`, `postgres_profiler`, `postgres_classifier`). Sem isso as
tabelas ficam invisíveis no OpenMetadata **sem nenhum aviso**. O
`tests/test_openmetadata_packaging.py` pega isso — e já pegou.

⚠ **Termo de glossário novo** vai em `helpers/openmetadata/glossaries/minc.csv`,
mas `sync_glossary` **não é chamado por nenhuma task**. Referência a termo não
sincronizado fica pendurada. Ver `helpers/openmetadata/GUIA.md`.

## Referências

| Arquivo | Quando ler |
|---|---|
| `references/conexao-e-vpn.md` | Toda vez que for conectar |
| `references/tipagem-e-casts.md` | Antes de escrever o primeiro cast do lote |
| `references/template-documentacao.md` | Ao escrever o `schema.yml` — é o padrão obrigatório |
| `scripts/inventario.py` | No começo de toda sessão |
