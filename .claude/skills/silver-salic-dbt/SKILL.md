---
name: silver-salic-dbt
description: >-
  Migra e documenta as silvers do SALIC (`dbt/minc/models/salic/silver_<banco>_<tabela>.sql`)
  sobre a Bronze que a ingestão grava em `salic.bronze_<banco>_<tabela>`: migração
  mecânica por lote, perfil do dado sem expor valor, análise semântica das tabelas
  e colunas, revisão de tipos, testes e dado pessoal, e validação por contagem.
  Use quando o usuário pedir para "migrar o lote <banco> do SALIC", "documentar
  as silvers do SALIC", "fazer a análise semântica", "descrever as colunas do
  SALIC", "continuar a migração do salic_dbt", ou citar `salic_dbt`,
  `salic.bronze_*` ou `silver_<banco>_*`. Também ao retomar esse trabalho depois
  de queda de VPN ou de limpeza de contexto.
allowed-tools: Bash, Read, Grep, Glob, Edit, Write
---

# Silvers do SALIC

Levar os ~590 modelos legados de `salic_dbt/bronze/` para `salic/silver_*`,
**documentados de verdade**, para a camada semântica sair no `dbt docs` e no
OpenMetadata.

**A documentação é o produto.** A migração de pasta e nome é mecânica e já
tem script. O que ninguém recupera depois é o significado de cada tabela e de
cada coluna, e isso só se descobre olhando o dado. Hoje a maior parte das
colunas está com `[NÃO VERIFICADO] Coluna ausente do dicionário de dados
original`, e os testes vieram de um perfil automático que ninguém conferiu.

## Como está o SALIC (ADR 0017)

| O quê | Onde |
|---|---|
| Bronze | tabela `salic.bronze_<banco>_<tabela>`, gravada pela `salic_ingestion_trino`. Tudo `text`, mais a coluna técnica `_fatia` (ADR 0005). **Não é modelo dbt** |
| Source da Bronze | source `salic`, em `salic/sources_<banco>.yml`, tabelas `bronze_<banco>_<tabela>` |
| Silver | `salic/silver_<banco>_<tabela>.sql`, view, schema `salic`, documentada em `salic/schema_<banco>.yml` |
| Legado ainda não migrado | `salic_dbt/bronze/<banco>/<banco>__<tabela>.sql`, com as sources em `salic_dbt/bronze/sources_<banco>.yml` apontando para `salic.bronze_*` por `identifier` (ponte do #110) |
| Dicionário original | `dbt/minc/docs/salic/` (export SchemaSpy, dez/2025) e as descrições já copiadas para as sources |

Os nomes não colidem no schema `salic`: `bronze_*` é da ingestão, `silver_*`
é do dbt, `<banco>__<tabela>` é o legado. **Nunca crie modelo `bronze_*`**:
ele substituiria a tabela que a DAG grava.

Os lotes são por banco de origem: `controledeacesso` (3), `bdcorporativo` (5),
`agentes` (23), `tabelas` (77) e `sac` (481). O `sac` é grande demais para um
PR só: divida por assunto.

## A sessão não lê o `.env`

O `.env` é proibido para a sessão. **Tudo que toca o banco vira um script que
o usuário roda e cuja saída ele cola de volta.** Grave o script no scratchpad,
dê o caminho absoluto completo e o comando exato. A saída que volta para a
conversa **não pode ter valor de linha**: nem CPF, nem nome, nem e-mail. Os
scripts desta skill já imprimem só formato, contagem e domínio de coluna
categórica. Ver `references/conexao-e-vpn.md`.

## Fluxo por lote

1. **Progresso.** `scripts/inventario.py` lê o repositório e mostra, por banco,
   o que já migrou e quantas colunas ainda estão `[NÃO VERIFICADO]`. Não toca o
   banco.
2. **Migração mecânica.** `python3 scripts/migrar_lote_salic.py <banco>` move
   os arquivos, troca nomes, cabeçalho, schema e sources, e reescreve os
   leitores. Leia os avisos que ele imprime. O corpo SQL não muda.
3. **Perfil.** Gere o comando de `scripts/perfilar.py` para as tabelas do lote
   e peça ao usuário para rodar. Ele faz uma leitura de 200 linhas por tabela e,
   com `--completo`, uma varredura agregada que mede preenchimento e falha de
   cast na tabela inteira. Ver `references/tipagem-e-casts.md`.
4. **Dicionário.** Para cada tabela, leia a descrição que veio do SchemaSpy e
   o catálogo em `dbt/minc/docs/salic/`. Tipo original do SQL Server e
   descrição de coluna, quando existem, estão lá.
5. **Análise semântica.** Escreva as descrições pelo
   `references/template-documentacao.md`: grão, significado, volume, armadilha
   e como cruza. Cruze perfil, dicionário e o SQL dos modelos que consomem a
   tabela (`grep` pelo nome em `dbt/minc/models`). Procure também o código
   público do SalicWeb (`lablivre-unb/salic-br` no GitHub): é onde aparece o
   significado de código que o banco não documenta. O que não se confirmar vai
   em prosa, dizendo o que foi medido e o que não se sabe. **O marcador
   `[NÃO VERIFICADO]` é proibido em silver**: a guarda
   `tests/test_salic_silver_governance.py` reprova. Coluna cujo significado
   ninguém consegue dizer não deveria ser publicada: tire do `select`.
6. **Revisão.** Passe o checklist de "Antes de considerar o lote pronto",
   abaixo. É onde entram tipos, testes, dado pessoal, credencial e o bloco de
   governança que todo modelo precisa declarar (`status`, `rag_publication`,
   `produto_dados`, `classificacao_uso`; ver `references/template-documentacao.md`).
7. **Validação.** `dbt parse`; um script para o usuário que roda
   `DBT_DEV_SCHEMA=dev_salic dbt run --target dev --select "salic.silver_<banco>_*"`,
   compara `count(*)` de cada `salic.bronze_*` com a silver em `dev_salic` e
   apaga o schema no fim; `dbt test` das silvers do lote no mesmo schema;
   `verificar.py` da skill `arquitetura-lakehouse-minc` em
   `dbt/minc/models/salic`; `pytest`, que inclui a guarda de governança das
   silvers; `make docs-collect`.
8. **PR**, com a skill `abrir-pr`, e a contagem antes e depois nas evidências.

Commite cada lote. A VPN cai, e o que não foi commitado se perde.

## Regras que não se negociam

**Credencial não entra em silver.** Coluna de senha, hash de senha, token ou
chave de API sai do `select` da silver. Não basta marcar `PII.Sensitive`: a
view é analítica, e o OpenMetadata lê o dado para perfilar. Registre na
descrição da tabela que a coluna existe na Bronze e foi retirada.

**Dado pessoal se classifica pela LGPD.** CPF, CNPJ de pessoa física,
data de nascimento, e-mail, telefone, endereço, raça e deficiência são
`PII.Sensitive` quando identificam alguém. Nome sozinho é `PII.NonSensitive`.
Na dúvida, `Sensitive`.

**Teste tem que significar alguma coisa e caber no Cosmos.** As silvers são
views sobre tabelas `text` sem índice, e cada teste vira uma varredura na
execução da DAG.
- `relationships` de uma tabela com ela mesma não testa nada e já rodou por
  mais de três horas em produção, segurando lock. Apague.
- `not_null` e `unique` marcados `# inferido do perfil do banco` só ficam se o
  perfil completo confirmar. Senão, saem.
- `unique` e `relationships` em tabela de milhões de linhas: só se a chave for
  usada de verdade num join a jusante.

**Nome passa de 63 caracteres.** Com o prefixo `bronze_`, alguns nomes do
`sac` passam do limite do Postgres, que corta em silêncio. A source declara
`identifier` com o nome cortado. Confira com `length(tablename)` antes de
escrever a source.

**Banco de produção.** Sessão read-only, `statement_timeout`, nunca `SELECT
DISTINCT` em coluna sem índice, nunca DDL. As tabelas da Bronze pertencem a
`dl_airflow`; o usuário do `.env` é `dl_analytics` e não consegue alterar
nenhuma delas.

## Antes de considerar o lote pronto

- [ ] Nenhuma coluna de credencial no `select` de silver nenhuma
- [ ] Toda tabela tem grão ("Uma linha por …") na descrição
- [ ] Nenhum `[NÃO VERIFICADO]`: o que não se confirmou está em prosa, com o que foi medido
- [ ] Todo modelo com `status`, `governance` (`rag_publication`, `produto_dados`, `classificacao_uso`) e `openmetadata` (domínio `Cultura.Incentivo Fiscal`, tier, dono)
- [ ] Chave que leva a uma pessoa (`idusuario`, `idagente`) com tag de PII
- [ ] Nenhuma descrição só repete o nome da coluna
- [ ] Classificação de dado pessoal revista coluna a coluna
- [ ] Todo teste `# inferido` confirmado pelo perfil completo ou removido
- [ ] Nenhum `relationships` de uma tabela com ela mesma
- [ ] Cast que vira NULL tem a contagem na descrição
- [ ] Contagem origem × silver igual, registrada no PR
- [ ] `verificar.py` sem violação em `salic/`, `pytest` verde, `make docs-collect` rodado
- [ ] Nenhum valor de linha no PR, no diário ou na conversa

## Referências

| Arquivo | Quando ler |
|---|---|
| `references/conexao-e-vpn.md` | Antes de gerar o primeiro script que toca o banco |
| `references/tipagem-e-casts.md` | Ao revisar os casts que a silver já faz |
| `references/template-documentacao.md` | Ao escrever o `schema_<banco>.yml`. É o padrão obrigatório |
| `scripts/inventario.py` | No começo de toda sessão |
| `scripts/perfilar.py` | Uma vez por lote, rodado pelo usuário |
| `docs/openmetadata/MEMORY.md` §10 | O diário da migração, com o que cada lote encontrou |
