# Falar com o `dbanalytics`

## Quem conecta é o usuário

A sessão não lê o `.env`, nem para conectar. O caminho é sempre este:

1. Você grava um script no scratchpad.
2. Você entrega o comando exato, com o caminho absoluto completo do script e
   do `.env` (`/Users/.../data-application-minc/.env`). Nunca um caminho de
   exemplo como `/caminho/do/.env`: o usuário roda literalmente.
3. O usuário roda e cola a saída.

O script lê as credenciais de dentro dele (`set -a; . "$1"; set +a` em shell,
ou o leitor de `.env` dos scripts Python desta skill) e não imprime nenhuma.
As chaves são as do `profiles.yml`: `DB_DW_HOST`, `DB_DW_PORT`, `DB_DW_USER`,
`DB_DW_PASSWORD`, `DB_DW_DBNAME`. Os scripts Python também aceitam o contrato
antigo (`IP`, `PORTA`, `USER`, `PASS`, `DB`).

**A saída vai para a conversa, então não pode ter valor de linha.** Conte,
meça formato, liste domínio só de coluna categórica com nome que não sugere
dado pessoal. Se um script precisar olhar valor, ele olha e resume.

Se o usuário colar uma senha ou um valor sensível por engano, diga que
apareceu, peça a troca quando for credencial e não repita o valor.

## Os papéis no banco

| Papel | Quem usa | O que pode |
|---|---|---|
| `dl_analytics` | o `.env` do usuário, o dbt, o Cosmos | dono dos schemas `salic` e `salic_bronze` e das views do dbt; lê a Bronze |
| `dl_airflow` | a ingestão, pelo catálogo `dw` do Trino | dono das tabelas `salic.bronze_*` |

Alterar uma tabela da Bronze (`ALTER`, `DROP`, `TRUNCATE`) exige ser o dono.
Do `.env` não dá. Isso é trabalho de operação, fora desta skill.

## A VPN cai

Cai com frequência, inclusive no meio de um comando. Sintomas:

- `Operation timed out` ou `timeout expired` ao conectar;
- `Can't assign requested address` ou `could not receive data from server`
  no meio de uma consulta.

Teste antes de rodar algo longo:

```bash
nc -z -G 3 <host> 5432 && echo ok
```

Você não sobe a VPN; só o usuário. Enquanto ela estiver fora, adiante o que
não depende do banco: descrições a partir do dicionário, revisão de testes,
`dbt parse`.

Para que a queda não custe trabalho:

- Script de leitura grava o resultado em disco a cada tabela e retoma de onde
  parou. O `perfilar.py` faz isso.
- Consulta que segura lock (DDL, transação longa) é coisa de operação, não
  desta skill. Se um dia precisar, mande numa instrução só, em autocommit, com
  `lock_timeout`, para a queda não deixar uma sessão órfã segurando tabela.

## Carga no banco

É produção do MinC.

- Sessão read-only e `statement_timeout` sempre.
- Nunca `SELECT DISTINCT` em coluna sem índice: a Bronze não tem índice
  nenhum, e isso vira varredura com ordenação. Já travou uma sessão em tabela
  de 18,6 M de linhas.
- Para amostra, uma leitura de 200 linhas por tabela, e o perfil de todas as
  colunas sai dela.
- Medida da tabela inteira (`--completo` do `perfilar.py`) é uma varredura só
  por tabela, com todas as colunas agregadas na mesma consulta. Em tabela
  grande do `sac`, rode fora do horário do Cosmos.
- Nunca use `reltuples` para decidir se a tabela está vazia: o schema não
  passa por `ANALYZE`, e o planejador reporta 0. Use `exists(select 1 ...)`.
