# ADR 0008 — A ingestão do Mapas leva só o `public`, sem o histórico de edições nem o log de acesso

- **Data:** 2026-10-05
- **Status:** aceito

## Contexto

A DAG `mapas_ingestion_trino` copia o banco do Mapas Culturais para a bronze pelo
Trino. O banco `mapas` tem três schemas: `public`, `tiger` e `topology`, e foi
medido pelo catálogo do Postgres (`pg_class`), sem varrer tabela nenhuma:

| Schema | Tabelas | Dados (heap + TOAST) | Índices |
|---|---|---|---|
| `public` | 69 | 60 GB | 56 GB |
| `tiger` | 34 | 1,5 MB | 1 MB |
| `topology` | 2 | 16 kB | 32 kB |

`tiger` e `topology` são a instalação padrão do PostGIS (geocodificador do Censo
dos EUA e topologia) e não contêm dado do Mapas. Dentro do `public`, cinco
tabelas concentram a maior parte do volume e não alimentam nenhuma análise de
cultura: `entity_revision*` guardam o histórico de edições, e `blame_*` guardam o
log de acesso dos usuários.

O destino é a bronze do DW, lida pelo dbt, para análise de dados.

## Decisão

Ingerir só o `public`, sem as tabelas abaixo. A lista é configuração (Variable
`mapas_trino_data`, chave `exclude_tables`), não código — a DAG já suportava isso.
Duas mudanças pequenas de código e de catálogo foram necessárias para os tipos do
banco real e estão descritas em "Por quê".

| Fica de fora | Tamanho | Por quê |
|---|---|---|
| `pcache`, `permission_cache_pending`, `job` | ~14 GB | cache de permissão e fila: quase tantos deletes quanto inserts, a aplicação reconstrói |
| `spatial_ref_sys` | 7 MB | pertence à extensão PostGIS |
| `entity_revision`, `entity_revision_data`, `entity_revision_revision_data` | ~24 GB | histórico de edições; as tabelas principais já têm o estado atual |
| `blame_log`, `blame_request` | ~16 GB | log de acesso (IP, sessão, navegador); auditoria, não dado cultural |
| `geo_division` | 0,8 GB | a coluna `geom` é `geometry`, e a conversão para `varchar` da bronze falha nesse tipo |
| as 10 views do `public`: `geometry_columns`, `geography_columns`, `pg_stat_statements`, `pg_stat_statements_info`, `blame`, `evaluations`, `rcv_bi`, `rcv_bi_registration`, `vw_rcv_fato_pontos`, `vw_rcv_fato_pontos_old` | — | o Trino as lista como tabela base e a DAG tentaria materializá-las; são derivadas das tabelas base (ou da extensão), e a `blame` junta o log de acesso que já ficou de fora |

Sobram 59 tabelas base (69 no `public`, menos 10 excluídas; as 10 views entram à
parte na lista acima) e ~6,5 GB de dado. A contagem foi conferida contra o banco
real, só por metadados.

## Por quê

**O volume que importa é pequeno.** Os ~6,5 GB cabem na carga completa que a DAG já
faz (`DROP` + `CREATE` + recarga). A maior tabela que sobra, `user_meta`, tem
~2,5 GB. Não é preciso modo incremental nem janela de releitura.

**O histórico e o log de acesso saem juntos porque são o que obrigaria código
novo.** `blame_request` tem PK `character(13)`, que a DAG não fatiaria (o filtro só
aceita inteiro) e iria numa única consulta de ~9,8 GB — o modo de falha que o
ADR 0005 descreve. As três tabelas `entity_revision*` precisariam de carga
incremental para não recarregar ~24 GB por execução. Nada disso se paga sem uma
análise que use o dado.

**`blame_request` carrega dado pessoal que não serve à análise:** `ip`,
`session_id`, `user_id`, `user_agent`. O `session_id` é identificador de sessão
e, se ainda for válido, permite assumir a sessão de quem o lê.

**A geometria derruba a carga da tabela.** A documentação da DAG dizia que
PostgreSQL → PostgreSQL não tem tipos exóticos. Tem: o PostGIS cria `geometry`.
Testado contra um Trino 478 local (a versão do compose) com PostGIS: o conector
expõe a coluna como `Geometry`, e o `CAST(... AS varchar)` que a bronze aplica a
toda coluna falha com `TYPE_MISMATCH` — a tabela termina em erro, sem
chegar a carregar. Só a `geo_division` tem coluna `geometry` no `public`
(conferido no banco real); o contorno territorial já vem do IBGE, pela DAG
`territorio`.

**O banco real tem dois outros tipos que o teste local não mostrou.** Conferido
nos metadados do banco real (nenhuma linha lida):

- **`json`/`jsonb` em 11 tabelas base do escopo**, entre elas `registration`,
  `opportunity` e `agent_relation`. O `CAST(json AS varchar)` só aceita valor
  escalar, e objeto ou lista falha com `INVALID_CAST_ARGUMENT`, derrubando a
  tabela. `cast_to_text`, em `plugins/trino_bronze.py`, passou a usar
  `json_format(col)` para o tipo `json`. É a única mudança em código
  compartilhado: o conector do SQL Server, do SALIC, não expõe esse tipo, e os
  testes fixam que todo outro tipo continua gerando o mesmo SQL de antes.
- **`geography` em `agent._geo_location` e `space._geo_location`.** Com o padrão
  `unsupported-type-handling=IGNORE`, o Trino **descarta a coluna em silêncio**:
  a bronze nasceria sem a localização e nenhuma mensagem avisaria. O catálogo
  `mapas` passou a usar `CONVERT_TO_VARCHAR`, e a coluna chega como texto
  hexadecimal (EWKB), que o PostGIS lê de volta. Testado localmente; os catálogos
  do SALIC já tinham essa opção e não foram tocados.

## O que foi descartado

**Carregar tudo, incluindo o histórico, em modo incremental.** Foi desenhado
(fatias de ~1 GB por faixa de id, releitura de sobreposição, `created_at` ou
`ctid` para o `blame_request`) e deixado de lado: custa código novo na DAG e no
`plugins/trino_bronze.py` para um dado que ninguém pediu.

**Trazer a `geom` convertendo-a na bronze.** `ST_AsText(geom)` e
`to_hex(ST_AsBinary(geom))` funcionam no Trino (testado), mas exigem tratar o
tipo em `cast_to_text`, em `plugins/trino_bronze.py` — código novo, para um
contorno que já existe em outra fonte.

## Consequências

Quem precisar do histórico de edições ou do log de acesso abre uma nova decisão
e não encontra nada pronto. O desenho descartado acima é o ponto de partida, com
três cuidados que já se sabem: o `session_id` não deve chegar à bronze em claro;
a bronze guarda tudo como `varchar`, então o marcador de "último id carregado"
precisa de `CAST` para inteiro, senão a comparação sai lexical; e os ids de
`entity_revision*` e `blame_log` têm buracos grandes, então a largura da fatia
sai de bytes e densidade, não do intervalo de ids.

A lista de exclusão vive só na Variable do Airflow. Quem recriar o ambiente do
zero precisa refazê-la a partir do docstring de `mapas_ingestion_trino.py`, que
a documenta.

### Limitações da DAG encontradas em teste local, não corrigidas aqui

Executando as funções da DAG contra um Trino 478 e um Postgres com PostGIS locais
(1,2 M de linhas na maior tabela), com a lista de exclusão acima:

- **Nenhuma tabela é fatiada.** As duas consultas de metadado falham com
  `TABLE_NOT_FOUND`: `information_schema.table_constraints` e
  `pg_catalog.pg_stat_user_tables` não existem no catálogo do Trino. A DAG captura
  o erro, avisa e carrega cada tabela numa só consulta; `row_count` fica 0 e o
  `dry_run` sempre mostra 0 linhas. Com o escopo atual a maior tabela tem ~2,5 GB,
  então não impede a carga, mas o fatiamento descrito na docstring não vale para o
  Mapas. A correção seguiria o SALIC: ler esses metadados por
  passthrough (`system.query`).
- **Views entram como tabela base.** O Trino as lista como `BASE TABLE`, então
  qualquer view nova no `public` é copiada, sem aviso, a menos que entre em
  `exclude_tables`. Hoje são 10, todas na lista. Vale conferir a cada mudança
  de versão do Mapas.
- **O cache de metadado de 30 min do catálogo `mapas`** faz o Trino continuar
  enxergando uma coluna removida na origem. Quem mudar a estrutura de uma tabela
  no meio de uma carga precisa reiniciar o Trino.
