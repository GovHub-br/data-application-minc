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

| Fica de fora | Tamanho | Por quê |
|---|---|---|
| `pcache`, `permission_cache_pending`, `job` | ~14 GB | cache de permissão e fila: quase tantos deletes quanto inserts, a aplicação reconstrói |
| `spatial_ref_sys` | 7 MB | pertence à extensão PostGIS |
| `entity_revision`, `entity_revision_data`, `entity_revision_revision_data` | ~24 GB | histórico de edições; as tabelas principais já têm o estado atual |
| `blame_log`, `blame_request` | ~16 GB | log de acesso (IP, sessão, navegador); auditoria, não dado cultural |
| `geo_division` | 0,8 GB | a coluna `geom` é `geometry`, tipo que o conector ignora em silêncio |

Sobram ~60 tabelas e ~6 GB de dado.

## Por quê

**O volume que importa é pequeno.** Os ~6 GB cabem na carga completa que a DAG já
faz (`DROP` + `CREATE` + recarga fatiada por chave inteira). Não é preciso modo
incremental, nem fatiamento especial, nem janela de releitura.

**O histórico e o log de acesso saem juntos porque são o que obrigaria código
novo.** `blame_request` tem PK `character(13)`, que a DAG não fatia (o filtro só
aceita inteiro), então iria numa única consulta de ~9,8 GB — o modo de falha que o
ADR 0005 descreve. As três tabelas `entity_revision*` precisariam de carga
incremental para não recarregar ~24 GB por execução. Nada disso se paga sem uma
análise que use o dado.

**`blame_request` carrega dado pessoal que não serve à análise:** `ip`,
`session_id`, `user_id`, `user_agent`. O `session_id` é identificador de sessão
e, se ainda for válido, permite assumir a sessão de quem o lê.

**A geometria some sem aviso.** O comentário anterior do catálogo dizia que
PostgreSQL → PostgreSQL não tem tipos exóticos. Tem: o PostGIS cria `geometry`.
Com `unsupported-type-handling=IGNORE`, o padrão do Trino, a coluna deixa de
aparecer no `information_schema` e a bronze nasce sem ela, sem erro. Só a
`geo_division` tem coluna `geometry` no `public` (conferido em
`geometry_columns`); o contorno territorial já vem do IBGE, pela DAG `territorio`.

## O que foi descartado

**Carregar tudo, incluindo o histórico, em modo incremental.** Foi desenhado
(fatias de ~1 GB por faixa de id, releitura de sobreposição, `created_at` ou
`ctid` para o `blame_request`) e deixado de lado: custa código novo na DAG e no
`plugins/trino_bronze.py` para um dado que ninguém pediu.

**`CONVERT_TO_VARCHAR` para trazer a `geom`.** Funcionaria para a coluna, mas o
texto que sai de um `geometry` precisa ser conferido antes de alguém depender
dele, e o contorno já existe em outra fonte.

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
