-- Silver Mapas — db_update: controle das atualizações de banco já aplicadas.
-- Origem: mapas.bronze_db_update, cópia fiel em que tudo chega como texto.
-- Esta camada só limpa, tipa e tira duplicata; não cruza tabelas nem fontes.
--
-- Grão: 1 linha por `name`.
--
-- Tipagem pelos tipos que o banco do Mapas declara. Texto vazio e só de espaços viram
-- NULL;
-- valor que não casa com o tipo vira NULL; ausente fica nulo, nunca vira falso.
with
    tipado as (
        select
            {{ bronze_texto("name") }} as name,
            {{ bronze_timestamp("exec_time") }} as exec_time,
            _fatia
        from {{ source("mapas", "bronze_db_update") }}
    ),

    deduplicado as (
        select *, row_number() over (partition by name order by _fatia desc) as ordem
        from tipado
        where name is not null
    )

select name, exec_time
from deduplicado
where ordem = 1
