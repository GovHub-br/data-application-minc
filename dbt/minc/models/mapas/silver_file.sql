-- Silver Mapas — file: metadados dos arquivos anexados aos objetos do Mapas (não o
-- conteúdo do arquivo).
-- Origem: mapas.bronze_file, cópia fiel em que tudo chega como texto.
-- Esta camada só limpa, tipa e tira duplicata; não cruza tabelas nem fontes.
--
-- Grão: 1 linha por `id`.
--
-- Tipagem pelos tipos que o banco do Mapas declara. Texto vazio e só de espaços viram
-- NULL;
-- valor que não casa com o tipo vira NULL; ausente fica nulo, nunca vira falso.
with
    tipado as (
        select
            {{ bronze_inteiro("id") }} as id,
            {{ bronze_texto("md5") }} as md5,
            {{ bronze_texto("mime_type") }} as mime_type,
            {{ bronze_texto("name") }} as name,
            {{ bronze_texto("object_type") }} as object_type,
            {{ bronze_inteiro("object_id") }} as object_id,
            {{ bronze_timestamp("create_timestamp") }} as create_timestamp,
            {{ bronze_texto("grp") }} as grp,
            {{ bronze_texto("description") }} as description,
            {{ bronze_inteiro("parent_id") }} as parent_id,
            {{ bronze_texto("path") }} as path,
            {{ bronze_booleano("private") }} as private,
            _fatia
        from {{ source("mapas", "bronze_file") }}
    ),

    deduplicado as (
        select *, row_number() over (partition by id order by _fatia desc) as ordem
        from tipado
        where id is not null
    )

select
    id,
    md5,
    mime_type,
    name,
    object_type,
    object_id,
    create_timestamp,
    grp,
    description,
    parent_id,
    path,
    private
from deduplicado
where ordem = 1
