-- Silver Mapas — agent_meta: atributos dos agentes em chave-valor.
-- Origem: mapas.bronze_agent_meta, cópia fiel em que tudo chega como texto.
-- Esta camada só limpa, tipa e tira duplicata; não cruza tabelas nem fontes.
--
-- Grão: 1 linha por registro de atributo (`id`). Um agente tem várias linhas,
-- uma por `key`. NÃO é pivotada aqui: virar uma linha por agente com uma coluna
-- por atributo é cruzamento e fica para a camada seguinte.
--
-- `value` é sempre texto: o tipo depende da `key`, e a tabela não diz qual é.
-- Texto vazio e só de espaços viram NULL, e os espaços das pontas saem.
--
-- `value` acima de 100 KB vira NULL. São 29 linhas, todas em
-- `rcv_links_coletivo` e `rcv_sede_realizaAtividades_outros_lista`, e o conteúdo
-- é a mesma string reescapada a cada gravação no Mapas: só barras invertidas, o
-- tamanho dobrando até 155 MB. O maior valor legítimo de qualquer chave tem
-- 65 KB. O tamanho é medido ANTES do trim: o trim de 155 MB pede 1,2 GB de uma
-- vez e o Postgres aborta com "invalid memory alloc request size".
--
-- ATENÇÃO — contém dado pessoal: documento (CPF/CNPJ), telefone e
-- autodeclaração. É o motivo de a tabela existir, e é também o que exige
-- cuidado de quem a consome.
--
-- Duplicata: a origem tem PK em `id`; fica a linha da fatia mais recente.
with
    tipado as (
        select
            {{ bronze_inteiro("id") }} as id,
            {{ bronze_inteiro("object_id") }} as object_id,
            {{ bronze_texto("key") }} as key,
            case
                when octet_length(value) <= 100000 then {{ bronze_texto("value") }}
            end as value,
            _fatia
        from {{ source("mapas", "bronze_agent_meta") }}
    ),

    deduplicado as (
        select *, row_number() over (partition by id order by _fatia desc) as ordem
        from tipado
        where id is not null
    )

select id, object_id, key, value
from deduplicado
where ordem = 1
