-- Bronze SALIC — sac__tbfundopatrimonialusuario.
-- Origem: salic_bronze.sac__tbfundopatrimonialusuario, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 13 colunas: 8 tipadas, 4 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idfundopatrimonialusuario") }} as idfundopatrimonialusuario,
    {{ bronze_inteiro("idfundopatrimonial") }} as idfundopatrimonial,
    {{ bronze_inteiro("idagenteapoiado") }} as idagenteapoiado,
    {{ bronze_texto("cpfcnpjapoiado") }} as cpfcnpjapoiado,
    {{ bronze_texto("nmapoiado") }} as nmapoiado,
    {{ bronze_numerico("vlvalordestinado") }} as vlvalordestinado,
    {{ bronze_timestamp("dtvinculacao") }} as dtvinculacao,
    {{ bronze_timestamp("dtdesvinculacao") }} as dtdesvinculacao,
    {{ bronze_texto("stvinculacao") }} as stvinculacao,
    {{ bronze_texto("dsobservacoes") }} as dsobservacoes,
    {{ bronze_inteiro("cdusuariocriacao") }} as cdusuariocriacao,
    {{ bronze_inteiro("cdusuarioatualizacao") }} as cdusuarioatualizacao,
    _fatia
from {{ source("bronze_sac", "sac__tbfundopatrimonialusuario") }}
