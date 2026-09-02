-- Bronze SALIC — sac__tbfundopatrimonial.
-- Origem: salic_bronze.sac__tbfundopatrimonial, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 18 colunas: 11 tipadas, 6 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idfundopatrimonial") }} as idfundopatrimonial,
    {{ bronze_inteiro("idpreprojeto") }} as idpreprojeto,
    {{ bronze_texto("nmfundopatrimonial") }} as nmfundopatrimonial,
    {{ bronze_texto("dsfundopatrimonial") }} as dsfundopatrimonial,
    {{ bronze_inteiro("cdusuariogestor") }} as cdusuariogestor,
    {{ bronze_inteiro("idagenteexecutor") }} as idagenteexecutor,
    {{ bronze_texto("cpfcnpjexecutor") }} as cpfcnpjexecutor,
    {{ bronze_texto("nmexecutor") }} as nmexecutor,
    {{ bronze_numerico("vlfundopatrimonial") }} as vlfundopatrimonial,
    {{ bronze_timestamp("dtcriacao") }} as dtcriacao,
    {{ bronze_timestamp("dtatualizacao") }} as dtatualizacao,
    {{ bronze_texto("stativo") }} as stativo,
    {{ bronze_inteiro("cdusuariocriacao") }} as cdusuariocriacao,
    {{ bronze_inteiro("cdusuarioatualizacao") }} as cdusuarioatualizacao,
    {{ bronze_inteiro("idarea") }} as idarea,
    {{ bronze_inteiro("idcontacorrente") }} as idcontacorrente,
    {{ bronze_texto("cpfcnpjfundo") }} as cpfcnpjfundo,
    _fatia
from {{ source("bronze_sac", "sac__tbfundopatrimonial") }}
