-- Bronze SALIC — sac__vwanalisedocumentalportecnico.
-- Origem: salic_bronze.sac__vwanalisedocumentalportecnico, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 10 colunas: 5 tipadas, 4 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idprojeto") }} as idprojeto,
    {{ bronze_texto("nomeprojeto") }} as nomeprojeto,
    {{ bronze_texto("tecnico") }} as tecnico,
    {{ bronze_timestamp("dtenvio") }} as dtenvio,
    {{ bronze_timestamp("dtmovimentacao") }} as dtmovimentacao,
    {{ bronze_timestamp("dtavaliacao") }} as dtavaliacao,
    {{ bronze_inteiro("dias") }} as dias,
    {{ bronze_texto("idorgao") }} as idorgao,
    {{ bronze_texto("conformidadeok") }} as conformidadeok,
    _fatia
from {{ source("bronze_sac", "sac__vwanalisedocumentalportecnico") }}
