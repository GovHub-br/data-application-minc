-- Bronze SALIC — sac__vportariacomplementacao.
-- Origem: salic_bronze.sac__vportariacomplementacao, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 5 colunas: 2 tipadas, 2 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("anoprojeto") }} as anoprojeto,
    {{ bronze_texto("sequencial") }} as sequencial,
    {{ bronze_texto("tipoaprovacao") }} as tipoaprovacao,
    {{ bronze_timestamp("dtaprovacao") }} as dtaprovacao,
    _fatia
from {{ source("bronze_sac", "sac__vportariacomplementacao") }}
