-- Bronze SALIC — sac__vdadosportariaaprovacao.
-- Origem: salic_bronze.sac__vdadosportariaaprovacao, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 6 colunas: 2 tipadas, 3 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_texto("anoprojeto") }} as anoprojeto,
    {{ bronze_texto("sequencial") }} as sequencial,
    {{ bronze_texto("numero") }} as numero,
    {{ bronze_timestamp("dtportaria") }} as dtportaria,
    {{ bronze_timestamp("dtpublicacao") }} as dtpublicacao,
    _fatia
from {{ source("bronze_sac", "sac__vdadosportariaaprovacao") }}
