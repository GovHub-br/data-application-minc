-- Bronze SALIC — sac__vmaioresincentivadoresprojetos.
-- Origem: salic_bronze.sac__vmaioresincentivadoresprojetos, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 6 colunas: 3 tipadas, 2 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("ano") }} as ano,
    {{ bronze_texto("incentivador") }} as incentivador,
    {{ bronze_texto("anoprojeto") }} as anoprojeto,
    {{ bronze_inteiro("sequencial") }} as sequencial,
    {{ bronze_numerico("valor") }} as valor,
    _fatia
from {{ source("bronze_sac", "sac__vmaioresincentivadoresprojetos") }}
