-- Bronze SALIC — sac__vw_dba_captacao_2007_2017.
-- Origem: salic_bronze.sac__vw_dba_captacao_2007_2017, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 3 colunas: 1 tipadas, 1 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_texto("anoprojeto") }} as anoprojeto,
    {{ bronze_inteiro("sequencial") }} as sequencial,
    _fatia
from {{ source("bronze_sac", "sac__vw_dba_captacao_2007_2017") }}
