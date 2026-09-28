-- Bronze SALIC — sac__vmostraordembancaria.
-- Origem: salic_bronze.sac__vmostraordembancaria, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 7 colunas: 3 tipadas, 3 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_texto("nrordembancaria") }} as nrordembancaria,
    {{ bronze_timestamp("dtordembancaria") }} as dtordembancaria,
    {{ bronze_numerico("valor") }} as valor,
    {{ bronze_texto("usuario") }} as usuario,
    {{ bronze_texto("anoprojeto") }} as anoprojeto,
    {{ bronze_inteiro("sequencial") }} as sequencial,
    _fatia
from {{ source("bronze_sac", "sac__vmostraordembancaria") }}
