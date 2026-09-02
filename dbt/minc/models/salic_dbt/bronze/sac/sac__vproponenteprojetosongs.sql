-- Bronze SALIC — sac__vproponenteprojetosongs.
-- Origem: salic_bronze.sac__vproponenteprojetosongs, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 6 colunas: 1 tipadas, 4 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_texto("cgccpf") }} as cgccpf,
    {{ bronze_texto("nome") }} as nome,
    {{ bronze_texto("anoprojeto") }} as anoprojeto,
    {{ bronze_texto("sequencial") }} as sequencial,
    {{ bronze_numerico("captacaoreal") }} as captacaoreal,
    _fatia
from {{ source("bronze_sac", "sac__vproponenteprojetosongs") }}
