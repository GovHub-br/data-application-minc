-- Bronze SALIC — sac__vprojetodiligencia.
-- Origem: salic_bronze.sac__vprojetodiligencia, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 4 colunas: 1 tipadas, 2 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_texto("anoprojeto") }} as anoprojeto,
    {{ bronze_inteiro("sequencial") }} as sequencial,
    {{ bronze_texto("diligencia") }} as diligencia,
    _fatia
from {{ source("bronze_sac", "sac__vprojetodiligencia") }}
