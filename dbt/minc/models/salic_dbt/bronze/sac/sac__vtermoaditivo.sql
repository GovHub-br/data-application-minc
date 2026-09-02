-- Bronze SALIC — sac__vtermoaditivo.
-- Origem: salic_bronze.sac__vtermoaditivo, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 4 colunas: 2 tipadas, 1 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_texto("anoprojeto") }} as anoprojeto,
    {{ bronze_inteiro("sequencial") }} as sequencial,
    {{ bronze_timestamp("dtfinalvigencia") }} as dtfinalvigencia,
    _fatia
from {{ source("bronze_sac", "sac__vtermoaditivo") }}
