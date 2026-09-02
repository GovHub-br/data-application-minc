-- Bronze SALIC — sac__cartaeventual.
-- Origem: salic_bronze.sac__cartaeventual, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 8 colunas: 4 tipadas, 3 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("contador") }} as contador,
    {{ bronze_texto("anoprojeto") }} as anoprojeto,
    {{ bronze_texto("sequencial") }} as sequencial,
    {{ bronze_timestamp("dtcartaeventual") }} as dtcartaeventual,
    {{ bronze_texto("texto") }} as texto,
    {{ bronze_inteiro("orgao") }} as orgao,
    {{ bronze_inteiro("logon") }} as logon,
    _fatia
from {{ source("bronze_sac", "sac__cartaeventual") }}
