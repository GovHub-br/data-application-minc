-- Bronze SALIC — sac__historicocarta.
-- Origem: salic_bronze.sac__historicocarta, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 9 colunas: 4 tipadas, 4 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idhistoricocarta") }} as idhistoricocarta,
    {{ bronze_texto("anoprojeto") }} as anoprojeto,
    {{ bronze_texto("sequencial") }} as sequencial,
    {{ bronze_texto("numerocarta") }} as numerocarta,
    {{ bronze_timestamp("dtcarta") }} as dtcarta,
    {{ bronze_texto("status") }} as status,
    {{ bronze_inteiro("orgao") }} as orgao,
    {{ bronze_inteiro("logon") }} as logon,
    _fatia
from {{ source("bronze_sac", "sac__historicocarta") }}
