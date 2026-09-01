-- Bronze SALIC — sac__instrumentobanda.
-- Origem: salic_bronze.sac__instrumentobanda, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 6 colunas: 2 tipadas, 3 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_texto("anoprojeto") }} as anoprojeto,
    {{ bronze_texto("sequencial") }} as sequencial,
    {{ bronze_texto("opcao") }} as opcao,
    {{ bronze_inteiro("codigo") }} as codigo,
    {{ bronze_inteiro("logon") }} as logon,
    _fatia
from {{ source("bronze_sac", "sac__instrumentobanda") }}
