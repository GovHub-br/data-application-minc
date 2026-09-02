-- Bronze SALIC — sac__estados.
-- Origem: salic_bronze.sac__estados, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 5 colunas: 2 tipadas, 2 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("id") }} as id,
    {{ bronze_texto("descricao") }} as descricao,
    {{ bronze_inteiro("fluxoid") }} as fluxoid,
    {{ bronze_texto("proximo") }} as proximo,
    _fatia
from {{ source("bronze_sac", "sac__estados") }}
