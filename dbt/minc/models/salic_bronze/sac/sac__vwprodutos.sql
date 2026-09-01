-- Bronze SALIC — sac__vwprodutos.
-- Origem: salic_bronze.sac__vwprodutos, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 4 colunas: 1 tipadas, 2 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("codigo") }} as codigo,
    {{ bronze_texto("nmproduto") }} as nmproduto,
    {{ bronze_texto("stestado") }} as stestado,
    _fatia
from {{ source("bronze_sac", "sac__vwprodutos") }}
