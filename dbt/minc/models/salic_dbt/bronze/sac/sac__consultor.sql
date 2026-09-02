-- Bronze SALIC — sac__consultor.
-- Origem: salic_bronze.sac__consultor, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 5 colunas: 2 tipadas, 2 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("codigo") }} as codigo,
    {{ bronze_texto("nome") }} as nome,
    {{ bronze_texto("area") }} as area,
    {{ bronze_booleano("status") }} as status,
    _fatia
from {{ source("bronze_sac", "sac__consultor") }}
