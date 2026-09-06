-- Bronze SALIC — sac__tbreadequacaoxparecer.
-- Origem: salic_bronze.sac__tbreadequacaoxparecer, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 4 colunas: 3 tipadas, 0 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idreadequacaoxparecer") }} as idreadequacaoxparecer,
    {{ bronze_inteiro("idreadequacao") }} as idreadequacao,
    {{ bronze_inteiro("idparecer") }} as idparecer,
    _fatia
from {{ source("bronze_sac", "sac__tbreadequacaoxparecer") }}
