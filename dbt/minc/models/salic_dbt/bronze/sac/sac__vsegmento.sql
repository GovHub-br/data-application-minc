-- Bronze SALIC — sac__vsegmento.
-- Origem: salic_bronze.sac__vsegmento, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 6 colunas: 3 tipadas, 2 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("area") }} as area,
    {{ bronze_inteiro("codigo") }} as codigo,
    {{ bronze_texto("segmento") }} as segmento,
    {{ bronze_inteiro("idorgao") }} as idorgao,
    {{ bronze_texto("tp_enquadramento") }} as tp_enquadramento,
    _fatia
from {{ source("bronze_sac", "sac__vsegmento") }}
