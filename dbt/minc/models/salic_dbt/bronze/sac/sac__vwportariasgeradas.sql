-- Bronze SALIC — sac__vwportariasgeradas.
-- Origem: salic_bronze.sac__vwportariasgeradas, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 8 colunas: 2 tipadas, 5 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idpronac") }} as idpronac,
    {{ bronze_inteiro("pronac") }} as pronac,
    {{ bronze_texto("nomeprojeto") }} as nomeprojeto,
    {{ bronze_texto("area") }} as area,
    {{ bronze_texto("segmento") }} as segmento,
    {{ bronze_texto("dsreadequacao") }} as dsreadequacao,
    {{ bronze_texto("tipoportaria") }} as tipoportaria,
    _fatia
from {{ source("bronze_sac", "sac__vwportariasgeradas") }}
