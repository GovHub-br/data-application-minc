-- Bronze SALIC — sac__internet.
-- Origem: salic_bronze.sac__internet, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 14 colunas: 6 tipadas, 7 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("tipo") }} as tipo,
    {{ bronze_texto("chavea") }} as chavea,
    {{ bronze_texto("chaveb") }} as chaveb,
    {{ bronze_texto("chavec") }} as chavec,
    {{ bronze_texto("chaved") }} as chaved,
    {{ bronze_texto("campoa") }} as campoa,
    {{ bronze_texto("campob") }} as campob,
    {{ bronze_texto("campoc") }} as campoc,
    {{ bronze_numerico("valora") }} as valora,
    {{ bronze_numerico("valorb") }} as valorb,
    {{ bronze_numerico("valorc") }} as valorc,
    {{ bronze_numerico("valord") }} as valord,
    {{ bronze_numerico("valore") }} as valore,
    _fatia
from {{ source("bronze_sac", "sac__internet") }}
