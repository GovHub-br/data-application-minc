-- Bronze SALIC — sac__intranet.
-- Origem: salic_bronze.sac__intranet, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 31 colunas: 18 tipadas, 12 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("tipo") }} as tipo,
    {{ bronze_texto("chavea") }} as chavea,
    {{ bronze_texto("chaveb") }} as chaveb,
    {{ bronze_texto("chavec") }} as chavec,
    {{ bronze_texto("chaved") }} as chaved,
    {{ bronze_texto("chavee") }} as chavee,
    {{ bronze_texto("chavef") }} as chavef,
    {{ bronze_texto("campoa") }} as campoa,
    {{ bronze_texto("campob") }} as campob,
    {{ bronze_texto("campoc") }} as campoc,
    {{ bronze_texto("campod") }} as campod,
    {{ bronze_texto("campoe") }} as campoe,
    {{ bronze_texto("campof") }} as campof,
    {{ bronze_inteiro("qtdea") }} as qtdea,
    {{ bronze_inteiro("qtdeb") }} as qtdeb,
    {{ bronze_inteiro("qtdec") }} as qtdec,
    {{ bronze_inteiro("qtded") }} as qtded,
    {{ bronze_inteiro("qtdee") }} as qtdee,
    {{ bronze_inteiro("qtdef") }} as qtdef,
    {{ bronze_numerico("valora") }} as valora,
    {{ bronze_numerico("valorb") }} as valorb,
    {{ bronze_numerico("valorc") }} as valorc,
    {{ bronze_numerico("valord") }} as valord,
    {{ bronze_numerico("valore") }} as valore,
    {{ bronze_timestamp("dataa") }} as dataa,
    {{ bronze_timestamp("datab") }} as datab,
    {{ bronze_timestamp("datac") }} as datac,
    {{ bronze_timestamp("datad") }} as datad,
    {{ bronze_timestamp("datae") }} as datae,
    {{ bronze_timestamp("dataf") }} as dataf,
    _fatia
from {{ source("bronze_sac", "sac__intranet") }}
