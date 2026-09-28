-- Bronze SALIC — sac__vwmediana.
-- Origem: salic_bronze.sac__vwmediana, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 18 colunas: 10 tipadas, 7 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_texto("tipo") }} as tipo,
    {{ bronze_inteiro("idproduto") }} as idproduto,
    {{ bronze_texto("dsproduto") }} as dsproduto,
    {{ bronze_inteiro("idplanilhaitem") }} as idplanilhaitem,
    {{ bronze_texto("dsitemorcamentario") }} as dsitemorcamentario,
    {{ bronze_inteiro("iduf") }} as iduf,
    {{ bronze_texto("dsunidadefederacao") }} as dsunidadefederacao,
    {{ bronze_inteiro("idmunicipio") }} as idmunicipio,
    {{ bronze_texto("dsmunicipio") }} as dsmunicipio,
    {{ bronze_inteiro("idunidade") }} as idunidade,
    {{ bronze_texto("dsunidade") }} as dsunidade,
    {{ bronze_inteiro("qtitens") }} as qtitens,
    {{ bronze_texto("simediana") }} as simediana,
    {{ bronze_numerico("vlmediana") }} as vlmediana,
    {{ bronze_numerico("vlminimo") }} as vlminimo,
    {{ bronze_numerico("vlmedio") }} as vlmedio,
    {{ bronze_numerico("vlmaximo") }} as vlmaximo,
    _fatia
from {{ source("bronze_sac", "sac__vwmediana") }}
