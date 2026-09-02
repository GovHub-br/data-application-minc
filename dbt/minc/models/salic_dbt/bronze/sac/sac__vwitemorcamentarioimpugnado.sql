-- Bronze SALIC — sac__vwitemorcamentarioimpugnado.
-- Origem: salic_bronze.sac__vwitemorcamentarioimpugnado, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 14 colunas: 2 tipadas, 11 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_texto("idpronac") }} as idpronac,
    {{ bronze_texto("pronac") }} as pronac,
    {{ bronze_texto("nomeprojeto") }} as nomeprojeto,
    {{ bronze_texto("produto") }} as produto,
    {{ bronze_texto("etapa") }} as etapa,
    {{ bronze_texto("item") }} as item,
    {{ bronze_texto("stitemavaliado") }} as stitemavaliado,
    {{ bronze_texto("documento") }} as documento,
    {{ bronze_texto("nrcomprovante") }} as nrcomprovante,
    {{ bronze_texto("tpformadepagamento") }} as tpformadepagamento,
    {{ bronze_inteiro("nrdocumentodepagamento") }} as nrdocumentodepagamento,
    {{ bronze_texto("dsjustificativa") }} as dsjustificativa,
    {{ bronze_numerico("vlcomprovado") }} as vlcomprovado,
    _fatia
from {{ source("bronze_sac", "sac__vwitemorcamentarioimpugnado") }}
