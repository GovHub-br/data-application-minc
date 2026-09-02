-- Bronze SALIC — sac__vwpaineldeliberacao.
-- Origem: salic_bronze.sac__vwpaineldeliberacao, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 17 colunas: 9 tipadas, 7 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("anoprojeto") }} as anoprojeto,
    {{ bronze_inteiro("sequencial") }} as sequencial,
    {{ bronze_inteiro("pronac") }} as pronac,
    {{ bronze_inteiro("idpronac") }} as idpronac,
    {{ bronze_texto("nomeprojeto") }} as nomeprojeto,
    {{ bronze_inteiro("cgccpf", tipo="bigint") }} as cgccpf,
    {{ bronze_numerico("percentualcaptado") }} as percentualcaptado,
    {{ bronze_numerico("vlcaptado") }} as vlcaptado,
    {{ bronze_texto("inabilitado") }} as inabilitado,
    {{ bronze_texto("certidao") }} as certidao,
    {{ bronze_texto("cadin") }} as cadin,
    {{ bronze_inteiro("idorgao") }} as idorgao,
    {{ bronze_texto("idsecretaria") }} as idsecretaria,
    {{ bronze_timestamp("dtsituacao") }} as dtsituacao,
    {{ bronze_texto("situacao") }} as situacao,
    {{ bronze_texto("providenciatomada") }} as providenciatomada,
    _fatia
from {{ source("bronze_sac", "sac__vwpaineldeliberacao") }}
