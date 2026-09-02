-- Bronze SALIC — sac__vwlistarcaptacoes.
-- Origem: salic_bronze.sac__vwlistarcaptacoes, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 21 colunas: 4 tipadas, 16 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idpronac") }} as idpronac,
    {{ bronze_texto("nrpronac") }} as nrpronac,
    {{ bronze_texto("nmprojeto") }} as nmprojeto,
    {{ bronze_texto("nmregiao") }} as nmregiao,
    {{ bronze_texto("sguf") }} as sguf,
    {{ bronze_texto("nmuf") }} as nmuf,
    {{ bronze_inteiro("cdorgao") }} as cdorgao,
    {{ bronze_texto("cdsituacao") }} as cdsituacao,
    {{ bronze_texto("nrlote") }} as nrlote,
    {{ bronze_texto("nrcnpj_cpf_incentivador") }} as nrcnpj_cpf_incentivador,
    {{ bronze_texto("nmincentivador") }} as nmincentivador,
    {{ bronze_texto("tpapoio") }} as tpapoio,
    {{ bronze_texto("dstipoapoio") }} as dstipoapoio,
    {{ bronze_timestamp("dtcaptacao") }} as dtcaptacao,
    {{ bronze_numerico("vlcaptado") }} as vlcaptado,
    {{ bronze_texto("sitransferenciarecurso") }} as sitransferenciarecurso,
    {{ bronze_texto("dstransferenciarecurso") }} as dstransferenciarecurso,
    {{ bronze_texto("dttransferenciarecurso") }} as dttransferenciarecurso,
    {{ bronze_texto("isbemservico") }} as isbemservico,
    {{ bronze_texto("dsbemservico") }} as dsbemservico,
    _fatia
from {{ source("bronze_sac", "sac__vwlistarcaptacoes") }}
