-- Bronze SALIC — sac__vwprogramarouanetincentivadorprojetosapoiados.
-- Origem: salic_bronze.sac__vwprogramarouanetincentivadorprojetosapoiados, onde tudo
-- chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 40 colunas: 22 tipadas, 17 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idpronac") }} as idpronac,
    {{ bronze_inteiro("nrpronac") }} as nrpronac,
    {{ bronze_texto("sguf") }} as sguf,
    {{ bronze_inteiro("cdarea") }} as cdarea,
    {{ bronze_texto("dsarea") }} as dsarea,
    {{ bronze_texto("cdsegmento") }} as cdsegmento,
    {{ bronze_texto("dssegmento") }} as dssegmento,
    {{ bronze_texto("nmprojeto") }} as nmprojeto,
    {{ bronze_texto("nrcnpfcpf") }} as nrcnpfcpf,
    {{ bronze_texto("nmproponente") }} as nmproponente,
    {{ bronze_texto("cdsituacao") }} as cdsituacao,
    {{ bronze_texto("dssituacao") }} as dssituacao,
    {{ bronze_texto("dscdsituacao") }} as dscdsituacao,
    {{ bronze_timestamp("dtadmissao") }} as dtadmissao,
    {{ bronze_inteiro("idunidade") }} as idunidade,
    {{ bronze_inteiro("idsecretaria") }} as idsecretaria,
    {{ bronze_timestamp("dtsituacao") }} as dtsituacao,
    {{ bronze_texto("dsresumoprojeto") }} as dsresumoprojeto,
    {{ bronze_timestamp("dtinicioexecucao") }} as dtinicioexecucao,
    {{ bronze_timestamp("dtfimexecucao") }} as dtfimexecucao,
    {{ bronze_texto("tptipicidade") }} as tptipicidade,
    {{ bronze_texto("dstipicidade") }} as dstipicidade,
    {{ bronze_inteiro("tptipologia") }} as tptipologia,
    {{ bronze_texto("dstipologia") }} as dstipologia,
    {{ bronze_inteiro("idfase") }} as idfase,
    {{ bronze_texto("dsfase") }} as dsfase,
    {{ bronze_texto("nrcnpjcpfincentivador") }} as nrcnpjcpfincentivador,
    {{ bronze_texto("nmincentivador") }} as nmincentivador,
    {{ bronze_timestamp("dtincentivo") }} as dtincentivo,
    {{ bronze_timestamp("dttransferenciarecurso") }} as dttransferenciarecurso,
    {{ bronze_numerico("vlincentivo") }} as vlincentivo,
    {{ bronze_numerico("vlaprovado") }} as vlaprovado,
    {{ bronze_numerico("vlcaptado") }} as vlcaptado,
    {{ bronze_numerico("percentualcaptado") }} as percentualcaptado,
    {{ bronze_numerico("vlcomprovado") }} as vlcomprovado,
    {{ bronze_numerico("vlsaldodiariocontas") }} as vlsaldodiariocontas,
    {{ bronze_numerico("vlutilizado") }} as vlutilizado,
    {{ bronze_numerico("percentualutilizado") }} as percentualutilizado,
    {{ bronze_numerico("percentualnascontas") }} as percentualnascontas,
    _fatia
from {{ source("bronze_sac", "sac__vwprogramarouanetincentivadorprojetosapoiados") }}
