-- Bronze SALIC — sac__vwpaineldocoordenadorgeral.
-- Origem: salic_bronze.sac__vwpaineldocoordenadorgeral, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 28 colunas: 7 tipadas, 20 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("nrproposta") }} as nrproposta,
    {{ bronze_texto("nmproposta") }} as nmproposta,
    {{ bronze_texto("nmproponente") }} as nmproponente,
    {{ bronze_texto("dstipicidade") }} as dstipicidade,
    {{ bronze_texto("dstipologia") }} as dstipologia,
    {{ bronze_texto("dstipodeexecucao") }} as dstipodeexecucao,
    {{ bronze_timestamp("dtenvioproposta") }} as dtenvioproposta,
    {{ bronze_inteiro("qtdiasemanalise") }} as qtdiasemanalise,
    {{ bronze_texto("dsenquadramentoproponente") }} as dsenquadramentoproponente,
    {{ bronze_texto("cdenquadramentominc") }} as cdenquadramentominc,
    {{ bronze_texto("dsenquadramentominc") }} as dsenquadramentominc,
    {{ bronze_texto("cdareaminc") }} as cdareaminc,
    {{ bronze_texto("dsareaminc") }} as dsareaminc,
    {{ bronze_texto("cdsegmentominc") }} as cdsegmentominc,
    {{ bronze_texto("dssegmentominc") }} as dssegmentominc,
    {{ bronze_numerico("vlproposta") }} as vlproposta,
    {{ bronze_texto("siestadoanaliseproposta") }} as siestadoanaliseproposta,
    {{ bronze_texto("dsestadoanaliseproposta") }} as dsestadoanaliseproposta,
    {{ bronze_inteiro("cdperfilavaliadorproposta") }} as cdperfilavaliadorproposta,
    {{ bronze_texto("dsperfilavaliadorproposta") }} as dsperfilavaliadorproposta,
    {{ bronze_texto("idsecretaria") }} as idsecretaria,
    {{ bronze_texto("dssecretaria") }} as dssecretaria,
    {{ bronze_inteiro("idtecnico") }} as idtecnico,
    {{ bronze_texto("nmtecnico") }} as nmtecnico,
    {{ bronze_inteiro("idcomponente") }} as idcomponente,
    {{ bronze_texto("nmcomponente") }} as nmcomponente,
    {{ bronze_texto("stestadorecurso") }} as stestadorecurso,
    _fatia
from {{ source("bronze_sac", "sac__vwpaineldocoordenadorgeral") }}
