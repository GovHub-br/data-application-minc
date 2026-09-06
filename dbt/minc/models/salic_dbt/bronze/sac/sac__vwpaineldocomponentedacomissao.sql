-- Bronze SALIC — sac__vwpaineldocomponentedacomissao.
-- Origem: salic_bronze.sac__vwpaineldocomponentedacomissao, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 18 colunas: 4 tipadas, 13 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("nrproposta") }} as nrproposta,
    {{ bronze_texto("nmproposta") }} as nmproposta,
    {{ bronze_texto("dstipicidade") }} as dstipicidade,
    {{ bronze_texto("dstipologia") }} as dstipologia,
    {{ bronze_texto("dsenquadramentoproponente") }} as dsenquadramentoproponente,
    {{ bronze_texto("cdenquadramentominc") }} as cdenquadramentominc,
    {{ bronze_texto("dsenquadramentominc") }} as dsenquadramentominc,
    {{ bronze_inteiro("cdareaminc") }} as cdareaminc,
    {{ bronze_texto("dsareaminc") }} as dsareaminc,
    {{ bronze_texto("cdsegmentominc") }} as cdsegmentominc,
    {{ bronze_texto("dssegmentominc") }} as dssegmentominc,
    {{ bronze_numerico("vlproposta") }} as vlproposta,
    {{ bronze_texto("idsecretaria") }} as idsecretaria,
    {{ bronze_texto("dssecretaria") }} as dssecretaria,
    {{ bronze_inteiro("idcomponente") }} as idcomponente,
    {{ bronze_texto("nmcomponente") }} as nmcomponente,
    {{ bronze_texto("cdperfilavaliadorproposta") }} as cdperfilavaliadorproposta,
    _fatia
from {{ source("bronze_sac", "sac__vwpaineldocomponentedacomissao") }}
