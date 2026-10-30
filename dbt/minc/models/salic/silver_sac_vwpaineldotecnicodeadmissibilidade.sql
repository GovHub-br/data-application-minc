-- Silver SALIC — silver_sac_vwpaineldotecnicodeadmissibilidade (ex-sac__vwpaineldotecnicodeadmissibilidade).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_sac_vwpaineldotecnicodeadmissibilidade, onde tudo chega como
-- texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 16 colunas: 5 tipadas, 10 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("nrproposta") }} as nrproposta,
    {{ bronze_texto("nmproposta") }} as nmproposta,
    {{ bronze_texto("dstipicidade") }} as dstipicidade,
    {{ bronze_texto("dstipologia") }} as dstipologia,
    {{ bronze_texto("dstipodeexecucao") }} as dstipodeexecucao,
    {{ bronze_timestamp("dtenvioproposta") }} as dtenvioproposta,
    {{ bronze_inteiro("qtdiasemanalise") }} as qtdiasemanalise,
    {{ bronze_texto("dsenquadramentominc") }} as dsenquadramentominc,
    {{ bronze_texto("dsareaminc") }} as dsareaminc,
    {{ bronze_texto("dssegmentominc") }} as dssegmentominc,
    {{ bronze_texto("idsecretaria") }} as idsecretaria,
    {{ bronze_inteiro("idtecnico") }} as idtecnico,
    {{ bronze_texto("dssecretaria") }} as dssecretaria,
    {{ bronze_numerico("vlproposta") }} as vlproposta,
    {{ bronze_texto("cdperfilavaliadorproposta") }} as cdperfilavaliadorproposta,
    _fatia
from {{ source("salic", "bronze_sac_vwpaineldotecnicodeadmissibilidade") }}
