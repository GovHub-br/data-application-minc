-- Bronze SALIC — sac__vwpainelenquadramentopropostas.
-- Origem: salic_bronze.sac__vwpainelenquadramentopropostas, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 14 colunas: 4 tipadas, 9 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idpreprojeto") }} as idpreprojeto,
    {{ bronze_texto("nomeprojeto") }} as nomeprojeto,
    {{ bronze_texto("dstipicidade") }} as dstipicidade,
    {{ bronze_texto("dstipologia") }} as dstipologia,
    {{ bronze_texto("dsexecucaoimediata") }} as dsexecucaoimediata,
    {{ bronze_texto("dsarea") }} as dsarea,
    {{ bronze_texto("dssegmento") }} as dssegmento,
    {{ bronze_texto("dsenquadramento") }} as dsenquadramento,
    {{ bronze_texto("cdsecretaria") }} as cdsecretaria,
    {{ bronze_inteiro("cdperfil") }} as cdperfil,
    {{ bronze_inteiro("cdcomponente") }} as cdcomponente,
    {{ bronze_texto("dscomponente") }} as dscomponente,
    {{ bronze_numerico("vlproposta") }} as vlproposta,
    _fatia
from {{ source("bronze_sac", "sac__vwpainelenquadramentopropostas") }}
