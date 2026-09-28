-- Bronze SALIC — sac__tbdeclaracaobeneficiosfiscais.
-- Origem: salic_bronze.sac__tbdeclaracaobeneficiosfiscais, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 13 colunas: 4 tipadas, 8 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("iddeclaracaobeneficiosfiscais") }}
    as iddeclaracaobeneficiosfiscais,
    {{ bronze_texto("nrexercicio") }} as nrexercicio,
    {{ bronze_texto("nranocalendario") }} as nranocalendario,
    {{ bronze_texto("tpdeclaracao") }} as tpdeclaracao,
    {{ bronze_timestamp('"dtgeração"') }} as dtgeracao,
    {{ bronze_texto("nrcpfcnpjproponente") }} as nrcpfcnpjproponente,
    {{ bronze_inteiro("idpronac") }} as idpronac,
    {{ bronze_texto("nrprojeto") }} as nrprojeto,
    {{ bronze_texto("nrcpfcnpjdoadorpatrocinador") }} as nrcpfcnpjdoadorpatrocinador,
    {{ bronze_texto("cdbaselegal") }} as cdbaselegal,
    {{ bronze_texto("tpcontribuicao") }} as tpcontribuicao,
    {{ bronze_numerico("vlcontribuicao") }} as vlcontribuicao,
    _fatia
from {{ source("bronze_sac", "sac__tbdeclaracaobeneficiosfiscais") }}
