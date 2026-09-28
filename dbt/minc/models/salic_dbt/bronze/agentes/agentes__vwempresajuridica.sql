-- Bronze SALIC — agentes__vwempresajuridica.
-- Origem: salic_bronze.agentes__vwempresajuridica, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 18 colunas: 5 tipadas, 12 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_texto("nrcnpj") }} as nrcnpj,
    {{ bronze_inteiro("tpestabelecimento") }} as tpestabelecimento,
    {{ bronze_texto("dsestabelecimento") }} as dsestabelecimento,
    {{ bronze_texto("nmempresarial") }} as nmempresarial,
    {{ bronze_texto("nmfantasia") }} as nmfantasia,
    {{ bronze_texto("cdsituacaocadastral") }} as cdsituacaocadastral,
    {{ bronze_texto("dssituacaocadastral") }} as dssituacaocadastral,
    {{ bronze_data("dtsituacaocadastral") }} as dtsituacaocadastral,
    {{ bronze_inteiro("cdnaturezajuridica") }} as cdnaturezajuridica,
    {{ bronze_texto("dsnaturezajuridica") }} as dsnaturezajuridica,
    {{ bronze_data("dtabertura") }} as dtabertura,
    {{ bronze_texto("eecorreioeletronico") }} as eecorreioeletronico,
    {{ bronze_numerico("vlcapitalsocial") }} as vlcapitalsocial,
    {{ bronze_texto("cdporteempresa") }} as cdporteempresa,
    {{ bronze_texto("dsporteempresa") }} as dsporteempresa,
    {{ bronze_texto("sioptantesimples") }} as sioptantesimples,
    {{ bronze_texto("sioptantemei") }} as sioptantemei,
    _fatia
from {{ source("bronze_agentes", "agentes__vwempresajuridica") }}
