-- Bronze SALIC — sac__vwprojetosapreciadosaguardandohomologacao.
-- Origem: salic_bronze.sac__vwprojetosapreciadosaguardandohomologacao, onde tudo chega
-- como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 20 colunas: 10 tipadas, 9 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idpronac") }} as idpronac,
    {{ bronze_texto("pronac") }} as pronac,
    {{ bronze_texto("nomeprojeto") }} as nomeprojeto,
    {{ bronze_inteiro("area") }} as area,
    {{ bronze_texto("dsarea") }} as dsarea,
    {{ bronze_texto("segmento") }} as segmento,
    {{ bronze_texto("dssegmento") }} as dssegmento,
    {{ bronze_texto("ufprojeto") }} as ufprojeto,
    {{ bronze_texto("situacao") }} as situacao,
    {{ bronze_timestamp("dtsituacao") }} as dtsituacao,
    {{ bronze_texto("prazorecursal") }} as prazorecursal,
    {{ bronze_timestamp("dtpublicacao") }} as dtpublicacao,
    {{ bronze_texto("nrportaria") }} as nrportaria,
    {{ bronze_inteiro("idnrreuniao") }} as idnrreuniao,
    {{ bronze_inteiro("nrreuniao") }} as nrreuniao,
    {{ bronze_numerico("vlsolicitado") }} as vlsolicitado,
    {{ bronze_numerico("vlaprovado") }} as vlaprovado,
    {{ bronze_numerico("vlcaptado") }} as vlcaptado,
    {{ bronze_numerico("perccaptado") }} as perccaptado,
    _fatia
from {{ source("bronze_sac", "sac__vwprojetosapreciadosaguardandohomologacao") }}
