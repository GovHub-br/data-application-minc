-- Bronze SALIC — sac__tbapiincentivos.
-- Origem: salic_bronze.sac__tbapiincentivos, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- Uma linha por aporte de incentivo (captação) publicado pela API do SALIC.
-- Documentos (CNPJ/CPF) e o nº PRONAC ficam TEXT (zero à esquerda). `hashregistro`
-- é hash SHA em hex, texto. `dtatualizacao` é o instante da carga da API.
select
    {{ bronze_inteiro("idcaptacao") }} as idcaptacao,
    {{ bronze_inteiro("idpronac") }} as idpronac,
    {{ bronze_texto("nrpronac") }} as nrpronac,
    {{ bronze_texto("nmprojeto") }} as nmprojeto,
    {{ bronze_texto("nrcnpjcpfproponente") }} as nrcnpjcpfproponente,
    {{ bronze_texto("nmproponente") }} as nmproponente,
    {{ bronze_texto("nrcnpjcpfincentivador") }} as nrcnpjcpfincentivador,
    {{ bronze_texto("nmincentivador") }} as nmincentivador,
    {{ bronze_texto("nmmunicipioincentivador") }} as nmmunicipioincentivador,
    {{ bronze_texto("sgufincentivador") }} as sgufincentivador,
    {{ bronze_texto("tppessoa") }} as tppessoa,
    {{ bronze_texto("nrlote") }} as nrlote,
    {{ bronze_inteiro("aaincentivo") }} as aaincentivo,
    {{ bronze_inteiro("mmincentivo") }} as mmincentivo,
    {{ bronze_data("dtincentivo") }} as dtincentivo,
    {{ bronze_numerico("vlcaptado") }} as vlcaptado,
    {{ bronze_texto("dstransferenciarecurso") }} as dstransferenciarecurso,
    {{ bronze_timestamp("dttransferenciarecurso") }} as dttransferenciarecurso,
    {{ bronze_texto("dsbemservico") }} as dsbemservico,
    {{ bronze_texto("hashregistro") }} as hashregistro,
    {{ bronze_timestamp("dtatualizacao") }} as dtatualizacao,
    _fatia
from {{ source("bronze_sac", "sac__tbapiincentivos") }}
