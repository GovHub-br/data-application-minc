-- Bronze SALIC — sac__tbmovimentacaobancariaitem.
-- Origem: salic_bronze.sac__tbmovimentacaobancariaitem, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 20 colunas: 7 tipadas, 12 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idmovimentacaobancariaitem") }} as idmovimentacaobancariaitem,
    {{ bronze_texto("tpregistro") }} as tpregistro,
    {{ bronze_texto("nragencia") }} as nragencia,
    {{ bronze_texto("nrconta") }} as nrconta,
    {{ bronze_texto("nmtitulorazao") }} as nmtitulorazao,
    {{ bronze_texto("nmabreviado") }} as nmabreviado,
    {{ bronze_timestamp("dtaberturaconta") }} as dtaberturaconta,
    {{ bronze_texto("nrcnpjcpf") }} as nrcnpjcpf,
    {{ bronze_numerico("vlsaldoinicial") }} as vlsaldoinicial,
    {{ bronze_texto("stsaldoinicial") }} as stsaldoinicial,
    {{ bronze_numerico("vlsaldofinal") }} as vlsaldofinal,
    {{ bronze_texto("stsaldofinal") }} as stsaldofinal,
    {{ bronze_timestamp("dtmovimento") }} as dtmovimento,
    {{ bronze_texto("cdhistorico") }} as cdhistorico,
    {{ bronze_texto("dshistorico") }} as dshistorico,
    {{ bronze_texto("nrdocumento") }} as nrdocumento,
    {{ bronze_numerico("vlmovimento") }} as vlmovimento,
    {{ bronze_texto("stmovimento") }} as stmovimento,
    {{ bronze_inteiro("idmovimentacaobancaria") }} as idmovimentacaobancaria,
    _fatia
from {{ source("bronze_sac", "sac__tbmovimentacaobancariaitem") }}
