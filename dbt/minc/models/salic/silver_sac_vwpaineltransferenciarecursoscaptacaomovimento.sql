-- Silver SALIC — silver_sac_vwpaineltransferenciarecursoscaptacaomovimento
-- (ex-sac__vwpaineltransferenciarecursoscaptacaomovimento).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_sac_vwpaineltransferenciarecursoscaptacaomovimento, onde tudo
-- chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 13 colunas: 5 tipadas, 7 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_texto("nrpronac") }} as nrpronac,
    {{ bronze_texto("nmprojeto") }} as nmprojeto,
    {{ bronze_numerico("vlcaptado") }} as vlcaptado,
    {{ bronze_numerico("vlatransferir") }} as vlatransferir,
    {{ bronze_numerico("vlrecebido") }} as vlrecebido,
    {{ bronze_numerico("vlpercentualcaptado") }} as vlpercentualcaptado,
    {{ bronze_texto("stcontaliberada") }} as stcontaliberada,
    {{ bronze_texto("sthabilitado") }} as sthabilitado,
    {{ bronze_texto("stcertidao") }} as stcertidao,
    {{ bronze_texto("stcadin") }} as stcadin,
    {{ bronze_texto("cdorgao") }} as cdorgao,
    {{ bronze_numerico("vlsaldodascontas") }} as vlsaldodascontas,
    _fatia
from {{ source("salic", "bronze_sac_vwpaineltransferenciarecursoscaptacaomovimento") }}
