-- Bronze SALIC — sac__vwprojetosgratuitopago.
-- Origem: salic_bronze.sac__vwprojetosgratuitopago, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 18 colunas: 9 tipadas, 8 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("ano") }} as ano,
    {{ bronze_inteiro("idpronac") }} as idpronac,
    {{ bronze_inteiro("pronac") }} as pronac,
    {{ bronze_texto("nomeprojeto") }} as nomeprojeto,
    {{ bronze_inteiro("area") }} as area,
    {{ bronze_texto("dsarea") }} as dsarea,
    {{ bronze_inteiro("segmento") }} as segmento,
    {{ bronze_texto("dssegmento") }} as dssegmento,
    {{ bronze_texto("gratuito") }} as gratuito,
    {{ bronze_texto("resumoprojeto") }} as resumoprojeto,
    {{ bronze_texto("objetivos") }} as objetivos,
    {{ bronze_texto("acessibilidade") }} as acessibilidade,
    {{ bronze_texto("democratizacaodeacesso") }} as democratizacaodeacesso,
    {{ bronze_inteiro("qtdeproduzida") }} as qtdeproduzida,
    {{ bronze_numerico("vlproduto") }} as vlproduto,
    {{ bronze_numerico("vlautorizado") }} as vlautorizado,
    {{ bronze_numerico("vlcaptado") }} as vlcaptado,
    _fatia
from {{ source("bronze_sac", "sac__vwprojetosgratuitopago") }}
