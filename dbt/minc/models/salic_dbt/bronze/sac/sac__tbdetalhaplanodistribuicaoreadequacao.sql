-- Bronze SALIC — sac__tbdetalhaplanodistribuicaoreadequacao.
-- Origem: salic_bronze.sac__tbdetalhaplanodistribuicaoreadequacao, onde tudo chega como
-- texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 30 colunas: 23 tipadas, 6 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("iddetalhaplanodistribuicao") }} as iddetalhaplanodistribuicao,
    {{ bronze_inteiro("idplanodistribuicao") }} as idplanodistribuicao,
    {{ bronze_inteiro("idreadequacao") }} as idreadequacao,
    {{ bronze_inteiro("iduf") }} as iduf,
    {{ bronze_inteiro("idmunicipio") }} as idmunicipio,
    {{ bronze_booleano("stdistribuicao") }} as stdistribuicao,
    {{ bronze_texto("dsproduto") }} as dsproduto,
    {{ bronze_inteiro("qtexemplares") }} as qtexemplares,
    {{ bronze_inteiro("qtgratuitadivulgacao") }} as qtgratuitadivulgacao,
    {{ bronze_inteiro("qtgratuitapatrocinador") }} as qtgratuitapatrocinador,
    {{ bronze_inteiro("qtgratuitapopulacao") }} as qtgratuitapopulacao,
    {{ bronze_inteiro("qtpopularintegral") }} as qtpopularintegral,
    {{ bronze_inteiro("qtpopularparcial") }} as qtpopularparcial,
    {{ bronze_numerico("vlunitariopopularintegral") }} as vlunitariopopularintegral,
    {{ bronze_numerico("vlreceitapopularintegral") }} as vlreceitapopularintegral,
    {{ bronze_numerico("vlreceitapopularparcial") }} as vlreceitapopularparcial,
    {{ bronze_inteiro("qtproponenteintegral") }} as qtproponenteintegral,
    {{ bronze_inteiro("qtproponenteparcial") }} as qtproponenteparcial,
    {{ bronze_numerico("vlunitarioproponenteintegral") }} as vlunitarioproponenteintegral,
    {{ bronze_numerico("vlreceitaproponenteintegral") }} as vlreceitaproponenteintegral,
    {{ bronze_numerico("vlreceitaproponenteparcial") }} as vlreceitaproponenteparcial,
    {{ bronze_numerico("vlreceitaprevista") }} as vlreceitaprevista,
    {{ bronze_texto("tplocal") }} as tplocal,
    {{ bronze_texto("tpespaco") }} as tpespaco,
    {{ bronze_texto("tpvenda") }} as tpvenda,
    {{ bronze_texto("tpsolicitacao") }} as tpsolicitacao,
    {{ bronze_texto("stativo") }} as stativo,
    {{ bronze_inteiro("idpronac") }} as idpronac,
    {{ bronze_inteiro("iddetalhaoriginal") }} as iddetalhaoriginal,
    _fatia
from {{ source("bronze_sac", "sac__tbdetalhaplanodistribuicaoreadequacao") }}
