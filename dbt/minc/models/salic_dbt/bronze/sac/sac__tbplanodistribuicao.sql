-- Bronze SALIC — sac__tbplanodistribuicao.
-- Origem: salic_bronze.sac__tbplanodistribuicao, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 30 colunas: 23 tipadas, 6 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idplanodistribuicao") }} as idplanodistribuicao,
    {{ bronze_inteiro("idreadequacao") }} as idreadequacao,
    {{ bronze_inteiro("idproduto") }} as idproduto,
    {{ bronze_texto("cdarea") }} as cdarea,
    {{ bronze_texto("cdsegmento") }} as cdsegmento,
    {{ bronze_inteiro("idposicaologo") }} as idposicaologo,
    {{ bronze_inteiro("qtproduzida") }} as qtproduzida,
    {{ bronze_inteiro("qtpatrocinador") }} as qtpatrocinador,
    {{ bronze_inteiro("qtproponente") }} as qtproponente,
    {{ bronze_inteiro("qtoutros") }} as qtoutros,
    {{ bronze_inteiro("qtvendanormal") }} as qtvendanormal,
    {{ bronze_inteiro("qtvendapromocional") }} as qtvendapromocional,
    {{ bronze_numerico("vlunitarionormal") }} as vlunitarionormal,
    {{ bronze_numerico("vlunitariopromocional") }} as vlunitariopromocional,
    {{ bronze_booleano("stprincipal") }} as stprincipal,
    {{ bronze_texto("tpsolicitacao") }} as tpsolicitacao,
    {{ bronze_texto("tpanalisetecnica") }} as tpanalisetecnica,
    {{ bronze_texto("tpanalisecomissao") }} as tpanalisecomissao,
    {{ bronze_texto("stativo") }} as stativo,
    {{ bronze_inteiro("idpronac") }} as idpronac,
    {{ bronze_booleano("canalaberto") }} as canalaberto,
    {{ bronze_inteiro("qtdevendapopularnormal") }} as qtdevendapopularnormal,
    {{ bronze_inteiro("qtdevendapopularpromocional") }} as qtdevendapopularpromocional,
    {{ bronze_numerico("vlunitariopopularnormal") }} as vlunitariopopularnormal,
    {{ bronze_numerico("precounitarionormal") }} as precounitarionormal,
    {{ bronze_numerico("receitapopularpromocional") }} as receitapopularpromocional,
    {{ bronze_numerico("receitapopularnormal") }} as receitapopularnormal,
    {{ bronze_numerico("vlreceitatotalprevista") }} as vlreceitatotalprevista,
    {{ bronze_inteiro("idplanodistribuicaooriginal") }} as idplanodistribuicaooriginal,
    _fatia
from {{ source("bronze_sac", "sac__tbplanodistribuicao") }}
