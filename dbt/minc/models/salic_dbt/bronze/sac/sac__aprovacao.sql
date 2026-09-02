-- Bronze SALIC — sac__aprovacao.
-- Origem: salic_bronze.sac__aprovacao, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 24 colunas: 18 tipadas, 5 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idaprovacao") }} as idaprovacao,
    {{ bronze_inteiro("idpronac") }} as idpronac,
    {{ bronze_inteiro("idparecer") }} as idparecer,
    {{ bronze_texto("anoprojeto") }} as anoprojeto,
    {{ bronze_texto("sequencial") }} as sequencial,
    {{ bronze_texto("tipoaprovacao") }} as tipoaprovacao,
    {{ bronze_timestamp("dtaprovacao") }} as dtaprovacao,
    {{ bronze_texto("resumoaprovacao") }} as resumoaprovacao,
    {{ bronze_texto("portariaaprovacao") }} as portariaaprovacao,
    {{ bronze_timestamp("dtportariaaprovacao") }} as dtportariaaprovacao,
    {{ bronze_timestamp("dtpublicacaoaprovacao") }} as dtpublicacaoaprovacao,
    {{ bronze_timestamp("dtiniciocaptacao") }} as dtiniciocaptacao,
    {{ bronze_timestamp("dtfimcaptacao") }} as dtfimcaptacao,
    {{ bronze_numerico("aprovadoufir") }} as aprovadoufir,
    {{ bronze_numerico("aprovadoreal") }} as aprovadoreal,
    {{ bronze_numerico("autorizadoufir") }} as autorizadoufir,
    {{ bronze_numerico("autorizadoreal") }} as autorizadoreal,
    {{ bronze_numerico("concedidocusteioreal") }} as concedidocusteioreal,
    {{ bronze_numerico("concedidocapitalreal") }} as concedidocapitalreal,
    {{ bronze_numerico("contrapartidareal") }} as contrapartidareal,
    {{ bronze_inteiro("logon") }} as logon,
    {{ bronze_inteiro("idprorrogacao") }} as idprorrogacao,
    {{ bronze_inteiro("idreadequacao") }} as idreadequacao,
    _fatia
from {{ source("bronze_sac", "sac__aprovacao") }}
