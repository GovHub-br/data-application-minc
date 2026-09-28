-- Bronze SALIC — sac__tbreadequacao.
-- Origem: salic_bronze.sac__tbreadequacao, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 18 colunas: 12 tipadas, 5 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idreadequacao") }} as idreadequacao,
    {{ bronze_inteiro("idpronac") }} as idpronac,
    {{ bronze_inteiro("idtiporeadequacao") }} as idtiporeadequacao,
    {{ bronze_timestamp("dtsolicitacao") }} as dtsolicitacao,
    {{ bronze_inteiro("idsolicitante") }} as idsolicitante,
    {{ bronze_texto("dsjustificativa") }} as dsjustificativa,
    {{ bronze_texto("dssolicitacao") }} as dssolicitacao,
    {{ bronze_inteiro("iddocumento") }} as iddocumento,
    {{ bronze_inteiro("idavaliador") }} as idavaliador,
    {{ bronze_timestamp("dtavaliador") }} as dtavaliador,
    {{ bronze_texto("dsavaliacao") }} as dsavaliacao,
    {{ bronze_texto("statendimento") }} as statendimento,
    {{ bronze_inteiro("siencaminhamento") }} as siencaminhamento,
    {{ bronze_texto("stanalise") }} as stanalise,
    {{ bronze_inteiro("idnrreuniao") }} as idnrreuniao,
    {{ bronze_booleano("stestado") }} as stestado,
    {{ bronze_timestamp("dtenvio") }} as dtenvio,
    _fatia
from {{ source("bronze_sac", "sac__tbreadequacao") }}
