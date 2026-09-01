-- Bronze SALIC — sac__tbhistoricodocumento.
-- Origem: salic_bronze.sac__tbhistoricodocumento, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 15 colunas: 12 tipadas, 2 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idhistorico") }} as idhistorico,
    {{ bronze_inteiro("idpronac") }} as idpronac,
    {{ bronze_inteiro("iddocumento") }} as iddocumento,
    {{ bronze_inteiro("idorigem") }} as idorigem,
    {{ bronze_inteiro("idunidade") }} as idunidade,
    {{ bronze_timestamp("dttramitacaoenvio") }} as dttramitacaoenvio,
    {{ bronze_inteiro("idusuarioemissor") }} as idusuarioemissor,
    {{ bronze_texto("medespacho") }} as medespacho,
    {{ bronze_inteiro("idlote") }} as idlote,
    {{ bronze_timestamp("dttramitacaorecebida") }} as dttramitacaorecebida,
    {{ bronze_inteiro("idusuarioreceptor") }} as idusuarioreceptor,
    {{ bronze_inteiro("acao") }} as acao,
    {{ bronze_booleano("stestado") }} as stestado,
    {{ bronze_texto("dsjustificativa") }} as dsjustificativa,
    _fatia
from {{ source("bronze_sac", "sac__tbhistoricodocumento") }}
