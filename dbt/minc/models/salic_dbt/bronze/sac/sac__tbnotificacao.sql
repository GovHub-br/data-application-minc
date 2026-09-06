-- Bronze SALIC — sac__tbnotificacao.
-- Origem: salic_bronze.sac__tbnotificacao, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 10 colunas: 8 tipadas, 1 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idnotificacao") }} as idnotificacao,
    {{ bronze_inteiro("idpronac") }} as idpronac,
    {{ bronze_inteiro("tpnotificacao") }} as tpnotificacao,
    {{ bronze_timestamp("dtenvio") }} as dtenvio,
    {{ bronze_texto("dsnotificacao") }} as dsnotificacao,
    {{ bronze_timestamp("dtvisualizacao") }} as dtvisualizacao,
    {{ bronze_booleano("stvisualizacao") }} as stvisualizacao,
    {{ bronze_booleano("stestado") }} as stestado,
    {{ bronze_inteiro("idusuario") }} as idusuario,
    _fatia
from {{ source("bronze_sac", "sac__tbnotificacao") }}
