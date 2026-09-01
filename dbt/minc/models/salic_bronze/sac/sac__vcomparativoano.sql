-- Bronze SALIC — sac__vcomparativoano.
-- Origem: salic_bronze.sac__vcomparativoano, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 8 colunas: 7 tipadas, 0 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("ano") }} as ano,
    {{ bronze_inteiro("q_apresentado") }} as q_apresentado,
    {{ bronze_numerico("t_apresentado") }} as t_apresentado,
    {{ bronze_inteiro("q_aprovado") }} as q_aprovado,
    {{ bronze_numerico("t_aprovado") }} as t_aprovado,
    {{ bronze_inteiro("q_captado") }} as q_captado,
    {{ bronze_numerico("t_captado") }} as t_captado,
    _fatia
from {{ source("bronze_sac", "sac__vcomparativoano") }}
