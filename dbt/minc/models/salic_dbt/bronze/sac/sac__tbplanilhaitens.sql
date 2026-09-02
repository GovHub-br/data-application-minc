-- Bronze SALIC — sac__tbplanilhaitens.
-- Origem: salic_bronze.sac__tbplanilhaitens, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 4 colunas: 2 tipadas, 1 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idplanilhaitens") }} as idplanilhaitens,
    {{ bronze_texto("descricao") }} as descricao,
    {{ bronze_inteiro("idusuario") }} as idusuario,
    _fatia
from {{ source("bronze_sac", "sac__tbplanilhaitens") }}
