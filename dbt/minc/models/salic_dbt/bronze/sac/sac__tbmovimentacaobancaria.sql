-- Bronze SALIC — sac__tbmovimentacaobancaria.
-- Origem: salic_bronze.sac__tbmovimentacaobancaria, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 8 colunas: 5 tipadas, 2 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idmovimentacaobancaria") }} as idmovimentacaobancaria,
    {{ bronze_texto("nrbanco") }} as nrbanco,
    {{ bronze_texto("nmarquivo") }} as nmarquivo,
    {{ bronze_timestamp("dtarquivo") }} as dtarquivo,
    {{ bronze_timestamp("dtiniciomovimento") }} as dtiniciomovimento,
    {{ bronze_timestamp("dtfimmovimento") }} as dtfimmovimento,
    {{ bronze_inteiro("idusuario") }} as idusuario,
    _fatia
from {{ source("bronze_sac", "sac__tbmovimentacaobancaria") }}
