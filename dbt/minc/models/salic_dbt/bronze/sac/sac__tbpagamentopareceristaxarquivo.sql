-- Bronze SALIC — sac__tbpagamentopareceristaxarquivo.
-- Origem: salic_bronze.sac__tbpagamentopareceristaxarquivo, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 4 colunas: 2 tipadas, 1 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idgerarpagamentoparecerista") }} as idgerarpagamentoparecerista,
    {{ bronze_inteiro("idarquivo") }} as idarquivo,
    {{ bronze_texto("siarquivo") }} as siarquivo,
    _fatia
from {{ source("bronze_sac", "sac__tbpagamentopareceristaxarquivo") }}
