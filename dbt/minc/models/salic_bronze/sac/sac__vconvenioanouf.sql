-- Bronze SALIC — sac__vconvenioanouf.
-- Origem: salic_bronze.sac__vconvenioanouf, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 5 colunas: 2 tipadas, 2 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("ano") }} as ano,
    {{ bronze_texto("regiao") }} as regiao,
    {{ bronze_texto("descricao") }} as descricao,
    {{ bronze_numerico("valorconvenio") }} as valorconvenio,
    _fatia
from {{ source("bronze_sac", "sac__vconvenioanouf") }}
