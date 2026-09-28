-- Bronze SALIC — sac__vproponentecaptacaoanouf.
-- Origem: salic_bronze.sac__vproponentecaptacaoanouf, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 6 colunas: 2 tipadas, 3 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("ano") }} as ano,
    {{ bronze_texto("cgccpf") }} as cgccpf,
    {{ bronze_texto("nome") }} as nome,
    {{ bronze_texto("uf") }} as uf,
    {{ bronze_numerico("captacao") }} as captacao,
    _fatia
from {{ source("bronze_sac", "sac__vproponentecaptacaoanouf") }}
