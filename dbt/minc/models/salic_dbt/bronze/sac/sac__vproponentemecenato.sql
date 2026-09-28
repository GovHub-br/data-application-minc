-- Bronze SALIC — sac__vproponentemecenato.
-- Origem: salic_bronze.sac__vproponentemecenato, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 8 colunas: 0 tipadas, 7 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_texto("cgccpf") }} as cgccpf,
    {{ bronze_texto("nome") }} as nome,
    {{ bronze_texto("responsavel") }} as responsavel,
    {{ bronze_texto("endereco") }} as endereco,
    {{ bronze_texto("cidade") }} as cidade,
    {{ bronze_texto("uf") }} as uf,
    {{ bronze_texto("cep") }} as cep,
    _fatia
from {{ source("bronze_sac", "sac__vproponentemecenato") }}
