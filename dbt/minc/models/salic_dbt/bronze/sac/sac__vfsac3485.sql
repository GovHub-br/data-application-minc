-- Bronze SALIC — sac__vfsac3485.
-- Origem: salic_bronze.sac__vfsac3485, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 7 colunas: 0 tipadas, 6 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_texto("proponente") }} as proponente,
    {{ bronze_texto("endereco") }} as endereco,
    {{ bronze_texto("cidade") }} as cidade,
    {{ bronze_texto("uf") }} as uf,
    {{ bronze_texto("cep") }} as cep,
    {{ bronze_texto("nomeprojeto") }} as nomeprojeto,
    _fatia
from {{ source("bronze_sac", "sac__vfsac3485") }}
