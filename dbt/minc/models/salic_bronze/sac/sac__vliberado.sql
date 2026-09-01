-- Bronze SALIC — sac__vliberado.
-- Origem: salic_bronze.sac__vliberado, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 5 colunas: 1 tipadas, 3 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_texto("anoprojeto") }} as anoprojeto,
    {{ bronze_texto("sequencial") }} as sequencial,
    {{ bronze_timestamp("dtliberacao") }} as dtliberacao,
    {{ bronze_texto("permissao") }} as permissao,
    _fatia
from {{ source("bronze_sac", "sac__vliberado") }}
