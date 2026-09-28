-- Bronze SALIC — sac__vparecerinicial.
-- Origem: salic_bronze.sac__vparecerinicial, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 6 colunas: 2 tipadas, 3 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_texto("anoprojeto") }} as anoprojeto,
    {{ bronze_inteiro("sequencial") }} as sequencial,
    {{ bronze_texto("tipoparecer") }} as tipoparecer,
    {{ bronze_timestamp("dtparecer") }} as dtparecer,
    {{ bronze_texto("parecer") }} as parecer,
    _fatia
from {{ source("bronze_sac", "sac__vparecerinicial") }}
