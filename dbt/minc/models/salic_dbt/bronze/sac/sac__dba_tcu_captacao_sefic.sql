-- Bronze SALIC — sac__dba_tcu_captacao_sefic.
-- Origem: salic_bronze.sac__dba_tcu_captacao_sefic, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 4 colunas: 3 tipadas, 0 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("ano") }} as ano,
    {{ bronze_inteiro("qtd") }} as qtd,
    {{ bronze_numerico('"valor captado sefic"') }} as valor_captado_sefic,
    _fatia
from {{ source("bronze_sac", "sac__dba_tcu_captacao_sefic") }}
