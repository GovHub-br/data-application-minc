-- Bronze SALIC — sac__vguiasderecolhimento.
-- Origem: salic_bronze.sac__vguiasderecolhimento, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 12 colunas: 5 tipadas, 6 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_texto("vinculo") }} as vinculo,
    {{ bronze_inteiro("numeroguia") }} as numeroguia,
    {{ bronze_texto("cgccpf") }} as cgccpf,
    {{ bronze_texto("subscritor") }} as subscritor,
    {{ bronze_timestamp("dtrecolhimento") }} as dtrecolhimento,
    {{ bronze_timestamp("dtvencimento") }} as dtvencimento,
    {{ bronze_inteiro("dias") }} as dias,
    {{ bronze_texto("anoprojeto") }} as anoprojeto,
    {{ bronze_texto("sequencial") }} as sequencial,
    {{ bronze_texto("nomeprojeto") }} as nomeprojeto,
    {{ bronze_numerico("valor") }} as valor,
    _fatia
from {{ source("bronze_sac", "sac__vguiasderecolhimento") }}
