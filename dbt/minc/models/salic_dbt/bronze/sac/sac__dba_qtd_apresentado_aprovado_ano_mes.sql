-- Bronze SALIC — sac__dba_qtd_apresentado_aprovado_ano_mes.
-- Origem: salic_bronze.sac__dba_qtd_apresentado_aprovado_ano_mes, onde tudo chega como
-- texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 6 colunas: 3 tipadas, 2 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("ano") }} as ano,
    {{ bronze_inteiro("mes") }} as mes,
    {{ bronze_texto("quantidade_apresentacao") }} as quantidade_apresentacao,
    {{ bronze_inteiro("quantidade_aprovacao") }} as quantidade_aprovacao,
    {{ bronze_texto("quantidade_captacao") }} as quantidade_captacao,
    _fatia
from {{ source("bronze_sac", "sac__dba_qtd_apresentado_aprovado_ano_mes") }}
