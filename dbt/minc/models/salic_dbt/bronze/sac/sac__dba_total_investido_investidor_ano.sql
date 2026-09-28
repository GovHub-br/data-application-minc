-- Bronze SALIC — sac__dba_total_investido_investidor_ano.
-- Origem: salic_bronze.sac__dba_total_investido_investidor_ano, onde tudo chega como
-- texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 4 colunas: 3 tipadas, 0 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("cnpj_cpf", tipo="bigint") }} as cnpj_cpf,
    {{ bronze_inteiro("ano_investimento") }} as ano_investimento,
    {{ bronze_numerico("valor_investido_total") }} as valor_investido_total,
    _fatia
from {{ source("bronze_sac", "sac__dba_total_investido_investidor_ano") }}
