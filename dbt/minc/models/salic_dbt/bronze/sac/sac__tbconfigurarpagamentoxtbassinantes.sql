-- Bronze SALIC — sac__tbconfigurarpagamentoxtbassinantes.
-- Origem: salic_bronze.sac__tbconfigurarpagamentoxtbassinantes, onde tudo chega como
-- texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 4 colunas: 3 tipadas, 0 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idconfigurarpagamento") }} as idconfigurarpagamento,
    {{ bronze_inteiro("idassinantes") }} as idassinantes,
    {{ bronze_inteiro("nrordenacao") }} as nrordenacao,
    _fatia
from {{ source("bronze_sac", "sac__tbconfigurarpagamentoxtbassinantes") }}
