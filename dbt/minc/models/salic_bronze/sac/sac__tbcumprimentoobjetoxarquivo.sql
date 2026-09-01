-- Bronze SALIC — sac__tbcumprimentoobjetoxarquivo.
-- Origem: salic_bronze.sac__tbcumprimentoobjetoxarquivo, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 5 colunas: 4 tipadas, 0 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idcumprimentoobjetoxarquivo") }} as idcumprimentoobjetoxarquivo,
    {{ bronze_inteiro("idcumprimentoobjeto") }} as idcumprimentoobjeto,
    {{ bronze_inteiro("idarquivo") }} as idarquivo,
    {{ bronze_inteiro("idposicao") }} as idposicao,
    _fatia
from {{ source("bronze_sac", "sac__tbcumprimentoobjetoxarquivo") }}
