-- Bronze SALIC — tabelas__orgaos_subordinados.
-- Origem: salic_bronze.tabelas__orgaos_subordinados, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 4 colunas: 3 tipadas, 0 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("sub_orgao") }} as sub_orgao,
    {{ bronze_inteiro("sub_superior") }} as sub_superior,
    {{ bronze_inteiro("sub_nivel") }} as sub_nivel,
    _fatia
from {{ source("bronze_tabelas", "tabelas__orgaos_subordinados") }}
