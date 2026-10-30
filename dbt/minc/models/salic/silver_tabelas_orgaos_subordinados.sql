-- Silver SALIC — silver_tabelas_orgaos_subordinados (ex-tabelas__orgaos_subordinados).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_tabelas_orgaos_subordinados, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 4 colunas: 3 tipadas, 0 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("sub_orgao") }} as sub_orgao,
    {{ bronze_inteiro("sub_superior") }} as sub_superior,
    {{ bronze_inteiro("sub_nivel") }} as sub_nivel,
    _fatia
from {{ source("salic", "bronze_tabelas_orgaos_subordinados") }}
