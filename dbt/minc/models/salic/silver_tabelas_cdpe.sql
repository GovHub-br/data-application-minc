-- Silver SALIC — silver_tabelas_cdpe (ex-tabelas__cdpe).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_tabelas_cdpe, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 6 colunas: 0 tipadas, 5 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_texto("campo2") }} as campo2,
    {{ bronze_texto("campo3") }} as campo3,
    {{ bronze_texto("campo4") }} as campo4,
    {{ bronze_texto("campo5") }} as campo5,
    {{ bronze_texto("campo6") }} as campo6,
    _fatia
from {{ source("salic", "bronze_tabelas_cdpe") }}
