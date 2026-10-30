-- Silver SALIC — silver_sac_etiqueta (ex-sac__etiqueta).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_sac_etiqueta, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 4 colunas: 1 tipadas, 2 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_texto("ano") }} as ano,
    {{ bronze_texto("mecanismo") }} as mecanismo,
    {{ bronze_inteiro("sequencial") }} as sequencial,
    _fatia
from {{ source("salic", "bronze_sac_etiqueta") }}
