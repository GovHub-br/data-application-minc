-- Silver SALIC — silver_agentes_vvisao (ex-agentes__vvisao).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_agentes_vvisao, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 5 colunas: 2 tipadas, 2 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idvisao") }} as idvisao,
    {{ bronze_inteiro("idagente") }} as idagente,
    {{ bronze_texto("visao") }} as visao,
    {{ bronze_texto("usuario") }} as usuario,
    _fatia
from {{ source("salic", "bronze_agentes_vvisao") }}
