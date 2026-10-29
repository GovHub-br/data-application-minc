-- Silver SALIC — silver_agentes_vvinculacao (ex-agentes__vvinculacao).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_agentes_vvinculacao, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 6 colunas: 5 tipadas, 0 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idvinculacao") }} as idvinculacao,
    {{ bronze_inteiro("idagente") }} as idagente,
    {{ bronze_inteiro("idvinculado") }} as idvinculado,
    {{ bronze_inteiro("idvinculoprincipal") }} as idvinculoprincipal,
    {{ bronze_inteiro("usuario") }} as usuario,
    _fatia
from {{ source("salic", "bronze_agentes_vvinculacao") }}
