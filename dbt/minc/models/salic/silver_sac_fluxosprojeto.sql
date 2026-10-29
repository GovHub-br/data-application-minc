-- Silver SALIC — silver_sac_fluxosprojeto (ex-sac__fluxosprojeto).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_sac_fluxosprojeto, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 7 colunas: 6 tipadas, 0 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("id") }} as id,
    {{ bronze_inteiro("idpronac") }} as idpronac,
    {{ bronze_inteiro("estadoid") }} as estadoid,
    {{ bronze_inteiro("orgao") }} as orgao,
    {{ bronze_inteiro("grupo") }} as grupo,
    {{ bronze_inteiro("idagente") }} as idagente,
    _fatia
from {{ source("salic", "bronze_sac_fluxosprojeto") }}
