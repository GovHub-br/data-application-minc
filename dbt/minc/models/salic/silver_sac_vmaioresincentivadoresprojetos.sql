-- Silver SALIC — silver_sac_vmaioresincentivadoresprojetos (ex-sac__vmaioresincentivadoresprojetos).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_sac_vmaioresincentivadoresprojetos, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 6 colunas: 3 tipadas, 2 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("ano") }} as ano,
    {{ bronze_texto("incentivador") }} as incentivador,
    {{ bronze_texto("anoprojeto") }} as anoprojeto,
    {{ bronze_inteiro("sequencial") }} as sequencial,
    {{ bronze_numerico("valor") }} as valor,
    _fatia
from {{ source("salic", "bronze_sac_vmaioresincentivadoresprojetos") }}
