-- Silver SALIC — silver_sac_vci (ex-sac__vci).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_sac_vci, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 6 colunas: 1 tipadas, 4 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_texto("anoprojeto") }} as anoprojeto,
    {{ bronze_inteiro("sequencial") }} as sequencial,
    {{ bronze_texto("ci") }} as ci,
    {{ bronze_texto("orgaoexpedidor") }} as orgaoexpedidor,
    {{ bronze_texto("dtexpedicao") }} as dtexpedicao,
    _fatia
from {{ source("salic", "bronze_sac_vci") }}
