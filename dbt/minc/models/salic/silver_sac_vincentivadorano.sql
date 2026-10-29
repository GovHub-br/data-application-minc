-- Silver SALIC — silver_sac_vincentivadorano (ex-sac__vincentivadorano).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_sac_vincentivadorano, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 9 colunas: 3 tipadas, 5 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("ano") }} as ano,
    {{ bronze_texto("cgccpf") }} as cgccpf,
    {{ bronze_texto("mp") }} as mp,
    {{ bronze_texto("tipoapoio") }} as tipoapoio,
    {{ bronze_texto("anoprojeto") }} as anoprojeto,
    {{ bronze_texto("sequencial") }} as sequencial,
    {{ bronze_numerico("apoioufir") }} as apoioufir,
    {{ bronze_numerico("apoioreal") }} as apoioreal,
    _fatia
from {{ source("salic", "bronze_sac_vincentivadorano") }}
