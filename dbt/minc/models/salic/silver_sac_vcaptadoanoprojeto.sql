-- Silver SALIC — silver_sac_vcaptadoanoprojeto (ex-sac__vcaptadoanoprojeto).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_sac_vcaptadoanoprojeto, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 5 colunas: 2 tipadas, 2 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("anorecibo") }} as anorecibo,
    {{ bronze_texto("anoprojeto") }} as anoprojeto,
    {{ bronze_texto("sequencial") }} as sequencial,
    {{ bronze_numerico("captacaoreal") }} as captacaoreal,
    _fatia
from {{ source("salic", "bronze_sac_vcaptadoanoprojeto") }}
