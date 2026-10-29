-- Silver SALIC — silver_sac_captacaoguia (ex-sac__captacaoguia).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_sac_captacaoguia, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 8 colunas: 4 tipadas, 3 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_texto("anoprojeto") }} as anoprojeto,
    {{ bronze_texto("sequencial") }} as sequencial,
    {{ bronze_inteiro("numeroguia") }} as numeroguia,
    {{ bronze_timestamp("dtcaptacao") }} as dtcaptacao,
    {{ bronze_texto("observacao") }} as observacao,
    {{ bronze_numerico("captacaoreal") }} as captacaoreal,
    {{ bronze_inteiro("logon") }} as logon,
    _fatia
from {{ source("salic", "bronze_sac_captacaoguia") }}
