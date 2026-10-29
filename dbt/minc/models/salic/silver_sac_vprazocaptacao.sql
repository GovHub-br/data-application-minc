-- Silver SALIC — silver_sac_vprazocaptacao (ex-sac__vprazocaptacao).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_sac_vprazocaptacao, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 5 colunas: 3 tipadas, 1 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_texto("anoprojeto") }} as anoprojeto,
    {{ bronze_inteiro("sequencial") }} as sequencial,
    {{ bronze_timestamp("dtiniciocaptacao") }} as dtiniciocaptacao,
    {{ bronze_timestamp("dtfimcaptacao") }} as dtfimcaptacao,
    _fatia
from {{ source("salic", "bronze_sac_vprazocaptacao") }}
