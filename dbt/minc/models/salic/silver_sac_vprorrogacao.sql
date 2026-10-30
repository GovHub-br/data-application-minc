-- Silver SALIC — silver_sac_vprorrogacao (ex-sac__vprorrogacao).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_sac_vprorrogacao, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 10 colunas: 7 tipadas, 2 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_texto("anoprojeto") }} as anoprojeto,
    {{ bronze_inteiro("sequencial") }} as sequencial,
    {{ bronze_timestamp("dtaprovacao") }} as dtaprovacao,
    {{ bronze_timestamp("dtiniciocaptacao") }} as dtiniciocaptacao,
    {{ bronze_timestamp("dtfimcaptacao") }} as dtfimcaptacao,
    {{ bronze_texto("portariaaprovacao") }} as portariaaprovacao,
    {{ bronze_timestamp("dtportariaaprovacao") }} as dtportariaaprovacao,
    {{ bronze_timestamp("dtpublicacaoaprovacao") }} as dtpublicacaoaprovacao,
    {{ bronze_numerico("saldoacaptar") }} as saldoacaptar,
    _fatia
from {{ source("salic", "bronze_sac_vprorrogacao") }}
