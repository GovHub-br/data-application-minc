-- Bronze SALIC — sac__vtodaaprovacao.
-- Origem: salic_bronze.sac__vtodaaprovacao, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 15 colunas: 7 tipadas, 7 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_texto("anoprojeto") }} as anoprojeto,
    {{ bronze_texto("sequencial") }} as sequencial,
    {{ bronze_timestamp("dtprotocolo") }} as dtprotocolo,
    {{ bronze_timestamp("dtaprovacao") }} as dtaprovacao,
    {{ bronze_inteiro("tipo") }} as tipo,
    {{ bronze_texto("portaria") }} as portaria,
    {{ bronze_timestamp("dtpublicacao") }} as dtpublicacao,
    {{ bronze_timestamp("dtiniciocaptacao") }} as dtiniciocaptacao,
    {{ bronze_timestamp("dtfimcaptacao") }} as dtfimcaptacao,
    {{ bronze_numerico("mecenato") }} as mecenato,
    {{ bronze_texto("artigo1") }} as artigo1,
    {{ bronze_texto("custeio") }} as custeio,
    {{ bronze_texto("artigo3") }} as artigo3,
    {{ bronze_texto("contrapartida") }} as contrapartida,
    _fatia
from {{ source("bronze_sac", "sac__vtodaaprovacao") }}
