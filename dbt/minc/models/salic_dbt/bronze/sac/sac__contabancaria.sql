-- Bronze SALIC — sac__contabancaria.
-- Origem: salic_bronze.sac__contabancaria, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 17 colunas: 5 tipadas, 11 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idcontabancaria") }} as idcontabancaria,
    {{ bronze_texto("anoprojeto") }} as anoprojeto,
    {{ bronze_texto("sequencial") }} as sequencial,
    {{ bronze_texto("mecanismo") }} as mecanismo,
    {{ bronze_texto("banco") }} as banco,
    {{ bronze_texto("agencia") }} as agencia,
    {{ bronze_texto("contabloqueada") }} as contabloqueada,
    {{ bronze_timestamp("dtloteremessacb") }} as dtloteremessacb,
    {{ bronze_texto("loteremessacb") }} as loteremessacb,
    {{ bronze_texto("ocorrenciacb") }} as ocorrenciacb,
    {{ bronze_texto("contalivre") }} as contalivre,
    {{ bronze_timestamp("dtloteremessacl") }} as dtloteremessacl,
    {{ bronze_texto("loteremessacl") }} as loteremessacl,
    {{ bronze_texto("ocorrenciacl") }} as ocorrenciacl,
    {{ bronze_inteiro("logon") }} as logon,
    {{ bronze_inteiro("idpronac") }} as idpronac,
    _fatia
from {{ source("bronze_sac", "sac__contabancaria") }}
