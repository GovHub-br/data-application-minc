-- Bronze SALIC — sac__cnic.
-- Origem: salic_bronze.sac__cnic, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 9 colunas: 5 tipadas, 3 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_texto("anoprojeto") }} as anoprojeto,
    {{ bronze_texto("sequencial") }} as sequencial,
    {{ bronze_inteiro("nrreuniao") }} as nrreuniao,
    {{ bronze_timestamp("dtreuniao") }} as dtreuniao,
    {{ bronze_inteiro("tipodepauta") }} as tipodepauta,
    {{ bronze_inteiro("resultadodaanalise") }} as resultadodaanalise,
    {{ bronze_texto("observacao") }} as observacao,
    {{ bronze_inteiro("logon") }} as logon,
    _fatia
from {{ source("bronze_sac", "sac__cnic") }}
