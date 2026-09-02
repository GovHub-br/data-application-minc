-- Bronze SALIC — sac__aberturadecontabancaria.
-- Origem: salic_bronze.sac__aberturadecontabancaria, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 11 colunas: 8 tipadas, 2 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_texto("anoprojeto") }} as anoprojeto,
    {{ bronze_texto("sequencial") }} as sequencial,
    {{ bronze_booleano("contabloqueada") }} as contabloqueada,
    {{ bronze_timestamp("dtcontabloqueada") }} as dtcontabloqueada,
    {{ bronze_booleano("contalivre") }} as contalivre,
    {{ bronze_timestamp("dtcontalivre") }} as dtcontalivre,
    {{ bronze_timestamp("dtnascimento") }} as dtnascimento,
    {{ bronze_inteiro("orgao") }} as orgao,
    {{ bronze_inteiro("logon") }} as logon,
    {{ bronze_inteiro("idaberturadeconta") }} as idaberturadeconta,
    _fatia
from {{ source("bronze_sac", "sac__aberturadecontabancaria") }}
