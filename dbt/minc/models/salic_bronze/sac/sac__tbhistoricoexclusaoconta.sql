-- Bronze SALIC — sac__tbhistoricoexclusaoconta.
-- Origem: salic_bronze.sac__tbhistoricoexclusaoconta, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 10 colunas: 4 tipadas, 5 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idhistoricoexclusaoconta") }} as idhistoricoexclusaoconta,
    {{ bronze_inteiro("idcontabancaria") }} as idcontabancaria,
    {{ bronze_texto("banco") }} as banco,
    {{ bronze_texto("agencia") }} as agencia,
    {{ bronze_texto("contabloqueada") }} as contabloqueada,
    {{ bronze_texto("contalivre") }} as contalivre,
    {{ bronze_timestamp("dtexclusao") }} as dtexclusao,
    {{ bronze_texto("motivo") }} as motivo,
    {{ bronze_inteiro("idusuario") }} as idusuario,
    _fatia
from {{ source("bronze_sac", "sac__tbhistoricoexclusaoconta") }}
