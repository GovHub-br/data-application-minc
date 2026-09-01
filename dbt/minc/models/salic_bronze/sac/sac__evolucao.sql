-- Bronze SALIC — sac__evolucao.
-- Origem: salic_bronze.sac__evolucao, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 8 colunas: 4 tipadas, 3 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_texto("anoprojeto") }} as anoprojeto,
    {{ bronze_texto("sequencial") }} as sequencial,
    {{ bronze_inteiro("fase") }} as fase,
    {{ bronze_timestamp("dtinicio") }} as dtinicio,
    {{ bronze_timestamp("dttermino") }} as dttermino,
    {{ bronze_texto("infrelevantes") }} as infrelevantes,
    {{ bronze_inteiro("logon") }} as logon,
    _fatia
from {{ source("bronze_sac", "sac__evolucao") }}
