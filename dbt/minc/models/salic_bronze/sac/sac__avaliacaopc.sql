-- Bronze SALIC — sac__avaliacaopc.
-- Origem: salic_bronze.sac__avaliacaopc, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 39 colunas: 34 tipadas, 4 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_texto("anoprojeto") }} as anoprojeto,
    {{ bronze_texto("sequencial") }} as sequencial,
    {{ bronze_booleano("prestacao") }} as prestacao,
    {{ bronze_inteiro("analista") }} as analista,
    {{ bronze_timestamp("dtanalise") }} as dtanalise,
    {{ bronze_inteiro("campo_0") }} as campo_0,
    {{ bronze_inteiro("campo_01") }} as campo_01,
    {{ bronze_inteiro("campo_02") }} as campo_02,
    {{ bronze_inteiro("campo_03") }} as campo_03,
    {{ bronze_inteiro("campo_04") }} as campo_04,
    {{ bronze_inteiro("campo_05") }} as campo_05,
    {{ bronze_inteiro("campo_06") }} as campo_06,
    {{ bronze_inteiro("campo_07") }} as campo_07,
    {{ bronze_inteiro("campo_08") }} as campo_08,
    {{ bronze_inteiro("campo_09") }} as campo_09,
    {{ bronze_inteiro("campo_010") }} as campo_010,
    {{ bronze_inteiro("campo_011") }} as campo_011,
    {{ bronze_inteiro("campo_012") }} as campo_012,
    {{ bronze_inteiro("campo_013") }} as campo_013,
    {{ bronze_inteiro("campo_014") }} as campo_014,
    {{ bronze_inteiro("campo_015") }} as campo_015,
    {{ bronze_inteiro("campo_016") }} as campo_016,
    {{ bronze_inteiro("campo_017") }} as campo_017,
    {{ bronze_inteiro("campo_018") }} as campo_018,
    {{ bronze_inteiro("campo_019") }} as campo_019,
    {{ bronze_inteiro("campo_020") }} as campo_020,
    {{ bronze_inteiro("campo_021") }} as campo_021,
    {{ bronze_inteiro("campo_022") }} as campo_022,
    {{ bronze_inteiro("campo_023") }} as campo_023,
    {{ bronze_inteiro("campo_024") }} as campo_024,
    {{ bronze_inteiro("campo_025") }} as campo_025,
    {{ bronze_inteiro("campo_026") }} as campo_026,
    {{ bronze_inteiro("resultado") }} as resultado,
    {{ bronze_inteiro("manifestacao") }} as manifestacao,
    {{ bronze_inteiro("pronunciamento") }} as pronunciamento,
    {{ bronze_texto("justificativa") }} as justificativa,
    {{ bronze_texto("providencia") }} as providencia,
    {{ bronze_inteiro("logon") }} as logon,
    _fatia
from {{ source("bronze_sac", "sac__avaliacaopc") }}
