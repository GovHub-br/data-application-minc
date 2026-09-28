-- Bronze SALIC — sac__aspectosfisicosx.
-- Origem: salic_bronze.sac__aspectosfisicosx, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 9 colunas: 5 tipadas, 3 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_texto("anoprojeto") }} as anoprojeto,
    {{ bronze_texto("sequencial") }} as sequencial,
    {{ bronze_inteiro("campo_01") }} as campo_01,
    {{ bronze_inteiro("campo_02") }} as campo_02,
    {{ bronze_inteiro("campo_03") }} as campo_03,
    {{ bronze_inteiro("campo_04") }} as campo_04,
    {{ bronze_texto("justificativas") }} as justificativas,
    {{ bronze_inteiro("logon") }} as logon,
    _fatia
from {{ source("bronze_sac", "sac__aspectosfisicosx") }}
