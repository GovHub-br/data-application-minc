-- Bronze SALIC — sac__informacaotrimestralx.
-- Origem: salic_bronze.sac__informacaotrimestralx, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 11 colunas: 7 tipadas, 3 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_texto("anoprojeto") }} as anoprojeto,
    {{ bronze_texto("sequencial") }} as sequencial,
    {{ bronze_inteiro("anorelatorio") }} as anorelatorio,
    {{ bronze_inteiro("fisicofinanceiro") }} as fisicofinanceiro,
    {{ bronze_booleano("trimestre1") }} as trimestre1,
    {{ bronze_booleano("trimestre2") }} as trimestre2,
    {{ bronze_booleano("trimestre3") }} as trimestre3,
    {{ bronze_booleano("trimestre4") }} as trimestre4,
    {{ bronze_texto("observacao") }} as observacao,
    {{ bronze_inteiro("logon") }} as logon,
    _fatia
from {{ source("bronze_sac", "sac__informacaotrimestralx") }}
