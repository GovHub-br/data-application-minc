-- Bronze SALIC — sac__historicounidade.
-- Origem: salic_bronze.sac__historicounidade, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 8 colunas: 4 tipadas, 3 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("contador") }} as contador,
    {{ bronze_texto("anoprojeto") }} as anoprojeto,
    {{ bronze_texto("sequencial") }} as sequencial,
    {{ bronze_timestamp("dtsaida") }} as dtsaida,
    {{ bronze_texto("unidadeanalise") }} as unidadeanalise,
    {{ bronze_timestamp("dtretorno") }} as dtretorno,
    {{ bronze_inteiro("logon") }} as logon,
    _fatia
from {{ source("bronze_sac", "sac__historicounidade") }}
