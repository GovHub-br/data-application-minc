-- Bronze SALIC — sac__vnegativainabilitadoetiqueta.
-- Origem: salic_bronze.sac__vnegativainabilitadoetiqueta, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 9 colunas: 4 tipadas, 4 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("horgao") }} as horgao,
    {{ bronze_inteiro("hnumerocarta") }} as hnumerocarta,
    {{ bronze_timestamp("hdtcarta") }} as hdtcarta,
    {{ bronze_texto("hanoprojeto") }} as hanoprojeto,
    {{ bronze_texto("hsequencial") }} as hsequencial,
    {{ bronze_texto("pnomeprojeto") }} as pnomeprojeto,
    {{ bronze_texto("pcgccpf") }} as pcgccpf,
    {{ bronze_inteiro("hlogon") }} as hlogon,
    _fatia
from {{ source("bronze_sac", "sac__vnegativainabilitadoetiqueta") }}
