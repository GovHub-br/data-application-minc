-- Silver SALIC — silver_sac_vnegativainabilitado (ex-sac__vnegativainabilitado).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_sac_vnegativainabilitado, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 13 colunas: 6 tipadas, 6 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("horgao") }} as horgao,
    {{ bronze_inteiro("hnumerocarta") }} as hnumerocarta,
    {{ bronze_timestamp("hdtcarta") }} as hdtcarta,
    {{ bronze_texto("hanoprojeto") }} as hanoprojeto,
    {{ bronze_texto("hsequencial") }} as hsequencial,
    {{ bronze_texto("pnomeprojeto") }} as pnomeprojeto,
    {{ bronze_inteiro("parea") }} as parea,
    {{ bronze_texto("pprocesso") }} as pprocesso,
    {{ bronze_texto("ianoprojeto") }} as ianoprojeto,
    {{ bronze_texto("isequencial") }} as isequencial,
    {{ bronze_inteiro("iorgao") }} as iorgao,
    {{ bronze_inteiro("hlogon") }} as hlogon,
    _fatia
from {{ source("salic", "bronze_sac_vnegativainabilitado") }}
