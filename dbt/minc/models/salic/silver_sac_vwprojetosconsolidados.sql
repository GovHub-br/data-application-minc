-- Silver SALIC — silver_sac_vwprojetosconsolidados (ex-sac__vwprojetosconsolidados).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_sac_vwprojetosconsolidados, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 11 colunas: 0 tipadas, 10 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_texto("pronac") }} as pronac,
    {{ bronze_texto("nomeprojeto") }} as nomeprojeto,
    {{ bronze_texto("valorproposta") }} as valorproposta,
    {{ bronze_texto("outrasfontes") }} as outrasfontes,
    {{ bronze_texto("valorsolicitado") }} as valorsolicitado,
    {{ bronze_texto("valorsugerido") }} as valorsugerido,
    {{ bronze_texto("elaboracao") }} as elaboracao,
    {{ bronze_texto("valorparecer") }} as valorparecer,
    {{ bronze_texto("perc") }} as perc,
    {{ bronze_texto("acima") }} as acima,
    _fatia
from {{ source("salic", "bronze_sac_vwprojetosconsolidados") }}
