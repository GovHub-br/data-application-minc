-- Silver SALIC — silver_sac_vwmemoriadecalculo (ex-sac__vwmemoriadecalculo).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_sac_vwmemoriadecalculo, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 9 colunas: 2 tipadas, 6 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idpronac") }} as idpronac,
    {{ bronze_texto("pronac") }} as pronac,
    {{ bronze_texto("valorproposta") }} as valorproposta,
    {{ bronze_texto("outrasfontes") }} as outrasfontes,
    {{ bronze_numerico("valorsolicitado") }} as valorsolicitado,
    {{ bronze_texto("elaboracao") }} as elaboracao,
    {{ bronze_texto("valorsugerido") }} as valorsugerido,
    {{ bronze_texto("valorparecer") }} as valorparecer,
    _fatia
from {{ source("salic", "bronze_sac_vwmemoriadecalculo") }}
