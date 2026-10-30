-- Silver SALIC — silver_sac_tbdespacho (ex-sac__tbdespacho).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_sac_tbdespacho, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 11 colunas: 7 tipadas, 3 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("iddespacho") }} as iddespacho,
    {{ bronze_inteiro("idpronac") }} as idpronac,
    {{ bronze_inteiro("idproposta") }} as idproposta,
    {{ bronze_inteiro("tipo") }} as tipo,
    {{ bronze_timestamp("data") }} as data,
    {{ bronze_texto("despacho") }} as despacho,
    {{ bronze_booleano("stestado") }} as stestado,
    {{ bronze_inteiro("idusuario") }} as idusuario,
    {{ bronze_texto("stencaminhamento") }} as stencaminhamento,
    {{ bronze_texto("sidespacho") }} as sidespacho,
    _fatia
from {{ source("salic", "bronze_sac_tbdespacho") }}
