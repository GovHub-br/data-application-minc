-- Silver SALIC — silver_sac_tbcovid (ex-sac__tbcovid).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_sac_tbcovid, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 8 colunas: 4 tipadas, 3 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idcovid") }} as idcovid,
    {{ bronze_inteiro("idpronac") }} as idpronac,
    {{ bronze_texto("tpprocedimento") }} as tpprocedimento,
    {{ bronze_timestamp("dtprocedimento") }} as dtprocedimento,
    {{ bronze_texto("dsmotivacao") }} as dsmotivacao,
    {{ bronze_texto("dsalcancedaacao") }} as dsalcancedaacao,
    {{ bronze_inteiro("idusuario") }} as idusuario,
    _fatia
from {{ source("salic", "bronze_sac_tbcovid") }}
