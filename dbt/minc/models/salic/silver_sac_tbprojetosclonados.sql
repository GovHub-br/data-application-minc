-- Silver SALIC — silver_sac_tbprojetosclonados (ex-sac__tbprojetosclonados).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_sac_tbprojetosclonados, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 14 colunas: 7 tipadas, 6 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idprojetosclonados") }} as idprojetosclonados,
    {{ bronze_inteiro("idpropostaclonada") }} as idpropostaclonada,
    {{ bronze_timestamp("dtclonagrem") }} as dtclonagrem,
    {{ bronze_inteiro("idusuarioclonador") }} as idusuarioclonador,
    {{ bronze_inteiro("idpronacoriginal") }} as idpronacoriginal,
    {{ bronze_inteiro("idpropostaoriginal") }} as idpropostaoriginal,
    {{ bronze_texto("nrpronac") }} as nrpronac,
    {{ bronze_texto("nrcnpjcpforiginal") }} as nrcnpjcpforiginal,
    {{ bronze_texto("cdsituacaoprojetooriginal") }} as cdsituacaoprojetooriginal,
    {{ bronze_texto("nrprocessooriginal") }} as nrprocessooriginal,
    {{ bronze_texto("cdareaoriginal") }} as cdareaoriginal,
    {{ bronze_texto("cdsegmentooriginal") }} as cdsegmentooriginal,
    {{ bronze_numerico("vlsolicitadooriginal") }} as vlsolicitadooriginal,
    _fatia
from {{ source("salic", "bronze_sac_tbprojetosclonados") }}
