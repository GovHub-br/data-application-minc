-- Silver SALIC — silver_sac_vdadosconvenio (ex-sac__vdadosconvenio).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_sac_vdadosconvenio, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 9 colunas: 5 tipadas, 3 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_texto("anoprojeto") }} as anoprojeto,
    {{ bronze_texto("sequencial") }} as sequencial,
    {{ bronze_timestamp("dtconvenio") }} as dtconvenio,
    {{ bronze_texto("nrconvenio") }} as nrconvenio,
    {{ bronze_timestamp("dtiniciovigencia") }} as dtiniciovigencia,
    {{ bronze_timestamp("dtfimvigencia") }} as dtfimvigencia,
    {{ bronze_numerico("vlconvenio") }} as vlconvenio,
    {{ bronze_inteiro("contador") }} as contador,
    _fatia
from {{ source("salic", "bronze_sac_vdadosconvenio") }}
