-- Silver SALIC — silver_sac_vmostraconvenio (ex-sac__vmostraconvenio).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_sac_vmostraconvenio, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 14 colunas: 1 tipadas, 12 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_texto("opcao") }} as opcao,
    {{ bronze_texto("nrconvenio") }} as nrconvenio,
    {{ bronze_texto("dtconvenio") }} as dtconvenio,
    {{ bronze_texto("dtpublicacao") }} as dtpublicacao,
    {{ bronze_texto("dtinicioexecucao") }} as dtinicioexecucao,
    {{ bronze_texto("dtfinalexecucao") }} as dtfinalexecucao,
    {{ bronze_texto("dtiniciovigencia") }} as dtiniciovigencia,
    {{ bronze_texto("dtfinalvigencia") }} as dtfinalvigencia,
    {{ bronze_numerico("valorconvenio") }} as valorconvenio,
    {{ bronze_texto("objeto") }} as objeto,
    {{ bronze_texto("usuario") }} as usuario,
    {{ bronze_texto("anoprojeto") }} as anoprojeto,
    {{ bronze_texto("sequencial") }} as sequencial,
    _fatia
from {{ source("salic", "bronze_sac_vmostraconvenio") }}
