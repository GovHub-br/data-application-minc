-- Bronze SALIC — sac__convenio.
-- Origem: salic_bronze.sac__convenio, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 16 colunas: 11 tipadas, 4 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("contador") }} as contador,
    {{ bronze_texto("anoprojeto") }} as anoprojeto,
    {{ bronze_texto("sequencial") }} as sequencial,
    {{ bronze_inteiro("opcao") }} as opcao,
    {{ bronze_texto("numeroconvenio") }} as numeroconvenio,
    {{ bronze_timestamp("dtconvenio") }} as dtconvenio,
    {{ bronze_timestamp("dtpublicacao") }} as dtpublicacao,
    {{ bronze_timestamp("dtinicioexecucao") }} as dtinicioexecucao,
    {{ bronze_timestamp("dtfinalexecucao") }} as dtfinalexecucao,
    {{ bronze_timestamp("dtiniciovigencia") }} as dtiniciovigencia,
    {{ bronze_timestamp("dtfinalvigencia") }} as dtfinalvigencia,
    {{ bronze_texto("objeto") }} as objeto,
    {{ bronze_numerico("valorconvenio") }} as valorconvenio,
    {{ bronze_inteiro("logon") }} as logon,
    {{ bronze_inteiro("idprojeto") }} as idprojeto,
    _fatia
from {{ source("bronze_sac", "sac__convenio") }}
