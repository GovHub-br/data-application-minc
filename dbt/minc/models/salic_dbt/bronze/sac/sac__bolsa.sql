-- Bronze SALIC — sac__bolsa.
-- Origem: salic_bronze.sac__bolsa, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 19 colunas: 3 tipadas, 15 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_texto("anoprojeto") }} as anoprojeto,
    {{ bronze_texto("sequencial") }} as sequencial,
    {{ bronze_texto("pais") }} as pais,
    {{ bronze_texto("cidade") }} as cidade,
    {{ bronze_texto("endereco") }} as endereco,
    {{ bronze_texto("cep") }} as cep,
    {{ bronze_texto("telefone") }} as telefone,
    {{ bronze_texto("instituicao") }} as instituicao,
    {{ bronze_texto("instcidade") }} as instcidade,
    {{ bronze_texto("instendereco") }} as instendereco,
    {{ bronze_texto("instcep") }} as instcep,
    {{ bronze_texto("insttelefone") }} as insttelefone,
    {{ bronze_texto("instemail") }} as instemail,
    {{ bronze_texto("orientador") }} as orientador,
    {{ bronze_timestamp("dtinicioajuda") }} as dtinicioajuda,
    {{ bronze_timestamp("dtfimajuda") }} as dtfimajuda,
    {{ bronze_texto("portfolio") }} as portfolio,
    {{ bronze_inteiro("logon") }} as logon,
    _fatia
from {{ source("bronze_sac", "sac__bolsa") }}
