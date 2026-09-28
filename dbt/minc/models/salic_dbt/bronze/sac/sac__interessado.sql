-- Bronze SALIC — sac__interessado.
-- Origem: salic_bronze.sac__interessado, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 22 colunas: 3 tipadas, 18 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_texto("cgccpf") }} as cgccpf,
    {{ bronze_texto("nome") }} as nome,
    {{ bronze_texto("tipopessoa") }} as tipopessoa,
    {{ bronze_texto("endereco") }} as endereco,
    {{ bronze_texto("cidade") }} as cidade,
    {{ bronze_texto("uf") }} as uf,
    {{ bronze_texto("cep") }} as cep,
    {{ bronze_texto("telefoneresidencial") }} as telefoneresidencial,
    {{ bronze_texto("telefonecomercial") }} as telefonecomercial,
    {{ bronze_texto("telefonecelular") }} as telefonecelular,
    {{ bronze_texto("telefonefax") }} as telefonefax,
    {{ bronze_texto("natureza") }} as natureza,
    {{ bronze_texto("esfera") }} as esfera,
    {{ bronze_texto("administracao") }} as administracao,
    {{ bronze_texto("utilidade") }} as utilidade,
    {{ bronze_texto("responsavel") }} as responsavel,
    {{ bronze_texto("enderecointernet") }} as enderecointernet,
    {{ bronze_texto("correioeletronico") }} as correioeletronico,
    {{ bronze_inteiro("grupo") }} as grupo,
    {{ bronze_inteiro("loc_codigo") }} as loc_codigo,
    {{ bronze_inteiro("logon") }} as logon,
    _fatia
from {{ source("bronze_sac", "sac__interessado") }}
