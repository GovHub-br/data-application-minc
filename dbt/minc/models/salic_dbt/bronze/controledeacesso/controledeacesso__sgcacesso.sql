-- Bronze SALIC — controledeacesso__sgcacesso.
-- Origem: salic_bronze.controledeacesso__sgcacesso, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 10 colunas: 5 tipadas, 4 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idusuario") }} as idusuario,
    {{ bronze_texto("cpf") }} as cpf,
    {{ bronze_texto("nome") }} as nome,
    {{ bronze_timestamp("dtnascimento") }} as dtnascimento,
    {{ bronze_texto("email") }} as email,
    {{ bronze_texto("senha") }} as senha,
    {{ bronze_timestamp("dtcadastro") }} as dtcadastro,
    {{ bronze_inteiro("situacao") }} as situacao,
    {{ bronze_timestamp("dtsituacao") }} as dtsituacao,
    _fatia
from {{ source("bronze_controledeacesso", "controledeacesso__sgcacesso") }}
