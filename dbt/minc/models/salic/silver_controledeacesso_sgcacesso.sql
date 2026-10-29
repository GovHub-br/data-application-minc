-- Silver SALIC — silver_controledeacesso_sgcacesso (ex-controledeacesso__sgcacesso).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_controledeacesso_sgcacesso, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 9 colunas: 5 tipadas, 3 mantidas como texto. A senha da Bronze não entra:
-- credencial não vai para camada analítica.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idusuario") }} as idusuario,
    {{ bronze_texto("cpf") }} as cpf,
    {{ bronze_texto("nome") }} as nome,
    {{ bronze_timestamp("dtnascimento") }} as dtnascimento,
    {{ bronze_texto("email") }} as email,
    {{ bronze_timestamp("dtcadastro") }} as dtcadastro,
    {{ bronze_inteiro("situacao") }} as situacao,
    {{ bronze_timestamp("dtsituacao") }} as dtsituacao,
    _fatia
from {{ source("salic", "bronze_controledeacesso_sgcacesso") }}
