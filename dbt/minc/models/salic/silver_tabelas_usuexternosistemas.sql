-- Silver SALIC — silver_tabelas_usuexternosistemas (ex-tabelas__usuexternosistemas).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_tabelas_usuexternosistemas, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 6 colunas: 1 tipadas, 4 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_texto("usuidentificacao") }} as usuidentificacao,
    {{ bronze_texto("usunome") }} as usunome,
    {{ bronze_texto("ususenha") }} as ususenha,
    {{ bronze_texto("usuobs") }} as usuobs,
    {{ bronze_inteiro("usustatus") }} as usustatus,
    _fatia
from {{ source("salic", "bronze_tabelas_usuexternosistemas") }}
