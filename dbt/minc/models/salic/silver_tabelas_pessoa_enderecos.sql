-- Silver SALIC — silver_tabelas_pessoa_enderecos (ex-tabelas__pessoa_enderecos).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_tabelas_pessoa_enderecos, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 7 colunas: 2 tipadas, 4 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("pen_pessoa") }} as pen_pessoa,
    {{ bronze_inteiro("pen_tipo") }} as pen_tipo,
    {{ bronze_texto("pen_endereco") }} as pen_endereco,
    {{ bronze_texto("pen_bairro") }} as pen_bairro,
    {{ bronze_texto("pen_cep") }} as pen_cep,
    {{ bronze_texto("pen_localidade") }} as pen_localidade,
    _fatia
from {{ source("salic", "bronze_tabelas_pessoa_enderecos") }}
