-- Bronze SALIC — tabelas__pessoa_enderecos.
-- Origem: salic_bronze.tabelas__pessoa_enderecos, onde tudo chega como texto da
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
from {{ source("bronze_tabelas", "tabelas__pessoa_enderecos") }}
