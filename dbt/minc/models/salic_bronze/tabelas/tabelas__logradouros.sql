-- Bronze SALIC — tabelas__logradouros.
-- Origem: salic_bronze.tabelas__logradouros, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 10 colunas: 6 tipadas, 3 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("log_codigo") }} as log_codigo,
    {{ bronze_inteiro("log_localidade") }} as log_localidade,
    {{ bronze_inteiro("log_tipo") }} as log_tipo,
    {{ bronze_texto("log_nome") }} as log_nome,
    {{ bronze_texto("log_limites") }} as log_limites,
    {{ bronze_inteiro("log_bairro") }} as log_bairro,
    {{ bronze_inteiro("log_bairro_final") }} as log_bairro_final,
    {{ bronze_texto("log_cep") }} as log_cep,
    {{ bronze_inteiro("log_status") }} as log_status,
    _fatia
from {{ source("bronze_tabelas", "tabelas__logradouros") }}
