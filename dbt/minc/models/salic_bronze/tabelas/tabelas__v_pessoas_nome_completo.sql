-- Bronze SALIC — tabelas__v_pessoas_nome_completo.
-- Origem: salic_bronze.tabelas__v_pessoas_nome_completo, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 7 colunas: 1 tipadas, 5 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("pes_codigo") }} as pes_codigo,
    {{ bronze_texto("pes_tipo") }} as pes_tipo,
    {{ bronze_texto("pes_superior") }} as pes_superior,
    {{ bronze_texto("pes_validade") }} as pes_validade,
    {{ bronze_texto("pes_nome_completo") }} as pes_nome_completo,
    {{ bronze_texto("pes_nome_superior") }} as pes_nome_superior,
    _fatia
from {{ source("bronze_tabelas", "tabelas__v_pessoas_nome_completo") }}
