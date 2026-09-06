-- Bronze SALIC — tabelas__sistemas_ares.
-- Origem: salic_bronze.tabelas__sistemas_ares, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 8 colunas: 2 tipadas, 5 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("sis_codigo") }} as sis_codigo,
    {{ bronze_texto("sis_sigla") }} as sis_sigla,
    {{ bronze_texto("sis_nome") }} as sis_nome,
    {{ bronze_inteiro("sis_status") }} as sis_status,
    {{ bronze_texto("sis_seguranca") }} as sis_seguranca,
    {{ bronze_texto("sis_url") }} as sis_url,
    {{ bronze_texto("sis_controle") }} as sis_controle,
    _fatia
from {{ source("bronze_tabelas", "tabelas__sistemas_ares") }}
