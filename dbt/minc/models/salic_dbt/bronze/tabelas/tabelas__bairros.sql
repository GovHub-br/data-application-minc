-- Bronze SALIC — tabelas__bairros.
-- Origem: salic_bronze.tabelas__bairros, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 5 colunas: 3 tipadas, 1 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("bai_codigo") }} as bai_codigo,
    {{ bronze_inteiro("bai_localidade") }} as bai_localidade,
    {{ bronze_texto("bai_nome") }} as bai_nome,
    {{ bronze_inteiro("bai_status") }} as bai_status,
    _fatia
from {{ source("bronze_tabelas", "tabelas__bairros") }}
