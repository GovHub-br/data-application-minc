-- Bronze SALIC — tabelas__eventos.
-- Origem: salic_bronze.tabelas__eventos, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 8 colunas: 4 tipadas, 3 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_timestamp("eve_datahora") }} as eve_datahora,
    {{ bronze_texto("eve_usuario") }} as eve_usuario,
    {{ bronze_inteiro("eve_operacao") }} as eve_operacao,
    {{ bronze_inteiro("eve_codigo") }} as eve_codigo,
    {{ bronze_texto("eve_parametros") }} as eve_parametros,
    {{ bronze_inteiro("eve_resultado") }} as eve_resultado,
    {{ bronze_texto("eve_texto") }} as eve_texto,
    _fatia
from {{ source("bronze_tabelas", "tabelas__eventos") }}
