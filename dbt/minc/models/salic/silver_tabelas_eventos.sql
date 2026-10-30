-- Silver SALIC — silver_tabelas_eventos (ex-tabelas__eventos).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_tabelas_eventos, onde tudo chega como texto da
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
from {{ source("salic", "bronze_tabelas_eventos") }}
