-- Silver SALIC — silver_tabelas_avisos (ex-tabelas__avisos).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_tabelas_avisos, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 7 colunas: 5 tipadas, 1 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("avi_destino") }} as avi_destino,
    {{ bronze_inteiro("avi_codigo") }} as avi_codigo,
    {{ bronze_inteiro("avi_tipo") }} as avi_tipo,
    {{ bronze_timestamp("avi_data_inicio") }} as avi_data_inicio,
    {{ bronze_timestamp("avi_data_limite") }} as avi_data_limite,
    {{ bronze_texto("avi_texto") }} as avi_texto,
    _fatia
from {{ source("salic", "bronze_tabelas_avisos") }}
