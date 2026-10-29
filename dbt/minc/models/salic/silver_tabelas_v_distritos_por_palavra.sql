-- Silver SALIC — silver_tabelas_v_distritos_por_palavra (ex-tabelas__v_distritos_por_palavra).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_tabelas_v_distritos_por_palavra, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 6 colunas: 2 tipadas, 3 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_texto("loc_nome") }} as loc_nome,
    {{ bronze_texto("loc_estruturado") }} as loc_estruturado,
    {{ bronze_inteiro("loc_tipo") }} as loc_tipo,
    {{ bronze_inteiro("loc_codigo") }} as loc_codigo,
    {{ bronze_texto("pal_texto") }} as pal_texto,
    _fatia
from {{ source("salic", "bronze_tabelas_v_distritos_por_palavra") }}
