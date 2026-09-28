-- Bronze SALIC — tabelas__indice_palavras.
-- Origem: salic_bronze.tabelas__indice_palavras, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 5 colunas: 4 tipadas, 0 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("ipl_palavra") }} as ipl_palavra,
    {{ bronze_inteiro("ipl_posicao") }} as ipl_posicao,
    {{ bronze_inteiro("ipl_tipo") }} as ipl_tipo,
    {{ bronze_inteiro("ipl_codigo") }} as ipl_codigo,
    _fatia
from {{ source("bronze_tabelas", "tabelas__indice_palavras") }}
