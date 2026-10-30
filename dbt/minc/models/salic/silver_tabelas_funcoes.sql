-- Silver SALIC — silver_tabelas_funcoes (ex-tabelas__funcoes).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_tabelas_funcoes, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 4 colunas: 2 tipadas, 1 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("fun_codigo") }} as fun_codigo,
    {{ bronze_texto("fun_descricao") }} as fun_descricao,
    {{ bronze_inteiro("fun_status") }} as fun_status,
    _fatia
from {{ source("salic", "bronze_tabelas_funcoes") }}
