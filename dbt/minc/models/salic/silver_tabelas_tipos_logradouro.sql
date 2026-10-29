-- Silver SALIC — silver_tabelas_tipos_logradouro (ex-tabelas__tipos_logradouro).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_tabelas_tipos_logradouro, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 4 colunas: 1 tipadas, 2 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("tlg_codigo") }} as tlg_codigo,
    {{ bronze_texto("tlg_sigla") }} as tlg_sigla,
    {{ bronze_texto("tlg_descricao") }} as tlg_descricao,
    _fatia
from {{ source("salic", "bronze_tabelas_tipos_logradouro") }}
