-- Silver SALIC — silver_tabelas_categorias_pessoa (ex-tabelas__categorias_pessoa).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_tabelas_categorias_pessoa, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 5 colunas: 3 tipadas, 1 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("ctp_codigo") }} as ctp_codigo,
    {{ bronze_texto("ctp_descricao") }} as ctp_descricao,
    {{ bronze_inteiro("ctp_orgao_gerente") }} as ctp_orgao_gerente,
    {{ bronze_inteiro("ctp_status") }} as ctp_status,
    _fatia
from {{ source("salic", "bronze_tabelas_categorias_pessoa") }}
