-- Bronze SALIC — tabelas__pessoa_identificacoes_bkp.
-- Origem: salic_bronze.tabelas__pessoa_identificacoes_bkp, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 5 colunas: 3 tipadas, 1 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("pid_pessoa") }} as pid_pessoa,
    {{ bronze_inteiro("pid_meta_dado") }} as pid_meta_dado,
    {{ bronze_inteiro("pid_sequencia") }} as pid_sequencia,
    {{ bronze_texto("pid_identificacao") }} as pid_identificacao,
    _fatia
from {{ source("bronze_tabelas", "tabelas__pessoa_identificacoes_bkp") }}
