-- Bronze SALIC — tabelas__vwrelservidorhpimpressao.
-- Origem: salic_bronze.tabelas__vwrelservidorhpimpressao, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 5 colunas: 0 tipadas, 4 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_texto("identificacao") }} as identificacao,
    {{ bronze_texto("nomefuncionario") }} as nomefuncionario,
    {{ bronze_texto("siglalotacao") }} as siglalotacao,
    {{ bronze_texto("descricaolotacao") }} as descricaolotacao,
    _fatia
from {{ source("bronze_tabelas", "tabelas__vwrelservidorhpimpressao") }}
