-- Bronze SALIC — tabelas__pessoa_identificacoes_alteradas.
-- Origem: salic_bronze.tabelas__pessoa_identificacoes_alteradas, onde tudo chega como
-- texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 5 colunas: 2 tipadas, 2 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_timestamp("pia_datahora") }} as pia_datahora,
    {{ bronze_inteiro("pia_pessoa") }} as pia_pessoa,
    {{ bronze_texto("pia_anterior") }} as pia_anterior,
    {{ bronze_texto("pia_atual") }} as pia_atual,
    _fatia
from {{ source("bronze_tabelas", "tabelas__pessoa_identificacoes_alteradas") }}
