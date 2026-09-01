-- Bronze SALIC — sac__verificacaopecaxveiculo.
-- Origem: salic_bronze.sac__verificacaopecaxveiculo, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 4 colunas: 3 tipadas, 0 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idverificacaopecaxveiculo") }} as idverificacaopecaxveiculo,
    {{ bronze_inteiro("idverificacaopeca") }} as idverificacaopeca,
    {{ bronze_inteiro("idverificacaoveiculo") }} as idverificacaoveiculo,
    _fatia
from {{ source("bronze_sac", "sac__verificacaopecaxveiculo") }}
