-- Bronze SALIC — sac__tbhistoricodevolucaofiscalizacao.
-- Origem: salic_bronze.sac__tbhistoricodevolucaofiscalizacao, onde tudo chega como
-- texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 6 colunas: 3 tipadas, 2 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idhistoricodevolucao") }} as idhistoricodevolucao,
    {{ bronze_inteiro("idrelatoriofiscalizacao") }} as idrelatoriofiscalizacao,
    {{ bronze_texto("dsjustificativadevolucao") }} as dsjustificativadevolucao,
    {{ bronze_timestamp("dtenviodevolucao") }} as dtenviodevolucao,
    {{ bronze_texto("stdevolucao") }} as stdevolucao,
    _fatia
from {{ source("bronze_sac", "sac__tbhistoricodevolucaofiscalizacao") }}
