-- Silver SALIC — silver_sac_tbhistoricodevolucaofiscalizacao (ex-sac__tbhistoricodevolucaofiscalizacao).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_sac_tbhistoricodevolucaofiscalizacao, onde tudo chega como
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
from {{ source("salic", "bronze_sac_tbhistoricodevolucaofiscalizacao") }}
