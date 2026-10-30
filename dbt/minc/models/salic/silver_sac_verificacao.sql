-- Silver SALIC — silver_sac_verificacao (ex-sac__verificacao).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_sac_verificacao, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 5 colunas: 3 tipadas, 1 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idverificacao") }} as idverificacao,
    {{ bronze_inteiro("idtipo") }} as idtipo,
    {{ bronze_texto("descricao") }} as descricao,
    {{ bronze_booleano("stestado") }} as stestado,
    _fatia
from {{ source("salic", "bronze_sac_verificacao") }}
