-- Silver SALIC — silver_sac_tbhistoricodiligencia (ex-sac__tbhistoricodiligencia).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_sac_tbhistoricodiligencia, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 8 colunas: 5 tipadas, 2 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idhistoricodiligencia") }} as idhistoricodiligencia,
    {{ bronze_inteiro("iddiligencia") }} as iddiligencia,
    {{ bronze_timestamp("dtsolicitacao") }} as dtsolicitacao,
    {{ bronze_texto("solicitacao") }} as solicitacao,
    {{ bronze_timestamp("dtresposta") }} as dtresposta,
    {{ bronze_texto("resposta") }} as resposta,
    {{ bronze_inteiro("idusuario") }} as idusuario,
    _fatia
from {{ source("salic", "bronze_sac_tbhistoricodiligencia") }}
