-- Silver SALIC — silver_sac_criteriosx (ex-sac__criteriosx).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_sac_criteriosx, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 10 colunas: 6 tipadas, 3 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idcriterios") }} as idcriterios,
    {{ bronze_texto("anoprojeto") }} as anoprojeto,
    {{ bronze_texto("sequencial") }} as sequencial,
    {{ bronze_timestamp("dtcriterio") }} as dtcriterio,
    {{ bronze_inteiro("prioridade") }} as prioridade,
    {{ bronze_inteiro("tipoapoio") }} as tipoapoio,
    {{ bronze_texto("justificativa") }} as justificativa,
    {{ bronze_inteiro("logon") }} as logon,
    {{ bronze_inteiro("idparecervinculdas") }} as idparecervinculdas,
    _fatia
from {{ source("salic", "bronze_sac_criteriosx") }}
