-- Silver SALIC — silver_sac_editalrelatorio (ex-sac__editalrelatorio).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_sac_editalrelatorio, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 11 colunas: 7 tipadas, 3 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idrelatorio") }} as idrelatorio,
    {{ bronze_texto("anoprojeto") }} as anoprojeto,
    {{ bronze_texto("sequencial") }} as sequencial,
    {{ bronze_inteiro("idedital") }} as idedital,
    {{ bronze_inteiro("nrparcela") }} as nrparcela,
    {{ bronze_timestamp("dtrelatorio") }} as dtrelatorio,
    {{ bronze_texto("relatorio") }} as relatorio,
    {{ bronze_booleano("comprovado") }} as comprovado,
    {{ bronze_booleano("aprovado") }} as aprovado,
    {{ bronze_inteiro("logon") }} as logon,
    _fatia
from {{ source("salic", "bronze_sac_editalrelatorio") }}
