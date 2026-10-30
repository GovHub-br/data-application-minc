-- Silver SALIC — silver_sac_historicoinabilitado (ex-sac__historicoinabilitado).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_sac_historicoinabilitado, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 9 colunas: 4 tipadas, 4 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("contador") }} as contador,
    {{ bronze_texto("cgccpf") }} as cgccpf,
    {{ bronze_texto("anoprojeto") }} as anoprojeto,
    {{ bronze_texto("sequencial") }} as sequencial,
    {{ bronze_timestamp("dthabilitado") }} as dthabilitado,
    {{ bronze_inteiro("orgao") }} as orgao,
    {{ bronze_texto("habilitado") }} as habilitado,
    {{ bronze_inteiro("logon") }} as logon,
    _fatia
from {{ source("salic", "bronze_sac_historicoinabilitado") }}
