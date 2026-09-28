-- Bronze SALIC — sac__captacaoquotas.
-- Origem: salic_bronze.sac__captacaoquotas, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 11 colunas: 3 tipadas, 7 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_texto("anoprojeto") }} as anoprojeto,
    {{ bronze_texto("sequencial") }} as sequencial,
    {{ bronze_texto("anocav") }} as anocav,
    {{ bronze_texto("sequencialcav") }} as sequencialcav,
    {{ bronze_texto("nintegra") }} as nintegra,
    {{ bronze_texto("cgccpfsub") }} as cgccpfsub,
    {{ bronze_timestamp("dtintegraliza") }} as dtintegraliza,
    {{ bronze_inteiro("qtdquotasintegr") }} as qtdquotasintegr,
    {{ bronze_texto("observacao") }} as observacao,
    {{ bronze_inteiro("logon") }} as logon,
    _fatia
from {{ source("bronze_sac", "sac__captacaoquotas") }}
