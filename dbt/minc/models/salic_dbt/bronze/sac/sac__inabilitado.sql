-- Bronze SALIC — sac__inabilitado.
-- Origem: salic_bronze.sac__inabilitado, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 10 colunas: 5 tipadas, 4 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_texto("cgccpf") }} as cgccpf,
    {{ bronze_texto("anoprojeto") }} as anoprojeto,
    {{ bronze_texto("sequencial") }} as sequencial,
    {{ bronze_inteiro("orgao") }} as orgao,
    {{ bronze_inteiro("logon") }} as logon,
    {{ bronze_texto("habilitado") }} as habilitado,
    {{ bronze_inteiro("idprojeto") }} as idprojeto,
    {{ bronze_inteiro("idtipoinabilitado") }} as idtipoinabilitado,
    {{ bronze_data("dtinabilitado") }} as dtinabilitado,
    _fatia
from {{ source("bronze_sac", "sac__inabilitado") }}
