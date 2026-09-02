-- Bronze SALIC — sac__vtermodecompromisso.
-- Origem: salic_bronze.sac__vtermodecompromisso, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 12 colunas: 4 tipadas, 7 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_texto("nrprojeto") }} as nrprojeto,
    {{ bronze_texto("nomeprojeto") }} as nomeprojeto,
    {{ bronze_texto("resumoprojeto") }} as resumoprojeto,
    {{ bronze_texto("nome") }} as nome,
    {{ bronze_texto("uf") }} as uf,
    {{ bronze_texto("cidade") }} as cidade,
    {{ bronze_texto("cgccpf") }} as cgccpf,
    {{ bronze_numerico("solicitado") }} as solicitado,
    {{ bronze_numerico("aprovado") }} as aprovado,
    {{ bronze_timestamp("dtiniciocaptacao") }} as dtiniciocaptacao,
    {{ bronze_timestamp("dtfimcaptacao") }} as dtfimcaptacao,
    _fatia
from {{ source("bronze_sac", "sac__vtermodecompromisso") }}
