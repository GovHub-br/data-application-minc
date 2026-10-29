-- Silver SALIC — silver_sac_vmostraordembancaria (ex-sac__vmostraordembancaria).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_sac_vmostraordembancaria, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 7 colunas: 3 tipadas, 3 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_texto("nrordembancaria") }} as nrordembancaria,
    {{ bronze_timestamp("dtordembancaria") }} as dtordembancaria,
    {{ bronze_numerico("valor") }} as valor,
    {{ bronze_texto("usuario") }} as usuario,
    {{ bronze_texto("anoprojeto") }} as anoprojeto,
    {{ bronze_inteiro("sequencial") }} as sequencial,
    _fatia
from {{ source("salic", "bronze_sac_vmostraordembancaria") }}
