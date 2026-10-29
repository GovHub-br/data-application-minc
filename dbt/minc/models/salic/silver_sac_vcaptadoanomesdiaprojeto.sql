-- Silver SALIC — silver_sac_vcaptadoanomesdiaprojeto (ex-sac__vcaptadoanomesdiaprojeto).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_sac_vcaptadoanomesdiaprojeto, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 7 colunas: 5 tipadas, 1 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("ano") }} as ano,
    {{ bronze_inteiro("mes") }} as mes,
    {{ bronze_inteiro("dia") }} as dia,
    {{ bronze_texto("anoprojeto") }} as anoprojeto,
    {{ bronze_inteiro("sequencial") }} as sequencial,
    {{ bronze_numerico("vlcaptado") }} as vlcaptado,
    _fatia
from {{ source("salic", "bronze_sac_vcaptadoanomesdiaprojeto") }}
