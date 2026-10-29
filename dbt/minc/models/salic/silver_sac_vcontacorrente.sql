-- Silver SALIC — silver_sac_vcontacorrente (ex-sac__vcontacorrente).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_sac_vcontacorrente, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 7 colunas: 0 tipadas, 6 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_texto("anoprojeto") }} as anoprojeto,
    {{ bronze_texto("sequencial") }} as sequencial,
    {{ bronze_texto("banco") }} as banco,
    {{ bronze_texto("agencia") }} as agencia,
    {{ bronze_texto("contacorrente") }} as contacorrente,
    {{ bronze_texto("artigo") }} as artigo,
    _fatia
from {{ source("salic", "bronze_sac_vcontacorrente") }}
