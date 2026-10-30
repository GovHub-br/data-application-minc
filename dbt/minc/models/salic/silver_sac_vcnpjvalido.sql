-- Silver SALIC — silver_sac_vcnpjvalido (ex-sac__vcnpjvalido).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_sac_vcnpjvalido, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 7 colunas: 1 tipadas, 5 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_texto("anoprojeto") }} as anoprojeto,
    {{ bronze_texto("sequencial") }} as sequencial,
    {{ bronze_texto("nomeprojeto") }} as nomeprojeto,
    {{ bronze_texto("cgccpf") }} as cgccpf,
    {{ bronze_inteiro("cnpjvalido") }} as cnpjvalido,
    {{ bronze_texto("nome") }} as nome,
    _fatia
from {{ source("salic", "bronze_sac_vcnpjvalido") }}
