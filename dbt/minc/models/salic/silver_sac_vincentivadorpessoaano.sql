-- Silver SALIC — silver_sac_vincentivadorpessoaano (ex-sac__vincentivadorpessoaano).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_sac_vincentivadorpessoaano, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 7 colunas: 1 tipadas, 5 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_texto("ano") }} as ano,
    {{ bronze_texto("regiao") }} as regiao,
    {{ bronze_texto("uf") }} as uf,
    {{ bronze_texto("tipopessoa") }} as tipopessoa,
    {{ bronze_texto("nome") }} as nome,
    {{ bronze_numerico("apoioreal") }} as apoioreal,
    _fatia
from {{ source("salic", "bronze_sac_vincentivadorpessoaano") }}
