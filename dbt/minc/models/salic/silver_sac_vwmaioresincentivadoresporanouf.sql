-- Silver SALIC — silver_sac_vwmaioresincentivadoresporanouf (ex-sac__vwmaioresincentivadoresporanouf).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_sac_vwmaioresincentivadoresporanouf, onde tudo chega como texto
-- da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 8 colunas: 3 tipadas, 4 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_texto("uf") }} as uf,
    {{ bronze_inteiro("ano") }} as ano,
    {{ bronze_texto("tipopessoa") }} as tipopessoa,
    {{ bronze_texto("cnpj_cpf_incentivador") }} as cnpj_cpf_incentivador,
    {{ bronze_texto("incentivador") }} as incentivador,
    {{ bronze_numerico("vlincentivado") }} as vlincentivado,
    {{ bronze_inteiro("ranking") }} as ranking,
    _fatia
from {{ source("salic", "bronze_sac_vwmaioresincentivadoresporanouf") }}
