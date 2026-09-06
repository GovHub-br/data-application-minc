-- Bronze SALIC — sac__vwmaioresincentivadoresporanouf.
-- Origem: salic_bronze.sac__vwmaioresincentivadoresporanouf, onde tudo chega como texto
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
from {{ source("bronze_sac", "sac__vwmaioresincentivadoresporanouf") }}
