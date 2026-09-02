-- Bronze SALIC — sac__dba_qtd_apresentado_aprovado_captado.
-- Origem: salic_bronze.sac__dba_qtd_apresentado_aprovado_captado, onde tudo chega como
-- texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 8 colunas: 2 tipadas, 5 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("ano") }} as ano,
    {{ bronze_texto("regiao") }} as regiao,
    {{ bronze_texto("uf") }} as uf,
    {{ bronze_texto("setor") }} as setor,
    {{ bronze_texto("qtd_apresentacao") }} as qtd_apresentacao,
    {{ bronze_texto("qtd_aprovacao") }} as qtd_aprovacao,
    {{ bronze_inteiro("qtd_captacao") }} as qtd_captacao,
    _fatia
from {{ source("bronze_sac", "sac__dba_qtd_apresentado_aprovado_captado") }}
