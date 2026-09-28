-- Bronze SALIC — sac__geralext.
-- Origem: salic_bronze.sac__geralext, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 18 colunas: 4 tipadas, 13 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("nrreuniao") }} as nrreuniao,
    {{ bronze_texto('"área cultural"') }} as area_cultural,
    {{ bronze_texto("projeto") }} as projeto,
    {{ bronze_texto('"nome do projeto"') }} as nome_do_projeto,
    {{ bronze_texto('"resumo do projeto"') }} as resumo_do_projeto,
    {{ bronze_texto('"tipo de parecer"') }} as tipo_de_parecer,
    {{ bronze_texto("parecer") }} as parecer,
    {{ bronze_texto('"parecer técnico"') }} as parecer_tecnico,
    {{ bronze_texto("proponente") }} as proponente,
    {{ bronze_texto('"cnpj/cpf"') }} as cnpj_cpf,
    {{ bronze_texto("cidade") }} as cidade,
    {{ bronze_texto("uf") }} as uf,
    {{ bronze_numerico("solicitado") }} as solicitado,
    {{ bronze_numerico("sugerido") }} as sugerido,
    {{ bronze_inteiro("enquadramento") }} as enquadramento,
    {{ bronze_texto("processo") }} as processo,
    {{ bronze_texto('"situação"') }} as situacao,
    _fatia
from {{ source("bronze_sac", "sac__geralext") }}
