-- Gold do Eixo 1, frente SNC: a situação de adesão ao SNC dos municípios de
-- cada região (gráfico 5.1 do SNC.R, eda_1_adesao_regiao). A proporção é dentro
-- da região. Conta só município com região e situação preenchidas, como no R.
with

    contagem as (
        select regiao, situacao_adesao, count(*) as quantidade_municipios
        from {{ ref("eixo1_snc_painel_municipio") }}
        where regiao is not null and situacao_adesao is not null
        group by regiao, situacao_adesao
    )

select
    regiao,
    situacao_adesao,
    quantidade_municipios,
    quantidade_municipios::numeric
    / sum(quantidade_municipios) over (partition by regiao) as proporcao
from contagem
