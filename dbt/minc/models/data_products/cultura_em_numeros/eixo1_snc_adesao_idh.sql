-- Gold do Eixo 1, frente SNC: a situação de adesão ao SNC por faixa de IDH 2021
-- do município (gráfico 6.1 do SNC.R, eda_7_adesao_vs_idh). Fica de fora o
-- município sem IDH, como no R. A proporção é dentro da faixa.
with

    contagem as (
        select categoria_idh, situacao_adesao, count(*) as quantidade_municipios
        from {{ ref("eixo1_snc_painel_municipio") }}
        where situacao_adesao is not null and categoria_idh is not null
        group by categoria_idh, situacao_adesao
    )

select
    categoria_idh,
    situacao_adesao,
    quantidade_municipios,
    quantidade_municipios::numeric
    / sum(quantidade_municipios) over (partition by categoria_idh) as proporcao
from contagem
