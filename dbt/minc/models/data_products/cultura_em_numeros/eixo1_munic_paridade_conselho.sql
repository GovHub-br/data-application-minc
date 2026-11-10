-- Gold do Eixo 1, frente MUNIC: a composição dos conselhos municipais de cultura
-- por edição da MUNIC. É a parte da MUNIC da Figura 15 da publicação (2014, 2018
-- e 2021) e o gráfico 13 do MUNIC.R. A linha de 2026 da figura vem do painel do
-- SNC e é mantida à mão no repositório do Eixo 1; não está aqui.
--
-- Conta só os conselhos com composição informada, 'Paritário' ou 'Não
-- Paritário', e a proporção é dentro da edição, como no R.
with

    contagem as (
        select
            ano, cons_paritario as composicao_conselho, count(*) as quantidade_municipios
        from {{ ref("eixo1_munic_conselho_municipio") }}
        where cons_paritario in ('Paritário', 'Não Paritário')
        group by ano, cons_paritario
    )

select
    ano,
    composicao_conselho,
    quantidade_municipios,
    quantidade_municipios::numeric
    / sum(quantidade_municipios) over (partition by ano) as proporcao
from contagem
