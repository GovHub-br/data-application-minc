-- Gold do Eixo 1, frente MUNIC: a faixa do percentual executado do orçamento da
-- cultura em 2021 (gráfico 3 do MUNIC.R, que o associa à Lei Aldir Blanc). Fora
-- a resposta 'Sem Informação'.
with

    contagem as (
        select
            orcamento_perc_executado,
            case
                orcamento_perc_executado
                when '0%'
                then 1
                when 'Até 10%'
                then 2
                when '11% a 20%'
                then 3
                when '21% a 30%'
                then 4
                when '31% a 40%'
                then 5
                when '41% a 50%'
                then 6
                when '51% a 60%'
                then 7
                when '61% a 70%'
                then 8
                when '71% a 80%'
                then 9
                when '81% a 90%'
                then 10
                when 'Mais de 90%'
                then 11
            end as ordem_faixa,
            count(*) as quantidade_municipios
        from {{ ref("eixo1_munic_painel_municipio") }}
        where
            ano = 2021
            and orcamento_perc_executado is not null
            and orcamento_perc_executado <> 'Sem Informação'
        group by 1, 2
    )

select
    orcamento_perc_executado,
    ordem_faixa,
    quantidade_municipios,
    quantidade_municipios::numeric / sum(quantidade_municipios) over () as proporcao
from contagem
