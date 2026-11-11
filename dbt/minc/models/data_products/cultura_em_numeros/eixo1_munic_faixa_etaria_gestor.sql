-- Gold do Eixo 1, frente MUNIC: a faixa de idade do gestor municipal de cultura
-- em 2014, 2018 e 2021 (gráfico 9 do MUNIC.R).
with

    contagem as (
        select
            ano,
            gestor_faixa_etaria,
            case
                gestor_faixa_etaria
                when 'Até 29 anos'
                then 1
                when '30 a 39 anos'
                then 2
                when '40 a 49 anos'
                then 3
                when '50 a 59 anos'
                then 4
                when '60 anos ou mais'
                then 5
            end as ordem_faixa,
            count(*) as quantidade_municipios
        from {{ ref("eixo1_munic_painel_municipio") }}
        where ano in (2014, 2018, 2021) and gestor_faixa_etaria is not null
        group by 1, 2, 3
    )

select
    ano,
    gestor_faixa_etaria,
    ordem_faixa,
    quantidade_municipios,
    quantidade_municipios::numeric
    / sum(quantidade_municipios) over (partition by ano) as proporcao
from contagem
