-- Gold do Eixo 1, frente MUNIC: nos municípios com plano de cultura, quantos o
-- instituíram por lei, em 2018 e 2021 (gráfico 16A do MUNIC.R).
with

    contagem as (
        select
            ano,
            case
                when plano_tem_lei = 'Sim'
                then 'Possui Instrumento Legal'
                when plano_tem_lei = 'Não'
                then 'Sem Instrumento Legal'
                else 'Sem Informação'
            end as status_lei,
            count(*) as quantidade_municipios
        from {{ ref("eixo1_munic_painel_municipio") }}
        where ano in (2018, 2021) and tem_plano = 'Sim'
        group by 1, 2
    )

select
    ano,
    status_lei,
    quantidade_municipios,
    quantidade_municipios::numeric
    / sum(quantidade_municipios) over (partition by ano) as proporcao
from contagem
