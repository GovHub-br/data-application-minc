-- Gold do Eixo 1, frente MUNIC: nos municípios com plano de cultura, quantos o
-- fizeram com participação social, em 2018 e 2021 (gráfico 16B do MUNIC.R).
with

    contagem as (
        select
            ano,
            case
                when plano_participacao_soc = 'Sim'
                then 'Com Participação Social'
                when plano_participacao_soc = 'Não'
                then 'Sem Participação'
                else 'Sem Informação'
            end as status_participacao,
            count(*) as quantidade_municipios
        from {{ ref("eixo1_munic_painel_municipio") }}
        where ano in (2018, 2021) and tem_plano = 'Sim'
        group by 1, 2
    )

select
    ano,
    status_participacao,
    quantidade_municipios,
    quantidade_municipios::numeric
    / sum(quantidade_municipios) over (partition by ano) as proporcao
from contagem
