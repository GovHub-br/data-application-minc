-- Gold do Eixo 1, frente MUNIC: a capacitação dos conselheiros municipais de
-- cultura em 2018 e 2021 (gráfico 17 do MUNIC.R). Cada conselho cai na primeira
-- categoria que responde 'Sim': continuada, eventual, não realiza. Fora os que
-- não respondem nenhuma.
with

    classificado as (
        select
            ano,
            case
                when cons_cap_continuada = 'Sim'
                then 'Capacitação Continuada'
                when cons_cap_eventual = 'Sim'
                then 'Capacitação Eventual'
                when cons_cap_nao_realiza = 'Sim'
                then 'Não Realiza Capacitação'
            end as capacitacao
        from {{ ref("eixo1_munic_painel_municipio") }}
        where ano in (2018, 2021) and tem_conselho = 'Sim'
    ),

    contagem as (
        select ano, capacitacao, count(*) as quantidade_municipios
        from classificado
        where capacitacao is not null
        group by ano, capacitacao
    )

select
    ano,
    capacitacao,
    quantidade_municipios,
    quantidade_municipios::numeric
    / sum(quantidade_municipios) over (partition by ano) as proporcao
from contagem
