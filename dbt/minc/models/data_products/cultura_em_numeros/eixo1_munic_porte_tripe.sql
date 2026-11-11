-- Gold do Eixo 1, frente MUNIC: o tripé completo (plano, conselho e fundo) por
-- porte populacional em 2021 (gráfico 15 do MUNIC.R). A população é a da MUNIC
-- 2021; a proporção é dentro de cada porte.
--
-- O tripé vem pronto do painel (tem_tripe_completo). Quem tem item vazio e nenhum
-- 'Não' fica fora da contagem e do total do porte; o R o contaria como
-- incompleto. Em 11/11/2026 nenhum município de 2021 tem item do tripé vazio.
with

    base as (
        select
            tem_tripe_completo,
            case
                when populacao <= 20000
                then 'Até 20 mil hab.'
                when populacao <= 50000
                then '20.001 a 50 mil hab.'
                when populacao <= 100000
                then '50.001 a 100 mil hab.'
                else 'Mais de 100 mil hab.'
            end as porte_populacional
        from {{ ref("eixo1_munic_painel_municipio") }}
        where ano = 2021 and populacao is not null and tem_tripe_completo is not null
    ),

    contagem as (
        select
            porte_populacional,
            case
                porte_populacional
                when 'Até 20 mil hab.'
                then 1
                when '20.001 a 50 mil hab.'
                then 2
                when '50.001 a 100 mil hab.'
                then 3
                when 'Mais de 100 mil hab.'
                then 4
            end as ordem_porte,
            case
                when tem_tripe_completo = 'Sim'
                then 'Tripé Completo'
                else 'Incompleto / Nenhum'
            end as status_tripe,
            count(*) as quantidade_municipios
        from base
        group by 1, 2, 3
    )

select
    porte_populacional,
    ordem_porte,
    status_tripe,
    quantidade_municipios,
    quantidade_municipios::numeric
    / sum(quantidade_municipios) over (partition by porte_populacional) as proporcao
from contagem
