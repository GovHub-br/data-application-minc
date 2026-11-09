-- Gold do Eixo 1, frente MUNIC: a escolaridade do gestor nos municípios com e
-- sem o tripé completo (plano, conselho e fundo) em 2021 (gráfico 5 do MUNIC.R).
-- A proporção é dentro de cada grupo de tripé.
with

    base as (
        select
            gestor_escolaridade_agrupada,
            (tem_plano = 'Sim')::int
            + (tem_conselho = 'Sim')::int
            + (tem_fundo = 'Sim')::int as instrumentos_do_tripe
        from {{ ref("eixo1_munic_painel_municipio") }}
        where
            ano = 2021
            and gestor_escolaridade_agrupada is not null
            and gestor_escolaridade_agrupada <> 'Sem Informação'
    ),

    contagem as (
        select
            gestor_escolaridade_agrupada,
            case
                when instrumentos_do_tripe = 3
                then 'Com Tripé Completo'
                when instrumentos_do_tripe is not null
                then 'Sem Tripé Completo'
            end as tripe_completo,
            count(*) as quantidade_municipios
        from base
        group by 1, 2
    )

select
    tripe_completo,
    gestor_escolaridade_agrupada,
    quantidade_municipios,
    quantidade_municipios::numeric
    / sum(quantidade_municipios) over (partition by tripe_completo) as proporcao
from contagem
