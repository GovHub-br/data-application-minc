-- Gold do Eixo 1, frente MUNIC: o tipo de órgão gestor da cultura em cada
-- edição, em três grupos (gráfico 4 do MUNIC.R). Fora 'Recusa'.
with

    agrupado as (
        select
            ano,
            case
                when tipo_orgao_gestor ~* 'exclusiva|fundação'
                then 'Secretaria Exclusiva / Fundação'
                when tipo_orgao_gestor ~* 'conjunto'
                then 'Secretaria em Conjunto (Ex: Educação e Cultura)'
                else 'Setor Subordinado / Outros'
            end as tipo_orgao_gestor
        from {{ ref("eixo1_munic_painel_municipio") }}
        where
            tipo_orgao_gestor is not null
            and tipo_orgao_gestor not in ('Recusa', 'Sem Informação')
    ),

    contagem as (
        select ano, tipo_orgao_gestor, count(*) as quantidade_municipios
        from agrupado
        group by ano, tipo_orgao_gestor
    )

select
    ano,
    tipo_orgao_gestor,
    quantidade_municipios,
    quantidade_municipios::numeric
    / sum(quantidade_municipios) over (partition by ano) as proporcao
from contagem
