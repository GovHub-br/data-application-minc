-- Gold do Eixo 1, frente MUNIC: o perfil do gestor municipal de cultura por sexo
-- e grupo de raça em 2021 (gráfico 7 do MUNIC.R, interseccionalidade).
with

    contagem as (
        select
            gestor_sexo || ' - ' || gestor_grupo_raca as perfil,
            count(*) as quantidade_municipios
        from {{ ref("eixo1_munic_painel_municipio") }}
        where
            ano = 2021
            and gestor_sexo in ('Feminino', 'Masculino')
            and gestor_grupo_raca is not null
        group by 1
    )

select
    perfil,
    quantidade_municipios,
    quantidade_municipios::numeric / sum(quantidade_municipios) over () as proporcao
from contagem
