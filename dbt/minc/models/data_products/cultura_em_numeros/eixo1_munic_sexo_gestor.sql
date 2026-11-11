-- Gold do Eixo 1, frente MUNIC: o sexo do gestor municipal de cultura em 2014,
-- 2018 e 2021 (gráfico 2 do MUNIC.R). Só 'Feminino' e 'Masculino'.
with

    contagem as (
        select ano, gestor_sexo, count(*) as quantidade_municipios
        from {{ ref("eixo1_munic_painel_municipio") }}
        where ano in (2014, 2018, 2021) and gestor_sexo in ('Feminino', 'Masculino')
        group by ano, gestor_sexo
    )

select
    ano,
    gestor_sexo,
    quantidade_municipios,
    quantidade_municipios::numeric
    / sum(quantidade_municipios) over (partition by ano) as proporcao
from contagem
