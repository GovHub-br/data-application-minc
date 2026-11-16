-- Gold do Eixo 1, frente MUNIC: a cor ou raça do gestor municipal de cultura em
-- 2018 e 2021 (gráfico 6 do MUNIC.R). Só as cinco categorias do IBGE.
with

    contagem as (
        select
            ano,
            gestor_cor_raca_limpa as gestor_cor_raca,
            count(*) as quantidade_municipios
        from {{ ref("eixo1_munic_painel_municipio") }}
        where ano in (2018, 2021) and gestor_cor_raca_limpa is not null
        group by ano, gestor_cor_raca_limpa
    )

select
    ano,
    gestor_cor_raca,
    quantidade_municipios,
    quantidade_municipios::numeric
    / sum(quantidade_municipios) over (partition by ano) as proporcao
from contagem
