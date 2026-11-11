-- Gold do Eixo 1, frente MUNIC: a competência dos conselhos municipais de cultura
-- em 2014, 2018 e 2021 (gráfico 14 do MUNIC.R). Fora a competência não
-- informada.
with

    contagem as (
        select ano, cons_competencia, count(*) as quantidade_municipios
        from {{ ref("eixo1_munic_conselho_municipio") }}
        where cons_competencia <> 'Sem Informação'
        group by ano, cons_competencia
    )

select
    ano,
    cons_competencia,
    quantidade_municipios,
    quantidade_municipios::numeric
    / sum(quantidade_municipios) over (partition by ano) as proporcao
from contagem
