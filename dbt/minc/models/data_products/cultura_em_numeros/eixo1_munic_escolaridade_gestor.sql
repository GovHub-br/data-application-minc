-- Gold do Eixo 1, frente MUNIC: a escolaridade do gestor municipal de cultura em
-- cada edição (gráfico 8 do MUNIC.R). Fora a resposta que não cabe em nenhum
-- grupo ('Sem Informação').
with

    contagem as (
        select ano, gestor_escolaridade_agrupada, count(*) as quantidade_municipios
        from {{ ref("eixo1_munic_painel_municipio") }}
        where
            gestor_escolaridade_agrupada is not null
            and gestor_escolaridade_agrupada <> 'Sem Informação'
        group by ano, gestor_escolaridade_agrupada
    )

select
    ano,
    gestor_escolaridade_agrupada,
    quantidade_municipios,
    quantidade_municipios::numeric
    / sum(quantidade_municipios) over (partition by ano) as proporcao
from contagem
