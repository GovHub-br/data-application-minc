-- Gold do Eixo 1, frente MUNIC: a proporção de municípios com lei e com conselho
-- de patrimônio cultural em cada edição (gráfico 12 do MUNIC.R). Só conta
-- resposta 'Sim' ou 'Não'.
with

    instrumentos as (
        select
            ano,
            'Legislação Específica de Patrimônio' as instrumento,
            tem_lei_patrimonio as resposta
        from {{ ref("eixo1_munic_painel_municipio") }}
        union all
        select
            ano,
            'Conselho de Patrimônio Cultural' as instrumento,
            tem_cons_patrimonio as resposta
        from {{ ref("eixo1_munic_painel_municipio") }}
    )

select
    ano,
    instrumento,
    count(*) filter (where resposta = 'Sim') as quantidade_municipios_sim,
    count(*) as quantidade_municipios,
    avg(case when resposta = 'Sim' then 1 else 0 end) as proporcao
from instrumentos
where resposta in ('Sim', 'Não')
group by ano, instrumento
