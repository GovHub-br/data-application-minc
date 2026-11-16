-- Gold do Eixo 1, frente MUNIC: a proporção de municípios com plano, conselho e
-- fundo de cultura (o tripé do SNC) em cada edição (gráfico 1 do MUNIC.R). Só
-- conta resposta 'Sim' ou 'Não'.
with

    instrumentos as (
        select ano, 'Plano de Cultura' as instrumento, tem_plano as resposta
        from {{ ref("eixo1_munic_painel_municipio") }}
        union all
        select ano, 'Conselho de Cultura' as instrumento, tem_conselho as resposta
        from {{ ref("eixo1_munic_painel_municipio") }}
        union all
        select ano, 'Fundo de Cultura' as instrumento, tem_fundo as resposta
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
