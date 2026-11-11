-- Gold do Eixo 1, frente ESTADIC: a proporção de estados com plano, conselho e
-- fundo de cultura (o tripé do SNC) em 2014, 2018 e 2021 (gráfico 6.1 do
-- ESTADIC.R). Só conta resposta 'Sim' ou 'Não'.
with

    instrumentos as (
        select ano, 'Plano Estadual' as instrumento, tem_plano as resposta
        from {{ ref("eixo1_estadic_painel_uf") }}
        where ano in (2014, 2018, 2021)
        union all
        select ano, 'Conselho Estadual' as instrumento, tem_conselho as resposta
        from {{ ref("eixo1_estadic_painel_uf") }}
        where ano in (2014, 2018, 2021)
        union all
        select ano, 'Fundo Estadual' as instrumento, tem_fundo as resposta
        from {{ ref("eixo1_estadic_painel_uf") }}
        where ano in (2014, 2018, 2021)
    )

select
    ano,
    instrumento,
    count(*) filter (where resposta = 'Sim') as quantidade_ufs_sim,
    count(*) as quantidade_ufs,
    avg(case when resposta = 'Sim' then 1 else 0 end) as proporcao
from instrumentos
where resposta in ('Sim', 'Não')
group by ano, instrumento
