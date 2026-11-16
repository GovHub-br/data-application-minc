-- Gold do Eixo 1, frente ESTADIC: a proporção de estados com plano, conselho e
-- fundo de cultura (o tripé do SNC) em 2014, 2018 e 2021 (gráfico 6.1 do
-- ESTADIC.R). Só conta resposta 'Sim' ou 'Não'.
--
-- As edições vêm de edicao_pergunta_tripe, do painel: 2012 pergunta o conselho
-- e o fundo, mas não o plano, e fica fora do gráfico, como no R.
with

    instrumentos as (
        select painel.ano, item.instrumento, item.resposta
        from {{ ref("eixo1_estadic_painel_uf") }} as painel
        cross join
            lateral(  -- noqa: LT01
                values
                    ('Plano Estadual', painel.tem_plano),
                    ('Conselho Estadual', painel.tem_conselho),
                    ('Fundo Estadual', painel.tem_fundo)
            ) as item(instrumento, resposta)  -- noqa: LT01
        where painel.edicao_pergunta_tripe
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
