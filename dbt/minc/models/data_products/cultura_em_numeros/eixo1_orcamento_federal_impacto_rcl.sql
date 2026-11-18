-- Gold do Eixo 1, frente orçamento, esfera federal: o gasto federal direto em
-- cultura como fração da Receita Corrente Líquida da União, por exercício
-- (gráfico VI do gasto_federal.R, grafico_impacto_rcl).
--
-- NUMERADOR: o empenhado nominal de todas as linhas da gold-base
-- eixo1_orcamento_federal_empenho_cultura no exercício, o siop_direto do R. A
-- LPG de 2022 fica em 2022; a Figura 3, que a leva para 2023, é
-- eixo1_orcamento_federal_rcl_lpg_2023.
--
-- DENOMINADOR: a RCL da União de eixo1_orcamento_federal_rcl_uniao.
--
-- DIFERENTE DO R: o R tira os exercícios sem RCL; aqui eles ficam, com a RCL e
-- a participação nulas (ADR 0010, regra 1), e quem consome corta. Hoje a RCL
-- cobre 2003 a 2017; de 2018 em diante ela chega com o ramo do SICONFI na
-- gold-base, e este modelo não muda.
-- GRÃO: uma linha por exercício da gold-base.
with

    gasto as (
        select ano, sum(empenhado) as valor_empenhado
        from {{ ref("eixo1_orcamento_federal_empenho_cultura") }}
        group by ano
    )

select
    g.ano,
    g.valor_empenhado,
    r.rcl,
    r.fonte_rcl,
    case when r.rcl > 0 then g.valor_empenhado / r.rcl end as participacao_rcl
from gasto as g
left join {{ ref("eixo1_orcamento_federal_rcl_uniao") }} as r on g.ano = r.ano
